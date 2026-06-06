"""
DAG: Построение витрин данных (Data Marts).

Расписание: по будням в 20:00 UTC — после завершения Spark ETL.
Читает из vault схемы, пишет в dm схему.
Идемпотентность: DELETE + INSERT по дате в каждом SQL файле.
"""

from datetime import datetime, timezone

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from base.base_dag import create_dag

dag = create_dag(
    dag_id="build_dm_marts",
    schedule="0 20 * * 1-5",
    description="Построение витрин dm_quotes_daily и dm_fx_rates из Data Vault",
    tags=["marts", "dm"],
)

# DDL для витрин — создаём если не существуют
CREATE_DM_QUOTES = """
CREATE TABLE IF NOT EXISTS dm.dm_quotes_daily (
    instrument_bk       VARCHAR(20)     NOT NULL,
    instrument_name     VARCHAR(255),
    exchange            VARCHAR(100),
    asset_type          VARCHAR(50),
    trade_date          DATE            NOT NULL,
    open_price          NUMERIC(18, 6),
    high_price          NUMERIC(18, 6),
    low_price           NUMERIC(18, 6),
    close_price         NUMERIC(18, 6),
    volume              BIGINT,
    daily_return_pct    NUMERIC(10, 4),
    price_range         NUMERIC(18, 6),
    loaded_at           TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (instrument_bk, trade_date)
);
"""

CREATE_DM_FX = """
CREATE TABLE IF NOT EXISTS dm.dm_fx_rates (
    base_currency       VARCHAR(10)     NOT NULL,
    target_currency     VARCHAR(10)     NOT NULL,
    rate                NUMERIC(18, 8)  NOT NULL,
    rate_date           DATE            NOT NULL,
    source              VARCHAR(100),
    loaded_at           TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (base_currency, target_currency, rate_date, source)
);
"""


def update_refresh_log(**context) -> None:
    """
    Записывает время последнего успешного обновления витрины в лог-таблицу.

    Полезно для мониторинга: можно легко узнать когда последний раз
    обновлялась каждая витрина не парся логи Airflow.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="dwh_postgres")
    execution_date = context["ds"]

    hook.run("""
        CREATE TABLE IF NOT EXISTS dm.mart_refresh_log (
            mart_name       VARCHAR(100)    NOT NULL,
            execution_date  DATE            NOT NULL,
            refreshed_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
            PRIMARY KEY (mart_name, execution_date)
        );

        INSERT INTO dm.mart_refresh_log (mart_name, execution_date)
        VALUES ('dm_quotes_daily', %(date)s), ('dm_fx_rates', %(date)s)
        ON CONFLICT (mart_name, execution_date)
        DO UPDATE SET refreshed_at = NOW();
    """, parameters={"date": execution_date})


with dag:

    # Создаём витрины если их нет
    ensure_dm_quotes = PostgresOperator(
        task_id="ensure_dm_quotes_table",
        postgres_conn_id="dwh_postgres",
        sql=CREATE_DM_QUOTES,
    )

    ensure_dm_fx = PostgresOperator(
        task_id="ensure_dm_fx_table",
        postgres_conn_id="dwh_postgres",
        sql=CREATE_DM_FX,
    )

    # Загружаем витрины — {{ ds }} подставляет дату запуска DAG-а
    load_dm_quotes = PostgresOperator(
        task_id="load_dm_quotes_daily",
        postgres_conn_id="dwh_postgres",
        sql="sql/marts/dm_quotes_daily.sql",
        parameters={"trade_date": "{{ ds }}"},
    )

    load_dm_fx = PostgresOperator(
        task_id="load_dm_fx_rates",
        postgres_conn_id="dwh_postgres",
        sql="sql/marts/dm_fx_rates.sql",
        parameters={"trade_date": "{{ ds }}"},
    )

    log_refresh = PythonOperator(
        task_id="update_refresh_log",
        python_callable=update_refresh_log,
    )

    # ── Зависимости ────────────────────────────────────────────────────────
    #
    #  ensure_dm_quotes ──→ load_dm_quotes ──┐
    #                                         ├──→ log_refresh
    #  ensure_dm_fx     ──→ load_dm_fx     ──┘
    #
    ensure_dm_quotes >> load_dm_quotes >> log_refresh
    ensure_dm_fx >> load_dm_fx >> log_refresh
