"""
DAG: Spark ETL — загрузка из Raw слоя в Data Vault.

Запускается только через TriggerDagRunOperator из dag_ingest_quotes,
не по расписанию (schedule=None).

Порядок загрузки важен:
    Hub грузится ДО Satellite — Satellite ссылается на Hub через FK.
    Hub Instrument и Hub Currency можно грузить параллельно.
"""

from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.dags.base.base_dag import create_dag

dag = create_dag(
    dag_id="spark_raw_to_vault",
    schedule=None,   # только ручной запуск или через TriggerDagRunOperator
    description="Spark ETL: перекладывает данные из raw схемы в Data Vault (vault схема)",
    tags=["etl", "spark", "data-vault"],
)


# ── Функции тасков ─────────────────────────────────────────────────────────

def validate_vault_counts(**context) -> None:
    """
    Проверяет что данные реально попали в vault после ETL.

    Если записей нет — скипаем DAG (не фейлим).
    AirflowSkipException — специальное исключение Airflow:
    таск становится жёлтым (skipped), а не красным (failed).
    Используем когда отсутствие данных — не ошибка, а ожидаемое состояние
    (например выходной день, биржа не работала).
    """
    execution_date = context["ds"]  # формат YYYY-MM-DD из контекста Airflow

    hook = PostgresHook(postgres_conn_id="dwh_postgres")

    hub_count = hook.get_first(
        "SELECT COUNT(*) FROM vault.hub_instrument WHERE load_date::date = %s",
        parameters=[execution_date],
    )[0]

    sat_count = hook.get_first(
        "SELECT COUNT(*) FROM vault.sat_quote_prices WHERE trade_date = %s",
        parameters=[execution_date],
    )[0]

    if hub_count == 0 and sat_count == 0:
        raise AirflowSkipException(
            f"No vault data found for {execution_date}. Skipping downstream tasks."
        )


# ── Таски ──────────────────────────────────────────────────────────────────

with dag:

    # BashOperator запускает spark-submit — так Spark job стартует
    # как отдельный процесс с правильным classpath и конфигами.
    # execution_date передаётся через Airflow template {{ ds }}.

    load_hub_instrument = BashOperator(
        task_id="load_hub_instrument",
        bash_command=(
            "python -m src.processing.raw_to_vault.load_hub_instrument "
            "--date {{ ds }}"
        ),
    )

    load_hub_currency = BashOperator(
        task_id="load_hub_currency",
        bash_command=(
            # TODO: создать LoadHubCurrency по аналогии с LoadHubInstrument
            "echo 'LoadHubCurrency placeholder for {{ ds }}'"
        ),
    )

    load_sat_quote_prices = BashOperator(
        task_id="load_sat_quote_prices",
        bash_command=(
            "python -m src.processing.raw_to_vault.load_sat_quote_prices "
            "--date {{ ds }}"
        ),
    )

    load_sat_rate_values = BashOperator(
        task_id="load_sat_rate_values",
        bash_command=(
            # TODO: создать LoadSatRateValues по аналогии с LoadSatQuotePrices
            "echo 'LoadSatRateValues placeholder for {{ ds }}'"
        ),
    )

    validate_counts = PythonOperator(
        task_id="validate_vault_counts",
        python_callable=validate_vault_counts,
    )

    # ── Зависимости ────────────────────────────────────────────────────────
    #
    #  load_hub_instrument ──→ load_sat_quote_prices ──┐
    #                                                   ├──→ validate_counts
    #  load_hub_currency   ──→ load_sat_rate_values  ──┘
    #
    load_hub_instrument >> load_sat_quote_prices >> validate_counts
    load_hub_currency >> load_sat_rate_values >> validate_counts
