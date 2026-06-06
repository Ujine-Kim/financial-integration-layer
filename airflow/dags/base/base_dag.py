"""
Базовые утилиты для стандартизации всех DAG-ов в проекте.

Цель: все DAG-и выглядят единообразно — одинаковые default_args,
одинаковые теги, одинаковая политика retry.
Стандарты определяются в одном месте, а не разбросаны по файлам.
"""

from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago


# ── Default Args ───────────────────────────────────────────────────────────

def create_default_args(owner: str = "fil-team") -> dict:
    """
    Возвращает стандартные аргументы для всех DAG-ов проекта.

    Args:
        owner: владелец DAG-а для отображения в Airflow UI.

    Returns:
        dict с default_args готовый для передачи в DAG().

    Почему retries=2, а не больше?
    Большинство ошибок либо исправляются со второй попытки (временная сеть),
    либо не исправятся вообще (баг в коде, нет данных).
    3+ ретрая только затягивают обнаружение реальной проблемы.
    """
    return {
        "owner": owner,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
        "depends_on_past": False,  # каждый запуск независим
    }


# ── DAG Factory ────────────────────────────────────────────────────────────

def create_dag(
    dag_id: str,
    schedule: str | None,
    description: str,
    tags: list[str] | None = None,
    owner: str = "fil-team",
    start_date_days_ago: int = 1,
) -> DAG:
    """
    Фабричная функция для создания стандартизированного DAG-а.

    Все DAG-и в проекте создаются через эту функцию —
    гарантирует единообразие настроек.

    Args:
        dag_id: уникальный идентификатор DAG-а.
        schedule: cron-выражение или None (только ручной запуск).
        description: человекочитаемое описание что делает DAG.
        tags: теги для фильтрации в UI. "financial-integration-layer"
              добавляется автоматически.
        owner: владелец DAG-а.
        start_date_days_ago: как давно начинается история запусков.

    Returns:
        Сконфигурированный объект DAG.

    Example:
        dag = create_dag(
            dag_id="ingest_quotes_daily",
            schedule="0 18 * * 1-5",
            description="Загрузка котировок из Alpha Vantage",
            tags=["ingestion", "alpha-vantage"],
        )
    """
    # Всегда добавляем проектный тег — удобно фильтровать в UI
    all_tags = ["financial-integration-layer"] + (tags or [])

    dag = DAG(
        dag_id=dag_id,
        description=description,
        schedule_interval=schedule,
        start_date=days_ago(start_date_days_ago),
        default_args=create_default_args(owner=owner),
        catchup=False,       # не запускать пропущенные интервалы при старте
        tags=all_tags,
        doc_md=description,  # описание отображается в Airflow UI
    )

    return dag
