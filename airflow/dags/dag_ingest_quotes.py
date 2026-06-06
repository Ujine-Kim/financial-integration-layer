"""
DAG: Ежедневная загрузка котировок и валютных курсов.

Расписание: по будням в 18:00 UTC (после закрытия NYSE в 16:00 ET).
Пайплайн:
    1. Проверяем доступность Alpha Vantage API
    2. Параллельно загружаем котировки (Alpha Vantage) и курсы ЦБ РФ в Kafka
    3. Проверяем что сообщения появились в топике
    4. Запускаем Spark ETL (Raw → Vault)
"""

import sys
import os

from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor

from base.base_dag import create_dag

# ── DAG ────────────────────────────────────────────────────────────────────

dag = create_dag(
    dag_id="ingest_quotes_daily",
    schedule="0 18 * * 1-5",   # по будням в 18:00 UTC
    description="Загрузка биржевых котировок (Alpha Vantage) и курсов ЦБ РФ в Kafka",
    tags=["ingestion", "alpha-vantage", "cbr"],
)


# ── Функции тасков ─────────────────────────────────────────────────────────

def check_alpha_vantage_available() -> bool:
    """
    Проверяет доступность Alpha Vantage API перед началом загрузки.

    Делает минимальный запрос (CURRENCY_EXCHANGE_RATE — лёгкий endpoint).
    Возвращает True если API отвечает, иначе бросает исключение.
    """
    import requests
    from src.common.config import settings

    response = requests.get(
        "https://www.alphavantage.co/query",
        params={
            "function": "CURRENCY_EXCHANGE_RATE",
            "from_currency": "USD",
            "to_currency": "RUB",
            "apikey": settings.alpha_vantage_api_key,
        },
        timeout=10,
    )
    response.raise_for_status()
    data = response.json()

    if "Error Message" in data:
        raise ValueError(f"Alpha Vantage API error: {data['Error Message']}")

    return True


def fetch_alpha_vantage(**context) -> None:
    """
    Запускает AlphaVantageProducer для списка символов из Airflow Variable.

    Символы хранятся в Variable "WATCH_SYMBOLS" — можно менять в UI
    без изменения кода и перезапуска Airflow.

    Airflow Variables — key-value хранилище в метабазе Airflow.
    Удобно для конфигурации которая меняется без деплоя.
    """
    import json
    from src.ingestion.producers.alpha_vantage_producer import AlphaVantageProducer

    # Достаём список символов из Airflow Variable (или берём дефолт)
    symbols_raw = Variable.get("WATCH_SYMBOLS", default_var='["AAPL","MSFT","GOOGL","AMZN","TSLA"]')
    symbols = json.loads(symbols_raw)

    with AlphaVantageProducer(symbols=symbols) as producer:
        producer.fetch_and_produce()


def fetch_cbr_rates(**context) -> None:
    """Запускает CBRProducer для загрузки курсов ЦБ РФ."""
    from src.ingestion.producers.cbr_producer import CBRProducer

    with CBRProducer() as producer:
        producer.fetch_and_produce()


def check_kafka_has_messages(**context) -> bool:
    """
    Сенсор: проверяет что в топике quotes.raw появились сообщения за сегодня.

    Используем confluent_kafka Consumer для проверки наличия сообщений.
    mode="reschedule" в сенсоре — освобождает воркер пока ждёт,
    не блокирует слот воркера всё время ожидания.

    Returns:
        True если сообщения есть, False если ещё нет (сенсор будет ждать).
    """
    from confluent_kafka import Consumer, TopicPartition
    from src.common.config import settings

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": "airflow-sensor-checker",
        "auto.offset.reset": "latest",
    })

    try:
        # Получаем метаданные топика
        metadata = consumer.list_topics("quotes.raw", timeout=5)
        if "quotes.raw" not in metadata.topics:
            return False

        topic_meta = metadata.topics["quotes.raw"]
        partitions = [
            TopicPartition("quotes.raw", p)
            for p in topic_meta.partitions.keys()
        ]

        # Получаем watermarks (low, high offset) для каждой партиции
        has_messages = False
        for tp in partitions:
            low, high = consumer.get_watermark_offsets(tp, timeout=5)
            if high > low:  # есть хоть одно сообщение
                has_messages = True
                break

        return has_messages
    finally:
        consumer.close()


# ── Таски ──────────────────────────────────────────────────────────────────

with dag:

    # 1. Проверяем API
    check_api = PythonOperator(
        task_id="check_api_availability",
        python_callable=check_alpha_vantage_available,
    )

    # 2. Параллельная загрузка из двух источников
    fetch_av = PythonOperator(
        task_id="fetch_alpha_vantage",
        python_callable=fetch_alpha_vantage,
    )

    fetch_cbr = PythonOperator(
        task_id="fetch_cbr_rates",
        python_callable=fetch_cbr_rates,
    )

    # 3. Ждём появления сообщений в Kafka
    validate_kafka = PythonSensor(
        task_id="validate_kafka_messages",
        python_callable=check_kafka_has_messages,
        mode="reschedule",      # освобождаем воркер пока ждём
        poke_interval=60,       # проверяем каждую минуту
        timeout=600,            # максимум 10 минут ждём
    )

    # 4. Запускаем Spark ETL
    trigger_etl = TriggerDagRunOperator(
        task_id="trigger_spark_etl",
        trigger_dag_id="spark_raw_to_vault",
        wait_for_completion=False,  # не ждём завершения ETL
    )

    # ── Зависимости ────────────────────────────────────────────────────────
    #
    #  check_api ──→ fetch_alpha_vantage ──┐
    #                                       ├──→ validate_kafka ──→ trigger_etl
    #           └──→ fetch_cbr_rates    ──┘
    #
    check_api >> [fetch_av, fetch_cbr] >> validate_kafka >> trigger_etl
