"""
Kafka Consumer: читает котировки из quotes.raw и пишет в raw.quotes (PostgreSQL).

Запускается один раз (batch mode) — вычитывает все накопленные сообщения
и завершается. В проде Airflow запускает его перед Spark job.
"""

import json
import logging
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException

from src.common.config import settings

logger = structlog.get_logger(__name__)

TOPIC = "quotes.raw"
GROUP_ID = "quotes-raw-consumer"


def consume_quotes(max_empty_polls: int = 10) -> int:
    """
    Вычитывает все доступные сообщения из quotes.raw и пишет в raw.quotes.

    Останавливается когда получает max_empty_polls пустых ответов подряд —
    значит все текущие сообщения обработаны.

    Args:
        max_empty_polls: сколько пустых poll() подряд считать концом потока.

    Returns:
        Количество записанных строк.
    """
    log = logger.bind(topic=TOPIC, group=GROUP_ID)

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",      # читаем с самого начала
        "enable.auto.commit": False,           # коммитим вручную — после записи в БД
    })

    conn = psycopg2.connect(
        host=settings.dwh_host,
        port=settings.dwh_port,
        dbname=settings.dwh_name,
        user=settings.dwh_user,
        password=settings.dwh_password,
    )
    conn.autocommit = False

    total_written = 0
    empty_polls = 0
    batch = []
    BATCH_SIZE = 50  # пишем в БД батчами по 50 записей

    try:
        consumer.subscribe([TOPIC])
        log.info("consumer_started")

        while empty_polls < max_empty_polls:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Нет сообщений — увеличиваем счётчик пустых опросов
                empty_polls += 1
                if batch:
                    # Записываем остаток батча
                    total_written += _write_batch(conn, batch, log)
                    consumer.commit()
                    batch = []
                continue

            empty_polls = 0  # сбрасываем счётчик при получении сообщения

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # достигли конца партиции — не ошибка
                raise KafkaException(msg.error())

            # Парсим JSON сообщение
            try:
                data = json.loads(msg.value().decode("utf-8"))
                batch.append(data)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                log.warning("parse_failed", key=msg.key(), error=str(e))
                continue

            # Пишем батч когда накопили достаточно
            if len(batch) >= BATCH_SIZE:
                total_written += _write_batch(conn, batch, log)
                consumer.commit()
                batch = []

        log.info("consumer_finished", total_written=total_written)
        return total_written

    finally:
        consumer.close()
        conn.close()


def _write_batch(conn, batch: list[dict], log) -> int:
    """
    Пишет батч котировок в raw.quotes через INSERT ... ON CONFLICT DO NOTHING.

    ON CONFLICT DO NOTHING — идемпотентность: если котировка за эту дату
    уже есть в таблице (от предыдущего запуска) — просто пропускаем.
    """
    if not batch:
        return 0

    rows = []
    for item in batch:
        try:
            rows.append((
                item["symbol"],
                item["open"],
                item["high"],
                item["low"],
                item["close"],
                item["volume"],
                item["trade_date"],
                item["source"],
                item.get("loaded_at", datetime.now(timezone.utc).isoformat()),
            ))
        except KeyError as e:
            log.warning("missing_field", error=str(e), item=item)
            continue

    if not rows:
        return 0

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO raw.quotes
                (symbol, open_price, high_price, low_price, close_price,
                 volume, trade_date, source, loaded_at)
            VALUES %s
            ON CONFLICT (symbol, trade_date, source) DO NOTHING
            """,
            rows,
        )
        written = cur.rowcount
    conn.commit()

    log.info("batch_written", rows=written)
    return written


def main() -> None:
    """
    Ручной запуск:
        python -m src.ingestion.consumers.quotes_consumer
    """
    written = consume_quotes()
    print(f"Done. Written {written} rows to raw.quotes")


if __name__ == "__main__":
    main()
