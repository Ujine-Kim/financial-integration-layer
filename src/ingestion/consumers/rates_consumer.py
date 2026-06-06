"""
Kafka Consumer: читает курсы валют из cbr.rates.raw и пишет в raw.rates.
"""

import json
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException

from src.common.config import settings

logger = structlog.get_logger(__name__)

TOPIC = "cbr.rates.raw"
GROUP_ID = "rates-raw-consumer"


def consume_rates(max_empty_polls: int = 10) -> int:
    """Вычитывает курсы валют из Kafka и пишет в raw.rates."""
    log = logger.bind(topic=TOPIC, group=GROUP_ID)

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
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
    BATCH_SIZE = 50

    try:
        consumer.subscribe([TOPIC])
        log.info("consumer_started")

        while empty_polls < max_empty_polls:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                empty_polls += 1
                if batch:
                    total_written += _write_batch(conn, batch, log)
                    consumer.commit()
                    batch = []
                continue

            empty_polls = 0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                data = json.loads(msg.value().decode("utf-8"))
                batch.append(data)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                log.warning("parse_failed", error=str(e))
                continue

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
    rows = []
    for item in batch:
        try:
            rows.append((
                item["base_currency"],
                item["target_currency"],
                item["rate"],
                item["rate_date"],
                item["source"],
                item.get("loaded_at", datetime.now(timezone.utc).isoformat()),
            ))
        except KeyError as e:
            log.warning("missing_field", error=str(e))
            continue

    if not rows:
        return 0

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO raw.rates
                (base_currency, target_currency, rate, rate_date, source, loaded_at)
            VALUES %s
            ON CONFLICT (base_currency, target_currency, rate_date, source) DO NOTHING
            """,
            rows,
        )
        written = cur.rowcount
    conn.commit()

    log.info("batch_written", rows=written)
    return written


def main() -> None:
    """python -m src.ingestion.consumers.rates_consumer"""
    written = consume_rates()
    print(f"Done. Written {written} rows to raw.rates")


if __name__ == "__main__":
    main()
