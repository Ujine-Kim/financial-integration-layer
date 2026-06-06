"""
Абстрактный базовый класс для всех Kafka продюсеров.

Все продюсеры в проекте наследуются от BaseProducer и реализуют
только метод fetch_and_produce() — логику получения данных из API.
Всё остальное (подключение к Kafka, retry, логирование) — здесь.
"""

import logging
from abc import ABC, abstractmethod

import structlog
from confluent_kafka import KafkaException, Producer

logger = structlog.get_logger(__name__)


class BaseProducer(ABC):
    """
    Базовый Kafka продюсер.

    Инкапсулирует:
    - создание и настройку confluent_kafka.Producer
    - отправку сообщений с callback
    - flush буфера
    - context manager протокол
    """

    def __init__(self, topic: str, bootstrap_servers: str) -> None:
        """
        Args:
            topic: Kafka топик для отправки сообщений.
            bootstrap_servers: адрес брокера, например "localhost:9092".
        """
        self.topic = topic
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "compression.type": "gzip",   # сжимаем батч перед отправкой
            "acks": "all",                 # ждём подтверждения от всех реплик
            "retries": 3,                  # авто-ретрай на уровне producer
            "retry.backoff.ms": 500,
        })
        self._log = logger.bind(topic=topic, producer=self.__class__.__name__)

    # ── Отправка ───────────────────────────────────────────────────────────

    def produce(self, key: str, value: str) -> None:
        """
        Отправляет одно сообщение в Kafka.

        Сообщение попадает в буфер продюсера — фактическая отправка
        происходит в фоне или при вызове flush().

        Args:
            key: ключ сообщения, определяет партицию (например "AAPL:2024-01-15").
            value: тело сообщения в виде строки (обычно JSON).
        """
        try:
            self._producer.produce(
                topic=self.topic,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                on_delivery=self._on_delivery,
            )
            # poll(0) даёт Kafka клиенту обработать события (доставки, ошибки)
            # без блокировки — важно вызывать периодически при большом объёме
            self._producer.poll(0)
        except KafkaException as e:
            self._log.error("produce_failed", key=key, error=str(e))
            raise

    def flush(self) -> None:
        """
        Сбрасывает буфер — ждёт пока все сообщения будут доставлены.

        Вызывается явно после окончания работы.
        Timeout 30 секунд — если за это время не доставилось, логируем.
        """
        remaining = self._producer.flush(timeout=30)
        if remaining > 0:
            self._log.warning("flush_incomplete", remaining_messages=remaining)
        else:
            self._log.info("flush_complete")

    # ── Callback ───────────────────────────────────────────────────────────

    def _on_delivery(self, err: Exception | None, msg: object) -> None:
        """
        Callback вызывается Kafka клиентом после каждой доставки.

        Не бросает исключение — только логирует. Вызывается в фоновом потоке
        confluent_kafka, поэтому side effects должны быть thread-safe.

        Args:
            err: None если доставлено успешно, иначе объект ошибки.
            msg: объект сообщения с метаданными (партиция, offset и т.д.).
        """
        if err:
            self._log.error(
                "delivery_failed",
                key=msg.key().decode("utf-8") if msg.key() else None,
                error=str(err),
            )
        else:
            self._log.debug(
                "delivery_success",
                key=msg.key().decode("utf-8") if msg.key() else None,
                partition=msg.partition(),
                offset=msg.offset(),
            )

    # ── Абстрактный метод ──────────────────────────────────────────────────

    @abstractmethod
    def fetch_and_produce(self) -> None:
        """
        Основная логика продюсера: получить данные из источника и отправить в Kafka.

        Реализуется в каждом наследнике отдельно.
        """

    # ── Context manager ────────────────────────────────────────────────────

    def __enter__(self) -> "BaseProducer":
        return self

    def __exit__(self, exc_type: type, exc_val: Exception, exc_tb: object) -> None:
        """Гарантируем flush при выходе из блока with — даже если было исключение."""
        self.flush()
