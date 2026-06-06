"""
Unit тесты для CBRProducer.

Мокаем requests.get — не делаем реальных HTTP запросов.
Мокаем confluent_kafka.Producer — не подключаемся к реальной Kafka.
"""

from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.producers.cbr_producer import CBRProducer
from src.ingestion.schemas.rate import RawRate

# Путь к фикстуре
FIXTURE_PATH = Path(__file__).parent.parent.parent / "fixtures" / "cbr_response.xml"


@pytest.fixture
def cbr_xml() -> bytes:
    """Загружает тестовый XML из файла фикстуры."""
    return FIXTURE_PATH.read_bytes()


@pytest.fixture
def mock_producer():
    """
    Мокаем confluent_kafka.Producer — чтобы тесты не требовали живой Kafka.
    patch заменяет класс Producer на MagicMock на время теста.
    """
    with patch("src.ingestion.producers.base_producer.Producer") as mock:
        yield mock


# ── Тесты парсинга XML ─────────────────────────────────────────────────────

class TestCBRProducerParsing:

    def test_correct_number_of_rates(self, cbr_xml, mock_producer):
        """Из XML с 3 валютами создаётся 3 объекта RawRate."""
        producer = CBRProducer(rate_date=date(2025, 6, 6))
        rates = producer._parse_xml(cbr_xml)
        assert len(rates) == 3

    def test_rate_types(self, cbr_xml, mock_producer):
        """Все результаты — объекты RawRate."""
        producer = CBRProducer(rate_date=date(2025, 6, 6))
        rates = producer._parse_xml(cbr_xml)
        assert all(isinstance(r, RawRate) for r in rates)

    def test_usd_rate_parsed_correctly(self, cbr_xml, mock_producer):
        """USD: nominal=1, value=90.5000 → rate=90.5000."""
        producer = CBRProducer(rate_date=date(2025, 6, 6))
        rates = producer._parse_xml(cbr_xml)

        usd = next(r for r in rates if r.target_currency == "USD")
        assert usd.base_currency == "RUB"
        assert usd.rate == Decimal("90.5000")
        assert usd.source == "cbr"

    def test_cny_nominal_normalization(self, cbr_xml, mock_producer):
        """CNY: nominal=10, value=12.5000 → rate=1.25 (нормализация на nominal)."""
        producer = CBRProducer(rate_date=date(2025, 6, 6))
        rates = producer._parse_xml(cbr_xml)

        cny = next(r for r in rates if r.target_currency == "CNY")
        assert cny.rate == Decimal("12.5000") / Decimal("10")

    def test_date_parsed_from_xml(self, cbr_xml, mock_producer):
        """Дата берётся из атрибута тега <ValCurs Date="06.06.2025">."""
        producer = CBRProducer(rate_date=date(2025, 6, 6))
        rates = producer._parse_xml(cbr_xml)

        assert all(r.rate_date == date(2025, 6, 6) for r in rates)


# ── Тесты ключа сообщения ──────────────────────────────────────────────────

class TestCBRProducerMessageKey:

    def test_message_key_format(self, cbr_xml, mock_producer):
        """Ключ сообщения: '{CURRENCY_CODE}:{YYYY-MM-DD}'."""
        produced_messages = []

        producer = CBRProducer(rate_date=date(2025, 6, 6))
        # Подменяем метод produce чтобы перехватить аргументы
        producer.produce = lambda key, value: produced_messages.append(key)

        with patch.object(producer, "_fetch_xml", return_value=cbr_xml):
            producer.fetch_and_produce()

        assert "USD:2025-06-06" in produced_messages
        assert "EUR:2025-06-06" in produced_messages
        assert "CNY:2025-06-06" in produced_messages

    def test_produces_for_all_currencies(self, cbr_xml, mock_producer):
        """Количество отправленных сообщений = количеству валют в XML."""
        produced_messages = []

        producer = CBRProducer(rate_date=date(2025, 6, 6))
        producer.produce = lambda key, value: produced_messages.append(key)

        with patch.object(producer, "_fetch_xml", return_value=cbr_xml):
            producer.fetch_and_produce()

        assert len(produced_messages) == 3
