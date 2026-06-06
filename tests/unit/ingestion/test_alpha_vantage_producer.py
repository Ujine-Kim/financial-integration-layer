"""
Unit тесты для AlphaVantageProducer.

Мокаем requests.get и confluent_kafka.Producer.
"""

import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.producers.alpha_vantage_producer import AlphaVantageProducer
from src.ingestion.schemas.quote import RawQuote

FIXTURE_PATH = Path(__file__).parent.parent.parent / "fixtures" / "alpha_vantage_response.json"


@pytest.fixture
def av_response() -> dict:
    """Загружает тестовый JSON из файла фикстуры."""
    return json.loads(FIXTURE_PATH.read_text())


@pytest.fixture
def mock_producer():
    """Мокаем Kafka Producer."""
    with patch("src.ingestion.producers.base_producer.Producer") as mock:
        yield mock


@pytest.fixture
def mock_requests(av_response):
    """
    Мокаем requests.get — возвращает тестовый JSON.
    MagicMock().json() вернёт av_response.
    """
    with patch("src.ingestion.producers.alpha_vantage_producer.requests.get") as mock:
        mock_response = MagicMock()
        mock_response.json.return_value = av_response
        mock_response.raise_for_status.return_value = None  # не бросает исключение
        mock.return_value = mock_response
        yield mock


# ── Тесты парсинга ─────────────────────────────────────────────────────────

class TestAlphaVantageProducerParsing:

    def test_returns_raw_quote(self, mock_producer, mock_requests):
        """Успешный ответ API → объект RawQuote."""
        producer = AlphaVantageProducer(symbols=["AAPL"])
        quote = producer._fetch_latest_quote("AAPL")

        assert isinstance(quote, RawQuote)

    def test_latest_date_selected(self, mock_producer, mock_requests):
        """Берётся последняя дата из ответа (2025-06-06, не 2025-06-05)."""
        producer = AlphaVantageProducer(symbols=["AAPL"])
        quote = producer._fetch_latest_quote("AAPL")

        assert quote.trade_date == date(2025, 6, 6)

    def test_prices_parsed_correctly(self, mock_producer, mock_requests):
        """Цены парсятся в Decimal без потери точности."""
        producer = AlphaVantageProducer(symbols=["AAPL"])
        quote = producer._fetch_latest_quote("AAPL")

        assert quote.open == Decimal("195.2300")
        assert quote.high == Decimal("196.5000")
        assert quote.low == Decimal("194.1000")
        assert quote.close == Decimal("195.8900")
        assert quote.volume == 52341000

    def test_symbol_set_correctly(self, mock_producer, mock_requests):
        """Символ из аргумента, не из тела ответа."""
        producer = AlphaVantageProducer(symbols=["AAPL"])
        quote = producer._fetch_latest_quote("AAPL")

        assert quote.symbol == "AAPL"
        assert quote.source == "alpha_vantage"

    def test_api_error_message_returns_none(self, mock_producer):
        """Alpha Vantage возвращает Error Message → None, не исключение."""
        with patch("src.ingestion.producers.alpha_vantage_producer.requests.get") as mock:
            mock_response = MagicMock()
            mock_response.json.return_value = {"Error Message": "Invalid API call."}
            mock_response.raise_for_status.return_value = None
            mock.return_value = mock_response

            producer = AlphaVantageProducer(symbols=["INVALID"])
            result = producer._fetch_latest_quote("INVALID")

            assert result is None

    def test_rate_limit_note_returns_none(self, mock_producer):
        """Alpha Vantage возвращает Note (rate limit) → None."""
        with patch("src.ingestion.producers.alpha_vantage_producer.requests.get") as mock:
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "Note": "Thank you for using Alpha Vantage! Our standard API rate limit..."
            }
            mock_response.raise_for_status.return_value = None
            mock.return_value = mock_response

            producer = AlphaVantageProducer(symbols=["AAPL"])
            result = producer._fetch_latest_quote("AAPL")

            assert result is None


# ── Тесты fetch_and_produce ────────────────────────────────────────────────

class TestAlphaVantageProducerFetchAndProduce:

    def test_message_key_format(self, mock_producer, mock_requests):
        """Ключ сообщения: '{SYMBOL}:{YYYY-MM-DD}'."""
        produced_keys = []

        producer = AlphaVantageProducer(symbols=["AAPL"])
        producer.produce = lambda key, value: produced_keys.append(key)
        producer.fetch_and_produce()

        assert produced_keys == ["AAPL:2025-06-06"]

    def test_failed_symbol_does_not_stop_others(self, mock_producer):
        """Ошибка для одного символа не останавливает остальные."""
        produced_keys = []

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            mock_response = MagicMock()
            if call_count == 1:
                # Первый символ (AAPL) падает
                mock_response.json.return_value = {"Error Message": "Invalid"}
            else:
                # Второй символ (MSFT) успешен
                mock_response.json.return_value = {
                    "Time Series (Daily)": {
                        "2025-06-06": {
                            "1. open": "420.00",
                            "2. high": "425.00",
                            "3. low": "419.00",
                            "4. close": "423.50",
                            "5. volume": "30000000",
                        }
                    }
                }
            mock_response.raise_for_status.return_value = None
            return mock_response

        with patch("src.ingestion.producers.alpha_vantage_producer.requests.get", side_effect=side_effect):
            producer = AlphaVantageProducer(symbols=["AAPL", "MSFT"])
            producer.produce = lambda key, value: produced_keys.append(key)
            producer.fetch_and_produce()

        # AAPL упал, MSFT загрузился
        assert produced_keys == ["MSFT:2025-06-06"]
