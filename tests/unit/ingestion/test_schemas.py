"""
Unit тесты для Pydantic схем входящих данных.
"""

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from src.ingestion.schemas.instrument import RawInstrument
from src.ingestion.schemas.quote import RawQuote
from src.ingestion.schemas.rate import RawRate


# ── RawQuote ───────────────────────────────────────────────────────────────

class TestRawQuote:

    def test_valid_quote(self):
        """Корректные данные проходят валидацию."""
        quote = RawQuote(
            symbol="AAPL",
            open=Decimal("195.23"),
            high=Decimal("196.50"),
            low=Decimal("194.10"),
            close=Decimal("195.89"),
            volume=52341000,
            trade_date=date(2025, 6, 6),
        )
        assert quote.symbol == "AAPL"
        assert quote.source == "alpha_vantage"  # default значение
        assert isinstance(quote.loaded_at, datetime)

    def test_decimal_precision_preserved(self):
        """Decimal не теряет точность при создании и сериализации."""
        quote = RawQuote(
            symbol="TEST",
            open=Decimal("150.123456789"),
            high=Decimal("150.123456789"),
            low=Decimal("150.123456789"),
            close=Decimal("150.123456789"),
            volume=1000,
            trade_date=date(2025, 1, 1),
        )
        # Значение не изменилось
        assert quote.open == Decimal("150.123456789")

        # В JSON сериализуется как строка, а не float — точность сохранена
        json_str = quote.model_dump_json()
        assert '"150.123456789"' in json_str  # строка, не число

    def test_decimal_serialized_as_string(self):
        """Decimal поля в JSON — строки, а не числа с плавающей точкой."""
        quote = RawQuote(
            symbol="AAPL",
            open=Decimal("195.23"),
            high=Decimal("196.50"),
            low=Decimal("194.10"),
            close=Decimal("195.89"),
            volume=100,
            trade_date=date(2025, 6, 6),
        )
        data = quote.model_dump()
        # model_dump() возвращает Decimal объекты — не float
        assert isinstance(data["open"], Decimal)

        json_str = quote.model_dump_json()
        # В JSON — строки
        assert '"195.23"' in json_str
        assert '"196.50"' in json_str

    def test_invalid_missing_required_field(self):
        """Отсутствие обязательного поля вызывает ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            RawQuote(
                # symbol отсутствует
                open=Decimal("195.23"),
                high=Decimal("196.50"),
                low=Decimal("194.10"),
                close=Decimal("195.89"),
                volume=100,
                trade_date=date(2025, 6, 6),
            )
        # Проверяем что ошибка именно про symbol
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("symbol",) for e in errors)

    def test_invalid_wrong_type(self):
        """Строка вместо числа в Decimal поле — ValidationError."""
        with pytest.raises(ValidationError):
            RawQuote(
                symbol="AAPL",
                open="not_a_number",
                high=Decimal("196.50"),
                low=Decimal("194.10"),
                close=Decimal("195.89"),
                volume=100,
                trade_date=date(2025, 6, 6),
            )

    def test_loaded_at_default_is_utc(self):
        """loaded_at по умолчанию — текущее время в UTC."""
        before = datetime.now(timezone.utc)
        quote = RawQuote(
            symbol="AAPL",
            open=Decimal("195.23"),
            high=Decimal("196.50"),
            low=Decimal("194.10"),
            close=Decimal("195.89"),
            volume=100,
            trade_date=date(2025, 6, 6),
        )
        after = datetime.now(timezone.utc)
        assert before <= quote.loaded_at <= after


# ── RawRate ────────────────────────────────────────────────────────────────

class TestRawRate:

    def test_valid_rate(self):
        """Корректный курс валюты проходит валидацию."""
        rate = RawRate(
            base_currency="RUB",
            target_currency="USD",
            rate=Decimal("90.5000"),
            rate_date=date(2025, 6, 6),
            source="cbr",
        )
        assert rate.base_currency == "RUB"
        assert rate.target_currency == "USD"
        assert rate.rate == Decimal("90.5000")

    def test_decimal_precision(self):
        """Курс с высокой точностью не теряет знаки."""
        rate = RawRate(
            base_currency="RUB",
            target_currency="JPY",
            rate=Decimal("0.60123456"),
            rate_date=date(2025, 6, 6),
            source="cbr",
        )
        assert rate.rate == Decimal("0.60123456")
        assert '"0.60123456"' in rate.model_dump_json()

    def test_invalid_missing_source(self):
        """source обязателен — нет default значения."""
        with pytest.raises(ValidationError):
            RawRate(
                base_currency="RUB",
                target_currency="USD",
                rate=Decimal("90.5"),
                rate_date=date(2025, 6, 6),
                # source отсутствует
            )


# ── RawInstrument ──────────────────────────────────────────────────────────

class TestRawInstrument:

    def test_valid_instrument(self):
        """Корректный инструмент проходит валидацию."""
        instrument = RawInstrument(
            symbol="AAPL",
            name="Apple Inc.",
            exchange="NASDAQ",
            asset_type="Stock",
            source="alpha_vantage",
        )
        assert instrument.symbol == "AAPL"
        assert instrument.asset_type == "Stock"

    def test_serialization_to_json(self):
        """Инструмент корректно сериализуется в JSON."""
        instrument = RawInstrument(
            symbol="SPY",
            name="SPDR S&P 500 ETF Trust",
            exchange="NYSE",
            asset_type="ETF",
            source="alpha_vantage",
        )
        json_str = instrument.model_dump_json()
        assert '"SPY"' in json_str
        assert '"ETF"' in json_str
        assert "loaded_at" in json_str

    def test_invalid_missing_fields(self):
        """Несколько обязательных полей отсутствуют — ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            RawInstrument(symbol="AAPL")  # нет name, exchange, asset_type, source

        errors = exc_info.value.errors()
        missing_fields = {e["loc"][0] for e in errors}
        assert "name" in missing_fields
        assert "exchange" in missing_fields
