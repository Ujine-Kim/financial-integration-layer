"""
Схема для биржевых котировок (Alpha Vantage).
"""

from datetime import date, datetime, timezone
from decimal import Decimal

from pydantic import BaseModel, Field, field_serializer


class RawQuote(BaseModel):
    """
    Одна строка дневной котировки инструмента.

    Decimal используется для цен — float теряет точность
    при арифметике (0.1 + 0.2 != 0.3).
    """

    symbol: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    trade_date: date
    source: str = "alpha_vantage"
    loaded_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_serializer("open", "high", "low", "close", when_used="json")
    def serialize_decimal(self, value: Decimal) -> str:
        """Сериализуем Decimal как строку только в JSON — без потери точности.

        when_used="json" означает: применяй только при model_dump_json(),
        но не при model_dump() — там возвращаем нативный Decimal.
        """
        return str(value)

    @field_serializer("trade_date", when_used="json")
    def serialize_date(self, value: date) -> str:
        return value.isoformat()

    @field_serializer("loaded_at", when_used="json")
    def serialize_datetime(self, value: datetime) -> str:
        return value.isoformat()
