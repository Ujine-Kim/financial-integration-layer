"""
Схема для валютных курсов (ExchangeRate API и ЦБ РФ).
"""

from datetime import date, datetime, timezone
from decimal import Decimal

from pydantic import BaseModel, Field, field_serializer


class RawRate(BaseModel):
    """
    Курс одной валютной пары за один день.

    Пример ЦБ РФ: base_currency=RUB, target_currency=USD, rate=0.011
    Пример ExchangeRate: base_currency=USD, target_currency=RUB, rate=90.5
    """

    base_currency: str
    target_currency: str
    rate: Decimal
    rate_date: date
    source: str  # "exchange_rate_api" или "cbr"
    loaded_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_serializer("rate", when_used="json")
    def serialize_decimal(self, value: Decimal) -> str:
        return str(value)

    @field_serializer("rate_date", when_used="json")
    def serialize_date(self, value: date) -> str:
        return value.isoformat()

    @field_serializer("loaded_at", when_used="json")
    def serialize_datetime(self, value: datetime) -> str:
        return value.isoformat()
