"""
Схема для финансовых инструментов (акции, ETF и т.д.).
"""

from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_serializer


class RawInstrument(BaseModel):
    """
    Справочник финансовых инструментов.

    Загружается отдельно от котировок — это медленно меняющиеся данные
    (название компании, биржа, тип актива).
    """

    symbol: str
    name: str
    exchange: str
    asset_type: str  # "Stock", "ETF", "Forex", и т.д.
    source: str
    loaded_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_serializer("loaded_at", when_used="json")
    def serialize_datetime(self, value: datetime) -> str:
        return value.isoformat()
