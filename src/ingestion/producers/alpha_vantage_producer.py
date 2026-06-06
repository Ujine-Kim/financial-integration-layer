"""
Продюсер биржевых котировок из Alpha Vantage API.

Документация API: https://www.alphavantage.co/documentation/
Endpoint: TIME_SERIES_DAILY
Бесплатный план: 25 запросов в день, 1 символ за раз.
"""

import logging
from datetime import date
from decimal import Decimal, InvalidOperation

import time

import requests
import structlog
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.common.config import settings
from src.ingestion.producers.base_producer import BaseProducer
from src.ingestion.schemas.quote import RawQuote

logger = structlog.get_logger(__name__)

# Топик в Kafka куда пишем котировки
TOPIC = "quotes.raw"

# URL Alpha Vantage API
API_URL = "https://www.alphavantage.co/query"


class AlphaVantageProducer(BaseProducer):
    """
    Загружает дневные котировки из Alpha Vantage и публикует в Kafka.

    Для каждого символа делает отдельный HTTP запрос,
    берёт только последнюю доступную дату и отправляет в топик quotes.raw.
    """

    def __init__(self, symbols: list[str]) -> None:
        """
        Args:
            symbols: список тикеров, например ["AAPL", "MSFT", "GOOGL"].
        """
        super().__init__(
            topic=TOPIC,
            bootstrap_servers=settings.kafka_bootstrap_servers,
        )
        self.symbols = symbols
        self.api_key = settings.alpha_vantage_api_key
        self._log = logger.bind(producer="AlphaVantageProducer")

    # ── Главный метод ──────────────────────────────────────────────────────

    def fetch_and_produce(self) -> None:
        """Загружает котировки для всех символов и отправляет в Kafka."""
        self._log.info("fetch_started", symbols=self.symbols)
        success_count = 0

        for i, symbol in enumerate(self.symbols):
            # Бесплатный план: не более 25 запросов в день и ~5 запросов в минуту
            # Ждём 15 секунд между запросами чтобы не получить rate limit
            if i > 0:
                self._log.info("rate_limit_pause", seconds=15, symbol=symbol)
                time.sleep(15)

            try:
                quote = self._fetch_latest_quote(symbol)
                if quote:
                    self.produce(
                        key=f"{quote.symbol}:{quote.trade_date}",
                        value=quote.model_dump_json(),
                    )
                    success_count += 1
                    self._log.info("quote_produced", symbol=symbol, date=str(quote.trade_date))
            except Exception as e:
                # Логируем и продолжаем — один упавший символ не должен
                # останавливать загрузку остальных
                self._log.error("symbol_failed", symbol=symbol, error=str(e))

        self._log.info("fetch_complete", total=len(self.symbols), success=success_count)

    # ── Получение данных ───────────────────────────────────────────────────

    @retry(
        # Повторяем только на сетевые ошибки и 5xx — не на 4xx (неверный ключ и т.д.)
        retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),  # 2s → 4s → 8s
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.WARNING),
        reraise=True,
    )
    def _fetch_raw_response(self, symbol: str) -> dict:
        """
        Делает HTTP запрос к Alpha Vantage с retry логикой.

        Retry только на ConnectionError и Timeout — не на бизнес-ошибки.
        Exponential backoff: ждёт 2s, потом 4s, потом 8s между попытками.
        """
        response = requests.get(
            url=API_URL,
            params={
                "function": "TIME_SERIES_DAILY",
                "symbol": symbol,
                "apikey": self.api_key,
                "outputsize": "compact",  # последние 100 дней, не весь архив
            },
            timeout=10,
        )
        response.raise_for_status()  # бросает HTTPError на 4xx/5xx
        return response.json()

    def _fetch_latest_quote(self, symbol: str) -> RawQuote | None:
        """
        Парсит ответ API и возвращает котировку за последнюю доступную дату.

        Returns:
            RawQuote или None если данные недоступны / невалидны.
        """
        data = self._fetch_raw_response(symbol)

        # Alpha Vantage возвращает ошибки в теле с кодом 200 — проверяем явно
        if "Error Message" in data:
            self._log.warning("api_error", symbol=symbol, message=data["Error Message"])
            return None

        if "Note" in data:
            # "Note" появляется при превышении лимита запросов
            self._log.warning("api_rate_limit", symbol=symbol, message=data["Note"])
            return None

        if "Information" in data:
            # "Information" — тоже rate limit, другой формат сообщения
            self._log.warning("api_rate_limit", symbol=symbol, message=data["Information"])
            return None

        time_series = data.get("Time Series (Daily)", {})
        if not time_series:
            self._log.warning("empty_response", symbol=symbol)
            return None

        # Берём только последнюю дату (ключи отсортированы по убыванию)
        latest_date_str = max(time_series.keys())
        day_data = time_series[latest_date_str]

        try:
            return RawQuote(
                symbol=symbol,
                open=Decimal(day_data["1. open"]),
                high=Decimal(day_data["2. high"]),
                low=Decimal(day_data["3. low"]),
                close=Decimal(day_data["4. close"]),
                volume=int(day_data["5. volume"]),
                trade_date=date.fromisoformat(latest_date_str),
                source="alpha_vantage",
            )
        except (KeyError, InvalidOperation, ValueError) as e:
            self._log.error("parse_failed", symbol=symbol, error=str(e))
            return None


# ── Точка входа ────────────────────────────────────────────────────────────

def main() -> None:
    """Ручной запуск: python -m src.ingestion.producers.alpha_vantage_producer"""
    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

    with AlphaVantageProducer(symbols=symbols) as producer:
        producer.fetch_and_produce()


if __name__ == "__main__":
    main()
