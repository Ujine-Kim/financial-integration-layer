"""
Продюсер валютных курсов из ЦБ РФ.

ЦБ РФ публикует курсы валют в XML без авторизации:
https://www.cbr.ru/scripts/XML_daily.asp

Особенность: ЦБ РФ даёт курс X единиц иностранной валюты к рублю.
Например: USD nominal=1, value=90.5 означает 1 USD = 90.5 RUB.
Но EUR nominal=1, value=98.2 и CNY nominal=10, value=12.5 означает 10 CNY = 12.5 RUB.
Мы нормализуем: всегда храним курс за 1 единицу валюты.
"""

import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

import requests
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.common.config import settings
from src.ingestion.producers.base_producer import BaseProducer
from src.ingestion.schemas.rate import RawRate

logger = structlog.get_logger(__name__)

TOPIC = "cbr.rates.raw"
CBR_URL = "https://www.cbr.ru/scripts/XML_daily.asp"


class CBRProducer(BaseProducer):
    """
    Загружает курсы валют ЦБ РФ и публикует в Kafka.

    Источник публичный, авторизация не нужна.
    Данные за сегодня — без параметров. За конкретную дату — ?date_req=DD/MM/YYYY.
    """

    def __init__(self, rate_date: date | None = None) -> None:
        """
        Args:
            rate_date: дата курсов. None = сегодня.
        """
        super().__init__(
            topic=TOPIC,
            bootstrap_servers=settings.kafka_bootstrap_servers,
        )
        self.rate_date = rate_date or date.today()
        self._log = logger.bind(producer="CBRProducer", rate_date=str(self.rate_date))

    # ── Главный метод ──────────────────────────────────────────────────────

    def fetch_and_produce(self) -> None:
        """Загружает курсы ЦБ РФ и отправляет каждый курс в Kafka."""
        self._log.info("fetch_started")

        xml_content = self._fetch_xml()
        rates = self._parse_xml(xml_content)

        for rate in rates:
            self.produce(
                key=f"{rate.target_currency}:{rate.rate_date}",
                value=rate.model_dump_json(),
            )

        self._log.info("fetch_complete", rates_count=len(rates))

    # ── Получение данных ───────────────────────────────────────────────────

    @retry(
        retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _fetch_xml(self) -> bytes:
        """
        Скачивает XML с сайта ЦБ РФ.

        Returns:
            Сырой XML в байтах (ЦБ РФ отдаёт в windows-1251).
        """
        params = {}
        if self.rate_date != date.today():
            # Формат даты для ЦБ РФ: DD/MM/YYYY
            params["date_req"] = self.rate_date.strftime("%d/%m/%Y")

        response = requests.get(CBR_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.content  # bytes, не str — парсер ET сам определит кодировку

    def _parse_xml(self, xml_content: bytes) -> list[RawRate]:
        """
        Парсит XML ответ ЦБ РФ в список RawRate объектов.

        Структура XML:
            <ValCurs Date="06.06.2025" name="Foreign Currency Market">
                <Valute ID="R01235">
                    <NumCode>840</NumCode>
                    <CharCode>USD</CharCode>
                    <Nominal>1</Nominal>
                    <Name>Доллар США</Name>
                    <Value>90,5000</Value>   ← запятая как разделитель!
                </Valute>
                ...
            </ValCurs>

        Args:
            xml_content: XML в байтах.

        Returns:
            Список RawRate, по одному на каждую валюту.
        """
        root = ET.fromstring(xml_content)

        # Дата из атрибута корневого тега: "06.06.2025" → date(2025, 6, 6)
        date_str = root.attrib.get("Date", "")
        try:
            actual_date = datetime.strptime(date_str, "%d.%m.%Y").date()
        except ValueError:
            self._log.warning("bad_date_in_xml", raw_date=date_str)
            actual_date = self.rate_date

        rates = []
        for valute in root.findall("Valute"):
            try:
                char_code = valute.findtext("CharCode", "").strip()
                nominal_str = valute.findtext("Nominal", "1").strip()
                value_str = valute.findtext("Value", "").strip()

                if not char_code or not value_str:
                    continue

                # ЦБ РФ использует запятую как десятичный разделитель
                value_clean = value_str.replace(",", ".")
                nominal = int(nominal_str)

                # Нормализуем: курс за 1 единицу валюты
                # Например CNY: nominal=10, value=12.5 → rate = 12.5 / 10 = 1.25
                rate_value = Decimal(value_clean) / Decimal(nominal)

                rates.append(RawRate(
                    base_currency="RUB",
                    target_currency=char_code,
                    rate=rate_value,
                    rate_date=actual_date,
                    source="cbr",
                ))
            except (InvalidOperation, ValueError, TypeError) as e:
                self._log.warning(
                    "valute_parse_failed",
                    char_code=valute.findtext("CharCode", "unknown"),
                    error=str(e),
                )
                continue

        return rates


# ── Точка входа ────────────────────────────────────────────────────────────

def main() -> None:
    """Ручной запуск: python -m src.ingestion.producers.cbr_producer"""
    with CBRProducer() as producer:
        producer.fetch_and_produce()


if __name__ == "__main__":
    main()
