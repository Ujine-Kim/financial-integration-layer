"""
Spark job: загрузка Satellite Quote Prices из raw слоя в Data Vault.

Паттерн delta-load через hash_diff:
1. Читаем новые котировки из raw за execution_date
2. Джойним с hub_instrument для получения hash key
3. Считаем hash_diff от ценовых атрибутов
4. Сравниваем с последними записями в satellite
5. Вставляем только те строки где hash_diff изменился

Это гарантирует что одни и те же данные не вставляются дважды,
даже если ETL запускается повторно за ту же дату.
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from src.common.vault_utils import add_vault_metadata, generate_hash_diff, hash_key_column
from src.processing.base.spark_job import SparkJob

# Атрибуты которые включаем в hash_diff для delta-load
HASH_DIFF_COLUMNS = ["open_price", "high_price", "low_price", "close_price", "volume"]


class LoadSatQuotePrices(SparkJob):
    """
    Загружает котировки из raw.quotes в vault.sat_quote_prices.

    Delta-load: вставляет строку только если данные изменились
    относительно последней загруженной записи (по hash_diff).
    """

    def __init__(self, execution_date: str) -> None:
        """
        Args:
            execution_date: дата в формате YYYY-MM-DD.
        """
        super().__init__(job_name="LoadSatQuotePrices")
        self.execution_date = execution_date

    # ── Основная логика ────────────────────────────────────────────────────

    def run(self) -> None:
        self._log.info("run_started", execution_date=self.execution_date)

        # 1. Читаем raw котировки за дату
        raw_df = self._read_raw_quotes()

        if raw_df.rdd.isEmpty():
            self._log.warning("no_raw_data", execution_date=self.execution_date)
            return

        # 2. Обогащаем: добавляем hub_instrument_hk из Hub
        enriched_df = self._enrich_with_hub(raw_df)

        # 3. Считаем hash_diff от атрибутов
        with_hash_df = self._apply_hash_diff(enriched_df)

        # 4. Читаем последние записи из Satellite для сравнения
        existing_df = self._read_existing_satellite()

        # 5. Оставляем только новые / изменившиеся строки
        delta_df = self._filter_delta(with_hash_df, existing_df)
        delta_count = delta_df.count()
        self._log.info("delta_computed", new_rows=delta_count)

        if delta_count == 0:
            self._log.info("no_changes_detected")
            return

        # 6. Добавляем vault метаданные и пишем
        final_df = add_vault_metadata(delta_df, record_source="alpha_vantage")
        self._write_satellite(final_df)

    # ── Чтение ─────────────────────────────────────────────────────────────

    def _read_raw_quotes(self) -> DataFrame:
        """Читает котировки из raw.quotes за execution_date."""
        jdbc_opts = self._get_jdbc_options(schema="raw")

        return (
            self.spark.read
            .format("jdbc")
            .options(**jdbc_opts)
            .option("dbtable", "raw.quotes")
            .load()
            .filter(F.col("trade_date") == F.lit(self.execution_date))
            .select(
                "symbol",
                F.col("open_price"),
                F.col("high_price"),
                F.col("low_price"),
                F.col("close_price"),
                F.col("volume"),
                F.col("trade_date"),
                F.col("source"),
            )
        )

    def _read_existing_satellite(self) -> DataFrame:
        """
        Читает последние записи из sat_quote_prices для delta-сравнения.

        Используем Window функцию чтобы взять только последнюю запись
        для каждого hub_instrument_hk + trade_date — нам не нужна вся история.
        """
        jdbc_opts = self._get_jdbc_options(schema="vault")

        sat_df = (
            self.spark.read
            .format("jdbc")
            .options(**jdbc_opts)
            .option("dbtable", "vault.sat_quote_prices")
            .load()
            .filter(F.col("trade_date") == F.lit(self.execution_date))
            .select("hub_instrument_hk", "trade_date", "hash_diff")
        )

        # Берём только самую последнюю запись для каждого инструмента+дата
        # (на случай если satellite уже содержит несколько версий)
        window = Window.partitionBy("hub_instrument_hk", "trade_date").orderBy(
            F.col("load_date").desc()
        )

        return (
            sat_df
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

    # ── Трансформации ──────────────────────────────────────────────────────

    def _enrich_with_hub(self, raw_df: DataFrame) -> DataFrame:
        """
        Джойним raw данные с hub_instrument для получения hash key.

        Без hub_instrument_hk мы не можем вставить запись в satellite —
        FK ссылается на hub.

        LEFT SEMI JOIN — берём только строки из raw где символ уже есть в Hub.
        Строки без Hub пропускаем (инструмент ещё не загружен).
        """
        jdbc_opts = self._get_jdbc_options(schema="vault")

        hub_df = (
            self.spark.read
            .format("jdbc")
            .options(**jdbc_opts)
            .option("dbtable", "vault.hub_instrument")
            .load()
            .select("hub_instrument_hk", "instrument_bk")
        )

        # Сначала добавляем hash key к raw данным
        raw_with_hk = raw_df.withColumn("hub_instrument_hk", hash_key_column("symbol"))

        # INNER JOIN — пропускаем символы которых нет в Hub
        # (в идеале Hub грузится до Satellite, поэтому пропусков быть не должно)
        return raw_with_hk.join(
            hub_df,
            on="hub_instrument_hk",
            how="inner",
        ).drop("instrument_bk")

    def _apply_hash_diff(self, df: DataFrame) -> DataFrame:
        """
        Приводим типы и считаем hash_diff от ценовых атрибутов.

        hash_diff = MD5(open_price || high_price || low_price || close_price || volume)
        Если хоть одно поле изменилось — hash_diff будет другим.
        """
        return generate_hash_diff(df, columns=HASH_DIFF_COLUMNS)

    def _filter_delta(self, new_df: DataFrame, existing_df: DataFrame) -> DataFrame:
        """
        Оставляет только строки где hash_diff отличается от последнего в Satellite.

        Логика:
        - LEFT JOIN new_df с existing_df по (hub_instrument_hk, trade_date)
        - Если записи нет в existing → новая запись → берём
        - Если запись есть и hash_diff совпадает → данные не изменились → пропускаем
        - Если запись есть и hash_diff отличается → данные изменились → берём

        Это классический anti-join паттерн для delta-load в Data Vault.
        """
        joined = new_df.alias("new").join(
            existing_df.alias("existing"),
            on=["hub_instrument_hk", "trade_date"],
            how="left",
        )

        return joined.filter(
            # Записи нет в satellite (новый инструмент или новая дата)
            F.col("existing.hash_diff").isNull()
            |
            # Запись есть, но данные изменились
            (F.col("new.hash_diff") != F.col("existing.hash_diff"))
        ).select("new.*")  # берём только колонки из new_df

    # ── Запись ─────────────────────────────────────────────────────────────

    def _write_satellite(self, df: DataFrame) -> None:
        """
        Пишет новые записи в vault.sat_quote_prices.

        mode("append") — satellite только пополняется, старые записи не трогаем.
        История изменений хранится как отдельные строки с разными load_date.
        """
        jdbc_opts = self._get_jdbc_options(schema="vault")

        final_df = df.select(
            "hub_instrument_hk",
            "load_date",
            "record_source",
            "hash_diff",
            F.col("open_price"),
            F.col("high_price"),
            F.col("low_price"),
            F.col("close_price"),
            F.col("volume"),
            F.col("trade_date"),
        )

        (
            final_df.write
            .format("jdbc")
            .options(**jdbc_opts)
            .option("dbtable", "vault.sat_quote_prices")
            .mode("append")
            .save()
        )

        self._log.info("satellite_written", rows=final_df.count())


# ── Точка входа ────────────────────────────────────────────────────────────

def main() -> None:
    """
    Ручной запуск:
        python -m src.processing.raw_to_vault.load_sat_quote_prices --date 2025-06-06
    """
    parser = argparse.ArgumentParser(description="Load Satellite Quote Prices")
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Execution date YYYY-MM-DD (default: today UTC)",
    )
    args = parser.parse_args()

    job = LoadSatQuotePrices(execution_date=args.date)
    job.execute()


if __name__ == "__main__":
    main()
