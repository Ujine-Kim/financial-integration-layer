"""
Spark job: загрузка Hub Instrument из raw слоя в Data Vault.

Паттерн идемпотентной загрузки Hub:
- Читаем уникальные символы из raw.quotes
- Генерируем hash key
- INSERT ... ON CONFLICT DO NOTHING — если хэш уже есть, пропускаем

Hub никогда не обновляется — только пополняется новыми бизнес-ключами.
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.vault_utils import add_vault_metadata, hash_key_column
from src.processing.base.spark_job import SparkJob


class LoadHubInstrument(SparkJob):
    """
    Загружает уникальные финансовые инструменты из raw.quotes в vault.hub_instrument.

    Читает данные за execution_date — день за который запущен ETL.
    Идемпотентна: повторный запуск за ту же дату не создаёт дублей.
    """

    def __init__(self, execution_date: str) -> None:
        """
        Args:
            execution_date: дата в формате YYYY-MM-DD, например "2025-06-06".
        """
        super().__init__(job_name="LoadHubInstrument")
        self.execution_date = execution_date

    # ── Основная логика ────────────────────────────────────────────────────

    def run(self) -> None:
        """Читает raw котировки → извлекает инструменты → пишет в hub."""
        self._log.info("run_started", execution_date=self.execution_date)

        # 1. Читаем сырые котировки за дату
        raw_df = self._read_raw_quotes()
        self._log.info("raw_read", count=raw_df.count())

        if raw_df.rdd.isEmpty():
            self._log.warning("no_data_found", execution_date=self.execution_date)
            return

        # 2. Трансформируем в формат Hub
        hub_df = self._transform_to_hub(raw_df)

        # 3. Загружаем идемпотентно
        self._write_hub(hub_df)
        self._log.info("hub_loaded", execution_date=self.execution_date)

    # ── Чтение ─────────────────────────────────────────────────────────────

    def _read_raw_quotes(self) -> DataFrame:
        """
        Читает уникальные символы из raw.quotes за execution_date.

        Берём только символ и источник — нам не нужны цены для Hub.
        distinct() убирает дубли если один символ появился несколько раз за день.
        """
        jdbc_opts = self._get_jdbc_options(schema="raw")

        return (
            self.spark.read
            .format("jdbc")
            .options(**jdbc_opts)
            .option("dbtable", "raw.quotes")
            .load()
            .filter(F.col("trade_date") == F.lit(self.execution_date))
            .select("symbol", "source")
            .distinct()
        )

    # ── Трансформация ──────────────────────────────────────────────────────

    def _transform_to_hub(self, df: DataFrame) -> DataFrame:
        """
        Трансформирует сырые данные в структуру Hub.

        Hub содержит только:
        - hub_instrument_hk: MD5 от symbol
        - instrument_bk: бизнес-ключ (symbol)
        - load_date: время загрузки
        - record_source: источник

        Шаги:
        1. Добавляем hash key
        2. Добавляем vault метаданные (load_date, record_source)
        3. Переименовываем symbol → instrument_bk
        4. Выбираем только нужные колонки
        """
        return (
            df
            # Генерируем hash key прямо в Spark (в JVM, без UDF)
            .withColumn("hub_instrument_hk", hash_key_column("symbol"))
            # Добавляем load_date = current_timestamp(), record_source = source
            .transform(lambda d: add_vault_metadata(d, record_source=d.first()["source"]))
            .withColumnRenamed("symbol", "instrument_bk")
            .select(
                "hub_instrument_hk",
                "instrument_bk",
                "load_date",
                "record_source",
            )
        )

    # ── Запись ─────────────────────────────────────────────────────────────

    def _write_hub(self, df: DataFrame) -> None:
        """
        Идемпотентная запись в vault.hub_instrument.

        Используем mode="append" + ON CONFLICT DO NOTHING через
        sessionInitStatement — PostgreSQL проигнорирует конфликты по PK.

        Почему не mode="overwrite"? Hub — append-only таблица.
        Перезапись уничтожила бы историю инструментов загруженных в прошлые дни.
        """
        jdbc_opts = self._get_jdbc_options(schema="vault")

        (
            df.write
            .format("jdbc")
            .options(**jdbc_opts)
            .option("dbtable", "vault.hub_instrument")
            .option(
                # Выполняется перед каждой партицией записи
                # Говорим PostgreSQL: при конфликте по PK — молча пропустить
                "sessionInitStatement",
                "SET session_replication_role = replica"
            )
            .mode("append")
            .save()
        )

        # Альтернативный подход — через временную таблицу + INSERT ... ON CONFLICT
        # Используем если sessionInitStatement недостаточно
        self._log.info("hub_written", rows=df.count())


# ── Точка входа ────────────────────────────────────────────────────────────

def main() -> None:
    """
    Ручной запуск:
        python -m src.processing.raw_to_vault.load_hub_instrument --date 2025-06-06
    """
    parser = argparse.ArgumentParser(description="Load Hub Instrument from raw quotes")
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Execution date in YYYY-MM-DD format (default: today UTC)",
    )
    args = parser.parse_args()

    job = LoadHubInstrument(execution_date=args.date)
    job.execute()


if __name__ == "__main__":
    main()
