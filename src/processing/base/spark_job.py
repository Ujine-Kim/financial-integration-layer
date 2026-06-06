"""
Абстрактный базовый класс для всех Spark jobs в проекте.

Все Spark jobs наследуются от SparkJob и реализуют только метод run().
Всё остальное — создание сессии, подключение к MinIO и PostgreSQL,
логирование, гарантированная остановка — здесь.
"""

import os
from abc import ABC, abstractmethod
from typing import Any

import structlog
from pyspark.sql import SparkSession

from src.common.config import settings

logger = structlog.get_logger(__name__)


class SparkJob(ABC):
    """
    Базовый класс для Spark ETL jobs.

    Инкапсулирует:
    - создание SparkSession с настройками MinIO (S3A)
    - JDBC параметры для PostgreSQL
    - context manager — гарантирует spark.stop() при выходе
    - логирование с именем джоба в каждом сообщении
    """

    def __init__(self, job_name: str, app_name: str | None = None) -> None:
        """
        Args:
            job_name: имя джоба для логов (например "LoadHubInstrument").
            app_name: имя Spark приложения в UI. Если None — берётся job_name.
        """
        self.job_name = job_name
        self.app_name = app_name or job_name
        self._log = logger.bind(job=job_name)
        self.spark: SparkSession = self._create_spark_session()

    # ── SparkSession ───────────────────────────────────────────────────────

    def _create_spark_session(self) -> SparkSession:
        """
        Создаёт SparkSession с настройками для подключения к MinIO как S3.

        SPARK_MASTER из env — "local[*]" для разработки,
        в проде можно переключить на "spark://master:7077" без изменений кода.

        fs.s3a.* — настройки Hadoop S3A коннектора для MinIO.
        path.style.access=true обязателен для MinIO (в отличие от AWS S3).
        """
        master = os.getenv("SPARK_MASTER", "local[*]")

        builder = (
            SparkSession.builder
            .appName(self.app_name)
            .master(master)
            # ── MinIO / S3A ──────────────────────────────────────────────
            .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", settings.minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", settings.minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            # ── Производительность ───────────────────────────────────────
            .config("spark.sql.adaptive.enabled", "true")         # AQE — автооптимизация
            .config("spark.sql.shuffle.partitions", "8")           # меньше для dev
            # ── Логирование — убираем лишний шум в dev ───────────────────
            .config("spark.ui.enabled", "false")                   # отключаем Spark UI
        )

        # TODO: Iceberg support — раскомментировать когда перейдём с Parquet
        # use_iceberg = os.getenv("USE_ICEBERG", "false").lower() == "true"
        # if use_iceberg:
        #     builder = builder.config(
        #         "spark.sql.extensions",
        #         "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        #     )

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")  # убираем INFO спам в консоли
        self._log.info("spark_session_created", master=master, app=self.app_name)
        return spark

    # ── JDBC ──────────────────────────────────────────────────────────────

    def _get_jdbc_options(self, schema: str = "public") -> dict[str, Any]:
        """
        Возвращает словарь JDBC опций для чтения/записи через Spark.

        Args:
            schema: схема PostgreSQL (raw, vault, dm).

        Returns:
            dict готовый для передачи в .options(**opts).

        Пример использования:
            df.write.format("jdbc")
              .options(**self._get_jdbc_options("vault"))
              .option("dbtable", "hub_instrument")
              .save()
        """
        return {
            "url": settings.dwh_jdbc_url,
            "user": settings.dwh_user,
            "password": settings.dwh_password,
            "driver": "org.postgresql.Driver",
            "currentSchema": schema,
            # Размер батча при записи — больше = быстрее, но больше памяти
            "batchsize": 1000,
            # Уровень изоляции транзакций
            "isolationLevel": "READ_COMMITTED",
        }

    # ── Жизненный цикл ────────────────────────────────────────────────────

    @abstractmethod
    def run(self) -> None:
        """
        Основная логика ETL джоба.

        Реализуется в каждом наследнике.
        Здесь читаем данные, трансформируем, пишем.
        """

    def execute(self) -> None:
        """
        Запускает джоб с гарантированной остановкой SparkSession.

        Используй этот метод для запуска — не run() напрямую.
        try/finally гарантирует spark.stop() даже при исключении.
        """
        self._log.info("job_started")
        try:
            self.run()
            self._log.info("job_finished")
        except Exception as e:
            self._log.error("job_failed", error=str(e))
            raise
        finally:
            self.spark.stop()
            self._log.info("spark_stopped")

    # ── Context manager ────────────────────────────────────────────────────

    def __enter__(self) -> "SparkJob":
        return self

    def __exit__(self, exc_type: type, exc_val: Exception, exc_tb: object) -> None:
        """Останавливаем SparkSession при выходе из блока with."""
        if self.spark:
            self.spark.stop()
