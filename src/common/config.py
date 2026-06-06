"""
Централизованная конфигурация приложения.

Все настройки читаются из переменных окружения (файл .env).
Используется pydantic-settings — автоматическая валидация типов при старте.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Настройки приложения.

    Pydantic читает значения из переменных окружения.
    Если переменная не задана и нет default — упадёт с ошибкой при импорте.
    Это намеренно: лучше упасть сразу, чем в середине пайплайна.
    """

    model_config = SettingsConfigDict(
        env_file=".env",           # читаем из .env файла
        env_file_encoding="utf-8",
        case_sensitive=False,      # KAFKA_BOOTSTRAP_SERVERS == kafka_bootstrap_servers
        extra="ignore",            # игнорируем лишние переменные из .env
    )

    # ── API ключи ──────────────────────────────────
    alpha_vantage_api_key: str
    exchange_rate_api_key: str

    # ── Kafka ──────────────────────────────────────
    kafka_bootstrap_servers: str = "localhost:9092"

    # ── MinIO ──────────────────────────────────────
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin123"

    # ── PostgreSQL (DWH) ───────────────────────────
    dwh_host: str = "localhost"
    dwh_port: int = 5432
    dwh_name: str = "dwh"
    dwh_user: str = "dwh"
    dwh_password: str = "dwh"

    @property
    def dwh_jdbc_url(self) -> str:
        """JDBC URL для подключения Spark к PostgreSQL."""
        return f"jdbc:postgresql://{self.dwh_host}:{self.dwh_port}/{self.dwh_name}"

    @property
    def dwh_psycopg2_url(self) -> str:
        """URL для psycopg2 (используется в тестах и утилитах)."""
        return (
            f"postgresql://{self.dwh_user}:{self.dwh_password}"
            f"@{self.dwh_host}:{self.dwh_port}/{self.dwh_name}"
        )


# Единственный экземпляр на всё приложение (singleton)
# Импортируй его везде: from src.common.config import settings
settings = Settings()
