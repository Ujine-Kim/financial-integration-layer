"""
Утилиты для работы с Data Vault 2.0.

Все функции работают с PySpark DataFrame и Column API.
UDF (User Defined Functions) не используем там где можно обойтись
встроенными функциями Spark — это значительно быстрее, т.к. UDF
выполняются построчно в Python, а встроенные функции — в JVM батчами.
"""

import hashlib

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column


# ── Хэш-ключи (Python уровень — для продюсеров и утилит) ──────────────────

def generate_hash_key(business_key: str) -> str:
    """
    Генерирует MD5 хэш-ключ из одного бизнес-ключа.

    Используется в Python коде (не в Spark).
    Например: в продюсерах для формирования ключей сообщений.

    Args:
        business_key: строковое значение бизнес-ключа, например "AAPL".

    Returns:
        MD5 хэш в верхнем регистре, 32 символа. Например: "D3B07384D113EDEC49EAA6238AD5FF00".

    Example:
        >>> generate_hash_key("AAPL")
        'D3B07384D113EDEC49EAA6238AD5FF00'
    """
    return hashlib.md5(business_key.encode("utf-8")).hexdigest().upper()


def generate_hash_key_composite(*keys: str) -> str:
    """
    Генерирует MD5 хэш-ключ из нескольких бизнес-ключей.

    Ключи конкатенируются через "||" перед хэшированием.
    Разделитель "||" выбран чтобы избежать коллизий:
    ("A", "BC") и ("AB", "C") дадут разные хэши.

    Args:
        *keys: бизнес-ключи для объединения.

    Returns:
        MD5 хэш в верхнем регистре.

    Example:
        >>> generate_hash_key_composite("AAPL", "NASDAQ")
        'A1B2C3...'
    """
    composite = "||".join(str(k) for k in keys)
    return hashlib.md5(composite.encode("utf-8")).hexdigest().upper()


# ── Хэш-ключи (Spark уровень — для DataFrame трансформаций) ───────────────

def hash_key_column(col_name: str) -> Column:
    """
    Spark Column expression для вычисления MD5 хэш-ключа.

    Используется внутри withColumn() для вычисления хэша
    прямо в JVM без сериализации данных в Python.

    Args:
        col_name: имя колонки с бизнес-ключом.

    Returns:
        Column expression: MD5 в верхнем регистре.

    Example:
        df.withColumn("hub_instrument_hk", hash_key_column("symbol"))
    """
    return F.upper(F.md5(F.col(col_name).cast("string")))


def hash_key_composite_column(*col_names: str) -> Column:
    """
    Spark Column expression для MD5 из нескольких колонок.

    Конкатенирует колонки через "||" и считает MD5.

    Args:
        *col_names: имена колонок для конкатенации.

    Returns:
        Column expression.

    Example:
        df.withColumn(
            "lnk_hk",
            hash_key_composite_column("hub_instrument_hk", "hub_market_hk")
        )
    """
    # concat_ws объединяет колонки через разделитель, игнорируя NULL
    return F.upper(F.md5(F.concat_ws("||", *[F.col(c).cast("string") for c in col_names])))


# ── Hash Diff для Satellite (delta-load) ───────────────────────────────────

def generate_hash_diff(df: DataFrame, columns: list[str]) -> DataFrame:
    """
    Добавляет колонку hash_diff — MD5 от конкатенации значений атрибутов.

    hash_diff используется в Satellite для определения изменений:
    если hash_diff новой записи совпадает с последним hash_diff в таблице —
    данные не изменились, вставка не нужна (delta-load паттерн).

    NULL значения заменяются строкой "NULL" перед конкатенацией,
    чтобы NULL не "поглощал" всё выражение (MD5(NULL) = NULL).

    Args:
        df: входной DataFrame.
        columns: список колонок-атрибутов для включения в хэш.

    Returns:
        DataFrame с добавленной колонкой hash_diff (CHAR(32), uppercase).

    Example:
        df = generate_hash_diff(df, ["open_price", "high_price", "low_price", "close_price"])
    """
    # coalesce(col, lit("NULL")) — заменяем NULL на строку "NULL"
    # чтобы хэш всегда вычислялся корректно
    null_safe_cols = [
        F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
        for c in columns
    ]
    return df.withColumn(
        "hash_diff",
        F.upper(F.md5(F.concat_ws("||", *null_safe_cols)))
    )


# ── Vault Metadata ─────────────────────────────────────────────────────────

def add_vault_metadata(df: DataFrame, record_source: str) -> DataFrame:
    """
    Добавляет стандартные метаданные Data Vault к DataFrame.

    Каждая таблица в Data Vault содержит:
    - load_date: когда запись была загружена (время ETL, не время события)
    - record_source: откуда пришли данные (для аудита и lineage)

    Args:
        df: входной DataFrame.
        record_source: источник данных, например "alpha_vantage" или "cbr".

    Returns:
        DataFrame с добавленными колонками load_date и record_source.

    Example:
        df = add_vault_metadata(df, record_source="alpha_vantage")
    """
    return (
        df
        .withColumn("load_date", F.current_timestamp())
        .withColumn("record_source", F.lit(record_source))
    )
