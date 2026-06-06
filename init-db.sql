-- Создаём базу данных и пользователя для DWH
CREATE USER dwh WITH PASSWORD 'dwh';
CREATE DATABASE dwh OWNER dwh;

-- Создаём базу для Airflow
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

-- Подключаемся к DWH и создаём схемы
\connect dwh dwh

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS vault;
CREATE SCHEMA IF NOT EXISTS dm;

-- Таблица для raw котировок (куда пишет Kafka consumer / Spark)
CREATE TABLE IF NOT EXISTS raw.quotes (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20)  NOT NULL,
    open_price      NUMERIC(18,6) NOT NULL,
    high_price      NUMERIC(18,6) NOT NULL,
    low_price       NUMERIC(18,6) NOT NULL,
    close_price     NUMERIC(18,6) NOT NULL,
    volume          BIGINT       NOT NULL,
    trade_date      DATE         NOT NULL,
    source          VARCHAR(100) NOT NULL,
    loaded_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, trade_date, source)
);

-- Таблица для raw валютных курсов
CREATE TABLE IF NOT EXISTS raw.rates (
    id              BIGSERIAL PRIMARY KEY,
    base_currency   VARCHAR(10)  NOT NULL,
    target_currency VARCHAR(10)  NOT NULL,
    rate            NUMERIC(18,8) NOT NULL,
    rate_date       DATE         NOT NULL,
    source          VARCHAR(100) NOT NULL,
    loaded_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (base_currency, target_currency, rate_date, source)
);

-- Таблица для raw инструментов
CREATE TABLE IF NOT EXISTS raw.instruments (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20)  NOT NULL,
    instrument_name VARCHAR(255) NOT NULL,
    exchange        VARCHAR(100),
    asset_type      VARCHAR(50),
    source          VARCHAR(100) NOT NULL,
    loaded_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, source)
);
