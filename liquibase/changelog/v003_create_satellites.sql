--liquibase formatted sql

-- ============================================================
-- v003: Data Vault Satellites
-- Сателлиты хранят атрибуты с полной историей изменений.
-- Новая строка появляется только если hash_diff изменился —
-- это называется delta-load.
--
-- load_end_date = NULL означает "текущая актуальная запись".
-- ============================================================

-- ── Satellite: Детали инструмента (медленно меняющиеся данные) ──
--changeset author:fil-team id:v003_sat_instrument_details
CREATE TABLE IF NOT EXISTS vault.sat_instrument_details (
    hub_instrument_hk   CHAR(32)        NOT NULL,
    load_date           TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    load_end_date       TIMESTAMPTZ     NULL,           -- NULL = актуальная запись
    record_source       VARCHAR(100)    NOT NULL,
    hash_diff           CHAR(32)        NOT NULL,       -- MD5 от атрибутов

    -- Атрибуты
    instrument_name     VARCHAR(255)    NOT NULL,
    exchange            VARCHAR(100)    NOT NULL,
    asset_type          VARCHAR(50)     NOT NULL,

    CONSTRAINT pk_sat_instrument_details
        PRIMARY KEY (hub_instrument_hk, load_date),
    CONSTRAINT fk_sat_instrument_details_hub
        FOREIGN KEY (hub_instrument_hk)
        REFERENCES vault.hub_instrument (hub_instrument_hk)
);

CREATE INDEX IF NOT EXISTS idx_sat_instrument_hk
    ON vault.sat_instrument_details (hub_instrument_hk);
CREATE INDEX IF NOT EXISTS idx_sat_instrument_current
    ON vault.sat_instrument_details (hub_instrument_hk)
    WHERE load_end_date IS NULL;   -- быстрый поиск актуальных записей

--rollback DROP TABLE IF EXISTS vault.sat_instrument_details;

-- ── Satellite: Котировки (одна строка на инструмент+дату) ──
--changeset author:fil-team id:v003_sat_quote_prices
CREATE TABLE IF NOT EXISTS vault.sat_quote_prices (
    hub_instrument_hk   CHAR(32)        NOT NULL,
    load_date           TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    record_source       VARCHAR(100)    NOT NULL,
    hash_diff           CHAR(32)        NOT NULL,

    -- Атрибуты
    open_price          NUMERIC(18, 6)  NOT NULL,
    high_price          NUMERIC(18, 6)  NOT NULL,
    low_price           NUMERIC(18, 6)  NOT NULL,
    close_price         NUMERIC(18, 6)  NOT NULL,
    volume              BIGINT          NOT NULL,
    trade_date          DATE            NOT NULL,

    CONSTRAINT pk_sat_quote_prices
        PRIMARY KEY (hub_instrument_hk, load_date),
    CONSTRAINT fk_sat_quote_prices_hub
        FOREIGN KEY (hub_instrument_hk)
        REFERENCES vault.hub_instrument (hub_instrument_hk)
);

CREATE INDEX IF NOT EXISTS idx_sat_quote_prices_hk
    ON vault.sat_quote_prices (hub_instrument_hk);
CREATE INDEX IF NOT EXISTS idx_sat_quote_prices_trade_date
    ON vault.sat_quote_prices (trade_date);

--rollback DROP TABLE IF EXISTS vault.sat_quote_prices;

-- ── Satellite: Значения курсов валют ───────────────────────
--changeset author:fil-team id:v003_sat_rate_values
CREATE TABLE IF NOT EXISTS vault.sat_rate_values (
    lnk_rate_currency_hk    CHAR(32)        NOT NULL,
    load_date               TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    record_source           VARCHAR(100)    NOT NULL,
    hash_diff               CHAR(32)        NOT NULL,

    -- Атрибуты
    rate                    NUMERIC(18, 8)  NOT NULL,
    rate_date               DATE            NOT NULL,

    CONSTRAINT pk_sat_rate_values
        PRIMARY KEY (lnk_rate_currency_hk, load_date),
    CONSTRAINT fk_sat_rate_values_lnk
        FOREIGN KEY (lnk_rate_currency_hk)
        REFERENCES vault.lnk_rate_currency (lnk_rate_currency_hk)
);

CREATE INDEX IF NOT EXISTS idx_sat_rate_values_lnk
    ON vault.sat_rate_values (lnk_rate_currency_hk);
CREATE INDEX IF NOT EXISTS idx_sat_rate_values_date
    ON vault.sat_rate_values (rate_date);

--rollback DROP TABLE IF EXISTS vault.sat_rate_values;
