--liquibase formatted sql

-- ============================================================
-- v001: Data Vault Hubs
-- Хабы хранят только бизнес-ключи — без истории, без атрибутов.
-- Если бизнес-ключ уже есть — INSERT игнорируется (ON CONFLICT DO NOTHING).
-- ============================================================

-- ── Hub: Финансовые инструменты ────────────────────────────
--changeset author:fil-team id:v001_hub_instrument
CREATE TABLE IF NOT EXISTS vault.hub_instrument (
    hub_instrument_hk   CHAR(32)        NOT NULL,   -- MD5 от symbol
    instrument_bk       VARCHAR(20)     NOT NULL,   -- бизнес-ключ: тикер ("AAPL")
    load_date           TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    record_source       VARCHAR(100)    NOT NULL,

    CONSTRAINT pk_hub_instrument PRIMARY KEY (hub_instrument_hk)
);

CREATE INDEX IF NOT EXISTS idx_hub_instrument_bk
    ON vault.hub_instrument (instrument_bk);

--rollback DROP TABLE IF EXISTS vault.hub_instrument;

-- ── Hub: Валюты ────────────────────────────────────────────
--changeset author:fil-team id:v001_hub_currency
CREATE TABLE IF NOT EXISTS vault.hub_currency (
    hub_currency_hk     CHAR(32)        NOT NULL,   -- MD5 от currency_code
    currency_code       VARCHAR(10)     NOT NULL,   -- бизнес-ключ: код валюты ("USD")
    load_date           TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    record_source       VARCHAR(100)    NOT NULL,

    CONSTRAINT pk_hub_currency PRIMARY KEY (hub_currency_hk)
);

CREATE INDEX IF NOT EXISTS idx_hub_currency_code
    ON vault.hub_currency (currency_code);

--rollback DROP TABLE IF EXISTS vault.hub_currency;

-- ── Hub: Торговые площадки ─────────────────────────────────
--changeset author:fil-team id:v001_hub_market
CREATE TABLE IF NOT EXISTS vault.hub_market (
    hub_market_hk       CHAR(32)        NOT NULL,   -- MD5 от market_code
    market_code         VARCHAR(20)     NOT NULL,   -- бизнес-ключ: код биржи ("NASDAQ")
    load_date           TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    record_source       VARCHAR(100)    NOT NULL,

    CONSTRAINT pk_hub_market PRIMARY KEY (hub_market_hk)
);

CREATE INDEX IF NOT EXISTS idx_hub_market_code
    ON vault.hub_market (market_code);

--rollback DROP TABLE IF EXISTS vault.hub_market;
