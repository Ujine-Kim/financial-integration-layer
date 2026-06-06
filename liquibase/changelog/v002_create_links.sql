--liquibase formatted sql

-- ============================================================
-- v002: Data Vault Links
-- Линки фиксируют связи между хабами.
-- Сами по себе не меняются — только появляются новые строки.
-- ============================================================

-- ── Link: Котировка ↔ Инструмент + Биржа ──────────────────
--changeset author:fil-team id:v002_lnk_quote_instrument
CREATE TABLE IF NOT EXISTS vault.lnk_quote_instrument (
    lnk_quote_instrument_hk CHAR(32)    NOT NULL,   -- MD5 от (instrument_hk || market_hk)
    hub_instrument_hk       CHAR(32)    NOT NULL,
    hub_market_hk           CHAR(32)    NOT NULL,
    load_date               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    record_source           VARCHAR(100) NOT NULL,

    CONSTRAINT pk_lnk_quote_instrument
        PRIMARY KEY (lnk_quote_instrument_hk),
    CONSTRAINT fk_lnk_quote_instrument_hub
        FOREIGN KEY (hub_instrument_hk)
        REFERENCES vault.hub_instrument (hub_instrument_hk),
    CONSTRAINT fk_lnk_quote_market_hub
        FOREIGN KEY (hub_market_hk)
        REFERENCES vault.hub_market (hub_market_hk)
);

CREATE INDEX IF NOT EXISTS idx_lnk_quote_instrument_hk
    ON vault.lnk_quote_instrument (hub_instrument_hk);
CREATE INDEX IF NOT EXISTS idx_lnk_quote_market_hk
    ON vault.lnk_quote_instrument (hub_market_hk);

--rollback DROP TABLE IF EXISTS vault.lnk_quote_instrument;

-- ── Link: Курс ↔ Базовая валюта + Целевая валюта ──────────
--changeset author:fil-team id:v002_lnk_rate_currency
CREATE TABLE IF NOT EXISTS vault.lnk_rate_currency (
    lnk_rate_currency_hk    CHAR(32)    NOT NULL,   -- MD5 от (currency_hk_base || currency_hk_target)
    hub_currency_hk_base    CHAR(32)    NOT NULL,   -- например RUB
    hub_currency_hk_target  CHAR(32)    NOT NULL,   -- например USD
    load_date               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    record_source           VARCHAR(100) NOT NULL,

    CONSTRAINT pk_lnk_rate_currency
        PRIMARY KEY (lnk_rate_currency_hk),
    CONSTRAINT fk_lnk_rate_currency_base
        FOREIGN KEY (hub_currency_hk_base)
        REFERENCES vault.hub_currency (hub_currency_hk),
    CONSTRAINT fk_lnk_rate_currency_target
        FOREIGN KEY (hub_currency_hk_target)
        REFERENCES vault.hub_currency (hub_currency_hk)
);

CREATE INDEX IF NOT EXISTS idx_lnk_rate_currency_base
    ON vault.lnk_rate_currency (hub_currency_hk_base);
CREATE INDEX IF NOT EXISTS idx_lnk_rate_currency_target
    ON vault.lnk_rate_currency (hub_currency_hk_target);

--rollback DROP TABLE IF EXISTS vault.lnk_rate_currency;
