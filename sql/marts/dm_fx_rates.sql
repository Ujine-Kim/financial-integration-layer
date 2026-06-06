-- Витрина: валютные курсы
-- Идемпотентность: DELETE + INSERT по rate_date

DELETE FROM dm.dm_fx_rates
WHERE rate_date = :trade_date;

INSERT INTO dm.dm_fx_rates (
    base_currency,
    target_currency,
    rate,
    rate_date,
    source,
    loaded_at
)
SELECT
    hb.currency_code    AS base_currency,
    ht.currency_code    AS target_currency,
    s.rate,
    s.rate_date,
    s.record_source     AS source,
    NOW()               AS loaded_at
FROM vault.sat_rate_values s
JOIN vault.lnk_rate_currency l
    ON s.lnk_rate_currency_hk = l.lnk_rate_currency_hk
JOIN vault.hub_currency hb
    ON l.hub_currency_hk_base = hb.hub_currency_hk
JOIN vault.hub_currency ht
    ON l.hub_currency_hk_target = ht.hub_currency_hk
WHERE s.rate_date = :trade_date
  AND s.load_date = (
      SELECT MAX(s2.load_date)
      FROM vault.sat_rate_values s2
      WHERE s2.lnk_rate_currency_hk = s.lnk_rate_currency_hk
        AND s2.rate_date = s.rate_date
  );
