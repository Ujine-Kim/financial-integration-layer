-- Витрина: ежедневные котировки с расчётными метриками
-- Идемпотентность: DELETE + INSERT по trade_date
-- Запускается из Airflow через PostgresOperator

DELETE FROM dm.dm_quotes_daily
WHERE trade_date = :trade_date;

INSERT INTO dm.dm_quotes_daily (
    instrument_bk,
    instrument_name,
    exchange,
    asset_type,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    daily_return_pct,
    price_range,
    loaded_at
)
SELECT
    h.instrument_bk,
    COALESCE(d.instrument_name, 'Unknown')      AS instrument_name,
    COALESCE(d.exchange, 'Unknown')             AS exchange,
    COALESCE(d.asset_type, 'Unknown')           AS asset_type,
    s.trade_date,
    s.open_price,
    s.high_price,
    s.low_price,
    s.close_price,
    s.volume,
    -- Дневная доходность: (close - open) / open * 100
    CASE
        WHEN s.open_price <> 0
        THEN ROUND((s.close_price - s.open_price) / s.open_price * 100, 4)
        ELSE 0
    END                                          AS daily_return_pct,
    -- Диапазон дня
    ROUND(s.high_price - s.low_price, 6)        AS price_range,
    NOW()                                        AS loaded_at
FROM vault.sat_quote_prices s
JOIN vault.hub_instrument h
    ON s.hub_instrument_hk = h.hub_instrument_hk
-- Берём только актуальные детали инструмента (load_end_date IS NULL)
LEFT JOIN vault.sat_instrument_details d
    ON s.hub_instrument_hk = d.hub_instrument_hk
    AND d.load_end_date IS NULL
WHERE s.trade_date = :trade_date
  -- Из satellite берём только последнюю запись за дату
  AND s.load_date = (
      SELECT MAX(s2.load_date)
      FROM vault.sat_quote_prices s2
      WHERE s2.hub_instrument_hk = s.hub_instrument_hk
        AND s2.trade_date = s.trade_date
  );
