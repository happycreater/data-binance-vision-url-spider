-- Active: 1770470972061@@127.0.0.1@8123@binance_um
CREATE DATABASE binance_um;

CREATE TABLE binance_um.symbol_info (
    symbol LowCardinality (String),
    base_asset LowCardinality (String),
    quote_asset LowCardinality (String),
    listed_at DateTime64 (3),
    delisted_at DateTime64 (3),
    status LowCardinality (String),
    api_kline_start_time DateTime64 (3),
    api_kline_end_time DateTime64 (3),
    updated_at DateTime64 (3)
) ENGINE = ReplacingMergeTree (updated_at)
ORDER BY symbol;

CREATE TABLE binance_um.klines_1m (
    symbol LowCardinality (String),
    open_time Datetime64 (3),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    close_time Datetime64 (3),
    quote_asset_volume Float64,
    number_of_trades UInt32,
    taker_buy_base_asset_volume Float64,
    taker_buy_quote_asset_volume Float64
) ENGINE = ReplacingMergeTree (close_time)
PARTITION BY
    toYYYYMM (open_time)
ORDER BY (symbol, open_time);

OPTIMIZE TABLE binance_um.klines_1m FINAL;

OPTIMIZE TABLE binance_um.symbol_info FINAL;

CREATE VIEW binance_um.v_klines_1m_clean_zero_volume_pct_info AS
WITH
    tmp AS (
        SELECT
            symbol,
            min(open_time) AS listed_at,
            max(open_time) AS delisted_at,
            countIf (volume = 0) AS zero_volume_bars,
            count() AS total_bars
        FROM binance_um.v_klines_1m_clean
        GROUP BY
            symbol
        ORDER BY zero_volume_bars DESC
    )
SELECT
    symbol,
    listed_at,
    delisted_at,
    zero_volume_bars,
    total_bars,
    zero_volume_bars / total_bars AS zero_volume_percentage
FROM tmp
ORDER BY zero_volume_percentage DESC;

CREATE VIEW binance_um.v_klines_1m_clean_gap_info AS
SELECT
    symbol,
    open_time,
    prev_open_time,
    dateDiff(
        'minute',
        prev_open_time,
        open_time
    ) AS gap_minutes
FROM (
        SELECT
            symbol, open_time, lag(open_time) OVER (
                PARTITION BY
                    symbol
                ORDER BY open_time
            ) AS prev_open_time
        FROM binance_um.v_klines_1m_clean
    ) AS gaps
WHERE
    prev_open_time != toDateTime ('1970-01-01 00:00:00')
    AND dateDiff(
        'minute',
        prev_open_time,
        open_time
    ) != 1;

SELECT * FROM system.view_refreshes;

CREATE VIEW binance_um.v_klines_1m_clean AS
SELECT k.*
FROM binance_um.klines_1m AS k LEFT ANY
    JOIN (
        SELECT symbol, greatest(
                listed_at, api_kline_start_time
            ) AS start_time, least(
                delisted_at, api_kline_end_time
            ) AS end_time
        FROM binance_um.symbol_info FINAL
    ) AS i USING (symbol)
WHERE
    k.open_time BETWEEN i.start_time AND i.end_time;

CREATE VIEW binance_um.v_klines AS
SELECT
    symbol,
    toStartOfInterval (open_time, toIntervalMinute({itv:UInt32})) AS open_time,
    argMin (open, open_time) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax (close, open_time) AS close,
    sum(volume) AS volume,
    max(close_time) AS close_time,
    sum(quote_asset_volume) AS quote_asset_volume,
    sum(number_of_trades) AS number_of_trades,
    sum(taker_buy_base_asset_volume) AS taker_buy_base_asset_volume,
    sum(taker_buy_quote_asset_volume) AS taker_buy_quote_asset_volume
FROM binance_um.v_klines_1m_clean
GROUP BY
    symbol,
    open_time
ORDER BY symbol, open_time;

-- 测试查询
SELECT *
FROM binance_um.v_klines (itv = 60 * 24)
WHERE
    symbol IN ('BTCUSDT');

SELECT *
FROM binance_um.v_klines (itv = 60 * 24)
WHERE
    open_time = '2025-10-01 00:00:00';

SELECT *
FROM binance_um.v_klines (itv = 60 * 24)
WHERE
    symbol = 'SUSDT';

SELECT DISTINCT symbol FROM binance_um.v_klines (itv = 60 * 24);