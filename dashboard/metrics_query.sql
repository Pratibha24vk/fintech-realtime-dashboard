-- dashboard/metrics_query.sql
-- Key SQL queries for connecting Power BI / Tableau to the trade metrics serving layer.
-- Run these against Snowflake ANALYTICS schema or Delta Lake via Databricks SQL.

-- ── 1. Latest VWAP per ticker (last completed 1-min window) ────────────────
with latest_window as (
    select ticker, max(window_start) as latest_window
    from analytics.trade_vwap_1min
    group by ticker
)
select
    v.ticker,
    v.window_start,
    v.window_end,
    round(v.vwap, 4)          as vwap,
    v.total_volume,
    v.trade_count,
    v.open,
    v.close,
    v.high,
    v.low,
    round(v.high - v.low, 4)  as price_range
from analytics.trade_vwap_1min v
join latest_window lw
    on v.ticker = lw.ticker
    and v.window_start = lw.latest_window
order by v.ticker;


-- ── 2. Top 5 movers by % price change (last 5 minutes) ────────────────────
with price_windows as (
    select
        ticker,
        first_value(open) over (
            partition by ticker order by window_start asc
            rows between unbounded preceding and unbounded following
        ) as open_price,
        last_value(close) over (
            partition by ticker order by window_start asc
            rows between unbounded preceding and unbounded following
        ) as close_price
    from analytics.trade_vwap_1min
    where window_start >= dateadd('minute', -5, current_timestamp())
),
deduplicated as (
    select distinct ticker, open_price, close_price from price_windows
)
select
    ticker,
    round(open_price, 4)                                        as open_price,
    round(close_price, 4)                                       as close_price,
    round((close_price - open_price) / open_price * 100, 3)    as pct_change,
    case
        when close_price > open_price then '▲'
        when close_price < open_price then '▼'
        else '–'
    end                                                          as direction
from deduplicated
order by abs((close_price - open_price) / open_price) desc
limit 5;


-- ── 3. Rolling volatility heatmap (last 30 minutes) ───────────────────────
select
    ticker,
    date_trunc('minute', window_start)       as minute_bucket,
    round(price_volatility, 6)               as volatility,
    round(avg_price, 4)                      as avg_price,
    trade_count,
    case
        when price_volatility > 2.0 then 'HIGH'
        when price_volatility > 0.5 then 'MEDIUM'
        else 'LOW'
    end                                      as volatility_level
from analytics.trade_volatility_5min
where window_start >= dateadd('minute', -30, current_timestamp())
order by ticker, minute_bucket;


-- ── 4. Volume profile by exchange (last hour) ─────────────────────────────
select
    ticker,
    exchange,
    sum(total_volume)                        as total_volume,
    round(
        sum(total_volume) * 100.0 /
        sum(sum(total_volume)) over (partition by ticker), 2
    )                                        as volume_share_pct
from analytics.trade_vwap_1min v
join analytics.trade_raw r using (ticker, window_start)  -- hypothetical join
where v.window_start >= dateadd('hour', -1, current_timestamp())
group by ticker, exchange
order by ticker, total_volume desc;


-- ── 5. Spike alert log (prices > 3σ from 5-min rolling mean) ─────────────
select
    ticker,
    window_start,
    avg_price,
    price_volatility,
    round(avg_price + 3 * price_volatility, 4) as upper_spike_threshold,
    round(avg_price - 3 * price_volatility, 4) as lower_spike_threshold
from analytics.trade_volatility_5min
where price_volatility is not null
  and window_start >= dateadd('hour', -1, current_timestamp())
order by window_start desc, price_volatility desc;
