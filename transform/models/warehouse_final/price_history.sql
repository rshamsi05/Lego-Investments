-- Full cleaned price history for every set over time.
-- What does this mean
{{ config(materialized='table') }}

-- How exactly does this sql code segment work?
WITH basic_prices AS (
    SELECT *
    FROM {{ source('lego_staging', 'stg_prices')}}
    WHERE avg_price_usd IS NOT NULL
)

SELECT 
    set_id,
    observed_date,
    avg_price_usd,
    --- How does the window function work here?
    AVG(avg_price_usd) OVER (
        PARTITION BY set_id
        ORDER BY UNIX_DATE(observed_date)
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS price_30d_avg_usd,
    source,
    CURRENT_TIMESTAMP() AS last_updated
FROM basic_prices
