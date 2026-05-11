-- Full cleaned price history for every set over time.
{{ config(materialized='table') }}

WITH basic_prices AS (
    SELECT *
    FROM {{ ref('stg_prices')}}
    WHERE avg_price_usd IS NOT NULL
)

SELECT 
    set_id,
    observed_date,
    avg_price_usd,
    AVG(avg_price_usd) OVER (
        PARTITION BY set_id
        ORDER BY UNIX_DATE(observed_date)
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS price_30d_avg_usd,
    source,
    CURRENT_TIMESTAMP() AS last_updated
FROM basic_prices
