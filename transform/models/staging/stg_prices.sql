-- Cleans and standardizes raw prices table from Bricklink
SELECT
    set_id,
    CAST(observed_date AS DATE) as observed_date,
    avg_price_usd,
    min_price_usd,
    max_price_usd,
    listing_count,
    source,
    CAST(ingested_at AS TIMESTAMP) as ingested_at
FROM {{source('lego_staging', 'src_prices')}}
