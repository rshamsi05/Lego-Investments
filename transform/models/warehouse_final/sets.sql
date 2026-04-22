-- Master sets table, joined with price history and minifigure counts
{{ config(materialized='table') }}
-- Pulls the most recent market price for each set_id
WITH latest_prices AS (
    SELECT 
        set_id,
        price_30d_avg_usd AS current_market_price,
        observed_date AS last_price_date
    FROM {{ ref('price_history') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY set_id ORDER BY observed_date DESC) = 1

)




SELECT s.* EXCEPT(ingested_at), 
    -- Deriving Metrics
    CASE 
        WHEN s.piece_count > 0 THEN s.retail_price_usd / s.piece_count
        ELSE NULL
    -- Ends case and creates a new column called price_per_piece based on the above calculation
    END AS price_per_piece,

    -- Era Classificaiton
    CASE
        WHEN s.year < 1999 THEN 'Classic'
        WHEN s.year BETWEEN 1999 AND 2005 THEN 'Prequel Trilogy'
        WHEN s.year BETWEEN 2006 AND 2014 THEN 'Expanded Universe'
        WHEN s.year BETWEEN 2015 AND 2019 THEN 'Sequel Trilogy'
        ELSE 'Modern'
    END AS era_classification,
    -- Market Data from join
    p.current_market_price,
    p.last_price_date,

    -- 4. ROI Calculation
    CASE
        WHEN s.retail_price_usd > 0 AND p.current_market_price IS NOT NULL THEN
            ((p.current_market_price - s.retail_price_usd) / s.retail_price_usd) * 100
        ELSE NULL
    END AS price_appreciation_percentage,
    -- Place holder section for future joins when we load Minifigures and Price History tables
    

    -- Metadata
    CURRENT_TIMESTAMP() AS ingested_at
FROM {{source('lego_staging', 'stg_sets')}} s
LEFT JOIN latest_prices p ON s.set_id = p.set_id

