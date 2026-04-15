-- Master sets table, joined with price history and minifigure counts
SELECT * EXCEPT(ingested_at), 
    -- Deriving Metrics
    CASE 
        WHEN piece_count > 0 THEN retail_price_usd / piece_count
        ELSE NULL
    -- Ends case and creates a new column called price_per_piece based on the above calculation
    END AS price_per_piece,

    -- Era Classificaiton
    CASE
        WHEN year < 1999 THEN 'Classic'
        WHEN year BETWEEN 1999 AND 2005 THEN 'Prequel Trilogy'
        WHEN year BETWEEN 2006 AND 2014 THEN 'Expanded Universe'
        WHEN year BETWEEN 2015 AND 2019 THEN 'Sequel Trilogy'
        ELSE 'Modern'
    END AS era_classification,
    -- Place holder section for future joins when we load Minifigures and Price History tables
    

    -- Metadata
    CURRENT_TIMESTAMP() AS ingested_at
FROM `lego-investments.lego_staging.stg_sets`
