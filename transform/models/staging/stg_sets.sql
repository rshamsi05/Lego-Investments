-- Cleans and standardizes raw sets table from Rebrickable
-- NOTE: Staging sets python file already renames the columns, so this SQL script is not needed as of now, we can refactor later.
SELECT
    set_id,
    name,
    year,
    -- Casting theme_id as a string for consistency
    CAST(theme_id AS STRING) as theme_id,
    piece_count,
    retail_price_usd,
    is_retired,
    rebrickable_url,
    -- Adding metadata 
    CAST(ingested_at AS TIMESTAMP) as ingested_at
-- Read from the raw staging table
FROM {{ source('lego_staging', 'src_sets')}}
