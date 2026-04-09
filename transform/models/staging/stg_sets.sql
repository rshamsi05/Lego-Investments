-- Cleans and standardizes raw sets table from Rebrickable
-- NOTE: Staging sets python file already renames the columns, so this SQL script is not needed as of now, we can refactor later.
SELECT set_num AS set_id, name, year,
    -- Casting theme_id as a string for consistency
    CAST(theme_id AS STRING) as theme_id,

    -- Rename num_parts to piece_count
    num_parts as piece_count,

    -- Add metadata
    CURRENT_TIMESTAMP() AS ingested_at
-- Read from the raw staging table
FROM 'lego-investments.lego_staging.stg_sets';
