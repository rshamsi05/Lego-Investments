-- All minifigueres with exclusivity and set appearance counts.
{{ config(materialized='table') }}

WITH minifig_set_history AS (
    -- Join Junction table with sets to get the release year
    SELECT
        sm.minifigure_id,
        sm.set_id,
        s.year,
        -- Rank sets by year to find true first/latest
        ROW_NUMBER() OVER (PARTITION BY sm.minifigure_id ORDER BY s.year ASC, s.set_id ASC) as rank_oldest,
        ROW_NUMBER() OVER (PARTITION BY sm.minifigure_id ORDER BY s.year DESC, s.set_id DESC) as rank_newest
    FROM {{ ref('stg_set_minifigures') }} sm
    JOIN {{ ref('stg_sets')}} s ON sm.set_id = s.set_id
),

minifig_stats AS (
    SELECT
        minifigure_id,
        COUNT(DISTINCT set_id) as set_count,
        -- Grab set_id from the top ranked rows
        MAX(CASE WHEN rank_oldest = 1 THEN set_id END) as first_appearance_set_id,
        MAX(CASE WHEN rank_oldest = 1 THEN year END) as first_appearance_year,
        MAX(CASE WHEN rank_newest = 1 THEN set_id END) as latest_appearance_set_id,
        MAX(CASE WHEN rank_newest = 1 THEN year END) as latest_appearance_year,
        -- Exclusivity check
        CASE WHEN COUNT(DISTINCT set_id) = 1 THEN TRUE ELSE FALSE END as is_exclusive
    FROM minifig_set_history
    GROUP BY 1
)

SELECT
    m.*,
    s.set_count,
    s.is_exclusive,
    s.first_appearance_set_id,
    s.first_appearance_year,
    s.latest_appearance_set_id,
    s.latest_appearance_year,
    CURRENT_TIMESTAMP() as warehouse_updated_at
FROM {{ ref('stg_minifigures') }} m
LEFT JOIN minifig_stats s ON m.minifigure_id = s.minifigure_id
