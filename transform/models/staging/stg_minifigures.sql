-- Cleans raw minifigure data
SELECT
    minifigure_id,
    name,
    num_parts,
    img_url,
    rebrickable_url,
    CAST(ingested_at AS TIMESTAMP) as ingested_at
FROM {{ source('lego_staging', 'src_minifigures') }}
