SELECT
    set_id,
    minifigure_id,
    quantity,
    CAST(ingested_at AS TIMESTAMP) as ingested_at
FROM {{ source('lego_staging', 'src_set_minifigures') }}
