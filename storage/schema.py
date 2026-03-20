'''
BigQuery table schema definitions, field names, types, and models
'''

from google.cloud import bigquery

# Staging tables (raw data from each source):
# for some of these cases wouldn't there be several answers like a part could belong to multiple sets.

SCHEMA_STG_SETS = [
    bigquery.SchemaField('set_id', 'STRING', mode='REQUIRED', description='Unique identifier for the set'),
    bigquery.SchemaFiled('name', 'STRING', mode='REQUIRED', description='Name of the set'),
    bigquery.SchemaField('year', 'INTEGER', mode='REQUIRED', description='Year the set was released'),
    bigquery.SchemaFiled('theme_name', 'STRING', mode='NULLABLE', description='Theme name of the set'),
    bigquery.SchemaFiled('theme_id', 'STRING', mode='NULLABLE', description='Theme ID of the set'),
    bigquery.SchemaFiled('subtheme_name', 'STRING', mode='NULLABLE', description='subtheme name of the set'),
    bigquery.SchemaFiled('subtheme_id', 'STRING', mode='NULLABLE', description='subtheme ID of the set'),
    bigquery.SchemaField('piece_count', 'INTEGER', mode='NULLABLE', description='Number of pieces in the set'),
    bigquery.SchemaField('retail_price_usd', 'INTEGER', mode='NULLABLE', description='Original retail price of set'),
    bigquery.SchemaField('is_retired', 'BOOLEAN', mode='REQUIRED', description='Whether the set is retired'),
    bigquery.SchemaField('rebrickable_url', 'STRING', mode='NULLABLE', description='URL to Rebrickable set page'),
    bigquery.SchemaField('ingested_at', 'TIMESTAMP', mode='REQUIRED', description='Timestamp when the record was ingested into the staging table')
]

SCHEMA_STG_PRICES = [
    bigquery.SchemaField('set_id', 'STRING', mode='REQUIRED', description='Unique identifier for the set'),
    bigquery.SchemaField('observed_date', 'DATE', mode='REQUIRED', description='Date when the price was observed'),
    bigquery.SchemaField('avg_price_usd', 'FLOAT', mode='NULLABLE', description='Average market price of the set on the observed date'),
    bigquery.SchemaField('min_price_usd', 'FLOAT', mode='NULLABLE', description='Minimum market price of the set on the observed date'),
    bigquery.SchemaField('max_price_usd', 'FLOAT', mode='NULLABLE', description='Maximum market price of the set on the observed date'),
    bigquery.SchemaField('listing_count', 'INTEGER', mode='NULLABLE', description='Number of active listings for the set'),
    bigquery.SchemaFiled('source', 'STRING', mode='REQUIRED', description='Source of the price data (e.g. BrickLink, eBay)'),
    bigquery.SchemaField('source_url', 'STRING', mode='NULLABLE', description='URL to price source (Bricklink listing/page)'),
    bigquery.SchemaField('ingested_at', 'TIMESTAMP', mode='REQUIRED', description='Timestamp when the record was ingested into the staging table')
]

SCHEMA_STG_MINIFIGURES = [
    bigquery.SchemaField('minifigure_id', 'STRING', mode='REQUIRED', description='Unique identifier for the minifigure'),
    bigquery.SchemaField('name', 'STRING', mode='REQUIRED', description='Name of the minifigure'),
    bigquery.SchemaField('num_parts', 'INTEGER', mode='NULLABLE', description='Number of parts in the minifigure'),
    bigquery.SchemaField('img_url', 'STRING', mode='NULLABLE', description='URL of the minifigure image'),
    bigquery.SchemaField('rebrickable_url', 'STRING', mode='NULLABLE', description='URL of the minifigure on Rebrickable'),
    bigquery.SchemaField('minifigure_name', 'STRING', mode='NULLABLE', description='Name of the character the minifigure represents'),
    bigquery.SchemaField('year_introduced', 'INTEGER', mode='NULLABLE', description='Year the minifigure was introduced'),
    bigquery.SchemaField('ingested_at', 'TIMESTAMP', mode='REQUIRED', description='Timestamp when the record was ingested into the staging table')
]

# Junction table link sets to minifigures
SCHEMA_STG_SET_MINIFIGURES = [
    bigquery.SchemaField('set_id', 'STRING', mode='NULLABLE', description='Set number the minifigure belongs to'),
    bigquery.SchemaField('minifigure_id', 'STRING', mode='REQUIRED', description='Unique identifier for the minifigure'),
    bigquery.SchemaField('quantity', 'INTEGER', mode='NULLABLE', description='Quantity of the minifigure in the set'),
    bigquery.SchemaField('is_exclusive', 'BOOLEAN', mode='NULLABLE', description='Whether the minifigure is exclusive to the set'),
    bigquery.SchemaField('ingested_at', 'TIMESTAMP', mode='REQUIRED', description='Timestamp when the record was ingested into the staging table')
]

SCHEMA_STG_PARTS = [
    bigquery.SchemaField('part_id', 'STRING', mode='REQUIRED', description='Unique identifier for the part'),
    bigquery.SchemaField('part_number', 'STRING', mode='REQUIRED', description='Part number(e.g "3001" for a standard 2x4 brick)'),
    bigquery.SchemaField('name', 'STRING', mode='NULLABLE', description='Name of the part'),
    bigquery.SchemaField('part_category_id', mode='NULLABLE', description='Identifier for the part category'),
    bigquery.SchemaField('part_category_name', 'STRING', mode='NULLABLE', description='Name of the part category(e.g Brick, Plate, Slope, etc)'),
    bigquery.SchemaField('img_url', 'STRING', mode='NULLABLE', description='URL of the part image'),
    bigquery.SchemaField('rebrickable_url', 'STRING', mode='NULLABLE', description='URL of the part on Rebrickable'),
    bigquery.SchemaField('year_introduced', 'INTEGER', mode='NULLABLE', description='Year the part was introduced'),
    bigquery.SchemaField('ingested_at', 'TIMESTAMP', mode='REQUIRED', description='Timestamp when the record was ingested into the staging table')
]
# Junction/Bridge table to represent the many-to-many relationship between sets and parts
SCHEMA_STG_SET_PARTS = [
    bigquery.SchemaField('set_id', 'STRING', mode='NULLABLE', description='Set number the part belongs to'),
    bigquery.SchemaField('part_id', 'STRING', mode='REQUIRED', description='Unique identifier for the part'),
    bigquery.SchemaField('color_id', 'STRING', mode='NULLABLE', description='Identifier for the part color from rebrickable'),
    bigquery.SchemaField('color_name', 'STRING', mode='NULLABLE', description='Name of the part color from rebrickable'),
    bigquery.SchemaField('quantity', 'INTEGER', mode='NULLABLE', description='Quantity of the part in the set'),
    bigquery.SchemaField('is_spare', 'BOOLEAN', mode='NULLABLE', description='Whether the part is a spare part included in the set'),
    bigquery.SchemaField('ingested_at', 'TIMESTAMP', mode='REQUIRED', description='Timestamp when the record was ingested into the staging table')
]

# Warehouse tables (cleaned and transformed data ready for analysis):

