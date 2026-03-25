'''
BigQuery table schema definitions, field names, types, and models
'''

from google.cloud import bigquery

# Staging tables (raw data from each source):

SCHEMA_STG_SETS = [
    bigquery.SchemaField('set_id', 'STRING', mode='REQUIRED', description='Unique identifier for the set'),
    bigquery.SchemaField('name', 'STRING', mode='REQUIRED', description='Name of the set'),
    bigquery.SchemaField('year', 'INTEGER', mode='REQUIRED', description='Year the set was released'),
    bigquery.SchemaField('theme_name', 'STRING', mode='NULLABLE', description='Theme name of the set'),
    bigquery.SchemaField('theme_id', 'STRING', mode='NULLABLE', description='Theme ID of the set'),
    bigquery.SchemaField('subtheme_name', 'STRING', mode='NULLABLE', description='subtheme name of the set'),
    bigquery.SchemaField('subtheme_id', 'STRING', mode='NULLABLE', description='subtheme ID of the set'),
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
    bigquery.SchemaField('source', 'STRING', mode='REQUIRED', description='Source of the price data (e.g. BrickLink, eBay)'),
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
    bigquery.SchemaField('part_category_id', 'STRING',  mode='NULLABLE', description='Identifier for the part category'),
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

'''
Warehouse tables (cleaned and transformed data ready for analysis):
'''
# Master sets table
SCHEMA_SETS = [
    bigquery.SchemaField('set_id', 'STRING', mode='REQUIRED', description='Unique identifier for the set'),
    bigquery.SchemaField('name', 'STRING', mode='REQUIRED', description='Name of the set.'),
    bigquery.SchemaField('year', 'INTEGER', mode='REQUIRED', description='Year the set was released'),
    bigquery.SchemaField('theme_name', 'STRING', mode='NULLABLE', description='Theme name of the set(e.g Star Wars)'),
    bigquery.SchemaField('subtheme_name', 'STRING', mode='NULABLE', description='Subtheme name of the set(e.g Star Wars: The Mandalorian)'),
    bigquery.SchemaField('piece_count', 'INTEGER', mode='NULLABLE', description='Number of pieces in the set'),
    bigquery.SchemaField('retail_price_usd', 'FLOAT', mode='NULLABLE', description='Original retail price of set'),
    bigquery.SchemaField('is_retired', 'BOOLEAN', mode='REQUIRED', description='Whether the set is retired'),
    bigquery.SchemaField('retirement_date', 'DATE', mode='NULLABLE', description='Date when the set was retired'),
    bigquery.SchemaField('days_since_retirement', 'INTEGER', mode='NULLABLE', description='Number of days since the set was retired'),
    bigquery.SchemaField('minifigure_count', 'INTEGER', mode='NULLABLE', description='Total number of unique minifigures included in the set'),
    bigquery.SchemaField('part_count_unique', 'INTEGER', mode='NULLABLE', description='Number of unique parts in the set'),
    # does this column makes sense, some pieces might be more expensive than other pieces?
    bigquery.SchemaField('price_per_piece', 'FLOAT', mode='NULLABLE', description='Original retail price divided by piece count'),
    bigquery.SchemaField('era_classification', 'STRING', mode='NULLABLE', description='Classification of the set into an era based on its release year (e.g. Classic, Modern, etc)'),
    bigquery.SchemaField('current_avg_price_usd', 'FLOAT', mode='NULLABLE', description='Current average market price of the set'),
    bigquery.SchemaField('price_appreciation_percentage', 'FLOAT', mode='NULLABLE', description='Percentage increase in price from original retail to current average market price'),
    # what is the purpose of this column?
    bigquery.SchemaField('annualized_return_percentage', 'FLOAT', mode='NULLABLE', description='Annualized return percentage based on the original retail price, current average market price, and number of years since release'),
    bigquery.SchemaField('rebrickable_url', 'STRING', mode='NULLABLE', description='URL to Rebrickable set page'),
    bigquery.SchemaField('last_updated', 'TIMESTAMP', mode='REQUIRED', description='Timestamp when the record was created in the warehouse'),
]

# Cleaned price history with rolling averages for trend analysis
SCHEMA_PRICE_HISTORY = [
    bigquery.SchemaField('set_id', 'STRING', mode='REQUIRED', description='Unique identifier for the set'),
    bigquery.SchemaField('observed_date', 'DATE', mode='REQUIRED', description='Date when the price was observed'),
    bigquery.SchemaField('avg_price_usd', 'FLOAT', mode='NULLABLE', description='Average market price of the set on the observed date'),
    bigquery.SchemaField('min_price_usd', 'FLOAT', mode='NULLABLE', description='Minimum market price of the set on the observed date'),
    bigquery.SchemaField('max_price_usd', 'FLOAT', mode='NULLABLE', description='Maximum market price of the set on the observed date'),
    bigquery.SchemaField('listing_count', 'INTEGER', mode='NULLABLE', description='Number of active listings for the set'),
    # What is a rolling average and why is it used here?
    bigquery.SchemaField('price_7d_avg_usd', 'FLOAT', mode='NULLABLE', description='7-day rolling average price of the set'),
    bigquery.SchemaField('price_30d_avg_usd', 'FLOAT', mode='NULLABLE', description='30-day rolling average price of the set'),
    bigquery.SchemaField('price_90d_avg_usd', 'FLOAT', mode='NULLABLE', description='90-day rolling average price of the set'),
    # what is list_count_7d_avg and why is it useful
    bigquery.SchemaField('listing_count_7d_avg', 'INTEGER', mode='NULLABLE', description='7-day average of listing count'),
    # What is a 30 day price volatility and why is it useful?
    bigquery.SchemaField('price_volatility_30d', 'FLOAT', mode='NULLABLE', description='30-day price volatility (standard deviation of avg_price_usd)'),
    bigquery.SchemaField('source', 'STRING', mode='REQUIRED', description='Source of the price data (e.g. BrickLink, eBay)'),
    bigquery.SchemaField('source_url', 'STRING', mode='NULLABLE', description='URL to price source (Bricklink listing/page)'),
    bigquery.SchemaField('last_updated', 'TIMESTAMP', mode='REQUIRED', description='Timestamp when the record was created in the warehouse')
]

# Master minifigure table with set appearaance metrics
SCHEMA_MINIFIGURES = [
    bigquery.SchemaField('minifigure_id', 'STRING', mode='REQUIRED', description='Unique identifier for the minifigure'),
    # Don we need character name what is the difference between name and character name 
    bigquery.SchemaField('name', 'STRING', mode='REQUIRED', description='Name of the minifigure'),
    bigquery.SchemaField('minifigure_name', 'STRING', mode='NULLABLE', description='Name of the character the minifigure represents'),
    bigquery.SchemaField('num_parts', 'INTEGER', mode='NULLABLE', description='Number of parts in the minifigure'),
    bigquery.SchemaField('year_introduced', 'INTEGER', mode='NULLABLE', description='Year the minifigure was introduced'),
    bigquery.SchemaField('set_count', 'INTEGER', mode='NULLABLE', description='Number of sets this minifigure appears in'),
    bigquery.SchemaField('is_exclusive', 'BOOLEAN', mode='NULLABLE', description='Whether the minifigure is exclusive to a single set'),
    bigquery.SchemaField('exclusive_set_id', 'STRING', mode='NULLABLE', description='Set ID if the minifigure is exclusive to one set'),
    # Why do we want to know the first and last appearance of a minifigure in a set, what insights can we gain from this?
    bigquery.SchemaField('first_appearance_set_id', 'STRING', mode='NULLABLE', description='Set ID of the minifigure\'s first appearance'),
    bigquery.SchemaField('last_appearance_set_id', 'STRING', mode='NULLABLE', description='Set ID of the minifigure\'s most recent appearance'),
    bigquery.SchemaField('img_url', 'STRING', mode='NULLABLE', description='URL of the minifigure image'),
    bigquery.SchemaField('rebrickable_url', 'STRING', mode='NULLABLE', description='URL of the minifigure on Rebrickable'),
    bigquery.SchemaField('set_count', 'INTEGER', mode='NULLABLE', description='Number of sets the minifigure appears in'),
    bigquery.SchemaField('year_introduced', 'INTEGER', mode='NULLABLE', description='Year the minifigure first appeared in a set'),
    bigquery.SchemaField('last_updated', 'TIMESTAMP', mode='REQUIRED', description='Timestamp when the record was created in the warehouse')
]

SCHEMA_SET_PERFORMANCE = [
    
]
