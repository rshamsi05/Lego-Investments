'''
Reusable query helpers for common lookups and checks
'''

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging
from config.settings import settings

logger = logging.getLogger('storage.queries')

def get_client() -> bigquery.Client:
    '''Get a BigQuery client using the configured project ID'''
    if(settings.GOOGLE_APPLICATION_CREDENTIALS):
        return bigquery.Client.from_service_account_json(
            settings.GOOGLE_APPLICATION_CREDENTIALS
        )
    return bigquery.Client(project=settings.GCP_PROJECT_ID)

def run_query(query: str, use_legacy_sql: bool = False) -> list[dict]:
    '''
    Execute a SQL query and return results as a list of dictionaries.

    Args:
        query: SQL query string
        use_legacy_sql: Use legacy SQL syntax(default: False)
    Returns:
        List of row dictionaries
    '''
    client = get_client()
    job_config = bigquery.QueryJobConfig(use_legacy_sql=use_legacy_sql)
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    return [dict(row) for row in results]

def run_query_to_dataframe(query: str, use_legacy_sql: bool = False):
    '''
    Execute a SQL query and return results as a pandas DataFrame.

    Args:
        query: SQL query string
        use_legacy_sql: Use legacy SQL syntax(default: False)
    Returns:
        pandas DataFrame with query results
    ''' 
    client = get_client()
    job_config = bigquery.QueryJobConfig(use_legacy_sql=use_legacy_sql)
    query_job = client.query(query, job_config=job_config)
    return query_job.result().to_dataframe()

def table_exists(table_name: str, dataset_id: str | None = None) -> bool: 
    '''
    Check if a table exists in BigQuery.

    Args:
        table_name: Name of the table
        dataset_id: Dataset ID (defaults to settings.GCP_BQ_DATASET)
    Returns:
        True if table exists, False otherwise
    '''
    client = get_client()
    dataset = dataset_id or settings.GCP_BQ_DATASET
    table_id = f"{client.project}.{dataset}.{table_name}"

    try:
        client.get_table(table_id)
        return True
    except NotFound:
        logger.debug(f"Table {table_id} does not exist.")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking table '{table_id}': {e}")
        raise

def get_table_schema(table_name: str, dataset_id: str | None = None) -> list[dict]:
    '''
    Get the schema of an existing table

    Args:
        table_name: Name of the table
        dataset_id: Dataset ID (defaults to settings.GCP_BQ_DATASET)
    
    Returns:
        List of schema field dictionaries
    '''
    client = get_client()
    dataset = dataset_id or settings.GCP_BQ_DATASET
    table_id = f"{client.project}.{dataset}.{table_name}"

    table = client.get_table(table_id)
    # for each column in the table schema, return a dictionary with the name, type, mode, and description of the column
    return [
        {
            'name': field.name,
            'type': field.field_type,
            'mode': field.mode,
            'description': field.description
        }
        # what does this do?
        for field in table.schema
    ]

def insert_rows(
    table_name: str,
    # how is the row represented?
    rows: list[dict],
    dataset_id: str | None = None
) -> list[dict]:
    '''
    Insert rows into a BigQuery table.

    Args:
        table_name: Name of the table
        rows: List of row dictionaries to insert
        dataset_id: Dataset ID (defaults to settings.GCP_BQ_DATASET)
    Returns:
        List of error dictionaries (empty if successful)
    '''
    client = get_client()
    dataset = dataset_id or settings.GCP_BQ_DATASET
    table_id = f"{client.project}.{dataset}.{table_name}"

    errors = client.insert_rows_json(table_id, rows)
    return errors

def create_table(
    table_name: str,
    schema: list[bigquery.SchemaField],
    dataset_id: str | None = None,
    description: str = ' '
) -> bool:
    '''
    Create a new table in BigQuery.

    Args:
        table_name: Name of the table
        schema: List of SchemaField objects,
        dataset_id: Dataset ID (defaults to settings.GCP_BQ_DATASET)
    Returns:
        True if created successfully 
    '''
    client = get_client()
    dataset = dataset_id or settings.GCP_BQ_DATASET
    table_id = f"{client.project}.{dataset}.{table_name}"

    table = bigquery.Table(table_id, schema=schema)
    table.description = description
    # bigQuery native create table method
    client.create_table(table)

    return True

def delete_table(table_name: str, dataset_id: str | None=None) -> bool:
    '''
    Delete a table from BigQuery
    
    Args:
        table_name: Name of the table
        dataset_id: Dataset ID (defaults to settings.GCP_BQ_DATASET)
    Returns:
        True if deleted successfully
    '''
    client = get_client()
    dataset = dataset_id or settings.GCP_BQ_DATASET
    table_id = f"{client.project}.{dataset}.{table_name}"
    # Not sure if I want this to throw an error or not 
    client.delete_table(table_id, not_found_ok=True)
    return True

# Is there a better name than truncate table it appears we are clearing a table
def truncate_table(table_name: str, dataset_id: str | None=None) -> bool:
    '''
    Delete all rows from a table (keep the table structure)
    
    Args:
        table_name: Name of the table
        dataset_id: Dataset ID (defaults to settings.GCP_BQ_DATASET)
    Returns:
        True if truncated successfully
    '''
    client = get_client()
    dataset = dataset_id or settings.GCP_BQ_DATASET
    query = f"DELETE FROM `{client.project}.{dataset}.{table_name}` WHERE TRUE"
    run_query(query)

    return True
