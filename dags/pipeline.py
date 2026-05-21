from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1), # YEAR, MONTH, DAY
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'lego_star_wars_pipeline',
    default_args=default_args,
    description='Full ELT pipeline for Lego Star Wars Investment Analysis',
    schedule_interval='@daily', # Runs once a day
    catchup=False
) as dag:
    
    # Extraction (API -> GCS)
    extract_rebrickable = BashOperator(
        task_id ='extract_rebrickable',
        bash_command='cd /opt/airflow && PYTHONPATH=. python ingestion/rebrickable.py'
    )

    extract_bricklink = BashOperator(
        task_id='extract_bricklink',
        bash_command='cd /opt/airflow && PYTHONPATH=. python ingestion/bricklink.py'
    )

    # Loading to Staging(GCS -> BigQuery Raw)
    load_sets = BashOperator(
        task_id='loading_sets_to_staging',
        bash_command='cd /opt/airflow && PYTHONPATH=. python "transform/Stagin Scripts/load_staging_sets.py"'
    )

    load_minifigs = BashOperator(
        task_id='load_minifigs_to_staging',
        bash_command='cd /opt/airflow && PYTHONPATH=. python "transform/Stagin Scripts/load_staging_minifigs.py"'
    )

    load_prices = BashOperator(
        task_id='load_prices_to_staging',
        bash_command='cd /opt/airflow && PYTHONPATH=. python "transform/Stagin Scripts/load_staging_prices.py"'
    )

    # Transformation(BigQuery Raw -> Warehouse)
    run_dbt = BashOperator(
        task_id='run_dbt_transformations',
        bash_command='cd /opt/airflow/transform && dbt run --project-dir .'
    )

    extract_rebrickable >> [load_sets, load_minifigs]
    extract_bricklink >> load_prices

    [load_sets, load_minifigs, load_prices] >> run_dbt

