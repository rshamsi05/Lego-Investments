'''
Script to take in GCS JSON files and place them into Staging Tables within BigQuery
'''

import json, os
import time
from datetime import datetime

from storage.lake import download_from_gcs
from storage.queries import create_table, insert_rows, table_exists, truncate_table
from storage.schema import SCHEMA_STG_SETS

GCS_PATH = "rawFiles/rebrickable/sets/2026-04-13.json"
LOCAL_PATH = "temp/sets.json"
TABLE_NAME = "src_sets"

def main():
    print("Starting staging sets load process...")

    # Downloading file from GCS
    print("Downloading file from GCS...")
    download_from_gcs(GCS_PATH, LOCAL_PATH)


    # Parse JSON
    print("Parsing JSON file...")
    with open(LOCAL_PATH, 'r') as f:
        raw_data = json.load(f)
    
    # Transform Data (Mapping API fields to Schema Fields)
    print("Transforming data...")
    clean_data = []
    current_time = datetime.now().isoformat()

    for item in raw_data:
        # Mapping Rebrickable keys to our staging table schema
        row = {
            'set_id': item.get('set_num'),
            'name': item.get('name'),
            'year': item.get('year'),
            'theme_id': str(item.get('theme_id')),
            'piece_count': item.get('num_parts'),
            'rebrickable_url': str(item.get('set_url')),
            'theme_name': None,
            'subtheme_name': None,
            'subtheme_id': None,
            'retail_price_usd': None,
            'is_retired': False,
            'ingested_at': current_time
        }
        clean_data.append(row)
    
    # ---LOADING DATA INTO BIGQUERY---

    # check status of current table
    print(f"Checking table {TABLE_NAME} status...")

    # create table if it doesnt exist
    if(not table_exists(TABLE_NAME)):
        print(f"Table {TABLE_NAME} does not exist. Creating...")
        create_table(TABLE_NAME, SCHEMA_STG_SETS, description="Raw sets from Rebrickable")
        # Wait for Google metadata catalog to sync
        time.sleep(5)
    # truncate table if it does exist
    else:
        print(f"Table {TABLE_NAME} exists. Truncation existing rows...")
        truncate_table(TABLE_NAME)
    
    #Inserting rows
    print(f"Inserting {len(clean_data)} rows into {TABLE_NAME}...")
    errorsWhileInserting = insert_rows(TABLE_NAME, clean_data)

    if(not errorsWhileInserting):
        print(f"Successfully inserted {len(clean_data)} rows into {TABLE_NAME}!")
    else:
        print(f"Encountered errors while inserting rows: {errorsWhileInserting}")
    
    # Cleanup local file
    if os.path.exists(LOCAL_PATH):
        os.remove(LOCAL_PATH)
        print(f"Cleaned up local file {LOCAL_PATH}")

if __name__ == "__main__":
    main()
