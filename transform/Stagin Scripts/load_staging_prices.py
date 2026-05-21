'''
Loads raw price data from GCS into BigQuery Staging Tables
'''

import json, os
import time
from datetime import datetime
from storage.lake import list_gcs_files, download_from_gcs
from storage.queries import create_table, insert_rows, table_exists, truncate_table
from storage.schema import SCHEMA_STG_PRICES
from utils.cleaning import clean_price, clean_int

GCS_PREFIX = "rawFiles/bricklink/prices/"
TABLE_NAME = "src_prices"


def main():
    print("Starting Price Load Process")

    # Listing all price files in GCS
    files = list_gcs_files(GCS_PREFIX)
    print(f"Found {len(files)} to be loaded...")

    all_rows = []
    
    # Processing each file
    for gcs_path in files:
        # extract set number from the path
        parts = gcs_path.split('/')
        set_number = parts[-2]

        local_path = f"temp/price_{set_number}.json"

        # Download file and parse
        download_from_gcs(gcs_path, local_path)

        if(os.path.getsize(local_path) == 0):
            print(f"ALERT: Downloaded file {gcs_path} is empty. Skipping.")
            continue
        with open(local_path, 'r') as f:
            content = f.read()
            try:
                rawData = json.loads(content)
            except json.JSONDecodeError as e:
                print(f"ERROR: Failed to parse JSON from {gcs_path}. Error: {e}")
                continue

        for row in rawData:
            # Clean data and map fields
            cleaned_row = {
                'set_id': set_number,
                'observed_date': datetime.now().date().isoformat(),
                'avg_price_usd': clean_price(row.get('avg_price')),
                'min_price_usd': clean_price(row.get('min_price')),
                'max_price_usd': clean_price(row.get('max_price')),
                'listing_count': clean_int(row.get('qty')),
                'source': 'Bricklink',
                'ingested_at': datetime.now().isoformat()
            }
            all_rows.append(cleaned_row)
        
        # Clean up local file inside loop
        if(os.path.exists(local_path)):
            os.remove(local_path)

    # -- Loading data into BigQuery (BATCH LOAD) --
    if all_rows:
        print(f"Checking status of {TABLE_NAME}...")

        if(not table_exists(TABLE_NAME)):
            print(f"Table {TABLE_NAME} does not exist. Creating...")
            create_table(TABLE_NAME, SCHEMA_STG_PRICES)
            time.sleep(5)
        else:
            print(f"Table {TABLE_NAME} already exists. Truncating...")
            truncate_table(TABLE_NAME)
        
        # Inserting rows 
        print(f"Inserting {len(all_rows)} rows into {TABLE_NAME}...")
        errorsWhileInserting = insert_rows(TABLE_NAME, all_rows)

        if(not errorsWhileInserting):
            print(f"Successfully loaded price data into {TABLE_NAME}")
        else:
            print(f"Errors while inserting into {TABLE_NAME}: {errorsWhileInserting}")

if __name__ == "__main__":
    main()
