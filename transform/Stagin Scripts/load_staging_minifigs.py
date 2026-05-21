'''
Loads raw minifig data from GCS into BigQuery Tables
'''

import json, os
import time
from datetime import datetime
from storage.lake import list_gcs_files, download_from_gcs
from storage.queries import create_table, insert_rows, table_exists, truncate_table
from storage.schema import SCHEMA_STG_MINIFIGURES, SCHEMA_STG_SET_MINIFIGURES

GCS_PREFIX = "rawFiles/rebrickable/minifigures/"
TABLE_MINIFIGS = "src_minifigures"
TABLE_LINKS = "src_set_minifigures"


def main():
    print("Start minfig loading process")

    #Listing all minifig files in GCS
    files = list_gcs_files(GCS_PREFIX)
    print(f"Found {len(files)} to be loaded...")

    all_minifigs = []
    seen_minifigs_ids = set()
    all_links = []
    
    processsedCount = 0
    total_files = len(files)


    # Processing each file
    for gcs_path in files:

        # Skip folder marker
        if(gcs_path.endswith('/')):
            continue
            
        processsedCount += 1
        if(processsedCount % 50 == 0):
            print(f"Progress: {processsedCount}/{total_files} processed...")
            
        # extract set id
        parts = gcs_path.split('/')
        set_id = parts[-2]

        local_path = f"temp/minifigs_{set_id}.json"

        # Download file and parse
        download_from_gcs(gcs_path, local_path)

        if(not os.path.exists(local_path) or os.path.getsize(local_path) == 0):
            continue

        with open(local_path, 'r') as f:
            try:
                rawData = json.load(f)
            except json.JSONDecodeError as e:
                print(f"ERROR: Failed to parse JSON from {gcs_path}. Error: {e}")
                continue
                
        for item in rawData:
            fig_id = item.get('set_num')
            if(not fig_id):
                continue
            if(fig_id not in seen_minifigs_ids):
                minifig_row = {
                    'minifigure_id': fig_id,
                    'name': item.get('name') or f"Unknown Minifigure {fig_id}",
                    'num_parts': item.get('num_parts'),
                    'img_url': item.get('set_img_url'),
                    'rebrickable_url': f"https://rebrickable.com/minifigs/{fig_id}/",
                    'ingested_at': datetime.now().isoformat()
                }
                all_minifigs.append(minifig_row)
                seen_minifigs_ids.add(fig_id)
            # Junction table row
            junction_row = {
                'set_id': set_id,
                'minifigure_id': fig_id,
                'quantity': item.get('quantity', 1),
                'is_exclusive': None, # Calculated in dbt
                'ingested_at': datetime.now().isoformat()
            }
            all_links.append(junction_row)

        # Local file cleanup
        if os.path.exists(local_path):
            os.remove(local_path)


    # Loading Master table into BigQuery
    if(all_minifigs):
        print(f"Loading {len(all_minifigs)} unique minifigures into {TABLE_MINIFIGS}...")
        # if table doesnt exist we create it
        if(not table_exists(TABLE_MINIFIGS)):
            print(f"Table {TABLE_MINIFIGS} does not exist. Creating...")
            create_table(TABLE_MINIFIGS, SCHEMA_STG_MINIFIGURES)
            time.sleep(5)
        else:
            print(f"Table {TABLE_MINIFIGS} already exists. Truncating table")
            truncate_table(TABLE_MINIFIGS)
        errorsWhileInserting = insert_rows(TABLE_MINIFIGS, all_minifigs)
        if errorsWhileInserting: print(f"Errors in {TABLE_MINIFIGS}: {errorsWhileInserting}")
        else: print(f"Successfully loaded {TABLE_MINIFIGS} data!")
    
    if(all_links):
        print(f"Loading {len(all_links)} unique links into {TABLE_LINKS}...")
        # if table doesnt exist we create it
        if(not table_exists(TABLE_LINKS)):
            print(f"Table {TABLE_LINKS} does not exist. Creating...")
            create_table(TABLE_LINKS, SCHEMA_STG_SET_MINIFIGURES)
            time.sleep(5)
        else:
            print(f"Table {TABLE_LINKS} already exists. Truncating table")
            truncate_table(TABLE_LINKS)
        errorsWhileInserting = insert_rows(TABLE_LINKS, all_links)
        if errorsWhileInserting: print(f"Errors in {TABLE_LINKS}: {errorsWhileInserting}")
        else: print(f"Successfully loaded {TABLE_LINKS} data!")
    
if __name__ == "__main__":
    main()
