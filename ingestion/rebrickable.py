'''
Pulls set, part, and minifigure data from Rebrickable API.
'''

import logging
import time
from typing import List, Dict, Optional

from ingestion.base import BaseIngestion
from config.settings import settings

class RebrickableIngestion(BaseIngestion):
    '''
    Ingests sets, parts, and minifigures from Rebrickable API
    '''
    def __init__(self):
        super().__init__(source_name='rebrickable')
        self.api_key = settings.REBRICK_API_KEY
        self.base_url = settings.REBRICKABLE_BASE_URL

        # each request will have the api key in the header for authentication
        self.session.headers.update({
            'Authorization': f'key {self.api_key}'
        })

        self.logger.info("RebrickableIngestion initialized")
    
    def fetch_sets(self):
        URL = f"{self.base_url}/sets/"
        all_sets = []
        page = 1
        # Loops until there is no next page URL
        while(True):
            params = {"theme_id": 209, "page": page} # Star Wars Theme
            response = self.make_request(URL, params=params)
            if(response is None):
                print("Request failed! Stopping.")
                break

            if(response.get('results')):
                # Add results to all_sets list, if 'results' key is missxing it defaults to an empty list
                all_sets.extend(response.get('results', []))
                print(f"Fetched {len(all_sets)} sets so far... (Total available: {response.get('count')})")
                # if there is no next page, we are done fetching sets and can break the loop
                next_page = response.get('next')
                if(not next_page):
                    print("No more pages to fetch. Finished fetching sets.")
                    break
                page += 1
            else:
                print("No results found on page {page}. Stopping.")
                break
        return all_sets

    def ingestMinifigures(self, set_nums):
        '''
        Fetches minifigures for a list of set numbers and uploads to GCS
        '''
        self.logger.info(f"Starting bulk minifigure ingestion for {len(set_nums)} sets")

        for set_num in set_nums:
            # Fetch from rebrickable API
            minifigs = self.fetch_set_minifigs(set_num)
            if(minifigs):
                # Generate path to upload to GCS
                gcs_path = self.get_gcs_path(f"minifigures/{set_num}")
                # Upload to GCS lake
                self.upload_to_lake(minifigs, gcs_path)
                self.logger.info(f"Ingested {len(minifigs)} minifigs for set {set_num}")
            # apply rate limit(5 requests per second)
            time.sleep(0.2)

    def ingest(self):
        sets = self.fetch_sets()
        if(not sets):
            self.logger.error("Failed to fetch sets from Rebrickable")
            return
        gcs_path = self.get_gcs_path('sets')
        self.upload_to_lake(sets, gcs_path)
        self.logger.info(f"Ingested {len(sets)} sets from Rebrickable to GCS at {gcs_path}")
    
    def fetch_set_parts(self, set_num):
        URL = f"{self.base_url}/sets/{set_num}/parts/"
        response = self.make_request(URL)
        if(response):
            return response.get('results')
        else:
            return []
    
    def fetch_set_minifigs(self, set_num):
        URL = f"{self.base_url}/sets/{set_num}/minifigs/"
        response = self.make_request(URL)
        if(response):
            return response.get('results')
        else:
            return []
    


