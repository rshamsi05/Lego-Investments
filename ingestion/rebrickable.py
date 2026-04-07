'''
Pulls set, part, and minifigure data from Rebrickable API.
'''

import logging
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
        params = {
            "theme_id": 18 # Star Wars Theme
        }
        response = self.make_request(URL, params=params)
        if(response):
            return response.get('results')
        else:
            return []
    
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
    


