'''
Shared base class with retry logic, rate limiting, and error handling
'''

import time
import logging
import json
from typing import Optional, Union
from abc import ABC, abstractmethod

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from datetime import datetime
from storage.lake import upload_from_string
from config.settings import settings

class BaseIngestion(ABC):
    '''
    Base class for all ingestion modules with retry logic, rate limiting, and error handling.
    '''

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.logger = logging.getLogger(f'ingestion.{source_name}')
        self.rate_limit = 0.5 # default delay
        self.setup_session()
    
    def setup_session(self):
        '''Configure HTTP session with retry logic'''
        self.session = requests.Session()

        # Retry configuration
        retry_strategy = Retry(
            total=settings.MAX_RETRIES,
            backoff_factor=settings.RETRY_BACKOFF,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )

        # Mount adapter to session
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        # Set default headers(used to identify ourselves when scraping for data on websites, helps to avoid being blocked)
        self.session.headers.update({
            'User-Agent': f'LegoInvestments/1.0 ({self.source_name})'
        })

        self.logger.info('HTTP session configured with retry strategy')

    def make_request(self, url: str, params: Optional[dict] = None, method: str = 'GET', parse_json:bool = True) -> Optional[Union[dict, str]]:
        # Avoid spamming request to the server and have a delay between requests 
        time.sleep(self.rate_limit) 

        # Creating request
        response = self.session.get(url, params=params)

        # Response validation
        if(response.status_code != 200):
            self.logger.error(f'Failed to fetch data from {url}. Status code: {response.status_code}')
            return None
        if(parse_json):
            return response.json()
        else:
            return response.text
    def upload_to_lake(self, data, gcs_path):
        # Convert data to JSON string before uploading to GCS
        json_data = json.dumps(data)

        # Upload JSON string to GCS
        upload_from_string(json_data, gcs_path)

        self.logger.info(f'Uploaded data to GCS at gs://lego-investment-lake/{gcs_path}')

    def get_gcs_path(self, data_type: str) -> str:
        '''
        Generates a standardized file path for saving data in GCS by date.
        '''
        current_date = datetime.now().strftime('%Y-%m-%d')

        return f"rawFiles/{self.source_name}/{data_type}/{current_date}.json"    
    
    @abstractmethod
    def ingest(self):
        '''
        Main method to run injestion process
        '''
        pass

