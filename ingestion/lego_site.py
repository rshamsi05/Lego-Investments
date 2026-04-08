'''
Scrapes current retail prices and availability from Lego.com
'''

import logging
import time
from typing import List, Dict, Optional

from bs4 import BeautifulSoup
from ingestion.base import BaseIngestion
from config.settings import settings

class LegoSiteIngestion(BaseIngestion):
    '''
    Scrapes retail prices and availability from Lego.com
    '''
    def __init__(self):
        super().__init__(source_name='lego_site')
        self.base_url = "https://www.lego.com"
        self.rate_limit = 1.5 # seconds between each request
        self.logger.info("LegoSiteIngestion initialized")
    
    def fetch_set_details(self, set_num):
        URL = f"{self.base_url}/en-us/catalog/products/star-wars?q={set_num}"
        html = self.make_request(URL, parse_json=False)
        if(not html):
            return None
        soup = BeautifulSoup(html, 'html.parser')
        product_title = soup.find('div', class_='product-title')
        if(product_title):
            # extract title and URL
            link_tag = product_title.find('a')
            title = link_tag.get_text(strip=True) if link_tag else None
            url = link_tag['href'] if link_tag and link_tag.has_attr('href') else None

            # Extract Price (usually found in a <span> with class price)
            price_tag = product_title.find('span', class_='price')
            price = price_tag.get_text(strip=True) if price_tag else None

            return {
                'title': title,
                'price': price,
                'url': f"https://www.lego.com{url}" if url else None
            }
        else:
            return None

    def ingest(self, set_numbers):
        for set_num in set_numbers:
            details = self.fetch_set_details(set_num)
            if(details):
                gcs_path = self.get_gcs_path(f"availability/{set_num}")
                self.upload_to_lake(details, gcs_path)
                self.logger.info(f"Ingested details for set {set_num} to GCS at {gcs_path}")
            time.sleep(self.rate_limit)
        
    
