'''
CURRENTLY DECIDING TO SKIP THIS IN FAVOR OF USING JUST REBRICKABLE AND BRICKLINK AS WE NEED TO FIGURE OUT A WAY TO SCRAPE LEGO.COM, Maybe use curl CFFI?
Scrapes current retail prices and availability from Lego.com
'''

import time
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
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        })
        self.logger.info("LegoSiteIngestion initialized")
    
    
    def fetch_set_details(self, set_num):
        URL = f"https://www.lego.com/en-us/service/products?query={set_num}"

        # Fetching JSON directly
        response = self.make_request(URL, parse_json=True)
        if(response and 'products' in response and len(response['products']) > 0):
            product = response['products'][0]
            return {
                'title': product.get('title'),
                'price': product.get('price', {}).get('value'), # if price is missing it returns an empty dictionary and returns None because value is not in the empty dictionary
                'currency': product.get('price', {}).get('currency'), # same principle applies above with currency.
                'url': f"https://www.lego.com{product.get('url')}",
                'availability': product.get('availability')
            }
        return None

    def ingest(self, set_numbers):
        for set_num in set_numbers:
            details = self.fetch_set_details(set_num)
            if(details):
                gcs_path = self.get_gcs_path(f"availability/{set_num}")
                self.upload_to_lake(details, gcs_path)
                self.logger.info(f"Ingested details for set {set_num} to GCS at {gcs_path}")
            time.sleep(self.rate_limit)
        
    
