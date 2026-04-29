import time
from typing import List, Dict, Optional
import random
import re
from bs4 import BeautifulSoup
from utils.cleaning import clean_int, clean_price
from ingestion.base import BaseIngestion
from config.settings import settings

class BrickLinkIngestion(BaseIngestion):
    '''
    Scrapes price history and listings from Bricklin
    '''

    def __init__(self):
        super().__init__(source_name='bricklink')
        self.base_url = settings.BRICKLINK_BASE_URL
        self.rate_limit = 2.0
        self.logger.info("BrickLinkIngestion initialized")
    
    
    def fetch_set_price_history(self, set_num):
        URL = f"{self.base_url}/catalogPG.asp?S={set_num}"
        html = self.make_request(URL, parse_json=False)
        if(not html):
            return []
        # HTML scraper to extract price data from page.
        soup = BeautifulSoup(html, 'html.parser')
        page_text = soup.get_text(separator=" ", strip=True)
        pattern = r"Times Sold:\s*([\d,]+)\s*Total Qty:\s*([\d,]+)\s*Min Price:\s*([^A]+)Avg Price:\s*([^Q]+)Qty Avg Price:\s*([^M]+)Max Price:\s*([^T|C|L]+)"
        
        matches = re.findall(pattern, page_text)

        final_data = []
        conditions = ['New', 'Used']

        for i, match in enumerate(matches[:2]):
            final_data.append({
                'type': conditions[i],
                'qty': match[1].replace(',', '').strip(),
                'min_price': match[2].strip(),
                'avg_price': match[3].strip(),
                'max_price': match[5].strip(),
                'date': "Last 6 Months"
            })

        if (not final_data):
            self.logger.warning(f"No summary price data extracted for {set_num}")
        return final_data        

    def ingest(self, set_numbers):
        for set_num in set_numbers:
            price_data = self.fetch_set_price_history(set_num)
            if(price_data):
                gcs_path = self.get_gcs_path(f"prices/{set_num}")
                self.upload_to_lake(price_data, gcs_path)
                self.logger.info(f"Ingested price history for set {set_num} to GCS at {gcs_path}")
            delay = random.uniform(4, 10)
            time.sleep(delay)

    
    
