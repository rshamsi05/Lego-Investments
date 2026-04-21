import time
from typing import List, Dict, Optional

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
        self.rate_limit = 2.0 # 2 seconds between requests
        self.logger.info("BrickLinkIngestion initialized")
    
    
    def fetch_set_price_history(self, set_num):
        URL = f"{self.base_url}/catalogPG.asp?S={set_num}"
        html = self.make_request(URL, parse_json=False)
        if(not html):
            return []
        # What is the point of soup?
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table', class_='fv')
        rows_data = []

        for row in table.find_all('tr')[1:]:
            cells = row.find_all('td')
            if (len(cells) >= 6):
                #Extracting text
                type_text = cells[0].get_text(strip=True)
                min_price = cells[1].get_text(strip=True)

                # Validation check for headers
                if(type_text in ["New", "Used", "Current Items for Sale"] or "Qty" in str(row.get('qty'))):
                    continue
                    
                labels = ["Min Price", "Avg_Price", "Qty", "Time Sold"]
                if any(lable in type_text for lable in labels):
                    continue
                    
                if(not any(char.isdigit() for char in min_price)):
                    continue
            
                # Creating the row to insert
                row_dict = {
                    'type': type_text,
                    'min_price': min_price,
                    'avg_price': cells[2].get_text(strip=True),
                    'max_price': cells[3].get_text(strip=True),
                    'qty': cells[4].get_text(strip=True),
                    'date': cells[5].get_text(strip=True)
                }
                rows_data.append(row_dict)
        return rows_data

    def ingest(self, set_numbers):
        for set_num in set_numbers:
            price_data = self.fetch_set_price_history(set_num)
            if(price_data):
                gcs_path = self.get_gcs_path(f"prices/{set_num}")
                self.upload_to_lake(price_data, gcs_path)
                self.logger.info(f"Ingested price history for set {set_num} to GCS at {gcs_path}")
            time.sleep(self.rate_limit)

    
    
