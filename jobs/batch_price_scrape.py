import logging
import time
import random
from storage.queries import run_query
from ingestion.bricklink import BrickLinkIngestion


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("batch_scraper")


def get_pending_sets(limit=20):
    '''
    Find sets that dint have price data yet
    '''
    query = f"""
        SELECT s.set_id
        FROM `lego-investments.lego_staging.stg_sets` s
        LEFT JOIN `lego-investments.lego_staging.stg_prices` p ON s.set_id = p.set_id
        WHERE p.set_id IS NULL
        LIMIT {limit} 
    """
    results = run_query(query)
    return [r['set_id'] for r in results]
def run_batch():
    '''
    Main function to ingest and scrape data
    '''
    batch_size = 20
    sets_to_scrape = get_pending_sets(limit=batch_size)
    if(not sets_to_scrape):
        logger.info("No pending sets found. All prices are up to date")
        return
    logger.info(f"Starting batch scrape for {len(sets_to_scrape)} sets: {sets_to_scrape}")

    # Initializng ingestor
    ingestor = BrickLinkIngestion()
    
    # Scrape price data for each set and upload to GCS
    ingestor.ingest(sets_to_scrape)

    logger.info("Batch ingestion complete. Files are in GCS")
    print(" Run load_staging_prices script to move into BigQuery" )

if __name__ == "__main__":
    run_batch()

