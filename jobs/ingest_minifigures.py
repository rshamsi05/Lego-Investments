import logging
import time
from storage.queries import run_query
from ingestion.rebrickable import RebrickableIngestion

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("minifig_job")

def get_all_set_ids():
    '''
    Fetches every set_id from warehouse list
    '''
    query = "SELECT set_id FROM `lego-investments.lego_staging.stg_sets`"
    results = run_query(query)
    return [r['set_id'] for r in results]

def run_minifig_ingestion():
    # Retrieve set Ids
    set_ids = get_all_set_ids()
    logger.info(f"Found {len(set_ids)} sets to process.")
    # Initialize ingestor
    ingestor = RebrickableIngestion()
    # Starting bulk ftch
    ingestor.ingestMinifigures(set_ids)
    logger.info(" Full minifigure ingestion complete.")

if __name__ == "__main__":
    run_minifig_ingestion()
