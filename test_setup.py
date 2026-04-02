import logging
import logging.config
import yaml
from pathlib import Path

# Testing logging loads
print("Testing logging.yml:")
with open('config/logging.yml', 'r') as f:
    config = yaml.safe_load(f)
logging.config.dictConfig(config)
logger = logging.getLogger('storage.lake')
logger.info("Loggng works!")
print("Logging is ok!")

# Testing settings loads
print("\nTesting settings.py:")
from storage.lake import get_client
client = get_client()
print(f"Connected to project: {client.project}")
print("GCS Connection is OK")

# Testing bucket access
print("\nTesting bucket access:")
from storage.lake import list_gcs_files
files = list_gcs_files('')
print(f"Found {len(files)} files in GCS bucket.")
print("Bucket access is OK")

print("All Tests pass!")
