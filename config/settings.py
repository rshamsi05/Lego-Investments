'''
Central configuration for Lego Investments data pipeline.
Loads environment variables and provides constants for all modules.
'''
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environments variables from .env file
load_dotenv()

class Settings:
    # GCP Configuration
    GCP_PROJECT_ID: str = os.getenv('GCP_PROJECT_ID', ' lego-investments')
    GCP_GCS_BUCKET: str = os.getenv('GCP_GCS_BUCKET', 'lego-investment-lake')
    GCP_BQ_DATASET: str = os.getenv('GCP_B0_DATASET', 'lego_staging')
    GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')

    # API Keys
    REBRICK_API_KEY: str = os.getenv('REBRICK_API_KEY', '')

    # Airflow Configuration
    AIRFLOW_UID: str = os.getenv('AIRFLOW_UID', '50000')
    AIRFLOW_ENV: str = os.getenv('AIRFLOW_ENV', 'local')

    # Paths
    BASE_DIR: Path = Path(__file__).resolve().parent.parent
    DAGS_DIR: Path = BASE_DIR / 'dags'
    CONFIG_DIR: Path = BASE_DIR / 'config'
    INGESTION_DIR: Path = BASE_DIR / 'ingestion'
    STORAGE_DIR: Path = BASE_DIR / 'storage'
    QUALITY_DIR: Path = BASE_DIR / 'quality'
    TRANSFORM_DIR: Path = BASE_DIR / 'transform'
    WAREHOUSE_DIR: Path = BASE_DIR / 'warehouse'

    # GCS Paths
    GCS_RAW_PREFIX: str = 'rawFiles'
    GCS_PROCESSED_PREFIX: str = 'processedFiles'

    # Rebrickable API
    REBRICKABLE_BASE_URL: str = 'https://rebrickable.com/api/v3/lego'
    REBRICKABLE_TIMEOUT: int = 30
    REBRICKABLE_RATE_LIMIT: float = 0.1  # seconds between requests

    # Bricklink Scraping
    BRICKLINK_BASE_URL: str = 'https://www.bricklink.com'
    BRICKLINK_TIMEOUT: int = 30
    BRICKLINK_RATE_LIMIT: float = 1.0  # seconds between requests (be respectful)

    # Lego.com Scraping
    LEGO_BASE_URL: str = 'https://www.lego.com'
    LEGO_TIMEOUT: int = 30
    LEGO_RATE_LIMIT: float = 0.5  # seconds between requests

    # Retry Configuration
    MAX_RETRIES: int = 3
    RETRY_BACKOFF: float = 2.0  # exponential backoff multiplier

    # Data Quality Thresholds
    MAX_NULL_RATE: float = 0.1  # 10% nulls allowed
    MIN_PRICE_USD: float = 0.0
    MAX_PRICE_USD: float = 10000.0
    MAX_DUPLICATE_RATE: float = 0.01  # 1% duplicates allowed

    # dbt Configuration
    DBT_PROFILES_DIR: Path = TRANSFORM_DIR

    @classmethod
    def validate(cls) -> None:
        '''Validate that all required settings are configured'''
        required = ['GCP_PROJECT_ID', 'GCP_GCS_BUCKET', 'REBRICK_API_KEY']
        # What does this do?
        missing = [attr for attr in required if not getattr(cls, attr)]

        if(missing):
            # How does the join work?
            raise ValueError(f'Missing required settings: {", ".join(missing)}. ' "Please check .env file.")
    
    @classmethod
    def is_production(cls) -> bool:
        # What does it mean to run in production?
        '''Check if running in production'''
        return cls.AIRFLOW_ENV == 'production'
    @classmethod
    def gcs_raw_path(cls, source: str, *parts: str) -> str:
        '''Build GCS path for raw files'''
        path_parts = [cls.GCS_RAW_PREFIX, source] + list(parts)
        return '/'.join(path_parts)
    @classmethod
    def gcs_processed_path(cls, destination: str, *parts: str) -> str:
        '''Build GCS path for processed files.'''
        path_parts = [cls.GCS_PROCESSED_PREFIX, destination] + list(parts)
        return '/'.join(path_parts)

# Singleton instance for easy access
settings = Settings()
