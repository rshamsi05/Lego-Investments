'''
Functions to upload, download, and list files in GCS buckets
'''

import os
from pathlib import Path
from typing import Optional
from google.cloud import storage
from config.settings import settings

def get_client() -> storage.Client:
    '''Get GCS client'''
    # if the google credentials are set in the environment use it to create the client, otherwise use the default credentials defined in GCP
    if(settings.GOOGLE_APPLICATION_CREDENTIALS):

        return storage.Client.from_service_account_json(
            settings.GOOGLE_APPLICATION_CREDENTIALS
        )
    return storage.Client(project=settings.GCP_PROJECT_ID)

def upload_to_gcs(
    local_path:str,
    gcs_path:str,
    bucket_name: Optional[str] = None
) -> str:
    '''
    Upload local file to GCS

    Args:
        local_path: Path to local file
        gcs_path: Path in GCS bucket (e.g. 'rawFiles/rebrickable/sets/2026-03-08.json)
        bucket_name: bucket name (optional, defaults to settings.GCP_GCS_BUCKET)
    Returns:
        Upload a local file to GCS (gs://bucket_name/gcs_path)
    '''
    client = get_client()
    bucket = client.bucket(bucket_name or settings.GCP_GCS_BUCKET)
    blob = bucket.blob(gcs_path)

    blob.upload_from_filename(local_path)

    return f"gs://{bucket.name or settings.GCP_GCS_BUCKET}/{gcs_path}"

def upload_from_string(
    data:str,
    gcs_path:str,
    # how does the application/json get defined? 
    content_type: str = 'application/json',
    bucket_name: Optional[str] = None
) -> str:
    '''
    Upload string data directly to GCS.

    Args:
        data: String content to upload
        gcs_path: Destination path in GCS
        content_type: MIME type of content
        bucket_name: Bucket name (defaults to settings.GCP_GCS_BUCKET)
    
    Returns:
        GCS URI to uploaded file
    '''
    client = get_client()
    bucket = client.bucket(bucket_name or settings.GCP_GCS_BUCKET)
    blob = bucket.blob(gcs_path)

    blob.upload_from_string(data, content_type=content_type)

    return f"gs://{bucket.name or settings.GCP_GCS_BUCKET}/{gcs_path}"


def download_from_gcs(
    gcs_path:str,
    local_path:str,
    bucket_name: Optional[str] = None
) -> str:
    '''
    Download a file from GCS to local storage

    Args:
        gcs_path: path in GCS (e.g. 'rawFiles/rebrickable/sets/2026-03-08.json) without prefix
        local_path: Path to save the downloaded file locally
        bucket_name: bucket name (optional, defaults to settings.GCP_GCS_BUCKET)
    Returns:
        local path where file was downloaded
    '''
    client = get_client()
    bucket = client.bucket(bucket_name or settings.GCP_GCS_BUCKET)
    blob = bucket.blob(gcs_path)

    # Create parent directories if they don't exist
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)

    blob.download_to_filename(local_path)

    return local_path

def list_gcs_files(
    prefix: str,
    bucket_name: Optional[str] = None
) -> list[str]:
    '''
    List files in GCS with a given prefix

    Args:
        prefix: Path prefix to filter by (e.g., 'rawFiles/rebrickable/sets/')
        bucket_name: bucket name (optional, defaults to settings.GCP_GCS_BUCKET)
    Returns:
        List of blob names matching the prefix
    '''
    client = get_client()
    bucket = client.bucket(bucket_name or settings.GCP_GCS_BUCKET)
    blobs = bucket.list_blobs(prefix=prefix)

    return [blob.name for blob in blobs]

def delete_from_gcs(
    gcs_path: str,
    bucket_name: Optional[str] = None
) -> bool:
    '''
    Delete a file from GCS

    Args:
        gcs_path: Path in GCS(without prefix) to delete (e.g. 'rawFiles/rebrickable/sets/2026-03-08.json)
        bucket_name: bucket name (optional, defaults to settings.GCP_GCS_BUCKET)
    Returns:
        True if deleted, False if file did not exist
    '''
    client = get_client()
    bucket = client.bucket(bucket_name or settings.GCP_GCS_BUCKET)
    blob = bucket.blob(gcs_path)

    if(blob.exists()):
        blob.delete()
        return True
    return False
