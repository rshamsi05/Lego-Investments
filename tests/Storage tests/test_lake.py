import pytest
import json
from pathlib import Path
from storage.lake import (
    get_client,
    upload_to_gcs,
    upload_from_string,
    download_from_gcs,
    list_gcs_files,
    delete_from_gcs,
)
from config.settings import settings

@pytest.fixture
def test_bucket():
    '''Use a test bucket or prefix to avoid polluting production data'''
    return settings.GCP_GCS_BUCKET

@pytest.fixture
def test_prefix():
    return 'test_files'

@pytest.fixture
def cleanup_files(test_bucket, test_prefix):
    '''Cleanup test files after test completes'''
    uploaded_paths = []
    
    yield lambda path: uploaded_paths.append(path)
    
    for path in uploaded_paths:
        try:
            delete_from_gcs(path, test_bucket)
        except:
            pass

class TestGCSOperations:
    '''Test Google Cloud Storage operations'''
    
    def test_get_client(self):
        '''Test that GCS client can be created'''
        client = get_client()
        assert client is not None
        assert client.project == settings.GCP_PROJECT_ID
    
    def test_upload_and_download_file(self, cleanup_files, test_bucket, test_prefix, tmp_path):
        '''Test uploading and downloading a file'''
        test_content = '{"test": "data", "value": 123}'
        local_file = tmp_path / "test_data.json"
        local_file.write_text(test_content)
        
        gcs_path = f"{test_prefix}/upload_test/test_data.json"
        result_uri = upload_to_gcs(str(local_file), gcs_path, test_bucket)
        
        assert f"gs://{test_bucket}/{gcs_path}" == result_uri
        cleanup_files(gcs_path)
        
        download_path = tmp_path / "downloaded_data.json"
        downloaded_path = download_from_gcs(gcs_path, str(download_path), test_bucket)
        
        assert Path(downloaded_path).exists()
        assert download_path.read_text() == test_content
    
    def test_upload_string(self, cleanup_files, test_bucket, test_prefix):
        '''Test uploading data from a string'''
        test_content = json.dumps({"message": "Hello from test", "timestamp": "2026-03-25"})
        gcs_path = f"{test_prefix}/string_test/data.json"
        
        result_uri = upload_from_string(test_content, gcs_path, test_bucket)
        
        assert f"gs://{test_bucket}/{gcs_path}" == result_uri
        cleanup_files(gcs_path)
    
    def test_list_files(self, cleanup_files, test_bucket, test_prefix):
        '''Test listing files with a prefix'''
        gcs_path = f"{test_prefix}/list_test/file1.json"
        upload_from_string('{"test": 1}', gcs_path, test_bucket)
        cleanup_files(gcs_path)
        
        files = list_gcs_files(f"{test_prefix}/list_test/", test_bucket)
        
        assert len(files) >= 1
        assert gcs_path in files
    
    def test_delete_file(self, cleanup_files, test_bucket, test_prefix):
        '''Test deleting a file'''
        gcs_path = f"{test_prefix}/delete_test/file.json"
        upload_from_string('{"test": "delete me"}', gcs_path, test_bucket)
        
        deleted = delete_from_gcs(gcs_path, test_bucket)
        assert deleted is True
        
        files = list_gcs_files(f"{test_prefix}/delete_test/", test_bucket)
        assert gcs_path not in files
    
    def test_delete_nonexistent_file(self, test_bucket, test_prefix):
        '''Test deleting a file that doesn't exist returns False'''
        result = delete_from_gcs(f"{test_prefix}/does_not_exist.json", test_bucket)
        assert result is False
