'''
Functions to upload, download, and list files in GCS buckets
'''

# Source - https://stackoverflow.com/a/41710678
# Posted by user1311888
# Retrieved 2026-03-07, License - CC BY-SA 3.0

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

credentials = GoogleCredentials.get_application_default()
service = discovery.build('storage', 'v1', credentials=credentials)

filename = '/Users/rayanshamsi/Documents/themes.csv'
bucket = 'lego-investment-lake'

body = {'name': 'themes.csv'}
req = service.objects().insert(bucket=bucket, body=body, media_body=filename)
resp = req.execute()
