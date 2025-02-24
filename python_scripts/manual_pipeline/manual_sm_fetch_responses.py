import requests 
import logging
import json
from api_library import SMConnect, StorageConnect


def fetch_responses(date):
    """Fetch all responses to survey since the specified date"""
    # Initalize the survey monkey api
    sm_conn = SMConnect()

    # Query bigquery for the lastest date, then format the resulting datetime object as string
    date = date.strftime('%Y-%m-%dT%H:%M:%S') 
    
    # Initialize list to hold pages of data from api call
    data_list = []
    
    # create URL for api call
    url = f'{sm_conn.api_url}/surveys/{sm_conn.survey_id}/responses/bulk?per_page={100}&start_modified_at={date}'
    
    while url:
        response = requests.get(url, headers=sm_conn.headers)
        if response.status_code == 200:
            data = response.json()
            data_list.extend(data['data'])
            
            # get the number of records fetched so far
            n_fetched = len(data_list)
            logging.info(f'Survey records fetched so far:{n_fetched}')
            
            # SurveyMonkey response provides a link to call the next page. 
            # If a next page doesn't exist, the object 'url' will not exist, stopping the while loop
            url = data.get("links", {}).get("next") 
        else:
            logging.error(f'Failed to get responses, status: {response.status_code}')
            raise
    
    logging.info(f'Total response records fetched: {len(data_list)}')
    
    # Iinitalize GCP Storage connection
    storage_conn = StorageConnect()

    try:
        logging.info(f"Uploading json string to GCP Storage blob: {storage_conn.sm_raw_responses_blob}")
        
        # upload file to newly created blob
        storage_conn.sm_raw_responses_blob.upload_from_string(json.dumps(data_list), content_type="application/json")
    
         # Verify the file exists in the bucket
        if not storage_conn.sm_raw_responses_blob.exists():
            raise FileNotFoundError(f"Verification failed: {storage_conn.sm_raw_responses_blob} was not found in GCP Storage.")
        
        logging.info(f"Successfully uploaded json string to blob:{storage_conn.sm_raw_responses_blob} in bucket:{storage_conn.raw_data_bucket.name}")
        
    except Exception as e:
        logging.error(f"Upload failed: {e}")
        raise 
        


# testing static date
from datetime import datetime

# Given datetime string
dt_string = '2024-12-11T21:10:18'

# Convert string to datetime object
date = datetime.strptime(dt_string, '%Y-%m-%dT%H:%M:%S')


fetch_responses(date)