import os
import logging
import json
import requests
from datetime import timedelta
from google.cloud import bigquery
from google.cloud import storage
from pathlib import Path

logging.basicConfig(level=logging.INFO)

# Define the GCPConnect parent class
class GCPConnect:
    # Initialize class attributes as 'None' --> will be set when first CGPConnect object is initialized
    # ' _ ' is used before attributes here to indicate 'internal' usage 
    _gcp_cred_filepath = None
    _gcp_cred_dict = None
    project_id = None
    
    
    def __init__(self):
        # Here, if the class attribute doesn't exist, we pass the result of the instance's (self) call of 
        # the function generating the needed value to the class attribute 
        if GCPConnect._gcp_cred_filepath is None:
            GCPConnect._gcp_cred_filepath = self.fetch_gcp_cred_filepath()
        if GCPConnect._gcp_cred_dict is None:
            GCPConnect._gcp_cred_dict = self.load_gcp_cred_json()
        if GCPConnect.project_id is None:
            GCPConnect.project_id = self.get_project_id()

    def fetch_gcp_cred_filepath(self):
        """Fetches the GCP credentials file path from env variable or defaults to a local path."""
        env_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

        if env_path:
            file_path = Path(env_path)
            logging.info(f"GCP credentials found in environment variable: {file_path}")
        else:
            # Resolve the path from the root of the project
            file_path = Path(__file__).resolve().parents[2] / "gcp_service_account.json"
            logging.warning("GOOGLE_APPLICATION_CREDENTIALS not set. Using local file.")

        # Ensure the file exists
        if not file_path.exists():
            raise FileNotFoundError(f"GCP credentials file not found: {file_path}")

        return file_path.resolve()

    def load_gcp_cred_json(self):
        """Loads the GCP service account JSON into a dictionary."""
        try:
            with open(GCPConnect._gcp_cred_filepath) as file:
                data_dict = json.load(file)
            logging.info("GCP credentials JSON loaded successfully.")
            return data_dict
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid GCP credentials JSON format: {e}")
        except Exception as e:
            raise ValueError(f"Unable to load GCP credentials JSON: {e}")
        
    def get_project_id(self):
        """Retrieves the project ID, first from credentials, then from metadata."""
        if GCPConnect._gcp_cred_dict:
            project_id = GCPConnect._gcp_cred_dict.get('project_id')
            if project_id:
                logging.info(f"Using project ID from JSON credentials: {project_id}")
                return project_id
        
        else: 
            try:
                metadata_url = 'http://metadata.google.internal/computeMetadata/v1/project/project-id' # url to GCP project's project_id
                headers = {"Metadata-Flavor": "Google"} # set headers for API call
                response = requests.get(metadata_url, headers=headers)
                response.raise_for_status() # Raises an HTTPError for bad responses (4xx, 5xx)
                
                logging.info(f'Successfully fetched project_id from metadata service: {response.text}')
                return response.text
            except requests.RequestException as e:
                logging.error(f"Error fetching project ID from metadata service: {e}")
                raise RuntimeError("Could not determine project ID in Cloud Run environment")
        


# Define the BQConnect child class
class BQConnect(GCPConnect):
    def __init__(self):
        super().__init__()
        self.dataset_pipeline = f'{self.project_id}.pipeline'
        self.client = self.create_client()
                
    def create_client(self):
        """Initializes and returns a BigQuery client."""

        # Initialize the BigQuery Client 
        client = bigquery.Client()
        return client

    def fetch_latest_response_date(self):
        """Fetches the latest response date from BigQuery and adds 1 second."""
        try:
            # Query the table to find the maximum date present 
            query = """
            select
                max(date) as latest_date
            from 
                `dbt-test-449821.pipeline.raw_sm_responses`
            """
            # assign the results of the query to a dataframe
            df = self.client.query(query).to_dataframe()

            # pull out the 1st row and 1st column,  
            latest_date = df.iloc[0,0] 
            
            # add 1 second to the resulting time to not pull the exact same record repeatdly
            latest_date += timedelta(seconds=1)
            
            logging.info(f'latest response date: {latest_date}')

            return latest_date
        
        except Exception as e:
            logging.error(f'Error fetching latest response date: {e}')
            raise
        
    
# Define the StorageConnect child class 
class StorageConnect(GCPConnect):
    def __init__(self):
        super().__init__()
        self.client = self.create_client()
        self.raw_data_bucket = self.client.get_bucket('airflow_raw_source_data')
        self.sm_raw_responses_blob = self.raw_data_bucket.blob('sm_raw_responses_string')
        self.sm_normalized_responses_blob = self.raw_data_bucket.blob('normalized_data.csv')
        self.sm_poor_fair_emails_blob = self.raw_data_bucket.blob('sm_poor_fair_emails')
        
    def create_client(self):
        """Initializes and returns a BigQuery client."""

        # Initialize the BigQuery Client 
        client = storage.Client()
        return client



