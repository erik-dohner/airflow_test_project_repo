from .gcp_api import BQConnect
from .gcp_api import StorageConnect
from .sm_api import SMConnect
from .ac_api import ACConnect
from google.cloud import bigquery
from io import StringIO
import requests 
import logging
import json
import pandas as pd


def fetch_responses():
    """Fetch all responses to survey since the specified date"""
    # Initalize the survey monkey api
    sm_conn = SMConnect()
    
    # Initalzie the BigQuery api --> create client
    bq_conn = BQConnect()

    # Query bigquery for the lastest date, then format the resulting datetime object as string
    date = bq_conn.fetch_latest_response_date().strftime('%Y-%m-%dT%H:%M:%S')
    
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
    
def parse_responses():

    # Initalize the GCP Storage object
    storage_conn = StorageConnect()
    
    try:
        
        logging.info(f'Reading {storage_conn.sm_raw_responses_blob.name} from {storage_conn.raw_data_bucket.name}...')

        # get json string
        json_data_string = storage_conn.sm_raw_responses_blob.download_as_text()

        responses_data_ls = json.loads(json_data_string)
        
        logging.info(f'Successfully loaded {storage_conn.sm_raw_responses_blob.name} from GCP Storage.')

    except Exception as e:
        logging.error(f'Failed to load JSON from GCP Storage: {e}')
        raise
                      
    # Initialize list to hold full response dfs as we loop through
    df_list = []

    # Normalize the JSON
    for response_dict in responses_data_ls:
        df = pd.json_normalize(response_dict['pages'][0]['questions'], 
                            record_path=['answers'], 
                            meta='id',
                            meta_prefix='question_'
        )
            
        # Create a dict of col names and their respective values, represented by the dict path in the each answer dict
        meta_assignments = {
            'respondent_id': 'id',
            'collector_id': 'collector_id',
            'survey_id': 'survey_id',
            'date': 'date_modified',
            'const_id': 'custom_variables.customer_no',
            'email': 'custom_variables.email',
            'performance_code': 'custom_variables.perf',
            'production_name': 'custom_variables.prod_name'
        }
        
        # Create function to loop through each level of the specificed values path to find the value at the 
        # end of that path (as everything else in the path is just dict --> dict -->dict)
        def get_nested_path(d, path):
            keys = path.split('.')
            for key in keys:
                d = d.get(key, None) 
                if not isinstance(d, dict):
                    break
            return d
        
        # Apply the function to the assignments dict using dict comprehension --> creates new dict
        # assign key,values from new dict to existing df
        # --> unpacking the dict will return keyword args, formatted as key=value
        # --> we can then pass these keyword args to the .assign(**kwargs) as it looking for an indefinit number of keyword args
        df = df.assign(**{col: get_nested_path(response_dict, value) for col, value in meta_assignments.items()})

        # append this responses data -- now stored in df -- to the df_list to be combined with all responses 
        df_list.append(df)
    
    # combine all the dfs in the df_list
    norm_answers = pd.concat(df_list)

    
    # Send norm_answers df to GCP storage as a CSV --> Utilize StringID() so that we don't need to save actaul CSV
    try:
        buffer = StringIO()
        norm_answers.to_csv(buffer, index=False)
        buffer.seek(0)
        
        storage_conn.sm_normalized_responses_blob.upload_from_string(buffer.getvalue(), content_type="text/csv")
        
        logging.info(f"Successfully uploaded {storage_conn.sm_normalized_responses_blob.name} to {storage_conn.raw_data_bucket.name}.")

    except Exception as e:
        logging.error(f"Failed to upload DataFrame to GCS: {e}")
        raise 


def bq_laod():
    # Initiate the StorageConnect object
    storage_conn = StorageConnect()

    # Get normalized.csv
    try:
        normalized_string_data = storage_conn.sm_normalized_responses_blob.download_as_text()
        if normalized_string_data:
            logging.info(f'Retrieved normalized responses csv from blob: {storage_conn.sm_normalized_responses_blob.name}, project: {storage_conn.project_id}')
        else: 
            raise RuntimeError('Downloaded file is empty')
    except Exception as e:
        logging.error(f"Failed to retrieve normalized responses CSV from blob: {storage_conn.sm_normalized_responses_blob.name}, project: {storage_conn.project_id}. Error: {e}", exc_info=True)

    # create dataframe
    normalized_df = pd.read_csv(StringIO(normalized_string_data))

    # Initiate the BigQueryConnect object
    bq_conn = BQConnect()

    # Fixing datatypes
    normalized_df['date'] = pd.to_datetime(normalized_df['date'], errors='coerce')

    str_columns = [ "choice_id", "row_id", "choice_metadata.weight", "question_id",
        "respondent_id", "collector_id", "survey_id", "const_id",
        "email", "performance_code", "production_name", "tag_data",
        "text", "other_id"]

    normalized_df[str_columns] = normalized_df[str_columns].astype(str)

    # define project_id, dataset_id and table_id
    dataset_id = bq_conn.dataset_pipeline
    table_id = f'{dataset_id}.raw_sm_responses'

    # renaming columns
    normalized_df.rename(columns={
        'choice_metadata.weight': 'choice_metadata_weight'
    }, inplace=True)

    # configure the specific table we're sending the df to in bigquery
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("choice_id", "STRING"),
            bigquery.SchemaField("row_id", "STRING"),
            bigquery.SchemaField("choice_metadata_weight", "STRING"),
            bigquery.SchemaField("question_id", "STRING"),
            bigquery.SchemaField("respondent_id", "STRING"),
            bigquery.SchemaField("collector_id", "STRING"),
            bigquery.SchemaField("survey_id", "STRING"),
            bigquery.SchemaField("date", "TIMESTAMP"),
            bigquery.SchemaField("const_id", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("performance_code", "STRING"),
            bigquery.SchemaField("production_name", "STRING"),
            bigquery.SchemaField("tag_data", "STRING"),
            bigquery.SchemaField("text", "STRING"),
            bigquery.SchemaField("other_id", "STRING"),
        ],
        write_disposition="WRITE_TRUNCATE"
    )

    # load the table to bigquery
    load_job = bq_conn.client.load_table_from_dataframe(
        normalized_df, table_id, job_config=job_config
    )

    logging.info(f'Loading normalized responses to BigQuery, table: {table_id}')

    # wait for the result
    load_job.result()  

    # Check job status and log
    if load_job.state == 'DONE':
        if load_job.error_result:
            logging.error(f"Job failed with error: {load_job.error_result}")
        else:
            logging.info(f"Job completed successfully with {load_job.output_rows} rows loaded.")
    else:
        logging.warning(f"Job state: {load_job.state}")


def fetch_sm_poor_fair():
        """Fetches any SurveyMoneky response records that have poor or fair rating. 
            The table from which this query pulls is overwritten everytime the associated DAG runs.
            Data in the table will always be new poor/fair responses
            
            Sends results to gcp storage
            
        """
        
        try:
            
            logging.info(f'Fetching emails for "poor" and "fair" survey responses from BigQuery')

            # Query the table to select email of survey response records that have 'poor' or 'fair' rating
            # Targeted table is already filtered to match necessary conditions
            query="""
            select
                email
            from
                `dbt-test-449821.pipeline.automate_sm_poor_fair_email`                
            """
            
            # Init the BQConnection
            bq_conn = BQConnect()
            
            # assign query results to dataframe
            df = bq_conn.client.query(query).to_dataframe()
            
            # extract list of email
            id_list = list(df['email'])

            if not len(id_list) > 0:
                logging.info(f'No emails returned for "poor" and "fair" survey responses')
                return
            
            # Init GCP Storage connection
            storage_conn = StorageConnect()
            
            logging.info(f"Uploading json string to GCP Storage blob: {storage_conn.sm_poor_fair_emails_blob.name}")

            # create the blob, by passing blob name to .blob() method
            storage_conn.sm_poor_fair_emails_blob.upload_from_string(json.dumps(id_list), content_type="application/json")

            # Verify the file exists in the bucket
            if not storage_conn.sm_poor_fair_emails_blob.exists():
                raise FileNotFoundError(f"Verification failed: {storage_conn.sm_poor_fair_emails_blob.name} was not found in GCP Storage.")
        
            logging.info(f"Successfully uploaded json string to blob:{storage_conn.sm_poor_fair_emails_blob.name} in bucket:{storage_conn.raw_data_bucket.name}")
            
            
                
        except Exception as e:
            logging.error(f'Failed to fetchemails for "poor" and "fair" survey responses from BigQuery: {e}')
            raise

        

def add_tags(tag, blob_name):
    """
    Initializes BigQuery and ActiveCampaign connections.
    
    - Fetches qualifying emails from a GCP storage blob.
    - Retrieves associated ActiveCampaign contact IDs.
    - Adds the specified tag to each contact in ActiveCampaign.
    
    Args:
        tag (int): The ActiveCampaign tag ID.
        blob_name (str): The name of the blob containing email data.
    """
    
    try:
        # Init StorageConnect
        storage_conn = StorageConnect()
        
        # Specify the blob name
        blob = storage_conn.raw_data_bucket.blob(blob_name)
        
        # retrieve the blob
        downloaded_data = blob.download_as_text()
        
        if not downloaded_data:
            logging.error(f'Blob {blob_name} is empty in project {storage_conn.project_id}.')
            raise RuntimeError(f'Downloaded file is empty: {blob_name}')

        logging.info(f'Successfully retrieved data from blob: {blob_name}, project: {storage_conn.project_id}')
    
        try: 
            # Convert JSON string to list
            email_ls = json.loads(downloaded_data)
        except json.JSONDecodeError as e:
            logging.error(f'Failed to parse JSON from blob {blob_name}: {e}')
            raise ValueError(f'Invalid JSON format in blob {blob_name}') from e

        # Init ACConnect, passing through the tag id
        ac_conn = ACConnect(tag)
        
        # Fetch the ac_contact_id for each email --> add tag for the email
        for email in email_ls:
            id = ac_conn.fetch_contact_id(email)
            if id:
                ac_conn.add_tag(id)
            else:
                logging.warning(f'Skipping email {email} - No ActiveCampaign contact ID found')
                
    except Exception as e:
        logging.error(f'Unexpected error in add_tags function: {e}', exc_info=True)
                
                    
            


    
  