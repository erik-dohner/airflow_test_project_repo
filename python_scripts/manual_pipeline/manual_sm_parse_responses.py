from api_library import StorageConnect
from google.cloud import storage
import logging
import json
import pandas as pd
from io import StringIO

logging.basicConfig(level=logging.INFO)

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
        
parse_responses()
