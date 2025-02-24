import os
import json
import logging
   

# Create class for SM connection 
class SMConnect():
    def __init__(self):
        self.current_directory = os.path.dirname(os.path.realpath(__file__))
        self.config_dict = self.fetch_config_file()
        self.survey_details_dict = self.fetch_survey_details()
        self.api_token = self.config_dict.get('API_TOKEN')
        self.api_url = self.config_dict.get('API_URL')
        self.headers = {
            'Authorization': f"Bearer {self.api_token}",
            'Content-Type': 'application/json'
            }
        self.survey_id = self.survey_details_dict.get('id')
        
    def fetch_config_file(self):
        logging.basicConfig(level=logging.INFO)


        # find the config file within that directory
        config_path = os.path.join(self.current_directory, 'sm_api_config.json')

        # log confirmation of the config files existance
        if not os.path.exists(config_path):
            logging.error(f'sm_api_config.json not found at {config_path}. Failing task.')
            raise FileNotFoundError(f'Config file not found at {config_path}')
            
        logging.info(f"sm_api_config.json found at {config_path}")

        # Load the JSON config
        try:
            with open(config_path) as file:
                sm_api_config = json.load(file) #json.load converts key-value pairs to python dictoary 
            logging.info('Successfully loaded sm_api_config.json')
        except Exception as e:
            logging.error(f'Fail_ced to load sm_apionfig.json: {str(e)}')
            raise 
            
        return sm_api_config
    
    def fetch_survey_details(self):
        logging.basicConfig(level=logging.INFO)

        # find the survey_details.json file within directory
        survey_details_path = os.path.join(self.current_directory, 'survey_details.json')

        # log confirmation of the survey_details.json files existance
        if not os.path.exists(survey_details_path):
            logging.error(f'survey_details.json not found at {survey_details_path}. Failing task.')
            raise FileNotFoundError(f'Survey Details file not found at {survey_details_path}')

        logging.info(f'sm_api_config.json found at {survey_details_path}')

        # Load survey_details.json
        try:
            with open(survey_details_path) as file:
                survey_details = json.load(file)
            logging.info('Successfully loaded survey_details.json')
        except Exception as e:
            logging.error(f'Failed to load survey_details.json: {str(e)}')
            raise
        
        return survey_details