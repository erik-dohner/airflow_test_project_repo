import pandas as pd
import os
import logging
import json
import requests

logging.basicConfig(level=logging.INFO)

class ACConnect():
    def __init__(self, tag):
        self.current_directory = os.path.dirname(os.path.realpath(__file__))
        self.activecampaign_api_config_dict = self.fetch_config_file()
        self.api_url = self.activecampaign_api_config_dict.get('url')
        self.api_key = self.activecampaign_api_config_dict.get('active_campaign_api_key')
        self.tag = tag
        
    
    
    def fetch_config_file(self):
        """find the config json for ActiveCampaign. Returns a python dictoary of the config json"""
        # find the config file within that directory
        config_path = os.path.join(self.current_directory, 'activecampaign_api_config.json')

        # log confirmation of the config files existance
        if not os.path.exists(config_path):
            logging.error(f'activecampaign_api_config.json not found at {config_path}. Failing task.')
            raise FileNotFoundError(f'Config file not found at {config_path}')
            
        logging.info(f"activecampaign_api_config.json found at {config_path}")

        # Load the JSON config
        try:
            with open(config_path) as file:
                activecampaign_api_config = json.load(file) #json.load converts key-value pairs to python dictoary 
            logging.info('Successfully loaded sm_api_config.json')
        except Exception as e:
            logging.error(f'Fail_ced to load sm_apionfig.json: {str(e)}')
            raise 
            
        return activecampaign_api_config
    
    def fetch_contact_id(self, email):
        """Fetch the ActiveCampaign contact id for a provided email address.

        Args:
            email (string): Email address.
        
        Returns:
            string: ActiveCampaign contact ID if found.
        
        Raises:
            ValueError: If no contact is found.
            RequestException: If the API request fails.
        """
        
        # Headers for authentication
        headers = {
            'Api-Token': self.api_key,
            'Content-Type': 'application/json',
        }
        
        try:
            response = requests.get(f'{self.api_url}/contacts?email={email}', 
                                    headers=headers
                                    )
            
            if response.status_code == 200:
                contact_ls = response.json().get('contacts')
                if contact_ls:
                    ac_contact_id = contact_ls[0]['id']
                    logging.info(f'ac_contact_id fetched successfully: {ac_contact_id}')
                    return ac_contact_id
                else:
                    logging.warning(f'No field values found for email: {email}')
                    return None
                
            else:
                logging.error(f'Failed to fetch ac_contact_id for email:  {email}')
                response.raise_for_status()
                
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for email: {email} - Error: {str(e)}")
            raise  

    
    def add_tag(self, id):
        """Post the 'sm_poor_fair' tag to a contact in ActiveCampaign
        
        Args:
            id (int): ActiveCampaign contact ID
        
        """
        logging.info(f'Adding "sm_poor_fair" tag to ac_contact_id: {id}')
        
        # Headers for authentication
        headers = {
            'Api-Token': self.api_key,
            'Content-Type': 'application/json',
        }
        
        # JSON payload sending over the needed ac_contact_id and specified tag_id (this is set when
        # an instance of the ACCOnnect class is created)
        payload = {
            'contactTag': {
                'contact': id,
                'tag': self.tag
            }
        }
        
        try:
            response = requests.post(f'{self.api_url}/contactTags',
                                    json=payload,
                                    headers=headers)
            
            if response.status_code in (200, 201):
                logging.info(f"Successfully added 'sm_poor_fair' tag to contact ID: {id}")
                return response.json()
            else:
                logging.error(f"Failed to add 'sm_poor_fair' tag to contact ID: {id}, Status Code: {response.status_code}, Response: {response.text}")
                response.raise_for_status()
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed while adding 'sm_poor_fair' tag to contact ID: {id} - Error: {str(e)}")
            raise
        
        
            
                
            