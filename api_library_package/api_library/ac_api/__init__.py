import pandas as pd
import os
import logging
import json
import requests

logging.basicConfig(level=logging.INFO)

class ACConnect():
    def __init__(self, tag):
        self.api_url = os.getenv('AC_API_URL')
        self.api_key = os.getenv('AC_API_KEY')
        self.tag = tag
        
    
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
        
        
            
                
            