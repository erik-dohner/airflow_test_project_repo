import os
import json
import logging
   
logging.basicConfig(level=logging.INFO)


# Create class for SM connection 
class SMConnect():
    def __init__(self, survey_id):   
        self.api_token = os.getenv('SM_API_TOKEN')
        self.api_url = os.getenv('SM_API_URL')
        self.headers = {
            'Authorization': f"Bearer {self.api_token}",
            'Content-Type': 'application/json'
            }
        self.survey_id = survey_id
    
    
