import os
import boto3
from botocore.exceptions import NoRegionError, ClientError
import json
import logging
from dotenv import load_dotenv

load_dotenv()

class SecretsManager:
    def __init__(self, secret_name=None, region_name=None):
        self.secret_name = secret_name or os.getenv('SECRET_NAME')
        self.region_name = region_name or os.getenv('REGION_NAME')

    def get_secret(self):
        """Retrieve and return the secret from AWS Secrets Manager."""
        session = boto3.Session(region_name=self.region_name)
        client = session.client(service_name='secretsmanager', region_name=self.region_name)
        
        try:
            get_secret_value_response = client.get_secret_value(SecretId=self.secret_name)
        except (NoRegionError, ClientError) as e:
            logging.error(f"Error retrieving secret: {e}")
            raise e
        
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)

secrets_manager = SecretsManager()
secret = secrets_manager.get_secret()

agolia_app_id = secret['agolia_app_id']
agolia_password = secret['agolia_API_key']