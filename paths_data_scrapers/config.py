import os
import boto3
from botocore.exceptions import NoRegionError, ClientError
import json
import logging
from dotenv import load_dotenv
load_dotenv()

secret_name = os.getenv('SECRET_NAME')
region_name = os.getenv('REGION_NAME')

def get_secret():
    session = boto3.Session(region_name=region_name)
    
    client = session.client(service_name='secretsmanager', region_name=region_name)
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        logging.error(f"Error retrieving secret: {e}")
        raise e
    
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

secret = get_secret()

# Extract credentials from the secret
db_host = secret['host']
db_paths = secret['dbpaths']
db_user = secret['username']
db_password = secret['password']
db_port = secret['port']