import os
import boto3
from botocore.exceptions import NoRegionError, ClientError
import json
import logging
from dotenv import load_dotenv
load_dotenv()

secret_name = os.getenv('SECRET_NAME')
region_name = os.getenv('REGION_NAME')
access_key = os.getenv('AWS_ACCESS_ID')
secret_key = os.getenv('AWS_SECRET_ACCESS')

def get_secret():
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name
    )
    
    client = session.client(service_name='secretsmanager', region_name=region_name)
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise e
    
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

secret = get_secret()

# Extract credentials from the secret
db_host = secret['host']
db_paths = secret['dbpaths']
db_storage = secret['dbstorage']
db_user = secret['username']
db_password = secret['password']
db_port = secret['port']
agolia_app_id = secret['agolia_app_id']
agolia_password = secret['agolia_API_key']