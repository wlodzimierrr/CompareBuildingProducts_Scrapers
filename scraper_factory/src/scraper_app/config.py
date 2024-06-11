import os
import boto3
import json
import psycopg2
from dotenv import load_dotenv
load_dotenv()

secret_name = os.getenv('SECRET_NAME')
region_name = os.getenv('REGION_NAME')


def get_secret():
    session = boto3.session.Session()
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

def conn_to_pathsdb():
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_paths,
            user=db_user,
            password=db_password,
            port=db_port
        )
        print("Connection successful")
        return conn
    except Exception as e:
        print(f"An error occurred: {e}")
        return e
    
def conn_to_storagedb():
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_storage,
            user=db_user,
            password=db_password,
            port=db_port
        )
        print("Connection successful")
        return conn
    except Exception as e:
        print(f"An error occurred: {e}")
        return e