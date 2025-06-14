import psycopg2
import logging

from secrets_manager import SecretsManager

secrets_manager = SecretsManager()
secret = secrets_manager.get_secret()

db_host = secret['host']
db_user = secret['username']
db_password = secret['password']
db_port = secret['port']
db_paths = secret['dbpaths']
db_storage = secret['dbstorage']

def paths_db_connection():
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_paths,
            user=db_user,
            password=db_password,
            port=db_port
        )
        logging.info("Connection successful")
        return conn
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return e
    
def storage_db_connection():
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_storage,
            user=db_user,
            password=db_password,
            port=db_port
        )
        logging.info("Connection successful")
        return conn
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return e