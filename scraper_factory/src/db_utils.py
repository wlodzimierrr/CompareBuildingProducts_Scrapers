import psycopg2
from config import db_host, db_password, db_paths, db_storage, db_port, db_user

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