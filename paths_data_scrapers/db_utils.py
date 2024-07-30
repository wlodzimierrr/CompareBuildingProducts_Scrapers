import psycopg2
import logging
from config import db_host, db_password, db_paths, db_port, db_user

def conn_to_pathsdb():
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