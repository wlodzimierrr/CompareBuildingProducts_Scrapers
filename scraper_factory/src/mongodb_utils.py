import pymongo
import logging

from db_postgres import storage_db_connection
from secrets_manager import mongodb_url

def fetch_batch(cursor, batch_size=1000):
    while True:
        records = cursor.fetchmany(batch_size)
        if not records:
            break
        yield records

def insert_mongodb():
    try:

        conn = storage_db_connection()
        logging.info("Storage database connection successful")
        cursor = conn.cursor()

        cursor.execute(
            """
                SELECT 
                    product_id,
                    price,
                    last_checked_at
                FROM 
                    products
                WHERE
                    last_checked_at >= NOW() - INTERVAL '30 hours';
            """)

        client = pymongo.MongoClient(mongodb_url)
        db = client['product_price_history']
        collection = db['products']

        logging.info("Processing data in batches")

        batch_number = 0
        for records in fetch_batch(cursor, batch_size=1000):
            results = []
            for record in records:
                items = {
                    'product_id': str(record[0]), 
                    'product_price': str(record[1]),  
                    'checked_at': record[2].isoformat()  
                }
                results.append(items)
            if results:
                collection.insert_many(results) 
                batch_number += 1

        logging.info(f'Data insertion complete. Total batches: {batch_number}')

        cursor.close()
        conn.close()
        client.close()
    except Exception as e:
        logging.error(f"Error when inserting data into MongoDB: {e}")
        raise e