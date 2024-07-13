from algoliasearch.search_client import SearchClient
import logging

from config import agolia_app_id, agolia_password
from db_utils import conn_to_storagedb

def fetch_batch(cursor, batch_size=1000):
    while True:
        records = cursor.fetchmany(batch_size)
        if not records:
            break
        yield records

def prepare_records(records):
    algolia_records = []
    for row in records:
        rating = row[6]
        if rating == 'no.rating':
            rating = '0.0'
        
        record = {
            "objectID": row[0],  
            "product_id": row[0],
            "shop_id": row[1],
            "category_id": row[12],
            "subcategory_id": row[13],
            "last_checked_at": row[14],
            "category_name": row[15],
            "subcategory_name": row[16],
            "product_name": row[2],
            "product_description": row[3],
            "price": row[4],
            "rating_count": row[5],
            "rating": rating,
            "image_url": row[7],
            "updated_at": row[8],
            "created_at": row[9],
            "page_url": row[10],
            "features": row[11]
        }
        algolia_records.append(record)
    return algolia_records


def insert_agolia():
    try:
        conn = conn_to_storagedb()
        print("Storage database connection successful")
        cursor = conn.cursor()
        cursor.execute(
            """
                SELECT 
                    p.product_id,
                    p.shop_id,
                    p.product_name,
                    p.product_description,
                    p.price,
                    p.rating_count,
                    p.rating,
                    p.image_url,
                    p.updated_at,
                    p.created_at,
                    p.page_url,
                    p.features,
                    p.category_id,
                    p.subcategory_id,
                    p.last_checked_at,
                    c.category_name,
                    s.subcategory_name
                FROM 
                    products p
                JOIN 
                    categories c ON p.category_id = c.category_id
                JOIN 
                    subcategories s ON p.subcategory_id = s.subcategory_id
                WHERE
                    p.last_checked_at >= NOW() - INTERVAL '24 hours';
            """)
        
        client = SearchClient.create(agolia_app_id, agolia_password)
        logging.info("Agolia connection successful")
        
        print('Creating temporary index...')
        temp_index = client.init_index('temp_index')

        print('Processing data in batches')
        for batch in fetch_batch(cursor, batch_size=1000):
            algolia_records = prepare_records(batch)
            temp_index.save_objects(algolia_records)
            
        logging.info('Renaming current main index to a backup index...')
        client.move_index('main_index', 'backup_index')
        
        logging.info('Replacing the main index with the temporary index...')
        client.move_index('temp_index', 'main_index')

        logging.info('Indexing complete. Old main index saved as backup index.')

        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error when inserting the data to Agolia Search: {e}")
        raise e    