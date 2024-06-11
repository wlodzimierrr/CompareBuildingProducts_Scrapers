from algoliasearch.search_client import SearchClient

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
        record = {
           "objectID": row[0],  
            "product_id": row[0],
            "shop_id": row[1],
            "category_id": row[2],
            "subcategory_id": row[3],
            "product_name": row[4],
            "product_description": row[5],
            "price": row[6],
            "rating_count": row[7],
            "rating": row[8],
            "image_url": row[9],
            "updated_at": row[10],
            "created_at": row[11],
            "page_url": row[12],
            "features": row[13]
        }
        algolia_records.append(record)
    return algolia_records


def insert_agolia():
    try:
        conn = conn_to_storagedb()
        print("Storage database connection successful")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM products")
        client = SearchClient.create(agolia_app_id, agolia_password)
        print("Agolia connection successful")
        
        print('Creating temporary index...')
        temp_index = client.init_index('temp_index')

        print('Processing data in batches')
        for batch in fetch_batch(cursor, batch_size=1000):
            algolia_records = prepare_records(batch)
            temp_index.save_objects(algolia_records)
            
        print('Replacing the main index with the temporary index...')
        client.move_index('temp_index', 'main_index')

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error when inserting the data to Agolia Search: {e}")
        raise e    
