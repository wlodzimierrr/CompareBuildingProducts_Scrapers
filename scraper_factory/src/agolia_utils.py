import logging
from algoliasearch.search_client import SearchClient

from db_utils import storage_db_connection

from secrets_manager import agolia_app_id, agolia_password

def fetch_batch(cursor, batch_size=1000):
    while True:
        records = cursor.fetchmany(batch_size)
        if not records:
            break
        yield records

def prepare_records(records):
    algolia_records = []
    for row in records:
        # Handle potential None values
        product_description = row[3] if row[3] is not None else ""
        features = row[11] if row[11] is not None else ""

        # Estimate the size of the entry (in bytes)
        estimated_size = sum(len(str(field)) for field in [
            row[0],  # product_id
            row[1],  # shop_id
            row[2],  # product_name
            product_description,
            row[4],  # price
            row[5],  # rating_count
            row[6],  # rating
            row[7],  # image_url
            row[8],  # updated_at
            row[9],  # created_at
            row[10], # page_url
            features,
            row[12], # category
            row[13], # subcategory
            row[14], # last_checked_at
            row[15], # brand
        ])

        # Check if the estimated size exceeds 10,000 bytes
        if estimated_size > 10000:
            continue  # Skip this entry if it is too large

        # Handle rating conversion
        rating = row[6]
        if rating == 'no.rating':
            rating = '0.0'
        
        # Create the dictionary with the proper data
        record = {
            "objectID": row[0],  
            "product_id": row[0],
            "shop_id": row[1],
            "category": row[12],
            "subcategory": row[13],
            "brand": row[15],
            "last_checked_at": row[14],
            "product_name": row[2],
            "product_description": product_description,
            "price": row[4],
            "rating_count": row[5],
            "rating": rating,
            "image_url": row[7],
            "updated_at": row[8],
            "created_at": row[9],
            "page_url": row[10],
            "features": features
        }
        algolia_records.append(record)
    
    return algolia_records



def insert_agolia():
    try:
        conn = storage_db_connection()
        logging.info("Storage database connection successful")
        cursor = conn.cursor()
        cursor.execute(
            """
                SELECT 
                    product_id,
                    shop_id,
                    product_name,
                    product_description,
                    price,
                    rating_count,
                    rating,
                    image_url,
                    updated_at,
                    created_at,
                    page_url,
                    features,
                    category,
                    subcategory,
                    last_checked_at,
                    brand
                FROM 
                    products
                WHERE
                    last_checked_at >= NOW() - INTERVAL '30 hours';
            """)
        
        client = SearchClient.create(agolia_app_id, agolia_password)
        logging.info("Agolia connection successful")
            
        logging.info('Deleting existing main index...')
        client.delete_index('main_index')

        logging.info('Creating new main index...')
        main_index = client.init_index('main_index')

        logging.info('Processing data in batches')
        for batch in fetch_batch(cursor, batch_size=1000):
            algolia_records = prepare_records(batch)
            main_index.save_objects(algolia_records)

        logging.info('Indexing complete. New main index created with updated data.')

        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error when inserting the data to Agolia Search: {e}")
        raise e    