import boto3
from botocore.exceptions import ClientError
import psycopg2
import requests
import json
import csv
import random
import time
import queue

from cookies_headers import cookies, headers

# Secret name and region
secret_name = "AWS_RDS"
region_name = "eu-west-2"

missing_paging = []

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

def get_scraping_target_data():
    # Get the secret
    secret = get_secret()

    # Extract credentials from the secret
    db_host = secret['host']
    db_paths = secret['dbpaths']
    db_user = secret['username']
    db_password = secret['password']
    db_port = secret['port'] 

    # Connect to PostgreSQL database using psycopg2
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_paths,
            user=db_user,
            password=db_password,
            port=db_port
        )
        print("Paths database connection successful")
        cursor = conn.cursor()
        cursor.execute("SELECT shop_id, category_id, subcategory_id, paths FROM screwfix LIMIT 5")  
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return [{"shop_id": row[0], "category_id": row[1], "subcategory_id": row[2], "paths": row[3]} for row in data]
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise e    

def insert_scraped_data(data):
    # Get the secret
    secret = get_secret()

    # Extract credentials from the secret
    db_host = secret['host']
    db_storage = secret['dbstorage']
    db_user = secret['username']
    db_password = secret['password']
    db_port = secret['port'] 
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_storage,
            user=db_user,
            password=db_password,
            port=db_port
        )
        print("Storage database connection successful")
        cursor = conn.cursor()
        enriched_data = data['enrichedData']
        products = enriched_data['products']
        
        for product in products:
            brand = product['brand']
            shop_id = data['shop_id']
            category_id = data['category_id']
            subcategory_id = data['subcategory_id']
            product_name = product['longDescription']
            page_url = f'www.screwfix.co.uk{product['detailPageUrl']}'
            features = product['bullets']
            image_url = product['imageUrl']
            rating = product.get('starRating', 'No rating') 
            rating_count = product['numberOfReviews']
            price = product['priceInformation']['currentPriceIncVat']['amount']
            
            cursor.execute(
                """
                INSERT INTO products (
                    shop_id, category_id, subcategory_id, product_name, page_url, features, image_url, rating, rating_count, price, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                ON CONFLICT (product_name) DO UPDATE SET
                    shop_id = EXCLUDED.shop_id,
                    category_id = EXCLUDED.category_id,
                    subcategory_id = EXCLUDED.subcategory_id,
                    page_url = EXCLUDED.page_url,
                    features = EXCLUDED.features,
                    image_url = EXCLUDED.image_url,
                    rating = EXCLUDED.rating,
                    rating_count = EXCLUDED.rating_count,
                    price = EXCLUDED.price,
                    updated_at = CURRENT_TIMESTAMP
                WHERE
                    products.price <> EXCLUDED.price
                    OR products.page_url <> EXCLUDED.page_url
                    OR products.features <> EXCLUDED.features
                    OR products.image_url <> EXCLUDED.image_url
                    OR products.rating <> EXCLUDED.rating
                    OR products.rating_count <> EXCLUDED.rating_count
                    OR products.category_id <> EXCLUDED.category_id
                    OR products.subcategory_id <> EXCLUDED.subcategory_id;
                """,
                (shop_id, category_id, subcategory_id, product_name, page_url, features, image_url, rating, rating_count, price)
            )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error inserting data into the target database: {e}")
        raise e

def initial_request(category_path):        
    
    params = {
        'page_size': '1',
        'page_start': '0',
    }

    try:
        time.sleep(random.uniform(5, 10))
        response = requests.get(
            f'https://www.screwfix.com/prod/ffx-browse-bff/v1/SFXUK/data{category_path}',
            params=params,
            cookies=cookies,
            headers=headers,
        )
        response.raise_for_status()  
        return response
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print("The request timed out:", timeout_err)
    except requests.exceptions.RequestException as err:
        print("An error occurred while handling your request:", err)

    return None  


def get_total_page_count(response):
    if response and response.status_code == 200:
        try:
            data = response.json()
            # Check if the expected keys exist
            if 'enrichedData' in data and 'totalProducts' in data['enrichedData']:
                meta = data.get('enrichedData', {})
                total_results = meta.get('totalProducts', None)
                return total_results, True
            else:
                print("Expected data not found in response")
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
    else:
        print("Failed to fetch data or no response")
    return 0, False


def final_request(category_path, total_results):        

    params = {
        'page_size': '100',
        'page_start': '0',
    }
   
    page_size = int(params['page_size'])
    num_pages = (total_results // page_size) + (1 if total_results % page_size > 0 else 0)
    print(f'Total number of pages: {num_pages}')
    all_responses = []
    remaining_pages = num_pages
    for page in range(num_pages):
        params['page_start'] = str(page * page_size)
        time.sleep(random.uniform(10, 20))
        try:
            response = requests.get(
                f'https://www.screwfix.com/prod/ffx-browse-bff/v1/SFXUK/data{category_path}',
                params=params,
                cookies=cookies,
                headers=headers,
            )
            response.raise_for_status()
            all_responses.append(response.json())
            print(f'{remaining_pages} / {num_pages}')
            remaining_pages -= 1
            
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred during the API request on page {page + 1}: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            print(f"Connection error occurred during the API request on page {page + 1}: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            print(f"Timeout occurred during the API request on page {page + 1}: {timeout_err}")
        except requests.exceptions.RequestException as err:
            print(f"An error occurred during the API request on page {page + 1}: {err}")
        except Exception as e:
            print(f"An error occurred during the API request on page {page + 1}: {e}")
            continue
    return all_responses 

def scraping_process(task_queue, total_jobs):
    while not task_queue.empty():
        category_path_data = task_queue.get()
        shop_id = category_path_data['shop_id']
        category_id = category_path_data['category_id']
        subcategory_id = category_path_data['subcategory_id']
        category_path = category_path_data['paths']
        
        print('Making initial request...')
        response = initial_request(category_path)
        print('Extracting item count...')
        total_results, success = get_total_page_count(response)
        print(f'Total item count for category: {total_results}')
        
        if success:
            print('Making final request...')
            all_responses = final_request(category_path, total_results)
            print('Handling data insertion...')
            total_resposnes = len(all_responses)
            print(f'Inserting {total_resposnes} responses')
            remaining_responses = total_resposnes
            for response_data in all_responses:
                enriched_data = response_data['enrichedData']
                data_to_insert = {
                    'shop_id': shop_id,
                    'category_id': category_id,
                    'subcategory_id': subcategory_id,
                    'enrichedData': enriched_data
                }
                print(f'{remaining_responses} / {total_resposnes}')
                remaining_responses -= 1
        else:
            missing_paging.append(category_path)
            print(f'Category error')
            continue
            
        task_queue.task_done()
        jobs_left = task_queue.qsize()
        print(f"Jobs left: {jobs_left}/{total_jobs}")
        

def main():
    category_paths = get_scraping_target_data()
    task_queue = queue.Queue()
    
    for path in category_paths:
        task_queue.put(path)
    
    total_jobs = task_queue.qsize()
    print(f'Total jobs to scrape: {total_jobs}')
        
    scraping_process(task_queue, total_jobs)
    
if __name__ == "__main__":
    main()