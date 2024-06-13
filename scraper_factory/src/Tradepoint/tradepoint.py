import boto3
from botocore.exceptions import ClientError
import psycopg2
import requests
import json
import random
import time
import queue
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from .cookies_headers import headers

from db_utils import conn_to_pathsdb, conn_to_storagedb

error_log = []

def get_scraping_target_data():

    # Connect to PostgreSQL database using psycopg2
    try:
        conn = conn_to_pathsdb()
        print("Paths database connection successful")
        cursor = conn.cursor()
        cursor.execute("SELECT category_code, url, category_id, subcategory_id FROM tradepoint LIMIT 1")  
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return [{"shop_id": 2, "category_code": row[0], "url": row[1], "category_id": row[2], "subcategory_id": row[3]} for row in data]
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise e    
    
def insert_scraped_data(data):
    
    try:
        conn = conn_to_storagedb()
        print("Storage database connection successful")
        cursor = conn.cursor()
        enriched_data = data['enrichedData']
        
        for product in enriched_data:
            shop_id = data['shop_id']
            category_id = data['category_id']
            subcategory_id = data['subcategory_id']
            attributes = product.get('attributes', {})
            product_name = attributes.get('name', 'No Name Provided')
            page_url = attributes.get('pdpURL', '')
            product_description = attributes.get('description', '')
            features = attributes.get('infoBullets', [])
            image_url = ''
            rating = 0.0
            price = 0.0
            rating_count = 0
            
            if 'pricing' in attributes:
                price_info = attributes['pricing']
                price = price_info['currentPrice']['amountIncTax']
                
            if 'averageRating' in attributes:
                average_rating = attributes['averageRating']
                rating_count = average_rating.get('count', 0)
                rating = average_rating.get('value', 0.0)
                        
            for item in attributes['mediaObjects']:
                if item['description'] == 'PrimaryImage':
                    image_url = item['url'].replace('{width}', str(284)).replace('{height}', str(284))
            
            try:
                cursor.execute(
                    """
                    INSERT INTO products (
                        shop_id, category_id, subcategory_id, product_name, page_url, product_description, features, image_url, rating, rating_count, price, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (page_url) DO UPDATE SET
                        shop_id = EXCLUDED.shop_id,
                        category_id = EXCLUDED.category_id,
                        subcategory_id = EXCLUDED.subcategory_id,
                        product_name = EXCLUDED.product_name,
                        product_description = EXCLUDED.product_description,
                        features = EXCLUDED.features,
                        image_url = EXCLUDED.image_url,
                        rating = EXCLUDED.rating,
                        rating_count = EXCLUDED.rating_count,
                        price = EXCLUDED.price,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE
                        products.price <> EXCLUDED.price
                        OR products.product_name <> EXCLUDED.product_name
                        OR products.features <> EXCLUDED.features
                        OR products.image_url <> EXCLUDED.image_url
                        OR products.rating <> EXCLUDED.rating
                        OR products.rating_count <> EXCLUDED.rating_count
                        OR products.category_id <> EXCLUDED.category_id
                        OR products.subcategory_id <> EXCLUDED.subcategory_id;
                    """,
                    (shop_id, category_id, subcategory_id, product_name, page_url, product_description, features, image_url, rating, rating_count, price)
                )
            except Exception as e:
                print(f"Error inserting product {product_name}: {e}")
                error_log.append({
                            "Product name": product_name,
                            "Page url": page_url,
                            "error": 'Inserting error'
                })
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error inserting data into the target database: {e}")
        raise e

def initial_request(category_code):
    try:
        time.sleep(random.uniform(5, 10))
        response = requests.get(
            f'https://api.kingfisher.com/v2/mobile/products/TPUK?filter[category]={category_code}&include=content&page[size]=1',
            headers=headers,
        )
        response.raise_for_status()  
        return response
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        error_log.append({
                            "Category_code": str(category_code),
                            "error": str(http_err)
                })  
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
        error_log.append({
                    "Category_code": str(category_code),
                    "error": str(conn_err)
        })  
    except requests.exceptions.Timeout as timeout_err:
        print("The request timed out:", timeout_err)
        error_log.append({
                    "Category_code": str(category_code),
                    "error": str(timeout_err)
        })  
    except requests.exceptions.RequestException as err:
        print("An error occurred while handling your request:", err)
        error_log.append({
                    "Category_code": str(category_code),
                    "error": str(err)
        })  
    return None  

def get_total_page_count(response):
    if response and response.status_code == 200:
        try:
            data = response.json()
            meta = data.get('meta', {})
            paging = meta.get('paging', None)
            if paging is None:
                print("Paging data is missing in the response.")
                return 0, False
            total_results = paging.get('totalResults', 0)
            return total_results, True
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            error_log.append({
                "Response": str(response),
                "error": str(e)
            }) 
    else:
        print("Failed to fetch data or no response")
        return 0, False

def final_request(category_code, total_results):    
    max_items_per_page = 200
    num_pages = (total_results // max_items_per_page) + (1 if total_results % max_items_per_page > 0 else 0)
    print(f'Total number of pages: {num_pages}')
    all_responses = []
    for page in range(1, num_pages + 1):
        time.sleep(random.uniform(5, 10))
        try:
            print(f"Requesting page {page} of {num_pages}")
            response = requests.get(
                f'https://api.kingfisher.com/v2/mobile/products/TPUK?filter[category]={category_code}&include=content&page[number]={page}&page[size]={max_items_per_page}',
                headers=headers,
            )
            response.raise_for_status()
            all_responses.append(response.json())
             
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred during the API request on page {page}: {http_err}")
            error_log.append({
                    "Category_code": str(category_code),
                    "Page": str(page),
                    "error": str(http_err)
            })  
        except requests.exceptions.ConnectionError as conn_err:
            print(f"Connection error occurred during the API request on page {page}: {conn_err}")
            error_log.append({
                    "Category_code": str(category_code),
                    "Page": str(page),
                    "error": str(conn_err)
            })  
        except requests.exceptions.Timeout as timeout_err:
            print(f"Timeout occurred during the API request on page {page}: {timeout_err}")
            error_log.append({
                    "Category_code": str(category_code),
                    "Page": str(page),
                    "error": str(timeout_err)
            })  
        except requests.exceptions.RequestException as err:
            print(f"An error occurred during the API request on page {page}: {err}")
            error_log.append({
                    "Category_code": str(category_code),
                    "Page": str(page),
                    "error": str(err)
            })  
        except Exception as e:
            print(f"An error occurred during the API request on page {page}: {e}")
            error_log.append({
                    "Category_code": str(category_code),
                    "Page": str(page),
                    "error": str(e)
            })  
            continue
    return all_responses

def scraping_process(task_queue, total_jobs, error_log):
    while not task_queue.empty():
        category_path_data = task_queue.get()
        shop_id = category_path_data['shop_id']
        category_id = category_path_data['category_id']
        subcategory_id = category_path_data['subcategory_id']
        category_code = category_path_data['category_code']
        url = category_path_data['url']
        
        print('Making initial request...')
        try:
            response = initial_request(category_code)
        except Exception as e:
            error_log.append({
                "path": url,
                "error": str(e)
            })
            task_queue.task_done()
            continue
            
        print('Extracting item count...')
        total_results, success_or_error = get_total_page_count(response)
        if isinstance(success_or_error, bool):
            success = success_or_error
        else:
            error_log.append({
                "path": url,
                "error": success_or_error
            })
            task_queue.task_done()
            continue
            
        print(f'Total item count for category: {total_results}')
        
        if success:
            print('Making final request...')
            all_responses = final_request(category_code, total_results)
            print('Handling data insertion...')
            total_resposnes = len(all_responses)
            print(f'Inserting {total_resposnes} responses')
            remaining_responses = total_resposnes
            total_products_scraped = 0
            for response_data in all_responses:
                enriched_data = response_data['data']
                products_count = len(enriched_data)
                total_products_scraped += products_count
                data_to_insert = {
                    'shop_id': shop_id,
                    'category_id': category_id,
                    'subcategory_id': subcategory_id,
                    'enrichedData': enriched_data
                }
                insert_scraped_data(data_to_insert)
                print(f'{remaining_responses} / {total_resposnes}')
                remaining_responses -= 1
            print(f'Total products scraped: {total_products_scraped}')
        else:
            error_log.append({
                            "path": url,
                            "error": 'Missing paging info'
            })
            continue
            
        task_queue.task_done()
        jobs_left = task_queue.qsize()
        print(f"Jobs left: {jobs_left}/{total_jobs}")
        

def run_tradepoint():
    try:
        category_paths = get_scraping_target_data()
        task_queue = queue.Queue()
        
        for path in category_paths:
            task_queue.put(path)
        
        total_jobs = task_queue.qsize()
        print(f'Total jobs to scrape: {total_jobs}')
            
        scraping_process(task_queue, total_jobs, error_log)

        return {"status": "success", "error_log": error_log}
    except Exception as e:
        print(f"Error in run_screwfix: {str(e)}")
        return {"status": "failed", "error": str(e)}
    
if __name__ == "__main__":
    run_tradepoint()
 