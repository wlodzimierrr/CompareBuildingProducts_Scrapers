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
from dotenv import load_dotenv
load_dotenv()

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from .cookies_headers import cookies, headers

from db_utils import conn_to_pathsdb, conn_to_storagedb

error_log = []

def get_scraping_target_data():
   
    try:
        conn = conn_to_pathsdb()
        print("Paths database connection successful")
        cursor = conn.cursor()
        cursor.execute("SELECT shop_id, category_id, subcategory_id, paths FROM screwfix")  
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return [{"shop_id": row[0], "category_id": row[1], "subcategory_id": row[2], "paths": row[3]} for row in data]
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise e    

def insert_scraped_data(data):
    try:
        conn = conn_to_storagedb()
        print("Storage database connection successful")
        cursor = conn.cursor()
        enriched_data = data['enrichedData']
        products = enriched_data['products']
        
        for product in products:
            shop_id = data['shop_id']
            category_id = data['category_id']
            subcategory_id = data['subcategory_id']
            product_name = product['longDescription']
            page_url = f'https://www.screwfix.com{product["detailPageUrl"]}'
            features = product['bullets']
            image_url = product['imageUrl']
            rating = product.get('starRating', 'No rating') 
            rating_count = product['numberOfReviews']
            price = product['priceInformation']['currentPriceIncVat']['amount']
    
            
            try:
                cursor.execute(
                    """
                    INSERT INTO products (
                        shop_id, category_id, subcategory_id, product_name, page_url, features, image_url, rating, rating_count, price, created_at, updated_at, last_checked_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (page_url) DO UPDATE SET
                        shop_id = EXCLUDED.shop_id,
                        category_id = EXCLUDED.category_id,
                        subcategory_id = EXCLUDED.subcategory_id,
                        product_name = EXCLUDED.product_name,
                        features = EXCLUDED.features,
                        image_url = EXCLUDED.image_url,
                        rating = EXCLUDED.rating,
                        rating_count = EXCLUDED.rating_count,
                        price = EXCLUDED.price,
                        updated_at = CURRENT_TIMESTAMP,
                        last_checked_at = CURRENT_TIMESTAMP
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
                    (shop_id, category_id, subcategory_id, product_name, page_url, features, image_url, rating, rating_count, price)
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
        error_log.append({
                            "Category_code": str(category_path),
                            "error": str(http_err)
                })  
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
        error_log.append({
                    "Category_code": str(category_path),
                    "error": str(conn_err)
        })  
    except requests.exceptions.Timeout as timeout_err:
        print("The request timed out:", timeout_err)
        error_log.append({
                    "Category_code": str(category_path),
                    "error": str(timeout_err)
        })  
    except requests.exceptions.RequestException as err:
        print("An error occurred while handling your request:", err)
        error_log.append({
                    "Category_code": str(category_path),
                    "error": str(err)
        })  
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
                return 0, "Expected data not found in response"
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            return 0, str(e)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            error_log.append({
                "Response": str(response),
                "error": str(e)
            }) 
            return 0, str(e)
    else:
        print("Failed to fetch data or no response")
        return 0, "Failed to fetch data or no response"


def final_request(category_path, total_results):        

    params = {
        'page_size': '100',
        'page_start': '0',
    }
   
    page_size = int(params['page_size'])
    num_pages = (total_results // page_size) + (1 if total_results % page_size > 0 else 0)
    print(f'Total number of pages: {num_pages}')
    all_responses = []
    for page in range(num_pages):
        params['page_start'] = str(page * page_size)
        time.sleep(random.uniform(5, 10))
        try:
            print(f"Requesting page {page + 1} of {num_pages}")
            response = requests.get(
                f'https://www.screwfix.com/prod/ffx-browse-bff/v1/SFXUK/data{category_path}',
                params=params,
                cookies=cookies,
                headers=headers,
            )
            response.raise_for_status()
            all_responses.append(response.json())
            
            
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred during the API request on page {page + 1}: {http_err}")
            error_log.append({
                    "Category_code": str(category_path),
                    "Page": str(page + 1),
                    "error": str(http_err)
            })  
        except requests.exceptions.ConnectionError as conn_err:
            print(f"Connection error occurred during the API request on page {page + 1}: {conn_err}")
            error_log.append({
                    "Category_code": str(category_path),
                    "Page": str(page + 1),
                    "error": str(conn_err)
            })  
        except requests.exceptions.Timeout as timeout_err:
            print(f"Timeout occurred during the API request on page {page + 1}: {timeout_err}")
            error_log.append({
                    "Category_code": str(category_path),
                    "Page": str(page + 1),
                    "error": str(timeout_err)
            })  
        except requests.exceptions.RequestException as err:
            print(f"An error occurred during the API request on page {page + 1}: {err}")
            error_log.append({
                    "Category_code": str(category_path),
                    "Page": str(page + 1),
                    "error": str(err)
            })  
        except Exception as e:
            print(f"An error occurred during the API request on page {page + 1}: {e}")
            error_log.append({
                    "Category_code": str(category_path),
                    "Page": str(page + 1),
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
        category_path = category_path_data['paths']
        
        print('Making initial request...')
        try:
            response = initial_request(category_path)
        except Exception as e:
            error_log.append({
                "path": category_path,
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
                "path": category_path,
                "error": success_or_error
            })
            task_queue.task_done()
            continue
            
        print(f'Total item count for category: {total_results}')
        
        if success:
            print('Making final request...')
            all_responses = final_request(category_path, total_results)
            print('Handling data insertion...')
            total_resposnes = len(all_responses)
            print(f'Inserting {total_resposnes} responses')
            remaining_responses = total_resposnes
            total_products_scraped = 0
            for response_data in all_responses:
                enriched_data = response_data['enrichedData']
                products_count = len(enriched_data['products'])
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
                            "path": category_path,
                            "error": 'Missing paging info'
            })
            continue
            
        task_queue.task_done()
        jobs_left = task_queue.qsize()
        print(f"Jobs left: {jobs_left}/{total_jobs}")
        

def run_screwfix():
    try:
        category_paths = get_scraping_target_data()
        task_queue = queue.Queue()
        error_log = []
        
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
    run_screwfix()
