import requests
import json
import random
import time
import queue
import os
import sys
import logging
from tqdm import tqdm

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from .headers import headers

from db_utils import conn_to_pathsdb, conn_to_storagedb

error_log = []

def get_scraping_target_data():
    """Retrieve scraping target data from paths database."""
    try:
        conn = conn_to_pathsdb()
        logging.info("Paths database connection successful")
        cursor = conn.cursor()
        cursor.execute("SELECT category_code, page_url, category, subcategory FROM bandq")  
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return [{"shop_id": 1, "category_code": row[0], "page_url": row[1], "category": row[2], "subcategory": row[3]} for row in data]
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        raise e    

# Check availability function
def check_availability(fulfilment_options):
    # Extract all the possible fulfilment methods
    methods = [
        fulfilment_options.get('homeDelivery', {}).get('shippingMethod', {}),
        fulfilment_options.get('clickAndCollect', {}).get('shippingMethodNextDay', {}),
        fulfilment_options.get('clickAndCollect', {}).get('shippingMethodSameDay', {}),
        fulfilment_options.get('clickAndCollect', {}).get('shippingMethodClickAndCollectFulfilmentCenter', {}),
        fulfilment_options.get('inStore', {}).get('shippingMethod', {})
    ]

    # Check if any method is available
    for method in methods:
        if method.get('availability') == 'Available':
            return True
    return False

def insert_scraped_data(data):
    """Insert scraped data into the storage database."""
    try:
        conn = conn_to_storagedb()
        logging.info("Storage database connection successful")
        
        with conn.cursor() as cursor:
            enriched_data = data['enrichedData']
            
            for product in enriched_data:
                fulfilment_options = product['attributes']['fulfilmentOptions']
                if check_availability(fulfilment_options):
                    shop_id = data['shop_id']
                    category = data['category']
                    subcategory = data['subcategory']
                    attributes = product.get('attributes', {})
                    product_name = attributes.get('name', 'No Name Provided')
                    page_url = attributes.get('pdpURL', '')
                    product_description = attributes.get('description', '')
                    features = attributes.get('infoBullets', [])
                    image_url = ''
                    rating = 0.0
                    price = 0.0
                    rating_count = 0

                    brand = next((spec.get('value') for spec in attributes.get("technicalSpecifications", []) if spec.get('name') == 'Brand'), 'No brand provided')

                    if 'pricing' in attributes:
                        price_info = attributes['pricing']
                        price = price_info['currentPrice']['amountIncTax']
                        
                    if 'averageRating' in attributes:
                        average_rating = attributes['averageRating']
                        rating_count = average_rating.get('count', 0)
                        rating = average_rating.get('value', 0.0)
                                
                    for item in attributes.get('mediaObjects', []):
                        if item.get('description') == 'PrimaryImage':
                            image_url = item['url'].replace('{width}', str(284)).replace('{height}', str(284))
                    
                    try:
                        cursor.execute(
                        """
                        INSERT INTO products (
                            shop_id, category, subcategory, product_name, page_url, features, image_url, product_description, rating, rating_count, price, brand, created_at, last_checked_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                        )
                        ON CONFLICT (page_url) DO UPDATE SET
                            shop_id = EXCLUDED.shop_id,
                            category = EXCLUDED.category,
                            subcategory = EXCLUDED.subcategory,
                            product_name = EXCLUDED.product_name,
                            features = EXCLUDED.features,
                            image_url = EXCLUDED.image_url,
                            product_description = EXCLUDED.product_description,
                            rating = EXCLUDED.rating,
                            rating_count = EXCLUDED.rating_count,
                            price = EXCLUDED.price,
                            brand = EXCLUDED.brand,
                            last_checked_at = CURRENT_TIMESTAMP;
                        """,
                        (shop_id, category, subcategory, product_name, page_url, features, image_url, product_description, rating, rating_count, price, brand)
                    )
                    except Exception as e:
                        logging.error(f"Error inserting product {product_name}: {e}")
                        error_log.append({
                            "Product name": product_name,
                            "Page url": page_url,
                            "error": 'Inserting error'
                        })
                else:
                    logging.info(f"Product {product_name} is not available")
        
        conn.commit()
    except Exception as e:
        logging.error(f"Error inserting data into the target database: {e}")
        raise e
    finally:
        if conn:
            conn.close()

def initial_request(category_code):
    """Make initial API request."""
    try:
        time.sleep(random.uniform(5, 10))
        response = requests.get(
            f'https://api.kingfisher.com/v2/mobile/products/BQUK?filter[category]={category_code}&include=content&page[size]=1',
            headers=headers,
        )
        response.raise_for_status()  
        return response
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
        error_log.append({
            "Category_code": str(category_code),
            "error": str(http_err)
        })  
    except requests.exceptions.ConnectionError as conn_err:
        logging.error(f"Connection error occurred: {conn_err}")
        error_log.append({
            "Category_code": str(category_code),
            "error": str(conn_err)
        })  
    except requests.exceptions.Timeout as timeout_err:
        logging.error("The request timed out: %s", timeout_err)
        error_log.append({
            "Category_code": str(category_code),
            "error": str(timeout_err)
        })  
    except requests.exceptions.RequestException as err:
        logging.error("An error occurred while handling your request: %s", err)
        error_log.append({
            "Category_code": str(category_code),
            "error": str(err)
        })  
    return None  

def get_total_page_count(response):
    """Extract total page count from API response."""
    if response and response.status_code == 200:
        try:
            data = response.json()
            meta = data.get('meta', {})
            paging = meta.get('paging', None)
            if paging is None:
                logging.error("Paging data is missing in the response.")
                return 0, False
            total_results = paging.get('totalResults', 0)
            return total_results, True
        except json.JSONDecodeError as e:
            logging.error("JSON decode error: %s", e)
        except Exception as e:
            logging.error("An unexpected error occurred: %s", e)
            error_log.append({
                "Response": str(response),
                "error": str(e)
            }) 
    else:
        logging.error("Failed to fetch data or no response")
        return 0, False

def final_request(category_code, total_results):
    """Make final API request."""
    max_items_per_page = 200
    num_pages = (total_results // max_items_per_page) + (1 if total_results % max_items_per_page > 0 else 0)
    logging.info(f'Total number of pages: {num_pages}')
    all_responses = []
    for page in range(1, num_pages + 1):
        time.sleep(random.uniform(5, 10))
        try:
            logging.info(f"Requesting page {page} of {num_pages}")
            response = requests.get(
                f'https://api.kingfisher.com/v2/mobile/products/BQUK?filter[category]={category_code}&include=content&page[number]={page}&page[size]={max_items_per_page}',
                headers=headers,
            )
            response.raise_for_status()
            all_responses.append(response.json())
             
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred during the API request on page {page}: {http_err}")
            error_log.append({
                "Category_code": str(category_code),
                "Page": str(page),
                "error": str(http_err)
            })  
        except requests.exceptions.ConnectionError as conn_err:
            logging.error(f"Connection error occurred during the API request on page {page}: {conn_err}")
            error_log.append({
                "Category_code": str(category_code),
                "Page": str(page),
                "error": str(conn_err)
            })  
        except requests.exceptions.Timeout as timeout_err:
            logging.error(f"Timeout occurred during the API request on page {page}: {timeout_err}")
            error_log.append({
                "Category_code": str(category_code),
                "Page": str(page),
                "error": str(timeout_err)
            })  
        except requests.exceptions.RequestException as err:
            logging.error(f"An error occurred during the API request on page {page}: {err}")
            error_log.append({
                "Category_code": str(category_code),
                "Page": str(page),
                "error": str(err)
            })  
        except Exception as e:
            logging.error(f"An error occurred during the API request on page {page}: {e}")
            error_log.append({
                "Category_code": str(category_code),
                "Page": str(page),
                "error": str(e)
            })  
            continue
    return all_responses

def scraping_process(task_queue, total_jobs, error_log, progress_bar):
    """Main scraping process."""
    while not task_queue.empty():
        category_path_data = task_queue.get()
        shop_id = category_path_data['shop_id']
        category = category_path_data['category']
        subcategory = category_path_data['subcategory']
        category_code = category_path_data['category_code']
        page_url = category_path_data['page_url']
        
        logging.info('Making initial request...')
        try:
            response = initial_request(category_code)
        except Exception as e:
            logging.error("Error making initial request for path %s: %s", page_url, e)
            error_log.append({
                "path": page_url,
                "error": str(e)
            })
            task_queue.task_done()
            progress_bar.update(1)
            continue
            
        logging.info('Extracting item count...')
        total_results, success_or_error = get_total_page_count(response)
        if isinstance(success_or_error, bool):
            success = success_or_error
        else:
            logging.error("Error extracting item count for path %s: %s", page_url, success_or_error)
            error_log.append({
                "path": page_url,
                "error": success_or_error
            })
            task_queue.task_done()
            progress_bar.update(1)
            continue
            
        logging.info(f'Total item count for category: {total_results}')
        
        if success:
            logging.info('Making final request...')
            all_responses = final_request(category_code, total_results)
            logging.info('Handling data insertion...')
            total_responses = len(all_responses)
            logging.info(f'Inserting {total_responses} responses')
            remaining_responses = total_responses
            total_products_scraped = 0
            for response_data in all_responses:
                enriched_data = response_data['data']
                products_count = len(enriched_data)
                total_products_scraped += products_count
                data_to_insert = {
                    'shop_id': shop_id,
                    'category': category,
                    'subcategory': subcategory,
                    'enrichedData': enriched_data
                }
                insert_scraped_data(data_to_insert)
                logging.info(f'{remaining_responses} / {total_responses}')
                remaining_responses -= 1
            logging.info(f'Total products scraped: {total_products_scraped}')
        else:
            logging.error('Missing paging info for category %s', page_url)
            error_log.append({
                "path": page_url,
                "error": 'Missing paging info'
            })
            
        task_queue.task_done()
        progress_bar.update(1)
        jobs_left = task_queue.qsize()
        logging.info(f"Jobs left: {jobs_left}/{total_jobs}")

def run_bandq():
    """Main function to run the scraping process."""
    try:
        category_paths = get_scraping_target_data()
        task_queue = queue.Queue()
        
        for path in category_paths:
            task_queue.put(path)
        
        total_jobs = task_queue.qsize()
        logging.info(f'Total jobs to scrape: {total_jobs}')
        with tqdm(total=total_jobs, desc="B&Q scraping progress") as progress_bar:
            scraping_process(task_queue, total_jobs, error_log, progress_bar)

        return {"status": "success", "error_log": error_log}
    except Exception as e:
        logging.error(f"Error in run_bandq: {str(e)}")
        return {"status": "failed", "error": str(e)}
    
if __name__ == "__main__":
    run_bandq()
