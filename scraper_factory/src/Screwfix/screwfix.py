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
from .cookies_headers import cookies, headers

from db_utils import conn_to_pathsdb, conn_to_storagedb

error_log = []

def get_scraping_target_data():
    """Retrieve scraping target data from paths database."""
    try:
        conn = conn_to_pathsdb()
        logging.info("Paths database connection successful")
        cursor = conn.cursor()
        cursor.execute("SELECT shop_id, category, subcategory, category_path FROM screwfix")  
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return [{"shop_id": row[0], "category": row[1], "subcategory": row[2], "category_path": row[3]} for row in data]
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        raise e    

def insert_scraped_data(data):
    """Insert scraped data into the storage database."""
    try:
        conn = conn_to_storagedb()
        logging.info("Storage database connection successful")
        cursor = conn.cursor()
        enriched_data = data['enrichedData']
        products = enriched_data['products']
        
        for product in products:
            shop_id = data['shop_id']
            category = data['category']
            subcategory = data['subcategory']
            product_name = product['longDescription']
            page_url = f'https://www.screwfix.com{product["detailPageUrl"]}'
            features = product['bullets']
            image_url = product['imageUrl']
            rating = product.get('starRating', 0.0) 
            rating_count = product.get('numberOfReviews', 0)
            price = product['priceInformation']['currentPriceIncVat']['amount']
            brand = product["brand"]
    
            try:
                cursor.execute(
                    """
                    INSERT INTO products (
                        shop_id, category, subcategory, product_name, page_url, features, image_url, rating, rating_count, price, brand, created_at, last_checked_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (page_url) DO UPDATE SET
                        shop_id = EXCLUDED.shop_id,
                        category = EXCLUDED.category,
                        subcategory = EXCLUDED.subcategory,
                        product_name = EXCLUDED.product_name,
                        features = EXCLUDED.features,
                        image_url = EXCLUDED.image_url,
                        rating = EXCLUDED.rating,
                        rating_count = EXCLUDED.rating_count,
                        price = EXCLUDED.price,
                        brand = EXCLUDED.brand,
                        last_checked_at = CURRENT_TIMESTAMP;
                    """,
                    (shop_id, category, subcategory, product_name, page_url, features, image_url, rating, rating_count, price, brand)
                )
            except Exception as e:
                logging.error(f"Error inserting product {product_name}: {e}")
                error_log.append({
                    "Product name": product_name,
                    "Page url": page_url,
                    "error": 'Inserting error'
                })
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error inserting data into the target database: {e}")
        raise e

def initial_request(category_path):
    """Make initial API request."""
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
        logging.error(f"HTTP error occurred: {http_err}")
        error_log.append({
            "Category_code": str(category_path),
            "error": str(http_err)
        })  
    except requests.exceptions.ConnectionError as conn_err:
        logging.error(f"Connection error occurred: {conn_err}")
        error_log.append({
            "Category_code": str(category_path),
            "error": str(conn_err)
        })  
    except requests.exceptions.Timeout as timeout_err:
        logging.error("The request timed out: %s", timeout_err)
        error_log.append({
            "Category_code": str(category_path),
            "error": str(timeout_err)
        })  
    except requests.exceptions.RequestException as err:
        logging.error("An error occurred while handling your request: %s", err)
        error_log.append({
            "Category_code": str(category_path),
            "error": str(err)
        })  
    return None  

def get_total_page_count(response):
    """Extract total page count from API response."""
    if response and response.status_code == 200:
        try:
            data = response.json()
            if 'enrichedData' in data and 'totalProducts' in data['enrichedData']:
                meta = data.get('enrichedData', {})
                total_results = meta.get('totalProducts', None)
                return total_results, True
            else:
                logging.error("Expected data not found in response")
                return 0, "Expected data not found in response"
        except json.JSONDecodeError as e:
            logging.error("JSON decode error: %s", e)
            return 0, str(e)
        except Exception as e:
            logging.error("An unexpected error occurred: %s", e)
            error_log.append({
                "Response": str(response),
                "error": str(e)
            }) 
            return 0, str(e)
    else:
        logging.error("Failed to fetch data or no response")
        return 0, "Failed to fetch data or no response"

def final_request(category_path, total_results):
    """Make final API request."""
    params = {
        'page_size': '100',
        'page_start': '0',
    }
   
    page_size = int(params['page_size'])
    num_pages = (total_results // page_size) + (1 if total_results % page_size > 0 else 0)
    logging.info(f'Total number of pages: {num_pages}')
    all_responses = []
    for page in range(num_pages):
        params['page_start'] = str(page * page_size)
        time.sleep(random.uniform(5, 10))
        try:
            logging.info(f"Requesting page {page + 1} of {num_pages}")
            response = requests.get(
                f'https://www.screwfix.com/prod/ffx-browse-bff/v1/SFXUK/data{category_path}',
                params=params,
                cookies=cookies,
                headers=headers,
            )
            response.raise_for_status()
            all_responses.append(response.json())
            
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred during the API request on page {page + 1}: {http_err}")
            error_log.append({
                "Category_code": str(category_path),
                "Page": str(page + 1),
                "error": str(http_err)
            })  
        except requests.exceptions.ConnectionError as conn_err:
            logging.error(f"Connection error occurred during the API request on page {page + 1}: {conn_err}")
            error_log.append({
                "Category_code": str(category_path),
                "Page": str(page + 1),
                "error": str(conn_err)
            })  
        except requests.exceptions.Timeout as timeout_err:
            logging.error(f"Timeout occurred during the API request on page {page + 1}: {timeout_err}")
            error_log.append({
                "Category_code": str(category_path),
                "Page": str(page + 1),
                "error": str(timeout_err)
            })  
        except requests.exceptions.RequestException as err:
            logging.error(f"An error occurred during the API request on page {page + 1}: {err}")
            error_log.append({
                "Category_code": str(category_path),
                "Page": str(page + 1),
                "error": str(err)
            })  
        except Exception as e:
            logging.error(f"An error occurred during the API request on page {page + 1}: {e}")
            error_log.append({
                "Category_code": str(category_path),
                "Page": str(page + 1),
                "error": str(e)
            })  
            continue
    return all_responses 

def scraping_process(task_queue, total_jobs, error_log, progress_bar, prometheus_metrics=None):
    """Main scraping process."""
    while not task_queue.empty():
        category_path_data = task_queue.get()
        shop_id = category_path_data['shop_id']
        category = category_path_data['category']
        subcategory = category_path_data['subcategory']
        category_path = category_path_data['category_path']
        
        logging.info('Making initial request...')
        try:
            response = initial_request(category_path)
        except Exception as e:
            logging.error("Error making initial request for path %s: %s", category_path, e)
            error_log.append({
                "path": category_path,
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
            logging.error("Error extracting item count for path %s: %s", category_path, success_or_error)
            error_log.append({
                "path": category_path,
                "error": success_or_error
            })
            task_queue.task_done()
            progress_bar.update(1)
            continue
            
        logging.info(f'Total item count for category {category_path}: {total_results}')
        
        if success:
            logging.info('Making final request...')
            all_responses = final_request(category_path, total_results)
            logging.info('Handling data insertion...')
            total_resposnes = len(all_responses)
            logging.info(f'Inserting {total_resposnes} responses')
            remaining_responses = total_resposnes
            total_products_scraped = 0
            for response_data in all_responses:
                enriched_data = response_data['enrichedData']
                products_count = len(enriched_data['products'])
                total_products_scraped += products_count
                data_to_insert = {
                    'shop_id': shop_id,
                    'category': category,
                    'subcategory': subcategory,
                    'enrichedData': enriched_data
                }
                insert_scraped_data(data_to_insert)
                logging.info(f'{remaining_responses} / {total_resposnes}')
                remaining_responses -= 1
            logging.info(f'Total products scraped for category {category_path}: {total_products_scraped}')
        else:
            logging.error('Missing paging info for category %s', category_path)
            error_log.append({
                "path": category_path,
                "error": 'Missing paging info'
            })
            
        task_queue.task_done()
        progress_bar.update(1)
        jobs_left = task_queue.qsize()

        if prometheus_metrics:
            progress = (jobs_left / total_jobs) * 100
            prometheus_metrics.update_progress('screwfix', progress)
            prometheus_metrics.update_jobs_remaining('screwfix', jobs_left)
        logging.info(f"Jobs left: {jobs_left}/{total_jobs}")

def run_screwfix(prometheus_metrics=None):
    """Main function to run the scraping process."""
    try:
        category_paths = get_scraping_target_data()
        task_queue = queue.Queue()
        
        for path in category_paths:
            task_queue.put(path)
        
        total_jobs = task_queue.qsize()
        logging.info(f'Total jobs to scrape: {total_jobs}')
            
        with tqdm(total=total_jobs, desc="Screwfix scraping progress") as progress_bar:
            scraping_process(task_queue, total_jobs, error_log, progress_bar, prometheus_metrics)

        if prometheus_metrics:
            prometheus_metrics.update_progress('screwfix', 100)
            prometheus_metrics.update_jobs_remaining('screwfix', 0)

        return {"status": "success", "error_log": error_log, "screwfix": total_jobs}
    except Exception as e:
        logging.error(f"Error in run_screwfix: {str(e)}")
        if prometheus_metrics:
            prometheus_metrics.update_progress('screwfix', 0)
        return {"status": "failed", "error": str(e)}
    
if __name__ == "__main__":
    run_screwfix()
