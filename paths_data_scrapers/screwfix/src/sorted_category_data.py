import time
import random
import requests
import csv
import json
import logging
from datetime import datetime
import re
from tqdm import tqdm

from cookies_headers import cookies, headers
input_csv = '/home/wlodzimierrr/Desktop/code/paths_data_scrapers/screwfix/data/category_paths.csv'
output_csv = f'/home/wlodzimierrr/Desktop/code/paths_data_scrapers/screwfix/data/sorted_screwfix_paths_{datetime.now().strftime("%d-%m-%y")}.csv'


def get_category_and_subcategory_from_url(url):
    # Extract the relevant part of the URL
    match = re.search(r'/c/([^/]+)/([^/]+)', url)
    if match:
        category = match.group(1).replace('-', ' ')
        subcategory = match.group(2).replace('-', ' ')
        return category.capitalize(), subcategory.capitalize()
    return None

def initial_request(category_path):
    """Make initial API request."""
    params = {
        'page_size': '1',
        'page_start': '0',
    }

    try:
        time.sleep(random.uniform(5, 10))
        response = requests.get(
            f'https://www.screwfix.com/prod/ffx-browse-bff/v1/SFXUK/data/{category_path}',
            params=params,
            cookies=cookies,
            headers=headers,
        )
        response.raise_for_status()  
        return response
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        logging.error(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        logging.error(f"The request timed out: {timeout_err}")
    except requests.exceptions.RequestException as err:
        logging.error(f"An error occurred while handling your request: {err}")
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
            return 0, str(e)
    else:
        logging.error("Failed to fetch data or no products found")
        return 0, False

def process_category_codes():
    """Process category codes from input CSV and save relevant data to output CSV."""
    try:
        with open(output_csv, mode='a', newline='') as file:
                            writer = csv.writer(file)
                            writer.writerow(['page_url', 'category', 'subcategory', 'category_path', 'shop_id'])
                            
        with open(input_csv, 'r', newline='', encoding='utf-8') as csvfile: 
            reader = csv.reader(csvfile)
            paths = list(reader)
            for path in tqdm(paths, desc="Processing category codes"):
                category_path = path[0]
                response = initial_request(category_path)
                count, is_valid = get_total_page_count(response)
                if is_valid:
                    try:
                        page_url = f'https://www.screwfix.com/{category_path}'
                        category, subcategory = get_category_and_subcategory_from_url(category_path)
                        shop_id = 3
                        logging.info(f"Page URL: {page_url}")
                        if count > 1:         
                            with open(output_csv, mode='a', newline='') as file:
                                writer = csv.writer(file)
                                writer.writerow([page_url, category, subcategory, category_path, shop_id])
                                logging.info(f"Saved category code: {category_path}")
                    except KeyError as e:
                        logging.error(f"Missing key in data: {e}")
                    except IndexError as e:
                        logging.error(f"Index error: {e}")
                    except Exception as e:
                        logging.error(f"An error occurred while processing data: {e}")
    except FileNotFoundError as e:
        logging.error(f"Input CSV file not found: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    process_category_codes()