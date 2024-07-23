import time
import random
import requests
import csv
import json
import logging
from datetime import datetime
from tqdm import tqdm

from header import headers

# Define the input and output CSV file paths
input_csv = '/home/wlodzimierrr/Desktop/code/paths_data_scrapers/tradepoint/data/all_tradepoint_category_codes.csv'
output_csv = f'/home/wlodzimierrr/Desktop/code/paths_data_scrapers/tradepoint/data/sorted_tradepoint_codes_{datetime.now().strftime("%d-%m-%y")}.csv'

def initial_request(category_code):
    """Make initial API request."""
    try:
        time.sleep(random.uniform(5, 10))
        response = requests.get(
            f'https://api.kingfisher.com/v2/mobile/products/TPUK?filter[category]={category_code}&include=content&page[size]=1',
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
            meta = data.get('meta', {})
            paging = meta.get('paging', None)
            if paging is None:
                logging.warning("Paging data is missing in the response.")
                return 0, False, data
            total_results = paging.get('totalResults', 0)
            return total_results, True, data
        except json.JSONDecodeError as e:
           logging.error(f"JSON decode error: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
    else:
        logging.warning("Failed to fetch data or no response")
    return 0, False, None

def process_category_codes():
    """Process category codes from input CSV and save relevant data to output CSV."""
    try:
        with open(output_csv, mode='a', newline='') as file:
                            writer = csv.writer(file)
                            writer.writerow(['page_url', 'category', 'subcategory', 'category_code', 'shop_id'])
                            
        with open(input_csv, 'r', newline='', encoding='utf-8') as csvfile: 
            reader = csv.reader(csvfile)
            total_rows = sum(1 for _ in csvfile)
            csvfile.seek(0)
            for code in tqdm(reader, total=total_rows, desc="Processing category codes"):
                category_code = code[0]
                logging.info(f"Checking category code: {category_code}")
                response = initial_request(category_code)
                count, is_valid, data = get_total_page_count(response)
                if is_valid and data:
                    try:
                        breadcrumb_list = data['data'][0]['attributes']['breadcrumbList']
                        index = len(breadcrumb_list) - 1 
                        page_url = data['data'][0]['attributes']['pdpURL']
                        category = breadcrumb_list[index - 1]['name'].strip().capitalize()
                        subcategory = breadcrumb_list[index - 2]['name'].strip().capitalize()
                        shop_id = 2
                        logging.info(f"Page URL: {page_url}")
                        if count > 1:         
                            with open(output_csv, mode='a', newline='') as file:
                                writer = csv.writer(file)
                                writer.writerow([page_url, category, subcategory, category_code, shop_id])
                                logging.info(f"Saved category code: {category_code}")
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
