import requests
import xml.etree.ElementTree as ET
import csv
import logging
from tqdm import tqdm

csv_input = 'paths_data_scrapers/screwfix/data/sitemap_urls.csv'
csv_output = 'paths_data_scrapers/screwfix/data/category_paths.csv'

def filter_and_clean_urls(urls):
    base_url = "https://www.screwfix.com"
    return [url.replace(base_url, "") for url in urls if "c" in url]

def sort_urls():
    urls = []
    try:
        with open(csv_input, mode='r', newline='') as file:
            reader = csv.reader(file)
            for url in reader:
                urls.append(url[0])

        logging.info("Starting the cleaning urls")
        base_url = "https://www.screwfix.com"
        paths = [url.replace(base_url, "") for url in tqdm(urls, desc="Cleaning URLs") if "c" in url]

        # Save cleaned URLs to a CSV file
        with open(csv_output, mode='w', newline='') as file:
            writer = csv.writer(file)
            for url in tqdm(paths[1:], desc="Saving cleaned paths"):
                writer.writerow([url])
        logging.info(f"Saved {len(paths)} cleaned paths to {csv_output}")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
    except ET.ParseError as e:
        logging.error(f"Failed to parse XML: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    sort_urls()
