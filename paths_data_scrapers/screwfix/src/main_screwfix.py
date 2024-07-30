import logging
import sys
from tqdm import tqdm
from sitemap_parser import sitemap_parser
from sort_urls import sort_urls
from sorted_category_data import process_category_codes

def main_screwfix():
    logging.basicConfig(
        level=logging.ERROR,
        format='%(asctime)s - %(module)s - %(levelname)s - %(message)s',
        filename='script_log.txt',
        filemode='a'
    )
    
    error_handler = logging.StreamHandler(sys.stderr)
    error_handler.setLevel(logging.ERROR)
    error_formatter = logging.Formatter('%(asctime)s - %(module)s - %(levelname)s - %(message)s')
    error_handler.setFormatter(error_formatter)
    logging.getLogger().addHandler(error_handler)
    
    try:
        logging.info("Starting the main script")
        
        sitemap_parser()
        sort_urls()
        process_category_codes()

        logging.info("Completed all tasks successfully")
        
    except Exception as e:
        logging.error(f"An error occurred in the main script: {e}")

if __name__ == "__main__":
    main_screwfix()
