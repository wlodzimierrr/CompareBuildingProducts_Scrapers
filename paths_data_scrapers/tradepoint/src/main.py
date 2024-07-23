import logging
import sys
from tqdm import tqdm
from sitemap_parser import sitemap_parser
from check_product_page import network_log_collector
from har_parser import har_parser
from sorted_valid_codes import process_category_codes

def main():
    # Set up logging to file and console, for errors
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
        print("Starting the main script")
        
        sitemap_parser()
        network_log_collector()
        har_parser()
        process_category_codes()

        print("Completed all tasks successfully")
        
    except Exception as e:
        logging.error(f"An error occurred in the main script: {e}")
        print(f"An error occurred. Check script_log.log for details.")

if __name__ == "__main__":
    main()
