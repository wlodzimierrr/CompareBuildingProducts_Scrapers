import logging
import concurrent.futures


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logging.getLogger().addHandler(logging.FileHandler('scraper.log'))

from screwfix.src.main_screwfix import main_screwfix
from tradepoint.src.main_tradepoint import main_tradepoint
from wickes.src.main_wickes import main_wickes

def run_scraper(scraper_func, scraper_name):
    try:
        logging.info(f"Starting {scraper_name} scraper")
        scraper_func()
        logging.info(f"{scraper_name} scraper completed successfully")
    except Exception as e:
        logging.error(f"Error in {scraper_name} scraper: {str(e)}")

def main():
    scrapers = [
        (main_screwfix, "Screwfix"),
        (main_tradepoint, "Tradepoint"),
        (main_wickes, "Wickes")
    ]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(lambda x: run_scraper(*x), scrapers)