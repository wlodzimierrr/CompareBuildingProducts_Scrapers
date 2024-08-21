import sys
import os
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

main_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(main_dir)

log_dir = os.path.join(main_dir, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f'scraper_{datetime.now().strftime("%d-%m-%y")}.log')

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    datefmt='%d-%m-%y %H:%M:%S',
    level=logging.INFO,
    handlers=[
        logging.FileHandler(log_file, mode='w')
    ]
)

console_handler = logging.StreamHandler(sys.stderr)
console_handler.setLevel(logging.ERROR)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s', datefmt='%d-%m-%y %H:%M:%S'))

logging.getLogger().addHandler(console_handler)

from Wickes.start_wickes import start_wickes
from Tradepoint.start_tradepoint import start_tradepoint
from Screwfix.start_screwfix import start_screwfix
from BandQ.start_bandq import start_bandq

from email_utils import send_email
from agolia_utils import insert_agolia
from lambda_ec2_stop import stop_ec2

from prometheus_metrics import initialize_prometheus_server
prometheus_metrics = initialize_prometheus_server(7000)

def main():
    try:
        logging.info('Starting main function...')
        
        scrapers = [
            ('wickes', lambda: start_wickes(prometheus_metrics, log_file)),
            ('bandq', lambda: start_bandq(prometheus_metrics, log_file)),
            ('tradepoint', lambda: start_tradepoint(prometheus_metrics, log_file)),
            ('screwfix', lambda: start_screwfix(prometheus_metrics, log_file))
        ]

        results = {}
        futures = []

        with ThreadPoolExecutor() as executor:
            for scraper_name, scraper_func in scrapers:
                futures.append((scraper_name, executor.submit(scraper_func)))

            for scraper_name, future in futures:
                try:
                    result = future.result()
                    results[scraper_name] = result
                    logging.info(f'{scraper_name} scraper result: {result}')
                except Exception as e:
                    logging.error(f'{scraper_name} scraper generated an exception: {e}')
                    results[scraper_name] = {"status": "failed", "error": str(e)}
                    send_email("Task Failed", f"An error occurred: {str(e)}", log_file)

        # logging.info('Inserting new data to Agolia Search...')
        # insert_agolia()

        # logging.info(f'Scraping Done! New data is updated in the Agolia Search.\n{results}')

        # logging.info('Stopping EC2 instance...')
        # stop_ec2()

        logging.info('Main function execution completed.')

    except Exception as e:
        logging.error('An unexpected error occurred: %s', e)
        send_email("Task Failed", f"An unexpected error occurred: {str(e)}", log_file)
        raise

if __name__ == '__main__':
    main()