import sys
import os
from concurrent.futures import ThreadPoolExecutor

main_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(main_dir)

from Wickes.start_wickes import start_wickes
from Tradepoint.start_tradepoint import start_tradepoint
from Screwfix.start_screwfix import start_screwfix
from BandQ.start_bandq import start_bandq

from email_utils import send_email
from agolia_utils import insert_agolia
from lambda_ec2_stop import stop_ec2
from logging_config import get_logger, setup_main_logger, error_propagation

from prometheus_metrics import initialize_prometheus_server
prometheus_metrics = initialize_prometheus_server(7000)

main_logger = setup_main_logger()

screwfix_logger = get_logger('screwfix')
tradepoint_logger = get_logger('tradepoint')
wickes_logger = get_logger('wickes')
bandq_logger = get_logger('bandq')

screwfix_logger = error_propagation(screwfix_logger)
tradepoint_logger = error_propagation(tradepoint_logger)
wickes_logger = error_propagation(wickes_logger)
bandq_logger = error_propagation(bandq_logger)

def main():
    try:
        main_logger.info('Starting main function...')

        scrapers = [
            ('wickes', lambda: start_wickes(prometheus_metrics, wickes_logger)),
            ('bandq', lambda: start_bandq(prometheus_metrics, bandq_logger)),
            ('tradepoint', lambda: start_tradepoint(prometheus_metrics, tradepoint_logger)),
            ('screwfix', lambda: start_screwfix(prometheus_metrics, screwfix_logger))
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
                    main_logger.info(f'{scraper_name} scraper result: {result}')
                except Exception as e:
                    main_logger.error(f'{scraper_name} scraper generated an exception: {e}')
                    results[scraper_name] = {"status": "failed", "error": str(e)}
                    send_email("Task Failed", f"An error occurred: {str(e)}", get_logger('main').handlers[0].baseFilename)

        main_logger.info('Inserting new data to Agolia Search...')
        insert_agolia()
        main_logger.info(f'Scraping Done! New data is updated in the Agolia Search.\n{results}')
        main_logger.info('Stopping EC2 instance...')
        stop_ec2()

        main_logger.info('Main function execution completed.')
    except Exception as e:
        main_logger.error('An unexpected error occurred: %s', e)
        send_email("Task Failed", f"An unexpected error occurred: {str(e)}", get_logger('main').handlers[0].baseFilename)
        raise

if __name__ == '__main__':
    main()