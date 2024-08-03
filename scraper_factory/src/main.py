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

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.ERROR)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s', datefmt='%d-%m-%y %H:%M:%S'))

logging.getLogger().addHandler(console_handler)

from Tradepoint.tradepoint import run_tradepoint
from Screwfix.screwfix import run_screwfix
from Wickes.wickes import run_wickes
from BandQ.bandq import run_bandq

from email_utils import send_email
from agolia_utils import insert_agolia
from lambda_ec2_stop import stop_ec2

def start_tradepoint():
    try:
        logging.info('Starting Tradepoint scraper...')
        tradepoint_output = run_tradepoint()
        if tradepoint_output['status'] == 'success':
            if tradepoint_output['error_log']:
                unsuccessfull_msg = ("Task Tradepoint", "Tradepoint Scraper has finished running with some errors. See the attached log file.", log_file)
                send_email(*unsuccessfull_msg)
                logging.warning('Tradepoint scraper finished with errors.')
                return unsuccessfull_msg
            else:
                successfull_msg = ("Task Tradepoint", "Tradepoint Scraper has finished running successfully with no errors.")
                send_email(*successfull_msg)
                logging.info('Tradepoint scraper finished successfully.')
                return successfull_msg
        else:
            failed_msg = ("Task Failed", f"An error occurred: {tradepoint_output['error']}", log_file)
            send_email(*failed_msg)
            logging.error('Tradepoint scraper failed: %s', tradepoint_output['error'])
            return failed_msg
    except Exception as e:
        tradepoint_output = {"status": "failed", "error": str(e)}
        send_email("Task Failed", f"An error occurred: {str(e)}", log_file)
        logging.error('An error occurred in Tradepoint scraper: %s', e)
    return tradepoint_output

def start_screwfix():
    try:
        logging.info('Starting Screwfix scraper...')
        screwfix_output = run_screwfix()
        if screwfix_output['status'] == 'success':
            if screwfix_output['error_log']:
                unsuccessfull_msg = ("Task Screwfix", "Screwfix Scraper has finished running with some errors. See the attached log file.", log_file)
                send_email(*unsuccessfull_msg)
                logging.warning('Screwfix scraper finished with errors.')
                return unsuccessfull_msg
            else:
                successfull_msg = ("Task Screwfix", "Screwfix Scraper has finished running successfully with no errors.")
                send_email(*successfull_msg)
                logging.info('Screwfix scraper finished successfully.')
                return successfull_msg
        else:
            failed_msg = ("Task Failed", f"An error occurred: {screwfix_output['error']}", log_file)
            send_email(*failed_msg)
            logging.error('Screwfix scraper failed: %s', screwfix_output['error'])
            return failed_msg
    except Exception as e:
        screwfix_output = {"status": "failed", "error": str(e)}
        send_email("Task Failed", f"An error occurred: {str(e)}", log_file)
        logging.error('An error occurred in Screwfix scraper: %s', e)
    return screwfix_output

def start_wickes():
    try:
        logging.info('Starting Wickes scraper...')
        wickes_output = run_wickes()
        if wickes_output['status'] == 'success':
            if wickes_output['error_log']:
                unsuccessfull_msg = ("Task Wickes", "Wickes Scraper has finished running with some errors. See the attached log file.", log_file)
                send_email(*unsuccessfull_msg)
                logging.warning('Wickes scraper finished with errors.')
                return unsuccessfull_msg
            else:
                successfull_msg = ("Task Wickes", "Wickes Scraper has finished running successfully with no errors.")
                send_email(*successfull_msg)
                logging.info('Wickes scraper finished successfully.')
                return successfull_msg
        else:
            failed_msg = ("Task Failed", f"An error occurred: {wickes_output['error']}", log_file)
            send_email(*failed_msg)
            logging.error('Wickes scraper failed: %s', wickes_output['error'])
            return failed_msg
    except Exception as e:
        wickes_output = {"status": "failed", "error": str(e)}
        send_email("Task Failed", f"An error occurred: {str(e)}", log_file)
        logging.error('An error occurred in Wickes scraper: %s', e)
    return wickes_output

def start_bandq():
    try:
        logging.info('Starting B&Q scraper...')
        bandq_output = run_bandq()
        if bandq_output['status'] == 'success':
            if bandq_output['error_log']:
                unsuccessfull_msg = ("Task B&Q", "B&Q Scraper has finished running with some errors. See the attached log file.", log_file)
                send_email(*unsuccessfull_msg)
                logging.warning('B&Q scraper finished with errors.')
                return unsuccessfull_msg
            else:
                successfull_msg = ("Task B&Q", "B&Q Scraper has finished running successfully with no errors.")
                send_email(*successfull_msg)
                logging.info('B&Q scraper finished successfully.')
                return successfull_msg
        else:
            failed_msg = ("Task Failed", f"An error occurred: {bandq_output['error']}", log_file)
            send_email(*failed_msg)
            logging.error('B&Q scraper failed: %s', bandq_output['error'])
            return failed_msg
    except Exception as e:
        bandq_output = {"status": "failed", "error": str(e)}
        send_email("Task Failed", f"An error occurred: {str(e)}", log_file)
        logging.error('An error occurred in B&Q scraper: %s', e)
    return bandq_output
def main():
      try:
          logging.info('Starting main function...')

          scrapers = [
              ('wickes', start_wickes),
              ('bandq', start_bandq),
              ('tradepoint', start_tradepoint),
              ('screwfix', start_screwfix)
          ]

          results = {}
          for scraper_name, scraper_func in scrapers:
              try:
                  result = scraper_func()
                  results[scraper_name] = result
                  logging.info(f'{scraper_name} scraper result: {result}')
              except Exception as e:
                  logging.error(f'{scraper_name} scraper generated an exception: {e}')
                  results[scraper_name] = {"status": "failed", "error": str(e)}
                  send_email("Task Failed", f"An error occurred: {str(e)}", log_file)

          logging.info('Inserting new data to Agolia Search...')
          insert_agolia()

          logging.info(f'Scraping Done! New data is updated in the Agolia Search.\n{results}')

          logging.info('Stopping EC2 instance...')
          stop_ec2()

          logging.info('Main function execution completed.')

      except Exception as e:
          logging.error('An unexpected error occurred: %s', e)
          send_email("Task Failed", f"An unexpected error occurred: {str(e)}", log_file)
          raise
if __name__ == '__main__':
    main()
