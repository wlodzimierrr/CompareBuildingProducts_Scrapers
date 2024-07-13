import sys
import os
import logging
from datetime import datetime

main_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(main_dir)

log_dir = '/home/ec2-user/logs'
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f'scraper_{datetime.now().strftime("%d-%m-%y")}.log')

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%y %H:%M:%S',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, mode='w')
    ]
)

from Tradepoint.tradepoint import run_tradepoint
from Screwfix.screwfix import run_screwfix
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


def main():
    try:
        logging.info('Starting main function...')
        
        logging.info('Starting tradepoint.py')
        tradepoint_result = start_tradepoint()
        
        logging.info('Starting screwfix.py')
        screwfix_result = start_screwfix()
        
        logging.info('Inserting new data to Agolia Search...')
        insert_agolia()
        
        logging.info(f'Scraping Done! New data is updated in the Agolia Search.\n{tradepoint_result}\n{screwfix_result}')
        
        logging.info('Stopping EC2 instance...')
        stop_ec2()
        
        logging.info('Main function execution completed.')
        
    except Exception as e:
        logging.error('An unexpected error occurred: %s', e)
        send_email("Task Failed", f"An unexpected error occurred: {str(e)}", log_file)
        raise

if __name__ == '__main__':
    main()
