import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from Screwfix.screwfix import run_screwfix

from email_utils import send_email

def start_screwfix(prometheus_metrics, log_file):
    try:
        logging.info('Starting Screwfix scraper...')
        screwfix_output = run_screwfix(prometheus_metrics)
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

if __name__ == "__main__":
    start_screwfix()