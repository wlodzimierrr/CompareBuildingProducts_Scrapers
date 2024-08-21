import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from Wickes.wickes import WickesScraper

from email_utils import send_email

def start_wickes(prometheus_metrics, log_file):
    try:
        logging.info('Starting Wickes scraper...')
        scraper = WickesScraper(prometheus_metrics)
        wickes_output = scraper.run()
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

if __name__ == "__main__":
    start_wickes()