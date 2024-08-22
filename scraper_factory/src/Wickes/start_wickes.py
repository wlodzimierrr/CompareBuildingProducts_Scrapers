import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from Wickes.wickes import WickesScraper
from email_utils import send_email

def start_wickes(prometheus_metrics, wickes_logger):
    try:
        wickes_logger.propagate = False
        wickes_logger.info('Starting Wickes scraper...')
        scraper = WickesScraper(prometheus_metrics, wickes_logger)
        wickes_output = scraper.run()
        if wickes_output['status'] == 'success':
            if wickes_output['error_log']:
                unsuccessfull_msg = ("Task Wickes", "Wickes Scraper has finished running with some errors. See the attached log file.", wickes_logger.handlers[0].baseFilename)
                send_email(*unsuccessfull_msg)
                wickes_logger.warning('Wickes scraper finished with errors.')
                return unsuccessfull_msg
            else:
                successfull_msg = ("Task Wickes", "Wickes Scraper has finished running successfully with no errors.")
                send_email(*successfull_msg)
                wickes_logger.info('Wickes scraper finished successfully.')
                return successfull_msg
        else:
            failed_msg = ("Task Failed", f"An error occurred: {wickes_output['error']}", wickes_logger.handlers[0].baseFilename)
            send_email(*failed_msg)
            wickes_logger.error('Wickes scraper failed: %s', wickes_output['error'])
            return failed_msg
    except Exception as e:
        wickes_output = {"status": "failed", "error": str(e)}
        send_email("Task Failed", f"An error occurred: {str(e)}", wickes_logger.handlers[0].baseFilename)
        wickes_logger.error('An error occurred in Wickes scraper: %s', e)
    return wickes_output
    
if __name__ == "__main__":
    start_wickes()