import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from Screwfix.screwfix import ScrewfixScraper

from email_utils import send_email

def start_screwfix(prometheus_metrics, screwfix_logger):
    try:
        screwfix_logger.propagate = False
        screwfix_logger.info('Starting Screwfix scraper...')
        scraper = ScrewfixScraper(prometheus_metrics, screwfix_logger)
        screwfix_output = scraper.run()
        if screwfix_output['status'] == 'success':
            if screwfix_output['error_log']:
                unsuccessfull_msg = ("Task Screwfix", "Screwfix Scraper has finished running with some errors. See the attached log file.", screwfix_logger.handlers[0].baseFilename)
                send_email(*unsuccessfull_msg)
                screwfix_logger.warning('Screwfix scraper finished with errors.')
                return unsuccessfull_msg
            else:
                successfull_msg = ("Task Screwfix", "Screwfix Scraper has finished running successfully with no errors.")
                send_email(*successfull_msg)
                screwfix_logger.info('Screwfix scraper finished successfully.')
                return successfull_msg
        else:
            failed_msg = ("Task Failed", f"An error occurred: {screwfix_output['error']}", screwfix_logger.handlers[0].baseFilename)
            send_email(*failed_msg)
            screwfix_logger.error('Screwfix scraper failed: %s', screwfix_output['error'])
            return failed_msg
    except Exception as e:
        screwfix_output = {"status": "failed", "error": str(e)}
        send_email("Task Failed", f"An error occurred: {str(e)}", screwfix_logger.handlers[0].baseFilename)
        screwfix_logger.error('An error occurred in Screwfix scraper: %s', e)
    return screwfix_output

if __name__ == "__main__":
    start_screwfix()