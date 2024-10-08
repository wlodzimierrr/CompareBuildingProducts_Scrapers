import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from BandQ.bandq import BandQScraper

from email_utils import send_email

def start_bandq(prometheus_metrics, bandq_logger):
    try:
        bandq_logger.propagate = False
        logging.info('Starting B&Q scraper...')
        scraper = BandQScraper(prometheus_metrics, bandq_logger)
        bandq_output = scraper.run()
        if bandq_output['status'] == 'success':
            if bandq_output['error_log']:
                unsuccessfull_msg = ("Task B&Q", "B&Q Scraper has finished running with some errors. See the attached log file.", bandq_logger.handlers[0].baseFilename)
                send_email(*unsuccessfull_msg)
                logging.warning('B&Q scraper finished with errors.')
                return unsuccessfull_msg
            else:
                successfull_msg = ("Task B&Q", "B&Q Scraper has finished running successfully with no errors.")
                send_email(*successfull_msg)
                logging.info('B&Q scraper finished successfully.')
                return successfull_msg
        else:
            failed_msg = ("Task Failed", f"An error occurred: {bandq_output['error']}", bandq_logger.handlers[0].baseFilename)
            send_email(*failed_msg)
            logging.error('B&Q scraper failed: %s', bandq_output['error'])
            return failed_msg
    except Exception as e:
        bandq_output = {"status": "failed", "error": str(e)}
        send_email("Task Failed", f"An error occurred: {str(e)}", bandq_logger.handlers[0].baseFilename)
        logging.error('An error occurred in B&Q scraper: %s', e)
    return bandq_output

if __name__ == "__main__":
    start_bandq()