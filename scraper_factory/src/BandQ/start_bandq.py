import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from BandQ.bandq import run_bandq

from email_utils import send_email

def start_bandq(prometheus_metrics, log_file):
    try:
        logging.info('Starting B&Q scraper...')
        bandq_output = run_bandq(prometheus_metrics)
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

if __name__ == "__main__":
    start_bandq()