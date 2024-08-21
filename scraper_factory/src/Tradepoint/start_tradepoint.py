import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from Tradepoint.tradepoint import run_tradepoint

from email_utils import send_email

def start_tradepoint(prometheus_metrics, log_file):
    try:
        logging.info('Starting Tradepoint scraper...')
        tradepoint_output = run_tradepoint(prometheus_metrics)
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


if __name__ == "__main__":
    start_tradepoint()