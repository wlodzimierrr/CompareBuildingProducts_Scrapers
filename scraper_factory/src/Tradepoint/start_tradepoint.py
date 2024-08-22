import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from Tradepoint.tradepoint import TradepointScraper

from email_utils import send_email

def start_tradepoint(prometheus_metrics, tradepoint_logger):
    try:
        tradepoint_logger.propagate = False
        tradepoint_logger.info('Starting Tradepoint scraper...')
        scraper = TradepointScraper(prometheus_metrics, tradepoint_logger)
        tradepoint_output = scraper.run()
        if tradepoint_output['status'] == 'success':
            if tradepoint_output['error_log']:
                unsuccessfull_msg = ("Task Tradepoint", "Tradepoint Scraper has finished running with some errors. See the attached log file.", tradepoint_logger.handlers[0].baseFilename)
                send_email(*unsuccessfull_msg)
                tradepoint_logger.warning('Tradepoint scraper finished with errors.')
                return unsuccessfull_msg
            else:
                successfull_msg = ("Task Tradepoint", "Tradepoint Scraper has finished running successfully with no errors.")
                send_email(*successfull_msg)
                tradepoint_logger.info('Tradepoint scraper finished successfully.')
                return successfull_msg
        else:
            failed_msg = ("Task Failed", f"An error occurred: {tradepoint_output['error']}", tradepoint_logger.handlers[0].baseFilename)
            send_email(*failed_msg)
            tradepoint_logger.error('Tradepoint scraper failed: %s', tradepoint_output['error'])
            return failed_msg
    except Exception as e:
        tradepoint_output = {"status": "failed", "error": str(e)}
        send_email("Task Failed", f"An error occurred: {str(e)}", tradepoint_logger.handlers[0].baseFilename)
        tradepoint_logger.error('An error occurred in Tradepoint scraper: %s', e)
    return tradepoint_output


if __name__ == "__main__":
    start_tradepoint()