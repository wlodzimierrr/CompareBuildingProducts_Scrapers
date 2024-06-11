import sys
import os

main_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(main_dir)

from Tradepoint.tradepoint import run_tradepoint
from Screwfix.screwfix import run_screwfix
from email_utils import send_email
from agolia_utils import insert_agolia

def start_tradepoint():
    try:
        tradepoint_output = run_tradepoint()
        if tradepoint_output['status'] == 'success':
            if tradepoint_output['error_log']:  
                error_message = f"Error log:\n{str(tradepoint_output['error_log'])}"
                unsuccessfull_msg = "Task Tradepoint", "Tradepoint Scraper has finished running with some errors.\n" + error_message
                send_email(unsuccessfull_msg)
                return unsuccessfull_msg
            else:
                successfull_msg = "Task Tradepoint", "Tradepoint Scraper has finished running successfully with no errors."
                send_email(successfull_msg)
                return successfull_msg
        else:
            failed_msg = "Task Failed", f"An error occurred: {tradepoint_output['error']}"
            send_email(failed_msg)
            return failed_msg
    except Exception as e:
        tradepoint_output = {"status": "failed", "error": str(e)}
        send_email("Task Failed", f"An error occurred: {str(e)}")
    return tradepoint_output

def start_screwfix():
    try:
        screwfix_output = run_screwfix()
        if screwfix_output['status'] == 'success':
            if screwfix_output['error_log']:  
                error_message = f"Error log:\n{str(screwfix_output['error_log'])}"
                unsuccessfull_msg = "Task Screwfix", "Screwfix Scraper has finished running with some errors.\n" + error_message
                send_email(unsuccessfull_msg)
                return unsuccessfull_msg
            else:
                successfull_msg = "Task Screwfix", "Screwfix Scraper has finished running successfully with no errors."
                send_email(successfull_msg)
                return successfull_msg
        else:
            failed_msg = "Task Failed", f"An error occurred: {screwfix_output['error']}"
            send_email(failed_msg)
            return failed_msg
    except Exception as e:
        screwfix_output = {"status": "failed", "error": str(e)}
        send_email("Task Failed", f"An error occurred: {str(e)}")
    return screwfix_output

def main():
    print('Starting tradepoint.py')
    tradepoint_result = start_tradepoint()
    print('Starting screwfix.py')
    screwfix_result = start_screwfix()
    print('Inserting new data to Agolia Search...')
    insert_agolia()
    return 'Scraping Done! New data is updated in the Agolia Serarch', tradepoint_result, screwfix_result

if __name__ == '__main__':
    main()
