import sys
import os
import boto3
from dotenv import load_dotenv
load_dotenv()

region_name = os.getenv('REGION_NAME')

app_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(app_dir)

from Tradepoint.tradepoint import run_tradepoint
from Screwfix.screwfix import run_screwfix

def send_email(subject, body):
    ses = boto3.client('ses', region_name)  
    try:
        response = ses.send_email(
            Source='cbm.comparebuildingmaterials@gmail.com',
            Destination={
                'ToAddresses': [
                    'szerszywlodzimierz@gmail.com'
                ]
            },
            Message={
                'Subject': {
                    'Data': subject
                },
                'Body': {
                    'Text': {
                        'Data': body
                    }
                }
            }
        )
        return response
    except Exception as e:
        print(f"Error sending email: {str(e)}")

def start_tradepoint():
    try:
        tradepoint_output = run_tradepoint()
        if tradepoint_output['status'] == 'success':
            if tradepoint_output['error_log']:  
                error_message = f"Error log:\n{str(tradepoint_output['error_log'])}"
                send_email("Task Tradepoint", "Tradepoint Scraper has finished running with some errors.\n" + error_message)
            else:
                send_email("Task Tradepoint", "Tradepoint Scraper has finished running successfully with no errors.")
        else:
            send_email("Task Failed", f"An error occurred: {tradepoint_output['error']}")
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
                send_email("Task Screwfix", "Screwfix Scraper has finished running with some errors.\n" + error_message)
            else:
                send_email("Task Screwfix", "Screwfix Scraper has finished running successfully with no errors.")
        else:
            send_email("Task Failed", f"An error occurred: {screwfix_output['error']}")
    except Exception as e:
        screwfix_output = {"status": "failed", "error": str(e)}
        send_email("Task Failed", f"An error occurred: {str(e)}")
    return screwfix_output

def main():
    print('Starting tradepoint.py')
    tradepoint_result = start_tradepoint()
    print('Starting screwfix.py')
    screwfix_result = start_screwfix()
    return {
        "tradepoint": tradepoint_result,
        "screwfix": screwfix_result
    }

if __name__ == '__main__':
    main()
