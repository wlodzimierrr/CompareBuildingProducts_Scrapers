import boto3
import os
from dotenv import load_dotenv
load_dotenv()


region_name = os.getenv('REGION_NAME')

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