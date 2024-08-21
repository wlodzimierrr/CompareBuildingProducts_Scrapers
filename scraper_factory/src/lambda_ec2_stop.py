import boto3
import json
import logging
import os
from dotenv import load_dotenv

load_dotenv()

region_name = os.getenv('REGION_NAME')

def stop_ec2():
    lam = boto3.client('lambda', region_name=region_name) 
    try:
        response = lam.invoke(
         FunctionName=f'arn:aws:lambda:{region_name}:339713072054:function:EC2_stop_function',
        InvocationType='Event',
        Payload=json.dumps({})
    )
        return response
    except Exception as e:
        logging.error(f"Error stopping EC2 instance: {str(e)}")