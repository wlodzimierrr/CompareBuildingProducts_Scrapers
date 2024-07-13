import boto3
import json
import logging
from config import region_name 

def stop_ec2():
    lam = boto3.client('lambda', region_name=region_name) 
    try:
        response = lam.invoke(
         FunctionName='arn:aws:lambda:eu-west-2:339713072054:function:EC2_stop_function',
        InvocationType='Event',
        Payload=json.dumps({})
    )
        return response
    except Exception as e:
        logging.error(f"Error stopping EC2 instance: {str(e)}")