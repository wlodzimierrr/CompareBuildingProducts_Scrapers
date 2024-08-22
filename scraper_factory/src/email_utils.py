import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
import logging

from secrets_manager import SecretsManager

secrets_manager = SecretsManager()
region_name = secrets_manager.get_region_name()

def send_email(subject, body, attachment=None):
    ses = boto3.client('ses', region_name)
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = 'cbm.comparebuildingmaterials@gmail.com'
    msg['To'] = 'szerszywlodzimierz@gmail.com'

    msg.attach(MIMEText(body, 'plain'))

    if attachment:
        with open(attachment, "rb") as file:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {os.path.basename(attachment)}",
        )
        msg.attach(part)

    try:
        response = ses.send_raw_email(
            Source=msg['From'],
            Destinations=[msg['To']],
            RawMessage={'Data': msg.as_string()}
        )
        return response
    except Exception as e:
        logging.error(f"Error sending email: {str(e)}")
        return None
