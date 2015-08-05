import sys
import os

env_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir, os.pardir))
sys.path.append(env_path)

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email.Utils import COMMASPACE, formatdate

from utils.config import get_io_config

env = get_io_config('email')
EMAIL_SERVER, EMAIL_PORT, EMAIL_PASS, EMAIL_USER, EMAIL_DOMAIN = env.EMAIL_SERVER,\
        env.EMAIL_PORT, env.EMAIL_PASS, env.EMAIL_USER, env.EMAIL_DOMAIN

def send_mail(to, subject, text, user_from = 'no-reply@namelyservices.com' ,
        files=[], cc=[], bcc=[], server=EMAIL_SERVER, port = EMAIL_PORT, user = EMAIL_USER,
        password = EMAIL_PASS, domain = EMAIL_DOMAIN):

    message = MIMEMultipart()
    message['From'] = user_from
    message['To'] = COMMASPACE.join(to)
    message['Date'] = formatdate(localtime=True)
    message['Subject'] = subject
    message['Cc'] = COMMASPACE.join(cc)
    message.attach(MIMEText(text))

    for f in files:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(f)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % \
        'report.csv')
        message.attach(part)

    addresses = []
    for x in to:
        addresses.append(x)
    for x in cc:
        addresses.append(x)
    for x in bcc:
        addresses.append(x)

    s = smtplib.SMTP_SSL(server, port, domain)
    s.login(user, password)

    s.sendmail(user_from, addresses, message.as_string())

if __name__ == '__main__':
    send_mail(['manuel@namely.com'], 'Test Subject', EMAIL_USER, \
            files = [], server = EMAIL_SERVER, port = EMAIL_PORT)
