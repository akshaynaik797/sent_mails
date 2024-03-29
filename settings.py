import os
import re
from datetime import datetime
from random import randint

import pdfkit
from pathlib import Path
from dateutil.parser import parse
from pytz import timezone

conn_data = {'host': "database-iclaim.caq5osti8c47.ap-south-1.rds.amazonaws.com",
             'user': "admin",
             'password': "Welcome1!",
             'database': 'python'}

portals_conn_data = {'host': "database-iclaim.caq5osti8c47.ap-south-1.rds.amazonaws.com",
             'user': "admin",
             'password': "Welcome1!",
             'database': 'portals'}

host = "http://3.7.8.68:9982"

time_out = 6000 #seconds
mail_time = 40 #minutes
interval = 60 #seconds

pdfconfig = pdfkit.configuration(wkhtmltopdf='/usr/bin/wkhtmltopdf')

hospital_data = {
    'iclaim': {
        "mode": "gmail_apiv2",
        "data": {
            "json_file": 'data/cred_iclaim.json',
            "token_file": "data/token_iclaim.json"
        }
    },

    'mediclaimnoble': {
        "mode": "gmail_apiv2",
        "data": {
            "json_file": 'data/cred_mediclaimnoble.json',
            "token_file": "data/token_mediclaimnoble.json"
        }
    },

    'inamdar': {
        "mode": "gmail_api",
        "data": {
            "json_file": 'data/credentials_inamdar.json',
            "token_file": "data/inamdar_token.pickle"
        }
    },
    'noble': {
        "mode": "gmail_api",
        "data": {
            "json_file": 'data/credentials_noble.json',
            "token_file": "data/noble_token.pickle"
        }
    },
    'ils': {
        "mode": "graph_api",
        "data": {
            "json_file": "data/credentials_ils.json",
            "email": 'ilsmediclaim@gptgroup.co.in'
        }
    },
    'ils_dumdum': {
        "mode": "graph_api",
        "data": {
            "json_file": "data/credentials_ils.json",
            "email": 'mediclaim.ils.dumdum@gptgroup.co.in'
        }
    },
    'ils_ho': {
        "mode": "graph_api",
        "data": {
            "json_file": "data/credentials_ils.json",
            "email": 'rgupta@gptgroup.co.in'
        }
    },
    'ils_agartala': {
        "mode": "imap_",
        "data": {
            "host": "gptgroup.icewarpcloud.in",
            "email": "billing.ils.agartala@gptgroup.co.in",
            "password": 'Gpt@2019'
        }
    },
    'ils_howrah': {
        "mode": "imap_",
        "data": {
            "host": "gptgroup.icewarpcloud.in",
            "email": "mediclaim.ils.howrah@gptgroup.co.in",
            "password": 'Gpt@2019'
        }
    },
}

for i in hospital_data:
    Path(os.path.join(i, "new_attach/")).mkdir(parents=True, exist_ok=True)

def gen_dict_extract(key, var):
    if isinstance(var,(list, tuple, dict)):
        for k, v in var.items():
            if k == key:
                yield v
            if isinstance(v, dict):
                for result in gen_dict_extract(key, v):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in gen_dict_extract(key, d):
                        yield result

def file_no(len):
    return str(randint((10 ** (len - 1)), 10 ** len)) + '_'

def clean_filename(filename):
    filename = filename.replace('.PDF', '.pdf')
    temp = ['/', ' ']
    for i in temp:
        filename = filename.replace(i, '')
    return filename

def file_blacklist(filename, **kwargs):
    fp = filename
    filename, file_extension = os.path.splitext(fp)
    ext = ['.pdf', '.htm', '.html', '.PDF', '.xls', '.xlsx']
    if file_extension not in ext:
        return False
    if 'email' in kwargs:
        if 'ECS' in fp and kwargs['email'] == 'paylink.india@citi.com':
            return False
        if 'ecs' in fp and kwargs['email'] == 'paylink.india@citi.com':
            return False
    if fp.find('ATT00001') != -1:
        return False
    # if (fp.find('MDI') != -1) and (fp.find('Query') == -1):
    #     return False
    if (fp.find('knee') != -1):
        return False
    if (fp.find('KYC') != -1):
        return False
    if fp.find('image') != -1:
        return False
    if (fp.find('DECLARATION') != -1):
        return False
    if (fp.find('Declaration') != -1):
        return False
    if (fp.find('notification') != -1):
        return False
    if (fp.find('CLAIMGENIEPOSTER') != -1):
        return False
    if (fp.find('declar') != -1):
        return False
    return True

def remove_img_tags(data):
    p = re.compile(r'<img.*?>')
    return p.sub('', data)


def format_date(date):
    date = date.split(',')[-1].strip()
    format = '%d %b %Y %H:%M:%S %z'
    if '(' in date:
        date = date.split('(')[0].strip()
    try:
        date = datetime.strptime(date, format)
    except:
        try:
            date = parse(date)
        except:
            with open('logs/date_err.log', 'a') as fp:
                print(date, file=fp)
            raise Exception
    date = date.astimezone(timezone('Asia/Kolkata')).replace(tzinfo=None)
    format1 = '%d/%m/%Y %H:%M:%S'
    date = date.strftime(format1)
    return date


def save_attachment(msg, download_folder, **kwargs):
    """
    Given a message, save its attachments to the specified
    download folder (default is /tmp)

    return: file path to attachment
    """
    attach_data = []
    flag = 0
    filename = None
    file_seq = file_no(4)
    for part in msg.walk():
        z = part.get_filename()
        z1 = part.get_content_type()
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None and part.get_content_type() != 'application/octet-stream':
            continue
        flag = 1
        filename = part.get_filename()
        if filename is not None:
            size = len(part.get_payload(decode=True))
            attach_data.append({'name': filename, 'size': size})
    return attach_data
