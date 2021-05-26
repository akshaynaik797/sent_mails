import base64
import email
import imaplib
import os.path
import pickle
import re
import signal
from pathlib import Path
from datetime import datetime, timedelta
import json
import logging
from shutil import copyfile

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
import mysql.connector
import msal
import pdfkit
import requests
from dateutil.parser import parse
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from pytz import timezone
from email.header import decode_header


from make_log import log_exceptions, custom_log_data
from push_api import api_update_trigger
from settings import mail_time, file_no, file_blacklist, conn_data, pdfconfig, format_date, save_attachment, \
    hospital_data, interval, clean_filename, time_out, gen_dict_extract, portals_conn_data


class TimeOutException(Exception):
    pass

def alarm_handler(signum, frame):
    print("ALARM signal received")
    raise TimeOutException()


def gmail_apiv2(data, hosp, deferred, text, cdate):
    mails = []
    try:
        print(hosp)
        cdate = datetime.strptime(cdate, '%d/%m/%Y %H:%M:%S')
        fromtime, totime = cdate-timedelta(minutes=15), cdate+timedelta(minutes=15)
        fromtime, totime = int(fromtime.timestamp()), int(totime.timestamp())
        token_file = data['data']['token_file']
        os.remove(token_file)
        cred_file = data['data']['json_file']
        SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

        creds = None
        if os.path.exists(token_file):
            creds = Credentials.from_authorized_user_file(token_file, SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    cred_file, SCOPES)
                creds = flow.run_local_server(port=0)
            with open(token_file, 'w') as token:
                token.write(creds.to_json())
        print("done")
    except RefreshError:
        api_update_trigger(hosp, "URGENT", 'token expired')
        log_exceptions(hosp=hosp)
    except:
        log_exceptions(hosp=hosp)
    finally:
        return mails

if __name__ == '__main__':
    hosp = 'mediclaimnoble'
    gmail_apiv2(hospital_data[hosp], hosp, '', 'text', '01/01/2021 11:11:11')
    pass