from datetime import datetime, timedelta

import mysql.connector
import requests
import time

from make_log import log_exceptions
from push_api import api_update_trigger
from settings import portals_conn_data, host
from utr_search_backend import search

mails_log = host + '/getmailslog'
get_doc_details = host + '/getuploaddocdetails'

htlog_fields = ("srno", "PatientID_TreatmentID", "transactionID", "Type_Ref", "Type", "status", "HospitalID",
                "cdate", "person_name", "smsTrigger", "pushTrigger", "insurerID", "fStatus", "fLock",
                "lock", "error", "errorDescription")

def get_db_conf(**kwargs):
    fields = ('host', 'database', 'port', 'user', 'password')
    if 'env' not in kwargs:
        kwargs['env'] = 'live'
    conn_data = {'host': "iclaimdev.caq5osti8c47.ap-south-1.rds.amazonaws.com",
                 'user': "admin",
                 'password': "Welcome1!",
                 'database': 'portals'}
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        q = 'SELECT host, dbName, port, userName, password FROM dbConfiguration where hospitalID=%s and environment=%s limit 1;'
        cur.execute(q, (kwargs['hosp'], kwargs['env']))
        result = cur.fetchone()
        if result is not None:
            conf_data = dict()
            for key, value in zip(fields, result):
                conf_data[key] = value
            return conf_data

def ins_sentmaillogs(transaction_id, refno, cdate, doc_count, push_content, push_success):
    q = "insert into sentmaillogs (transactionID, refNo, cdate, doc_count, push_content, push_success)" \
        "values (%s, %s, %s, %s, %s, %s)"
    with mysql.connector.connect(**portals_conn_data) as con:
        cur = con.cursor()
        cur.execute(q, (transaction_id, refno, cdate, doc_count, push_content, push_success))
        con.commit()

def process_sent_mails():
    htlog_data, srno = [], 19482

    q = "select * from hospitalTLog where transactionID != '' and srno>%s and sent_mails_processed is null order by srno"

    ####for test purpose
    # q = "select * from hospitalTLog where transactionID != '' and srno=%s order by srno desc"

    with mysql.connector.connect(**portals_conn_data) as con:
        cur = con.cursor()
        cur.execute(q, (srno,))
        result = cur.fetchall()

    for row in result:
        temp = {}
        for k, v in zip(htlog_fields, row):
            temp[k] = v
        htlog_data.append(temp)
        pass


    for row in htlog_data:
        try:
            cdate = datetime.strptime(row["cdate"], '%d/%m/%Y %H:%M:%S') + timedelta(minutes=5)

            while 1:
                if datetime.now() > cdate:
                    break
                time.sleep(60)
                print('.')
            r1_data, r2_data, alerts, main_diff, pname = [], [], [], 0, '-'
            q = "select p_sname from preauth where refno=%s limit 1"
            dbconf = get_db_conf(hosp=row['HospitalID'])
            with mysql.connector.connect(**dbconf) as con1:
                cur1 = con1.cursor()
                cur1.execute(q, (row['Type_Ref'],))
                tmp = cur1.fetchone()
                if tmp is not None:
                    pname = pname + tmp[0]
            r = None
            with mysql.connector.connect(**portals_conn_data) as con:
                cur = con.cursor()
                q = "select * from sentmaillogs where transactionID=%s limit 1"
                cur.execute(q, (row['transactionID'],))
                r = cur.fetchone()
            ####for test purpose
            # r = None
            if r is None:
                print(row['srno'])
                data1 = {'hospitalID': row['HospitalID'], 'transactionID': row['transactionID']}
                r1 = requests.post(mails_log, data=data1)
                if r1.status_code == 200:
                    r1_data = r1.json()
                r1 = requests.post(get_doc_details, data=data1)
                if r1.status_code == 200:
                    r2_data = r1.json()
                row['mail_log'], row['doc_details'] = r1_data, r2_data
                hospital = ''
                with mysql.connector.connect(**portals_conn_data) as con:
                    cur = con.cursor()
                    q = "select hospital from sent_mails_config where hospital_id=%s limit 1"
                    cur.execute(q, (data1['hospitalID'],))
                    r = cur.fetchone()
                    if r is not None:
                        hospital = r[0]
                hospital_inbox = ""
                with mysql.connector.connect(**portals_conn_data) as con:
                    cur = con.cursor()
                    q = "select hospital1 from sent_mails_config where hospital=%s limit 1"
                    cur.execute(q, (hospital,))
                    r = cur.fetchone()
                    if r is not None:
                        hospital_inbox = r[0]
                if len(r1_data) == 0:
                    alerts.append("mail_log-NA")
                    # pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "mail_log", "NA")
                    # ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody, pstatus)
                    # cond_flag = 1
                else:
                    sentornot, saveornot, pagerror = [], [], []
                    for temp in r1_data:
                        if temp['sentornot'] != 'Yes':
                            sentornot.append(True)
                        if temp['saveornot'] != 'Yes':
                            saveornot.append(True)
                        if temp['pagerror'] != 'success':
                            pagerror.append(True)

                    if True in sentornot:
                        alerts.append("mail_log-sentornot")
                        # pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "mail_log", "sentornot")
                        # ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody,
                        #                  pstatus)
                        # cond_flag = 1
                    if True in saveornot:
                        alerts.append("mail_log-saveornot")
                        # pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "mail_log", "saveornot")
                        # ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody,
                        #                  pstatus)
                        # cond_flag = 1
                    if True in pagerror:
                        alerts.append("mail_log-pagerror")
                        # pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "mail_log", "pagerror")
                        # ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody,
                        #                  pstatus)
                        # cond_flag = 1

                if len(r2_data) == 0:
                    alerts.append("Documentdetails-NA")
                    # pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "Documentdetails", "NA")
                    # ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody, pstatus)
                    # cond_flag = 1
                else:
                    docsize = []
                    for temp in r2_data:
                        if float(temp['docSize']) < 100:
                            docsize.append(True)
                    if True in docsize:
                        alerts.append("Documentdetails-docsize")
                        # pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "Documentdetails", "docsize")
                        # ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody,
                        #                  pstatus)
                        # cond_flag = 1

                if len(row['mail_log']) > 0 and hospital != '':
                    count_mismatch, mail_not_sent, size_mismatch, rec_mails_not_sent = [], [], [], []
                    rec_count_mismatch, rec_size_mismatch = [], []
                    for temp in row['mail_log']:
                        mails = search(temp['subjectline'], hospital, row['mail_log'][0]['cdate'])
                        rec_mails = search(temp['subjectline'], hospital_inbox,
                                           row['mail_log'][0]['cdate'])
                        #mails -> mail not sent {hospital} sentmails
                        #rec mails -> mail not sent {iclaim noblememedicliam}

                        if len(mails) == 0:
                            mail_not_sent.append(True)
                        else:
                            main_diff = 0
                            for mail in mails:
                                if len(mail['attach_data']) != len(row['doc_details']):
                                    count_mismatch.append(True)
                                mail_doc_size, db_doc_size = 0, 0
                                for doc in mail['attach_data']:
                                    try:
                                        mail_doc_size = mail_doc_size + float(doc['size'])
                                    except:
                                        log_exceptions(size=doc['size'])
                                for doc in row['doc_details']:
                                    try:
                                        db_doc_size = db_doc_size + float(doc['docSize'])
                                    except:
                                        log_exceptions(docSize=doc['docSize'])
                                if mail_doc_size != db_doc_size and abs(mail_doc_size - db_doc_size) > 500:
                                    size_mismatch.append(True)
                                    diff = abs(mail_doc_size - db_doc_size)/1000
                                    main_diff = main_diff + diff

                        if len(rec_mails) == 0:
                            rec_mails_not_sent.append(True)
                        else:
                            main_diff1 = 0
                            for mail in rec_mails:
                                if len(mail['attach_data']) != len(row['doc_details']):
                                    rec_count_mismatch.append(True)
                                mail_doc_size, db_doc_size = 0, 0
                                for doc in mail['attach_data']:
                                    try:
                                        mail_doc_size = mail_doc_size + float(doc['size'])
                                    except:
                                        log_exceptions(size=doc['size'])
                                for doc in row['doc_details']:
                                    try:
                                        db_doc_size = db_doc_size + float(doc['docSize'])
                                    except:
                                        log_exceptions(docSize=doc['docSize'])
                                if mail_doc_size != db_doc_size and abs(mail_doc_size - db_doc_size) > 500:
                                    rec_size_mismatch.append(True)
                                    diff = abs(mail_doc_size - db_doc_size)/1000
                                    main_diff1 = main_diff1 + diff

                    if True in mail_not_sent:
                        alerts.append("URGENT-MAIL NOT SENT")
                        # pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "URGENT", "MAIL NOT SENT")
                        # ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data),
                        #                  pbody, pstatus)
                    if True in rec_mails_not_sent:
                        alerts.append("URGENT-MAIL NOT RECIEVED")
                        # pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "URGENT", "MAIL NOT RECIEVED")
                        # ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data),
                        #                  pbody, pstatus)
                        # cond_flag = 1
                    if True in count_mismatch:
                        alerts.append("URGENT-COUNT MISMATCH-" + hospital + '-SENTMAILS')
                        # pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "URGENT", "COUNT MISMATCH")
                        # ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'],
                        #                  len(r2_data),
                        #                  pbody, pstatus)
                        # cond_flag = 1
                    if True in size_mismatch:
                        alerts.append("URGENT-SIZE MISMATCH-" + hospital + '-SENTMAILS ' + str(main_diff) + ' KB')
                        # pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "URGENT",
                        #                                     "SIZE MISMATCH " + main_diff + ' KB')
                        # ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'],
                        #                  len(r2_data),
                        #                  pbody, pstatus)
                        # cond_flag = 1
                    if True in rec_count_mismatch:
                        alerts.append("URGENT-COUNT MISMATCH-" + hospital_inbox)
                        # pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "URGENT", "COUNT MISMATCH")
                        # ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'],
                        #                  len(r2_data),
                        #                  pbody, pstatus)
                        # cond_flag = 1
                    if True in rec_size_mismatch:
                        alerts.append("URGENT-SIZE MISMATCH-" + str(main_diff1) + ' KB ' + hospital_inbox)
                        # pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "URGENT",
                        #                                     "SIZE MISMATCH " + main_diff + ' KB')
                        # ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'],
                        #                  len(r2_data),
                        #                  pbody, pstatus)
                        # cond_flag = 1
            if len(alerts) == 0:
                ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'],
                                 len(r2_data),
                                 '', '')
            else:
                pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "URGENT", ','.join(alerts))
                ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'],
                                 len(r2_data),
                                 pbody, pstatus)
            with mysql.connector.connect(**portals_conn_data) as con:
                cur = con.cursor()
                q = "update hospitalTLog set sent_mails_processed='X' where srno=%s"
                cur.execute(q, (row['srno'],))
                con.commit()
        except:
            log_exceptions(row=row)

if __name__ == '__main__':
    while 1:
        print('process_sent_mails')
        process_sent_mails()
        time.sleep(60)
        print('done')
