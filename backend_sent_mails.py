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
    htlog_data, srno = [], 17770

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

    with mysql.connector.connect(**portals_conn_data) as con:
        cur = con.cursor()
        for row in htlog_data:
            try:
                cond_flag = None
                pname = '-'
                q = "select p_sname from preauth where refno=%s limit 1"
                dbconf = get_db_conf(hosp=row['HospitalID'])
                with mysql.connector.connect(**dbconf) as con1:
                    cur1 = con1.cursor()
                    cur1.execute(q, (row['Type_Ref'],))
                    tmp = cur1.fetchone()
                    if tmp is not None:
                        pname = pname + tmp[0]
                q = "select * from sentmaillogs where transactionID=%s limit 1"
                cur.execute(q, (row['transactionID'],))
                r = cur.fetchone()
                data1 = {'hospitalID': row['HospitalID'], 'transactionID': row['transactionID']}
                if r is None:
                    print(row['srno'])
                    r1_data, r2_data = [], []
                    r1 = requests.post(mails_log, data=data1)
                    if r1.status_code == 200:
                        r1_data = r1.json()
                    r1 = requests.post(get_doc_details, data=data1)
                    if r1.status_code == 200:
                        r2_data = r1.json()
                    row['mail_log'], row['doc_details'] = r1_data, r2_data
                    q = "select hospital from sent_mails_config where hospital_id=%s limit 1"
                    cur.execute(q, (data1['hospitalID'],))
                    r = cur.fetchone()
                    hospital = ''
                    if r is not None:
                        hospital = r[0]
                    if len(r1_data) == 0:
                        pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "mail_log", "NA")
                        ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody, pstatus)
                        cond_flag = 1
                    else:
                        for temp in r1_data:
                            if temp['sentornot'] != 'Yes':
                                pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "mail_log", "sentornot")
                                ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody,
                                                 pstatus)
                                cond_flag = 1
                            if temp['saveornot'] != 'Yes':
                                pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "mail_log", "saveornot")
                                ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody,
                                                 pstatus)
                                cond_flag = 1
                            if temp['pagerror'] != 'success':
                                pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "mail_log", "pagerror")
                                ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody,
                                                 pstatus)
                                cond_flag = 1

                    if len(r2_data) == 0:
                        pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "Documentdetails", "NA")
                        ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody, pstatus)
                        cond_flag = 1
                    else:
                        for temp in r2_data:
                            if float(temp['docSize']) < 100:
                                pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "Documentdetails", "docsize")
                                ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody,
                                                 pstatus)
                                cond_flag = 1

                    if len(row['mail_log']) > 0 and len(row['doc_details']) > 0 and hospital != '':
                        for temp in row['mail_log']:
                            mails = search(temp['subjectline'], hospital, row['mail_log'][0]['cdate'])
                            if len(mails) == 0:
                                pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "URGENT", "MAIL NOT SENT")
                                ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data),
                                                 pbody, pstatus)
                                cond_flag = 1
                            else:
                                for mail in mails:
                                    if len(mail['attach_data']) != len(row['doc_details']):
                                        pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "URGENT", "COUNT MISMATCH")
                                        ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'],
                                                         len(r2_data),
                                                         pbody, pstatus)
                                        cond_flag = 1
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
                                    if mail_doc_size != db_doc_size:
                                        pbody, pstatus = api_update_trigger(row['Type_Ref'] + pname, "URGENT", "SIZE MISMATCH")
                                        ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'],
                                                         len(r2_data),
                                                         pbody, pstatus)
                                        cond_flag = 1
                if cond_flag is None:
                    ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'],
                                     len(r2_data),
                                     '', '')
                q = "update hospitalTLog set sent_mails_processed='X' where srno=%s"
                cur.execute(q, (row['srno'],))
                con.commit()
            except:
                log_exceptions(row=row)

if __name__ == '__main__':
    while 1:
        print('process_sent_mails')
        process_sent_mails()
        print('done')
        time.sleep(60)