import mysql.connector
import requests

from push_api import api_update_trigger
from settings import portals_conn_data, host

mails_log = host + '/getmailslog'
get_doc_details = host + '/getuploaddocdetails'

htlog_fields = ("srno", "PatientID_TreatmentID", "transactionID", "Type_Ref", "Type", "status", "HospitalID",
                "cdate", "person_name", "smsTrigger", "pushTrigger", "insurerID", "fStatus", "fLock",
                "lock", "error", "errorDescription")


def ins_sentmaillogs(transaction_id, refno, cdate, doc_count, push_content, push_success):
    q = "insert into sentmaillogs (transactionID, refNo, cdate, doc_count, push_content, push_success)" \
        "values (%s, %s, %s, %s, %s, %s)"
    with mysql.connector.connect(**portals_conn_data) as con:
        cur = con.cursor()
        cur.execute(q, (transaction_id, refno, cdate, doc_count, push_content, push_success))
        con.commit()


q = "select * from hospitalTLog where transactionID != '' order by srno desc limit 10"
with mysql.connector.connect(**portals_conn_data) as con:
    cur = con.cursor()
    cur.execute(q)
    result = cur.fetchall()

htlog_data = []
for row in result:
    temp = {}
    for k, v in zip(htlog_fields, row):
        temp[k] = v
    htlog_data.append(temp)
    # code to check if exists in sentmaillogs
    pass

with mysql.connector.connect(**portals_conn_data) as con:
    cur = con.cursor()
    for row in htlog_data:
        q = "select * from sentmaillogs where transactionID=%s limit 1"
        cur.execute(q, (row['transactionID'],))
        r = cur.fetchone()
        data1 = {'hospitalID': row['HospitalID'], 'refNo': row['Type_Ref'],
                 'type': row['Type'], 'status': row['status']}
        ####for test purpose
        data1 = {'hospitalID': '8900080123380', 'refNo': 'NH-1002190',
                 'type': 'Claim', 'status': 'Sent To TPA/ Insurer'}
        ####
        if r is None:
            r1_data, r2_data = [], []
            r1 = requests.post(mails_log, data=data1)
            if r1.status_code == 200:
                r1_data = r1.json()
            r1 = requests.post(get_doc_details, data=data1)
            if r1.status_code == 200:
                r2_data = r1.json()
            row['mail_log'], row['doc_details'] = r1_data, r2_data
            # if len(r1_data) == 0:
            #     pbody, pstatus = api_update_trigger(row['Type_Ref'], "mail_log", "NA")
            #     ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody, pstatus)
            # else:
            #     for temp in r1_data:
            #         if temp['sentornot'] != 'Yes':
            #             pbody, pstatus = api_update_trigger(row['Type_Ref'], "mail_log", "sentornot")
            #             ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody,
            #                              pstatus)
            #         if temp['saveornot'] != 'Yes':
            #             pbody, pstatus = api_update_trigger(row['Type_Ref'], "mail_log", "saveornot")
            #             ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody,
            #                              pstatus)
            #         if temp['pagerror'] != 'success':
            #             pbody, pstatus = api_update_trigger(row['Type_Ref'], "mail_log", "pagerror")
            #             ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody,
            #                              pstatus)

            if len(r2_data) == 0:
                pbody, pstatus = api_update_trigger(row['Type_Ref'], "Documentdetails", "NA")
                ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody, pstatus)
            else:
                for temp in r2_data:
                    if float(temp['docSize']) < 100:
                        pbody, pstatus = api_update_trigger(row['Type_Ref'], "Documentdetails", "docsize")
                        ins_sentmaillogs(row['transactionID'], row['Type_Ref'], row['cdate'], len(r2_data), pbody,
                                         pstatus)
