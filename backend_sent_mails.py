import mysql.connector
from settings import portals_conn_data

q = "select transactionID, hospitalID from hospitalTLog where transactionID != '' order by srno desc limit 10"
with mysql.connector.connect(**portals_conn_data) as con:
    cur = con.cursor()
    cur.execute(q)
    result = cur.fetchall()
    #code to check if exists in sentmaillogs
    pass