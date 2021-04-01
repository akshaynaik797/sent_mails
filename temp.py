import mysql.connector

from settings import portals_conn_data

a = '7946217ee3ef2d205cff5422e195c26e_309t3duj8ou949oul6k4jmbb1k'
with mysql.connector.connect(**portals_conn_data) as con:
    cur = con.cursor()
    q = "select * from sentmaillogs where transactionID=%s limit 1"
    cur.execute(q, (a,))
    r = cur.fetchone()
    pass