import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
import pyodbc

def read(cnxn):
    print("Read")
    rs = cnxn.execute("SELECT TOP (5)[email]\
      ,[optin_date]\
      ,[is_duplicate]\
      ,[is_ok]\
      ,[is_blacklisted]\
      ,[is_banned_word]\
      ,[is_banned_domain]\
      ,[is_complaint]\
      ,[is_hardbounce]\
      ,[domain]\
      ,[user_status]\
      ,[last_open]\
      ,[last_click]\
      ,[system_created]\
      ,[master_filter]\
      ,[import_filter]\
      ,[email_id]\
      ,[primary_membership_id]\
      ,[primary_membership] from dbo.temp_tia")
    data = rs.fetchone()
    print(rs.keys())
    print("Data: %s" % data) 

directory = 'E:/A-plan November/A-Plan November Renewal Data/'
file = 'A-Plan_readyforSQL_Nov19.csv'




server = '78.129.204.215'
database = 'ListRepository'

engine = create_engine("mssql+pyodbc://perf_webuser:n3tw0rk!5t@t5@" + server + "/" + database + "?driver=ODBC+Driver+17+for+SQL+Server")

cnxn = engine.connect()

read(cnxn)

cnxn.close()