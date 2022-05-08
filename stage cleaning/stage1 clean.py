import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import pyodbc

from validate_email import validate_email

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

def table_factory (name, tablename, schemaname):

    table_class = type(
        name,
        (Base,),
        dict (
            __tablename__ = tablename,
            __table_args__ = {'schema': schemaname},
            list_id = Column(String(50)),
            source_url = Column(String(100)),
            title = Column(String(10)), 
            first_name = Column(String(50)),
            last_name = Column(String(50)),
            id = Column(Integer, primary_key=True),
            email = Column(String(255)),
            optin_date = Column(String(20)),
            is_duplicate = Column(Integer),
            is_ok = Column(Integer),
            is_blacklisted = Column(Integer),
            is_banned_word = Column(Integer),
            is_banned_domain = Column(Integer),
            is_complaint = Column(Integer),
            is_hardbounce = Column(Integer),
            domain = Column(String(100)),
            user_status = Column(String(50)),
            last_open = Column(Integer),
            last_click = Column(Integer),
            system_created = Column(Integer),
            master_filter = Column(String(50)),
            import_filter = Column(String(50)),
            email_id = Column(Integer),
            primary_membership_id = Column(Integer),
            primary_membership = Column(String(50))
            
            )
        )
    return table_class

directory = 'E:/Cleaning-todo/'
file = 'LM ESB UK 25.09.2019_inboxed.csv'
month = 'Nov19' 

df = pd.read_csv(directory + file,encoding ='ISO-8859-1',usecols=['email'])
df.columns = map(str.lower, df.columns)

df['is_valid_email'] = df['email'].apply(lambda x:validate_email(x))
df = df[df['is_valid_email']]
print("Removed invalid email formats", df.shape)
print(df[~df.email.str.contains("@",na=False)])
df = df[df.email.str.contains("@",na=False)]
char = '\+|\*|\'| |\%|,|\"|\/'
df = df[~df['email'].str.contains(char,regex=True)]

print("Removed invalid email addresses", df.shape)

new = df["email"].str.split(pat="@", expand=True)
df['domain'] = new[1]

df.drop_duplicates(subset='email', keep='first',inplace=True)
df = df.reset_index(drop=True)
print("SQL File", df.shape)

df1 = pd.DataFrame(columns = ['list_id', 'source_url', 'title', 'first_name',\
'last_name', 'id', 'email', 'optin_date','is_duplicate','is_ok',\
'is_blacklisted','is_banned_word','is_banned_domain','is_complaint',\
'is_hardbounce','domain','user_status', 'last_open','last_click',\
'system_created','master_filter' ,'import_filter','email_id',\
'primary_membership_id','primary_membership'])

df1.astype({'list_id' : str, 'source_url' : str, 'title': str, 'first_name' : str,\
'last_name' : str, 'id' : int, 'email' : str, 'optin_date' : str,'is_duplicate' : int,'is_ok' :int,\
'is_blacklisted' :int,'is_banned_word' : int,'is_banned_domain' : int,'is_complaint' : int,\
'is_hardbounce' : int,'domain' : str,'user_status' : str, 'last_open' : int,'last_click' : int,\
'system_created' : int,'master_filter' : str ,'import_filter'  : str,'email_id' : int,\
'primary_membership_id' : int,'primary_membership' : str})

df1['list_id'] = 'inboxed'
df1['email'] = df['email']
df1['domain'] = df['domain']
df1['id'] = np.arange(len(df))
df1['is_duplicate'] = 0
df1['is_ok'] = 0
df1['is_blacklisted'] = 0
df1['is_banned_word'] = 0
df1['is_banned_domain'] = 0
df1['is_complaint'] = 0
df1['is_hardbounce'] = 0
df1['last_open'] = 0
df1['last_click'] = 0
df1['system_created'] = 0
df1['email_id'] = 0
df1['primary_membership_id'] = 0



print(df1.shape)
print(df1.head(5))


server = '78.129.204.215'
database = 'ListRepository'

engine = create_engine("mssql+pyodbc://perf_webuser:n3tw0rk!5t@t5@" + server + "/" + database + "?driver=ODBC+Driver+17+for+SQL+Server",fast_executemany=True)


cnxn = engine.connect()
rs = cnxn.execute('DELETE FROM dbo.temp_tia')
cnxn.close()
Base = declarative_base()
tia = table_factory("temp_tia","temp_tia", "dbo")
print(rs)
df1.to_sql(tia, con = engine, schema = 'dbo', if_exists = 'append', index=False, chunksize = 1000)



cnxn = engine.connect()
rs = cnxn.execute('EXEC dbo.Temp_Tia_UpdateMetadata')
print(rs)
cnxn.close()



query = "SELECT * FROM dbo.temp_tia"
df2 = pd.read_sql_query(query,engine)
df2.to_csv(directory + "LM ESB UK 25.09.2019_inboxed_SQLDone_" + month + ".csv", index=False)
print("Completed Successfully")