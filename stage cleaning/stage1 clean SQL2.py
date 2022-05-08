import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import PrimaryKeyConstraint, UniqueConstraint, CheckConstraint
import pyodbc
import time

from validate_email import validate_email

directory = 'E:/Cleaning-todo/'
filename = 'ESB_data_20191114.csv'
month = 'Nov19'
listname = 'GFYP' 

df = pd.read_csv(directory + filename,encoding ='ISO-8859-1',usecols=['EMAIL'])
#df.rename(columns={'EmailAddress' : 'email'},inplace=True)
print(df.shape)
df.columns = map(str.lower, df.columns)
df.drop_duplicates(subset=['email'], inplace=True)
print(df.shape)

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
df['list_id'] = listname

df.drop_duplicates(subset='email', keep='first',inplace=True)
df = df.reset_index(drop=True)
df.drop('is_valid_email',axis =1,inplace=True)
print("SQL File", df.shape)


df.astype({ 'email' : str, 'domain' : str})



print(df.shape)
print(df.head(5))


server = '78.129.204.215'
database = 'ListRepository'

engine = create_engine("mssql+pyodbc://perf_webuser:n3tw0rk!5t@t5@" + server + "/" + database + "?driver=ODBC+Driver+17+for+SQL+Server",fast_executemany=True)


cnxn = engine.connect()
rs = cnxn.execute('DELETE FROM dbo.temp_tia')
cnxn.close()
print(rs)

df.to_sql('temp_tia', con = engine, schema = 'dbo', if_exists = 'append', index=False, chunksize = 1000)


cursor = engine.raw_connection().cursor()
cursor.execute("dbo.Temp_Tia_UpdateMetadata")
cursor.commit()

query = "SELECT * FROM dbo.temp_tia"
df2 = pd.read_sql_query(query,engine)
df2.to_csv(directory + "ESB_data_20191114.csv_wellat_SQLDone_" + month + ".csv", index=False)
print(df2.shape)
print("Completed Successfully")