import pandas as pd
import os
from sqlalchemy import create_engine
import csv
from datetime import datetime, date
import dateparser

dbdirectory= 'E:/MDEG/'

tablename = 'jnb_win' 
db = 'mdeg.db'
sqlite_engine = create_engine('sqlite:///' + dbdirectory + db)

df = pd.read_sql(tablename, sqlite_engine)

df.loc[:,'email_date_of_consent'] = pd.to_datetime(df['email_date_of_consent'],infer_datetime_format=True)

print(df.shape)

print(df[df['email_date_of_consent'] > '2018-05-18'].shape)
print(df[df['email_date_of_consent'] > '2019-05-18'].shape)
print(df[df['email_date_of_consent'] > '2019-11-18'].shape)

df.to_sql(tablename, sqlite_engine, if_exists='replace',chunksize=500, index=False)
