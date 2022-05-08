import pandas as pd
import os
from sqlalchemy import create_engine
import csv
from datetime import datetime, date
import dateparser

def calculate_age(born):
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

dbdirectory= 'E:/MDEG/'
directory = 'E:/MDEG/FPC/'
file1 = 'Experian legacy 1.csv'
file2 = 'Experian legacy2.csv'
file3 = 'MDEG seed week 1.csv'
tablename = 'main' 
db = 'mdeg-v2.db'
listname = 'fpc'
sqlite_engine = create_engine('sqlite:///' + dbdirectory + db)
row = []

df1 = pd.read_csv(directory + file1, encoding = 'utf-8', low_memory=False)
print(df1.shape)
df2 = pd.read_csv(directory + file2, encoding = 'utf-8', low_memory=False)
print(df2.shape)
df3 = pd.read_csv(directory + file3, encoding = 'utf-8', low_memory=False)
print(df3.shape)

df = pd.concat([df1,df2,df3])
print('All', df.shape)
print(df['Email_Address'].dtypes)

df.columns = map(str.lower, df.columns)
rename = {'email__date of consent' : 'email_date_of_consent',\
    'landline__date of consent' : 'landline_date_of_consent',\
    'mobile__date of consent' : 'mobile_date_of_consent',\
    'postal__date of consent' : 'postal_date_of_consent',\
    'cookie__date of consent' : 'cookie_date_of_consent'}

df.rename(columns= rename, inplace=True)
#df['email_address'] = df['email_address'].apply(lambda x: x.casefold())
df = df.apply(lambda x: x.astype(str).str.lower())
cols = ['address line 1', 'address line 2', 'address line 3', 'address line 4', 'address line 5']

for col in cols:
    df[col] = df[col].str.replace(',','')
print(df['email_date_of_consent'].head())
df.loc[:,'email_date_of_consent'] = pd.to_datetime(df['email_date_of_consent'],dayfirst=True, infer_datetime_format=True)
df.loc[:,'landline_date_of_consent'] = pd.to_datetime(df['landline_date_of_consent'],dayfirst=True, infer_datetime_format=True)
df.loc[:,'mobile_date_of_consent'] = pd.to_datetime(df['mobile_date_of_consent'],dayfirst=True, infer_datetime_format=True)
df.loc[:,'postal_date_of_consent'] = pd.to_datetime(df['postal_date_of_consent'],dayfirst=True, infer_datetime_format=True)
df.loc[:,'cookie_date_of_consent'] = pd.to_datetime(df['cookie_date_of_consent'],dayfirst=True, infer_datetime_format=True)
df.loc[:,'cookie_date_of_consent'] = pd.to_datetime(df['cookie_date_of_consent'],dayfirst=True, infer_datetime_format=True)

df.drop_duplicates(subset='email_address', keep='last', inplace=True)
print('dedup', df.shape)

print(df['email_date_of_consent'].head())
df['list'] = listname
df['date_added'] = date.today()

df.to_sql(tablename, sqlite_engine, if_exists='replace',chunksize=500, index=False)
