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
directory = 'E:/MDEG/JNB/'
#file1 = 'UK LM 2017 WinInaClick revised.csv'
#file2 = 'UK LM 2018 WinInaClick revised.csv'
#file3 = 'UK LM 2019 WinInaClick revised.csv'
tablename = 'main' 
table = 'jnb_all'
dbinput = 'mdeg.db'
db = 'mdeg-v2.db'
listname = 'jnb_all'

sqlite_engine = create_engine('sqlite:///' + dbdirectory + db)
input_engine  = create_engine('sqlite:///' + dbdirectory + dbinput)


cols = ["title", "first_name", "initials", "surname", "address line 1", "address line 2", \
"address line 3", "address line 4", "address line 5", "postcode", "client_urn", "email_address", \
"mobile_number", "landline_number", "ip_address", "digital advertising_cookie", "date of birth", \
"year_of_birth", "building_ird", "contents_ird", "motor_ird", "source_code", "privacy_policy_code", \
"consent_statement_code", "email_date_of_latest_engagement", "email_unsub_bounce_block", "ip_date_of_capture", \
"email_date_of_consent", "landline_date_of_consent", "mobile_date_of_consent", "postal_date_of_consent", \
"cookie_date_of_consent", "experian_named_in_pp", "prospect", "linkage", "modelling", "profiling", \
"enrichment", "retail", "automotive ", "lifestyle", "charity", "utility", "telecommunications", \
"insurance ", "publishing / media", "entertainment/gaming/leisure", "public sector", "financial services", \
"travel", "mail order", "health/beauty", "education", "fmcg", "marketing agencies and brokers", \
"email notification date", "filler 2", "filler 3", "filler 4", "filler 5", "female", "male", "date moved in", \
"date first appeared on file", "owned outright", "mortgaged", "private rental", "social rental", "single", \
"married", "live with spouse or partner"]

upload = pd.DataFrame(columns=cols)

df = pd.read_sql_table(table, input_engine, parse_dates = {'date of birth':'%d %m %Y'})

#df1 = pd.read_csv(directory + file1, encoding = 'utf-8', low_memory=False)
#df2 = pd.read_csv(directory + file2, encoding = 'utf-8', low_memory=False)
#df3 = pd.read_csv(directory + file3, encoding = 'utf-8', low_memory=False)

#df = pd.concat([df1,df2,df3])
print('All', df.shape)
#df = pd.read_excel(directory + filename)
#df.columns = map(str.lower, df.columns)
#df = df.apply(lambda x: x.astype(str).str.lower())

#for col in list(df.columns):
#    df[col] = df[col].str.replace(',','')

df.loc[:,'timestamp'] = pd.to_datetime(df['timestamp'],infer_datetime_format=True)
df.drop_duplicates(subset='email', keep='last', inplace=True)
print('dedup', df.shape)
print(df.columns)
upload['email_address'] = df['email']
upload['title'] = df['title']
upload['first_name'] = df['first_name']
upload['surname'] = df['surname']
upload['address line 1'] = df['address']
upload['address line 3'] = df['city']
upload['postcode'] = df['postcode']
upload['date of birth'] = df['date of birth']
#upload['mobile_number'] = df['mobile_number']
upload['ip_address'] = df['ip']
upload['email_date_of_consent'] = df['timestamp']
upload['source_code'] = df['source']

genders = ['m','f', 'male', 'female']
df = df[df['gender'].isin(genders)]

print(df['gender'].value_counts(dropna=False))

print(df['gender'].head())
df.loc[df['gender'] == 'female', 'female'] = 'f'
df.loc[df['gender'] == 'f', 'female'] = 'f'
df.loc[df['gender'] == 'm', 'male'] = 'm'
df.loc[df['gender'] == 'male', 'male'] = 'm'


upload['female'] = df['female']
upload['male'] = df['male']


upload['email_date_of_latest_engagement']=pd.to_datetime(upload['email_date_of_latest_engagement'])
upload['email_unsub_bounce_block']=pd.to_datetime(upload['email_unsub_bounce_block'])
upload['ip_date_of_capture']=pd.to_datetime(upload['ip_date_of_capture'])
upload['landline_date_of_consent']=pd.to_datetime(upload['landline_date_of_consent'])
upload['mobile_date_of_consent']=pd.to_datetime(upload['mobile_date_of_consent'])
upload['postal_date_of_consent']=pd.to_datetime(upload['postal_date_of_consent'])
upload['cookie_date_of_consent']=pd.to_datetime(upload['cookie_date_of_consent'])
upload['date moved in']=pd.to_datetime(upload['date moved in'])
upload['date first appeared on file']=pd.to_datetime(upload['date first appeared on file'])

upload['list'] = listname
upload['date_added'] = date.today()
print(upload.columns)
print(upload.shape)


upload.to_sql(tablename, sqlite_engine, if_exists='append',chunksize=500, index=False)
