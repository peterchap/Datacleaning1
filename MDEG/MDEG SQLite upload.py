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
file1 = 'UK LM 2017 WinInaClick revised.csv'
file2 = 'UK LM 2018 WinInaClick revised.csv'
file3 = 'UK LM 2019 WinInaClick revised.csv'
tablename = 'jnb-win' 
db = 'mdeg.db'
sqlite_engine = create_engine('sqlite:///' + dbdirectory + db)
row = []

cols = ["title", "first_name", "initials", "surname", "address line 1", "address line 2", \
"address line 3", "address line 4", "address line 5", "postcode", "client_urn", "email_address", \
"mobile_number", "landline_number", "ip_address", "digital advertising_cookie", "date of birth", \
"year_of_birth", "building_ird", "contents_ird", "motor_ird", "source_code", "privacy_policy_code", \
"consent_statement_code", "email_date_of_latest_engagement", "email_unsub_bounce_block", "ip_date_of_capture", \
"email__date of consent", "landline__date of consent", "mobile__date of consent", "postal__date of consent", \
"cookie__date of consent", "experian_named_in_pp", "prospect", "linkage", "modelling", "profiling", \
"enrichment", "retail", "automotive ", "lifestyle", "charity", "utility", "telecommunications", \
"insurance ", "publishing / media", "entertainment/gaming/leisure", "public sector", "financial services", \
"travel", "mail order", "health/beauty", "education", "fmcg", "marketing agencies and brokers", \
"email notification date", "filler 2", "filler 3", "filler 4", "filler 5", "female", "male", "date moved in ", \
"date first appeared on file", "owned outright", "mortgaged", "private rental", "social rental", "single", \
"married", "live with spouse or partner"]

upload = pd.DataFrame(columns=cols)

df1 = pd.read_csv(directory + file1, encoding = 'utf-8', low_memory=False)
df2 = pd.read_csv(directory + file2, encoding = 'utf-8', low_memory=False)
df3 = pd.read_csv(directory + file3, encoding = 'utf-8', low_memory=False)

df = pd.concat([df1,df2,df3])
print('All', df.shape)
#df = pd.read_excel(directory + filename)
df.columns = map(str.lower, df.columns)
df = df.apply(lambda x: x.astype(str).str.lower())

for col in list(df.columns):
    df[col] = df[col].str.replace(',','')

df.loc[:,'email_date_of_consent'] = pd.to_datetime(df['email_date_of_consent'],infer_datetime_format=True)
#df.drop_duplicates(subset='email', keep='last', inplace=True)
print('dedup', df.shape)

print(df.columns)
#''zipc' : 'postcode', 'birth date' : 'dob''countryname' : 'country','leaddate' : 'timestamp', 'leadurl' : 'source''emailaddress' : 'email', 'mailaddress1' : 'address','cityname' : 'city','genderid': 'gender',
#'phonenumberhome' : 'phone', 'birthdate' : 'dob','leadipaddress' : 'ip','first_name' : 'firstname', 'last_name' : 'lastname',

#df.rename(columns={ 'town' : 'city', 'mobile' : 'phone'}, inplace=True)
#print(df.columns)

#dob = pd.to_datetime(df['dob'],infer_datetime_format=True, errors='coerce')
#df['dob'] = pd.to_datetime(df['birthdate day']+df['birthdate month']+df['birthdate year'],  format='%d/%m/%Y', infer_datetime_format=True, errors='coerce')

#df['age'] = df['dob'].apply(calculate_age)

#df['title'] = ''
#df['housenumber']  = ''
#df['address']  = ''
#df['street'] = ''
#df['postcode'] = ''
#df['city'] = ''
#df['country'] = ''
#df['phone']  = ''
#df['gender'] = ''
#df['dob'] = ''
#df['age'] = ''
#df['ip'] = ''
#print(list(df.columns))
#final = df[['email', 'title', 'firstname', 'lastname', 'housenumber', 'address', 'street', 'postcode', 'city', 'country', 'phone', 'gender',  'dob', 'age', 'ip', 'timestamp', 'source']]
#print(final.columns)
#print(df.shape)
#with open(dbdirectory +"columnlistbf.csv","w") as f:
#   for filename in os.listdir(directory):
#       df = pd.read_csv(directory + filename,encoding = "utf-8",low_memory=False,nrows=1)
#       a = list(df.columns)
#        b = str(filename)
#       a.insert(0,b)
#        print(a)
#      wr = csv.writer(f)
#      wr.writerow(a)
#df.to_sql(tablename, sqlite_engine, if_exists='replace',chunksize=500, index=False)
