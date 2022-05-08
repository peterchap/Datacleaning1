import os
import glob
import pandas as pd
from sqlalchemy import create_engine

from datetime import datetime, date
import dateparser
os.chdir("E:/MDEG/FPC/")

extension = 'csv'
directory = "E:/MDEG/FPC/"
dbdirectory= 'E:/MDEG/'

tablename = 'FPC_all' 
db = 'mdeg.db'
sqlite_engine = create_engine('sqlite:///' + dbdirectory + db)



all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
print(all_filenames)

'''
colstouse = ['EMAILADDRESS', 'FIRSTNAME', 'GENDER', 'LASTNAME', 'LEADDATE',\
     'LEADURL', 'LEADIPADDRESS', 'MAILADDRESS1', 'PHONENUMBERMOBILE', 'POSTALZIPCODE']
colsrename = {'EMAILADDRESS' : 'email', 'FIRSTNAME' : 'first_name', 'GENDER' : 'gender', 'LASTNAME' : 'surname',\
    'LEADDATE' : 'email_date_of_consent', 'LEADURL' : 'url', 'LEADIPADDRESS' : 'ip_address',\
        'MAILADDRESS1' : 'address line 1', 'PHONENUMBERMOBILE' : 'mobile_number', 'POSTALZIPCODE' : 'postcode'}
'''
cols = ['address line 1', 'address line 2', 'address line 3','address line 4','address line 5']
for f in all_filenames:
    df = pd.read_csv(directory + f,encoding ="utf-8", low_memory=False)
    #df1.rename(columns=colsrename, inplace=True)
    #df = df1[['email', 'first_name', 'surname', 'address line 1', 'postcode', 'mobile_number', 'gender',\
    #    'ip_address', 'email_date_of_consent', 'url']]
    print(f, df.shape)  
    df.drop_duplicates(subset='Email_Address', keep='last', inplace=True)
    df.columns = map(str.lower, df.columns)
    print('dedup', df.shape)
    df.loc[:,'ip_date_of_capture'] = pd.to_datetime(df['ip_date_of_capture'],infer_datetime_format=True)
    df.loc[:,'email__date of consent'] = pd.to_datetime(df['email__date of consent'],infer_datetime_format=True)
    df.loc[:,'landline__date of consent'] = pd.to_datetime(df['landline__date of consent'],infer_datetime_format=True)
    df.loc[:,'mobile__date of consent'] = pd.to_datetime(df['mobile__date of consent'],infer_datetime_format=True)
    df.loc[:,'postal__date of consent'] = pd.to_datetime(df['postal__date of consent'],infer_datetime_format=True)
    df.loc[:,'cookie__date of consent'] = pd.to_datetime(df['cookie__date of consent'],infer_datetime_format=True)
    
    for field in cols:
        #print(df[field].dtype)
        #if df1[field].dtype == 'object' & df1[field] != '':
        df[field] = df[field].astype(str).apply(lambda x: x.replace(',', ''))
        
    df.to_sql(tablename, sqlite_engine, if_exists='append',chunksize=500, index=False)
#combine all files in the list
#combined_csv = pd.concat([pd.read_csv(f, encoding = "ISO-8859-1", low_memory=False) for f in all_filenames ])
#export to csv
#combined_csv.to_csv( "combinedinbox.csv", index=False, encoding='utf-8-sig')
#print(combined_csv.shape)
print("Completed Successfully")