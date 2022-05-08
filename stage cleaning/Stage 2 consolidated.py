import pandas as pd 
from validate_email import validate_email
from datetime import datetime
from datetime import date
from sqlalchemy import create_engine
from ftplib import FTP
from io import StringIO
import io

#Remove invalid email formats

def invalid_emails(data):
    data.loc[~data['email'].apply(lambda x:validate_email(x)), 'data flag'] = 'remove'
    data.loc[~data['email'].apply(lambda x:validate_email(x)), 'status'] = 'Invalid email address'
    data.loc[~data.email.str.contains("@",na=False), 'data flag'] ='remove'
    data.loc[~data.email.str.contains("@",na=False), 'status'] ='Invalid email address'
    char = '\+|\*|\'| |\%|,|\"|\/'
    data.loc[data.email.str.contains(char,regex=True,na=False), 'data flag'] = 'remove'
    data.loc[data.email.str.contains(char,regex=True,na=False), 'status'] = 'Invalid email address'
    return data

# Remove Bad statuss

def junk_emails(data):
    patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|\
        enquir|fake|feedback|finance|free|garbage|generic|hello|info|invalid|\
        junk|loan|office|market|penis|person|phruit|police|postmaster|random|recep|\
        register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
    data.loc[data['email'].str.contains(patternDel, na=False), 'status'] = 'Junk Email'
    data.loc[data['firstname'].str.contains(patternDel, na=False), 'status'] = 'Junk Email'
    data.loc[data['lastname'].str.contains(patternDel, na=False), 'status'] = 'Junk Email'
    return data

def report_ISP_groups(data, ispgroup):
        
    new = data['email'].str.split(pat="@", expand=True)
    data.loc[:,'Left']= new.iloc[:,0]
    data.loc[:,'Domain'] = new.iloc[:,1]

    ispdata = pd.merge(data, ispgroup, on='Domain', how='left')
    ispdata.loc[:,'Group'].fillna("Other", inplace = True)
    stat = pd.DataFrame(ispdata['Group'].value_counts()).reset_index()
    stat.rename(columns={'index' : 'ISP', 'Group' : 'count'}, inplace=True)
    return stat

# End of Functions

directory = 'E:/MDEG/FPC/'
dbdirectory= 'E:/MDEG/'
onedrive= 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/Data Cleaning Project/'
onedrive2 = 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/'


file = 'UK_FPC_all_Mdeg_May20_Stage1Complete.csv'
name = 'fpc_all_mdeg'
statusfile = 'UK_FPC_all_Mdeg_May20_Stage1Complete.csv'

month = 'May20'

sqlselect = '''
SELECT
    email_address, 
    title, 
    first_name, 
    surname, 
    "address line 1", 
    "address line 2", 
    "address line 3", 
    "address line 4", 
    postcode, 
    mobile_number, 
    landline_number,
    female, 
    male, 
     "date of birth", 
      source_code,
       ip_address, 
       "email__date of consent"
FROM FPC_all
WHERE "email__date of consent" > "2019-05-01 00:00:00.000000"
GROUP BY email_address;
'''

db = 'mdeg.db'
sqlite_engine = create_engine('sqlite:///' + dbdirectory + db)


df = pd.read_sql(sqlselect, sqlite_engine)

renamecols = {'email_address' : 'email', 'first_name' : 'firstname',  'surname' : 'lastname',\
    'address line 1' : 'address1', 'address line 2' : 'address2','address line 3' : 'address3',\
    'address line 4' : 'county','landline_number' : 'phone', 'mobile_number' : 'mobile',\
        'date of birth' : 'dob', 'source_code' : 'url', 'ip_address' : 'ip', 'email__date of consent' : 'joindate'}
df.rename(columns=renamecols,inplace=True)

if df['female'].empty:
    df['gender'] = df['female']
elif df['male'].empty:
    df['gender'] = df['male']
else:
    df['gender'] = ''

#df1 = pd.read_csv(directory + file, encoding ='utf-8')
#df1['email'] = df1['email'].str.lower()
#cols = ['last_name', 'first_name', 'street_name']
#for field in cols:
#    print(df1[field].dtype)
#    #if df1[field].dtype == 'object' & df1[field] != '':
#    df1[field] = df1[field].astype(str).apply(lambda x: x.replace(',', ''))

print(df.shape)
df.drop_duplicates(subset=['email'], inplace=True)
print(df.shape)

df2 = pd.read_csv(directory + statusfile, usecols= ['email', 'status', 'data flag'])
ispgroups = pd.read_csv(onedrive2+'ISP Group domains.csv',encoding = "ISO-8859-1")

print('Gross', df2.shape[0])
df2 = df2.drop_duplicates(subset='email', keep='first')
print("All Data Flag", df2.shape[0])

df3 = pd.merge(df, df2, left_on='email', right_on='email', how='left')
print(df3.columns)
print(df3.shape)

new = df3['email'].str.split(pat="@", expand=True)
df3.loc[:,'Left']= new.iloc[:,0]
df3.loc[:,'Domain'] = new.iloc[:,1]
df =  pd.merge(df3, ispgroups, left_on='Domain', right_on='Domain', how='left')

df3 = invalid_emails(df3)    
df4 = junk_emails(df3)

# Generate Cleaning report data

print(df4.shape[0])
dataflags = df4['data flag'].value_counts()
report1 = dataflags.rename_axis('Description').reset_index(name='Count')
print(report1)
statusflags= df4['status'].value_counts()
report2 = statusflags.rename_axis('Description').reset_index(name='Count')
print(report2)
#Output ESB import file in correct 


df7 = df4[df4['data flag'] != 'remove']
print('eamils1',df7.shape)
#df7['timestamp2'] = pd.to_datetime(df7['timestamp'],infer_datetime_format=True) 
#df8 = df7[df7['timestamp2'] > '01/05/2019 00:00']
print('emails', df7.shape)
print('df7',df7.columns)    

to_mdropcols = [ 'status', 'data flag', 'Left', 'Domain']
df7.drop(to_mdropcols, axis=1, inplace=True)
'''
renamecols = {'first_name' : 'firstname', 'last_name' : 'lastname', 'date_of_birth' : 'dob',\
     'street_name' : 'address1', 'postal_code' : 'postcode', 'state' : 'county',\
   'suburb' : 'city', 'mobile_nr' : 'mobile', 'ipaddress' : 'ip', 'timestamp2' : 'joindate'}
df8.rename(columns=renamecols, inplace=True)

df8['title'] = ''
df8['address2'] = ''
df8['address3'] = ''
df8['phone'] = ''
df8['url'] = ''
'''
df7['city'] = ''
y = df7[['email','title', 'firstname', 'lastname', 'address1', 'address2', 'address3',\
    'city', 'county', 'postcode', 'phone', 'mobile', 'gender', 'dob','url','ip','joindate']]

print(y.shape)
print(y.columns)


ftp = FTP('ftp.emailswitchboard.co.uk')
ftp.login('ems_data', 'em@1lsw1tchb0@rd')
ftp.cwd('/import/raw')
buffer = StringIO()
y.to_csv(buffer, index=False)
text = buffer.getvalue()
bio = io.BytesIO(str.encode(text))
ftp.storbinary('STOR ' + name, bio)


"""
df4.loc[:,'timestamp'] = pd.to_datetime(df4['timestamp'],infer_datetime_format=True) #errors='coerce'
df4.loc[:,'date_of_birth'] = pd.to_datetime(df4['date_of_birth'],infer_datetime_format=True)

def calculate_age(born):
    today=date.today()
    return today.year - born.year -((today.month, today.day) < (born.month, born.day))

df4.loc[:,'age'] = df4['date_of_birth'].apply(calculate_age)
print(df4['age'].head(5))

bins = [18,34,50,200]
df4['bin'] =pd.cut(df4['age'], bins)
print(df4.columns)
df4['year'] = df4['timestamp'].dt.year
print(df4['year'].value_counts(dropna=False))     
print(df4['gender'].value_counts())
    
   # y = df7[['email','title', 'firstname', 'lastname', 'address1', 'address2', 'address3',\
   # 'city', 'county', 'postcode', 'phone', 'mobile', 'gender', 'dob',\
    #'source','ip','timestamp'] ]
    
    #z=y.rename(columns={'source' : 'url', 'timestamp' : 'joindate'})
    
counts = df4['status'].value_counts().rename_axis('status').reset_index(name='counts')
counts.to_csv(directory + file[:-4] +  month + "Statuscounts.csv", index=False)
optins = df4['year'].value_counts(dropna=False).rename_axis('optin year').reset_index(name='counts')
optins.to_csv(directory + file[:-4] +  month + "optincounts.csv", index=False) 
dob = df4['bin'].value_counts().rename_axis('age band').reset_index(name='band')
dob.to_csv(directory + file[:-4] +  month + "dobcounts.csv", index=False)
gender = df4['gender'].value_counts().rename_axis('gender').reset_index(name='counts')
gender.to_csv(directory + file[:-4] +  month + "gendercounts.csv", index=False)      
            
ispstats = pd.DataFrame(report_ISP_groups(df7,ispgroups))
print(ispstats)

ispstats.to_csv(directory + file[:-4] + "_ispstats_" + month + ".csv", index=True)

print('Postcode count', (df4['postal_code'].notnull()).shape)
"""