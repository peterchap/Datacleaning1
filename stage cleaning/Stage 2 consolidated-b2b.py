import pandas as pd 
from validate_email import validate_email
from datetime import datetime
from datetime import date

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
    data.loc[data['first_name'].str.contains(patternDel, na=False), 'status'] = 'Junk Email'
    data.loc[data['last_name'].str.contains(patternDel, na=False), 'status'] = 'Junk Email'
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

directory = 'E:/MDEG/affilyads/'
onedrive= 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/'

file = 'MDEG_AA_07.05.csv'
statusfile = 'MDEG_AA_07.05May20Stage1Complete.csv'

month = 'May20'

df1 = pd.read_csv(directory + file, encoding ='utf-8')

print(df1.shape)
df1.drop_duplicates(subset=['email'], inplace=True)
print(df1.shape)

df2 = pd.read_csv(directory + statusfile, usecols= ['email', 'status', 'data flag'])
ispgroups = pd.read_csv(onedrive+'ISP Group domains.csv',encoding = "ISO-8859-1")

print('Gross', df2.shape[0])
df2 = df2.drop_duplicates(subset='email', keep='first')
print("All Data Flag", df2.shape[0])

df3 = pd.merge(df1, df2, left_on='email', right_on='email', how='left')
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
print('emails', df7.shape)
print(df7.columns)    

to_mdropcols = ['data flag']
df7.drop(to_mdropcols, axis=1, inplace=True)

df4.loc[:,'timestamp'] = pd.to_datetime(df4['timestamp'],infer_datetime_format=True) #errors='coerce'
df4.loc[:,'date_of_birth'] = pd.to_datetime(df4['date_of_birth'],infer_datetime_format=True)

def calculate_age(born):
    today=date.today()
    return today.year - born.year -((today.month, today.day) < (born.month, born.day))

df4['age'] = df4['date_of_birth'].apply(calculate_age)
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
