import ast
import pandas as pd 
from validate_email import validate_email
from datetime import datetime
from datetime import date
from ast import literal_eval

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
    patternDel = "abuse|backup|cancel|comp|crap|fake|free|garbage|generic|invalid|\
    junk|loan|penis|person|phruit|police|random|\
    register|shit|shop|signup|spam|stuff|survey|test|trash|xx"
    data.loc[data['email'].str.contains(patternDel, na=False), ['status', 'data flag']] = ['Junk Email', 'remove']
    data.loc[data['firstname'].str.contains(patternDel, na=False), ['status', 'data flag']] = ['Junk Email', 'remove']
    data.loc[data['lastname'].str.contains(patternDel, na=False), ['status', 'data flag']] = ['Junk Email', 'remove']
    return data
def role_based_emails(data):
    patternDel = "account|admin|career|contact|email|enquir|feedback|finance|hello|info|\
        office|market|postmaster|recep|sales|support|webmaster"
    data.loc[data['email'].str.contains(patternDel, na=False), 'status'] = 'Role based Email'
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

def explodedict(row):
    row['custom_attributes'] = row['custom_attributes'].replace('=>', ':')
    row['custom_attributes'] = ast.literal_eval((row['custom_attributes']))
    for key in row['custom_attributes']:
        row[key] = row['custom_attributes'][key]
    return(row)

# End of Functions

directory = 'E:/Cleaning-todo/'
onedrive= 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/'

file = '23022021 - Full FF Opted in Database.csv'
statusfile = '23022021 - Full FF Opted in DatabaseFeb21Stage1Completev2.csv'

month = 'Feb21'

readcols = ['email', 'uuid', 'phone_number', 'joined_at', 'firstname', 'lastname', 'gender',\
    'custom_attributes']

df1 = pd.read_csv(directory + file, encoding ='utf-8', usecols= readcols)

df1 = df1.apply(lambda row: explodedict(row), axis=1)
print(df1.shape)
df1.drop_duplicates(subset=['email'], inplace=True)
print(df1.shape)

df2 = pd.read_csv(directory + statusfile, usecols= ['email','owner', 'status', 'data flag'])
ispgroups = pd.read_csv(onedrive+'ISP Group domains.csv',encoding = "ISO-8859-1")

print('Gross', df2.shape[0])
df2 = df2.drop_duplicates(subset='email', keep='first')
print("All Data Flag", df2.shape[0])

df3 = pd.merge(df1, df2, left_on='email', right_on='email', how='left')
print(df3.columns)
print(df3.shape)
'''
new = df3['email'].str.split(pat="@", expand=True)
df3.loc[:,'Left']= new.iloc[:,0]
df3.loc[:,'Domain'] = new.iloc[:,1]
df =  pd.merge(df3, ispgroups, left_on='Domain', right_on='Domain', how='left')
'''
df3 = invalid_emails(df3)    
df4 = junk_emails(df3)
df4 = role_based_emails(df4)
# Generate Cleaning report data

print(df4.shape[0])
dataflags = df4['data flag'].value_counts()
report1 = dataflags.rename_axis('Description').reset_index(name='Count')
print(report1)
statusflags= df4['status'].value_counts()
report2 = statusflags.rename_axis('Description').reset_index(name='Count')
print(report2)
#Output ESB import file in correct 


#df4['custom_attributes'] = df4['custom_attributes'].apply(literal_eval)
#df5 = pd.DataFrame([y for x in df4['custom_attributes'].values.tolist() for y in x])
#df = pd.DataFrame([dict(y, ID=i) for i, x in df.values.tolist() for y in x])


df7 = df4[df4['data flag'] != 'remove']
print('emails', df7.shape)
print(df7.columns)
df7.to_csv(directory + file[:-4] +  month + "cleaned_original.csv", index=False)
df7['address1'] = ''
df7['address2'] = ''
df7['address3'] = ''
df7['city'] = ''
df7['county'] = ''
df7['url'] = ''
df7['IP'] = ''
df8 = df7[['email', 'title', 'firstname', 'lastname', 'address1', 'address2',\
    'address3', 'city', 'county', 'postcode', 'phone_number', 'mobile_phone',\
    'gender', 'date_of_birth', 'url', 'IP', 'joined_at']]
renamecols = {'phone_number' : 'phone', 'mobile_phone' : 'mobile',\
    'joined_at' : 'joindate'}
df9 = df8.rename(columns = renamecols).copy()
df9.to_csv(directory + file[:-4] +  month + "cleaned_ESBformat.csv", index=False)    
'''
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
'''    
counts = df4['status'].value_counts().rename_axis('status').reset_index(name='counts')
counts.to_csv(directory + file[:-4] +  month + "Statuscounts.csv", index=False)
'''
optins = df4['year'].value_counts(dropna=False).rename_axis('optin year').reset_index(name='counts')
optins.to_csv(directory + file[:-4] +  month + "optincounts.csv", index=False) 
dob = df4['bin'].value_counts().rename_axis('age band').reset_index(name='band')
dob.to_csv(directory + file[:-4] +  month + "dobcounts.csv", index=False)
gender = df4['gender'].value_counts().rename_axis('gender').reset_index(name='counts')
gender.to_csv(directory + file[:-4] +  month + "gendercounts.csv", index=False)      
            
ispstats = pd.DataFrame(report_ISP_groups(df7,ispgroups))
print(ispstats)
'''
#ispstats.to_csv(directory + file[:-4] + "_ispstats_" + month + ".csv", index=True)

#print('Postcode count', (df4['postal_code'].notnull()).shape)
