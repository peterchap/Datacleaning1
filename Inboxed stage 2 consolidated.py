import pandas as pd 
from validate_email import validate_email
from datetime import datetime
from datetime import date

from ftplib import FTP
from io import StringIO
import io

#Remove invalid email formats

def remove_invalid_emails(data):
    data.loc[~data['email'].apply(lambda x:validate_email(x)), 'data flag'] = 'remove'
    data.loc[~data['email'].apply(lambda x:validate_email(x)), 'status'] = 'Invalid email address'
    data.loc[~data.email.str.contains("@",na=False), 'data flag'] ='remove'
    data.loc[data.email.str.contains("@",na=False), 'status'] ='Invalid email address'
    char = '\+|\*|\'| |\%|,|\"|\/'
    data.loc[data.email.str.contains(char,regex=True,na=False), 'data flag'] = 'remove'
    data.loc[data.email.str.contains(char,regex=True,na=False), 'status'] = 'Invalid email address'
    return data

# Remove Bad statuss

def remove_bad_status(data):
    patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|\
        enquir|fake|feedback|finance|free|garbage|generic|hello|info|invalid|\
        junk|loan|office|market|penis|person|phruit|police|postmaster|random|recep|\
        register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
    data.loc[data['email'].str.contains(patternDel, na=False), 'status'] = 'Bad name'
    data.loc[data['firstname'].str.contains(patternDel, na=False), 'status'] = 'Bad name'
    data.loc[data['lastname'].str.contains(patternDel, na=False), 'status'] = 'Bad name'
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

directory = 'E:/inboxed/'
onedrive= 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/'

file1 = 'FR LM ESB 02.03.2020.csv'
file2 = 'FR LM ESB 02.03.2020mar20Stage1Complete.csv'
month = 'March20'

df1 = pd.read_csv(directory + file1, encoding ='utf-8')

print(df1.shape)


df1.drop_duplicates(subset=['email'], inplace=True)
print(df1.shape)

df2 = pd.read_csv(directory + file2, encoding ='ISO-8859-1',\
        usecols=['email', 'domain', 'status',  'data flag'])

print(df2.shape)


ispgroups = pd.read_csv(onedrive+'ISP Group domains.csv',encoding = "ISO-8859-1")



df3 = pd.merge(df1, df2, left_on='email', right_on='email', how='left')
print(df3.columns)
print(df3.shape)

df3 = remove_invalid_emails(df3)    
df4 = remove_bad_status(df3)

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

df7['title'] = ""
df7['address1'] = ""
df7['address2'] = ""
df7['address3'] = ""
df7['city'] = ""
df7['county'] = ""
df7['postcode'] = ""
df7['phone'] = ""
df7['mobile'] = ""


to_mdropcols = ['data flag']
df7.drop(to_mdropcols, axis=1, inplace=True)

df7['dob'] = pd.to_datetime(df7['dob'],infer_datetime_format=True, errors='coerce')

def calculate_age(born):
    today=date.today()
    return today.year - born.year -((today.month, today.day) < (born.month, born.day))

df7['age'] = df7['dob'].apply(calculate_age)
print(df7['age'].head(5))

bins = [18,34,50,200]
df7['bin'] =pd.cut(df7['age'], bins)
       
for i, x in df7.groupby(['gender']):
    #print(i)
    #print(x.columns)
    #print(x.shape)      
    y = x[['email','title', 'firstname', 'lastname', 'address1', 'address2', 'address3',\
    'city', 'county', 'postcode', 'phone', 'mobile', 'gender', 'dob',\
    'source','ip','timestamp'] ]
    
    z=y.rename(columns={'source' : 'url', 'timestamp' : 'joindate'})
    print(z.columns)
    print(i,z.shape[0])
    
    
    ftp = FTP('ftp.emailswitchboard.co.uk')
    ftp.login('ems_data', 'em@1lsw1tchb0@rd')
    ftp.cwd('/import/raw')
    buffer = StringIO()
    z.to_csv(buffer, index=False)
    text = buffer.getvalue()
    bio = io.BytesIO(str.encode(text))
    ftp.storbinary('STOR ' + str(i) + file1, bio)

            
ispstats = pd.DataFrame(report_ISP_groups(df7,ispgroups))
print(ispstats)

ispstats.to_csv(directory + file1[:-4] + "_ispstats_" + month + ".csv", index=True)