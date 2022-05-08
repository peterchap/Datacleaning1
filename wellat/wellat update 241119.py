import pandas as pd
from disposable_email_domains import blocklist
from validate_email import validate_email

#Functions

#Remove invalid email formats

def remove_invalid_emails(data):
    data.loc[~data['email'].apply(lambda x:validate_email(x)), 'data flag'] = 'Remove'
    data.loc[~data.email.str.contains("@",na=False), 'data flag'] ='Remove'
    char = '\+|\*|\'| |\%|,|\"|\/'
    data.loc[df.email.str.contains(char,regex=True,na=False), 'data flag'] = 'Remove'
    return data

# Remove Bad statuss

def remove_bad_status(data):
    patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|enquir|fake|feedback|finance|free|garbage|generic|hello|info|invalid|\
    junk|loan|office|market|penis|person|phruit|postmaster|random|recep|register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
    data.loc[data['email'].str.contains(patternDel, na=False), 'data flag'] = 'Remove'
    data.loc[data['firstname'].str.contains(patternDel, na=False), 'data flag'] = 'Remove'
    data.loc[data['lastname'].str.contains(patternDel, na=False), 'data flag'] = 'Remove'
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

directory = "E:/Wellat/"
onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"

filename = "datamixx_feb250220_12mthoptin.csv"

month = 'Feb20' 


#Start processing
df2 = pd.read_csv(directory + "datamixx_feb250220_12mthoptinfeb20Stage1Complete.csv",\
    usecols=['email','status'])
ispgroups = pd.read_csv(onedrive+'ISP Group domains.csv',encoding = "ISO-8859-1")

print('Gross', df2.shape[0])
df2 = df2.drop_duplicates(subset='email', keep='first')


print("All Status Flag", df2.shape[0])

print(df2['status'].value_counts())

df2 = df2[df2['status'] == 'OK']

print('OK', df2.shape[0])

inputcheck = 0
outputcheck = 0
ispstats = pd.DataFrame()
filecounts = pd.DataFrame(columns=['List','Type','Count'])

df1 = pd.read_csv(directory + filename, encoding = "ISO-8859-1",low_memory=False)
print(filename,df1.shape[0])

df = pd.merge(df1, df2, left_on='email', right_on='email', how='inner') 
print(df.columns)
   
# get domain analysis stats
    
e = pd.DataFrame(df['status'].value_counts()).reset_index()
e.rename(columns={'index' : 'reason', 'status' : 'count'}, inplace=True)
print(e.shape)
   
        
nulls = df[df['status'].isnull()]
print( "Nulls",nulls.shape[0])


print(df.columns)   


    
for i, x in df.groupby(['supplier_code']):
    print(i)    
    to_mdropcols = ['dmlurn', 'status', 'supplier_code']
    x.drop(to_mdropcols, axis=1, inplace=True)
    
    x['phone'] = ""
    x['mobile'] = ""
    x['gender'] = ""
    y = x[['email','title', 'firstname', 'lastname', 'address1', 'address2', 'address3',\
    'town', 'county', 'postcode', 'phone', 'mobile', 'gender', 'dob', 'consent_source',\
    'consent_ipv4', 'consent_email'] ]
    
    y.rename(columns={ 'town' :'city', 'consent_source' : 'url',\
    'consent_ipv4' : 'ip', 'consent_email' : 'joindate'}, inplace=True)
    print(y.columns)
    print(i,y.shape[0])
    y.to_csv(directory + i + "_"+  month + ".csv", index=False)
            
ispstats = pd.DataFrame(report_ISP_groups(df,ispgroups))
print(ispstats)
print(filecounts)

ispstats.to_csv(directory + "ipstats_all_"+ month + ".csv", index=True)
filecounts.to_csv(directory + "filecounts_all_" + month + ".csv", index=False)