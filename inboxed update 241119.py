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
    data.loc[data['first_name'].str.contains(patternDel, na=False), 'data flag'] = 'Remove'
    data.loc[data['last_name'].str.contains(patternDel, na=False), 'data flag'] = 'Remove'
    return data

#Remove Temp Domains
    
def remove_temp_domains(data):
    domains = data['domain']
    domains.drop_duplicates(inplace=True)
    m =[]
    
    for domain in domains:
        m.append(( domain, domain in blocklist))
    
    n = pd.DataFrame(m, columns=('domain', 'Temp'))
    data.domain = data.domain.astype(str)
    n.domain = n.domain.astype(str)
    
    data2 = data.merge(n, on='domain', how='left')

    data2.loc[data2.Temp == 1, 'data flag'] = 'Remove'
    return data2

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

directory = "E:/Cleaning-todo/"
onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"

filename = "LM ESB UK 25.09.2019_inboxed.csv"

month = 'Nov19' 


#Start processing
touse = ['email', 'optin_date', 'is_duplicate', 'is_ok', 'is_blacklisted', 'is_banned_word',\
'is_banned_domain', 'is_complaint', 'is_hardbounce', 'domain', 'user_status', 'last_open',\
'last_click', 'system_created',	'master_filter', 'import_filter', 'email_id',\
'primary_membership_id', 'primary_membership', 'status', 'rtotal', 'data flag']
df2 = pd.read_csv(directory + "LM ESB UK 25.09.2019_inboxed_Nov19_all_data_flag_Nov19.csv",usecols=touse)
ispgroups = pd.read_csv(onedrive+'ISP Group domains.csv',encoding = "ISO-8859-1")

print('Gross', df2.shape[0])
df2 = df2.drop_duplicates(subset='email', keep='first')
df2.replace(',','', regex=True, inplace=True)

print("All Data Flag", df2.shape[0])

inputcheck = 0
outputcheck = 0
ispstats = pd.DataFrame()
filecounts = pd.DataFrame(columns=['List','Type','Count'])

df1 = pd.read_csv(directory + filename, encoding = "ISO-8859-1",low_memory=False)
df1.replace(',','', regex=True, inplace=True)
df1.columns = map(str.lower, df1.columns)
print(filename,df1.shape[0])

df = pd.merge(df1, df2, left_on='email', right_on='email', how='left') 

    
# get domain analysis stats
    
e = pd.DataFrame(df['status'].value_counts()).reset_index()
e.rename(columns={'index' : 'reason', 'status' : 'count'}, inplace=True)
print(e.shape)
   
        
nulls = df[df['data flag'].isnull()]
print( "Nulls",nulls.shape[0])
a = df['data flag'].value_counts(dropna='False')

df = remove_invalid_emails(df)
b = df['data flag'].value_counts() 
stats = pd.DataFrame.from_dict([['Invalid Emails', str(b[2] - a[2])]])
     
df = remove_bad_status(df)
c = df['data flag'].value_counts()
stats = stats.append(pd.DataFrame.from_dict([['Bad status', str(c[2] - b[2])]]))
    
df = remove_temp_domains(df)
d = df['data flag'].value_counts()
stats = stats.append(pd.DataFrame.from_dict([['Temp Domains', str(d[2] - c[2])]]))
stats.columns = [ 'reason', 'count']

#generate remove reason file
removed = df[df['data flag'] == 'Remove']
print(removed.columns)
to_dropremove = ['first_name', 'last_name', 'date_of_birth', 'ipaddress', 'timestamp',  'source',\
'optin_date','is_duplicate', 'is_ok',  'domain','user_status', 'last_open', 'last_click', 'system_created',\
'master_filter', 'import_filter', 'email_id',  'rtotal','data flag','Temp' ]
removed.drop(to_dropremove, axis=1, inplace=True)
print('Removed', removed.shape)
print(removed.columns)

print(stats)  
    
for item in ['is_blacklisted', 'is_banned_word', 'is_banned_domain', 'is_complaint', 'is_hardbounce']:
    f = df[item].value_counts()[1]
    
    
    g = pd.DataFrame.from_dict([[item[3:], str(f)]])    
    g.columns = [ 'reason', 'count']
    stats = stats.append(g)
    
   
    stats = stats.append(e)

print(stats)
#stats.to_csv(directory + "removestats_all_"+ month + ".csv", index=True)           

to_dropcols = ['optin_date','is_duplicate', 'is_ok',  'domain','user_status', 'last_open', 'last_click', 'system_created',\
'master_filter', 'import_filter', 'email_id',  'rtotal','Temp' ]

df.drop(to_dropcols, axis=1, inplace=True)
df = df[df['data flag'] != 'Remove']    
df['title'] = ""  
df['address1'] = ""
df['address2'] = ""
df['address3'] = ""
df['town'] = ""
df['county'] = ""
df['postcode'] = ""
df['phone'] = ""
df['mobile'] = "" 
print('Data flags', df['data flag'].value_counts())   
to_mdropcols = ['data flag','primary_membership_id','primary_membership']
df.drop(to_mdropcols, axis=1, inplace=True)    
    

    
for i, x in df.groupby(['gender']):
    print(i)
    print(x.columns)
    print(x.shape)      
    y = x[['email','title', 'first_name', 'last_name', 'address1', 'address2', 'address3',\
    'town', 'county', 'postcode', 'phone', 'mobile', 'gender', 'date_of_birth',\
    'source','ipaddress','timestamp'] ]
    
    y.rename(columns={'source' : 'url',\
    'ipaddress' : 'ip', 'timestamp' : 'joindate'}, inplace=True)
    print(y.columns)
    y.to_csv(directory + i + "LM ESB UK 25.09.2019_inboxed_"+  month + ".csv", index=False)
            
ispstats = pd.DataFrame(report_ISP_groups(df,ispgroups))
print(ispstats)
print(filecounts)

ispstats.to_csv(directory + "LM ESB UK 25.09.2019_ipstats_all_"+ month + ".csv", index=True)
filecounts.to_csv(directory + "LM ESB UK 25.09.2019_filecounts_all_" + month + ".csv", index=False)