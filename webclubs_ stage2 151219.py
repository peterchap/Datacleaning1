import pandas as pd

from validate_email import validate_email

#Functions

#Remove invalid email formats

def remove_invalid_emails(data):
    data.loc[~data['email'].apply(lambda x:validate_email(str(x))), 'data flag'] = 'remove'
    data.loc[~data.email.str.contains("@",na=False), 'status'] ='Invalid email address'
    char = '\+|\*|\'| |\%|,|\"|\/'
    data.loc[df.email.str.contains(char,regex=True,na=False), 'status'] = 'Invalid email address'
    return data

# Remove Bad statuss

def remove_bad_status(data):
    patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|enquir|fake|feedback|finance|free|garbage|generic|hello|info|invalid|\
    junk|loan|office|market|penis|person|phruit|postmaster|random|recep|register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
    data.loc[data['email'].str.contains(patternDel, na=False), 'status'] = 'Bad name'
    data.loc[data['fname'].str.contains(patternDel, na=False), 'status'] = 'Bad name'
    data.loc[data['lname'].str.contains(patternDel, na=False), 'status'] = 'Bad name'
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

directory = 'E:/Cleaning-todo/'
onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"

filestage1 = "ESB_Webclubs_standard_export_2019-12-12jan20Stage1Complete.csv"
fileoriginal = 'ESB_Webclubs_standard_export_2019-12-12_12mthselection.csv'
month = '15Dec19' 


#Start processing

df1 = pd.read_csv(directory + fileoriginal, encoding = "ISO-8859-1",low_memory=False)
df1.rename(columns={'Email' : 'email'},inplace=True)
df1.replace(',',' ', regex=True, inplace=True)
df1.columns = map(str.lower, df1.columns)
df1['email'] = df1['email'].str.lower()
print(fileoriginal,df1.shape[0])

touse = ['email', 'domain', 'status',  'data flag']
df2 = pd.read_csv(directory + filestage1,usecols=touse)

ispgroups = pd.read_csv(onedrive+'ISP Group domains.csv',encoding = "ISO-8859-1")

print('Gross', df2.shape[0])


df = pd.merge(df1, df2, left_on='email', right_on='email', how='left')

df = remove_invalid_emails(df)
    
df = remove_bad_status(df)

# Generate Cleaning report data

print(df.shape[0])
dataflags = df['data flag'].value_counts(dropna=False)
report1 = dataflags.rename_axis('Description').reset_index(name='Count')
print(report1)
statusflags= df['status'].value_counts(dropna=False)
report2 = statusflags.rename_axis('Description').reset_index(name='Count')

report1.to_csv(directory + fileoriginal[:-4] + "_dataflags_" + month + ".csv", index=True)
report2.to_csv(directory + fileoriginal[:-4] + "_removestatus_" + month + ".csv", index=True)

print(report2)
#Output ESB import file in correct format

#print(df.columns)

df = df[df['data flag'] != 'remove']    
print("Final count", df.shape[0])

#df['postcode'] = df.loc['outpc'] + " " + df.loc['inpc']
df['postcode']=df.apply(lambda x:'%s %s' % (x['outpc'],x['inpc']),axis=1)
df['url'] = "web-clubs.co.uk"
   
to_mdropcols = ['data flag']
df.drop(to_mdropcols, axis=1, inplace=True)    
       
 
y = df[['email','title', 'fname', 'lname', 'ad1', 'ad2', 'ad3',\
    'town', 'county', 'postcode', 'tel', 'tel_mobile', 'gender', 'dob',\
    'url','ipaddress','create'] ]
    
y.rename(columns={'fname' : 'firstname', 'lname' : 'lastname', 'ad1' : 'address1',\
    'ad2' : 'address2','ad3' : 'address3', 'town' : 'city', 'tel' : 'phone',\
    'tel_mobile' : 'mobile', 'ipaddress' : 'ip', 'create' : 'joindate'}, inplace=True)
print(y.columns)
y.to_csv(directory + fileoriginal[:-4] + month + "_final.csv", index=False)
            
ispstats = pd.DataFrame(report_ISP_groups(df,ispgroups))
print(ispstats)

ispstats.to_csv(directory + fileoriginal[:-4] + "_ispstats_" + month + ".csv", index=True)
