import pandas as pd
import numpy as np
import glob


from disposable_email_domains import blocklist
from validate_email import validate_email

#Functions

#Remove invalid email formats

def remove_invalid_emails(data):
    data.drop(data[~data[email].apply(lambda x:validate_email(x))].index, inplace=True)
    data.drop(data[~data[email].str.contains("@",na=False)].index, inplace=True)
    char = '\+|\*|\'| |\%|,|\"|\/'
    data.drop(data[data[email].str.contains(char,regex=True,na=False)].index, inplace=True)
    return data

# Remove Bad Names

def remove_bad_names(data):
    patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|enquir|fake|\
    feedback|finance|free|fuck|garbage|generic|hello|info|invalid|\
    junk|loan|office|market|penis|person|phruit|postmaster|random|\
    recep|register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
 #   data.drop(data[data[email].str.contains(patternDel, na=False)].index, inplace=True)
#    data.drop(data[data['lastname'].str.contains(patternDel, na=False)].index, inplace=True)
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

    data2.drop(data2[data2.Temp == 1].index, inplace=True)
    return data2

def report_ISP_groups(data, ispgroup,email):
        
    new = data[email].str.split(pat="@", expand=True)
    data.loc[:,'Left']= new.iloc[:,0]
    data.loc[:,'Domain'] = new.iloc[:,1]

    ispdata = pd.merge(data, ispgroup, on='Domain', how='left')
    ispdata.loc[:,'Group'].fillna("Other", inplace = True)
    stat = pd.DataFrame(ispdata['Group'].value_counts()).reset_index()
    stat.rename(columns={'index' : 'ISP', 'Group' : 'count'}, inplace=True)
    return stat

# End of Functions

#directory = 'C:/Users/Peter/Downloads/'
#onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"

directory = "C:/Users/Peter Chaplin/Downloads/"
onedrive="C:/Users/Peter Chaplin/OneDrive - ESB Connect/"

month = 'oct22-19'
file = '2019-10-18_DMX_Irma_120K_6-12months_1614_1571387118_1.csv'

email = 'Email_Address'
bad =['FOREIGN', 'UNKNOWN', 'NO MX', 'EXCLUDED', 'BAD', 'BLACKLISTED','SPAM TRAP', 'EXPIRED', 'Not Set', 'TEMP', 'INVALID']


ispgroups = pd.read_csv(onedrive+'ISP Group domains.csv',encoding = "ISO-8859-1")
domainstatus = pd.read_csv(onedrive + "domain_status_UK.csv",encoding = "ISO-8859-1",low_memory=False)

#Start processing

df1 = pd.read_csv(directory + file, encoding= "ISO-8859-1",sep= ",", low_memory=False)
print(list(df1.columns.values))
# remove characters such as commas:
cols = ['Lead_ID','Email_Address','participation_pk','city','zipcode','street_no_num','street_num']

 #pass them to df.replace(), specifying each char and it's replacement:    
df1[cols] = df1[cols].replace({'\$': '', ',': ''}, regex=True)
df1[email] = df1[email].astype(str).str.lower()
print('Gross', df1.shape[0])
df1 = df1.drop_duplicates(subset=email, keep='first')
print("Net", df1.shape[0])
df1 = remove_invalid_emails(df1)
df1 = remove_bad_names(df1)
    
print("Deletes", df1.shape[0])
new = df1[email].str.split(pat="@", expand=True)
df1['domain'] = new[1]
df = pd.merge(df1, domainstatus, on=['domain'], how='left')
df.drop(df[df['status'].isin(bad)].index, inplace=True)
df1 = remove_temp_domains(df1)
print("Removed", df.shape[0])
nulls = df[df['status'].isnull()]
print("Nulls",nulls.shape[0])

df.drop(df[df['status'].isnull()].index, inplace=True)
print("Final", df.shape[0])    
df['title'] = ''
df['firstname'] = ''
df['lastname'] = ''
df['address3'] = ''
df['county'] = ''   
df['phone'] = ''
df['mobile'] = ''
df['gender'] = ''
df['dob'] = ''
df['consent_source'] = ''
df['ip'] = ''
df['consent_email'] = ''
final = df[[email,'title', 'firstname','lastname', 'street_num', 'street_no_num','address3','city',\
'county','zipcode','phone', 'mobile', 'gender','dob','consent_source', 'ip', 'consent_email']]


final.to_csv(directory+ "clean_egentic.csv", index=False)
nulls.to_csv(directory + "nulls_egentic.csv", index=False)


 
            
            

