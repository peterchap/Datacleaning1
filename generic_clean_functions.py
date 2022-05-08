import pandas as pd
import numpy as np

from disposable_email_domains import blocklist
from validate_email import validate_email

from datetime import datetime
from datetime import date

email =  'Email'
# Drop null emails

df.dropna(subset=[email],inplace=True)

print("Removed null emails", df.shape)

#Remove invalid emails formats

def remove_invalid_emails(data):
    data.loc[~data['Email'].apply(lambda x:validate_email(x)), 'data flag'] = 'Remove'
    data.loc[~data.Email.str.contains("@",na=False), 'data flag'] ='Remove'
    char = '\+|\*|\'| |\%|,|\"|\/'
    data.loc[df.email.str.contains(char,regex=True), 'data flag'] = 'Remove'
    return data

# Remove Bad Names

new = df[email].str.split(pat="@", expand=True)
df['Left']= new[0]
df['Domain'] = new[1]

def remove_bad_names(data):
    patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|enquir|fake|feedback|finance|free|garbage|generic|hello|info|invalid|\
    junk|loan|office|market|penis|person|phruit|postmaster|random|recep|register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
    data.loc[data['Email'].str.contains(patternDel, na=False), 'data flag'] = 'Remove'
    data.loc[data['FIRSTNAME'].str.contains(patternDel, na=False), 'data flag'] = 'Remove'
    data.loc[data['LASTNAME'].str.contains(patternDel, na=False), 'data flag'] = 'Remove'
    return data


#Remove Temp domains

def remove_temp_domains(data):
    domains = data['Domain']
    domains.drop_duplicates(inplace=True)
    m =[]
    for domain in domains:
        m.append(( domain, domain in blocklist))

    n = pd.DataFrame(m, columns=('Domain', 'Temp'))
    data = pd.merge(data, n, on='Domain', how='left')
    data.loc[data.Temp == 1, 'data flag'] = 'Remove'
    return data

def report_ISP_groups(data):
    ispgroup = pd.read_csv(onedrive+'ISP Group domains.csv')
    data = pd.merge(data, ispgroup, on='Domain', how='left')
    data['Group'].fillna("Other", inplace = True)
    stat = df['Group'].value_counts()
    return stat
    
#d = c - df.shape[0]
#report.append(["Bad names",d])
print("Removed bad names", df.shape)

df = df.sort_values(email, ascending=False)
df = df.drop_duplicates(subset=email, keep='first')

#f = d - df.shape[0]
#report.append(["Duplicates",f])
print("Removed duplicates", df.shape)

df[email] = df[email].str.lower()
new = df[email].str.split(pat="@", expand=True)
df['Left']= new[0]
df['Domain'] = new[1]

patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|enquir|fake|feedback|finance|free|garbage|generic|hello|info|invalid|\
junk|loan|office|market|penis|person|phruit|postmaster|random|recep|register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
df = df[~df["Left"].str.contains(patternDel, na=False)]

#report.append("Removed bad names",df.shape[0])
print("Removed bad names", df.shape)

onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"

ispgroup = pd.read_csv(onedrive+'ISP Group domains.csv')
df = pd.merge(df, ispgroup, on='Domain', how='left')
df['Group'].fillna("Other", inplace = True)

# remove email temp domains
domains = df[df['Group']=="Other"]['Domain']
domains.drop_duplicates(inplace=True)
m =[]
for domain in domains:
    m.append(( domain, domain in blocklist))

n = pd.DataFrame(m, columns=('Domain', 'Temp'))
df = pd.merge(df, n, on='Domain', how='left')
df = df[df.Temp != 1]

#g = f - df.shape[0]
#report.append(["Temp Domains",g])
print("Removed Temp Domains", df.shape)


print(list(df.columns.values))
#report.append(df['Group'].value_counts())
print(df['Group'].value_counts())

    
dob = pd.to_datetime(df['dob'],infer_datetime_format=True, errors='coerce')


def calculate_age(born):
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

age = dob.apply(calculate_age)

bins = [1,17,34,50,70,90,100,200]
s = age.groupby(pd.cut(age, bins=bins)).size()

#print(report)

print(s)

print(df['sex'].value_counts())

#df.to_csv(outputdir+"JUNB_201907_check.csv", index=False)

#Split data into ISP groups 



af = df[df["Group"].str.contains('Other', na=False)]
#af=af[[email, 'title', 'firstname', 'lastname',  'address1', 'address2', 'address3', 'town', 'county','postcode','phone', 'mobile', 'gender','dob','URL', 'IP', 'consent_email']]
#af.to_csv(outputdir+"datamixx_dmlndg2707_Other.csv", index=False)
print("Other Count", af.shape)
bf = df[~df["Group"].str.contains('Other', na=False)]
#bf=bf[[email, 'TITLE', 'FIRSTNAME', 'LASTNAME',  'ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'CITY', 'COUNTY','POSTCODE','PHONE', 'MOBILE', 'GENDER','DOB','URL', 'IP', 'JOINDATE']]
#bf.to_csv(outputdir+"datamixx_dmlndg2707_ISP.csv", index=False)
print("ISP Count", bf.shape)
print("Completed Successfully")