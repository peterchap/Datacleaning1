import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date
import dateparser

from disposable_email_domains import blocklist
from validate_email import validate_email

directory = "C:/Users/Peter/downloads/"
file = "datamixx_uk_tt.csv"
outputdir = "C:/Users/Peter/downloads/"

report = pd.DataFrame(columns = ['Name', 'Count'])

df = pd.read_csv(directory+file,sep=',')
print(df.shape)

df = df[df['Created'] > '2018-6-19 00:00:00']
print("Emails less than 12 months", df.shape[0])

print(list(df.columns.values))
a =df.shape[0]
report = report.append(["Gross",a])
print("Gross file", df.shape)

df.dropna(subset=['Email'],inplace=True)

b = a - df.shape[0]
report = report.append(["Null Email",b])
print("Removed null emails", df.shape)

df['is_valid_email'] = df['Email'].apply(lambda x:validate_email(x))
df = df[df['is_valid_email']]
print("Removed invalid email formats", df.shape)

df = df[df.Email.str.contains("@",na=False)]
char = '\+|\*|\'| |\%|,|\"|\/'
df = df[~df['Email'].str.contains(char,regex=True)]

c = b - df.shape[0]
report = report.append(["Invalid emailformats",c])
print("Removed invalid email addresses", df.shape)


new = df["Email"].str.split(pat="@", expand=True)
df['Left']= new[0]
df['Domain'] = new[1]

patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|enquir|fake|feedback|finance|free|garbage|generic|hello|info|invalid|\
junk|loan|office|market|penis|person|phruit|postmaster|random|recep|register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
df = df[~df["Left"].str.contains(patternDel, na=False)]
df = df[~df["Firstname"].str.contains(patternDel, na=False)]
df = df[~df["Lastname"].str.contains(patternDel, na=False)]

d = c - df.shape[0]
report = report.append(["Bad names",d])
print("Removed bad names", df.shape)

df = df.sort_values('Email', ascending=False)
df = df.drop_duplicates(subset='Email', keep='first')

e = d - df.shape[0]
report = report.append(["Duplicates",e])
print("Removed duplicates", df.shape)

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

f = e - df.shape[0]
report = report.append(["Temp Domains",f])
print("Removed Temp Domains", df.shape)


print(list(df.columns.values))

print(df['Group'].value_counts())

    
dob = pd.to_datetime(df['DOB'],infer_datetime_format=True, errors='coerce')


def calculate_age(born):
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

age = dob.apply(calculate_age)

bins = [1,17,34,50,70,90,100,200]
s = pd.DataFrame(age.groupby(pd.cut(age, bins=bins)).size())

print(type(s))

print(s)

report = report.append(df['Gender'].value_counts())
print(df['Gender'].value_counts())

#Split data into ISP groups 

af = df[df["Group"].str.contains('Other', na=False)]
#af=af[['EMAIL', 'TITLE', 'FIRSTNAME', 'LASTNAME',  'ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'CITY', 'COUNTY','POSTCODE','PHONE', 'MOBILE', 'GENDER','DOB','URL', 'IP', 'JOINDATE']]
#af.to_csv(outputdir+"datamixx_uk_tt_Other.csv", index=False)

print("Other Count", af.shape[0])


bf = df[~df["Group"].str.contains('Other', na=False)]
#bf=bf[['EMAIL', 'TITLE', 'FIRSTNAME', 'LASTNAME',  'ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'CITY', 'COUNTY','POSTCODE','PHONE', 'MOBILE', 'GENDER','DOB','URL', 'IP', 'JOINDATE']]
#bf.to_csv(outputdir+"datamixx_uk_tt_ISP.csv", index=False)

print("ISP Count", bf.shape[0])

print("Completed Successfully")
print(report)