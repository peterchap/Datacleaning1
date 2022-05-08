import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date
import dateparser

from disposable_email_domains import blocklist
from validate_email import validate_email

directory = "C:/Users/Peter/downloads/"
file = "2018 FEMALES UK (1).csv"
outputdir = "C:/Users/Peter/downloads/inboxed 150519/"

df = pd.read_csv(directory+file,sep=';',usecols=range(0,8),parse_dates=['Day of DOB','Hour of Timestamp'], date_parser=dateparser.parse)


#df['Day of DOB'] = df['Day of DOB'].str.replace(',','')
#df['Hour of Timestamp'] = df['Hour of Timestamp'].str.replace(',',' ')
df['IP'] = df['IP'].str.replace(',','')
df['Name'] = df['Name'].str.replace(',','')
df['Surname'] = df['Surname'].str.replace(',','')
#df['Source'] = df['Source'].str.replace(',',' ')


print(list(df.columns.values))
print("Gross file", df.shape)

# Remove columns
to_dropcols = ['Country_id']
df.drop(to_dropcols, axis=1, inplace=True)
df.columns = ['DOB','EMAIL','JOINDATE','IP','FIRSTNAME','LASTNAME','URL']

df.dropna(subset=['EMAIL'],inplace=True)

print("Removed null emails", df.shape)

df['is_valid_email'] = df['EMAIL'].apply(lambda x:validate_email(x))
df = df[df['is_valid_email']]
print("Removed invalid email formats", df.shape)

df = df[df.EMAIL.str.contains("@",na=False)]
char = '\+|\*|\'| |\%|,|\"|\/'
df = df[~df['EMAIL'].str.contains(char,regex=True)]

print("Removed invalid email addresses", df.shape)


new = df["EMAIL"].str.split(pat="@", expand=True)
df['Left']= new[0]
df['Domain'] = new[1]

patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|enquir|fake|feedback|finance|free|garbage|generic|hello|info|invalid|\
junk|loan|office|market|penis|person|phruit|postmaster|random|recep|register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
df = df[~df["Left"].str.contains(patternDel, na=False)]
df = df[~df["FIRSTNAME"].str.contains(patternDel, na=False)]
df = df[~df["LASTNAME"].str.contains(patternDel, na=False)]
print("Removed bad names", df.shape)

df = df.sort_values('EMAIL', ascending=False)
df = df.drop_duplicates(subset='EMAIL', keep='first')

print("Removed duplicates", df.shape)

onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"

ispgroup = pd.read_csv(onedrive+'ISP Group domains.csv')
df = pd.merge(df, ispgroup, on='Domain', how='left')
df['Group'].fillna("Other", inplace = True)

# remove email temp domains
domains = df[df['Group']=="Other"]['Domain']
domains.drop_duplicates(inplace=True)
d =[]
for domain in domains:
    d.append(( domain, domain in blocklist))

e = pd.DataFrame(d, columns=('Domain', 'Temp'))
df = pd.merge(df, e, on='Domain', how='left')
df = df[df.Temp != 1]
print("Removed Temp Domains", df.shape)


print(list(df.columns.values))
print(df['Group'].value_counts())

# Convert date from string to date times
#df['DOB'] = pd.to_datetime(df['DOB'],infer_datetime_format=True, errors='coerce')

#for birthday in df['DOB']:
#    df['DOB'] = dateparser.parse(birthday)

#for timestamp in df['JOINDATE']:
#    df['JOINDATE'] = dateparser.parse(timestamp)
    
dob = pd.to_datetime(df['DOB'],infer_datetime_format=True, errors='coerce')


def calculate_age(born):
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

age = dob.apply(calculate_age)

bins = [1,17,34,50,70,90,100,200]
s = age.groupby(pd.cut(age, bins=bins)).size()
print(s)

df['TITLE'] =""
df['ADDRESS1'] = ""
df['ADDRESS2'] = ""
df['ADDRESS3'] = ""
df['CITY'] = ""
df['COUNTY'] = ""
df['POSTCODE'] = ""
df['PHONE'] = ""
df['MOBILE'] = ""
df['GENDER'] = "F"
df['IP'] = ""

#Split data into ISP groups 

af = df[df["Group"].str.contains('Other', na=False)]
af=af[['EMAIL', 'TITLE', 'FIRSTNAME', 'LASTNAME',  'ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'CITY', 'COUNTY','POSTCODE','PHONE', 'MOBILE', 'GENDER','DOB','URL', 'IP', 'JOINDATE']]
af.to_csv(outputdir+"Inbox150519Other.csv", index=False)
print("Other Count", af.shape)
bf = df[~df["Group"].str.contains('Other', na=False)]
bf=bf[['EMAIL', 'TITLE', 'FIRSTNAME', 'LASTNAME',  'ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'CITY', 'COUNTY','POSTCODE','PHONE', 'MOBILE', 'GENDER','DOB','URL', 'IP', 'JOINDATE']]
bf.to_csv(outputdir+"Inbox150519ISP.csv", index=False)
print("ISP Count", bf.shape)
print("Completed Successfully")