import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date

from disposable_email_domains import blocklist
from validate_email import validate_email

directory = "C:/Users/Peter/downloads/webclubs/"
file = "ESB_Webclubs_standard_export_2019-03-07.csv"
outputdir = "C:/Users/Peter/downloads/filter data clean/"
df = pd.read_csv(directory+file,usecols=range(0,29),error_bad_lines=False,low_memory=False)
df['Title'] = df['Title'].str.replace(',','')
df['Fname'] = df['Fname'].str.replace(',','')
df['Lname'] = df['Lname'].str.replace(',','')
df['Ad1'] = df['Ad1'].str.replace(',',' ')
df['Ad2'] = df['Ad2'].str.replace(',',' ')
df['Ad3'] = df['Ad3'].str.replace(',',' ')
df['Town'] = df['Town'].str.replace(',',' ')
df['County'] = df['County'].str.replace(',',' ')
df['OutPC'] = df['OutPC'].str.replace(',','')
df['InPC'] = df['InPC'].str.replace(',','')
df['IPAddress'] = df['IPAddress'].str.replace(',','')
print(list(df.columns.values))
print("Gross file", df.shape)

# Remove columns
to_dropcols = ['Create', 'Urn',  'ConsumerClub', 'GardenersClub', 'HomeOwnersClub', 'MotoristsClub', 'NetOffers', 'QuizClub', 'TravellersClub', 'VinoClub', 'LastSent', ]
df.drop(to_dropcols, axis=1, inplace=True)

df['Postcode'] = df['OutPC'] + " " + df['InPC']

to_dropPC = ['OutPC', 'InPC']
df.drop(to_dropPC, axis=1, inplace=True)

df.dropna(subset=['Email'],inplace=True)

print("Removed null emails", df.shape)

df['is_valid_email'] = df['Email'].apply(lambda x:validate_email(x))
df = df[df['is_valid_email']]
print("Removed invalid email formats", df.shape)

df2 = df[df['Gender'].isnull()]
print(df2.shape)


df= df[df.Gender.str.contains('F|M', regex=True, na=False)]
df = df[df.Email.str.contains("@",na=False)]
char = '\+|\*|\'| |\%|,|\"'
df = df[~df['Email'].str.contains(char,regex=True)]

print(df.shape)
df=df.append(df2)
print("Removed invalid email addresses", df.shape)


new = df["Email"].str.split(pat="@", expand=True)
df['Left']= new[0]
df['Domain'] = new[1]

patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|enquir|fake|feedback|finance|free|garbage|generic|hello|info|invalid|\
junk|loan|office|market|penis|person|phruit|postmaster|random|recep|register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
df = df[~df["Left"].str.contains(patternDel, na=False)]
df = df[~df["Fname"].str.contains(patternDel, na=False)]
df = df[~df["Lname"].str.contains(patternDel, na=False)]
print("Removed bad names", df.shape)

#df = df.sort_values('Email', ascending=False)
#df = df.drop_duplicates(subset='Email', keep='first')

#print("Removed duplicates", df.shape)

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
print(df['Gender'].value_counts())

# Convert date from string to date times
#df['DOB'] = pd.to_datetime(df['DOB'],infer_datetime_format=True, errors='coerce')
dob = pd.to_datetime(df['DOB'],infer_datetime_format=True, errors='coerce')


def calculate_age(born):
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

age = dob.apply(calculate_age)

bins = [1,17,34,50,70,90,100,200]
s = age.groupby(pd.cut(age, bins=bins)).size()
print(s)

df['URL'] =""
df['Joindate'] = ""


#Split data into ISP groups 
for isp, df_Group in df.groupby('Group'):
    af=df.loc[df['Group'] == isp]
    af=af[['Email', 'Title', 'Fname', 'Lname',  'Ad1', 'Ad2', 'Ad3', 'Town', 'County','Postcode','Tel', 'Tel_Mobile', 'Gender','DOB','URL', 'IPAddress', 'Joindate']]
    af.to_csv(directory+"webclub"+isp+".csv", index=False)
    print(isp, af.shape)

print("Completed Successfully")