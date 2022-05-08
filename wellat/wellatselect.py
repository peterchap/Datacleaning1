import pandas as pd
import glob, os
from disposable_email_domains import blocklist
from validate_email import validate_email

directory = "C:/wellatideal/"
file = "wellatall.csv"

os.chdir("C:\wellatideal")

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

#combine all files in the list
df = pd.concat([pd.read_csv(f, low_memory=False) for f in all_filenames ])


print("Gross file", df.shape)
print(list(df.columns.values))


df['is_valid_email'] = df['email'].apply(lambda x:validate_email(x))
df = df[df['is_valid_email']]
print("Removed invalid email formats", df.shape)

char = '\+|\*|\'| |\%|,|\"'
df = df[~df['email'].str.contains(char,regex=True)]

print("Removed invalid email addresses", df.shape)



patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|enquir|fake|feedback|finance|free|garbage|generic|hello|info|invalid|\
junk|loan|office|market|penis|person|phruit|postmaster|random|recep|register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
df = df[~df["localpart"].str.contains(patternDel, na=False)]
print("Removed bad names", df.shape)

df = df.sort_values('email', ascending=False)
df = df.drop_duplicates(subset='email', keep='first')

#print("Removed duplicates", df.shape)
df['ISP'].fillna("Other", inplace = True)

# remove email temp domains
domains = df[df['ISP']=="Other"]['domain']
domains.drop_duplicates(inplace=True)
d =[]
for domain in domains:
    d.append(( domain, domain in blocklist))

e = pd.DataFrame(d, columns=('domain', 'Temp'))
df = pd.merge(df, e, on='domain', how='left')
df = df[df.Temp != 1]
print("Removed Temp Domains", df.shape)


print(list(df.columns.values))

df.rename(columns={'email' : 'EMAIL','title' : 'TITLE', 'forename':'FIRSTNAME', 'surname': 'LASTNAME', 'add1' : 'ADDRESS1', 'postcode' : 'POSTCODE', 'landline' : 'PHONE',\
'mobile' : 'MOBILE', 'gender' : 'GENDER', 'dob' : 'DOB', 'opt_in_date' : 'JOINDATE'}, inplace=True)

header_list = ['EMAIL', 'TITLE', 'FIRSTNAME', 'LASTNAME',  'ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'CITY', 'COUNTY','POSTCODE','PHONE', 'MOBILE', 'GENDER','DOB','URL', 'IP', 'JOINDATE', 'ISP']


af = df[df["ISP"].str.contains('Other', na=False)]
af=af.reindex(columns = header_list)
af=af[['EMAIL', 'TITLE', 'FIRSTNAME', 'LASTNAME',  'ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'CITY', 'COUNTY','POSTCODE','PHONE', 'MOBILE', 'GENDER','DOB','URL', 'IP', 'JOINDATE']]
af.to_csv(directory+"wellatideal_Other.csv", index=False)
print("Other Count", af.shape)

bf = df[~df["ISP"].str.contains('Other', na=False)]
bf=bf.reindex(columns = header_list)
bf=bf[['EMAIL', 'TITLE', 'FIRSTNAME', 'LASTNAME',  'ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'CITY', 'COUNTY','POSTCODE','PHONE', 'MOBILE', 'GENDER','DOB','URL', 'IP', 'JOINDATE']]
bf.to_csv(directory+"wellatideal_ISP.csv", index=False)
print("ISP Count", bf.shape)


print("Completed Successfully")    
