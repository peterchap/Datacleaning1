import pandas as pd
import glob, os
from disposable_email_domains import blocklist
from validate_email import validate_email

directory = "C:/Users/Peter/downloads/wellat/"
outputdir = "C:/Users/Peter/downloads/wellat clean/"
os.chdir(directory)
df = pd.DataFrame()
for file in glob.glob("*.csv"):
    dffile = pd.read_csv(file,usecols=range(0,5),error_bad_lines=False)
    dffile['Source'] = file.split('(')[-1].split(')')[0]
    df = df.append(dffile)

print("Gross file", df.shape)
print(list(df.columns.values))

df.dropna(subset=['Email Address'],inplace=True)

print("Removed null emails", df.shape)

df['is_valid_email'] = df['Email Address'].apply(lambda x:validate_email(x))
df = df[df['is_valid_email']]
print("Removed invalid email formats", df.shape)

char = '\+|\*|\'| |\%|,|\"'
df = df[~df['Email Address'].str.contains(char,regex=True)]

print("Removed invalid email addresses", df.shape)

df['Email Address'] = df['Email Address'].str.lower()
new = df["Email Address"].str.split(pat="@", expand=True)
df['Left']= new[0]
df['Domain'] = new[1]

patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|enquir|fake|feedback|finance|free|garbage|generic|hello|info|invalid|\
junk|loan|office|market|penis|person|phruit|postmaster|random|recep|register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
df = df[~df["Left"].str.contains(patternDel, na=False)]
df = df[~df["Forename"].str.contains(patternDel, na=False)]
df = df[~df["Surname"].str.contains(patternDel, na=False)]
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
df = df[['Email Address', 'Title', 'Forename', 'Surname','List code','Source','Group']]
df.rename(columns={'Email Address' : 'Email', 'Forename':'Firstname', 'Surname': 'Lastname'}, inplace=True)
header_list = ['Email', 'Title', 'Firstname', 'Lastname',  'Ad1', 'Ad2', 'Ad3', 'Town', 'County','Postcode','Tel', 'Tel_Mobile', 'Gender','DOB','URL', 'IPAddress', 'Joindate']

#Split data into ISP groups 
for list, df_List in df.groupby('List code'):
    lf=df.loc[df['List code'] == list]
    print(lf.shape)
    af = lf[lf["Group"].str.contains('Other', na=False)]
    af=af.reindex(columns = header_list)
    af['Gender'][af['Title'] =='Mr'] = 'M'
    af['Gender'][af['Title'] !='Mr'] = 'F'
    af.to_csv(outputdir+list+"other"+".csv", index=False)
    print(list, af.shape)
    bf = lf[~lf["Group"].str.contains('Other', na=False)]
    bf=bf.reindex(columns = header_list)
    bf['Gender'][bf['Title'] =='Mr'] = 'M'
    bf['Gender'][bf['Title'] !='Mr'] = 'F'
    bf.to_csv(outputdir+list+"isp"+".csv", index=False)
    print(list, bf.shape)

print("Completed Successfully")    