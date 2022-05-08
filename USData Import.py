import pandas as pd

from disposable_email_domains import blocklist
from validate_email import validate_email

directory = "C:/Users/Peter/downloads/"
file = "New CIO US Data - Clean.csv"
outputdir = "C:/Users/Peter/downloads/"

df = pd.read_csv(directory+file,usecols=range(0,1),encoding = "ISO-8859-1")


print(list(df.columns.values))
print("Gross file", df.shape)


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
junk|linkedin|loan|office|market|penis|person|phruit|postmaster|random|recep|register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
df = df[~df["Left"].str.contains(patternDel, na=False)]

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
af = df[df["Group"].str.contains('Other', na=False)]
#af.to_csv(outputdir+"US140619Other.csv", index=False)
print("Other Count", af.shape)

bf = df[~df["Group"].str.contains('Other', na=False)]

#bf.to_csv(outputdir+"140619ISP.csv", index=False)
print("ISP Count", bf.shape)
print("Completed Successfully")