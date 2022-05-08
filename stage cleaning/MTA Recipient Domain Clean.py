import pandas as pd

onedrive= 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/Data Cleaning Project/'

file1 = 'reciipient domains.csv'
file2 = 'Domain_merged-withMXNS.csv'

df1 = pd.read_csv(onedrive + file1,encoding ='UTF-8', low_memory=False)
#df.rename(columns={'Email' : 'email'},inplace=True)
print(df1.columns)
print(df1.head(5))

df2 = pd.read_csv(onedrive + file2,encoding ='ISO-8859-1',\
     low_memory=False, usecols=['Original', 'overallstatus'])
print(df2.columns)
print(df2.head(5))

df1 = pd.merge(df1, df2, left_on=['RecipientDomain'], right_on=['Original'], how='left')

print(df1.head(5))

df1.to_csv(onedrive + "Recipientdomain_Analysis.csv", index=False)