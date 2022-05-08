import pandas as pd

onedrive= 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/'
directory = "E:/Wellat/"

filename = "datamixx_feb250220_12mthoptin.csv"

df = pd.read_csv(directory+filename,encoding ="ISO-8859-1",engine='python', error_bad_lines=False)
df.replace(',','', regex=True, inplace=True) 

print(df['supplier_code'].value_counts())

#print(df[df['consent_email'] > '2019-02-24'].shape)

print(df.head(1))
#df.to_csv(onedrive +"wellatfeb20.csv", index=False)




