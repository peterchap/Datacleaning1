# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np


directory = "E:/wellat/"
filename = "ESB_data_20190920.csv"
#email = 'Email'
outputdir = "E:/wellat/"
colstokeep = [ 'TITLE', 'FIRSTNAME', 'LASTNAME', 'supplier_code', 'CONSENT_SOURCE', 'EMAIL']
['DMLURN', 'TITLE', 'FIRSTNAME', 'LASTNAME', 'Address1', 'Address2', 'Address3', 'TOWN', 'COUNTY', 'POSTCODE', 'DOB', 'CONSENT_IPV4', 'supplier_code', 'CONSENT_SOURCE', 'CONSENT_EMAIL', 'EMAIL']
df = pd.read_csv(directory+filename,encoding ="ISO-8859-1", low_memory=False, usecols=colstokeep, )

df.rename(columns={ 'TITLE' : 'Title', 'FIRSTNAME': 'First_Name', 'LASTNAME' : 'Last_Name',\
'supplier_code': 'List_ID', 'CONSENT_SOURCE' :'Source_URL', 'EMAIL' : 'Email'},inplace=True)

#df.rename(columns={ 'first_name': 'First_Name', 'last_name' : 'Last_Name',\
#'source' :'Source_URL', 'email' : 'Email'},inplace=True)


df['First_Name'] = df['First_Name'].astype(str).str[:50]
datafn = df['First_Name'].str.find(',')
print(sum(datafn))
df['First_Name'] = df['First_Name'].apply(lambda x: x.replace(',',''))
datafn = df['First_Name'].str.find(',')
fn = sum(datafn)
df['Last_Name'] = df['Last_Name'].astype(str).str[:50]
dataln = df['Last_Name'].str.find(',')
print(sum(dataln))
df['Last_Name'] = df['Last_Name'].apply(lambda x: x.replace(',',''))
dataln = df['Last_Name'].str.find(',')
ln = sum(dataln)
df['Title'] = df['Title'].astype(str).str[:10]
datatitle = df['Title'].str.find(',')
title = sum(datatitle)
#df['Title'] = ''
#df['List_ID'] = 'Inboxed'
new = df['Email'].str.split(pat="@", expand=True)
df['Domain'] = new[1]
print('firstname',fn,'lastname',ln)
df.to_csv(directory+"ESB_data_20190920_forSQL.csv", index=False)

print(list(df.columns.values))

print(list(df.shape))

