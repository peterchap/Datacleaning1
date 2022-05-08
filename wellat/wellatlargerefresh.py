# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime
from dateutil.parser import parse

directory = "C:/Users/Peter Chaplin/Downloads/ESB_data_20190920/"
file = "ESB_data_20190920.csv"

cols_to_keep = ['Title', 'Firstname', 'Lastname','Address1',\
'Address2', 'Address3', 'Address4', 'Address5', 'Postcode', 'Town', 'County', 'Email',\
'Mobile', 'Consent_Mobile']

chunksize = 100000



# First setup dataframe iterator, ‘usecols’ parameter filters the columns, and 'chunksize' sets the number of rows per chunk in the csv. (you can change these parameters as you wish)

df_iter = pd.read_csv(directory+file,encoding ="ISO-8859-1",low_memory=False,sep=',',\
chunksize=chunksize,parse_dates= ['CONSENT_EMAIL']) 

# this list will store the filtered dataframes for later concatenation 
df_lst = [] 
iter=0
count = 0
# Iterate over the file based on the criteria and append to the list
for df_ in df_iter: 
        tmp_df = (df_.rename(columns={col: col.lower() for col in df_.columns}) # filter eg. rows where 'consent_email' value grater than date
                                  .pipe(lambda x:  x[x['email'].notnull()])
                                  .pipe(lambda x:  x[(x['consent_email'] > '2018-06-30')] ))
        df_lst += [tmp_df.copy()]
        count = count + df_.shape[0] 
        iter=iter+1
        print(iter)
# And finally combine filtered df_lst into the final lareger output say 'df_final' dataframe
 
df_final = pd.concat(df_lst)

to_drop = ['dmlurn', 'consent_ipv4']
df_final.drop(to_drop, axis=1, inplace=True)
df_final.to_csv(directory+"ESB_data_20190920_optin.csv", index=False)
print(list(df_final.columns.values))
print(list(df_final.shape))
print(df_final['supplier_code'].value_counts())

for i, x in df_final.groupby(['supplier_code']):
    x.to_csv(directory +"ESB_data_20190920_optin_" + i + ".csv", index=False)
    print(x.shape)

to_drop2 = ['address1', 'address2', 'address3', 'town', 'county', 'postcode', 'dob',  'consent_email']
df_final.drop(to_drop2, axis=1, inplace=True)
df_final.rename(columns={ 'title' : 'Title', 'firstname': 'First_Name', 'lastname' : 'Last_Name',\
'supplier_code': 'List_ID', 'consent_source' :'Source_URL', 'email' : 'Email'},inplace=True)
new = df_final['Email'].str.split(pat="@", expand=True)
df_final['Domain'] = new[1]

df_final.to_csv(directory+"ESB_data_20190920_sqlready.csv", index=False)