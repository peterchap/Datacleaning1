# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime
from dateutil.parser import parse

directory = "E:/Wellat/"

filename = "MixxitMedia_20190701.csv"

cols_to_keep = ['Title', 'Firstname', 'Lastname','Address1',\
'Address2', 'Address3', 'Address4', 'Address5', 'Postcode', 'Town', 'County', 'Email',\
'Mobile', 'Consent_Mobile']

chunksize = 100000

#df = pd.read_csv(directory+filename, usecols = tokeep, nrows=100)

#dropcols = ['URN', 'Supplier_Code', 'Title', 'Firstname', 'Lastname', 'DOB', 'Gender',\
#'Children_0To5', 'Children_6To13', 'Children_14To15', 'Children_16', 'Address1',\
#''Address2', 'Address3', 'Address4', 'Address5', 'Postcode', 'Town', 'County',\
#'Country', 'Email', 'Phone', 'Mobile', 'Consent_Cookie', 'Consent_Email',\
#'Consent_Mobile', 'Consent_SMS', 'Consent_Phone', 'Consent_Postal', 'Consent_Social',\
#'Consent_Source', 'Consent_Ipv4', 'Consent_IPv6', 'OptOut_Cookie', 'OptOut_Email',\
#'OptOut_SMS', 'OptOut_Mobile', 'OptOut_Phone', 'OptOut_Social', 'OptOut_Postal',\
#'Eng_Email_Last', 'Eng_Mobile_Last', 'Eng_Online_Last', 'Eng_Phone_Last',\
#'Eng_Postal_Last', 'Eng_Social_Last', 'Valid_Email', 'Valid_Mobile', 'Valid_Phone',\
#'Valid_Postal', 'AOI_Retail', 'AOI_Automotive', 'AOI_Lifestyle', 'AOI_Charity',\
#'AOI_Utility', 'AOI_Telecommunications', 'AOI_Insurance', 'AOI_PublishingMedia',\
#'AOI_Entertainment', 'AOI_PublicSector', 'AOI_FinancialServices', 'AOI_Travel',\
#'AOI_MailOrder', 'AOI_HealthBeauty', 'AOI_Education', 'AOI_FMCG',\
#'AOI_MarketingAgencies', 'Created', 'LastUpdated', 'Delete']

# First setup dataframe iterator, ‘usecols’ parameter filters the columns, and 'chunksize' sets the number of rows per chunk in the csv. (you can change these parameters as you wish)

df_iter = pd.read_csv(directory+filename,encoding ="ISO-8859-1",low_memory=False,sep=',', chunksize=chunksize,parse_dates= ['Consent_Mobile'], usecols=cols_to_keep) 

# this list will store the filtered dataframes for later concatenation 
df_lst = [] 
iter=0
count = 0
# Iterate over the file based on the criteria and append to the list
for df_ in df_iter: 
        tmp_df = (df_.rename(columns={col: col.lower() for col in df_.columns}) # filter eg. rows where 'consent_email' value grater than date
                                  .pipe(lambda x:  x[x['mobile'].notnull()])
                                  .pipe(lambda x:  x[x['email'].notnull()])
                                  .pipe(lambda x:  x[x['consent_mobile'].notnull()])
                                  #.pipe(lambda x:  x[pd.to_datetime(x['consent_mobile'])])
                                  .pipe(lambda x:  x[(x['consent_mobile'] > '2018-06-30')] ))
        df_lst += [tmp_df.copy()]
        count = count + df_.shape[0] 
        iter=iter+1
# And finally combine filtered df_lst into the final lareger output say 'df_final' dataframe 
df_final = pd.concat(df_lst)
df_final.to_csv(directory+"ssn mobile selection.csv", index=False)
print(list(df_final.columns.values))
print(iter)
print(list(df_final.shape))
