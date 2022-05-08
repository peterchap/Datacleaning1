# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np


directory = "E:/Wellat/"
file = "ESB_data_2020025.csv"
#email = 'Email'
outputdir = "C:/Users/Peter/Downloads/"


chunksize = 100000

# First setup dataframe iterator, ‘usecols’ parameter filters the columns, and 'chunksize' sets the number of rows per chunk in the csv. (you can change these parameters as you wish)
df_iter = pd.read_csv(directory+file,encoding ="ISO-8859-1",\
        low_memory=False, chunksize=chunksize) 

# this list will store the filtered dataframes for later concatenation 
df_lst = [] 
iter=0
count = 0
# Iterate over the file based on the criteria and append to the list
for df_ in df_iter: 
        tmp_df = (df_.rename(columns={col: col.lower() for col in df_.columns}) # filter eg. rows where 'consent_email' value grater than date
                                  .pipe(lambda x:  x[x.consent_email > '2019-02.24'] ))
        df_lst += [tmp_df.copy()]
        count = count + df_.shape[0] 
        iter=iter+1
# And finally combine filtered df_lst into the final lareger output say 'df_final' dataframe 
df_final = pd.concat(df_lst)
print(df_final.head(1))
df_final.to_csv(directory+"datamixx_feb250220_12mthoptin.csv", index=False)
print(list(df_final.columns.values))

print(list(df_final.shape))
print("Gross file", count)
