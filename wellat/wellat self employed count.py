# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np


directory = "C:/Users/Peter/Downloads/"
file = "mixxitmedia_20190523.csv"
#email = 'Email'
outputdir = "C:/Users/Peter/Downloads/"


chunksize = 100000



cols_to_keep = ['email', 'opt_in_date', 'occupation','occupation_selfemployed',\
'occupation_director', 'occupation_professional', 'occupation_professional_occupation']


# First setup dataframe iterator, ‘usecols’ parameter filters the columns, and 'chunksize' sets the number of rows per chunk in the csv. (you can change these parameters as you wish)
df_iter = pd.read_csv(directory+file,encoding ="ISO-8859-1",low_memory=False, chunksize=chunksize, usecols=cols_to_keep) 

# this list will store the filtered dataframes for later concatenation 
df_lst = [] 
iter=0

# Iterate over the file based on the criteria and append to the list
for df_ in df_iter: 
        tmp_df = (df_.rename(columns={col: col.lower() for col in df_.columns}) # filter eg. rows where 'consent_email' value grater than date
                             .pipe(lambda x:  x[x.email.notna()]  )
                             .pipe(lambda y: y[y.occupation_selfemployed.notna()]))   
        df_lst += [tmp_df.copy()] 
        iter=iter+1
# And finally combine filtered df_lst into the final lareger output say 'df_final' dataframe 
df_final = pd.concat(df_lst)
df_final.to_csv(outputdir+"wellat occupation3007.csv", index=False)
print(list(df_final.columns.values))
print(iter)
print(list(df_final.shape))
# .pipe(lambda x:  x[x.email.isnotnull()] & x[x.occupation.isnotnull()]  ))
