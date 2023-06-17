# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np


directory = "E:/wellat/"
filename = "ESB_data_20190920.csv"
#email = 'Email'
outputdir = "E:/wellat/"

df = pd.read_csv(directory+filename,encoding ="ISO-8859-1", low_memory=False)
print(df.head(5))

cols = [ 'FIRSTNAME', 'LASTNAME', 'Address1', 'POSTCODE',  'CONSENT_SOURCE', 'EMAIL']

for field in cols:
    print(df[field].dtype)
    if df[field].dtype == 'object':
        datafn = df[field].str.find(',')
        print(field, sum(datafn))
        df[field] = df[field].apply(lambda x: x.replace(',',''))


#df.to_csv(directory+"ESB_data_20190920_format.csv", index=False)

print(list(df.columns.values))

print(list(df.shape))

