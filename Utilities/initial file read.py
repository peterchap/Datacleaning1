import pandas as pd
from datetime import datetime
from datetime import date




directory = 'E:/Company house/'
file = 'BasicCompanyDataAsOneFile-2021-01-01.csv'
df = pd.read_csv(directory+file,delimiter=',',encoding ="ISO-8859-1", low_memory=False)
print(df.shape)
print(list(df.columns.values))
print(df['CompanyStatus'].value_counts())
