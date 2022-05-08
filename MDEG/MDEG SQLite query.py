import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, date
import dateparser

dbdirectory= 'E:/MDEG/'
directory = 'E:/MDEG/Data Samples/'
tablename = 'affy_all' 
db = 'mdeg.db'
file = 'Sample_1020_Motion_DM_v2.csv'
sqlite_engine = create_engine('sqlite:///' + dbdirectory + db)

df = pd.read_sql_table(tablename, sqlite_engine)
print(df.shape)

df['timestamp']= pd.to_datetime(df['timestamp'])
df['date of birth'] = pd.to_datetime(df['date of birth'])
print(df['date of birth'].head())

sample = df.loc[(df['timestamp'] > '2019-12-01 00:00:00') & (df['timestamp'] < '2019-12-31 00:00:00')].copy()
print(sample.shape)

final = sample.loc[(df['date of birth'] > '1970-06-22 00:00:00') & (sample['date of birth'] < '2000-06-23 00:00:00')].copy()
final.drop_duplicates(subset='mobile_nr', keep='last', inplace=True)
print(final.shape)

final.to_csv(directory + file, index=False)