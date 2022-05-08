import pandas as pd
from sqlalchemy import create_engine

directory= 'E:/Acquirze/'
directorydb = 'E:/'
month = 'Feb21'

table1 = 'ESB_JAN21_refresh_270121'

filename= 'Experian_270121_refesh.csv'

db = 'acquirz.db'
sqlite_engine = create_engine('sqlite:///' + directorydb + db)
df = pd.read_csv(directory + filename,encoding ='utf-8', low_memory=False)
print(df.columns)
#df.to_sql(table1, sqlite_engine, if_exists='replace',chunksize=500, index=False)