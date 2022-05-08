import pandas as pd

from sqlalchemy import create_engine, MetaData, Table, select, Integer, Column

#onedrive= 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/Data Cleaning Project/'
directory = '//DISKSTATION/Documents/'

#df = pd.read_csv(onedrive + "Domain_all2_MX_Revised090220.csv",encoding = "ISO-8859-1",low_memory=False)
#print(df.shape)

tablename = 'domain_all' 
db = 'cleaning.db'
sqlite_engine = create_engine('sqlite:///' + directory + db)

df = pd.read_sql(tablename, sqlite_engine)
print(df.shape)

#df = pd.read_csv(onedrive + "typos.csv",encoding = "ISO-8859-1",low_memory=False)

#frame  = df.to_sql(tableName, sql_engine, if_exists='replace',chunksize=500);

#print("Table %s created successfully."%tableName)


connect_string = 'mysql+mysqlconnector://peter:AcmYHPSsTE4pvzCZhEpE@domain-masater.cgr5rn0pnasl.eu-west-1.rds.amazonaws.com/domains'

sql_engine = create_engine(connect_string,echo=False)
df.to_sql(tablename, sql_engine, if_exists='replace',chunksize=500)

#nav = meta.tables[tablename]

#df = pd.read_sql(test, sql_engine)
#df.drop_duplicates(subset='Domain', keep='first',inplace=True)

#


#sqlite_engine = create_engine('sqlite:///' +onedrive + 'cleaning.db')
#


