import pandas as pd
from sqlalchemy import create_engine

directory= 'E:/MDEG/'
onedrive = 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/'
month = 'May20'

table1 = 'affy_all'
table2 = 'FPC_all'
table3 = 'jnb_all'
table4 = 'jnb_win2'
filename= 'OttyConversions.csv'

db = 'mdeg.db'
sqlite_engine = create_engine('sqlite:///' + directory + db)
cols = ['email', 'first_name', 'surname', 'postcode', 'gender', 'date of birth' ]
colsfpc = ['email_address', 'first_name', 'surname', 'postcode',  'date of birth' ]
colsjnbwin = ['email', 'first_name', 'surname', 'postcode', 'gender']
affy = pd.read_sql(table1, sqlite_engine, columns=cols)
affy.drop_duplicates(subset='email', inplace=True)
affy['list'] = 'affy'
print(affy.shape)
print(affy.columns)
fpc = pd.read_sql(table2, sqlite_engine, columns=colsfpc)
fpc.rename(columns={'email_address' : 'email'}, inplace=True)
fpc['gender'] = ''
fpc.drop_duplicates(subset='email', inplace=True)
fpc['list'] = 'fpc'
print(fpc.columns)
print(fpc.shape)
jnb = pd.read_sql(table3, sqlite_engine, columns=cols)
jnb.drop_duplicates(subset='email', inplace=True)
jnb['list'] = 'jnb'
print(jnb.shape)
print(jnb.columns)

jnbwin = pd.read_sql(table4, sqlite_engine, columns=colsjnbwin)
jnbwin['date of birth'] = '00/01/1950'
jnbwin.drop_duplicates(subset='email', inplace=True)
jnbwin['list'] = 'jnbwin'
print(jnbwin.shape)
print(jnbwin.columns)

df = pd.concat([affy, fpc, jnb, jnbwin])
print('All', df.shape)

otty = pd.read_csv(onedrive + filename,encoding ='utf-8')
print("Otty", otty.shape)

dfall = df.merge(otty, on='email', how='inner')
print(dfall.shape)
print(dfall.columns)

dfall.to_csv(directory + 'ottylookup.csv', index=False)
