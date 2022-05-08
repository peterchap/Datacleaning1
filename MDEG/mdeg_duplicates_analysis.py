import pandas as pd
from sqlalchemy import create_engine

directory= 'E:/MDEG/'

month = 'May20'

table1 = 'affy_all'
table2 = 'FPC_all'
table3 = 'jnb_all'
table4 = 'jnb_win2'

db = 'mdeg.db'
sqlite_engine = create_engine('sqlite:///' + directory + db)
cols = ['email', 'first_name', 'surname', 'postcode', ]
affy = pd.read_sql(table1, sqlite_engine, columns=cols)
affy.drop_duplicates(subset='email', inplace=True)
affy['list'] = 'affy'
print(affy.shape)
print(affy.columns)
fpc = pd.read_sql(table2, sqlite_engine, columns=cols)
fpc.drop_duplicates(subset='email', inplace=True)
fpc['list'] = 'fpc'
print(fpc.columns)
print(fpc.shape)
jnb = pd.read_sql(table3, sqlite_engine, columns=cols)
jnb.drop_duplicates(subset='email', inplace=True)
jnb['list'] = 'jnb'
print(jnb.shape)
print(jnb.columns)

jnbwin = pd.read_sql(table4, sqlite_engine, columns=cols)
jnbwin.drop_duplicates(subset='email', inplace=True)
jnbwin['list'] = 'jnbwin'
print(jnbwin.shape)
print(jnbwin.columns)

df = pd.concat([affy, fpc, jnb, jnbwin])
print('All', df.shape)
print(df.head(5))
df.drop_duplicates(subset='email', inplace=True)
print('uniques', df.shape[0])


data1 = affy.merge(fpc, left_on='email', right_on='email', how='inner')
print( "affy/fpc Duplicates", data1.shape)

data2 = affy.merge(jnb, on='email', how='inner')
print( "affy jnb Duplicates", data2.shape)  

data3 = jnb.merge(fpc, left_on='email',right_on='email', how='inner')
print( "fpc-jnb Duplicates", data3.shape)

data4 = jnbwin.merge(fpc, left_on='email', right_on='email', how='inner')
print( "jnbwin/fpc Duplicates", data4.shape)

data5= jnbwin.merge(jnb, on='email', how='inner')
print( "jnbwin jnb Duplicates", data5.shape)  

data6 = jnbwin.merge(affy, left_on='email',right_on='email', how='inner')
print( "jnbwin-affy Duplicates", data6.shape)

dfall = pd.concat([data1, data2, data6])
print("allmatch", dfall.shape)
dfall.drop_duplicates(subset='email', keep='first', inplace=True)
print("allmatch", dfall.shape)
print(dfall.columns)
dfall.to_csv(directory + 'mdegmatchallv2.csv', index=False)
