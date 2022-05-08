import pandas as pd
from sqlalchemy import create_engine
from pandas.api.types import CategoricalDtype

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

cat_type = CategoricalDtype(categories=['fpc','jnb','jnbwin','affy'], ordered=True)

df['list'] = df['list'].astype(cat_type)

df.sort_values('list').drop_duplicates('email', keep='first')

df = df.sort_values('list').groupby('email', as_index=False).first()
print (df['list'].value_counts())