import pandas as pd
from sqlalchemy import create_engine

directory= 'E:/MDEG/'

month = 'May20'

table1 = 'affy_all'
table2 = 'FPC_all'
table3 = 'jnb_all'

db = 'mdeg.db'
sqlite_engine = create_engine('sqlite:///' + directory + db)

affy = pd.read_sql(table1, sqlite_engine, columns=['email'])
affy.drop_duplicates(subset='email', inplace=True)
print(affy.shape)
print(affy.columns)
fpc = pd.read_sql(table2, sqlite_engine, columns=['email'])
fpc.drop_duplicates(subset='email', inplace=True)
print(fpc.columns)
print(fpc.shape)
jnb = pd.read_sql(table3, sqlite_engine, columns=['email'])
jnb.drop_duplicates(subset='email', inplace=True)
print(jnb.shape)
print(jnb.columns)

df = pd.concat([affy, fpc, jnb])
print('All', df.shape)
print(df.head(5))
df.drop_duplicates(subset='email', inplace=True)
print('uniques', df.shape[0])

duplicates = pd.DataFrame(columns = ['list', 'count'])
data1 = affy.merge(fpc, left_on='email', right_on='email', how='inner')
print( "affy/fpc Duplicates", data1.shape[0])
duplicates = duplicates.append(pd.Series(['affy-fpc', data1.shape[0]],index=duplicates.columns), ignore_index=True)
data2 = affy.merge(jnb, on='email', how='inner')
print( "affy jnb Duplicates", data2.shape[0])  
duplicates = duplicates.append(pd.Series(['affy-jnb', data2.shape[0]],index=duplicates.columns), ignore_index=True)
data3 = jnb.merge(fpc, left_on='email',right_on='email', how='inner')
print( "fpc-jnb Duplicates", data3.shape[0])
duplicates = duplicates.append(pd.Series(['fpc-jnb', data3.shape[0]],index=duplicates.columns), ignore_index=True)

dfall = pd.concat([data1, data2])
print("allmatch", dfall.shape)
dfall.drop_duplicates(subset='email', keep='first', inplace=True)
print("allmatch", dfall.shape)


duplicates = duplicates.append(pd.Series(['uniques' ,  df.shape[0]],index=duplicates.columns), ignore_index=True)
print(duplicates)
affy.drop_duplicates(subset='email', inplace=True)
print('affy-uniques', affy.shape[0])
fpc.drop_duplicates(subset='email', inplace=True)
print('fpc-uniques', fpc.shape[0])
jnb.drop_duplicates(subset='email', inplace=True)
print('jnb-uniques', jnb.shape[0])
#duplicates.to_csv(directory + "duplicates" + month +".csv", index = False)