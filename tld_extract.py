import tldextract
import pandas as pd 

from sqlalchemy import create_engine

directory = '/home/peter/Documents/'
table='mx_table'

sqlite_engine = create_engine('sqlite:///' + directory + 'cleaning.db')

df = pd.read_sql(table, sqlite_engine)
print(df.columns)
print(df.shape)
df.drop_duplicates('mx', keep='first')
print(df.shape)
result = []

extract = tldextract.TLDExtract(include_psl_private_domains=True)
#extract = tldextract.TLDExtract(cache_file=False)
extract.update()

for mx in df['mx']:
    
    if type(mx) == str:  

        ext = tldextract.extract(mx)
        row = [mx,  ext.registered_domain]
        result.append(row)
    else:
        row = [mx, ""]
        result.append(row)

df1 = pd.DataFrame(result,\
    columns = ['mx',  'newdomain'],dtype=str)
print(df1.shape)

df['domain'] = df1['newdomain']

print(df.shape)

df.to_sql(table, sqlite_engine, if_exists='replace', chunksize=500, index=False)