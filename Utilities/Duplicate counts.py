import pandas as pd

directory = 'C:/domains-monitor/'
file1 = 'mx_status.csv'

df = pd.read_csv(directory + file1, encoding="utf-8", low_memory=False)
print(df.shape)
print(df.columns)

df.drop_duplicates(subset='mx_domain', keep='last',
                   inplace=True)
df.sort_values(by='mx_domain', inplace=True)
print(df.shape)
#df.to_parquet(directory + 'mxlookup.parquet', index=False)
