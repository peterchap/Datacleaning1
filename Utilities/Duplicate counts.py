import pandas as pd

directory = 'E:/rapid7/'
file1 = 'ukdomaintest.csv'

df = pd.read_csv(directory + file1, encoding="ISO-8859-1", low_memory=False)
print(df.shape)

df.drop_duplicates(subset='domain', keep='last',
                   inplace=True)
df.sort_values(by='domain', inplace=True)
print(df.shape)
df.to_parquet(directory + 'mxlookup.parquet', index=False)
