import pandas as pd

df = pd.read_csv("E:/rapid7/domaintest9.csv",usecols=['domain'],encoding= 'utf-8' )
print(df.shape)
clean = df[df['domain'].str.contains((r'[^a-zA-Z0-9\.\-]'))==False]
print(clean.shape)