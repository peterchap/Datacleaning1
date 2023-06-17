import pandas as pd 

directory = 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/Data Cleaning Project/'
file1 = 'domain_all2.csv'
file2 = 'Main_domain_breakdown-v2.csv'


df1 = pd.read_csv(directory+file1,encoding = "ISO-8859-1",low_memory=False, names = ['Original'])
print(df1.columns)
df1.drop_duplicates('Original', keep='first', inplace=True)
print(df1.shape)

df2 = pd.read_csv(directory+file2,encoding = "ISO-8859-1",low_memory=False)
df2.drop_duplicates('Original', keep='first', inplace=True)
print(df2.shape)
print(df2.columns)

df3 = (df1.merge(df2, on='Original', how='outer', indicator=True)
     .query('_merge != "both"')
     .drop('_merge', 1))

print(df3.shape)
df3.to_csv(directory + "Domain_unmatched.csv", index=False)