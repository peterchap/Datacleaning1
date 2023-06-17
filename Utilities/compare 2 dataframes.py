import pandas as pd

directory1 = 'E:/Acquirze/'
file1 ='Acquirz July20.csv'
directory2 = 'E:/Company house/'
file2 = 'BasicCompanyDataAsOneFile-2020-07-01.csv'

df1 = pd.read_csv(directory1 + file1,encoding = "ISO-8859-1",low_memory=False, usecols=['Company Name'])
df1['Company Name'] = df1['Company Name'].str.lower()
print(df1.shape)
df1.drop_duplicates(subset=['Company Name'], inplace=True)
print(df1.shape)
print(list(df1.columns.values))

df2 = pd.read_csv(directory2 + file2 ,encoding = "utf-8",low_memory=False, usecols=['CompanyName'])
df2['CompanyName'] = df2['CompanyName'].str.lower()
#df2['mobile'] = df2['mobile'].apply(str)
print(df2.shape)
print(df2.head())


df = pd.merge(df1, df2, left_on=['Company Name'], right_on=['CompanyName'], how='inner')

#df.to_csv(directory + 'mxcombined.csv', index=False)
print(df.shape)



