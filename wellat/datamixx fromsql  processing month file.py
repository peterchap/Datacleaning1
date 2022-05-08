import pandas as pd

directory = 'C:/Users/Peter/Downloads/'
file = 'WA_GUFP_JUNB_sqldone.csv'

df1 = pd.read_csv(directory + file,sep=',',encoding = "ISO-8859-1",low_memory=False)
print(df1.shape)
print(list(df1.columns.values))

df1.drop_duplicates(subset='email', keep='first',inplace=True)
print(df1.shape)

df2 = pd.read_csv("C:/Users/Peter/Downloads/domain_status_UK.csv",encoding = "ISO-8859-1",low_memory=False)
print(df2.shape)
print(list(df2.columns.values))



df = pd.merge(df1, df2, on=['domain'], how='left')

df['dataflag'] = ''

print(df.shape)

df['rtotal'] = df['is_blacklisted'] + df['is_banned_word'] + df['is_banned_domain'] + df['is_complaint'] + df['is_hardbounce']


print(df['rtotal'].value_counts())
print(list(df.columns.values))
print(df['name'].value_counts(dropna=False))

df.loc[df.rtotal > 0, 'data flag'] ='Remove'

print(df['data flag'].value_counts())

print(df['user_status'].value_counts())

df.loc[(df['data flag'] != 'Remove') & (df['user_status'] != 'Mailable'), 'data flag'] = 'In Cleaning'



df.loc[(df['data flag'].isnull()) , 'data flag'] = 'In Production'
print(df['data flag'].value_counts())

bad =['FOREIGN', 'UNKNOWN', 'NO MX', 'EXCLUDED', 'BAD', 'BLACKLISTED','SPAM TRAP', 'EXPIRED', 'Not Set', 'TEMP', 'INVALID']
df.loc[df.name.isin(bad), 'data flag'] = 'Remove'

print(df['data flag'].value_counts())

cols = ['domain','data flag','name']
a = df[cols][df['name'].isna() & (df['data flag'] != 'Remove')]

df.loc[(df['name'].isna() & (df['data flag'] != 'Remove')),'data flag'] = 'Domain Check'


print(df['data flag'].value_counts())
print('Domain Unknown',a.count(), a)

print(df['name'].value_counts())

print(list(df.columns.values))

cols2 =['email', 'data flag']

df[cols2].to_csv(directory + "WA_GUFP_JUNB_matched.csv", index=False)
#a.to_csv(directory +  "datamixx_matched_unknowns.csv", index=False)