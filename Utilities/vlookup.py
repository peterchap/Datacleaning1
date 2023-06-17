import pandas as pd

directory = 'C:/Users/Peter/Downloads/A-Plan August Renewal Data/'
file = '00005A_ORG23301_A_Plan_August_Car_insurance_Branch.csv'
category = 'Car National'

df1 = pd.read_csv(directory + file,encoding = "ISO-8859-1",low_memory=False)
print(df1.shape)
print(list(df1.columns.values))

df2 = pd.read_csv(directory + "aplan_aug.all_data_flag.csv")
df2 = df2.drop_duplicates(subset='email', keep='first')

print(df2.shape)
print(list(df2.columns.values))

df = pd.merge(df1, df2, left_on='Email', right_on='email', how='left')
df['CODEDT'] = df['CODE'] + df['DT_EmailSource'].map(str)

print(df.shape)

to_dropcols = ['list_id', 'source_url', 'title', 'first_name', 'last_name', 'id', 'email', 'optin_date', 'is_duplicate', 'is_ok', \
'is_blacklisted', 'is_banned_word', 'is_banned_domain', 'is_complaint', 'is_hardbounce', 'Remove', 'domain', 'user_status', 'last_open',\
'last_click', 'system_created', 'master_filter', 'import_filter', 'email_id',  'domain flag', 'name', 'rtotal']
df.drop(to_dropcols, axis=1, inplace=True)

print(df.shape)
print(list(df.columns.values))

for i, x in df.groupby(['CODEDT']):
      #x.to_csv(directory + category +"_" + i + ".csv", index=False)
      print(i, x.shape)
      for j, m in x.groupby('data flag'):
            print(j, m.shape)

#for i, x in df.groupby(['CODE','DT_EmailSource']):
#        x.to_csv(directory + category + i[0] + str(int(i[1])) + ".csv", index=False)

#df.to_csv(directory + file +"_with data flag.csv", index=False)