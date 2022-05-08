import pandas as pd

onedrive= 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/'

df1 = pd.read_csv(onedrive + "domain_status_UK.csv",encoding = "ISO-8859-1",low_memory=False)

print(df1.shape)

df1 = df1[df1['status'] == 'SPAM TRAP']

print(df1.shape)

df2 = pd.read_csv(onedrive + "domain_main.csv",encoding = "ISO-8859-1",low_memory=False)
df3 = pd.read_csv(onedrive + "Domain_MX.csv",encoding = "ISO-8859-1",low_memory=False)

df4 = pd.merge(df1, df2, on=['domain'], how='left')
mxresult = pd.merge(df4, df3, on=['id'], how='left')


to_drop = [ 'status', 'tld', 'mx_compare', 'mx_processed', 'who_processed',\
'reprocess_paid', 'mx_selected', 'who_selected', 'created', 'file_added_by_id', 'domain_id',\
'a_count', 'mx_count',  'last_updated']

mxresult.drop(to_drop, axis=1, inplace=True)

mxresult.to_csv(onedrive +"spamtrap_MX_lookup.csv", index=False)