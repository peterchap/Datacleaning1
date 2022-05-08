import pandas as pd

directory = "E:/Wellat/"

file1 = 'optin wellat savings20190925.csv'
file2 = 'ESB_data_20190920.csv'
colstokeep = [ 'TITLE', 'FIRSTNAME', 'LASTNAME',  'DOB', 'supplier_code', 'CONSENT_SOURCE', 'CONSENT_EMAIL', 'EMAIL']
df1 = pd.read_csv(directory+file1,low_memory=False)
df2 = pd.read_csv(directory+file2,encoding ="ISO-8859-1",low_memory=False,usecols=colstokeep)

df3 = pd.merge(df1,df2, left_on = 'email', right_on = 'EMAIL', how='left')

df3.to_csv(directory+"oldstinvestor_optedinsavings_email_selection.csv", index=False)
print(df3.shape)
print("Completed Successfully")