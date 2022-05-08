import pandas as pd

directory = "E:/Wellat/"

file1 = 'ssn wellat selection.csv'
file2 = 'ssn mobile selection.csv'

df1 = pd.read_csv(directory+file1,low_memory=False)
df2 = pd.read_csv(directory+file2,low_memory=False)

df3 = pd.merge(df1,df2[['email','mobile','consent_mobile']], left_on = 'email', right_on = 'email', how='inner')

df3.to_csv(directory+"ssn_optedin_mobile_selection.csv", index=False)
print("Completed Successfully")