import pandas as pd


directory = "C:/Users/Peter/downloads/"
file = "amo wellat selection.csv"

df = pd.read_csv(directory+file, low_memory=False)
df = df[df['dob'].notnull()]
df['dob'] = pd.to_datetime(df['dob'], format='%Y%m%d.0')
df = df[(df['dob'] > '1984-06-30') & (df['dob'] < '2001-06-30')] 
print("Selection", df.shape)

df.to_csv(directory+"amo selection.csv", index=False)

print("Completed Successfully")    
