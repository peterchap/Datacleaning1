import pandas as pd 

directory = 'E:/MDEG/Data Samples/'
file1 = 'affy_octopus_170620.csv'
file2 = 'affy_octopus_sample.csv'

df1 = pd.read_csv(directory + file1, encoding = "utf-8",low_memory=False)
print(df1.shape)
print(list(df1.columns.values))

df2 = pd.read_csv(directory + file2, encoding = "utf-8",low_memory=False)

print(df2.shape)
print(list(df2.columns.values))

df = pd.merge(df1, df2, left_on='mobile_nr', right_on='mobile_nr', how='left', indicator=True).query('_merge == "left_only"').drop('_merge', 1)
df.to_csv(directory + "Octopus_170620.csv", index=False)