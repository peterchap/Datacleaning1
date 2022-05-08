import pandas as pd

directory = 'C:/Users/Peter/Downloads/'

df1 = pd.read_csv(directory + "JUNB_201907_check.csv",sep=',',encoding = "ISO-8859-1",low_memory=False)
df2 = pd.read_csv(directory + "WA_GUFP_JUNB_matched.csv",sep=',',encoding = "ISO-8859-1",low_memory=False)


print("All Data Flag", df1.shape[0])
print("All original", df2.shape[0])



df = pd.merge(df1, df2, left_on='email', right_on='email', how='left')
    
nulls = df[df['data flag'].isnull()]
print("Nulls",nulls.shape)
    
to_dropcols = [ 'is_valid_email', 'Left', 'Domain', 'Temp' ]
df.drop(to_dropcols, axis=1, inplace=True)

for i, x in df.groupby(["data flag"]):
        x.to_csv(directory +"WA_GUFP3007_" + i + ".csv", index=False)
        print(i, x.shape[0])    
        
    
