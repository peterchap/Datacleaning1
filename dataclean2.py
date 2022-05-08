import pandas as pd

df = pd.read_csv("C:/Users/Peter/Downloads/webclubs/ESB_Webclubs_standard_export_2019-03-07.csv",usecols=range(0,28))

print(df.shape)
print(list(df.columns.values))