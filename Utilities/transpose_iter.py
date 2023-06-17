import pandas as pd

filename = "2021-09-24-1632441867-fdns_mx.json"
directory = "C:/Users/Peter/Downloads/"
chunksize = 100000
df_iter = pd.read_json(directory + filename, lines=True, chunksize=chunksize)

df_lst = []
iter = 0
count = 0

for df_ in df_iter:
    df_lst += [df_]
    count = count + df_.shape[0]
    iter = iter + 1
    print(iter)
# And finally combine filtered df_lst into the final lareger output say 'df_final' dataframe

df_final = pd.concat(df_lst)
df_final.drop(columns=["timestamp", "type"], inplace=True)
df_final.rename(columns={0: "mx"}, inplace=True)

print(list(df_final.columns.values))
print(list(df_final.shape))


df_final.to_csv(directory + "mxall.csv", index=False)
