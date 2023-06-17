import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client


filename = "2021-09-24-1632441867-fdns_mx.json"
# filename = "mxall-222.csv"
directory = "C:/Users/Peter/Downloads/"
client = Client()
df = dd.read_json(
    directory + filename, lines=True, sample=10000000, blocksize=100000000
)

# And finally combine filtered df_lst into the final lareger output say 'df_final' dataframe

df.drop(columns=["timestamp", "type"])
df["value"] = df["value"].str[3:]
df = (
    df.groupby(by="name")["value"]
    .apply(lambda df: df.reset_index(drop=True))
    .compute()
    .unstack()
    .reset_index()
)

dd.to_csv(df, directory + "mxall-*.csv", index=False)

if __name__ == "__main__":
    freeze_support()
