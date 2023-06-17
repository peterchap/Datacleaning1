import pandas as pd

filename = "mxall-222.csv"
directory = "C:/Users/Peter/Downloads/"

df = pd.read_csv(directory + filename)

df.drop(columns=["timestamp", "type"], inplace=True)
df["name"] = df["name"].str.rstrip(". ")
print(df.shape)
result = (
    df.groupby("name")["value"]
    .apply(lambda df: df.reset_index(drop=True))
    .unstack()
    .reset_index()
)
# result.rename(columns={0: "mx"}, inplace=True)
# result["mx"] = result["mx"].str[3:]
result.to_csv(directory + "mxall-test2.csv", index=False)
