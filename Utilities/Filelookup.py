import pandas as pd

directory = "C:/Users/Peter/Downloads/"

file1 = "bounced.csv"
file2 = "rs3MTA_queryBOUNCED190321.csv"
datecols = ["m_LogDate"]
df1 = pd.read_csv(directory + file1, low_memory=False, encoding="utf-8")
print(df1.shape)
df1.drop_duplicates(subset=["email"], keep="last", inplace=True)
print(df1.shape)
df2 = pd.read_csv(
    directory + file2, low_memory=False, encoding="utf-8", parse_dates=datecols
)
print(df2.shape)
df2.drop_duplicates(subset=["m_To"], keep="last", inplace=True)
print(df2.shape)
df = df1.merge(df2, left_on="email", right_on="m_To", how="left")
print(df.shape)
df.to_csv(directory + "bounced_with_detail.csv", index=False)
