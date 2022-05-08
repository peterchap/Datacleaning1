import pandas as pd
import numpy as np

file1 = "py80lookup-GL2022.xlsx"
file2 = "Burghfield Wednesday Evening Personal Handicap 27 04 22.xlsx"
file3 = "Wednesday Results R3 270422.xlsx"

directory = "C:/Users/PeterChaplin/OneDrive - esbconnect/Burghfield/"


date = "04 05 22"

# import data

df1 = pd.read_excel(
    directory + file1, usecols=["Class", "PN", "Start"],
)
print(df1.shape)
print(df1.columns)
df2 = pd.read_excel(directory + file2,
                    usecols=["Name", "Boat", "Sail", "Personal Handicap"])
print(df2.shape)
print(df2.columns)
df3 = pd.read_excel(
    directory + file3, usecols=["Name", "Boat", "Sail", "Result"]
)
print(df3.shape)
print(df3.columns),
df4 = df3.merge(df2, on=["Name", "Boat", "Sail"], how="outer")
df4 = df4.merge(df1, left_on="Boat", right_on="Class", how="left")
df4["Result"] = df4["Result"].apply(pd.to_numeric, errors="coerce")
df4["Result"] = df4["Result"].fillna(0)
df4["Personal Handicap"] = df4["Personal Handicap"].fillna(0)

print(df4.shape)


# calculate revised personal handicap

# model variables

starters = df4["Result"].count()
print("Starters ", starters)

rt = 80  # race time
m1 = 2
m2 = 0.4
rb = 25
t = 3300

df4["L"] = (rt - df4["Start"] + df4["Personal Handicap"]) * 60
df4["D"] = (starters / 2) - df4["Result"]
df4["P"] = 50 - (df4["Result"] / starters * 100)
df4["A"] = (df4["D"] * m1) + (df4["P"] * m2)
df4["A1"] = df4["A"] * df4["L"] / ((rt - rb) * 60)
df4["NRT"] = df4["L"] + df4["A1"]
df4["NPH1"] = df4["NRT"] - ((rt - df4["Start"]) * 60)
df4.loc[df4["Result"] > 0, "NPH"] = (
    round(((100 + df4["NPH1"] / 60) * 2)) / 2) - 100
df4.loc[df4["NPH"].isna(), "NPH"] = df4["Personal Handicap"]
print(df4.head(5))

df4.to_csv(directory + "bsctest.csv", index=False)

final = df4[["Name", "Class", "Sail", "PN", "Start", "NPH"]].copy()
final["Start time"] = final["Start"] + final["NPH"]
final.sort_values(by=["Start time"], ascending=True, inplace=True)

final.to_excel(
    directory + "Burghfield Wednesday Evening Personal Handicap " + date + ".xlsx",
    index=False,
)
