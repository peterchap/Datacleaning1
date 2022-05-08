import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
import pyodbc
from validate_email import validate_email
from disposable_email_domains import blocklist
import time


def flag_temp_domains(data):
    domains = data["domain"]
    # domains.drop_duplicates(inplace=True)
    m = []
    for domain in domains:
        m.append((domain, domain in blocklist))
    n = pd.DataFrame(m, columns=("domain", "temp"))
    data["temp"] = n["temp"]
    return data


directory = "C:/Users/Peter/Downloads/"
# dbdirectory= 'E:/Acquirze/'
onedrive = "C:/Users/Peter/OneDrive - Email Switchboard Ltd/Data Cleaning Project/"

filename = "IPQS_5K_280921.csv"

month = "oct21"
listname = "test"
email = "Email"


print("Start Time: ", time.ctime(time.time()))
# df = pd.read_sql(sqlselect, sqlite_engine,  columns='email_address')
df = pd.read_csv(directory + filename, encoding="ISO-8859-1", usecols=[email])
df.rename(columns={email: "email"}, inplace=True)
print(df.columns)
df.columns = map(str.lower, df.columns)
df["email"] = df["email"].str.lower()
df.drop_duplicates(subset=["email"], inplace=True)
print(df.shape)

# df['is_valid_email'] = df['email'].apply(lambda x:validate_email(x))
# df = df[df['is_valid_email']]
# print("Removed invalid email formats", df.shape)
print(df[~df.email.str.contains("@", na=False)])
df = df[df.email.str.contains("@", na=False)]
char = "\+|\*|'| |\%|,|\"|\/"
df = df[~df["email"].str.contains(char, regex=True)]

print("Removed invalid email addresses", df.shape)

new = df["email"].str.split(pat="@", expand=True)
df["domain"] = new[1]
df["list_id"] = listname

df.drop_duplicates(subset="email", keep="first", inplace=True)
df = df.reset_index(drop=True)
# df.drop('is_valid_email',axis =1,inplace=True)
print("SQL Input File", df.shape)

df.astype({"email": str, "domain": str})


server = "78.129.204.215"
database = "ListRepository"

engine = create_engine(
    "mssql+pyodbc://perf_webuser:n3tw0rk!5t@t5@"
    + server
    + "/"
    + database
    + "?driver=ODBC+Driver+17+for+SQL+Server",
    fast_executemany=True,
)


cnxn = engine.connect()
rs = cnxn.execute("DELETE FROM dbo.temp_tia")
cnxn.close()
print(rs)

df.to_sql(
    "temp_tia",
    con=engine,
    schema="dbo",
    if_exists="append",
    index=False,
    chunksize=1000,
)


cursor = engine.raw_connection().cursor()
cursor.execute("dbo.Temp_Tia_UpdateMetadata")
cursor.commit()

query = "SELECT * FROM dbo.temp_tia"
df1 = pd.read_sql_query(query, engine)

print(df1.shape)
print("Temp_tia processing completed successfully")


df2 = pd.read_csv(
    onedrive + "domain_status.csv",
    encoding="UTF-8",
    low_memory=False,
    usecols=["name", "status", "owner", "description", "location"],
)
print(df2.shape)

cols = {"name": "domain"}
df2.rename(columns=cols, inplace=True)
df.drop_duplicates(subset=["domain"], inplace=True)
print(" Domain Status File", df2.shape)
print(list(df2.columns.values))

df = pd.merge(df1, df2, on=["domain"], how="left")

print("Merged File", df.shape)
cols = ["domain", "status"]
df.loc[(df["status"].isnull()), "status"] = "unknown"
unknowns = df[cols][df["status"].isin(["unknown"])]
unknowns.to_csv(directory + filename[:-4] + month + "_unknowns.csv", index=False)
print("Unknowns", unknowns.shape)
# df.loc[((df['is_blacklisted'] == 1) & (df['status'] == 'OK')), 'status'] ='Blacklisted'
# df.loc[((df['is_banned_word'] == 1) & (df['status'] == 'OK')), 'status'] ='Banned words'
# df.loc[df['is_banned_domain'] == 1, 'status'] ='Banned domains'
df.loc[((df["is_complaint"] == 1) & (df["status"] == "OK")), "status"] = "Complainers"
df.loc[((df["is_hardbounce"] == 1) & (df["status"] == "OK")), "status"] = "Hard Bounces"
# df.loc[((df['user_status'].isin(['Rejected', 'Cleaning - Quarantined', 'Quarantine']) & df['status'] == 'OK',\
#'status'] = 'Cleaning - Rejected'
# df.loc[df['user_status'] == 'Rejected', 'status'] = 'Cleaning - Rejected'
df.loc[df["domain"].str.contains(".gov"), "status"] = "Gov domain"
print(df["location"].value_counts(dropna=False))
print(df["status"].value_counts(dropna=False))

df.loc[
    (
        (~df["location"].isin(["generic", "Generic", "United Kingdom (UK)", "UK"]))
        & (df["status"].isnull())
    ),
    "status",
] = "Non-UK"


mailable = ["OK", "Not Checked", "Recheck", "Gov domain", "Non-UK"]
df.loc[(df["status"].isin(mailable)), "data flag"] = "cleaning"
df.loc[(~df["status"].isin(mailable)), "data flag"] = "remove"

print("Final File", df.shape)
print(df["data flag"].value_counts())
print(df["status"].value_counts())
counts = df["status"].value_counts().rename_axis("status").reset_index(name="counts")
counts.to_csv(directory + filename[:-4] + month + "Statuscounts.csv", index=False)
df.to_csv(directory + filename[:-4] + month + "Stage1Completev2.csv", index=False)
print("Finish Time: ", time.ctime(time.time()))

