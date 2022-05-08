import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
import pyodbc
from validate_email import validate_email
from disposable_email_domains import blocklist 

# Functions


def flag_temp_domains(data):
    domains = data["domain"]
    # domains.drop_duplicates(inplace=True)
    m = []
    for domain in domains:
        m.append((domain, domain in blocklist))
    n = pd.DataFrame(m, columns=("domain", "temp"))
    data["temp"] = n["temp"]
    return data


# Remove invalid email formats


def remove_invalid_emails(data):
    data.loc[~data["email"].apply(lambda x: validate_email(x)), "data flag"] = "remove"
    data.loc[
        ~data.email.str.contains("@", na=False), "status"
    ] = "Invalid email address"
    char = "\+|\*|'| |\%|,|\"|\/"
    data.loc[
        df.email.str.contains(char, regex=True, na=False), "status"
    ] = "Invalid email address"
    return data


# Remove Bad status


def remove_bad_status(data):
    patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|\
        enquir|fake|feedback|finance|free|garbage|generic|hello|info|invalid|\
        junk|loan|office|market|penis|person|phruit|police|postmaster|random|recep|\
        register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
    data.loc[data["email"].str.contains(patternDel, na=False), "status"] = "Bad name"
    return data


directory = 'E:/MDEG/Data Samples/'
directory1 = 'E:/MDEG/Data Samples/clean/'
onedrive = "C:/Users/Peter/OneDrive - Email Switchboard Ltd/"

filename = "winaclick_sample.csv"
month = "Jun20"
listname = "inboxed"
email = ['email']

df = pd.read_csv(directory + filename, encoding="ISO-8859-1", usecols=email)
#df.rename(columns={"EmailAddress": "email"}, inplace=True)
print(df.shape)
df['email'] = df['email'].str.lower()
df.drop_duplicates(subset=["email"], inplace=True)
print(df.shape)

df["is_valid_email"] = df["email"].apply(lambda x: validate_email(x))
df = df[df["is_valid_email"]]
print("Removed invalid email formats", df.shape)
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
df.drop("is_valid_email", axis=1, inplace=True)
print("SQL File", df.shape)

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
    onedrive + "Data Cleaning Project/domain_status.csv",
    encoding="ISO-8859-1",
    low_memory=False,
    usecols=["name", "status"],
)
cols = {"name": "domain"}
df2.rename(columns=cols, inplace=True)
print(" Domain Status File", df2.shape)
print(list(df2.columns.values))

df = pd.merge(df1, df2, left_on=["domain"], right_on=["domain"], how="left")

print("Merged File", df.shape)


df = flag_temp_domains(df)

# df = pd.merge(df, temps, on=['domain'], how='left')
df.loc[df["temp"] == 1, "status"] = "Temp domain"
print("temps File", df.shape)

df.loc[((df['is_blacklisted'] == 1) & (df['status'] == 'OK')), 'status'] ='Blacklisted'
df.loc[((df['is_banned_word'] == 1) & (df['status'] == 'OK')), 'status'] ='Banned words'
#df.loc[df['is_banned_domain'] == 1, 'status'] ='Banned domains'
df.loc[((df['is_complaint'] == 1) & (df['status'] == 'OK')), 'status'] ='Complainers'
df.loc[((df['is_hardbounce'] == 1) & (df['status'] == 'OK')), 'status'] ='Hard Bounces'
#df.loc[((df['user_status'].isin(['Rejected','Cleaning - Dead', 'Cleaning - Quarantined', 'Quarantine']) & df['status'] == 'OK',\
#'status'] = 'Cleaning - Rejected'
#df.loc[df['user_status'] == 'Rejected', 'status'] = 'Cleaning - Rejected'
df.loc[df['domain'].str.contains('.gov'), 'status'] = 'EXCLUDED'
#df.loc[df["user_status"].isin(["Rejected", "Cleaning - Quarantined", "Quarantine"]),"status",] = "Cleaning - Rejected"
# df.loc[df['user_status'] == 'Rejected', 'status'] = 'Cleaning - Rejected'
mailable = ["OK"]
df.loc[(df["status"].isin(mailable)), "data flag"] = "cleaning"
df.loc[(~df["status"].isin(mailable)), "data flag"] = "remove"


df3 = pd.read_csv(
    onedrive + "TLDGeneric lookup.csv", encoding="ISO-8859-1", low_memory=False
)

tld = df["domain"].str.rsplit(pat=".", n=1, expand=True)

df["tld"] = tld[1]
df = pd.merge(df, df3, on=["tld"], how="left")
print("tld File", df.shape)
df.loc[
    ~df["location"].isin(["generic", "Generic", "United Kingdom (UK)"]), "status"
] = "Non-UK"


cols = ["domain", "status"]
unknowns = df[cols][df["status"].isnull()]
unknowns.to_csv(directory + filename[:-4] + "_unknowns" + month + ".csv", index=False)
print("Unknowns", unknowns.shape)
df.loc[(df["status"].isnull()), "status"] = "unknown"

# Start stage 2 processing

df4 = pd.read_csv(directory + filename, encoding="ISO-8859-1")
df4.rename(columns={"EmailAddress": "email"}, inplace=True)
df['email'] = df['email'].str.lower()
print(df4.shape)
df4.columns = map(str.lower, df4.columns)

df4.drop_duplicates(subset=["email"], inplace=True)
print(df4.shape)

df6 = df[["email", "domain", "status", "data flag"]]



print("Gross", df6.shape[0])


df7 = pd.merge(df4, df6, left_on="email", right_on="email", how="left")
print(df7.columns)

df7 = remove_bad_status(df7)

# Generate Cleaning report data

print(df7.shape[0])
dataflags = df7["data flag"].value_counts()
report1 = dataflags.rename_axis("Description").reset_index(name="Count")
print(report1)
statusflags = df7["status"].value_counts()
report2 = statusflags.rename_axis("Description").reset_index(name="Count")

print(report2)
# Output ESB import file in correct


#df8 = df7[df7["data flag"] == "cleaning"].copy()
df7.drop_duplicates(subset=["email"], inplace=True)
df7.rename(columns={"email" : "EmailAddress"}, inplace=True)
df7.drop(columns = ['domain', 'status', 'data flag'], inplace=True)

print(df7.shape)
print(df7.columns)
df7.to_csv(directory1 + filename, index=False)