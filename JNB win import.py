import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
import pyodbc
from validate_email import validate_email
from disposable_email_domains import blocklist
from datetime import datetime, date
import dateparser

from ftplib import FTP
from io import StringIO
import io

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
    data.loc[data["first_name"].str.contains(patternDel, na=False), "status"] = "Bad name"
    data.loc[data["surname"].str.contains(patternDel, na=False), "status"] = "Bad name"
    return data


def report_ISP_groups(data, ispgroup):

    new = data["email"].str.split(pat="@", expand=True)
    data.loc[:, "Left"] = new.iloc[:, 0]
    data.loc[:, "Domain"] = new.iloc[:, 1]

    ispdata = pd.merge(data, ispgroup, on="Domain", how="left")
    ispdata.loc[:, "Group"].fillna("Other", inplace=True)
    stat = pd.DataFrame(ispdata["Group"].value_counts()).reset_index()
    stat.rename(columns={"index": "ISP", "Group": "count"}, inplace=True)
    return stat


# End of Functions

directory = "E:/inboxed/"
onedrive = "C:/Users/Peter/OneDrive - Email Switchboard Ltd/"
directory = "E:/MDEG/JNB/"
dbdirectory= 'E:/MDEG/'

table1 = 'jnb-win2' 

db = 'mdeg.db'
sqlite_engine = create_engine('sqlite:///' + dbdirectory + db)


month = "May20"
listname = "JNB_win"
email = ["email"]

sqlselect = '''
SELECT  *
FROM jnb_win2
WHERE email_date_of_consent > "2019-05-01 00:00:00.000000"
GROUP BY email
'''
db = 'mdeg.db'
sqlite_engine = create_engine('sqlite:///' + dbdirectory + db)

df = pd.read_sql(sqlselect, sqlite_engine)
#df.rename(columns={"EmailAddress": "email"}, inplace=True)
print(df.shape)
print(df.columns)

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

df[['email','domain']].to_sql(
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
    usecols=["name", "location","status"],
)
cols = {"name": "domain"}
df2.rename(columns=cols, inplace=True)
print(" Domain Status File", df2.shape)
print(list(df2.columns.values))

df3 = pd.merge(df1, df2, left_on=["domain"], right_on=["domain"], how="left")

print("Merged File", df3.shape)


#df = flag_temp_domains(df)

# df = pd.merge(df, temps, on=['domain'], how='left')
#df.loc[df["temp"] == 1, "status"] = "Temp domain"
#print("temps File", df.shape)

df3.loc[((df3['is_blacklisted'] == 1) & (df3['status'] == 'OK')), 'status'] ='Blacklisted'
df3.loc[((df3['is_banned_word'] == 1) & (df3['status'] == 'OK')), 'status'] ='Banned words'
#df.loc[df['is_banned_domain'] == 1, 'status'] ='Banned domains'
df3.loc[((df3['is_complaint'] == 1) & (df3['status'] == 'OK')), 'status'] ='Complainers'
df3.loc[((df3['is_hardbounce'] == 1) & (df3['status'] == 'OK')), 'status'] ='Hard Bounces'
#df.loc[((df['user_status'].isin(['Rejected','Cleaning - Dead', 'Cleaning - Quarantined', 'Quarantine']) & df['status'] == 'OK',\
#'status'] = 'Cleaning - Rejected'
#df.loc[df['user_status'] == 'Rejected', 'status'] = 'Cleaning - Rejected'
df3.loc[df3['domain'].str.contains('.gov'), 'status'] = 'EXCLUDED'
#df.loc[df["user_status"].isin(["Rejected", "Cleaning - Quarantined", "Quarantine"]),"status",] = "Cleaning - Rejected"
# df.loc[df['user_status'] == 'Rejected', 'status'] = 'Cleaning - Rejected'
mailable = ["OK"]
df3.loc[(df3["status"].isin(mailable)), "data flag"] = "cleaning"
df3.loc[(~df3["status"].isin(mailable)), "data flag"] = "remove"

df3.loc[~df3["location"].isin(["generic", "Generic", "UK"]), "status"] = "Non-UK"


cols = ["domain", "status"]
unknowns = df3[cols][df3["status"].isnull()]
unknowns.to_csv(directory + listname + "_unknowns" + month + ".csv", index=False)
print("Unknowns", unknowns.shape)
df3.loc[(df3["status"].isnull()), "status"] = "unknown"



print("Final File", df3.shape)
print(df3["data flag"].value_counts())
print(df3["status"].value_counts())

# Start stage 2 processing

df4 = df3[["email", "domain", "status", "data flag"]]


ispgroups = pd.read_csv(onedrive + "ISP Group domains.csv", encoding="ISO-8859-1")

print("Gross", df4.shape[0])


df7 = pd.merge(df, df4, left_on="email", right_on="email", how="left")
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


df7 = df7[df7["data flag"] != "remove"]

df7["title"] = ""
df7["address2"] = ""
df7["address3"] = ""
df7["city"] = ""
df7["county"] = ""
df7["phone"] = ""
df7["dob"] = ""

colsrename = {'first_name' : 'firstname', 'surname' : 'lastname', 'address line 1' : 'address1',\
    'ip_address' : 'ip', 'mobile_number' : 'mobile', 'email_date_of_consent' : 'joindate'}
df7.rename(columns=colsrename, inplace=True)

y = df7[[
    "email",
    "title",
    "firstname",
    "lastname",
    "address1",
    "address2",
    "address3",
    "city",
    "county",
    "postcode",
    "phone",
    "mobile",
    "gender",
    "dob",
    "url",
    "ip",
    "joindate",
        ]
    ]

print(y['gender'].value_counts())
print(y['postcode'].notnull().shape)
print(y['mobile'].notnull().shape)
'''
ftp = FTP("ftp.emailswitchboard.co.uk")
ftp.login("ems_data", "em@1lsw1tchb0@rd")
ftp.cwd("/import/raw")
buffer = StringIO()
y.to_csv(buffer, index=False)
text = buffer.getvalue()
bio = io.BytesIO(str.encode(text))
ftp.storbinary("STOR " + listname + ".csv", bio)
'''

ispstats = pd.DataFrame(report_ISP_groups(df7, ispgroups))
print(ispstats)

ispstats.to_csv(directory + listname + "_ispstats_" + month + ".csv", index=True)
report2.to_csv(directory + listname + "_status_" + month + ".csv", index=True)