import pandas as pd

from sqlalchemy import create_engine, MetaData, Table, select, Integer, Column

directory = "C:/Users/Peter/Downloads/"
file = "2021-09-24-1632441867-fdns_mx.json"
tablename = "mx_all"

connect_string = "mysql+pymysql://root:1Francis2@127.0.0.1/global"

sql_engine = create_engine(connect_string, echo=False, pool_recycle=3600)

chunk_iter = pd.read_json(
    directory + file, encoding="ISO-8859-1", lines=True, chunksize=5000
)
for chunk in chunk_iter:
    chunk.drop(columns=["timestamp", "type"], inplace=True)
    chunk["name"] = chunk["name"].str.rstrip(". ")
    print(chunk.shape)
    chunk = (
        chunk.groupby("name")["value"]
        .apply(lambda chunk: chunk.reset_index(drop=True))
        .unstack()
        .reset_index()
    )
    print(chunk.columns)
    if 1 not in chunk.columns:
        chunk[1] = ""
    if 2 not in chunk.columns:
        chunk[2] = ""
    if 3 not in chunk.columns:
        chunk[3] = ""
    if 4 not in chunk.columns:
        chunk[4] = ""
    chunk = chunk[["name", 0, 1, 2, 3, 4]]
    chunk["grp"] = chunk["name"].apply(lambda x: x[0])
    chunk.to_sql(tablename, sql_engine, if_exists="append", chunksize=50000)

