import polars as pl
import pandas as pd


directory = "C:/Users/PeterChaplin/Downloads/"
file = "Apollo_V7_V5_org_all_fields.csv"
"""
df = pd.read_csv(directory + file, sep="\t", nrows=50, engine="python")
print(df.head())
df.to_parquet(directory + "Apollo_V7_V5_per_all_fields.parquet")
"""

pl.scan_csv(
    directory + file,
    separator="\t",
    truncate_ragged_lines=True,
    ignore_errors=True,
    infer_schema_length=2000,
).collect().write_parquet(directory + "Apollo_V7_V5_org_all_fields.parquet")
print("done")
