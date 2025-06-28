import csv
import numpy as np
import polars as pl
import os
import re
from google.cloud import bigquery
import pandas as pd
import pandas_gbq as gbq


# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/PeterChaplin/OneDrive - Datazag Ltd/dns-project-17-02-25-5473ff0b8dd9.json"



def flag_invalid_emails(data):
    # First ensure email is string type, handling nulls properly
    data = data.with_columns(pl.col("email").cast(pl.Utf8))
    
    # Apply validation and create data_flag column
    data = data.with_columns(
        # Use str.extract with regex instead of map_elements
        pl.when(~pl.col("email").str.contains(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'))
        .then(pl.lit("Invalid Email Format"))
        .otherwise(pl.lit(None))
        .alias("data_flag")
    )
    
    # Check for @ symbol
    data = data.with_columns(
        pl.when(~pl.col("email").str.contains("@", literal=True))
        .then(pl.lit("Invalid Email Format"))
        .otherwise(pl.col("data_flag"))
        .alias("data_flag")
    )
    
    # Check for invalid characters
    char = r'\+|\*|\'| |\%|,|\"|\/'
    data = data.with_columns(
        pl.when(pl.col("email").str.contains(char))
        .then(pl.lit("Invalid Characters"))
        .otherwise(pl.col("data_flag"))
        .alias("data_flag")
    )
    
    return data

def flag_bad_names(data: pl.DataFrame) -> pl.DataFrame:
    """
    Removes rows from a Polars DataFrame where 'Email', 'FIRSTNAME', or 'LASTNAME'
    columns contain certain blacklisted words, by setting a 'data flag' column to 'Remove'.

    Args:
        data: A Polars DataFrame with 'Email', 'FIRSTNAME', and 'LASTNAME' columns.

    Returns:
        A Polars DataFrame with an added 'data flag' column. Rows containing
        blacklisted words in the specified columns will have 'data flag' set to 'Remove'.
    """
    patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|enquir|fake|feedback|finance|free|garbage|generic|hello|info|invalid|\
    junk|loan|office|market|penis|person|phruit|postmaster|random|recep|register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"

    data = data.with_columns(
        pl.when(
            (pl.col('username').str.contains(patternDel))#|
            #(pl.col('FIRSTNAME').str.contains(patternDel)) |
            #(pl.col('LASTNAME').str.contains(patternDel))
        )
        .then(pl.lit('Remove'))
        .otherwise(pl.col('data_flag'))  # Keep existing value if 'data flag' exists, otherwise None
        .alias('data_flag')
    )
    return data

# Load the dataset
directory = "C:/Users/PeterChaplin/Downloads/"
email = 'email'

df = pl.read_csv(directory + "big_sample.csv", has_header=True, 
                 separator =",", columns = "mailaddress", 
                 new_columns=["email"], low_memory=False)

df_all = df.with_columns(pl.col("email").str.to_lowercase())

print(df_all.head(5))
print("Original count: ", df.shape)

# Remove invalid email formats
df = flag_invalid_emails(df)


df1 = df.with_columns(
    pl.col("email")
    .str.split_exact("@", 1)
    .struct.rename_fields(["username", "domain"])
    .alias("split_email")
).unnest("split_email")
df1.write_parquet(directory + "split_email.parquet", compression="snappy")

df1 = flag_bad_names(df1)
print("Flagged emails", df1.group_by("data_flag").agg(pl.len()).sort("data_flag"))
#df = df.filter(df["data_flag"].is_null())
#print("Removed invalid emails", df.shape)
# Remove duplicates
df1 = df1.unique(subset=["email"])
print("Removed duplicates", df.shape)

#df.write_csv(directory + "cleaned_emails.csv", has_header=True, separator=",", quote_char='"', null_value="NULL")

# Split email into left and domain parts


print(df1.head(5))
print("Split email into left and domain parts")

domains = df1["domain"].unique()
print("Unique domains: ", domains.shape[0])
''''
with open(directory + 'cleaning_domains.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows([[domain] for domain in domains.to_list()])

# Initialize BigQuery client
client = bigquery.Client()
print(os)
# Convert domains to a list
domain_list = domains.to_list()
project_id = "dns-project-17-02-25"
dataset_id = "domains_all_dataset_180225"
# Create a temporary table in BigQuery with the domains
temp_table_name = project_id +"." + dataset_id + ".temp_domains"
job_config = bigquery.LoadJobConfig(
    schema=[bigquery.SchemaField("domain", "STRING")],
    write_disposition="WRITE_TRUNCATE",
)
# Convert Polars Series to Pandas DataFrame
domain_df = pd.DataFrame({"domain": df1["domain"].unique().to_list()})

job = client.load_table_from_dataframe(
    domain_df, temp_table_name, job_config=job_config
)
job.result()  # Wait for the job to complete

# First, get the schema of your big table to extract all column names
table_ref = client.dataset('domains_all_dataset_180225').table('domains_table_deduped')
table = client.get_table(table_ref)

# Build a list of column names, excluding any you want to skip
column_names = [field.name for field in table.schema]

# Build the SQL query dynamically
column_projection = ",\n    t2.".join(column_names)

# Query to match domains with the BigQuery table
sql = f"""
SELECT t1.domain, t2.{column_projection}
FROM `{temp_table_name}` AS t1
LEFT JOIN `dns-project-17-02-25.domains_all_dataset_180225.domains_table_deduped` AS t2
ON t1.domain = t2.domain
"""

nested_df = client.query(sql).to_dataframe()

# Print information about the first few rows
print(f"Original DataFrame shape: {nested_df.shape}")
print(f"Columns: {nested_df.columns}")

# Flattening function specifically for numpy arrays containing lists of dictionaries
flattened_data = []

for idx, row in nested_df.iterrows():
    domain = row['domain']
    result = {'domain': domain}
    
    try:
        data = row['DATA']
        
        # Handle numpy array containing a list with dictionary
        if isinstance(data, np.ndarray) and data.size > 0:
            # Convert numpy array to list
            data_list = data.tolist()
            
            # Extract the dictionary from the list
            if isinstance(data_list, list) and len(data_list) > 0:
                if isinstance(data_list[0], dict):
                    result.update(data_list[0])
    except Exception as e:
        print(f"Error processing row {idx}: {str(e)}")
    
    flattened_data.append(result)

# Create final DataFrame
results_df = pd.DataFrame(flattened_data)

print(f"Final DataFrame shape: {results_df.shape}")
print(f"Final columns: {list(results_df.columns)[:10]} + {len(results_df.columns)-10} more")
write_path = "C:/Users/PeterChaplin/Downloads/"
results_df.to_csv(write_path + "matched_domains.csv", index=False)
print("Matched domains saved to CSV")
'''
#Process the matched domains
domains_df = pl.read_csv(directory + "matched_domains.csv", has_header=True,
                 separator =",", encoding= "latin", columns = ["domain", "tld_country", "is_mailable",  "is_phishing", "is_malware", "is_disposable", "is_spf_block",
                "is_parked", "is_new_domain", "decision_flag",], low_memory=False)
#columns = ["domain", "tld_country", "is_phishing", "is_malware", "is_disposable", "is_spf_block",
#        "is_parked", "is_new_domain", "decision_flag",
#],
print("Matched domains loaded from CSV")
print("Matched domains shape: ", domains_df.shape)
domains_df.write_parquet(directory + "matched_domains.parquet", compression="snappy")
print("Null Decision flag: ", domains_df.filter(pl.col("decision_flag").is_null()).shape[0])
print("Null Decision flag: ", domains_df.filter(pl.col("decision_flag").is_null()).group_by("domain").agg(pl.len()).sort("domain"))  
#process retry and dormant domains
df_retry = pl.read_parquet(directory + "gcs_awbatch_0.parquet", 
            columns=["domain", "tld_country", "is_mailable", "is_phishing", 
                     "is_malware", "is_disposable", "is_spf_block", "is_parked",
                       "is_new_domain", "decision_flag"]).select(
            pl.col("domain"),
            pl.col("tld_country"),
            pl.col("is_mailable"),
            pl.col("is_disposable"),
            pl.col("is_phishing"),
            pl.col("is_malware"),
            pl.col("is_spf_block"),
            pl.col("is_parked"),
            pl.col("is_new_domain"),
            pl.col("decision_flag"),
            pl.lit(0).alias("is_dormant").cast(pl.Boolean))

#print("Retry columns: ", df_retry.columns)
print("df_retry: ", df_retry.shape)
#print("domains_df columns", domains_df.columns)

df_dormant1 = pl.read_parquet(directory + "failed_retry3_aw_missing.parquet",columns = ["domain"]).with_columns(
    pl.lit("None").alias("tld_country").cast(pl.String),
    pl.lit(0).alias("is_mailable").cast(pl.Boolean),
    pl.lit(0).alias("is_disposable").cast(pl.Boolean),
    pl.lit(0).alias("is_phishing").cast(pl.Boolean),
    pl.lit(0).alias("is_malware").cast(pl.Boolean),
    pl.lit(0).alias("is_spf_block").cast(pl.Boolean),
    pl.lit(0).alias("is_parked").cast(pl.Boolean),
    pl.lit(0).alias("is_new_domain").cast(pl.Boolean),
    pl.lit(0).alias("decision_flag").cast(pl.Boolean),
    pl.lit(1).alias("is_dormant").cast(pl.Boolean))

df_dormant2 = pl.read_parquet(directory + "failed_retry4_aw_missing.parquet",columns = ["domain"]).with_columns(
    pl.lit("None").alias("tld_country").cast(pl.String),
    pl.lit(0).alias("is_mailable").cast(pl.Boolean),
    pl.lit(0).alias("is_disposable").cast(pl.Boolean),
    pl.lit(0).alias("is_phishing").cast(pl.Boolean),
    pl.lit(0).alias("is_malware").cast(pl.Boolean),
    pl.lit(0).alias("is_spf_block").cast(pl.Boolean),
    pl.lit(0).alias("is_parked").cast(pl.Boolean),
    pl.lit(0).alias("is_new_domain").cast(pl.Boolean),
    pl.lit(0).alias("decision_flag").cast(pl.Boolean),
    pl.lit(1).alias("is_dormant").cast(pl.Boolean))

df_dormant = pl.concat([df_dormant1, df_dormant2]).unique(subset=["domain"])
print("Dormant shape: ", df_dormant.shape)

print("Dormant count: ", df_dormant.filter(pl.col("is_dormant") == 1).shape)
print("Dormant columns: ", df_dormant.columns)

# Ensure both DataFrames have the same schema and column names
# Ensure both DataFrames have the same schema and column names

df_retry = df_retry.with_columns(
    pl.col("is_mailable").cast(pl.Boolean),
    pl.col("is_phishing").cast(pl.Boolean),
    pl.col("is_malware").cast(pl.Boolean),
    pl.col("is_disposable").cast(pl.Boolean),
    pl.col("is_spf_block").cast(pl.Boolean),
    pl.col("is_parked").cast(pl.Boolean),
    pl.col("is_new_domain").cast(pl.Boolean),
    pl.col("decision_flag").cast(pl.Boolean),
    pl.lit(0).alias("is_dormant").cast(pl.Boolean))

domains_df = domains_df.with_columns(
    pl.col("is_mailable").cast(pl.Boolean),
    pl.col("is_phishing").cast(pl.Boolean),
    pl.col("is_malware").cast(pl.Boolean),
    pl.col("is_disposable").cast(pl.Boolean),
    pl.col("is_spf_block").cast(pl.Boolean),
    pl.col("is_parked").cast(pl.Boolean),
    pl.col("is_new_domain").cast(pl.Boolean),
    pl.col("decision_flag").cast(pl.Boolean),
    pl.lit(0).alias("is_dormant").cast(pl.Boolean))

print("Retry columns: ", df_retry.columns)
print("Retry shape: ", df_retry.shape)
print("Domains columns: ", domains_df.columns)
print("Domains shape: ", domains_df.shape)
print("Dormant columns: ", df_dormant.columns)
print("Dormant shape: ", df_dormant.shape)

# check for duplicates in the domain column
duplicates = df_retry.filter(pl.col("domain").is_in(domains_df["domain"])).shape[0]
print("Duplicates in df_retry: ", duplicates)
print("Duplicates in df_dormant: ", df_dormant.filter(pl.col("domain").is_in(domains_df["domain"])).shape[0])   
dupdormant = df_dormant.filter(pl.col("domain").is_in(domains_df["domain"])).shape[0]
print("Duplicates in df_dormant: ", dupdormant)
dupdomains = domains_df.filter(pl.col("domain").is_in(df_retry["domain"])).shape[0]
print("Duplicates in df_domains: ", dupdomains)
dupretry = df_retry.filter(pl.col("domain").is_in(df_dormant["domain"])).shape[0]
print("Duplicates in df_retry: ", dupretry)
#remove duplicates from domains_df
domains_df = domains_df.filter(~pl.col("domain").is_in(df_retry["domain"]))
print("Duplicates removed from domains_df: ", domains_df.shape[0])
domains_df = domains_df.filter(~pl.col("domain").is_in(df_dormant["domain"]))
print("Duplicates removed from domains_df: ", domains_df.shape[0])
# Concatenate the DataFrames
df_domains_all = pl.concat([df_retry, domains_df, df_dormant])

print("Domains_all shape: ", df_domains_all.shape)
#print("Domains_unique: ", df_domains_all.unique(subset=["domain"]))

df_final = df1.join(df_domains_all, on="domain", how="left")
print("Final shape: ", df_final.shape)


missed_domains = df1.join(df_domains_all, on="domain", how="anti")
missed_unique = missed_domains.unique(subset=["domain"])
print("Missed unique domains: ", missed_unique.shape[0])

missed_unique.write_parquet(directory + "missed_domains.parquet", compression="snappy")
print("Missed domains saved to Parquet")
print("Missed domains: ", missed_domains.shape[0])
# If decision flag is null, set it to 1
df_final = df_final.with_columns(
    pl.when(pl.col("decision_flag").is_null())
    .then(pl.lit(1))
    .otherwise(pl.col("decision_flag"))
    .alias("decision_flag")
)
# find data for traffordave.freeserve.co.uk
print("Test: ", df_final.filter(pl.col("domain") == "traffordave.freeserve.co.uk"))
print("Initial shape: ", df1.shape)
print("Final columns: ", df_final.columns)
print("Final shape: ", df_final.shape)
print("Bad email shape: ", df_final.filter(pl.col("decision_flag") == 0).shape)
print("Good emails shape: ", df_final.filter(pl.col("decision_flag") == 1).shape)
print("Bad demails: ", df_final.filter(pl.col("decision_flag") == 0).group_by("decision_flag").agg(pl.len()).sort("decision_flag"))
print("Dormant domains: ", df_final.filter(pl.col("is_dormant") == 1).group_by("is_dormant").agg(pl.len()).sort("is_dormant"))
print("bad dataflag", df_final.filter(pl.col("data_flag").is_not_null()).shape)
print("bad dataflag: ", df_final.filter(pl.col("data_flag").is_not_null()).group_by("data_flag").agg(pl.len()).sort("data_flag"))
# Ensure the columns exist in df_final before filtering
if "decision_flag" in df_final.columns and "data_flag" in df_final.columns:
    df_bad = df_final.filter(
        (pl.col("decision_flag") == 0) | 
        (pl.col("data_flag").is_not_null()) |
        (pl.col("is_dormant") == 1))
    df_good = df_final.filter(
        (pl.col("decision_flag") == 1) & 
        (pl.col("data_flag").is_null())
        & (pl.col("is_dormant") == 0))
    print("Emails Good: ", df_good.shape)
    print("Emails Bad: ", df_bad.shape)
else:
    print("Error: Required columns 'decision_flag' or 'data_flag' are missing in df_final.")
print("Decision FLag Breakdown: ", df_final.group_by("decision_flag").agg(pl.len()).sort("decision_flag"))
print("Dormant flag: ", df_final.filter(pl.col("is_dormant") == 1).shape)
print("is_mailable flag: ", df_final.filter(pl.col("is_mailable") == 0).shape)
print("Phishing flag: ", df_final.filter(pl.col("is_phishing") == 1).shape)
print("Malware flag: ", df_final.filter(pl.col("is_malware") == 1).shape)
print("Disposable flag: ", df_final.filter(pl.col("is_disposable") == 1).shape)
print("SPF block flag: ", df_final.filter(pl.col("is_spf_block") == 1).shape)
print("Parked flag: ", df_final.filter(pl.col("is_parked") == 1).shape)
print("New domain flag: ", df_final.filter(pl.col("is_new_domain") == 1).shape)
print("Decision flag: ", df_final.filter(pl.col("decision_flag") == 0).shape)
print("Data flag: ", df_final.filter(pl.col("data_flag").is_not_null()).shape)
print("Data flag: ", df_final.filter(pl.col("data_flag").is_not_null()).group_by("data_flag").agg(pl.len()).sort("data_flag"))

# Save the final DataFrame to a Parquet file
df_final.write_parquet(directory + "cleaned_emails.parquet", compression="snappy")
print("Cleaned emails saved to Parquet")
# Save Bad Emails to Parquet
