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
            (pl.col('email').str.contains(patternDel))#|
            #(pl.col('FIRSTNAME').str.contains(patternDel)) |
            #(pl.col('LASTNAME').str.contains(patternDel))
        )
        .then(pl.lit('Profanity or Role Address'))
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

df = flag_bad_names(df)
print("Flagged emails", df.group_by("data_flag").agg(pl.count()).sort("data_flag"))
df = df.filter(df["data_flag"].is_null())
print("Removed invalid emails", df.shape)


# Remove duplicates
df = df.unique(subset=["email"])
print("Removed duplicates", df.shape)

#df.write_csv(directory + "cleaned_emails.csv", has_header=True, separator=",", quote_char='"', null_value="NULL")

# Split email into left and domain parts
df1 = df.with_columns(
    pl.col("email")
    .str.split_exact("@", 1)
    .struct.rename_fields(["username", "domain"])
    .alias("split_email")
).unnest("split_email")

print(df1.head(5))
print("Split email into left and domain parts")
''''
domains = df1["domain"].unique()
print("Unique domains: ", domains.shape[0])

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
domains_df = pl.read_csv(directory + "matched_domains.csv", has_header=True,
                 separator =",", encoding= "latin", columns = ["domain", "tld_country", "is_phishing", "is_malware", "is_disposable", "is_spf_block",
        "is_parked", "is_new_domain", "decision_flag",
], low_memory=False)

print("Matched domains loaded from CSV")
print("Matched domains shape: ", domains_df.shape)  

df_bad = domains_df.filter(pl.col("decision_flag") == 0)
df_good = domains_df.filter(pl.col("decision_flag") == 1)
print("Domains Good: ", df_good.shape)
print("Domains Bad: ", df_bad.shape)
df_final = df1.join(domains_df, on="domain", how="left")
'''
df_final = df_final.with_columns(
    pl.when(pl.col("domain").is_null())
    .then(pl.lit(0))
    .otherwise(pl.lit(1))
    .alias("decision_flag")
)
'''
print("Final shape: ", df_final.shape)
print("Bad shape: ", df_final.filter(pl.col("decision_flag") == 0).shape)
print("Good shape: ", df_final.filter(pl.col("decision_flag") == 1).shape)

#process retries and
df_retry = pl.read_parquet(directory + "processed_aw_missing.parquet")
df_retry2 = pl.read_parquet(directory + "processed_retry3_aw_missing.parquet")