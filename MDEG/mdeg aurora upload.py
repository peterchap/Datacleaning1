import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, date
import dateparser
import boto3
import pyarrow as pa


dbdirectory= 'E:/MDEG/'
directory = 'E:/MDEG/'

tablename = 'main' 
db = 'mdeg-v2.db'
db2 = 'mdeg.db'

#rds_client = boto3.client('rds-data')
#database_name = 'mdeg-main'
sqlite_engine = create_engine('sqlite:///' + dbdirectory + db)

'''
cluster_arn = "arn:aws:rds:eu-west-1:176349594720:cluster:mdeg-main"
secret_arn = "arn:aws:secretsmanager:eu-west-1:176349594720:secret:mdeg-main-aurora-YUuFGr"
engine = create_engine('mysql+auroradataapi://:@/mdeg-main',
                       echo=True,
                       connect_args=dict(aurora_cluster_arn=cluster_arn, secret_arn=secret_arn))
'''

df = pd.read_sql_table(tablename, sqlite_engine)
print(df.shape)

df['email_date_of_latest_engagement']=pd.to_datetime(df['email_date_of_latest_engagement'])
df['email_unsub_bounce_block']=pd.to_datetime(df['email_unsub_bounce_block'])
df['ip_date_of_capture']=pd.to_datetime(df['ip_date_of_capture'])
df['landline_date_of_consent']=pd.to_datetime(df['landline_date_of_consent'])
df['mobile_date_of_consent']=pd.to_datetime(df['mobile_date_of_consent'])
df['postal_date_of_consent']=pd.to_datetime(df['postal_date_of_consent'])
df['cookie_date_of_consent']=pd.to_datetime(df['cookie_date_of_consent'])
df['date moved in']=pd.to_datetime(df['date moved in'])
df['date first appeared on file']=pd.to_datetime(df['date first appeared on file'])
df['date_added'] = date.today()

print(df.shape)
#dd.to_parquet(df=df, path= directory+'main.parquet', engine='pyarrow', index=False)
#print('parquet successful')

#df.to_sql('main', engine, if_exists='replace',chunksize=500, index=False)
df.to_csv(directory + "main_v2.csv")
print("completed successfully")
