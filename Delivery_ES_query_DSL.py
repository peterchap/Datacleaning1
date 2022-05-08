#import requests
import pandas as pd
#from pandas.io.json import json_normalize
#connect to our cluster
from pandasticsearch import Select
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import connections

connections.create_connection(alias='logs', hosts=['http://localhost:9200'], timeout=60)
es = Elasticsearch(hosts=["localhost"])
#
s = Search(using='logs', index="logs*") \
       .query("match", m_process="mailgun") \
       .query("match", m_SendingDomain="mg1.dailygoodybag.com") \
       .query("match", m_Status="bounced") \
       .filter("range", m_LogDate={'gte': "2019-08-12T00:00:00.000Z", 'lt': "2019-08-12"})

data = s.execute()

df= Select.from_dict(data).to_pandas()
to_dropcols = ['_id', '_index', '_score', '_type','m_From','m_LogType','m_MessageId']
df.drop(to_dropcols, axis=1, inplace=True)
print (df.shape)
print(list(df.columns.values))
#print (data['hits'][1])
#print(df)
