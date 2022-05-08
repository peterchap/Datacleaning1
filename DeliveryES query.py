import requests
import pandas as pd
from pandas.io.json import json_normalize
#connect to our cluster
from pandasticsearch import Select
from elasticsearch import Elasticsearch

res = requests.get('http://localhost:9200')
es = Elasticsearch(hosts=["localhost"])
#
data = es.search(index="logs*", body={	
          "from" : 0, "size" : 1000000,  
	  "query" : {
	  "bool":{
	    "must": [
	      {"match":{"m_SendingDomain": "email.market-savings.com"}},
	      {"match":{"m_CampaignId": "ESB"}}
	      ],
	      "filter": [
	        {"range" : {"m_LogDate" : { 
            "gte" : "2019-08-13T00:00:00.000Z", 
            "lt" : "2019-08-20T00:00:00.000Z"}}}
	        ]
	  }
	  }
})

df= Select.from_dict(data).to_pandas()
to_dropcols = ['_id', '_index', '_score', '_type','m_From','m_LogType','m_MessageId']
df.drop(to_dropcols, axis=1, inplace=True)
print (df.shape)
print(list(df.columns.values))
#print (data['hits'][1])
#print(df)
