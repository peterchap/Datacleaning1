import requests
#import urllib3
import socket
import pandas as pd
from pandas.io.json import json_normalize
#connect to our cluster
from pandasticsearch import Select
from elasticsearch import Elasticsearch

onedrive = 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/'

retry_count = 5
for retries in range(retry_count):
    try:
        res = requests.get('http://localhost:9200')
        #Jumps Out Of Loop
        break
    except (socket.gaierror, requests.ConnectionError) as e:
        if e.errno != 10054:
            continue
        else:
            reconnect()
#Does Something If Loop Never Breaks
    else:
        print("Couldn't Connect")

es = Elasticsearch(hosts=["localhost"])
df_lst= pd.DataFrame()
# Process hits here
def process_hits(hits):
    for item in hits:
        # Process hits here
        a = Select.from_dict(data).to_pandas()
        print(a.shape)
        print(list(a.columns.values))
        return(a)
#
data = es.search(index="logs", scroll="20m", body={	"from" : 0, "size" : 9999,   
"query": {
    "bool": {
      "filter": {"term":{
        "m_LogType": "messagelog"}},
    "must": {
        "range": {
          "m_LogDate": {
            "gte": "2020-02-22T00:00:00.000Z",
            "lte": "2020-02-23T00:00:00.000Z"
          }
        }
      }
    }
  },
  "aggs": {
    "Daily Volume": {
      "terms": {
        "field": "m_Status.keyword"
      }
    }
  }
}
        )

# Get the scroll ID
sid = data['_scroll_id']
scroll_size = len(data['hits']['hits'])

while scroll_size > 0:
    print("Scrolling...", sid)
    data = es.scroll(scroll_id=sid, scroll='10m')

    # Process current batch of hits
    
    a = process_hits(data['hits']['hits'])
    df_lst = pd.concat([df_lst,a])

    # Update the scroll ID
    sid = data['_scroll_id']

    # Get the number of results that returned in the last scroll
    scroll_size = len(data['hits']['hits'])

#df= Select.from_dict(df_lst).to_pandas()
print (df_lst.shape)
print(list(df_lst.columns.values))

to_dropcols = ['_id', '_index', '_score', '_type','m_CampaignId', 'm_LogEntry',\
    'm_StatusCode', 'm_From','m_LogType','m_MessageId', 'm_To', 'm_Process']
df_lst.drop(to_dropcols, axis=1, inplace=True)
print (df_lst.shape)
print(list(df_lst.columns.values))
#df_lst = df_lst.sort_values('m_LogDate').drop_duplicates('m_To',keep='last')
print (df_lst.shape)
df_lst.to_csv(onedrive + "bokehgraph_testdata.csv", index=False)
#print (data['hits'][1])
#print(df)
