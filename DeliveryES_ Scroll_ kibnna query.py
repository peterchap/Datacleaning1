import requests
import socket
import pandas as pd
from pandas.io.json import json_normalize
#connect to our cluster
from pandasticsearch import Select
from elasticsearch import Elasticsearch

directory = 'C:/Users/Peter/Downloads/'

retry_count = 5
for retries in range(retry_count):
    try:
        res = requests.get('http://localhost:9200')
        #Jumps Out Of Loop
        break
    except (socket.gaierror, requests.ConnectionError) as e:
        if e.errno != 10054:
            continue
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
data = es.search(index="logs*", scroll="2m",body={
  "aggs": {
    "4": {
      "terms": {
        "field": "m_Status.keyword",
        "size": 5,
        "order": {
          "2": "desc"
        }
      },
      "aggs": {
        "2": {
          "cardinality": {
            "field": "m_MessageId.keyword"
          }
        },
        "3": {
          "filters": {
            "filters": {
              "Microsoft": {
                "query_string": {
                  "query": "m_RecipientDomain : hotmail.com OR hotmail.co.uk OR outlook.com OR live.co.uk OR live.com OR msn.com"
                }
              },
              "Google": {
                "query_string": {
                  "query": "m_RecipientDomain : gmail.com OR googlemail"
                }
              },
              "Yahoo": {
                "query_string": {
                  "query": "m_RecipientDomain : yahoo.com OR yahoo.co.uk OR ymail.com OR sky.com OR  aol.com OR rocketmail.com"
                }
              },
              "BTInternet": {
                "query_string": {
                  "query": "m_RecipientDomain : btinternet.com OR btconnect.com OR bt.com OR talk21.com OR openreach.co.uk"
                }
              },
              "Apple": {
                "query_string": {
                  "query": "m_RecipientDomain : icloud.com OR me.com OR mac.com"
                }
              },
              "TalkTalk": {
                "query_string": {
                  "query": "m_RecipientDomain : talktalk.net OR tiscali.co.uk OR tinyworld.co.uk OR tinyonline.co.uk OR lineone.net OR ukgateway.net OR worldonline.co.uk OR screaming.net"
                }
              },
              "Virgin": {
                "query_string": {
                  "query": "m_RecipientDomain : virginmedia.com OR virgin.net OR blueyonder.com OR ntlworld.com"
                }
              },
              "1&1": {
                "query_string": {
                  "query": "m_RecipientDomain : mail.com OR gmx.co.uk OR gmx.com"
                }
              }
            }
          },
          "aggs": {
            "2": {
              "cardinality": {
                "field": "m_MessageId.keyword"
              }
            }
          }
        }
      }
    }
  },
  "size": 0,
  "_source": {
    "excludes": []
  },
  "stored_fields": [
    "*"
  ],
  "script_fields": {},
  "docvalue_fields": [
    {
      "field": "m_LogDate",
      "format": "date_time"
    }
  ],
  "query": {
    "bool": {
      "must": [
              {"filter": [
        	      {"range" : {"m_LogDate" : { 
                  "gte" : "2020-02-21T00:00:00.000Z", 
                  "lte" : "2019-09-22"}}}]
              }
            }
          }
        }
      ]
    }
  }
})

# Get the scroll ID
sid = data['_scroll_id']
scroll_size = len(data['hits']['hits'])

while scroll_size > 0:
    print("Scrolling...", sid)
    data = es.scroll(scroll_id=sid, scroll='2m')

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

to_dropcols = ['_id', '_index', '_score', '_type','m_From','m_LogType','m_MessageId']
df_lst.drop(to_dropcols, axis=1, inplace=True)
print (df_lst.shape)
print(list(df_lst.columns.values))
df_lst = df_lst.sort_values('m_LogDate').drop_duplicates('m_To',keep='last')
print (df_lst.shape)
df_lst.to_csv(directory + "InternalMTA_delivery.csv", index=False)
#print (data['hits'][1])
#print(df)
