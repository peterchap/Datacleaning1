import requests
#import urllib3
import socket
import pandas as pd
from pandas.io.json import json_normalize
#connect to our cluster
from pandasticsearch import Select
from elasticsearch import Elasticsearch


def blocker (row):
    if row['m_Status'] == 'DELIVERED':
        return 'Delivered'
    elif  row['m_Status'] == 'FILTERED':
        return 'Filtered'
    elif row['m_Status'] == 'EXPIRED':
        return 'Expired'
    if row['Group'] == 'Other':
        if ('cyren') in str(row['m_LogEntry']).lower():
            return 'Cyren'
        elif ('symantec') in str(row['m_LogEntry']).lower():
            return 'Symantec'
        elif ('proofpoint') in str(row['m_LogEntry']).lower():
            return 'Proofpoint'
        elif ('barracuda') in str(row['m_LogEntry']).lower():
            return 'Barracuda'
        elif ('trend') in str(row['m_LogEntry']).lower():
            return 'Trend Micro'
        elif ('mimecast') in str(row['m_LogEntry']).lower():
            return 'Mimecast'
        elif ('cloudmark') in str(row['m_LogEntry']).lower():
            return 'Cloudmark'
        elif ('spamhaus/invaluement/returnpath') in str(row['m_LogEntry']).lower():
            return 'Spamhaus/Invaluement/ReturnPath'    
        elif ('spamhaus') in str(row['m_LogEntry']).lower():
            return 'Spamhaus'
        elif ('invaluement') in str(row['m_LogEntry']).lower():
            return 'Invaluement'
        elif ('return path') in str(row['m_LogEntry']).lower():
            return 'Return Path'
        elif ('returnpath') in str(row['m_LogEntry']).lower():
            return 'Return Path'
        elif ('ers-rbl') in str(row['m_LogEntry']).lower():
            return 'ERS-RBL'
        elif ('spamcop') in str(row['m_LogEntry']).lower():
            return 'SpamCop'
        elif ('sophos') in str(row['m_LogEntry']).lower():
            return 'Sophos'
        elif ('outlook') in str(row['m_LogEntry']).lower():
            return 'Microsoft Block'
        elif ('kundserver') in str(row['m_LogEntry']).lower():
            return '1&1 Block'
        elif ('invalid') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'        
        elif ('no mx') in str(row['m_LogEntry']).lower():
            return 'No MX'
        elif ('unknown') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'    
        elif ('not exist') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'    
        elif ('recipient address rejected') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif ('recipient undeliverable') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif ('recipientnotfound') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif ('no such') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif ('could not be found') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif ('not found') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif ('No user') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif ('deleted') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif ('disabled') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif ('permanent') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif ('relay') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif ('route') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif ('unroutable') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif ('verify') in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif ('is not') in str(row['m_LogEntry']).lower() and row['m_Status'] == 'BOUNCED':
            return 'Undeliverable email'
        elif ('unable') in str(row['m_LogEntry']).lower() and row['m_Status'] == 'BOUNCED':
            return 'Undeliverable email'
        elif ('quota') in str(row['m_LogEntry']).lower():
            return 'Over Quota'    
        elif ('spam') in str(row['m_LogEntry']).lower():
            return 'Spam Block'
        elif ('blocked') in row['m_Status']:
            return 'Spam Block'    
        return 'Unknown'
    elif row['Group'] == '1&1':
        if row['m_StatusCode'] == '4.2.1':
            return 'Throttled'
        elif row['m_StatusCode'] == '4.5.1':
            return 'Throttled'
        elif row['m_StatusCode'] == '5.5.0':
            return 'Undeliverable email'        
        elif row['m_StatusCode'] == '5.5.4':
            return '1&1 Block'
        elif '6.0' in row['m_StatusCode']:
            return '1&1 Block'
        else:
            return 'Unknown'
    elif row['Group'] == 'Apple':
        if row['m_StatusCode'] == '4.5.0':
            return 'Over Quota'
        elif '4.0.0' in row['m_StatusCode']:
            return 'Throttled'
        elif '4.2.1' in row['m_StatusCode']:
            return 'Throttled'
        elif '4.5.1' in row['m_StatusCode']:
            return 'Throttled'                  
        elif '5.5.0' in row['m_StatusCode']:
            return 'Apple Block'
        elif '6.0' in row['m_StatusCode']:
            return 'Apple Block'
        elif ('proofpoint') in str(row['m_LogEntry']).lower().lower():
            return 'Proofpoint'
        else:
            return 'Unknown'
    elif row['Group'] == 'BT':
        if '4.0.0' in row['m_StatusCode']:
            return 'Throttled'
        elif '4.2.1' in row['m_StatusCode']:
            return 'Spam Throttle'      
        elif '4.5.0' in row['m_StatusCode']:
            return 'Undeliverable email'     
        elif '4.5.1' in row['m_StatusCode']:
            return 'Undeliverable email'     
        elif row['m_StatusCode'] == '5.2.2':
            return 'BT SPR block'
        elif '5.5.0' in row['m_StatusCode']:
            return 'BT Block'
        elif '6.0' in row['m_StatusCode']:
            return 'BT Block'
        elif row['m_StatusCode'] == '5.5.4':
            return 'BT Spam block'
        else:
            return 'Unknown'
    elif row['Group'] == 'Google':
        if '4.5.2' in row['m_StatusCode']:
            return 'Over Quota'
        elif '5.5.2' in row['m_StatusCode']:
            return 'Over Quota'       
        elif '5.5.0' in row['m_StatusCode']:
            return 'Google Block'
        elif '6.0' in row['m_StatusCode']:
            return 'Google Block'
        else:
            return 'Unknown'
    elif row['Group'] == 'Microsoft':
        if '4.5.1' in row['m_StatusCode']:
            return 'Throttled'
        elif '5.5.0' in row['m_StatusCode']:
            return 'Microsoft Block'
        elif '6.0' in row['m_StatusCode']:
            return 'Microsoft Block'   
        else:
            return 'Unknown'
    elif row['Group'] == 'Talk Talk':
        if '4.0.0' in row['m_StatusCode']:
            return 'Throttled'
        elif '4.5.3' in row['m_StatusCode']:
            return 'Spam Throttle'    
        elif row['m_StatusCode'] == '5.5.0':
            return 'Undeliverable email'
        elif '6.0' in row['m_StatusCode']:
            return 'Talk Talk block'    
        else:
            return 'Unknown'
    elif row['Group'] == 'Verizon':
        if '4.0.0' in row['m_StatusCode']:
            return 'Throttled'        
        elif row['m_StatusCode'] == '4.2.1':
            return 'Verizon Block'
        elif row['m_StatusCode'] == '4.5.1':
            return 'Verizon Block'    
        elif row['m_StatusCode'] == '5.5.4':
            return 'Undeliverable email'    
        elif '6.0' in row['m_StatusCode']:
            return 'Verizon Block'    
        else:
            return 'Unknown'
    elif row['Group'] == 'Virgin':
        if '4.0.0' in row['m_StatusCode']:
            return 'Throttled'        
        elif row['m_StatusCode'] == '4.5.1':
            return 'Mimecast Block'
        elif row['m_StatusCode'] == '4.2.1':
            return 'Virgin Block'    
        elif row['m_StatusCode'] == '5.5.0':
            return 'Undeliverable email'    
        elif row['m_StatusCode'] == '5.5.4':
            return 'Spam Throttle'
        elif '6.0' in row['m_StatusCode']:
            return 'Virgin Block'    
        else:
            return 'Unknown'
    elif row['Group'] == 'Other':
        if 'invalid' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'        
        elif 'no mx' in str(row['m_LogEntry']).lower():
            return 'No MX'
        elif 'unknown' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'    
        elif 'not exist' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'    
        elif 'recipient address rejected' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif 'recipient undeliverable' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif 'recipientnotfound' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif 'no such' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif 'could not be found' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif 'not found' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif 'No user' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif 'deleted' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif 'disabled' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif 'permanent' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif 'relay' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif 'route' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif 'unroutable' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif 'verify' in str(row['m_LogEntry']).lower():
            return 'Undeliverable email'
        elif 'is not' in str(row['m_LogEntry']).lower() and row['m_status'] == 'BOUNCED':
            return 'Undeliverable email'
        elif 'unable' in str(row['m_LogEntry']).lower() and row['m_status'] == 'BOUNCED':
            return 'Undeliverable email'
        elif 'quota' in str(row['m_LogEntry']).lower():
            return 'Over Quota'    
        elif 'spam' in str(row['m_LogEntry']).lower():
            return 'Spam Block'
        elif 'blocked' in row['m_Status']:
            return 'Spam Block' 
        else:
            return 'Unknown'
    return 'Unknown'
     
directory = 'C:/Users/Peter/Downloads/'
onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"
day = '10122019'
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
data = es.search(index="logs", scroll="20m",body={	
          "from" : 0, "size" : 9999,   
	  "query" : {
	  "bool":{
	    #"must": [
	    #   {"match" :{"m_CampaignId": "OemPro"}}
	    #  ],
	    #"must_not": [
        "must": [
	     {"match" :{"m_Process": "mailgun"}}
	      ],
	      "filter": [
        	   {"range" : {"m_LogDate" : { 
                    "gte" : "2019-12-10T00:00:00.000Z", 
                    "lte" : "2019-12-11T00:00:00.000Z"}}}
	        ]
	  }
	  }
})

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

to_dropcols = ['_id', '_index', '_score', '_type','m_From','m_LogType','m_MessageId']
df_lst.drop(to_dropcols, axis=1, inplace=True)
print (df_lst.shape)
print(list(df_lst.columns.values))
df_lst = df_lst.sort_values('m_LogDate').drop_duplicates('m_To',keep='last')
print (df_lst.shape)



ispgroups = pd.read_csv(onedrive+'ISP Group domains.csv',encoding = "ISO-8859-1")

df = pd.merge(df_lst, ispgroups, left_on='m_RecipientDomain', right_on='Domain', how='left')
df['Group'].fillna("Other", inplace = True)

df['Block'] = df.apply (lambda row: blocker(row), axis=1)

print(list(df.columns.values))
print(df.shape)
df.to_csv(directory + "Mailgun_bounces_classification_" + day + ".csv", index=False)
pivot = pd.pivot_table(df,index=['m_SendingDomain'], columns='Block', values='m_To', aggfunc= 'count')
pivot.to_csv(directory + "Mailgun_Domainpivot_" + day + ".csv")