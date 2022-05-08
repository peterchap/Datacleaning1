import pandas as pd
from pandas.io.json import json_normalize
import urllib.request
from urllib.error import HTTPError
import json

#try:
#    from urllib.request import urlopen
#except ImportError:
#    from urllib2 import urlopen

onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"
mx = 'void.blackhole.mx'
api_key = 'at_OP1atvVvpWICg5PzlQFBzDpG64Def'
api_url = 'https://reverse-mx-api.whoisxmlapi.com/api/v1?'

url = api_url + 'apiKey=' + api_key + '&mx=' + mx
req = urllib.request.Request(url)
try:
    handler = urllib.request.urlopen(req).read().decode('utf8')
    print(handler)

    c = json.loads(handler)
    d = pd.DataFrame.from_dict(c['result'])
    print(d)
    d.to_csv(onedrive + "reverse mx" + mx + ".csv", index=False)
except HTTPError as e:
    content = e.read().decode('utf8')
    print(content)
#a = urlopen(url).read().decode('utf8')


#print(urlopen(url).read().decode('utf8'))

