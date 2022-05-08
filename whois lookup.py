try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

import json
import pandas as pd
from pandas.io.json import json_normalize

directory ="C:/Users/Peter/Downloads/"
file= 'spamtrapnowhois.csv'

domainName = pd.read_csv(directory+file)
domains = list(domainName.Name)

apiKey = 'at_OP1atvVvpWICg5PzlQFBzDpG64Def'
whoisdata = []
noresult = []

for domain in domains:
    url = 'https://www.whoisxmlapi.com/whoisserver/WhoisService?' + 'domainName=' + domain + '&apiKey=' + apiKey + "&outputFormat=JSON"
    try:
        print(domain)
        data = urlopen(url).read().decode('utf8')          
    except:
        noresult.append([domain, "Error"])
    else:
        data1 =(json.loads(data))
        a = data1['WhoisRecord']['registrarName']
        b = data1['WhoisRecord']['registryData']['nameServers']['hostNames'][0]
        #c = data1['WhoisRecord']['registrant']['countryCode']
        whoisdata.append([domain,a, b])

df = pd.DataFrame(whoisdata,columns=['Domain', 'Registrar' 'Host'])
errors = pd.DataFrame(noresult, columns= ['Domain', 'No Result'])

print("Results", df.shape[0])
print("Errors", errors.shape[0])  

df.to_csv(directory +  "Spamtrap_whois.csv", index=False)
errors.to_csv(directory +  "Spamtrap_whoiserrors.csv", index=False)
print("Completed Successfully") 