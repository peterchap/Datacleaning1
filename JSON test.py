import pandas as pd
from pandas.io.json import json_normalize
import json



onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"
file = 'JSONTEST.txt'



with open(onedrive + file) as json_file:
    data = json.load(json_file)
    for p in data['result']:
        print('Name: ' + p['name'])


b = json_normalize(data['result'])
print(b)
#b.to_csv(onedrive +  "Spamtrap_mx37m1bpcom.csv", index=False) 