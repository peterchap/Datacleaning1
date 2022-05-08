import whois
#import json
#import pandas as pd

#onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"
#file= 'spam trap com domains.csv'
#domain = 'microsoft.com'
data = whois.query('esbconnect.com')
print(data.__dict__)