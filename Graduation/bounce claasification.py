import pandas as pd

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
day = '28102019'
bouncefile = 'Aplan_Cleaning_Delivery_Oct19.csv'

df = pd.read_csv(directory + bouncefile)
print(list(df.columns.values))
print(df.shape)
print(df.dtypes)

ispgroups = pd.read_csv(onedrive+'ISP Group domains.csv',encoding = "ISO-8859-1")

df = pd.merge(df, ispgroups, left_on='m_RecipientDomain', right_on='Domain', how='left')
df['Group'].fillna("Other", inplace = True)

df['Block'] = df.apply (lambda row: blocker(row), axis=1)

print(list(df.columns.values))
print(df.shape)
df.to_csv(directory + "Aplan_Cleaning_bounces_classification_" + day + ".csv", index=False)
pivot = pd.pivot_table(df,index=['m_CampaignId'], columns='Block', values='m_To', aggfunc= 'count')
pivot.to_csv(directory + "Aplan_Cleaning_Campaignpivot_" + day + ".csv")