import pandas as pd

def report_ISP_groups(data, ispgroup):
        
    
    ispdata = pd.merge(data, ispgroup, left_on='domain', right_on='Domain', how='left')
    ispdata.loc[:,'Group'].fillna("Other", inplace = True)
    
    return ispdata

directory = 'E:/A-plan October/'
onedrive='C:/Users/Peter/OneDrive - Email Switchboard Ltd/'
month = 'Oct19'
file = 'tableall.csv'

ispgroups = pd.read_csv(onedrive+'ISP Group domains.csv',encoding = "ISO-8859-1")

df1 = pd.read_csv(directory + file,encoding = "ISO-8859-1",low_memory=False)

df = report_ISP_groups(df1,ispgroups)

df.to_csv(directory + "deliveryclean" + month + "ISP.csv", index=False)