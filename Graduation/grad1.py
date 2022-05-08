import numpy as np
import pandas as pd
import re

directory = "C:/Users/Peter/downloads/graduation work/"
date = "060420"
df = pd.read_csv(directory + "tableall.csv",usecols=range(0,15),encoding='latin-1')
print(list(df))
print("Gross File",df.shape)

df['last_update'] =pd.to_datetime(df['last_update'], errors='coerce')
df.sort_values('last_update', inplace = True)
df.drop_duplicates(subset ="email", keep = False, inplace = True) 
print("Drop Duplicates",df.shape)

df1 = pd.read_csv(directory + "decision_cleaning_mar260320.csv",usecols=range(0,6),encoding='latin-1',
 names = ['email','IP','Send_domain','Message1','Message2','Bounce'])

print(list(df1))
print("Total Bounces", df1.shape)

df2 = pd.merge(df,df1[['email','Bounce']],on='email',how='left')
print("Tableall with Bounce",df2.shape)
df2 = df2.replace({r'\\':''},regex=True)
df2.to_csv(directory + 'Tableall_with_Bounce.csv',index=None,encoding ='ISO-8859-1')
to_drop1 = ['HARD']
hardcount=df2[df2['Bounce'].isin(to_drop1)]
print("Hard Bounces",hardcount.shape)
m1=df2[~df2['Bounce'].isin(to_drop1)]
print("Tableall without Hard bounces",m1.shape)
to_drop2 = ['1']
sndscount=m1[m1['snds'].isin(to_drop2)]
print("snds",sndscount.shape)
m1=m1[~m1['snds'].isin(to_drop2)]
print("Tableall with out SNDS",m1.shape)
to_drop3 = ['DEFERED','EXPIRED','FILTERED','QUEUED','REJECTED']
m1=m1[~m1['status'].isin(to_drop3)]
print("Mailable", m1.shape)

m1['email'].to_csv(directory + "mailable" + date + ".csv",header=False, index=False)
print("Completed Successfully")

to_quarantine = ['1']
q1=df2[df2['snds'].isin(to_quarantine)]
print("SNDS Count",q1.shape)

to_drop5 = ['BOUNCED','DEFERED','DELIVERED','EXPIRED','QUEUED',]
q2=df2[~df2['status'].isin(to_drop5)]
print("Q2",q2.shape)

to_drop6 = ['SOFT',np.nan]
q3=df2[~df2['Bounce'].isin(to_drop6)]
print("Q3",q3.shape)

q4=q2.append(q1)
q5=q4.append(q3)
print("Quarantined",q5.shape)

q5['email'].to_csv(directory + "quarantined" + date + ".csv",header=False, index=False)
print("All Done")   