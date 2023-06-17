import pandas as pd
import numpy as np

directory ='C:/Users/Peter/Downloads/'

file1 = 'Email counts per segment 20200227.csv'
file2 = 'Email counts per segment 20200228.csv'

touse = ['Active','Active Probational 140-180 days', 'Active Probational 181 days plus',\
     'Active Probational 91-180 days', 'Active-Gmail', 'Active-Hotmail', 'Cleansing Hotmail',\
     'Cleansing Other', 'New', 'New-Gmail', 'New-Hotmail', 'Probational 31-90 days',\
     'Probational 91-180 days', 'Probational-Gmail', 'Probational-Hotmail',\
    'Reactive 30-90 days', 'Repossess 30', 'Repossess B', 'Repossess C', 'Repossess C GMail',\
    'Repossess C Hotmail', 'Repossess-Gmail', 'Repossess-Hotmail']

previous  = pd.read_csv(directory + file1,encoding ='ISO-8859-1')
recent  = pd.read_csv(directory + file2,encoding ='ISO-8859-1')

prevpivot = previous.pivot(index='List name', columns=' Segment', values = ' Count')
recentpivot = recent.pivot(index='List name', columns=' Segment', values = ' Count')

a = prevpivot.to_numpy(copy=True)
b = recentpivot.to_numpy(copy=True)

c= b - a

colstodrop = ['All','Psna 50+F Active GM','Psna 50+F Active MS', 'Psna 50+F Active Oth',\
       'Psna 50+F Non Active GM', 'Psna 50+F Non Active MS','Psna 50+F Non Active Oth', 'Psna 50+M Active GM',\
       'Psna 50+M Active MS', 'Psna 50+M Active Oth','Psna 50+M Non Active GM', 'Psna 50+M Non Active MS',\
       'Psna 50+M Non Active Oth', 'Psna DKYF Active GM', 'Psna DKYF Active MS', 'Psna DKYF Active Oth',\
       'Psna DKYF Non Active GM', 'Psna DKYF Non Active MS', 'Psna DKYF Non Active Oth', 'Psna DKYM Active GM',\
       'Psna DKYM Active MS', 'Psna DKYM Active Oth','Psna DKYM Non Active GM', 'Psna DKYM Non Active MS',\
       'Psna DKYM Non Active Oth', 'Psna GnF Active GM', 'Psna GnF Active MS', 'Psna GnF Active Oth', 'Psna GnF Non Active GM',\
       'Psna GnF Non Active MS', 'Psna GnF Non Active Oth', 'Psna GnM Active GM', 'Psna GnM Active MS', 'Psna GnM Active Oth',\
       'Psna GnM Non Active GM', 'Psna GnM Non Active MS', 'Psna GnM Non Active Oth', 'Psna GnU Active GM', 'Psna GnU Active MS',\
       'Psna GnU Active Oth', 'Psna GnU Non Active GM', 'Psna GnU Non Active MS', 'Psna GnU Non Active Oth',\
       'Psna YFF Active GM', 'Psna YFF Active MS', 'Psna YFF Active Oth', 'Psna YFF Non Active GM', 'Psna YFF Non Active MS',\
       'Psna YFF Non Active Oth', 'Psna YFM Active GM', 'Psna YFM Active MS', 'Psna YFM Active Oth', 'Psna YFM Non Active GM',\
       'Psna YFM Non Active MS', 'Psna YFM Non Active Oth', 'Psna YSF Active GM', 'Psna YSF Active MS', 'Psna YSF Active Oth',\
       'Psna YSF Non Active GM', 'Psna YSF Non Active MS', 'Psna YSF Non Active Oth', 'Psna YSM Active GM', 'Psna YSM Active MS',\
       'Psna YSM Active Oth', 'Psna YSM Non Active GM', 'Psna YSM Non Active MS', 'Psna YSM Non Active Oth']
final = pd.DataFrame( data = c, index = recentpivot.index, columns = recentpivot.columns )
final.drop(colstodrop, axis=1, inplace=True) 
print(final.shape)
final['Total'] = final.iloc[:,0:23].sum(axis=1)
final['Modulus'] = final['Total'].abs()
final = final[final['Modulus'] > 0]
print(final)
final.to_csv(directory + "list segment change_feb28.csv")