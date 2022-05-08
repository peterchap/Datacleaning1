# importing required modules 
from zipfile import ZipFile
import fnmatch 
import os
import pandas as pd
import pandas.io.common
  
# specifying the zip file name 
directory = "C:/Users/Peter/downloads/graduation work/"
dfTableAll = pd.DataFrame()
dfBounceAll = pd.DataFrame()
for zipname in os.listdir(directory): 
    print(zipname)
    # extracting tablleall and bounce files 
    with ZipFile(directory+zipname, 'r') as zip:  
        fnames = zip.namelist()
        for name in fnames:
            if fnmatch.fnmatch(directory+name, '*table_all.csv'):
                zip.extract(name, directory)
                try:
                    table = pd.read_csv(directory+"/"+name,usecols=range(0,15),encoding='latin-1')
                except pandas.io.common.EmptyDataError:
                    print(name, "is empty")
                else:
                    dfTableAll =  dfTableAll.append(table)              
            elif fnmatch.fnmatch(name, '*bounce.csv'):
                zip.extract(name, directory)
                try:
                    table = pd.read_csv(directory+"/"+name, header=None,names=list('abcde'))
                except pandas.io.common.EmptyDataError:
                    print(name, "is empty")
                else:
                    dfBounceAll =  dfBounceAll.append(table)
    
print("Writing")
dfTableAll.to_csv(directory+'\\tableall.csv',index=None)
dfBounceAll.to_csv(directory+'\\bounceall.csv',index=None,header=None) 
print("Processing Complete") 