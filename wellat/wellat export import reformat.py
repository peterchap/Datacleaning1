# importing required modules 
from zipfile import ZipFile
import fnmatch 
import os
import pandas as pd
import pandas.io.common
  
# specifying the zip file name 
directory = "C:/Users/Peter/downloads/wellat export/"
directory2 = "C:/Users/Peter/downloads/wellat import/"
cols = ['EMAIL','TITLE','FIRSTNAME','LASTNAME','ADDRESS1','ADDRESS2','ADDRESS3',\
'CITY','COUNTY','POSTCODE','PHONE','MOBILE','GENDER','DOB','URL','IP','JOINDATE']

for zipname in os.listdir(directory): 
    print(zipname)
    # extracting file
    with ZipFile(directory+zipname, 'r') as zip:  
        fnames = zip.namelist()
        for name in fnames:
                zip.extract(name, directory)
                try:
                    table = pd.read_csv(directory+"/"+name,encoding='latin-1', usecols=['email'])
                except pandas.io.common.EmptyDataError:
                    print(name, "is empty")
                else:
                    df = pd.DataFrame(columns=cols)
                    df['EMAIL'] = table['email']             
                    df.to_csv(directory2+name,index=None)
    
print("Processing Complete") 