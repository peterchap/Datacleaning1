import os
import glob
import pandas as pd
os.chdir("E:/Cleaning-todo/UK ESB LM 22.01.2020/")

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

#combine all files in the list
combined_csv = pd.concat([pd.read_csv(f, encoding = "ISO-8859-1", low_memory=False) for f in all_filenames ])
#export to csv
combined_csv.to_csv( "combinedinbox.csv", index=False, encoding='utf-8-sig')

print("Completed Successfully")