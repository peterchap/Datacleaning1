import pandas as pd 

directory = 'E:/Cleaning-todo/'
file = 'Experian_270121_refesh.csv'

df = pd.read_csv(directory+file, encoding = "ISO-8859-1", low_memory=False)

counts = df['status'].value_counts().rename_axis('Status').reset_index(name='Counts')
print(counts)

counts.to_csv(directory +  "Experian_status_counts" + ".csv", index=False)