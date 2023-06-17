import pandas as pd
import numpy as np
#file=sys.argv[1]Home_GBO_Cleaning_Sept19.csv
directory = 'E:/Wellat/'
file = 'oldstinvestor_optedinsavings_email_selection.csv'
file1 = file[0:17] + 'Oct19_part'
print(file1)
df = pd.read_csv(directory+file,encoding = "ISO-8859-1",low_memory=False)
print("Gross file",df.shape[0])
count = 1
for g, data in df.groupby(np.arange(len(df)) // 95000):
    print(data.shape)
    data.to_csv(directory + file1 + str(count)+ '.csv', index=False, header=True)
    count += 1  