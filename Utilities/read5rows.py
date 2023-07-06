# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np


# directory = 'E:/Cleaning-todo/'

directory = "C:/Users/PeterChaplin/Downloads/"
file="dnstxt_records_full.txt"

N = 200
with open(directory + file, mode="r") as file1, open(
    directory + "domtxt.csv", mode="a"
) as file2:
    for i in range(0, N):
        a = file1.readline()
        file2.write(a)
file1.close()
file2.close()

"""
# df = pd.read_json(directory + filename, lines=True, nrows=5)
# print(df)
# df.to_json(directory + "jsontest.j jtes
# df = pd.read_csv(directory+filename,delimiter='|',encoding ="ISO-8859-1",engine='python', error_bad_lines=False, nrows=5)
df.drop(columns=["timestamp", "type"], inplace=True)
df.drop_duplicates(subset=["name"], keep="first", inplace=True)

print(list(df.columns.values))
print(df.shape)

if __name__ == "__main__":
    freeze_support()
    ...
"""

