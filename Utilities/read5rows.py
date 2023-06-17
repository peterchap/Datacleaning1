# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np


# directory = 'E:/Cleaning-todo/'

filename = "2021-09-24-1632441867-fdns_mx.json"
directory = "C:/Users/Peter/Downloads/"

N = 200
with open(directory + filename, mode="r") as file1, open(
    directory + "jmxtest.json", mode="a"
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

