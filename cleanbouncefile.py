
import re
import numpy as np
import pandas as pd

df = pd.read_csv("C:\\20190201_bounce.csv", header=None)

df = df.replace({r'\\':''},regex=True)
df = df.replace('"','',regex=True)
print(df.head())
