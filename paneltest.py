import pandas as pd

directory = 'C:/Users/PeterChaplin_fkt3wwy/Downloads/'
file = 'paneldatatest.json'

df = pd.read_json(directory + file, orient='columns')
print(df)
