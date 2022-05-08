import pandas as pd
import ast

directory = 'E:/Cleaning-todo/'
file = '23022021 - Full FF Opted in Database.csv'

def explodedict(row):
    row['custom_attributes'] = row['custom_attributes'].replace('=>', ':')
    row['custom_attributes'] = ast.literal_eval((row['custom_attributes']))
    for key in row['custom_attributes']:
        row[key] = row['custom_attributes'][key]
    return(row)


readcols = ['email', 'uuid', 'phone_number', 'joined_at', 'firstname', 'lastname', 'gender',\
    'custom_attributes']

df = pd.read_csv(directory + file, encoding ='utf-8', usecols= readcols)

df = df.apply(lambda row: explodedict(row), axis=1)
print(df.columns)
