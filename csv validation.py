import pandas as pd
from pandas_schema import Column, Schema
from pandas_schema.validation import LeadingWhitespaceValidation, TrailingWhitespaceValidation,\
 CanConvertValidation, MatchesPatternValidation, InRangeValidation, InListValidation

schema = Schema([
    Column('Given Name', [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()]),
    Column('Family Name', [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()]),
    Column('Age', [InRangeValidation(0, 120)]),
    Column('Sex', [InListValidation(['Male', 'Female', 'Other'])]),
    Column('Customer ID', [MatchesPatternValidation(r'\d{4}[A-Z]{4}')])
])

directory = 'E:/MDEG/TDP/'
file = 'Postcode M.csv'

df = pd.read_csv(directory + file, encoding="utf-8",low_memory=False)
print(df.shape)

df = df[df['phone'].notna() | df['mobile'].notna()]
df = df[df['lastname'].notna()]
print(df.shape)
print(df.columns)
df.to_csv(directory + "postcode_m_selection.csv", index=False)