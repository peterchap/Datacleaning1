import tabula
import pandas as pd

directory = 'C:/Users\PeterChaplin_fkt3wwy/OneDrive - Datazag Ltd\Documents/bank statements/'
file = "Bank statements.pdf"
# Convert your file
df = tabula.read_pdf(directory+file, pages=2, pandas_options={'header': None})
print(len(df))
x = df[0]
print(x)
