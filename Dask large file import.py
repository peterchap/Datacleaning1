import pandas as pd
import dask.dataframe as dd

directory = "C:/Users/Peter/downloads/"
file = "mixxitmedia_20190523.csv"

type={'finance_pension_company': 'object',
       'occupation_military': 'object',
       'occupation_professional': 'object',
       'occupation_professional_occupation': 'object',
       'personal_broadband_aol': 'object',
       'personal_broadband_bt': 'object',
       'personal_broadband_orange': 'object',
       'personal_broadband_sky': 'object',
       'personal_broadband_talktalk': 'object',
       'personal_broadband_tiscali': 'object',
       'personal_broadband_virgin': 'object',
       'personal_car_breakdown_aa': 'object',
       'personal_car_breakdown_greenflag': 'object',
       'personal_car_breakdown_rac': 'object',
       'personal_car_company': 'object',
       'personal_electric_provider': 'object',
       'personal_gas_provider': 'object',
       'personal_shopping_asda': 'object',
       'personal_shopping_coop': 'object',
       'personal_shopping_morrisons': 'object',
       'personal_shopping_sainsbury': 'object',
       'personal_shopping_waitrose': 'object',
       'personal_suffers_asthma': 'object',
       'personal_suffers_bloodpressure': 'object',
       'personal_suffers_cholesterol': 'object',
       'personal_suffers_diabetes': 'object',
       'personal_suffers_eyesight': 'object',
       'personal_suffers_hairloss': 'object',
       'personal_suffers_hearingproblems': 'object',
       'personal_tv_freeview': 'object',
       'personal_tv_virgin': 'object'}
       
dfd = dd.read_csv(directory+file, encoding = "ISO-8859-1",assume_missing=True, blocksize=64000000,dtype=type,low_memory=False)
df = dfd[dfd.email.notnull()].compute() 

df.to_csv(directory+"wellatsample.csv", index=False)
print(df.shape)