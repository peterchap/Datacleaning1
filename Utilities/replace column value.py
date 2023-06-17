import pandas as pd

directory = 'C:/Users/Peter/Downloads/A-Plan September Renewal Data/'

home_Ins_1 = '00006_ORG23434_A_Plan_September_Home_insurance.csv'
home_Ins_2 = '00006_ORG23434_A_Plan_September_Home_insurance_TopUp.csv'

ins_1 = pd.read_csv(directory + home_Ins_1,encoding = "ISO-8859-1",low_memory=False)
ins_2 = pd.read_csv(directory + home_Ins_2,encoding = "ISO-8859-1",low_memory=False)

df = ins_1.append(ins_2)
df.loc[df['Brand'] == 'Welwyn Garden City', 'A-PLAN_BRANCH_LINK'] = 'https://www.aplan.co.uk/welwyn-garden-city/?int=4538&utm_source=email&utm_medium=coldemail&utm_campaign=TPLHome'
df.to_csv(directory + "home_ins_Sept19.csv", index=False)

df1 = pd.read_csv(directory + '00005A_ORG23434_A_Plan_September_Car_insurance_Branch.csv',encoding = "ISO-8859-1",low_memory=False)

df1.loc[df1['Brand'] == 'Welwyn Garden City', 'A-PLAN_BRANCH_LINK'] = 'https://www.aplan.co.uk/welwyn-garden-city/?int=4476&utm_source=email&utm_medium=coldemail&utm_campaign=TPLCar'

df1.to_csv(directory + "car_ins_Sept19.csv", index=False)