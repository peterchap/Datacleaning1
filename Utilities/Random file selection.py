import pandas as pd
import random
import csv

directory = 'C:/Users/Peter Chaplin/Downloads/'
filename = 'mixxitmedia_20190523.csv'
p = 0.0025  # 1% of the lines
# keep the header, then take only 1% of lines
# if random from [0,1] interval is greater than 0.01 the row will be skipped
colstokeep = ['gender', 'property_tenure', 'residency_years', \
 'charity_general_donations', 'charity_animals', 'charity_birds', 'charity_blind', \
 'charity_children', 'charity_elderly', 'charity_environmental', 'charity_health', \
 'charity_homeless', 'charity_medical', 'charity_overseas', 'charity_wildlife', \
 'finance_creditband', 'finance_creditdesc', 'individual_income', 'finance_owns_creditcard',\
 'finance_owns_debitcard', 'finance_owns_storecard', 'finance_credit_card_number_of', \
 'finance_have_loan', 'finance_have_mobilecontract', 'finance_have_mobilepayg', 'finance_have_ppi',\
 'finance_have_savings', 'finance_have_will', 'finance_bill_electric', 'finance_bill_gas',\
 'finance_bill_phone', 'finance_have_overdraft', 'finance_bank_online', 'finance_creditcard_amex',\
 'finance_creditcard_visa', 'finance_pension_company', 'finance_pension_private',\
 'insurance_buildings_renewal', 'insurance_contents_renewal', 'insurance_car_renewal', \
 'insurance_havepets', 'insurance_privatemedical_renewal', 'interests_newspapertype_quality',\
 'interests_newspapertype_midmarket', 'interests_newspapertype_popular', \
 'interests_newspaper_dailymail', 'interests_newspaper_express', 'interests_newspaper_mirror', \
 'interests_newspaper_star', 'interests_newspaper_sun', 'interests_newspaper_guardian',\
 'interests_newspaper_ftimes', 'interests_newspaper_independent', 'interests_newspaper_telegraph',\
 'interests_newspaper_times', 'interests_newspaper_scotsman', 'interests_newspaper_sunday',\
 'interests_magazine_business', 'interests_magazine_car', 'interests_magazine_computer', \
 'interests_magazine_health', 'interests_magazine_home', 'interests_magazine_sport', \
 'interests_magazine_womens', 'interests_betting', 'interests_bingo', 'interests_birds',\
 'interests_cd_music', 'interests_cinema', 'interests_collect', 'interests_computing',\
 'interests_cricket', 'interests_cycling', 'interests_culture', 'interests_currentaffairs',\
 'interests_diy', 'interests_eatingout', 'interests_entertainment', 'interests_environment',\
 'interests_fashion', 'interests_fineartantiques', 'interests_fishing', 'interests_foreigntravel',\
 'interests_furthereducation', 'interests_gaming', 'interests_gardening', 'interests_golf',\
 'interests_gym', 'interests_healthfood', 'interests_knitting', 'interests_motoring',\
 'interests_motorcycling', 'interests_nature', 'interests_physicalexercise', 'interests_pets',\
 'interests_photography', 'interests_pools', 'interests_pub', 'interests_puzzles',\
 'interests_reading', 'interests_religion', 'interests_rugby', 'interests_running',\
 'interests_skiing', 'interests_stampscoins', 'interests_swimming', 'interests_theatre',\
 'interests_tvfilms', 'interests_voluntarywork', 'interests_walking', 'interests_wines',\
 'interests_wildlife', 'investor', 'investor_band', 'investor_sharevalue', 'investor_sector',\
 'mail_order_shopping_mailorder', 'mail_order_shopping_internet', 'mail_order_health_buyers',\
 'mail_order_dvd_cd_records', 'mail_order_clothing', 'mail_order_books_magazines', 'occupation',\
 'occupation_selfemployed', 'occupation_director', 'occupation_professional',\
 'occupation_professional_occupation', 'occupation_professional_qualyear',\
 'occupation_military', 'personal_maritalstatus', 'personal_numberofchildrenathome', \
 'marketing_respond_email', 'marketing_respond_mobile', 'marketing_respond_phone',\
 'personal_children_0to5', 'personal_children_6to10', 'personal_children_11to15',\
 'personal_children_16', 'personal_grandchildren', 'personal_pets_dog', 'personal_pets_cat',\
 'personal_smoker', 'insurance_havetravel', 'personal_holiday_booked_on_internet',\
 'personal_holiday_uk', 'personal_holiday_europe', 'personal_holiday_row',\
 'personal_holiday_campingcaravan', 'personal_holiday_city', 'personal_holiday_cruise',\
 'personal_holiday_sun', 'personal_holiday_wintersun', 'personal_holiday_wintersnow',\
 'personal_suffers_arthritis', 'personal_suffers_asthma', 'personal_suffers_backpain',\
 'personal_suffers_bloodpressure', 'personal_suffers_cholesterol', 'personal_suffers_diabetes',\
 'personal_suffers_eatingdisorder', 'personal_suffers_eyesight', 'personal_suffers_hairloss',\
 'personal_suffers_hearingproblems', 'personal_tv_freeview', 'personal_tv_sky', 'personal_tv_virgin',\
 'personal_car_company', 'personal_car_newused', 'personal_car_numberof', 'personal_car_makemodel',\
 'personal_car_registration', 'personal_car_breakdown_aa', 'personal_car_breakdown_greenflag',\
 'personal_car_breakdown_rac', 'personal_car_breakdown_other', 'personal_shopping_aldi',\
 'personal_shopping_asda', 'personal_shopping_coop', 'personal_shopping_iceland',\
 'personal_shopping_lidl', 'personal_shopping_morrisons', 'personal_shopping_sainsbury',\
 'personal_shopping_tesco', 'personal_shopping_waitrose', 'personal_electric_provider',\
 'personal_gas_provider', 'personal_internetaccess', 'personal_broadband_aol', 'personal_broadband_bt',\
 'personal_broadband_orange', 'personal_broadband_sky', 'personal_broadband_talktalk',\
 'personal_broadband_tiscali', 'personal_broadband_virgin',\
 'property_propertytype', 'property_newbuild', 'property_leasefree', ]
df = pd.read_csv(
         directory + filename,usecols=colstokeep,
         header=0, encoding ="ISO-8859-1",low_memory=False,
         skiprows=lambda i: i>0 and random.random() > p)
     
#df.to_csv(directory + "SSN_Test_300919.csv", index=False)

#df.drop(['urn','yob','dob'],axis=1,inplace=True)
#dict = df.apply(pd.Series.value_counts)
#print(df.describe(include='all').loc['unique', :])with open('employee_file.csv', mode='w') as employee_file:
with open(directory +'wellat_dictionary.csv', mode='w') as wellat:  
    wellat_writer = csv.writer(wellat, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    for col in df:
        a = df[col].unique().tolist()
        a.insert(0,col)
        wellat_writer.writerow(a)



#dict.to_csv(directory + "wellat_dictionary.csv")