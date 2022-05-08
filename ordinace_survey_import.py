import os
import glob
import pandas as pd
os.chdir("E:/OS Open Name/DATA/")

directory = 'E:/OS Open Name/'
extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

columns = ['id','names_uri','name1','name1_lang','name2','name2_lang','type','local_type','geometry_x','geometry_y',\
    'most_detail_view_res','least_detail_view_res','mbr_xmin','mbr_ymin','mbr_xmax','mbr_ymax','postcode_district',\
    'postcode_district_uri','populated_place','populated_place_uri','populated_place_type','district_borough',\
    'district_borough_uri','district_borough_type','county_unitary','county_unitary_uri','county_unitary_type','region',\
    'region_uri','country','country_uri','related_spatial_object','same_as_dbpedia','same_as_geonames']

dropcols = ['id','names_uri','name1_lang','name2','name2_lang','type','geometry_x','geometry_y',\
    'most_detail_view_res','least_detail_view_res','mbr_xmin','mbr_ymin','mbr_xmax','mbr_ymax',\
    'postcode_district_uri','populated_place_uri', 'district_borough_uri','district_borough_type','county_unitary_uri',\
    'county_unitary_type','region_uri','country_uri','related_spatial_object','same_as_dbpedia','same_as_geonames']

streets = ['Named Road','Numbered Road','Section Of Named Road', 'Section Of Numbered Road']

combined = pd.DataFrame()

#combine all files in the list
for f in all_filenames:
    x = pd.read_csv(f, encoding = "ISO-8859-1",names=columns, low_memory=False)
    print(f)
    x.drop(columns=dropcols, inplace=True)
    y = x[x['local_type'].isin(streets)]
    y['pos'] = y['populated_place_type'].astype(str).str.find('#')
    y['place_type'] = y['populated_place_type'].astype(str).str[y['pos']+1:]
    #y.drop(columns=['populated_place_type','pos'], inplace=True)
    combined = pd.concat([combined,y])

#export to csv
combined.to_csv( directory + "combinedOS070720.csv", index=False)
print(combined.shape)

print("Completed Successfully")