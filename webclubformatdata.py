import pandas as pd

directory = 'E:/Cleaning-todo/'

filename = 'ESB_Webclubs_standard_export_2019-12-12.csv'

colstouse = ['Create',  'Email', 'Title', 'Fname', 'Lname', 'DOB', \
    'Ad1', 'Ad2', 'Ad3', 'Town', 'County', 'OutPC', 'InPC', 'Gender',\
    'ConsumerClub', 'GardenersClub', 'HomeOwnersClub', 'MotoristsClub',\
    'NetOffers', 'QuizClub', 'TravellersClub', 'VinoClub', 'IPAddress',\
    'Tel', 'Tel_Mobile', 'LastOpened', 'LastClicked']

df = pd.read_csv(directory+filename, low_memory=False, error_bad_lines=False, usecols=colstouse)
print(list(df.columns.values))
print(df.shape)

df = df.replace(',',' ', regex=True)


df1 =df[df['Create'] > '2018-11-30']
df2 =df[df['LastOpened'] > '2018-11-30']
df3 =df[df['LastClicked'] > '2018-11-30']
print(df1.shape)
print(df2.shape)
print(df3.shape)
print(df1.head(5))

df4 = pd.concat([df1,df2,df3])
df4.drop_duplicates(subset='Email', keep='first',inplace=True)
print(df4.shape)

df4.to_csv(directory + filename[:-4] + "_12mthselection.csv", index=False)
#print("Completed Successfully")