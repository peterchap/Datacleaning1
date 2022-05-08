import pandas as pd
import glob, os

directory = "C:/Users/Peter/downloads/import050419/"
filterdir = "C:/Users/Peter/downloads/filter data clean/"
os.chdir(directory)
for file in glob.glob("*.csv"):
    df = pd.read_csv(file,usecols=range(0,21),error_bad_lines=False)
    
    print(df.shape)
    print(list(df.columns.values))

    to_drop = ['FOREIGN', 'UNKNOWN', 'SPAM', 'TEMP', 'NO MX', 'BAD', 'BLACKLISTED', 'SPAM TRAP', 'EXCLUDED','EXPIRED','INVALID']
    df = df[~df['DOMAIN_STATUS'].isin(to_drop)]
    print(df.shape)

    to_dropb = [1]

    df = df[~df['BANNED_WORD'].isin(to_dropb)]
    print(df.shape)

    to_dropemail = ['0']
    df = df[~df['EMAIL_OK'].isin(to_dropemail)]
    print(df.shape)

    new = df["EMAIL"].str.split(pat="@", expand=True)
    df["LEFT"]= new[0]

    patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|enquir|fake|feedback|finance|free|garbage|generic|\
    info|junk|loan|office|market|penis|phruit|postmaster|recep|register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster"

    df = df[~df["LEFT"].str.contains(patternDel, na=False)]
    to_dropcol = ['EMAIL_OK','BANNED_WORD',' DOMAIN','DOMAIN_STATUS','LEFT']
    df.drop(to_dropcol, axis=1, inplace=True)
    print(df.shape)
    
    filename = filterdir+"filter_"+file
    print(filename)

    df.to_csv(filename, index=False)
   
    print(list(df.columns.values))

    print("Completed Successfully")
    
print("All Done")