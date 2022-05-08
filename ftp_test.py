import pandas as pd
from ftplib import FTP
from io import StringIO
import io


directory = 'E:/inboxed/'


filename = 'GYFP_UKWeekly20191209002512.csv'

email= ['EmailAddress']

df = pd.read_csv(directory + filename,encoding ='ISO-8859-1',usecols= email)
ftp = FTP('ftp.emailswitchboard.co.uk')
ftp.login('ems_data', 'em@1lsw1tchb0@rd')

ftp.cwd('/import/raw')
buffer = StringIO()
df.to_csv(buffer)
text = buffer.getvalue()
bio = io.BytesIO(str.encode(text))
ftp.storbinary('STOR ' + filename, bio)