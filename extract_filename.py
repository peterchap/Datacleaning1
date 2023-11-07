import os
import datetime

directory = "E:/domains-monitor/updates/"


for filename in os.listdir(directory):
    x = filename.split(".")[0]
    b = x[19:29]
    date = datetime.datetime.strptime(b, date_format)
    print(date)
