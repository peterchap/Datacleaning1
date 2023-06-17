import pandas as pd
import glob
import csv

with open('C:/Users/Peter Chaplin/Downloads/file_listing.csv', mode='w', newline='') as filelisting:
    file_writer = csv.writer(filelisting)
    a =glob.glob('C:/Users/Peter Chaplin/Downloads/A-Plan August oempro Data/*.csv')

    for l in a:
        file_writer.writerow([l])

    b = glob.glob('C:/Users/Peter/Downloads/A-Plan August Final Data/*.csv')
    for l1 in b:
        file_writer.writerow([l1])

