


import csv

directory = "E:/rapid7/"
file = 'domain_all_refresh3.csv'

with open(directory + file, newline='', encoding= 'latin-1') as f_input:
    csv_input = csv.reader(f_input)
    header = csv_input.__next__()
    expected = len(header)
    print(expected)
    
    for line_number, row in enumerate(csv_input, start=2):
        print(line_number, row)
        if len(row) != expected:
            print(line_number, row)