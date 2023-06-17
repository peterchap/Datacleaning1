#import io
import gzip

directory = "E:/rapid7/"
file = "domain_all_refresh3.csv"


with open(directory + file, 'rb') as f:
    for i, l in enumerate(f):
        pass
print("File {1} contain {0} lines".format(i + 1, file))
