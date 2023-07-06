#import io
import gzip

directory = "E:/rapid7/"
file = "rapid7_refresh5.csv"


with open(directory + file, 'rb') as f:
    for i, l in enumerate(f):
        pass
print("File {1} contain {0} lines".format(i + 1, file))
