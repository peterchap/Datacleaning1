# import io
import zipfile

# directory = "C:/Users/PeterChaplin/Downloads/"
directory = "C:/Users/PeterChaplin/Downloads/"
file = "Apollo_V7_V5_per_all_fields.csv"
# zip_folder = "domains-detailed.zip"
# file = "domains-detailed.csv"
"""

with zipfile.ZipFile(directory + zip_folder) as zf:
    with zf.open(file) as f:
        for i, l in enumerate(f):
            pass
print("File {1} contain {0} lines".format(i + 1, file))

"""
with open(directory + file, "rb") as f:
    for i, l in enumerate(f):
        pass
print("File {1} contain {0} lines".format(i + 1, file))
