import io

directory = '/home/peter/Documents/'
filename = 'mxtable.csv'

count = 0
for line in open(directory+filename,encoding = "ISO-8859-1"): count += 1

print ('Filename: ', filename[:-4],'  Row Count:', f"{count:,}")