directory = "E:/rapid7/"
file = "mx050223.json"

k = '}'
v = '},'
outfile = open(directory + "mx050223replace.json", "w", encoding='utf-8')
with open(directory + file, "r", encoding='utf-8') as f:
    a = f.readlines()
    for string in a:
        if string.startswith(k):
            string = string.replace(k, v)
        outfile.write(string)
outfile.close()
