def read_first_lines(filename, limit):
    result = []
    with open(filename, "r") as input_file:
        # files are iterable, you can have a for-loop over a file.
        for line_number, line in enumerate(input_file):
            if line_number > limit:  # line_number starts at 0.
                break
            result.append(line)
    return result


filename = "2021-09-24-1632441867-fdns_mx.json"
directory = "C:/Users/Peter/Downloads/"

data = read_first_lines(directory + filename, 5)
print(data)
