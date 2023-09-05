import zipfile


def search_string_in_file_in_zip(zip_filename, target_file, target_string):
    with zipfile.ZipFile(zip_filename, "r") as z:
        if target_file in z.namelist():
            with z.open(target_file) as f:
                for line_no, line in enumerate(f, 1):
                    # Decode each line from bytes to string
                    decoded_line = line.decode("utf-8", errors="ignore").strip()

                    # Check if the line starts with the target_string
                    if decoded_line.startswith(target_string):
                        print(
                            f"Found line starting with '{target_string}' in {target_file} at line {line_no}: {decoded_line}"
                        )


# Example usage:
directory = "C:/Users/PeterChaplin/Downloads/"
zip_filename = "dnstxt-records-full.zip"
target_file = "dnstxt-records-full.txt"
target_string = "datazag.com:"
search_string_in_file_in_zip(directory + zip_filename, target_file, target_string)
