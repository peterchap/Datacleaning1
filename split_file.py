import pandas as pd
import ast
directory = "C:/Users/PeterChaplin/Downloads/"
file = "matched_domains.csv"
# Load the CSV file
df = pd.read_csv(directory + file)

# Convert the string representation of the list with a dictionary to an actual Python object
def process_data(data):
    try:
        # Use ast.literal_eval to safely evaluate the string into a Python object
        return ast.literal_eval(data)
    except (ValueError, SyntaxError) as e:
        print(f"Error parsing data: {data}\nError: {e}")
        return {}

def clean_and_extract(data):
    try:
        # Safely evaluate the row
        parsed_data = ast.literal_eval(data)

        # Check if it's a list with at least one dictionary
        if isinstance(parsed_data, list) and len(parsed_data) > 0 and isinstance(parsed_data[0], dict):
            return parsed_data[0]  # Return the first dictionary
        else:
            print(f"Unexpected format: {data}")
            return {}
    except (ValueError, SyntaxError) as e:
        print(f"Error parsing data: {data}\nError: {e}")
        return {}

# Apply the function to the 'DATA' column
df["DATA"] = df["DATA"].apply(clean_and_extract)

df["DATA"] = df["DATA"].apply(process_data)


# Normalize the dictionary into separate columns
df_expanded = df.join(pd.json_normalize(df["DATA"]))

# Drop the original `DATA` column if needed
df_expanded = df_expanded.drop(columns=["DATA"])

print(df_expanded.head())
print(df_expanded.columns)
