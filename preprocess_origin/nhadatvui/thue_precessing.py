import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from vnaddress import VNAddressStandardizer
import re
import swifter
# from pandarallel import pandarallel
import ast
def clean_dataframe(df):
    """
    Cleans a DataFrame by removing specific unwanted characters and trimming whitespace.

    Args:
    df (pd.DataFrame): DataFrame to clean.

    Returns:
    pd.DataFrame: Cleaned DataFrame.
    """
    # Make a copy of the DataFrame to avoid modifying the original data
    df_cleaned = df.copy()

    # List of characters to remove
    chars_to_remove = ['\n', '\t', '|','\r']

    # Iterate over all columns in the DataFrame
    for column in df_cleaned.columns:
        if df_cleaned[column].dtype == 'object':  # Only apply to object columns, which are typically strings
            for char in chars_to_remove:
                df_cleaned[column] = df_cleaned[column].str.replace(char, '', regex=False)
            df_cleaned[column] = df_cleaned[column].str.strip()

    return df_cleaned

def convert_to_vnd(s):
    parts = s.split()
    total_vnd = 0.0
    for i, part in enumerate(parts):
        if 'tỷ' in part:
            # Convert the previous part to float and multiply by 1 billion
            total_vnd += float(parts[i-1].replace(',', '.')) * 1e9
        elif 'triệu' in part:
            # Convert the previous part to float and multiply by 1 million
            total_vnd += float(parts[i-1].replace(',', '.')) * 1e6
    return total_vnd

def standardize_address(address):
  # Create VNAddressStandardizer object
  address = re.sub("-", "", address)
  address = address.strip()
  pattern = r'\s*Đường [^,]+,'
  address = re.sub(pattern, "", address)
  if not re.search('[a-zA-Z]', address):
        # If no alphabetic characters are found, prepend 'Phường '
        address = 'Phường ' + address
  print("----------------------")
  print("raw address: ",address)
  address_standardizer = VNAddressStandardizer(raw_address=address, comma_handle=True)
  result= address_standardizer.execute_list()
  print("processed address: ",result)
  # Extract the standardized address
  # standardized_address = address_standardizer.result
  return result
def fill_empty_location(row):
    location = row['Location']
    std_location = row['Standardized_Location']
    if std_location == "[]":
        std_location = []
        location_data = {
                    "match_address" : location,
                    "match_percent" : -1
                }
        std_location.append(location_data)
        print(std_location)
        return str(std_location)
    else: 
        return std_location
def find_best_match(row):

    locations = ast.literal_eval(row['Standardized_Location'])
    description = str(row['Description'])
    potential_matches = []
    # print(locations)
    # Extract city, district, and ward from description for matching
    for location in locations:
        print(location)
        match_address = location['match_address']
        match_percent = location['match_percent']
        if match_percent == -1:
            potential_matches.append(match_address)
        else:
            parts = match_address.split(', ')
            ward, district, city = parts[0], parts[1], parts[2]

            # Check for city match first
            if city in description:
                if district in description:
                    if ward in description:
                        potential_matches.append(match_address)  # Best case: all match
                    else:
                        potential_matches.append(match_address)  # Match district and city
                else:
                    potential_matches.append(match_address)  # Match city only
    # Return the most detailed matches
    if potential_matches:
        # print(potential_matches)
        return potential_matches[0]  # Return the first most detailed match, can be adjusted
    else:
        return locations[0]['match_address']

def split_location(location_str):
    parts = location_str.split(', ')
    if len(parts) == 3:
        return parts
    else:
        # print(">>")
        return [None, None, None]

  # Standardize the address
  
  
# pandarallel.initialize()
nhadatvui_ban = pd.read_csv('./crawled_data/nhadatvui_thue.csv')
cleaned_nhadatvui_ban = clean_dataframe(nhadatvui_ban)
cleaned_nhadatvui_ban['Acreage'] = cleaned_nhadatvui_ban['Acreage'].str.replace(' m²', '')
cleaned_nhadatvui_ban['Created_date'] = cleaned_nhadatvui_ban['Created_date'].str.replace('/', '-')
cleaned_nhadatvui_ban['Created_date'] = pd.to_datetime(cleaned_nhadatvui_ban['Created_date'], format='%d-%m-%Y')
cleaned_nhadatvui_ban['Created_date'] = cleaned_nhadatvui_ban['Created_date'].dt.strftime('%Y-%m-%d')
cleaned_nhadatvui_ban= cleaned_nhadatvui_ban.sort_values(by='Created_date', ascending=False)
cleaned_nhadatvui_ban['Price'] = cleaned_nhadatvui_ban['Price'].apply(convert_to_vnd)
cleaned_nhadatvui_ban['Standardized_Location'] = cleaned_nhadatvui_ban['Location'].parallel_apply(lambda address: standardize_address(address))
cleaned_nhadatvui_ban['Standardized_Location'] = cleaned_nhadatvui_ban.apply(fill_empty_location, axis=1)
cleaned_nhadatvui_ban['Best_Match_Location'] = cleaned_nhadatvui_ban.apply(find_best_match, axis=1)

cleaned_nhadatvui_ban[['Ward', 'District', 'City']] = cleaned_nhadatvui_ban['Best_Match_Location'].apply(lambda x: pd.Series(split_location(x)))
num_none = cleaned_nhadatvui_ban[['Ward', 'District', 'City']].isna().sum()

cleaned_nhadatvui_ban.to_csv('./processed_data/nhadatvui/processed_nhadatvui_thue.csv', index=False)
