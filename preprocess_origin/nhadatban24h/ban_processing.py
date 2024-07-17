import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from vnaddress import VNAddressStandardizer
import re
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
            df_cleaned[column] = re.sub(r' {2,}', ' ', df_cleaned[column])

    return df_cleaned

def convert_to_vnd(row):
    price = row['Price']
    
    if price is None:
        return -2.0
    if price == "Thỏa thuận":
        return -1.0
    # print(row['URL'],"-",price)
    if "m²" in str(price):
        acreage = float(row['Acreage'])
        parts = str(price).split()
        total_vnd = 0.0
        for i, part in enumerate(parts):
            if 'tỷ' in part:
                # Convert the previous part to float and multiply by 1 billion
                total_vnd += float(parts[i-1].replace(',', '.')) * 1e9
            elif 'triệu' in part:
                # Convert the previous part to float and multiply by 1 million
                total_vnd += float(parts[i-1].replace(',', '.')) * 1e6
        return total_vnd * acreage
    parts = str(price).split()
    total_vnd = 0.0
    for i, part in enumerate(parts):
        if 'tỷ' in part:
            # Convert the previous part to float and multiply by 1 billion
            total_vnd += float(parts[i-1].replace(',', '.')) * 1e9
        elif 'triệu' in part:
            # Convert the previous part to float and multiply by 1 million
            total_vnd += float(parts[i-1].replace(',', '.')) * 1e6
    return total_vnd

def fill_empty_location(row):
    location = row['Location']
    std_location = str(row['Standardized_Location'])
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
        return str(std_location)
def find_best_match(row):

    locations = ast.literal_eval(str(row['Standardized_Location']))
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


def standardize_address(address):
  # Create VNAddressStandardizer object
  address = re.sub("-", "", address)
  address = re.sub('"', "", address)
  address = re.sub('HCM', "Hồ Chí Minh", address)
  address = re.sub('Thị xã', "", address, flags=re.IGNORECASE)
  address = re.sub('xã', "", address, flags=re.IGNORECASE)
  address = re.sub('huyện', "", address, flags=re.IGNORECASE)
  address = re.sub('thị trấn', "", address, flags=re.IGNORECASE)
  address = re.sub('thành phố', "", address, flags=re.IGNORECASE)
  space_pattern = r',\s+'

# Apply str.replace with regex=True to ensure correct pattern matching
  address = re.sub(space_pattern, ', ', address)
  address = address.strip()
  pattern = r'\s*Đường [^,]+,'
  address = re.sub(pattern, "", address)
  parts = address.split(',')
    
    # Check if there are at least four parts
  if len(parts) >= 4:
        # Get the last three parts and strip any leading/trailing whitespace
        last_three_parts = [part.strip() for part in parts[-3:]]
        
        # Join the last three parts with a comma and return
        address = ', '.join(last_three_parts)
  if not re.search('[a-zA-Z]', address):
        # If no alphabetic characters are found, prepend 'Phường '
        address = 'Phường ' + address
  district_pattern = r'Phường(?!\s+\d)'
    
    # Replace the matched pattern with an empty string
  address = re.sub(district_pattern, '', address)
#   address = re.sub(district_pattern, '', address)
  print("----------------------")
  print("raw address: ",address)
  address_standardizer = VNAddressStandardizer(raw_address=address, comma_handle=True)
  

  # Standardize the address
  
  result= address_standardizer.execute_list()
  print("processed address: ",result)
  # Extract the standardized address
  # standardized_address = address_standardizer.result
  return result
nhadatban24h_ban = pd.read_csv('./crawled_data/nhadatban24h_ban.csv',encoding='utf-8')

cleaned_nhadatban24h_ban = clean_dataframe(nhadatban24h_ban)
cleaned_nhadatban24h_ban = cleaned_nhadatban24h_ban.drop(['Facade'], axis=1)
cleaned_nhadatban24h_ban['Acreage'] = cleaned_nhadatban24h_ban['Acreage'].str.replace('[^\d]', '', regex=True)
cleaned_nhadatban24h_ban = cleaned_nhadatban24h_ban.astype(str)
cleaned_nhadatban24h_ban['Horizontal_length'] = cleaned_nhadatban24h_ban['Horizontal_length'].str.replace('[^\d]', '', regex=True)
cleaned_nhadatban24h_ban['Vertical_length'] = cleaned_nhadatban24h_ban['Vertical_length'].str.replace('[^\d]', '', regex=True)
cleaned_nhadatban24h_ban['Num_floors'] = cleaned_nhadatban24h_ban['Num_floors'].str.replace('[^\d]', '', regex=True)
cleaned_nhadatban24h_ban['Num_rooms'] = cleaned_nhadatban24h_ban['Num_rooms'].str.replace('[^\d]', '', regex=True)
cleaned_nhadatban24h_ban['Num_toilets'] = cleaned_nhadatban24h_ban['Num_toilets'].str.replace('[^\d]', '', regex=True)
cleaned_nhadatban24h_ban['Street_size'] = cleaned_nhadatban24h_ban['Street_size'].str.replace('[^\d]', '', regex=True)
cleaned_nhadatban24h_ban['Created_date'] = cleaned_nhadatban24h_ban['Created_date'].str.replace('/', '-')
cleaned_nhadatban24h_ban['Created_date'] = pd.to_datetime(cleaned_nhadatban24h_ban['Created_date'], format='%d-%m-%Y')
cleaned_nhadatban24h_ban['Created_date'] = cleaned_nhadatban24h_ban['Created_date'].dt.strftime('%Y-%m-%d')
cleaned_nhadatban24h_ban= cleaned_nhadatban24h_ban.sort_values(by='Created_date', ascending=False)
# print(cleaned_nhadatban24h_ban['Price'].unique())
cleaned_nhadatban24h_ban['Price'] = cleaned_nhadatban24h_ban.apply(convert_to_vnd,axis=1)

cleaned_nhadatban24h_ban['Standardized_Location'] = cleaned_nhadatban24h_ban['Location'].apply(lambda address: standardize_address(address))
cleaned_nhadatban24h_ban['Standardized_Location'] = cleaned_nhadatban24h_ban.apply(fill_empty_location, axis=1)
cleaned_nhadatban24h_ban.to_csv('./processed_data/nhadatban24h/processed_nhadatban24h_ban.csv', index=False)
cleaned_nhadatban24h_ban['Best_Match_Location'] = cleaned_nhadatban24h_ban.apply(find_best_match, axis=1)

cleaned_nhadatban24h_ban[['Ward', 'District', 'City']] = cleaned_nhadatban24h_ban['Best_Match_Location'].apply(lambda x: pd.Series(split_location(x)))
num_none = cleaned_nhadatban24h_ban[['Ward', 'District', 'City']].isna().sum()

print("Number of None values in each column:")
print(num_none)
cleaned_nhadatban24h_ban.to_csv('./processed_data/nhadatban24h/processed_nhadatban24h_ban.csv', index=False)
