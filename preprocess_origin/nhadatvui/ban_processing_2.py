import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from vnaddress import VNAddressStandardizer
import re
import ast

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


cleaned_nhadatvui_ban = pd.read_csv('./processed_data/nhadatvui/processed_nhadatvui_thue.csv')
cleaned_nhadatvui_ban['Standardized_Location'] = cleaned_nhadatvui_ban.apply(fill_empty_location, axis=1)
cleaned_nhadatvui_ban['Best_Match_Location'] = cleaned_nhadatvui_ban.apply(find_best_match, axis=1)

cleaned_nhadatvui_ban[['Ward', 'District', 'City']] = cleaned_nhadatvui_ban['Best_Match_Location'].apply(lambda x: pd.Series(split_location(x)))
num_none = cleaned_nhadatvui_ban[['Ward', 'District', 'City']].isna().sum()

print("Number of None values in each column:")
print(num_none)
cleaned_nhadatvui_ban.to_csv('./processed_data/nhadatvui/processed_nhadatvui_thue_p2.csv', index=False)
