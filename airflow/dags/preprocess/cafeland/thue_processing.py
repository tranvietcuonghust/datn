import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# import seaborn as sns
from vnaddress import VNAddressStandardizer
import re
import ast
import unicodedata


    



class CafelandThueProcessor:
    def __init__(self, input_path, output_path):
        self.name = "cafeland_thue_daily"
        self.input_path = input_path
        self.output_path = output_path
        self.data = None
        # self.data = pd.read_csv(input_path, encoding='utf-8')
    
    def clean_dataframe(self,df):
        """
        Cleans a DataFrame by removing specific unwanted characters and trimming whitespace.

        Args:
        df (pd.DataFrame): DataFrame to clean.

        Returns:
        pd.DataFrame: Cleaned DataFrame.
        """
        # Make a copy of the DataFrame to avoid modifying the original data
        df = df.astype(str)
        df_cleaned = df.copy()

        # List of characters to remove
        chars_to_remove = ['\n', '\t', '|','\r']

        # Iterate over all columns in the DataFrame
        for column in df_cleaned.columns:
            if df_cleaned[column].dtype == 'object':  # Only apply to object columns, which are typically strings
                for char in chars_to_remove:
                    df_cleaned[column] = df_cleaned[column].str.replace(char, '', regex=False)
                df_cleaned[column] = df_cleaned[column].str.strip()
                df_cleaned[column] = df_cleaned[column].str.replace(r' {2,}', ' ', regex=True)

        return df_cleaned

    def clean_location(self,input_string):
        # Remove the "Khu vực: ... tại " pattern
        input_string_normalized = unicodedata.normalize('NFC', input_string)
        cleaned_string = re.sub(r'Khu vực: .* tại ', '', input_string_normalized)
        
        # Replace "-" with ","
        cleaned_string = cleaned_string.replace('-', ',')
        cleaned_string = cleaned_string.replace('Chọn Phường/Xã', '')
        cleaned_string = cleaned_string.replace('Đường/Phố ', '')
        cleaned_string = cleaned_string.replace('Phường/Xã', '')
        cleaned_string = cleaned_string.replace('Quận/Huyện', '')
        cleaned_string = re.sub(r'(?i)tp.', '', cleaned_string)
        cleaned_string = re.sub(r'(?i)tp', '', cleaned_string)
        cleaned_string = re.sub(r'(?i)thành phố', '', cleaned_string)
        cleaned_string = re.sub(r'(?i)hcm', 'Hồ Chí Minh', cleaned_string)
        cleaned_string = re.sub(r'\s*,\s*', ', ', cleaned_string)  # Normalize spaces around commas
        cleaned_string = re.sub(r'^,\s*|\s*,$', '', cleaned_string)  # Remove leading and trailing commas and spaces
        # cleaned_string = re.sub(r',\s*,', ', ', cleaned_string)
        

        pattern = r"^,+\s*,+"
        cleaned_string = re.sub(pattern, ", ", cleaned_string)  # Collapse multiple consecutive commas
        cleaned_string = cleaned_string.lstrip(" ")
        cleaned_string = cleaned_string.lstrip(",")
        cleaned_string = cleaned_string.strip(" ")
        parts = cleaned_string.split(",")
        text_part = []
        for part in parts:
            if any(char.isalpha() for char in part):
                text_part.append(part)

        cleaned_string = ",".join(text_part)
        cleaned_string = cleaned_string.strip(" ")
        
        return cleaned_string


    def convert_to_vnd(self,row):
        price = row['Price']
        
        if price is None:
            return -2.0
        if price == "Thỏa thuận":
            return -1.0
        # print(row['URL'],"-",price)
        if "m²" in str(price):
            acreage = row['Acreage']
            if acreage !='':
                acreage = float(row['Acreage'])
            else:
                acreage =1.0
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

    def fill_empty_location(self,row):
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
    def find_best_match(self,row):

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

    def split_location(self,location_str):
        parts = location_str.split(', ')
        if len(parts) == 3:
            for part in parts:
                # Strip spaces and non-alphabet characters from the beginning and end
                part = re.sub(r'^[^A-Za-z]+|[^A-Za-z]+$', '', part)
                # cleaned_parts.append(cleaned_part)
            return parts
        else:
            # print(">>")
            return [None, None, None]


    def standardize_address(self,address):
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
        pattern = r'\s*Phố  [^,]+,'
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
        address = address.strip()
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
    def replace_dates(row):
        if row['Created_date'] == 'hôm nay':
            return row['Crawled_date_2']
        elif row['Created_date'] == 'hôm qua':
            return (pd.to_datetime(row['Crawled_date_2'], format='%d/%m/%Y') - pd.Timedelta(days=1)).strftime('%d/%m/%Y')
        elif row['Created_date'] == 'hôm kia':
            return (pd.to_datetime(row['Crawled_date_2'], format='%d/%m/%Y') - pd.Timedelta(days=2)).strftime('%d/%m/%Y')
        else:
            return row['Created_date']


    def preprocess(self):
        cleaned_cafeland_thue = self.clean_dataframe(self.data)
        cleaned_cafeland_thue['Acreage'] = cleaned_cafeland_thue['Acreage'].str.replace('[^\d]', '', regex=True)
        cleaned_cafeland_thue = cleaned_cafeland_thue.astype(str)
        cleaned_cafeland_thue['Num_floors'] = cleaned_cafeland_thue['Num_floors'].str.replace('[^\d]', '', regex=True)
        cleaned_cafeland_thue['Num_rooms'] = cleaned_cafeland_thue['Num_rooms'].str.replace('[^\d]', '', regex=True)
        cleaned_cafeland_thue['Num_toilets'] = cleaned_cafeland_thue['Num_toilets'].str.replace('[^\d]', '', regex=True)
        cleaned_cafeland_thue['Street_size'] = cleaned_cafeland_thue['Street_size'].str.replace('[^\d]', '', regex=True)

        cleaned_cafeland_thue['Created_date'] = cleaned_cafeland_thue['Created_date'].str.replace('Cập nhật: ', '')
        cleaned_cafeland_thue['Created_date'] = pd.to_datetime(cleaned_cafeland_thue['Created_date'], format='%d-%m-%Y')
        cleaned_cafeland_thue['Created_date'] = cleaned_cafeland_thue['Created_date'].dt.strftime('%Y-%m-%d')
        cleaned_cafeland_thue =  cleaned_cafeland_thue.sort_values(by='Created_date', ascending=False)
        # print(cleaned_cafeland_thue['Price'].unique())
        cleaned_cafeland_thue['Price'] = cleaned_cafeland_thue.apply(self.convert_to_vnd,axis=1)
        cleaned_cafeland_thue['Location'] = cleaned_cafeland_thue['Location'].apply(self.clean_location)
        # cleaned_cafeland_thue['Location'].to_csv('./processed_data/cafeland/processed_cafeland_thue.csv', index=False)
        cleaned_cafeland_thue['Standardized_Location'] = cleaned_cafeland_thue['Location'].apply(lambda address: self.standardize_address(address))
        cleaned_cafeland_thue['Standardized_Location'] = cleaned_cafeland_thue.apply(self.fill_empty_location, axis=1)
        # cleaned_cafeland_thue.to_csv('./processed_data/cafeland/processed_cafeland_thue.csv', index=False)
        cleaned_cafeland_thue['Best_Match_Location'] = cleaned_cafeland_thue.apply(self.find_best_match, axis=1)

        cleaned_cafeland_thue[['Ward', 'District', 'City']] = cleaned_cafeland_thue['Best_Match_Location'].apply(lambda x: pd.Series(self.split_location(x)))
        num_none = cleaned_cafeland_thue[['Ward', 'District', 'City']].isna().sum()

        print("Number of None values in each column:")
        print(num_none)
        cleaned_cafeland_thue.to_csv(self.output_path, index=False)
        return cleaned_cafeland_thue

