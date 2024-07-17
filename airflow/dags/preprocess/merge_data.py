import pandas as pd
from preprocess.mappings import re_type_mapping

class RealEstateDataMerger:
    def __init__(self, input_files, output_file):
        self.input_files = input_files
        self.output_file = output_file
        self.dataframes = []

    def load_and_process_data(self, file_path, type_value, source_value, contact_replace=None, direction_replace=None, re_type_replace=None, legal_replace=None):
        df = pd.read_csv(file_path)
        df['Type'] = type_value
        df['Source'] = source_value

        if contact_replace:
            df['Contact_phone'] = df['Contact_phone'].str.extract(contact_replace)

        if direction_replace:
            df['Direction'] = df['Direction'].astype(str).str.replace(direction_replace, "", regex=False)
            df['Direction'] = df['Direction'].replace("Không xác định", pd.NA)

        if re_type_replace:
            df['RE_Type'] = df['RE_Type'].astype(str).str.replace(re_type_replace, "", regex=False)
        
        if legal_replace:
            df['Legal'] = df['Legal'].replace("Không xác định", pd.NA)
        
        return df

    def process(self):
        for file_info in self.input_files:
            df = self.load_and_process_data(*file_info)
            self.dataframes.append(df)

        merged_df = pd.concat(self.dataframes, ignore_index=True, sort=False)

        merged_df['Acreage'] = merged_df['Acreage'].astype(str).str.replace(",", ".").astype(str).str.replace(".", "").astype(float)
        merged_df['Price'] = merged_df['Price'].astype(float)
        merged_df['Num_floors'] = merged_df['Num_floors'].astype(str).str.split('-').str[1]
        merged_df['Num_floors'] = merged_df['Num_floors'].astype(str).str.extract('(\d+)', expand=False)
        merged_df['Num_floors'] = merged_df['Num_floors'].apply(lambda x: None if x=='' else x)
        merged_df['Num_floors'] = merged_df['Num_floors'].astype(float)

        merged_df['Num_rooms'] = merged_df['Num_rooms'].astype(float)
        merged_df['Num_toilets'] = merged_df['Num_toilets'].astype(float)
        merged_df['Horizontal_length'] = merged_df['Horizontal_length'].astype(str).str.split('-').str[1]
        merged_df['Horizontal_length'] = merged_df['Horizontal_length'].astype(str).str.extract('(\d+)', expand=False)
        merged_df['Horizontal_length'] = merged_df['Horizontal_length'].apply(lambda x: None if x=='' else x)
        merged_df['Horizontal_length'] = merged_df['Horizontal_length'].astype(float)


        merged_df['Vertical_length'] = merged_df['Vertical_length'].astype(str).str.split('-').str[1]
        merged_df['Vertical_length'] = merged_df['Vertical_length'].astype(str).str.extract('(\d+)', expand=False)
        merged_df['Vertical_length'] = merged_df['Vertical_length'].apply(lambda x: None if x=='' else x)
        merged_df['Vertical_length'] = merged_df['Vertical_length'].astype(float)


        merged_df['RE_Type'] = merged_df['RE_Type'].map(re_type_mapping).fillna(df['RE_Type'])

        merged_df.to_csv(self.output_file, index=False)

# Example usage
# input_files = [
#     ("./processed_alomuabannhadat_ban.csv", "ban", "alomuabannhadat", r'(\d+)', "Hướng xây dựng: ", "Loại địa ốc: "),
#     ("./processed_alomuabannhadat_dat.csv", "ban", "alomuabannhadat", r'(\d+)', None, "Loại địa ốc: "),
#     ("./processed_alomuabannhadat_thue.csv", "thue", "alomuabannhadat", r'(\d+)', "Hướng xây dựng: ", "Loại địa ốc: "),
#     ("./processed_cafeland_ban.csv", "ban", "cafeland", r'(\d+)', None, None),
#     ("./processed_cafeland_thue.csv", "thue", "cafeland", r'(\d+)', None, None),
#     ("./processed_nhadat247_ban.csv", "ban", "nhadat247", None, None, None),
#     ("./processed_nhadat247_thue.csv", "thue", "nhadat247", None, None, None),
#     ("./processed_nhadatban24h_ban.csv", "ban", "nhadatban24h", None, None, None),
#     ("./processed_nhadatban24h_thue.csv", "thue", "nhadatban24h", None, None, None),
#     ("./processed_nhadatvui_ban.csv", "ban", "nhadatvui", None, None, None),
#     ("./processed_nhadatvui_thue.csv", "thue", "nhadatvui", None, None, None)
# ]
# output_file = "./merged_dataframe.csv"

# processor = RealEstateDataMerger(input_files, output_file)
# processor.process()
