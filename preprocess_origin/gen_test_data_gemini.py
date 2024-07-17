import pandas as pd
import google.generativeai as genai
import uuid
# from genai import GenerativeModel
GOOGLE_API_KEY="AIzaSyACyu29QVWPwCb34h3Oy2VDfM1dEJZSsHw"

genai.configure(api_key=GOOGLE_API_KEY)

# Function to generate a unique ID
def generate_unique_id():
    return str(uuid.uuid4())

final_df = pd.read_csv('./processed_data/sampled_test_df.csv')
final_df['Description'] = final_df['Description'].astype(str).str.replace(r'[^\w\s,.]', '', regex=True)
final_df = final_df.fillna(0)
final_df = final_df.astype(str)

# Define DataFrame and other variables
duplicate_df = pd.DataFrame()

# Function to generate prompt for a batch of rows
def generate_prompt(df):
    prompt_base = """ i have this real estate listing data in vietnamese, they are from five different sources (alomuabannhadat, cafeland, nhadat247, nhadatvui, nhadatban24h), I want to create test data for a deduplication module. Below are the 5 samples from 5 source. For each row create its 4 duplicated versions for 4 other sources, You should analyze each source sample feature to generate more accurate test data. This data should have rows that are considered duplicated when read by a person, The data in duplicated rows shouldn't be exactly like each other, and should modify some characters or replace some words, focusing on these fields: Title, Description, Ward, District, City; Below are column names and 5 data rows

Acreage,Contact_address,Contact_email,Contact_name,Contact_phone,Crawled_date,Created_date,Description,Direction,Horizontal_length,Legal,Location,Num_floors,Num_rooms,Num_toilets,Post_id,Price,RE_Type,Title,URL,Vertical_length,Ward,District,City,Type,Source,Street_size,Contact_city,Contact_type
"""
    prompt = prompt_base
    for i in range(0, len(df), 5):
        chunk = df.iloc[i:i+5]  # Get 5 rows
        data_rows = ['"'+ '","'.join(map(str, row)) + '"' for index, row in chunk.iterrows()]
        prompt = prompt + "\n" + ",".join(data_rows)
    prompt = prompt + "\nYour response should only give me the data of 25 rows (5 original rows and 20 rows that you generated), Don't include any explanation"
    return str(prompt)

# Load Gemini model
generation_config = {
  "temperature": 0.4,
  "top_p": 0.95,
  "top_k": 64,
  "max_output_tokens": 20000,
}
model = genai.GenerativeModel(model_name="gemini-1.5-flash-latest",
                              generation_config=generation_config)


# Prepare prompts for batches of 5 rows
for i in range(0, len(final_df), 5):
    try:
        duplicate_id = generate_unique_id()
        batch_df = final_df.iloc[i:i+5]  # Get 5 rows
        prompt = generate_prompt(batch_df)
        print(prompt)

        # Generate response using Gemini
        response = model.generate_content(prompt)
        lines = response.text.strip().split("\n")
        for line in lines:
        # Process response and append to duplicate_df
            if line:
                print(line)
                values = line.split('",')
                duplicate_row = {}
                for idx, col in enumerate(final_df.columns):
                    duplicate_row[col] = values[idx]
                duplicate_row['Duplicate'] = duplicate_id
                duplicate_df = pd.concat([duplicate_df, pd.DataFrame([duplicate_row])], ignore_index=True)
    except Exception as e:
        print(f"An error occurred: {e}")

# Review and adjust DataFrame structure
print(duplicate_df.head())
duplicate_df.to_csv("./processed_data/duplicate_test_data.csv", index=False)
