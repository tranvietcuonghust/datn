import pandas as pd
from openai import OpenAIApi
import uuid

# Function to generate a unique ID
def generate_unique_id():
    return str(uuid.uuid4())

# Assuming you have already initialized your OpenAI API key
client = OpenAIApi(api_key="YOUR_API_KEY")

final_df = pd.read_csv('./processed_data/5_sampled_test_df.csv')
final_df['Description'] = final_df['Description'].astype(str).str.replace(r'[^\w\s,.]', '', regex=True)
final_df = final_df.fillna(0)
final_df = final_df.astype(str)

# Define DataFrame and other variables
duplicate_df = pd.DataFrame()  # DataFrame to hold duplicates

# Function to generate prompt for a batch of rows
def generate_prompt(df):
    prompt_base = """
    i have this real estate listing data in vietnamese, they are from five different sources (alomuabannhadat, cafeland, nhadat247, nhadatvui, nhadatban24h), i want to create test data for a deduplication module.
    Below is the 5 samples from 5 source. For each row create its 4 duplicated version for 4 other source, you should analyze each source sample feature to generate more accurate,
    this test data should have rows that considered duplicated when read by a person, the data in duplicated rows shouldn't be exactly like each other, should be modify some characters or replace some words, focus on these fields Title, Description, Ward, District, City; below are column names and 5 data rows
    Acreage,Contact_address,Contact_email,Contact_name,Contact_phone,Crawled_date,Created_date,Description,Direction,Horizontal_length,Legal,Location,Num_floors,Num_rooms,Num_toilets,Post_id,Price,RE_Type,Title,URL,Vertical_length,Ward,District,City,Type,Source,Street_size,Contact_city,Contact_type
    """
    prompt = prompt_base
    for i in range(0, len(df), 5):
        chunk = df.iloc[i:i+5]  # Get 5 rows
        data_rows = ['"'+ '","'.join(map(str, row)) + '"' for index, row in chunk.iterrows()]
        prompt = prompt + "\n" + ",".join(data_rows)
    prompt = prompt + "\nyour response should only give me the data of 25 rows (5 original rows and 20 rows that you generated), don't include any explanation"
    return str(prompt)

# Prepare prompts for batches of 5 rows
for i in range(0, len(final_df), 5):
    try:
        duplicate_id = generate_unique_id() 
        batch_df = final_df.iloc[i:i+5]  # Get 5 rows
        prompt = generate_prompt(batch_df)
        print(prompt)
        # Generate API response
        response = client.completions.create(
            prompt=prompt,
            max_tokens=3000,
            temperature=0.7,
            n=20,
            stop=["your response should only give me the data of 25 rows (5 original rows and 20 rows that you generated), don't include any explanation"],
            presence_penalty=0.5,
            frequency_penalty=0.5
        )
        print(response)

        # Process API response and append to duplicate_df
        if response.choices:
            for choice in response.choices:
                values = choice.text.strip().split(',')  # Split response by comma to get values
                duplicate_row = {}
                for idx, col in enumerate(final_df.columns):  # Match values to columns
                    duplicate_row[col] = values[idx]
                duplicate_row['Duplicate'] = duplicate_id  # Generate a unique ID for each duplicate
                duplicate_df = pd.concat([duplicate_df, pd.DataFrame([duplicate_row])], ignore_index=True)
    except Exception as e:
        print(f"An error occurred: {e}")

# Review and adjust DataFrame structure
print(duplicate_df.head())
duplicate_df.to_csv("./processed_data/duplicate_test_data.csv", index=False)