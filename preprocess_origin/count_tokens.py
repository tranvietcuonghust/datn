import csv
import re

def count_tokens_in_csv(file_path):
    token_count = 0

    # Regular expression to match words
    token_pattern = re.compile(r'\b\w+\b')

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        
        for row in csvreader:
            for cell in row:
                # Find all tokens in the cell
                tokens = token_pattern.findall(cell)
                # Update the total token count
                token_count += len(tokens)
    
    return token_count

# Example usage
file_path = './processed_data/merged_dataframe.csv'
total_tokens = count_tokens_in_csv(file_path)
print(f"Total number of tokens in the CSV file: {total_tokens}")
