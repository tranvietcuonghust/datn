from vnaddress.constants.standard_addresses import STANDARD_ADDRESSES
from google.oauth2 import service_account
from google.cloud import bigquery

# Prepare data
cities = set()
districts = set()
wards = []

for address in STANDARD_ADDRESSES:
    ward, district, city = [x.strip() for x in address.split(',')]
    cities.add(city)
    districts.add((district, city))
    wards.append((ward, district))

cities = list(cities)
districts = list(districts)
wards = list(wards)

key_path = "./bds-warehouse-424208-fca17cf1fff7.json"

# Load credentials from the service account key file
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials, project='bds-warehouse-424208')

# Define table schema
city_table = 'bds-warehouse-424208.bds.city'
district_table = 'bds-warehouse-424208.bds.district'
ward_table = 'bds-warehouse-424208.bds.ward'

# Insert data into City table
city_rows = [{'City': city} for city in cities]
errors = client.insert_rows_json(city_table, city_rows)
if errors:
    print(f"Errors occurred while inserting rows into City: {errors}")

# Insert data into District table
district_rows = [{'District': district, 'City': city} for district, city in districts]
errors = client.insert_rows_json(district_table, district_rows)
if errors:
    print(f"Errors occurred while inserting rows into District: {errors}")

# Insert data into Ward table
ward_rows = [{'Ward': ward, 'District': district} for ward, district in wards]
errors = client.insert_rows_json(ward_table, ward_rows)
if errors:
    print(f"Errors occurred while inserting rows into Ward: {errors}")
