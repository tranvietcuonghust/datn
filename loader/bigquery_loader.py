from google.cloud import bigquery

def create_table_if_not_exists(dataset_id, table_id, project_id):
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id)

    # Set table_id to the ID of the table to create.
    table_id = f"{project_id}.{dataset_id}.{table_id}"

    # Define the schema of the table
    schema = [
        bigquery.SchemaField("Acreage", "FLOAT"),
        bigquery.SchemaField("Contact_address", "STRING"),
        bigquery.SchemaField("Contact_email", "STRING"),
        bigquery.SchemaField("Contact_name", "STRING"),
        bigquery.SchemaField("Contact_phone", "STRING"),
        bigquery.SchemaField("Crawled_date", "STRING"),
        bigquery.SchemaField("Created_date", "STRING"),
        bigquery.SchemaField("Description", "STRING"),
        bigquery.SchemaField("Direction", "STRING"),
        bigquery.SchemaField("Horizontal_length", "FLOAT"),
        bigquery.SchemaField("Legal", "STRING"),
        bigquery.SchemaField("Location", "STRING"),
        bigquery.SchemaField("Num_floors", "FLOAT"),
        bigquery.SchemaField("Num_rooms", "FLOAT"),
        bigquery.SchemaField("Num_toilets", "FLOAT"),
        bigquery.SchemaField("Post_id", "STRING"),
        bigquery.SchemaField("Price", "STRING"),
        bigquery.SchemaField("RE_Type", "STRING"),
        bigquery.SchemaField("Title", "STRING"),
        bigquery.SchemaField("URL", "STRING"),
        bigquery.SchemaField("Vertical_length", "FLOAT"),
        bigquery.SchemaField("Ward", "STRING"),
        bigquery.SchemaField("District", "STRING"),
        bigquery.SchemaField("City", "STRING"),
        bigquery.SchemaField("Type", "STRING"),
        bigquery.SchemaField("Source", "STRING"),
        bigquery.SchemaField("Street_size", "STRING"),
        bigquery.SchemaField("Contact_city", "STRING"),
        bigquery.SchemaField("Contact_type", "STRING"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    try:
        table = client.create_table(table)  # Make an API request.
        print(f"Created table {table_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"Table {table_id} already exists.")
        else:
            raise

# Use the function


def load_csv_to_bigquery(dataset_id, table_id, csv_file_path, project_id):
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id)

    # Set table_id to the ID of the table to create.
    table_id = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=False,
    )

    with open(csv_file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}"
    )

# Use the function

create_table_if_not_exists('bds', 'bds_3', 'bds-warehouse-424208')
load_csv_to_bigquery('bds', 'bds_3', './processed_data/merged_dataframe.csv', 'bds-warehouse-424208')