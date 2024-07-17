from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from google.oauth2 import service_account
from google.cloud import bigquery
# from crawler_daily.spiders.coingecko_spider import CrawlerSpider
from crawler.spiders.alomuabannhadat_ban import CrawlerSpider as alomuabannhadat_ban_spider
from crawler.spiders.alomuabannhadat_thue import CrawlerSpider as alomuabannhadat_thue_spider
from crawler.spiders.alomuabannhadat_dat import CrawlerSpider as alomuabannhadat_dat_spider
from crawler.spiders.cafeland_ban import CrawlerSpider as cafeland_ban_spider
from crawler.spiders.cafeland_thue import CrawlerSpider as cafeland_thue_spider
from crawler.spiders.nhadat247_ban import CrawlerSpider as nhadat247_ban_spider
from crawler.spiders.nhadat247_thue import CrawlerSpider as nhadat247_thue_spider
from crawler.spiders.nhadatban24h_ban import CrawlerSpider as nhadatban24h_ban_spider
from crawler.spiders.nhadatban24h_thue import CrawlerSpider as nhadatban24h_thue_spider
from crawler.spiders.nhadatvui_ban import CrawlerSpider as nhadatvui_ban_spider
from crawler.spiders.nhadatvui_thue import CrawlerSpider as nhadatvui_thue_spider

from preprocess.alomuabannhadat.ban_processing import AlomuabannhadatBanProcessor
from preprocess.alomuabannhadat.thue_processing import AlomuabannhadatThueProcessor
from preprocess.alomuabannhadat.dat_processing import AlomuabannhadatDatProcessor
from preprocess.cafeland.ban_processing import CafelandBanProcessor
from preprocess.cafeland.thue_processing import CafelandThueProcessor
from preprocess.nhadat247.ban_processing import Nhadat247BanProcessor
from preprocess.nhadat247.thue_processing import Nhadat247ThueProcessor
from preprocess.nhadatban24h.ban_processing import Nhadatban24hBanProcessor
from preprocess.nhadatban24h.thue_processing import Nhadatban24hThueProcessor
from preprocess.nhadatvui.ban_processing import NhadatvuiBanProcessor
from preprocess.nhadatvui.thue_processing import NhadatvuiThueProcessor

from preprocess.merge_data import RealEstateDataMerger 
from preprocess.deduplicate import RemoveDuplicate

from datetime import datetime, timedelta
from snowflake.connector.pandas_tools import write_pandas
from airflow.models import DAG, Variable
import sys
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
sys.path.append('/opt/airflow/dags/crawler')
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import pandas as pd
import os
ban_spiders = [alomuabannhadat_ban_spider,
        #    cafeland_ban_spider,
        #    nhadat247_ban_spider,
        #    nhadatban24h_ban_spider,
           nhadatvui_ban_spider
           
           ]
thue_spiders = [
    # alomuabannhadat_thue_spider, 
    # alomuabannhadat_dat_spider,
    # cafeland_thue_spider,
    # nhadat247_thue_spider,
    # nhadatban24h_thue_spider,
    nhadatvui_thue_spider
]

alomuabannhadat_ban_processor = AlomuabannhadatBanProcessor("","")
alomuabannhadat_thue_processor = AlomuabannhadatThueProcessor("","")
alomuabannhadat_dat_processor = AlomuabannhadatDatProcessor("","")
cafeland_ban_processor = CafelandBanProcessor("","")
cafeland_thue_processor = CafelandThueProcessor("","")
nhadat247_ban_processor = Nhadat247BanProcessor("","")
nhadat247_thue_processor = Nhadat247ThueProcessor("","")
nhadatban24h_ban_processor = Nhadatban24hBanProcessor("","")
nhadatban24h_thue_processor = Nhadatban24hThueProcessor("","")
nhadatvui_ban_processor = NhadatvuiBanProcessor("","")
nhadatvui_thue_processor = NhadatvuiThueProcessor("","")
                                                  



processors = [
    alomuabannhadat_ban_processor,
    alomuabannhadat_thue_processor,
    alomuabannhadat_dat_processor,
    cafeland_ban_processor,
    cafeland_thue_processor,
    nhadat247_ban_processor,
    # nhadat247_thue_processor,
    nhadatban24h_ban_processor,
    # nhadatban24h_thue_processor,
    nhadatvui_ban_processor,
    nhadatvui_thue_processor
]


# s = {"FEED_FORMAT": "csv", "LOG_LEVEL": "INFO", "FEED_URI": f'/opt/airflow/data/crawled/crawled_{date}/{spider.name}_{date}.csv'}
    # s['FEED_FORMAT'] = 'csv'
    # s['LOG_LEVEL'] = 'INFO'
    # s['FEED_URI'] = f'/opt/airflow/data/crawled/crawled_{date}/{spider.name}_{date}.csv'
    # s['LOG_FILE'] = f'/opt/airflow/logs/crawler_logs/coingecko_{date}.log'

#This variable for connecting to Snowflake datawarehouse


from google.cloud import bigquery

def create_table_if_not_exists(dataset_id, table_id, project_id):
    key_path = "/opt/airflow/data/bds-warehouse-424208-fca17cf1fff7.json"

# Load credentials from the service account key file
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials,project=project_id)

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


def load_csv_to_bigquery(dataset_id, table_id,temp_table_id, csv_file_path, project_id):
    # Construct a BigQuery client object.
    key_path = "/opt/airflow/data/bds-warehouse-424208-fca17cf1fff7.json"
    df = pd.read_csv(csv_file_path)
    df['Horizontal_length'] = df['Horizontal_length'].astype(float)

# Load credentials from the service account key file
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials,project=project_id)


    # Set table_id to the ID of the table to create.
    table_id = f"{project_id}.{dataset_id}.{table_id}"
    temp_table_id = f"{project_id}.{dataset_id}.{temp_table_id}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=False,
    )

    

    with open(csv_file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, temp_table_id, job_config=job_config)
    # job = client.load_table_from_dataframe(df, f"{dataset_id}.{temp_table_id}", job_config=job_config)
    try:
        job.result()  # Wait for the job to complete.
    except Exception as e:
        print("Job failed.")
        for error in e.errors:
            print(error) # Wait for the job to complete.

    # table = client.get_table(table_id)  # Make an API request.
    merge_query = f"""
    MERGE {table_id} T
USING {temp_table_id} S
ON T.URL = S.URL AND T.Post_id = S.Post_id
WHEN MATCHED THEN
  UPDATE SET 
    T.Acreage = S.Acreage,
    T.Contact_address = S.Contact_address,
    T.Contact_email = S.Contact_email,
    T.Contact_name = S.Contact_name,
    T.Contact_phone = S.Contact_phone,
    T.Crawled_date = S.Crawled_date,
    T.Created_date = S.Created_date,
    T.Description = S.Description,
    T.Direction = S.Direction,
    T.Horizontal_length = S.Horizontal_length,
    T.Legal = S.Legal,
    T.Location = S.Location,
    T.Num_floors = S.Num_floors,
    T.Num_rooms = S.Num_rooms,
    T.Num_toilets = S.Num_toilets,
    T.Price = S.Price,
    T.RE_Type = S.RE_Type,
    T.Title = S.Title,
    T.URL = S.URL,
    T.Vertical_length = S.Vertical_length,
    T.Ward = S.Ward,
    T.District = S.District,
    T.City = S.City,
    T.Type = S.Type,
    T.Source = S.Source,
    T.Street_size = S.Street_size,
    T.Contact_city = S.Contact_city,
    T.Contact_type = S.Contact_type
    WHEN NOT MATCHED THEN
    INSERT ROW
    """
    query_job = client.query(merge_query)
    query_job.result()  # Wait for the job to complete.

    # Delete the temporary table
    client.delete_table(temp_table_id, not_found_ok=True)

    print(f"Merged data from {csv_file_path} into {table_id}")

# Use the function

def load_data_to_bigquery(date):
    create_table_if_not_exists('bds', 'bds', 'bds-warehouse-424208')
    create_table_if_not_exists('bds', 'temp_bds', 'bds-warehouse-424208')
    load_csv_to_bigquery('bds', 'bds','temp_bds', f"/opt/airflow/data/deduplicated/deduplicated_{date}/deduplicated_dataframe_{date}.csv", 'bds-warehouse-424208')



def run_scrapy(date,spider):
    # from scrapy.utils.project import get_project_settings
    output_path = f'/opt/airflow/data/crawled/crawled_{date}/{spider.name}_{date}.csv'
    s = get_project_settings()
    print(s)
    s.update({'FEED_URI': output_path,
                'FEED_FORMAT': 'csv',
                'DEFAULT_REQUEST_HEADERS': {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
                    }
                })
    runner = CrawlerRunner(settings=s)
    # for spider in ban_spiders:
    
    # process.crawl(spider,feed_uri = output_path)
    # process.start()
    d = runner.crawl(spider,feed_uri = output_path)
    d.addBoth(lambda _: reactor.stop())
    reactor.run()
    # date='2024-05-09'
    # 'followall' is the name of one of the spiders of the project.
    

# def run_scrapy(date, spider):
#     subprocess.check_call(['scrapy', 'crawl', spider.name, '-o', f'/opt/airflow/data/crawled/crawled_{date}/{spider.name}_{date}.csv', '-s', 'LOG_LEVEL=INFO'])


def process_crawled_data(date, processor):
    outdir = f'/opt/airflow/data/processed/processed_{date}'  
    if not os.path.exists(outdir):  
        os.mkdir(outdir)  
    input_path = f"/opt/airflow/data/crawled/crawled_{date}/{processor.name}_{date}.csv"
    output_path = f"/opt/airflow/data/processed/processed_{date}/{processor.name}_{date}.csv"
    processor.input_path = input_path
    processor.output_path = output_path
    processor.data = pd.read_csv(input_path)
    processor.preprocess()


def merge_data(date):
    outdir = f'/opt/airflow/data/merged/merged_{date}'  
    if not os.path.exists(outdir):  
        os.mkdir(outdir)  
    input_files = [
        (f"/opt/airflow/data/processed/processed_{date}/alomuabannhadat_ban_daily_{date}.csv", "ban", "alomuabannhadat", r'(\d+)', "Hướng xây dựng: ", "Loại địa ốc: "),
        (f"/opt/airflow/data/processed/processed_{date}/alomuabannhadat_dat_daily_{date}.csv", "ban", "alomuabannhadat", r'(\d+)', None, "Loại địa ốc: "),
        (f"/opt/airflow/data/processed/processed_{date}/alomuabannhadat_thue_daily_{date}.csv", "thue", "alomuabannhadat", r'(\d+)', "Hướng xây dựng: ", "Loại địa ốc: "),
        (f"/opt/airflow/data/processed/processed_{date}/cafeland_ban_daily_{date}.csv", "ban", "cafeland", r'(\d+)', None, None),
        (f"/opt/airflow/data/processed/processed_{date}/cafeland_thue_daily_{date}.csv", "thue", "cafeland", r'(\d+)', None, None),
        (f"/opt/airflow/data/processed/processed_{date}/nhadat247_ban_daily_{date}.csv", "ban", "nhadat247", None, None, None),
        (f"/opt/airflow/data/processed/processed_{date}/nhadat247_thue_daily_{date}.csv", "thue", "nhadat247", None, None, None),
        (f"/opt/airflow/data/processed/processed_{date}/nhadatban24h_ban_daily_{date}.csv", "ban", "nhadatban24h", None, None, None),
        (f"/opt/airflow/data/processed/processed_{date}/nhadatban24h_thue_daily_{date}.csv", "thue", "nhadatban24h", None, None, None),
        (f"/opt/airflow/data/processed/processed_{date}/nhadatvui_ban_daily_{date}.csv", "ban", "nhadatvui", None, None, None),
        (f"/opt/airflow/data/processed/processed_{date}/nhadatvui_thue_daily_{date}.csv", "thue", "nhadatvui", None, None, None),
    ]
    output_path = f"/opt/airflow/data/merged/merged_{date}/merged_dataframe_{date}.csv"
    merger= RealEstateDataMerger(input_files, output_path)
    merger.process()

def deduplicate_data(date):
    outdir = f'/opt/airflow/data/deduplicated/deduplicated_{date}'  
    if not os.path.exists(outdir):  
        os.mkdir(outdir)
    input_path = f"/opt/airflow/data/merged/merged_{date}/merged_dataframe_{date}.csv"
    output_path = f"/opt/airflow/data/deduplicated/deduplicated_{date}/deduplicated_dataframe_{date}.csv"
    df = pd.read_csv(input_path)
    df = df.drop_duplicates(subset='URL', keep='first')
    df = df.astype(str)
    ban_df =  df[df["Type"] == "ban"]
    thue_df = df[df["Type"] == "thue"]
    ban_deduplicator = RemoveDuplicate(type="ban")
    ban_deduplicated_df = ban_deduplicator.remove_inside(ban_df)
    thue_deduplicator = RemoveDuplicate(type="thue")
    thue_deduplicated_df = thue_deduplicator.remove_inside(thue_df)
    deduplicated_df = pd.concat([ban_deduplicated_df, thue_deduplicated_df])
    deduplicated_df = deduplicated_df.drop(columns=['Standardized_Location', 'Best_Match_Location','milvus_id'])
    deduplicated_df = deduplicated_df.astype({'Vertical_length': float, 'Horizontal_length': float})


    deduplicated_df.to_csv(output_path, index=False)



dag = DAG(
    'bds_data_pipeline_history',
    default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    },
    schedule_interval=None
)




# run_spider_task = PythonOperator(
#     task_id='run_scrapy_spider',
#     python_callable=run_scrapy,
#     op_kwargs={'date': 'history'}, 
#     dag=dag
# )

dummy_operator = DummyOperator(task_id='start_preprocessing', dag=dag,trigger_rule=TriggerRule.ALL_DONE)

for spider in ban_spiders:
    run_spider_task = PythonOperator(
        task_id=f'run_spider_{spider.name}',
        python_callable=run_scrapy,
        op_kwargs={'date':'history','spider': spider},
        dag=dag,
    )
    run_spider_task >> dummy_operator

for spider in thue_spiders:
    run_spider_task = PythonOperator(
        task_id=f'run_spider_{spider.name}',
        python_callable=run_scrapy,
        op_kwargs={'date':'history','spider': spider},
        dag=dag,
    )
    run_spider_task >> dummy_operator

merge_data_task = PythonOperator(
        task_id=f"merge_crawled_data",
        python_callable=merge_data,
        op_kwargs={'date': 'history'}, 
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE
    )

deduplicate_data_task = PythonOperator(
        task_id=f"deduplicate_data",
        python_callable=deduplicate_data,
        op_kwargs={'date': 'history'}, 
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

load_data_to_bigquery_task = PythonOperator(
        task_id=f"load_data_to_bigquery",
        python_callable=load_data_to_bigquery,
        op_kwargs={'date': 'history'}, 
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

for processor in processors:
    process_data_task = PythonOperator(
        task_id=f"process_crawled_data_{processor.name}",
        python_callable=process_crawled_data,
        op_kwargs={'date': 'history', 'processor': processor}, 
        dag=dag
    )

    dummy_operator >> process_data_task >> merge_data_task >>deduplicate_data_task>>load_data_to_bigquery_task


# run_spider_task = PythonOperator(
#     task_id=f'run_scrapy_spider',
#     python_callable=run_scrapy,
#     op_kwargs={'date':'history','ban_spiders': ban_spiders,'thue_spiders': thue_spiders},
#     dag=dag,
# )
# run_spider_task >> process_data_task
# dummy_operator  >> 


