"""
Mark Porath, Nov 30

this script moved csv rows flawlessly; 
but later I learned that apache_beam would be the real challenge and prize

practice-springml:nyc19_dataset.bnb_table
practice-springml:nyc19_dataset.listings_per_hood
"""


from google.cloud import bigquery
from dotenv import load_dotenv


#--------------------------------------------------------------------
# CONNECT TO GCS PROJECT

load_dotenv()

client = bigquery.Client()

#--------------------------------------------------------------------
# CREATE DATASET TO HOLD TABLES

def create_dataset():

    dataset_name = "nyc19_dataset"
    dataset_id = "{}.{}".format(client.project, dataset_name)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"

    dataset = client.create_dataset(dataset, timeout=30)

    return dataset, dataset_name

#--------------------------------------------------------------------
# CREATE TABLE FROM BUCKET-BASED CSV

def load_csv_to_table(dataset):

    table_name = "bnb_table"
    table_id = "{}.{}.{}".format(
        client.project, dataset.dataset_id, table_name
        )

    schema = [
        bigquery.SchemaField("id", "INT64"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("host_id", "INT64"),
        bigquery.SchemaField("host_name", "STRING"),
        bigquery.SchemaField("neighbourhood_group", "STRING"),
        bigquery.SchemaField("neighbourhood", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT64"),
        bigquery.SchemaField("longitude", "FLOAT64"),
        bigquery.SchemaField("room_type", "STRING"),
        bigquery.SchemaField("price", "INT64"),
        bigquery.SchemaField("minimum_nights", "INT64"),
        bigquery.SchemaField("number_of_reviews", "INT64"),
        bigquery.SchemaField("last_review", "STRING"),
        bigquery.SchemaField("reviews_per_month", "FLOAT64"),
        bigquery.SchemaField("calculated_host_listings_count", "INT64"),
        bigquery.SchemaField("availability_365", "INT64")
        ]

    job_config = bigquery.LoadJobConfig(
        schema = schema,
        skip_leading_rows = 1,
        source_format = bigquery.SourceFormat.CSV,
        max_bad_records = 10,
        write_disposition = "WRITE_TRUNCATE",
        ignore_unknown_values = True,
        allow_jagged_rows = True,
        allow_quoted_newlines = True
        )

    uri = "gs://airbnb-nyc-19/AB_NYC_2019.csv"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config = job_config
        )

    load_job.result()  # Wait for job to complete

    return table_name

#--------------------------------------------------------------------
# CREATE TABLE FROM QUERY

def write_query_to_table(dataset, dataset_name, table_name):

    q_table_name = "listings_per_nhood"
    q_table_id = "{}.{}.{}".format(
        client.project, dataset.dataset_id, q_table_name
        )

    q_job_config = bigquery.QueryJobConfig(
        destination = q_table_id,
        write_disposition = "WRITE_EMPTY",
        priority = "BATCH"
        )

    sql = """
        SELECT neighbourhood, COUNT(*) as n_listings 
        FROM `{}.{}`
        GROUP BY neighbourhood;
    """.format(dataset_name, table_name)

    query_job = client.query(sql, job_config = q_job_config)

    query_job.result()  # Wait for the job to complete.

#--------------------------------------------------------------------

if __name__ == "__main__":
    
    dataset, dataset_name = create_dataset()
    table_name = load_csv_to_table(dataset)
    write_query_to_table(dataset, dataset_name, table_name)
