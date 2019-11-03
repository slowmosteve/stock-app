# utility script for uploading news files from Storage to BigQuery

import os
import yaml
from google.cloud import bigquery
from google.cloud import storage

# get company info from YAML file
config_file = "util_config.yaml"
with open(config_file, 'r') as yamlfile:
    cfg = yaml.safe_load(yamlfile)
company_list = cfg["company info"]

# configure BQ details
project_id = cfg["project id"]
news_dataset_id = cfg["bigquery"]["news dataset id"]
news_table_id = cfg["bigquery"]["news table id"]
bq_client = bigquery.Client(project_id)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
dataset_ref = bq_client.dataset(news_dataset_id)

# configure GCS details
source_bucket_name = cfg["storage"]["news staging"]
destination_bucket_name = cfg["storage"]["news processed"]
gcs_client = storage.Client()
source_bucket = gcs_client.get_bucket(source_bucket_name)
destination_bucket = gcs_client.get_bucket(destination_bucket_name)

# set date for files to upload
file_date = "2019-11-02"

# loop through company symbols and load files matching the date
for i in range(len(company_list)):
    try:
        # set file name and GCS bucket URI
        filename = "{}_{}_news.json".format(file_date, company_list[i]["symbol"])
        file_uri = "gs://{}/{}".format(source_bucket_name, filename)

        # load file to BQ
        load_job = bq_client.load_table_from_uri(file_uri, dataset_ref.table(news_table_id), job_config=job_config)
        print("Starting job {}".format(load_job.job_id))
        load_job.result()
        print("Loaded file to BigQuery: {}".format(filename))
        destination_table = bq_client.get_table(dataset_ref.table(news_table_id))
        print("Loaded {} rows.".format(destination_table.num_rows))

        # transfer file to processed bucket
        source_blob = source_bucket.blob(filename)
        destination_blob = source_bucket.copy_blob(source_blob, destination_bucket, filename)
        print("Transfered file to processed bucket: {}".format(filename))

        # delete file from staging bucket
        source_blob.delete()
        print("Deleted file from staging bucket: {}".format(filename))
    except Exception as e:
        print('Error: {}'.format(e))
