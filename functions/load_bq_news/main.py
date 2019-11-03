import os
from google.cloud import bigquery
from google.cloud import storage

def load_bigquery(event, context):
    """A cloud function triggered by a change to a Cloud Storage bucket to load data to BigQuery
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    os.chdir("/tmp")

    file = event
    print("Found file: {}".format(file['name']))
    
    # use this flag to enable loading data (otherwise will only print logs that the file was found)
    bq_active = False
    
    # bigquery config
    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    dataset_id = os.environ.get("bq_dataset")
    dataset_ref = bq_client.dataset(dataset_id)
    table_id = os.environ.get("bq_table")

    # cloud storage config
    gcs_client = storage.Client()
    source_bucket_name = os.environ.get("gcs_bucket_staging")
    source_bucket = gcs_client.get_bucket(source_bucket_name)
    source_file_uri = "gs://{}/{}".format(source_bucket_name, file["name"])
    destination_bucket_name = os.environ.get("gcs_bucket_processed")
    destination_bucket = gcs_client.get_bucket(destination_bucket_name)
    
    if bq_active:
        # load data to bigquery
        load_job = bq_client.load_table_from_uri(source_file_uri, dataset_ref.table(table_id), job_config=job_config)
        print("Starting job {}".format(load_job.job_id))
        load_job.result()
        print("Job finished")
        destination_table = bq_client.get_table(dataset_ref.table(table_id))
        print("Loaded {} rows.".format(destination_table.num_rows))

        # move processed file from staging to processed bucket
        filename = file["name"]
        source_blob = source_bucket.blob(filename)
        destination_blob = source_bucket.copy_blob(source_blob, destination_bucket, filename)
        print("Moved file to processed bucket: {}".format(destination_blob.name))

        # delete file from staging bucket
        source_blob.delete()
        print("Deleted file from staging bucket: {}".format(filename))
    else:
        print("Loading to BQ is disabled. Set `bq_active` to True to enable.")