# utility script for generating sentiment scores and storing to Cloud Storage

import os
import yaml
import pandas
import uuid
import datetime
import textblob
from google.cloud import bigquery
from google.cloud import storage

# specify date to generate scores for
date = "2019-11-08"

# get config details from YAML
config_file = "util_config.yaml"
with open(config_file, 'r') as yamlfile:
    cfg = yaml.safe_load(yamlfile)

# configure bigquery
project_id = cfg["project id"]
dataset_id = cfg["bigquery"]["news dataset id"]
table_id = cfg["bigquery"]["news table id"]
client = bigquery.Client(project_id)

# configure cloud storage
bucket_name = cfg["storage"]["sentiment staging"]
gcs_client = storage.Client()
gcs_bucket = gcs_client.get_bucket(bucket_name)

# define query
bq_query = """
    SELECT
        symbol,
        date,
        news_result_id,
        title,
        description,
        content
    FROM
        `{}.{}.{}`
    WHERE
        date = "{}"
""".format(project_id, dataset_id, table_id, date)

# run query
query_job = client.query(bq_query)
query_result = query_job.result()
print("Query results found: {}".format(query_result.total_rows))

# store query results in dataframe and replace nulls with empty strings
df = query_result.to_dataframe()
df.fillna("", inplace=True)

# create an insert id and date for the job
insert_id = str(uuid.uuid4())
insert_date = datetime.date.today().isoformat()
df["insert_id"] = insert_id
df["insert_date"] = insert_date

def get_sentiment(string_list):
    """Given an input list of strings, returns lists with scores for sentiment polarity and subjectivity
    
    Args:
        string_list: list of strings to analyse
        
    Returns:
        sentiment: list of sentiment polarity scores
        subjectivity: list of sentiment subjectivity scores
    """
    sentiment = []
    subjectivity = []
    
    for text in string_list:
        blob = textblob.TextBlob(text)
        sentiment.append(blob.sentiment.polarity)
        subjectivity.append(blob.sentiment.subjectivity)
    
    return sentiment, subjectivity

string_fields = ["title", "description", "content"]

for field in string_fields:
    field_sentiment = field + "_sentiment"
    field_subjectivity = field + "_subjectivity"

    df[field_sentiment], df[field_subjectivity] = get_sentiment(df[field])

print(df.head())

# convert date format to string
df["date"] = pandas.to_datetime(df["date"])
df["date"] = df["date"].dt.strftime("%Y-%m-%d")

# save to new line delimited json
filename = "{}_sentiment.json".format(date)
df.to_json(filename, orient="records", lines=True)

# upload sentiment scores to storage
blob = gcs_bucket.blob(filename)
blob.upload_from_filename(filename)