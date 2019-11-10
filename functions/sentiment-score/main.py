import os
import uuid
import datetime
import pandas
import textblob
from google.cloud import bigquery
from google.cloud import storage

# change working directory in order to write to disk
os.chdir("/tmp")

# configure bigquery
project_id = os.environ.get("project_id")
dataset_id = os.environ.get("dataset_id")
table_id = os.environ.get("table_id")
client = bigquery.Client()

# configure cloud storage
bucket_name = os.environ.get("bucket_name")
gcs_client = storage.Client()
gcs_bucket = gcs_client.get_bucket(bucket_name)

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

def sentiment_score(request):
    """A function to generate sentiment scores for news text and store in BigQuery

    Args:
        request (flask.Request): HTTP request object
    """

    # set date to filter BQ records
    date = "2019-11-01"

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

    query_job = client.query(bq_query)  # API request
    query_result = query_job.result()  # Waits for query to finish
    print("Query results found: {}".format(query_result.total_rows))

    # store query results in dataframe and replace null with empty strings
    df = query_result.to_dataframe()
    df.fillna("", inplace=True)

    # create an insert id and date for the job
    insert_id = str(uuid.uuid4())
    insert_date = datetime.date.today().isoformat()
    df["insert_id"] = insert_id
    df["insert_date"] = insert_date

    # specify string fields to retrieve sentiment scores for
    string_fields = ["title", "description", "content"]
    for field in string_fields:
        field_sentiment = field + "_sentiment"
        field_subjectivity = field + "_subjectivity"
        df[field_sentiment], df[field_subjectivity] = get_sentiment(df[field])

    # save to new line delimited json
    filename = "{}_sentiment.json".format(date)
    df.to_json(filename, orient="records", lines=True)

    # upload sentiment scores to storage
    blob = gcs_bucket.blob(filename)
    blob.upload_from_filename(filename)