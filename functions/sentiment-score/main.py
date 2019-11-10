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
news_dataset_id = os.environ.get("news_dataset_id")
news_table_id = os.environ.get("news_table_id")
sentiment_dataset_id = os.environ.get("sentiment_dataset_id")
sentiment_table_id = os.environ.get("sentiment_table_id")
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

def check_bigquery(bq_date):
    """Checks BigQuery table for a given date to ensure that records do not exist

    Args:
        bq_date: the date that will be checked for records in BigQuery

    Returns:
        records_found: the number of records found on the date provided
    """
    check_date_query = """
        SELECT
            COUNT(1)
        FROM `{}.{}.{}`
        WHERE
            date = "{}"
    """.format(project_id, sentiment_dataset_id, sentiment_table_id, bq_date)
    
    query_job = client.query(check_date_query)
    query_result = query_job.result()
    query_rows = query_result.total_rows
    print("records found for {}.{} on {}: {}".format(sentiment_dataset_id, sentiment_table_id, bq_date, query_rows))
    return query_rows

def sentiment_score(request):
    """A function to generate sentiment scores for news text and upload to Cloud Storage

    Args:
        request (flask.Request): HTTP request object
    """

    # set date to filter BQ records for yesterday
    date = (datetime.date.today() - datetime.timedelta(days = 1)).strftime('%Y-%m-%d')

    # check if records exist for this date
    existing_data = check_bigquery(date)

    if (existing_data > 0):
        print("data already exists for date provided")
    else:
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
            """.format(project_id, news_dataset_id, news_table_id, date)

        # run query
        query_job = client.query(bq_query)
        query_result = query_job.result()
        print("query results found: {}".format(query_result.total_rows))

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

        # convert date format to string
        df["date"] = pandas.to_datetime(df["date"])
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")

        # save to new line delimited json
        filename = "{}_sentiment.json".format(date)
        df.to_json(filename, orient="records", lines=True)

        # upload sentiment scores to storage
        blob = gcs_bucket.blob(filename)
        blob.upload_from_filename(filename)