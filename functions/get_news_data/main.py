import os
import datetime
import json
import uuid
import requests
from google.cloud import storage

def get_news_data(request):
    """Defines a Cloud Function to request news data from the NewsAPI using yesterday's date

    Args:
        request (flask.Request): HTTP request object
    """
    # change directory to temporary folder available in Cloud Function
    os.chdir("/tmp")

    # retrieve API key from environment variables
    key = os.environ.get("access_key")

    # instantiate GCS client and configure details
    client = storage.Client()
    destination_bucket_name = os.environ.get("gcs_bucket_name")
    destination_bucket = client.get_bucket(destination_bucket_name)
    
    # set company info used for news search queries
    company_info = [
        {
        "symbol": "AAPL",
        "company name": "Apple, Inc."
        },
        {
        "symbol": "AXP",
        "company name": "American Express Co."
        },
        {
        "symbol": "BA",
        "company name": "The Boeing Co."
        },
        {
        "symbol": "CAT",
        "company name": "Caterpillar, Inc."
        },
        {
        "symbol": "CSCO",
        "company name": "Cisco Systems, Inc."
        },
        {
        "symbol": "CVX",
        "company name": "Chevron Corp."
        },
        {
        "symbol": "DIS",
        "company name": "The Walt Disney Co."
        },
        {
        "symbol": "DOW",
        "company name": "Dow, Inc."
        },
        {
        "symbol": "GS",
        "company name": "The Goldman Sachs Group, Inc."
        },
        {
        "symbol": "HD",
        "company name": "The Home Depot, Inc."
        },
        {
        "symbol": "IBM",
        "company name": "International Business Machines Corp."
        },
        {
        "symbol": "INTC",
        "company name": "Intel Corp."
        },
        {
        "symbol": "JNJ",
        "company name": "Johnson & Johnson"
        },
        {
        "symbol": "JPM",
        "company name": "JPMorgan Chase & Co."
        },
        {
        "symbol": "KO",
        "company name": "The Coca-Cola Co."
        },
        {
        "symbol": "MCD",
        "company name": "McDonald's Corp."
        },
        {
        "symbol": "MMM",
        "company name": "3M Co."
        },
        {
        "symbol": "MRK",
        "company name": "Merck & Co., Inc."
        },
        {
        "symbol": "MSFT",
        "company name": "Microsoft Corp."
        },
        {
        "symbol": "NKE",
        "company name": "NIKE, Inc."
        },
        {
        "symbol": "PFE",
        "company name": "Pfizer Inc."
        },
        {
        "symbol": "PG",
        "company name": "Procter & Gamble Co."
        },
        {
        "symbol": "TRV",
        "company name": "The Travelers Cos., Inc."
        },
        {
        "symbol": "UNH",
        "company name": "UnitedHealth Group, Inc."
        },
        {
        "symbol": "UTX",
        "company name": "United Technologies Corp."
        },
        {
        "symbol": "V",
        "company name": "Visa, Inc."
        },
        {
        "symbol": "VZ",
        "company name": "Verizon Communications, Inc."
        },
        {
        "symbol": "WBA",
        "company name": "Walgreens Boots Alliance, Inc."
        },
        {
        "symbol": "WMT",
        "company name": "Walmart, Inc."
        },
        {
        "symbol": "XOM",
        "company name": "Exxon Mobil Corp."
        }
    ]

    # use yesterday for date filter
    query_date = (datetime.date.today() - datetime.timedelta(days = 1)).strftime('%Y-%m-%d')

    # create an insert id for the job
    insert_id = str(uuid.uuid4())

    # configure request for news API
    url = 'https://newsapi.org/v2/everything'
    headers = {
        'Accept': 'application/json'
    }
    
    # loop through companies and search for articles using company names
    for i in range(len(company_info)):
        params = {
            'q':company_info[i]["company name"],
            'from':query_date,
            'language':'en',
            'sortBy':'relevancy',
            'apiKey':key
        }
        resp = requests.get(url, headers=headers, params=params)

        # populate articles json object with query results
        if (resp.status_code == 200):
            print("request successful for company: {}".format(company_info[i]["company name"]))
            articles = resp.json()['articles']
            total_results = resp.json()['totalResults']

            for j in range(len(articles)):
                articles[j]["symbol"] = company_info[i]["symbol"]
                articles[j]["company_name"] = company_info[i]["company name"]
                articles[j]["date"] = query_date
                articles[j]["total_results"] = total_results
                articles[j]["news_result_id"] = str(uuid.uuid4())
                articles[j]["insert_id"] = insert_id

            # write preprocessed file
            filename_preprocess = "{}_{}_news_preprocess.json".format(query_date, company_info[i]["symbol"])
            with open(filename_preprocess, 'w') as outfile:
                json.dump(articles, outfile)

            # read preprocessed file
            with open(filename_preprocess, "r") as read_file:
                data = json.load(read_file)
                output = [json.dumps(record) for record in data]

            # create new file for newline delimited json
            filename = "{}_{}_news.json".format(query_date, company_info[i]["symbol"])

            with open(filename, 'w') as obj:
                for i in output:
                    obj.write(i+'\n')
                print("converted file to newline delimited json")

            # delete preprocessed file
            if os.path.exists(filename_preprocess):
                os.remove(filename_preprocess)
                print("deleting file: {}".format(filename_preprocess))
            else:
                print("{} does not exist".format(filename_preprocess))

            # load file to storage
            blob = destination_bucket.blob(filename)
            blob.upload_from_filename(filename)
            print("loaded file to storage: {}".format(filename))