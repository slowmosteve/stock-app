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
        "companyName": "Apple, Inc."
        },
        {
        "symbol": "AXP",
        "companyName": "American Express Co."
        },
        {
        "symbol": "BA",
        "companyName": "The Boeing Co."
        },
        {
        "symbol": "CAT",
        "companyName": "Caterpillar, Inc."
        },
        {
        "symbol": "CSCO",
        "companyName": "Cisco Systems, Inc."
        },
        {
        "symbol": "CVX",
        "companyName": "Chevron Corp."
        },
        {
        "symbol": "DIS",
        "companyName": "The Walt Disney Co."
        },
        {
        "symbol": "DOW",
        "companyName": "Dow, Inc."
        },
        {
        "symbol": "GS",
        "companyName": "The Goldman Sachs Group, Inc."
        },
        {
        "symbol": "HD",
        "companyName": "The Home Depot, Inc."
        },
        {
        "symbol": "IBM",
        "companyName": "International Business Machines Corp."
        },
        {
        "symbol": "INTC",
        "companyName": "Intel Corp."
        },
        {
        "symbol": "JNJ",
        "companyName": "Johnson & Johnson"
        },
        {
        "symbol": "JPM",
        "companyName": "JPMorgan Chase & Co."
        },
        {
        "symbol": "KO",
        "companyName": "The Coca-Cola Co."
        },
        {
        "symbol": "MCD",
        "companyName": "McDonald's Corp."
        },
        {
        "symbol": "MMM",
        "companyName": "3M Co."
        },
        {
        "symbol": "MRK",
        "companyName": "Merck & Co., Inc."
        },
        {
        "symbol": "MSFT",
        "companyName": "Microsoft Corp."
        },
        {
        "symbol": "NKE",
        "companyName": "NIKE, Inc."
        },
        {
        "symbol": "PFE",
        "companyName": "Pfizer Inc."
        },
        {
        "symbol": "PG",
        "companyName": "Procter & Gamble Co."
        },
        {
        "symbol": "TRV",
        "companyName": "The Travelers Cos., Inc."
        },
        {
        "symbol": "UNH",
        "companyName": "UnitedHealth Group, Inc."
        },
        {
        "symbol": "UTX",
        "companyName": "United Technologies Corp."
        },
        {
        "symbol": "V",
        "companyName": "Visa, Inc."
        },
        {
        "symbol": "VZ",
        "companyName": "Verizon Communications, Inc."
        },
        {
        "symbol": "WBA",
        "companyName": "Walgreens Boots Alliance, Inc."
        },
        {
        "symbol": "WMT",
        "companyName": "Walmart, Inc."
        },
        {
        "symbol": "XOM",
        "companyName": "Exxon Mobil Corp."
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
            'q':company_info[i]["companyName"],
            'from':query_date,
            'language':'en',
            'sortBy':'relevancy',
            'apiKey':key
        }
        resp = requests.get(url, headers=headers, params=params)

        # populate articles json object with query results
        if (resp.status_code == 200):
            print("request successful for company: {}".format(company_info[i]["companyName"]))
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