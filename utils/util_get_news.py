# utility script for pulling stock data from the NewsAPI
# update the query_date variable to determine the date used in the request

import os
import datetime
import json
import yaml
import uuid
import requests
from google.cloud import storage

# uncomment for use in cloud function
# os.chdir("/tmp")
# key = os.environ.get("access_key")

# util script uses yaml file to get API key
config_file = "util_config.yaml"
with open(config_file, 'r') as yamlfile:
    cfg = yaml.safe_load(yamlfile)

api_key = cfg["news api"]["api key"]

# uncomment for use in cloud function
# company_info = [
#     {
#     "symbol": "AAPL",
#     "companyName": "Apple, Inc."
#     },
#     {
#     "symbol": "AXP",
#     "companyName": "American Express Co."
#     },
#     {
#     "symbol": "BA",
#     "companyName": "The Boeing Co."
#     },
#     {
#     "symbol": "CAT",
#     "companyName": "Caterpillar, Inc."
#     },
#     {
#     "symbol": "CSCO",
#     "companyName": "Cisco Systems, Inc."
#     },
#     {
#     "symbol": "CVX",
#     "companyName": "Chevron Corp."
#     },
#     {
#     "symbol": "DIS",
#     "companyName": "The Walt Disney Co."
#     },
#     {
#     "symbol": "DOW",
#     "companyName": "Dow, Inc."
#     },
#     {
#     "symbol": "GS",
#     "companyName": "The Goldman Sachs Group, Inc."
#     },
#     {
#     "symbol": "HD",
#     "companyName": "The Home Depot, Inc."
#     },
#     {
#     "symbol": "IBM",
#     "companyName": "International Business Machines Corp."
#     },
#     {
#     "symbol": "INTC",
#     "companyName": "Intel Corp."
#     },
#     {
#     "symbol": "JNJ",
#     "companyName": "Johnson & Johnson"
#     },
#     {
#     "symbol": "JPM",
#     "companyName": "JPMorgan Chase & Co."
#     },
#     {
#     "symbol": "KO",
#     "companyName": "The Coca-Cola Co."
#     },
#     {
#     "symbol": "MCD",
#     "companyName": "McDonald's Corp."
#     },
#     {
#     "symbol": "MMM",
#     "companyName": "3M Co."
#     },
#     {
#     "symbol": "MRK",
#     "companyName": "Merck & Co., Inc."
#     },
#     {
#     "symbol": "MSFT",
#     "companyName": "Microsoft Corp."
#     },
#     {
#     "symbol": "NKE",
#     "companyName": "NIKE, Inc."
#     },
#     {
#     "symbol": "PFE",
#     "companyName": "Pfizer Inc."
#     },
#     {
#     "symbol": "PG",
#     "companyName": "Procter & Gamble Co."
#     },
#     {
#     "symbol": "TRV",
#     "companyName": "The Travelers Cos., Inc."
#     },
#     {
#     "symbol": "UNH",
#     "companyName": "UnitedHealth Group, Inc."
#     },
#     {
#     "symbol": "UTX",
#     "companyName": "United Technologies Corp."
#     },
#     {
#     "symbol": "V",
#     "companyName": "Visa, Inc."
#     },
#     {
#     "symbol": "VZ",
#     "companyName": "Verizon Communications, Inc."
#     },
#     {
#     "symbol": "WBA",
#     "companyName": "Walgreens Boots Alliance, Inc."
#     },
#     {
#     "symbol": "WMT",
#     "companyName": "Walmart, Inc."
#     },
#     {
#     "symbol": "XOM",
#     "companyName": "Exxon Mobil Corp."
#     }
# ]

# util script gets company info from yaml
company_info = cfg["company info"]

# create a list of companies to use for the news API search
results = []
for i in range(len(company_info)):
    results.append(company_info[i])

print(results)

# create an insert id for the job
insert_id = str(uuid.uuid4())

# set date for news API search (use yesterday by default)
query_date = (datetime.date.today() - datetime.timedelta(days = 1)).strftime('%Y-%m-%d')

## uncomment to use another date YYYY-MM-DD
# query_date = "2019-11-03"

# configure request for news API
url = 'https://newsapi.org/v2/everything'
headers = {
    'Accept': 'application/json'
}

for i in range(len(company_info)):
    params = {
        'q':company_info[i]["company name"],
        'from':query_date,
        'language':'en',
        'sortBy':'relevancy',
        'apiKey':api_key
    }
    resp = requests.get(url, headers=headers, params=params)

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
        # remove preprocessed file
        if os.path.exists(filename_preprocess):
            os.remove(filename_preprocess)
            print("deleting file: {}".format(filename_preprocess))
        else:
            print("{} does not exist".format(filename_preprocess))
        
        # instantiate GCS client and upload
        project_id = cfg["project id"]
        client = storage.Client(project_id)
        bucket_name = cfg["storage"]["news staging"]
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(filename)
        blob.upload_from_filename(filename)
