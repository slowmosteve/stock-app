# utility script for pulling stock data from the IEX API 
# note that this pulls the previous trading day's data

import os
import datetime
import json
import uuid
import yaml
import requests
from google.cloud import storage

# uncomment for use in cloud function
# os.chdir("/tmp")
# key = os.environ.get("access_key")

# util script uses yaml file to get API key
config_file = "util_config.yaml"
with open(config_file, 'r') as yamlfile:
    cfg = yaml.safe_load(yamlfile)

key = cfg["iex"]["api publishable key"]

# uncomment for use in cloud function
# list_symbols = [
#     "AXP",
#     "AAPL",
#     "BA",
#     "CAT",
#     "CSCO",
#     "CVX",
#     "XOM",
#     "GS",
#     "HD",
#     "IBM",
#     "INTC",
#     "JNJ",
#     "KO",
#     "JPM",
#     "MCD",
#     "MMM",
#     "MRK",
#     "MSFT",
#     "NKE",
#     "PFE",
#     "PG",
#     "TRV",
#     "UNH",
#     "UTX",
#     "VZ",
#     "V",
#     "WBA",
#     "WMT",
#     "DIS",
#     "DOW"
# ]

# util script gets symbol list from yaml
company_info = cfg["company info"]
list_symbols = []
for i in range(len(company_info)):
    list_symbols.append(company_info[i]["symbol"])

# loop through symbol list and add to results list
results = []
for i in range(len(list_symbols)):
    results.append({"symbol":list_symbols[i]})

# create an insert id and date for the job
insert_id = str(uuid.uuid4())
insert_date = datetime.date.today().isoformat()

# configure request details
headers = {
        'Accept': 'application/json'
}
params = { 
        "token": key
}

# loop through companies and request prices
for i in range(len(results)):
    url = 'https://cloud.iexapis.com/stable/stock/{}/previous'.format(results[i]["symbol"])
    resp = requests.get(url, headers=headers, params=params)
    if (resp.status_code == 200):
        print("request successful for symbol: {}".format(results[i]["symbol"]))
        keys = list(resp.json().keys())
        for j in range(len(keys)):
            results[i][keys[j]] = resp.json()[keys[j]]
        results[i]["insert_id"] = insert_id
        results[i]["insert_date"] = insert_date

# use response to get date for filename
file_date = results[0]["date"]

# write preprocessed file
filename_preprocess = "{}_dow_prices_preprocess.json".format(file_date)
with open(filename_preprocess, 'w') as outfile:
    json.dump(results, outfile)

# read preprocessed file
with open(filename_preprocess, "r") as read_file:
    data = json.load(read_file)
    output = [json.dumps(record) for record in data]
filename = "{}_dow_prices.json".format(file_date)

# create new file for newline delimited json
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
bucket_name = cfg["storage"]["stock staging"]
bucket = client.get_bucket(bucket_name)
blob = bucket.blob(filename)
blob.upload_from_filename(filename)
print("Uploaded file to staging bucket: {}".format(blob.name))