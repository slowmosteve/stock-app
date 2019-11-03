# utility script for getting company info from IEX API and generating new line delimited JSON for upload to BigQuery

import os
import json
import uuid
import yaml
import requests

config_file = "util_config.yaml"
with open(config_file, 'r') as yamlfile:
    cfg = yaml.safe_load(yamlfile)

api_key = cfg["iex"]["api publishable key"]
headers = {
    'Accept': 'application/json'
}
params = { 
    "token": api_key
}

company_list = cfg["company info"]

# create an insert id for the job
insert_id = str(uuid.uuid4())

# create a list for the results
results = []
for i in range(len(company_list)):
    results.append({"symbol":company_list[i]["symbol"]})

print(results)

# loop through symbols and request the company info
for i in range(len(company_list)):
    url = 'https://cloud.iexapis.com/stable/stock/{}/company'.format(company_list[i]["symbol"])
    print("\nrequesting company info from: {}".format(url))
    resp = requests.get(url, headers=headers, params=params)
    if (resp.status_code == 200):
        print("request successful for symbol: {}".format(results[i]["symbol"]))
        keys = list(resp.json().keys())
        for j in range(len(keys)):
            results[i][keys[j]] = resp.json()[keys[j]]
        results[i]["insert_id"] = insert_id

# write preprocessed file
filename_preprocess = "company_details_preprocess.json"
with open(filename_preprocess, 'w') as outfile:
    json.dump(results, outfile)

# open preprocess and convert to newline delimited JSON
with open(filename_preprocess, "r") as read_file:
    data = json.load(read_file)
    output = [json.dumps(record) for record in data]

# write processed file
filename = "company_info.json"
with open(filename, 'w') as obj:
    for i in output:
        obj.write(i+'\n')

# delete preprocessed file
if os.path.exists(filename_preprocess):
    os.remove(filename_preprocess)
    print("deleting file: {}".format(filename_preprocess))
else:
    print("{} does not exist".format(filename_preprocess))