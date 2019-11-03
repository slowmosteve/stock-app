# utility file for moving files from one bucket to another

import os
import yaml
from google.cloud import storage

config_file = "util_config.yaml"
with open(config_file, 'r') as yamlfile:
    cfg = yaml.safe_load(yamlfile)

project_id = cfg["project id"]
source_bucket_name = cfg["storage"]["stock processed"]
destination_bucket_name = cfg["storage"]["news processed"]
filename = "test.py"

gcs_client = storage.Client()
source_bucket = gcs_client.get_bucket(source_bucket_name)
destination_bucket = gcs_client.get_bucket(destination_bucket_name)

# copy to new destination
source_blob = source_bucket.blob(filename)
destination_blob = source_bucket.copy_blob(source_blob, destination_bucket, filename)
print("Moved file to processed bucket: {}".format(destination_blob.name))

# delete in old destination
source_blob.delete()
print("Deleted file from staging bucket: {}".format(filename))