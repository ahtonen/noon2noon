import requests
import datetime
import os
import json
import urllib.parse
import boto3

print('Loading function')

s3 = boto3.client('s3')

#
# Pass the data to send as `event.data`, and the request options as
# `event.options`
#
def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    #headers = {'Authorization': 'apikey THISISHIDDEN'}
    baseURL = 'https://yb.tl/fastnet2023-adrena.txt'
    target_path = '/tmp/' + datetime.datetime.now().strftime('%Y-%m-%d_posreport') + '.txt'

    response = requests.get(baseURL, stream=True)
    handle = open(target_path, "wb")

    for chunk in response.iter_content(chunk_size=512):
        if chunk:  # filter out keep-alive new chunks
            handle.write(chunk)

    handle.close()