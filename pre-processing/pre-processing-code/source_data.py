import os
import boto3
import json
import csv
from urllib.request import urlopen

def source_dataset(new_filename, s3_bucket, new_s3_key):
            
    json_url = urlopen(
        'https://data.cdc.gov/resource/kn79-hsxy.json').read().decode('UTF-8')

    data = json.loads(json_url)

    with open('/tmp/' + new_filename + '.json', 'w', encoding='utf-8') as j:
        j.write('\n'.join(json.dumps(datum) for datum in data))

    with open('/tmp/' + new_filename + '.csv', 'w', encoding='utf-8') as c:
        writer = csv.DictWriter(c, fieldnames=data[0])
        writer.writeheader()
        writer.writerows(data)

    s3 = boto3.client('s3')
    s3.upload_file('/tmp/' + new_filename + '.json',
                   s3_bucket, new_s3_key + '.json')
    s3.upload_file('/tmp/' + new_filename + '.csv',
                   s3_bucket, new_s3_key + '.csv')
