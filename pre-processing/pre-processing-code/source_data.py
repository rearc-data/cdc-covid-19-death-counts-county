import os
import boto3
from urllib.request import urlretrieve

def source_dataset(new_filename, s3_bucket, new_s3_key):

    source_dataset_url = 'https://data.cdc.gov/api/views/kn79-hsxy/rows'

    # Download the file from `url` and save it locally under `file_name`:
    for frmt in ['.csv', '.json']:
        urlretrieve(source_dataset_url + frmt, '/tmp/' + new_filename + frmt)

    asset_list = []

    # Creates S3 connection
    s3 = boto3.client('s3')

    # Looping through filenames, uploading to S3
    for filename in os.listdir('/tmp'):

        s3.upload_file('/tmp/' + filename, s3_bucket,
                       new_s3_key + filename)

        asset_list.append(
            {'Bucket': s3_bucket, 'Key': new_s3_key + filename})

    return asset_list
