import boto3
import datetime
from io import StringIO
import json
import pandas as pd
import os

s3 = boto3.client('s3')
data_dir = 'data/'
output_dir = 'output/'
tmp_dir = '/tmp/'
first_scraping_day = datetime.date(2020, 1, 6)

def key_from_sns_from_s3_put(event):
    s3_json = json.loads(event['Records'][0]['Sns']['Message'])
    return s3_json['Records'][0]['s3']['object']['key']

def string_to_file(data, filename, location=''):
    text_file = open(location + filename, 'w')
    text_file.write(data)

def string_from_file(filename, location=''):
    text_file = open(location + filename, 'r')
    return text_file.read()

def s3_get(key):
    object = s3.get_object(Bucket='andrei-housing-prices', Key=key)
    data = object['Body'].read().decode('utf-8')
    return data

def download_from_s3(key, location=''):
    string_to_file(s3_get(key), key, location)

def s3_put(key, data):
    s3.put_object(Bucket='andrei-housing-prices', Key=key, Body=data)

def upload_to_s3(key, location=''):
    s3_put(key, string_from_file(key, location))

# JSON

def json_from_file(filename, location=''):
    return json.load(open(location + filename))

def json_to_file(data, filename, location=''):
    json.dump(data, open(location + filename, 'w'))

def json_from_s3(key):
    return json.loads(s3_get(key))

def json_to_s3(data, key):
    s3_put(key, json.dumps(data))

def load_json(key, local=False):
    if local:
        return json_from_file(key, data_dir)
    else:
        return json_from_s3(key)

def save_json(data, key, local=False):
    if local:
        return json_to_file(data, key, output_dir)
    else:
        return json_to_s3(data, key)

# dataframe

def df_from_file(filename, location=''):
    return pd.read_csv(location + filename, index_col=0, sep='\t')

def df_to_file(df, filename, location=''):
    df.to_csv(location + filename, sep='\t')

def df_from_s3(key):
    return pd.read_csv(StringIO(s3_get(key)), index_col=0, sep='\t')

def df_to_s3(df, key):
    df_to_file(df, key, tmp_dir)
    upload_to_s3(key, tmp_dir)

def load_df(key, local=False):
    if local:
        return df_from_file(key, data_dir)
    else:
        return df_from_s3(key)

def save_df(data, key, local=False):
    if local:
        return df_to_file(data, key, output_dir)
    else:
        return df_to_s3(data, key)

# date

def today():
    return datetime.date.today()

def date_str(date):
    return date.strftime('%Y%m%d')

def sync_local_with_s3():
    day_delta = datetime.timedelta(days=1)
    for i in range((today() - first_scraping_day).days + 1):
        current_str = date_str(first_scraping_day + i * day_delta)
        if not os.path.exists(data_dir + current_str):
            download_from_s3(current_str, data_dir)

    download_from_s3('price_history', data_dir)
    download_from_s3('inventory', data_dir)
    download_from_s3('new_today', data_dir)
