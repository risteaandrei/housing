import boto3
import datetime
from io import StringIO
import json
import pandas as pd
import os

s3 = boto3.client('s3')
data_dir = 'data/'
first_scraping_day = datetime.date(2020, 1, 6)

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

def json_from_file(filename, location=''):
    return json.load(open(location + filename))

def df_from_file(filename, location=''):
    return pd.read_csv(location + filename, index_col=0, sep='\t')

def df_from_s3(key):
    return pd.read_csv(StringIO(s3_get(key)), index_col=0, sep='\t')

def df_to_file(df, filename, location=''):
    df.to_csv(location + filename, sep='\t')

def load_df(key, local=False):
    if local:
        return df_from_file(key, 'data/')
    else:
        return df_from_s3(key)

def today():
    return datetime.date.today()

def date_to_str(date):
    return date.strftime('%Y%m%d')

def sync_local_with_s3():
    day_delta = datetime.timedelta(days=1)
    for i in range((today() - first_scraping_day).days):
        current_str = date_to_str(first_scraping_day + i * day_delta)
        if not os.path.exists(data_dir + current_str):
            download_from_s3(current_str, data_dir)

    download_from_s3('price_history', data_dir)
    download_from_s3('inventory', data_dir)


if __name__ == "__main__":
    sync_local_with_s3()
