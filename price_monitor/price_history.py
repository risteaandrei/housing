from io import StringIO

import boto3
import json

import pandas as pd

import sys
sys.path.append('tools')
from housing_common import *

s3 = boto3.client('s3')
price_history_key = 'price_history'

def create_df_from_prices(json_data, key):
    ids = []
    prices = []

    for ap in json_data[key]:
        ids.append(ap['id'])
        prices.append(int(ap['price'].replace('.', '')))

    df = pd.DataFrame('', index=[key], columns=ids)
    df.loc[key] = prices

    return df

def lambda_handler(event, context):
    execute(key_from_sns_from_s3_put(event))

def execute(key, local=False):
    today_json = load_json(key, local)
    today_df = create_df_from_prices(today_json, key)
    today_df = today_df.iloc[:, ~today_df.columns.duplicated()]
    
    alltime_df = load_df(price_history_key, local)
    alltime_df = pd.concat([alltime_df, today_df])
    save_df(alltime_df, price_history_key, local)

if __name__ == "__main__":
    execute(date_to_str(today()), local=True)