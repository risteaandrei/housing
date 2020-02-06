from io import StringIO

import boto3
import json

import pandas as pd

s3 = boto3.client('s3')
price_history_key = 'price_history'

def create_df_from_prices(data, key):
    ids = []
    prices = []

    json_data = json.loads(data)
    for ap in json_data[key]:
        ids.append(ap['id'])
        prices.append(int(ap['price'].replace('.', '')))

    df = pd.DataFrame('', index=[key], columns=ids)
    df.loc[key] = prices

    return df

def create_prices_df():
    key = "20200106"
    object = s3.get_object(Bucket='andrei-housing-prices', Key=key)
    data = object['Body'].read().decode('utf-8')
    alltime_df = create_df_from_prices(data, key)

    for i in range(20200107, 20200118):
        key = str(i)
        object = s3.get_object(Bucket='andrei-housing-prices', Key=key)
        data = object['Body'].read().decode('utf-8')
        current_df = create_df_from_prices(data, key)

        alltime_df = pd.concat([alltime_df, current_df])
    
    alltime_df.to_csv(price_history_key, sep='\t')
    data = open(price_history_key, 'rb')
    s3.put_object(Bucket='andrei-housing-prices', Key=price_history_key, Body=data)

def lambda_handler(event, context):
    s3_event = event['Records'][0]['Sns']['Message']
    s3_json = json.loads(s3_event)
    key = s3_json['Records'][0]['s3']['object']['key']

    object = s3.get_object(Bucket='andrei-housing-prices', Key=key)
    data = object['Body'].read().decode('utf-8')
    today_df = create_df_from_prices(data, key)
    
    object = s3.get_object(Bucket='andrei-housing-prices', Key=price_history_key)
    data = object['Body'].read().decode('utf-8')
    data_io = StringIO(data)
    alltime_df = pd.read_csv(data_io, index_col=0, sep='\t')
    
    alltime_df = pd.concat([alltime_df, today_df])
    alltime_df.to_csv('/tmp/price_history.csv', sep='\t')
    data = open('/tmp/price_history.csv', 'rb')
    s3.put_object(Bucket='andrei-housing-prices', Key=price_history_key, Body=data)

if __name__ == "__main__":
    #create_prices_df()
    lambda_handler('', '')