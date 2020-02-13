from io import StringIO

import boto3
import datetime
import json
import pandas as pd
import re

import sys
sys.path.append('tools')
from housing_common import *
############################
## common
############################

#import boto3
#import datetime
#from io import StringIO
#import json
#import pandas as pd
#import os
#
#s3 = boto3.client('s3')
#data_dir = 'data/'
#output_dir = 'output/'
#tmp_dir = '/tmp/'
#first_scraping_day = datetime.date(2020, 1, 6)
#
#def key_from_sns_from_s3_put(event):
#    s3_json = json_loads(event['Records'][0]['Sns']['Message'])
#    return s3_json['Records'][0]['s3']['object']['key']
#
#def string_to_file(data, filename, location=''):
#    text_file = open(location + filename, 'w')
#    text_file.write(data)
#
#def string_from_file(filename, location=''):
#    text_file = open(location + filename, 'r')
#    return text_file.read()
#
#def s3_get(key):
#    object = s3.get_object(Bucket='andrei-housing-prices', Key=key)
#    data = object['Body'].read().decode('utf-8')
#    return data
#
#def download_from_s3(key, location=''):
#    string_to_file(s3_get(key), key, location)
#
#def s3_put(key, data):
#    s3.put_object(Bucket='andrei-housing-prices', Key=key, Body=data)
#
#def upload_to_s3(key, location=''):
#    s3_put(key, string_from_file(key, location))
#
## JSON
#
#def json_from_file(filename, location=''):
#    return json.load(open(location + filename))
#
#def json_to_file(data, filename, location=''):
#    json.dump(data, open(location + filename, 'w'))
#
#def json_from_s3(key):
#    return json.load(s3_get(key))
#
#def json_to_s3(data, key):
#    s3_put(key, json.dumps(data))
#
#def load_json(key, local=False):
#    if local:
#        return json_from_file(key, data_dir)
#    else:
#        return json_from_s3(key)
#
#def save_json(data, key, local=False):
#    if local:
#        return json_to_file(data, key, output_dir)
#    else:
#        return json_to_s3(data, key)
#
## dataframe
#
#def df_from_file(filename, location=''):
#    return pd.read_csv(location + filename, index_col=0, sep='\t')
#
#def df_to_file(df, filename, location=''):
#    df.to_csv(location + filename, sep='\t')
#
#def df_from_s3(key):
#    return pd.read_csv(StringIO(s3_get(key)), index_col=0, sep='\t')
#
#def df_to_s3(df, key):
#    df_to_file(df, key, tmp_dir)
#    s3_put(key, tmp_dir + key)
#
#def load_df(key, local=False):
#    if local:
#        return df_from_file(key, data_dir)
#    else:
#        return df_from_s3(key)
#
#def save_df(data, key, local=False):
#    if local:
#        return df_to_file(data, key, output_dir)
#    else:
#        return df_to_s3(data, key)
#
## date
#
#def today():
#    return datetime.date.today()
#
#def date_to_str(date):
#    return date.strftime('%Y%m%d')
#
#def sync_local_with_s3():
#    day_delta = datetime.timedelta(days=1)
#    for i in range((today() - first_scraping_day).days + 1):
#        current_str = date_to_str(first_scraping_day + i * day_delta)
#        if not os.path.exists(data_dir + current_str):
#            download_from_s3(current_str, data_dir)
#
#    download_from_s3('price_history', data_dir)
#    download_from_s3('inventory', data_dir)

############################
## end common
############################

s3 = boto3.client('s3')
inventory_key = 'inventory'
new_key = 'new_today'

characteristics = dict(
    [ ("url", "url")
    , ("price", "price")
    , ("rooms", "Nr. camere")
    , ("usable_surface", "Suprafaţă utilă")
    , ("total_surface", "Suprafaţă utilă totală")
    , ("built_surface", "Suprafaţă construită")
    , ("partitioning", "Compartimentare")
    , ("confort", "Confort")
    , ("floor", "Etaj")
    , ("kitchens", "Nr. bucătării")
    , ("bathrooms", "Nr. băi")
    , ("year", "An construcţie")
    , ("building_structure", "Structură rezistenţă")
    , ("building_type", "Tip imobil")
    , ("building_height", "Regim înălţime")
    , ("balconies", "Nr. balcoane")
    , ("neighborhood", "neighborhood")
])

def first_number_in_str(str):
    return [int(s) for s in str.split() if s.isdigit()][0]

def converter(attribute, value):
    if attribute == 'price':
        return int(value.replace('.', ''))
    elif attribute == 'usable_surface' or \
         attribute == 'total_surface' or \
         attribute == 'built_surface':
        mo = re.match('.+([0-9])[^0-9]*$', value)
        num = value[0:mo.start(1)+1]
        return float(num.replace(',', '.'))
    elif attribute == 'floor':
        if 'Parter' in value:
            return 'ground'
        elif 'Demisol' in value:
            return 'semibasement'
        elif 'Mansarda' in value:
            return 'mansard'
        else:
            value = value.replace('Etaj', '')
            return first_number_in_str(value)
    elif attribute == 'year':
        return first_number_in_str(value)
    elif attribute == 'balconies':
        return first_number_in_str(value)
    else:
        return value


def create_inventory_df(data, key):
    id = []

    columns = {}
    for k in characteristics:
        columns[k] = []

    for ap in data[key]:
        id.append(ap['id'])

        for k, v in characteristics.items():
            if v in ap:
                columns[k].append(converter(k, ap[v]))
            else:
                columns[k].append(None)

    df = pd.DataFrame(columns, index=[id])
    return df

def lambda_handler(event, context):
    execute(key_from_sns_from_s3_put(event))

def execute(key, local=False):
    today_json = load_json(key, local)
    today_df = create_inventory_df(today_json, key)

    inventory_df = load_df(inventory_key, local)

    new_df = pd.DataFrame(columns=inventory_df.columns)

    inventory_df['active'] = False
    for index, row in today_df.iterrows():
        if index[0] not in inventory_df.index:
            new_df.loc[index[0]] = row

        inventory_df.loc[index[0]] = row
        inventory_df.loc[index[0], 'active'] = True
    
    save_df(inventory_df, inventory_key, local)
    save_df(new_df, new_key, local)

if __name__ == "__main__":
    sync_local_with_s3()
    #execute('tomorrow', True)
