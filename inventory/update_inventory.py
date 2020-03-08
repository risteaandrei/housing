from io import StringIO

import boto3
import datetime
import json
import pandas as pd
import re

import sys
sys.path.append('tools')
from housing_common import *

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
        else:
            inventory_df.loc[index[0], 'price'] = row['price']

        inventory_df.loc[index[0], 'active'] = True


    save_df(inventory_df, inventory_key, local)
    if len(new_df.index) > 0:
        save_df(new_df, new_key, local)

if __name__ == "__main__":
    sync_local_with_s3()
    execute(date_to_str(today()), local=True)
    #upload_to_s3(inventory_key, data_dir)
    #upload_to_s3(date_str(today()), data_dir)
    #upload_to_s3('price_history', data_dir)
