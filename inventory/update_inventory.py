from io import StringIO

import boto3
import datetime
import json
import pandas as pd
import re

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

    json_data = json.loads(data)
    for ap in json_data[key]:
        id.append(ap['id'])

        for k, v in characteristics.items():
            if v in ap:
                columns[k].append(converter(k, ap[v]))
            else:
                columns[k].append(None)

    df = pd.DataFrame(columns, index=[id])
    return df

def upload_to_s3(file_path, key):
    data = open(file_path, 'rb')
    s3.put_object(Bucket='andrei-housing-prices', Key=key, Body=data)

def lambda_handler(event, context):
    s3_event = event['Records'][0]['Sns']['Message']
    s3_json = json.loads(s3_event)
    key = s3_json['Records'][0]['s3']['object']['key']

    object = s3.get_object(Bucket='andrei-housing-prices', Key=key)
    data = object['Body'].read().decode('utf-8')
    today_df = create_inventory_df(data, key)

    object = s3.get_object(Bucket='andrei-housing-prices', Key=inventory_key)
    data = object['Body'].read().decode('utf-8')
    data_io = StringIO(data)
    alltime_df = pd.read_csv(data_io, index_col=0, sep='\t')

    new_df = pd.DataFrame(columns=alltime_df.columns)

    alltime_df['active'] = False
    for index, row in today_df.iterrows():
        if index[0] not in alltime_df.index:
            new_df.loc[index[0]] = row

        alltime_df.loc[index[0]] = row
        alltime_df.loc[index[0], 'active'] = True

    file_path = '/tmp/' + inventory_key
    alltime_df.to_csv(file_path, sep='\t')
    upload_to_s3(file_path, inventory_key)

    if len(new_df.index) > 0:
        file_path = '/tmp/' + new_key
        new_df.to_csv(file_path, sep='\t')
        upload_to_s3(file_path, new_key)

def process_daily_files(start, end, f):
    for i in range((end - start).days + 1):
        f((start + datetime.timedelta(days=i)).strftime('%Y%m%d'), inventory_key)
        #break

def update_df(current_key, inventory_key):
    print("Updating for " + current_key)

    object = s3.get_object(Bucket='andrei-housing-prices', Key=current_key)
    current_data = object['Body'].read().decode('utf-8')
    #current_data = open(current_key, "r")
    #current_df = create_inventory_df(current_data.read(), current_key)

    try:
        object = s3.get_object(Bucket='andrei-housing-prices', Key=inventory_key)
    except:
        # if this is the first processing ever
        current_df.to_csv(inventory_key, sep='\t')
        upload_to_s3(inventory_key)
        return
    inventory_data = object['Body'].read().decode('utf-8')
    inventory_data_io = StringIO(inventory_data)
    #inventory_data_io = inventory_key
    inventory_df = pd.read_csv(inventory_data_io, index_col=0, sep='\t')

    new_df = pd.DataFrame(columns=inventory_df.columns)

    inventory_df['active'] = False
    for index, row in current_df.iterrows():
        if index[0] not in inventory_df.index:
            new_df.loc[index[0]] = row
            #continue
        inventory_df.loc[index[0]] = row
        inventory_df.loc[index[0], 'active'] = True
        #break
    
    inventory_df.to_csv(inventory_key, sep='\t')
    new_df.to_csv(new_key, sep='\t')

if __name__ == "__main__":
    process_daily_files(datetime.datetime(2020, 2, 9), datetime.datetime.now(), update_df)
    #upload_to_s3('20200208', '20200208')
