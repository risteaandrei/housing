from io import StringIO

import boto3
import datetime
import json
import pandas as pd
import re

s3 = boto3.client('s3')
inventory_key = 'inventory'

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

def create_all_time_inventory_df():
    key = "20200106"
    object = s3.get_object(Bucket='andrei-housing-prices', Key=key)
    data = object['Body'].read().decode('utf-8')
    first_df = create_inventory_df(data, key)
    first_df.to_csv(inventory_key, sep='\t')

    alltime_df = pd.read_csv(inventory_key, index_col=0, sep='\t')
    alltime_df['active'] = True

    for i in range(20200107, 20200130):
        key = str(i)
        object = s3.get_object(Bucket='andrei-housing-prices', Key=key)
        data = object['Body'].read().decode('utf-8')
        current_df = create_inventory_df(data, key)

        alltime_df['active'] = False
        for index, row in current_df.iterrows():
            alltime_df.loc[index[0]] = row
            alltime_df.loc[index[0], 'active'] = True

    alltime_df.to_csv(inventory_key, sep='\t')
    data = open(inventory_key, 'rb')
    s3.put_object(Bucket='andrei-housing-prices', Key=inventory_key, Body=data)

def upload_to_s3(file_path):
    data = open(file_path, 'rb')
    s3.put_object(Bucket='andrei-housing-prices', Key=inventory_key, Body=data)

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

    alltime_df['active'] = False
    for index, row in today_df.iterrows():
        alltime_df.loc[index[0]] = row
        alltime_df.loc[index[0], 'active'] = True

    file_path = '/tmp/all_time.csv'
    alltime_df.to_csv(file_path, sep='\t')
    upload_to_s3(file_path)

def process_daily_files(start, end, f):
    for i in range((end - start).days + 1):
        f((start + datetime.timedelta(days=i)).strftime('%Y%m%d'), inventory_key)
        #break

def update_df(current_key, inventory_key):
    upload_to_s3(inventory_key)
    return
    print("Updating for " + current_key)

    object = s3.get_object(Bucket='andrei-housing-prices', Key=current_key)
    current_data = object['Body'].read().decode('utf-8')
    current_df = create_inventory_df(current_data, current_key)

    try:
        object = s3.get_object(Bucket='andrei-housing-prices', Key=inventory_key)
    except:
        current_df.to_csv(inventory_key, sep='\t')
        upload_to_s3(inventory_key)
        return

    inventory_data = object['Body'].read().decode('utf-8')
    inventory_data_io = StringIO(inventory_data)
    inventory_df = pd.read_csv(inventory_data_io, index_col=0, sep='\t')
    inventory_df['neighborhood'] = ''

    inventory_df['active'] = False
    for index, row in current_df.iterrows():
        inventory_df.loc[index[0]] = row
        inventory_df.loc[index[0], 'active'] = True
        #print(inventory_df.loc[index[0]])
        #break
    
    inventory_df.to_csv(inventory_key, sep='\t')
    #upload_to_s3(inventory_key)

if __name__ == "__main__":
    #create_all_time_inventory_df()
    process_daily_files(datetime.datetime(2020, 2, 8), datetime.datetime.now(), update_df)
