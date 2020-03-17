import boto3
from botocore.exceptions import ClientError
import math

import sys
sys.path.append('tools')
from housing_common import *

SENDER = "Andrei Ristea <andreiionutristea" + "@gm" + "ail." + "com>"
RECIPIENT = "risteaandrei" + "@ya" + "hoo." + "com"
AWS_REGION = "eu-central-1"
SUBJECT = "New appartments today"
CHARSET = "UTF-8"
HEADER_HTML = """<html>
<head></head>

<table>
  <tr>
    <th>Link</th>
    <th>Price</th>
    <th>Surface</th>
    <th>Price/Surface</th>
    <th>Rooms</th>
    <th>Neighborhood</th>
    <th>Partitioning</th>
    <th>Floor</th>
    <th>Year</th>
  </tr>
            """

FOOTER_HTML = """
</table>
</body>
</html>
            """

new_today_key = 'new_today'
client = boto3.client('ses',region_name=AWS_REGION)

def lambda_handler(event, context):
    execute()

def execute(local=False):
    df = load_df(new_today_key, local)

    ROWS_HTML = ""
    for index, row in df.iterrows():
        price_per_surface = '' if math.isnan(row['price']) or math.isnan(row['usable_surface']) \
                            else str(round(row['price'] / row['usable_surface']))
        year = '' if math.isnan(row['year']) \
               else str(round(row['year']))
        ROWS_HTML += '<tr>' \
            + '<td>' + '<a href="' + row['url'] + '">link</a>' + '</td>' \
            + '<td>' + str(row['price']) + '</td>' \
            + '<td>' + str(row['usable_surface']) + '</td>' \
            + '<td>' + price_per_surface + '</td>' \
            + '<td>' + str(row['rooms']) + '</td>' \
            + '<td>' + row['neighborhood'] + '</td>' \
            + '<td>' + row['partitioning'] + '</td>' \
            + '<td>' + str(row['floor']) + ' / ' + str(row['building_height']) + '</td>' \
            + '<td>' + year + '</td>' \
            + '</tr>'

    BODY_HTML = HEADER_HTML + ROWS_HTML + FOOTER_HTML

    try:
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )

    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

if __name__ == "__main__":
    sync_local_with_s3()
    execute(True)
