import json
import boto3
from datetime import datetime
import base64

# Creating a DynamoDB connection
dynamodb = boto3.resource('dynamodb')
table_name = 'my_table'
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    for record in event['Records']:
        # Data from kinesis are base64 encoded. Decoding the data before loading it into DynamoDB
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        payload = json.loads(payload)

        item = {
            "tstamp": payload["Timestamp"],
            "Item_ID": payload["Item_ID"],
            "Item_Name": payload["Item_Name"],
            "Click_Counts": payload["Click_Counts"]
        }

        # Inserting the item into DynamoDB
        table.put_item(Item=item)

    return {
        'statusCode': 200,
        'body': json.dumps('Data inserted successfully')
    }