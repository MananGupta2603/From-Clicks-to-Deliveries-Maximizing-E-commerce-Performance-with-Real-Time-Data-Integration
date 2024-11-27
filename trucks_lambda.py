import json
import boto3
from datetime import datetime
from decimal import Decimal

def lambda_handler(event, context):
    if 'body' in event:
        # getting data from the payload and parsing the float values as Decimal
        body = json.loads(event['body'], parse_float=Decimal)
        
        # getting the expiration dates for the old records
        Expiration_date = dict()
        for truck in body["trucks"]:
            Expiration_date[truck["Truck_ID"]] = truck["Effective_Date"]

        # Creating a DynamoDB connection : 
        dynamodb = boto3.resource('dynamodb')
        table_name = 'Trucks_data'
        table = dynamodb.Table(table_name)
        
        truck_ids = ["TRK001", "TRK002", "TRK003"]

        for truck in truck_ids:
            # Querying the table to get the existing data
            response = table.query(
                    KeyConditionExpression="Truck_ID = :truck_id",
                    FilterExpression="is_active = :active",
                    ExpressionAttributeValues={
                        ':truck_id': truck,
                        ':active': True
                    })
            
            # Updating the existing data to set Expiration date and is_active as False    
            if response['Items']:
                current_record = response['Items'][0]
                table.update_item(
                    Key={'Truck_ID': truck, 
                    'Effective_Date': current_record["Effective_Date"]},
                    UpdateExpression='SET Expiration_Date = :d, is_active = :a',
                    ExpressionAttributeValues={
                        ':d' : Expiration_date[truck],
                        ':a' : False}
                        )
            
        # Inserting the new values
        for data in body["trucks"]:
            table.put_item(Item = data)
        
        return {
            'statusCode': 200,
            'body': json.dumps({"message":"Data successfully updated"})
        }
