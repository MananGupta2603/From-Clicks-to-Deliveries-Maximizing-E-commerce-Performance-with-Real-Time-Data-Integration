import json
import boto3
from decimal import Decimal
from datetime import datetime

# Initialize the DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table_name = 'Trucks_data'  # Ensure this matches your table name
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    try:
        # Log the received event
        print("Received Event:", event)
        
        # Convert float types in the event data to Decimal
        trucks_data = json.loads(json.dumps(event.get('trucks', [])), parse_float=Decimal)
        
        for truck in trucks_data:
            truck_id = truck.get('Truck_ID')
            effective_date = truck.get('Effective_Date', datetime.now().isoformat())
            
            # Log each truck's data
            print("Processing Truck:", truck_id)
            
            # Deactivate old records for this truck
            deactivate_old_records(truck_id, effective_date)
            
            # Insert new record into the table
            table.put_item(
                Item={
                    'Truck_ID': truck_id,
                    'Effective_Date': effective_date,
                    'Expiration_Date': truck.get('Expiration_Date'),
                    'is_active': truck.get('is_active', True),
                    'gps_location': truck.get('gps_location', {}),
                    'vehicle_speed': truck.get('vehicle_speed'),
                    'engine_diagnostics': truck.get('engine_diagnostics', {}),
                    'odometer_reading': truck.get('odometer_reading'),
                    'fuel_consumption': truck.get('fuel_consumption'),
                    'vehicle_health_and_maintenance': truck.get('vehicle_health_and_maintenance', {}),
                    'environmental_conditions': truck.get('environmental_conditions', {})
                }
            )
            print(f"Successfully added data for Truck_ID: {truck_id}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Data stored successfully'})
        }
    except Exception as e:
        print(f"Error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'An error occurred: {str(e)}'})
        }

def deactivate_old_records(truck_id, new_effective_date):
    """
    Mark the previous active record for the given truck_id as inactive.
    """
    try:
        print(f"Deactivating old records for Truck_ID: {truck_id}")
        
        # Scan for active records for this Truck_ID
        response = table.scan(
            FilterExpression='Truck_ID = :truck_id AND is_active = :active',
            ExpressionAttributeValues={
                ':truck_id': truck_id,
                ':active': True
            }
        )
        
        items = response.get('Items', [])
        print(f"Found {len(items)} active records for Truck_ID: {truck_id}")
        
        # Update each active record to set is_active to False and Expiration_Date
        for item in items:
            table.update_item(
                Key={
                    'Truck_ID': item['Truck_ID'],
                    'Effective_Date': item['Effective_Date']
                },
                UpdateExpression='SET is_active = :inactive, Expiration_Date = :expiration_date',
                ExpressionAttributeValues={
                    ':inactive': False,
                    ':expiration_date': new_effective_date
                }
            )
            print(f"Deactivated record for Truck_ID: {truck_id}")
    except Exception as e:
        print(f"Error while deactivating records for Truck_ID {truck_id}: {e}")
        raise
