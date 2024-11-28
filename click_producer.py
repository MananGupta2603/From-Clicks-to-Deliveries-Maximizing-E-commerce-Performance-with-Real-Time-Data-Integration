import boto3
import json
import random
from datetime import datetime

# Initialize the Kinesis client
kinesis_client = boto3.client('kinesis')

# Sample product data
products = [
    {"Item_ID": 'C1', "Item_Name": "Canon EOS R5"},
    {"Item_ID": 'C2', "Item_Name": "Nikon Z7 II"},
    {"Item_ID": 'M1', "Item_Name": "Apple iPhone 14 Pro"},
    {"Item_ID": 'M2', "Item_Name": "Samsung Galaxy S23 Ultra"},
    {"Item_ID": 'L1', "Item_Name": "Dell XPS 13"},
    {"Item_ID": 'L2', "Item_Name": "Apple MacBook Air M2 2022"},
]

def lambda_handler(event, context):
    try:
        # Generate and send clickstream data for each product
        for product in products:
            click_counts = random.randint(10, 100)
            payload = {
                "Item_ID": product["Item_ID"],
                "Item_Name": product["Item_Name"],
                "Click_Counts": click_counts,
                "Timestamp": datetime.now().isoformat(),
            }

            # Send data to the Kinesis stream
            response = kinesis_client.put_record(
                StreamName='Kinesis_stream_name',  
                PartitionKey=product["Item_ID"], 
                Data=json.dumps(payload)          
            )

            print(f"Sent data to Kinesis: {payload}")
            print(f"Kinesis Response: {response}")

        # Return a success message after processing
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Clickstream data sent to Kinesis"})
        }

    except Exception as e:
        print(f"Error occurred: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Failed to send data", "error": str(e)})
        }
