import boto3
import json
import random
import time
from datetime import datetime

# Creating a session for Kinesis
session = boto3.Session(
    aws_access_key_id='your_access_key_id',
    aws_secret_access_key='your_secret_key',
    region_name='your_region'
)

client = session.client('kinesis')

# Sample data to stimulate live stream
products = [
    {"Item_ID" : 'C1',
    "Item_Name" : "Canon EOS R5"},
    {"Item_ID" : 'C2',
    "Item_Name" : "Nikon Z7 II"},
    {"Item_ID" : 'M1',
    "Item_Name" : "Apple IPhone 14 Pro"},
    {"Item_ID" : 'M2',
    "Item_Name" : "Samsung Galaxy S23 Ultra"},
    {"Item_ID" : 'L1',
    "Item_Name" : "Dell XPS 13"}, 
    {"Item_ID" : 'L2',
    "Item_Name" : "Apple MacBook Air M2 2022"}
]

# Adding click counts and timestamp to data
while True:
    for product in products:
        Click_Counts = random.randint(10,100)
        payload = {
            "Item_ID" : product["Item_ID"],
            "Item_Name" : product["Item_Name"],
            "Click_Counts" : Click_Counts,
            "Timestamp": datetime.now().isoformat()
        }
        
        # streaming the data into kinesis
        response = client.put_record(
            StreamName = 'Kinesis_stream_name',
            StreamARN = 'Kinesis_stream_ARN',
            PartitionKey = 'Item_ID',
            Data = json.dumps(payload).encode('utf-8')
        )
        
        print(f'Sent data to Kinesis: {payload}')
