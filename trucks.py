import random
import requests
import json
import time
from datetime import datetime

# API URL to trigger the Lambda function
url = "your_API_URL"

# Generating random data
truck_ids = ["TRK001", "TRK002", "TRK003"]
odometer_reading = [10235.6, 11000.9, 13656.4]
lat = [34 + round(random.random(), 6), 25 + round(random.random(), 6), 44 + round(random.random(), 6)]
lon = [100 + round(random.random(), 6), -118 + round(random.random(), 6), 128 + round(random.random(), 6)]
counter = 0
while True:
    data = {"trucks" : list()}
    tire_pressure = [i for i in range(30,41)]
    for truck in range(len(truck_ids)):
        speed = float(random.choice([i for i in range(50, 70, 5)]))
        truck_data = {
            "Truck_ID": truck_ids[truck],
            "gps_location": {
                "latitude": lat[truck] + counter,
                "longitude": lon[truck] + counter,
                "altitude": float(random.choice([i for i in range(85, 91)])),
                "speed": speed
                },
            "vehicle_speed": speed,
            "engine_diagnostics": {
                "engine_rpm": random.choice([i for i in range(2000, 3500, 100)]),
                "fuel_level": float(random.choice([i for i in range(25, 105, 5)])),
                "temperature": float(random.choice([i for i in range(85, 100, 5)])),
                "oil_pressure": float(random.choice([i for i in range(30, 50, 5)])),
                "battery_voltage": float(random.choice([i for i in range(10, 25, 5)]))
                },
            "odometer_reading": odometer_reading[truck] + counter,
            "fuel_consumption": float(random.choice([i for i in range(10, 25, 5)])),
            "vehicle_health_and_maintenance": {
                "brake_status": random.choice(["Good", "Operational", "Needs Inspection"]),
                "tire_pressure": {
                    "front_left": float(random.choice(tire_pressure)),
                    "front_right": float(random.choice(tire_pressure)),
                    "rear_left": float(random.choice(tire_pressure)),
                    "rear_right": float(random.choice(tire_pressure))
                    },
                "transmission_status": random.choice(["Good", "Operational", "Needs Inspection"])
                },
            "environmental_conditions": {
                "temperature": float(random.choice([i for i in range(20, 30)])),
                "humidity": float(random.choice([i for i in range(50, 61)])),
                "atmospheric_pressure": random.choice([1013.25, 1012.00, 1012.25, 1012.50, 1012.75, 1013.00, 1013.75])
                },
            "Effective_Date" : datetime.now().isoformat(),
            "Expiration_Date" : None,
            "is_active" : True
        }
        data["trucks"].append(truck_data)

    headers = {'Content-Type': 'application/json'}
    
    # Posting the data to the API
    response = requests.post(url, data = json.dumps(data))
    if response.status_code == 200:
        print(response.json())
        counter += 1
        time.sleep(60)
    else:
        print(f"Failed to send data. Status code: {response.status_code}")
        break