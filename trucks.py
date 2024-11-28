import random
import requests
import json
import time
from datetime import datetime

# API URL to trigger the Lambda function
url = "https://qcmijf7q5k.execute-api.us-east-1.amazonaws.com/mystage"

# Generating random data
truck_ids = ["TRK001", "TRK002", "TRK003"]
odometer_reading = [10235.6, 11000.9, 13656.4]
lat = [34 + round(random.random(), 6), 25 + round(random.random(), 6), 44 + round(random.random(), 6)]
lon = [100 + round(random.random(), 6), -118 + round(random.random(), 6), 128 + round(random.random(), 6)]
counter = 0

while True:
    data = {"trucks": []}
    tire_pressure = [i for i in range(30, 41)]

    for truck in range(len(truck_ids)):
        speed = float(random.choice([i for i in range(50, 70, 5)]))
        lat[truck] += random.uniform(-0.001, 0.001)
        lon[truck] += random.uniform(-0.001, 0.001)

        truck_data = {
            "Truck_ID": truck_ids[truck],
            "gps_location": {
                "latitude": lat[truck],
                "longitude": lon[truck],
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
                "atmospheric_pressure": random.choice(
                    [1013.25, 1012.00, 1012.25, 1012.50, 1012.75, 1013.00, 1013.75])
            },
            "Effective_Date": datetime.now().isoformat(),
            "Expiration_Date": None,
            "is_active": True
        }
        data["trucks"].append(truck_data)

    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, data=json.dumps(data), headers=headers)
        response.raise_for_status()

        print("Successful:", response.json())
        counter += 1
        time.sleep(60)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        print(f"Payload: {json.dumps(data, indent=2)}")
        break
