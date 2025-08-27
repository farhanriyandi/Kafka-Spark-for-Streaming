# Generate random events data
import datetime
import time
import uuid
import random
import json

my_devices = ["dev-101", "dev-102", "dev-103"]
cities = [("Indonesia", "Jakarta"), ("Indonesia", "Bandung"), ("Singapore", "Singapore")]


# Generate event data from devices
def generate_weather():
    devices = random.choice(my_devices)
    country, city = random.choice(cities)
    _weather_ = {
        "device_id": devices,
        "location": {
            "country": country,
            "city": city
        },
        "metrics": {
            "temperature": round(random.uniform(20, 35), 2),
            "humidity": random.randint(40, 90)
        },
        "timestamp": datetime.datetime.utcnow().isoformat()
    }

    return json.dumps(_weather_)

if __name__ == "__main__":
    while True:
        print(generate_weather())
        time.sleep(random.randint(0, 5))