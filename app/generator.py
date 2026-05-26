from faker import Faker
from random import uniform, choice
from datetime import datetime
import uuid

fake = Faker()

DELIVERY_STATUS = [
    "in_transit",
    "arriving",
    "delivered",
    "delayed"
]

def generate_gps_data():
    return {
        "vehicle_id": f"VEH-{uuid.uuid4().hex[:6]}",
        "delivery_id": f"DEL-{uuid.uuid4().hex[:8]}",
        "driver_name": fake.name(),
        "latitude": round(uniform(6.4500, 6.7000), 6),   # Lagos range
        "longitude": round(uniform(3.2000, 3.5000), 6),
        "speed": round(uniform(20, 100), 2),
        "status": choice(DELIVERY_STATUS),
        "timestamp": datetime.utcnow().isoformat()
    }