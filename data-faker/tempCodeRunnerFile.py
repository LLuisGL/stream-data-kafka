import json
import random
import time
from kafka import KafkaProducer # type: ignore
from faker import Faker # type: ignore
from datetime import datetime

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  
)

TOPIC_MAP = {
    "view": "page_views",
    "add_to_cart": "cart_events",
    "purchase": "orders"
}

CATEGORIES = ["Tech", "Home", "Toys", "Clothes", "Books"]
EVENT_TYPES = ["view", "add_to_cart", "purchase"]
STORE_TYPES = ["physical_store", "online_store"]
PROVINCES = ["Guayas", "Pichincha", "Azuay", "Manabí"]
CITIES = ["Guayaquil", "Quito", "Cuenca", "Manta"]

def generate_random_event():
    city = random.choice(CITIES)
    province = random.choice(PROVINCES)

    event_type = random.choice(EVENT_TYPES)

    return event_type, {
        "user_id": random.randint(1, 1000),
        "product_id": random.randint(1, 1000),
        "category": random.choice(CATEGORIES),
        "event_type": event_type,
        "price": round(random.uniform(5.00, 1500.00), 2),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "store_id": f"ST{str(random.randint(1, 99)).zfill(3)}",
        "store_type": random.choice(STORE_TYPES),
        "city": city,
        "province": province,
        "location": {
            "lat": float(fake.latitude()),
            "lon": float(fake.longitude())
        }
    }

while True:
    event_type, event = generate_random_event()
    topic = TOPIC_MAP[event_type]
    producer.send(topic, value=event)
    print(f"Enviado a {topic}:", event)
    time.sleep(1)