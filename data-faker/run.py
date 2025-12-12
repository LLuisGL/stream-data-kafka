import json
import random
import time
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

CATEGORIES = ["Tech", "Home", "Toys", "Clothes", "Books"]
EVENT_TYPES = ["view", "add_to_cart", "purchase"]
STORE_TYPES = ["physical_store", "online_store"]
PROVINCES = ["Guayas", "Pichincha", "Azuay", "Manabí"]
CITIES = ["Guayaquil", "Quito", "Cuenca", "Manta"]

def generate_random_event():
    city = random.choice(CITIES)
    province = PROVINCES[CITIES.index(city)] if city in CITIES[:3] else random.choice(PROVINCES)

    return {
        "user_id": random.randint(1, 1000),
        "product_id": random.randint(1, 1000),
        "category": random.choice(CATEGORIES),
        "event_type": random.choice(EVENT_TYPES),
        "price": round(random.uniform(5.00, 1500.00), 2),
        "timestamp": datetime.now().isoformat() + "Z",
        "store_id": f"ST{str(random.randint(1, 99)).zfill(3)}",
        "store_type": random.choice(STORE_TYPES),
        "city": city,
        "province": province,
        "location": {
            "lat": float(fake.latitude()),
            "lon": float(fake.longitude())
        }
    }

topic_name = "stream_in"

try:
    while True:
        event = generate_random_event()
        producer.send(topic_name, event)
        print("Enviado:", event)
        time.sleep(1)
except KeyboardInterrupt:
    print("Detenido por el usuario")