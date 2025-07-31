from confluent_kafka import Producer
from faker import Faker
import uuid
import json
import random
import time

fake = Faker()

def get_kafka_config():
    return {
        "bootstrap.servers": "pkc-56d1g.eastus.azure.confluent.cloud:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "SFR6JQYEHI35Z6ZN",
        "sasl.password": "6q36a7psSxmOZv0PjawlecPzyVeVNjdpBY4Nq1k6AEejE7epBhE1Gd2YoblTnxyQ",
        "topic": "customers"
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Message delivery failed: {err}")
    else:
        print(f"[SUCCESS] Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def generate_random_customer():
    return {
        "customer_id": str(random.randint(1000000000, 9999999999)),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "gender": random.choice(["Male", "Female", "Other"]),
        "date_of_birth": str(fake.date_of_birth(minimum_age=18, maximum_age=70)),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "address_line1": fake.street_address(),
        "address_line2": fake.secondary_address(),
        "city": fake.city(),
        "state": fake.state(),
        "postal_code": fake.postcode(),
        "country": fake.country(),
        "national_id_number": fake.bothify(text='????####'),  # e.g., PAN/Aadhar mock
        "id_type": random.choice(["Aadhar", "PAN", "Passport"]),
        "kyc_verified": random.choice([True, False]),
        "registration_date": str(fake.date_time_this_decade()),
        "employment_status": random.choice(["Employed", "Self-employed", "Unemployed", "Student", "Retired"]),
        "annual_income": round(random.uniform(200000, 2500000), 2),
        "credit_score": random.randint(300, 900),
        "preferred_language": random.choice(["English", "Hindi", "Tamil", "Telugu", "Bengali"]),
        "is_active": random.choice([True, False])
    }

def produce_customers(n=10, delay=1):
    config = get_kafka_config()
    topic = config.pop("topic")

    producer = Producer(config)

    print(f"[INFO] Producing {n} random customers to topic: {topic}")

    for i in range(n):
        customer = generate_random_customer()
        producer.produce(topic, json.dumps(customer).encode("utf-8"), callback=delivery_report)
        producer.poll(0)
        time.sleep(delay)  # To simulate stream-like behavior

    producer.flush()
    print("[INFO] All messages sent.")

if __name__ == "__main__":
    produce_customers(n=100, delay=10)  # Change n and delay as needed
