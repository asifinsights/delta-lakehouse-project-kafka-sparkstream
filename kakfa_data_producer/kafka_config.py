from confluent_kafka import Producer
import json


def get_kafka_config():
    return {
        "bootstrap.servers": "{your bootstrap server}",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "{your api key}",
        "sasl.password": "{your api secret}",
        "topic": "{}"
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Message delivery failed: {err}")
    else:
        print(f"[SUCCESS] Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def test_kafka_connection():
    config = get_kafka_config()
    topic = config.pop("topic")

    try:
        print("[INFO] Connecting to Kafka...")
        p = Producer(config)
        p.produce(topic, json.dumps({"test": "connection"}).encode("utf-8"), callback=delivery_report)
        p.flush()
    except Exception as e:
        print(f"[ERROR] {e}")


if __name__ == "__main__":
    test_kafka_connection()
