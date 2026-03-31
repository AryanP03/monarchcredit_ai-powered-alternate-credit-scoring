from kafka import KafkaProducer
import json
from app.messaging.kafka_config import KAFKA_BROKER  # Fixed: was bare import


class JSONKafkaProducer:
    def __init__(self, bootstrap_servers: str = KAFKA_BROKER):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            acks="all"
        )

    def send(self, topic, data):
        try:
            future = self.producer.send(topic, value=data)
            record_metadata = future.get(timeout=10)
            print(f"[✔] Sent to {record_metadata.topic} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")
        except Exception as e:
            print(f"[✘] Error sending message: {e}")

    def close(self):
        self.producer.flush()
        self.producer.close()