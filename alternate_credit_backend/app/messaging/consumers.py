from kafka import KafkaConsumer
import json
from app.messaging.kafka_config import KAFKA_BROKER  # Fixed: was bare import


class JSONKafkaConsumer:
    def __init__(self, topic, group_id):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )

    def __iter__(self):
        """
        Makes JSONKafkaConsumer directly iterable.
        All services use:  for message in consumer: ...
        message.value is already deserialized to a dict by value_deserializer.
        """
        return iter(self.consumer)

    def __next__(self):
        return next(self.consumer)

    def listen(self, callback):
        """
        Original callback-style interface — kept for backwards compatibility.
        """
        print("[👂] Listening to topic...")
        for message in self.consumer:
            data = message.value
            print(f"[📥] Received: {data}")
            try:
                callback(data)
            except Exception as e:
                print(f"[✘] Error processing message: {e}")

    def close(self):
        self.consumer.close()