import os
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
PRODUCT_TOPIC = os.getenv("PRODUCT_TOPIC")

consumer = KafkaConsumer(
    PRODUCT_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

if __name__ == "__main__":
    for message in consumer:
        print(f"Consumed: {message.value.decode("utf-8")}")