import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


TOPIC_NAME = "orders"

# Vì Python chạy trên máy thật nên dùng localhost:9092.
# Nếu sau này chạy Python trong Docker container thì đổi thành kafka:19092.
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=3,
    )


def build_order_event(order_number: int) -> dict:
    return {
        "order_id": f"O{1000 + order_number}",
        "customer_id": f"C{random.randint(1, 5):03d}",
        "event_type": "order_created",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "amount": round(random.uniform(20, 300), 2),
        "currency": "USD",
    }


def main() -> None:
    producer = build_producer()

    print(f"[PRODUCER] Connecting to Kafka at {BOOTSTRAP_SERVERS}")
    print(f"[PRODUCER] Sending events to topic '{TOPIC_NAME}'")

    for i in range(1, 11):
        event = build_order_event(i)

        metadata = producer.send(TOPIC_NAME, value=event).get(timeout=10)

        print(
            "[PRODUCER] Sent event "
            f"topic={metadata.topic}, "
            f"partition={metadata.partition}, "
            f"offset={metadata.offset}, "
            f"value={event}"
        )

        time.sleep(1)

    producer.flush()
    producer.close()

    print("[PRODUCER] Finished sending order events.")


if __name__ == "__main__":
    main()