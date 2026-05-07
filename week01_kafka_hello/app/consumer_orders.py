import json
import os
from pathlib import Path

from kafka import KafkaConsumer


TOPIC_NAME = "orders"

# Vì Python chạy trên máy thật nên dùng localhost:9092.
# Nếu sau này chạy Python trong Docker container thì đổi thành kafka:19092.
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Nếu muốn đọc lại từ đầu trong lần chạy mới, hãy đổi GROUP_ID sang tên mới.
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "orders-consumer-group-v1")

OUTPUT_LOG_PATH = Path(__file__).resolve().parent.parent / "consumer_output.log"


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
    )


def main() -> None:
    consumer = build_consumer()

    print(f"[CONSUMER] Connecting to Kafka at {BOOTSTRAP_SERVERS}")
    print(f"[CONSUMER] Listening to topic '{TOPIC_NAME}'")
    print(f"[CONSUMER] Consumer group: {GROUP_ID}")
    print(f"[CONSUMER] Writing output to: {OUTPUT_LOG_PATH}")

    with open(OUTPUT_LOG_PATH, "a", encoding="utf-8") as log_file:
        for message in consumer:
            log_line = (
                f"topic={message.topic}, "
                f"partition={message.partition}, "
                f"offset={message.offset}, "
                f"value={message.value}"
            )

            print(f"[CONSUMER] {log_line}")
            log_file.write(log_line + "\n")
            log_file.flush()


if __name__ == "__main__":
    main()