import json
import os

from kafka import KafkaConsumer

from src.common.config import (
    BOOTSTRAP_SERVERS,
    RAW_EVENTS_OUTPUT_DIR,
    TOPIC_ORDER_CREATED,
    TOPIC_PAYMENT_CONFIRMED,
    TOPIC_ORDER_DELIVERED,
)


TOPICS = [
    TOPIC_ORDER_CREATED,
    TOPIC_PAYMENT_CONFIRMED,
    TOPIC_ORDER_DELIVERED,
]


def get_output_path(topic_name: str) -> str:
    filename_map = {
        TOPIC_ORDER_CREATED: "order_created.jsonl",
        TOPIC_PAYMENT_CONFIRMED: "payment_confirmed.jsonl",
        TOPIC_ORDER_DELIVERED: "order_delivered.jsonl",
    }

    return os.path.join(RAW_EVENTS_OUTPUT_DIR, filename_map[topic_name])


def main() -> None:
    os.makedirs(RAW_EVENTS_OUTPUT_DIR, exist_ok=True)

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="raw-writer-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print("raw_writer_consumer is listening...")

    for message in consumer:
        record = {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
            "key": message.key,
            "value": message.value,
        }

        output_path = get_output_path(message.topic)

        with open(output_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            f.flush()

        print(f"[RAW] {record}")


if __name__ == "__main__":
    main()