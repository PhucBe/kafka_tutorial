import json
import os

from kafka import KafkaConsumer

from schemas import validate_event


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "orders_events"
GROUP_ID = "raw-writer-group"

OUTPUT_DIR = "data/sample_events_output"
OUTPUT_PATH = f"{OUTPUT_DIR}/raw_orders_events.jsonl"


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    consumer = build_consumer()

    print(
        f"[RAW-WRITER] listening "
        f"topic={TOPIC_NAME}, "
        f"group_id={GROUP_ID}"
    )

    with open(OUTPUT_PATH, "a", encoding="utf-8") as output_file:
        for message in consumer:
            is_valid, reason = validate_event(message.value)

            record = {
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "key": message.key,
                "schema_valid": is_valid,
                "invalid_reason": None if is_valid else reason,
                "value": message.value,
            }

            output_file.write(json.dumps(record, ensure_ascii=False) + "\n")
            output_file.flush()

            print(f"[RAW-WRITER] {record}")


if __name__ == "__main__":
    main()