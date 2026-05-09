import json
import os
from collections import defaultdict
from datetime import datetime

from kafka import KafkaConsumer

from schemas import validate_event


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "orders_events"
GROUP_ID = "metrics-group-latest-test"

OUTPUT_DIR = "data/sample_events_output"
OUTPUT_PATH = f"{OUTPUT_DIR}/order_metrics_per_minute.csv"


def normalize_to_minute(timestamp_value: str) -> str:
    try:
        normalized_value = str(timestamp_value).replace("Z", "")
        dt = datetime.fromisoformat(normalized_value)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "unknown"


def write_metrics_to_csv(metrics: dict[str, int]) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as output_file:
        output_file.write("minute_bucket,order_count\n")

        for minute_bucket, count in sorted(metrics.items()):
            output_file.write(f"{minute_bucket},{count}\n")


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def main() -> None:
    metrics = defaultdict(int)
    consumer = build_consumer()

    print(
        f"[METRICS] listening "
        f"topic={TOPIC_NAME}, "
        f"group_id={GROUP_ID}"
    )

    for message in consumer:
        payload = message.value
        is_valid, reason = validate_event(payload)

        if not is_valid:
            print(
                f"[METRICS][SKIP_INVALID] "
                f"partition={message.partition}, "
                f"offset={message.offset}, "
                f"reason={reason}, "
                f"payload={payload}"
            )
            continue

        if payload.get("event_type") != "order_created":
            continue

        minute_bucket = normalize_to_minute(payload.get("event_time"))
        metrics[minute_bucket] += 1

        write_metrics_to_csv(metrics)

        print(
            f"[METRICS] "
            f"partition={message.partition}, "
            f"offset={message.offset}, "
            f"key={message.key}, "
            f"minute={minute_bucket}, "
            f"order_count={metrics[minute_bucket]}"
        )


if __name__ == "__main__":
    main()