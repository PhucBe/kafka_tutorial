import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer

from schemas import validate_event


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "orders_events"

ORDERS_PATH = "data/olist_orders_dataset.csv"
PAYMENTS_PATH = "data/olist_order_payments_dataset.csv"

MAX_EVENTS_PER_TYPE = 10
SLEEP_SECONDS = 0.5


def build_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False
        ).encode("utf-8"),
    )


def load_olist_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    orders = pd.read_csv(ORDERS_PATH)
    payments = pd.read_csv(PAYMENTS_PATH)

    return orders, payments


def build_order_created_events(orders: pd.DataFrame) -> list[dict]:
    subset = (
        orders[
            [
                "order_id",
                "customer_id",
                "order_purchase_timestamp",
            ]
        ]
        .dropna()
        .head(MAX_EVENTS_PER_TYPE)
    )

    events = []

    for _, row in subset.iterrows():
        event = {
            "order_id": str(row["order_id"]),
            "customer_id": str(row["customer_id"]),
            "event_type": "order_created",
            "event_time": str(row["order_purchase_timestamp"]),
        }

        events.append(event)

    return events


def build_payment_confirmed_events(payments: pd.DataFrame) -> list[dict]:
    subset = (
        payments[
            [
                "order_id",
                "payment_sequential",
                "payment_value",
            ]
        ]
        .dropna()
        .head(MAX_EVENTS_PER_TYPE)
    )

    events = []

    for _, row in subset.iterrows():
        order_id = str(row["order_id"])
        payment_sequential = int(row["payment_sequential"])

        event = {
            "order_id": order_id,
            "payment_id": f"{order_id}_{payment_sequential}",
            "event_type": "payment_confirmed",
            "event_time": datetime.now(timezone.utc).isoformat(),
            "payment_value": float(row["payment_value"]),
        }

        events.append(event)

    return events


def build_order_delivered_events(orders: pd.DataFrame) -> list[dict]:
    subset = (
        orders[
            [
                "order_id",
                "order_delivered_customer_date",
            ]
        ]
        .dropna()
        .head(MAX_EVENTS_PER_TYPE)
    )

    events = []

    for _, row in subset.iterrows():
        event = {
            "order_id": str(row["order_id"]),
            "event_type": "order_delivered",
            "event_time": datetime.now(timezone.utc).isoformat(),
            "delivered_customer_date": str(row["order_delivered_customer_date"]),
        }

        events.append(event)

    return events


def main() -> None:
    producer = build_kafka_producer()
    orders, payments = load_olist_data()

    all_events = (
        build_order_created_events(orders)
        + build_payment_confirmed_events(payments)
        + build_order_delivered_events(orders)
    )

    print(f"[GENERATOR] total_events={len(all_events)}")

    for event in all_events:
        is_valid, reason = validate_event(event)

        if not is_valid:
            print(f"[GENERATOR][SKIP_INVALID] reason={reason}, event={event}")
            continue

        order_id = event["order_id"]

        producer.send(
            TOPIC_NAME,
            key=order_id,
            value=event,
        )

        print(
            f"[GENERATOR] sent "
            f"topic={TOPIC_NAME}, "
            f"key={order_id}, "
            f"event_type={event['event_type']}, "
            f"value={event}"
        )

        time.sleep(SLEEP_SECONDS)

    producer.flush()
    producer.close()

    print("[GENERATOR] finished.")


if __name__ == "__main__":
    main()