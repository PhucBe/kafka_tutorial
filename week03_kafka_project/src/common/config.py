BOOTSTRAP_SERVERS = "localhost:9092"

TOPIC_ORDER_CREATED = "ecom.order_created.v1"
TOPIC_PAYMENT_CONFIRMED = "ecom.payment_confirmed.v1"
TOPIC_ORDER_DELIVERED = "ecom.order_delivered.v1"
TOPIC_INVALID_EVENTS = "ecom.invalid_events.v1"

ORDERS_CSV_PATH = "data/olist_orders_dataset.csv"
PAYMENTS_CSV_PATH = "data/olist_order_payments_dataset.csv"

RAW_EVENTS_OUTPUT_DIR = "data/output/raw_events"
METRICS_OUTPUT_DIR = "data/output/metrics"