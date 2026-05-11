docker compose up -d

MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic ecom.order_created.v1 \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic ecom.payment_confirmed.v1 \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic ecom.order_delivered.v1 \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic ecom.invalid_events.v1 \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092

MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic ecom.order_created.v1 \
  --bootstrap-server localhost:9092

python -m src.consumers.raw_writer_consumer

python -m src.consumers.invalid_events_consumer

python -m src.consumers.metrics_consumer

python -m src.producers.historical_replay_producer

## Kiểm tra message bằng CLI

### Consume order_created từ đầu
MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic ecom.order_created.v1 \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | "

### Consume invalid events
MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic ecom.invalid_events.v1 \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | "

## Kiểm tra consumer lag

### Kiểm tra lag của metrics-group
MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group metrics-group

## Cấu hình retention

### đặt retention 1 ngày cho ecom.order_created.v1
MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name ecom.order_created.v1 \
  --alter \
  --add-config retention.ms=86400000

### Xem lại config
MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name ecom.order_created.v1 \
  --describe
