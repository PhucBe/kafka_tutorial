MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic orders_events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic orders_events \
  --bootstrap-server localhost:9092

MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092

python app/consumer_to_file.py

python app/consumer_metrics.py

python app/event_generator.py

cat data/sample_events_output/raw_orders_events.jsonl

cat data/sample_events_output/order_metrics_per_minute.csv

MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group raw-writer-group

MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group metrics-group

python app/consumer_manual_commit_demo.py

MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group metrics-group-manual-demo