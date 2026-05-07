Lệnh tạo orders:
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

Lệnh tạo payments:
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic payments \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

Mở producer:
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --topic orders \
  --bootstrap-server localhost:9092

Nhập message mẫu:
{"order_id": "O2001", "customer_id": "C001", "event_type": "order_created", "event_time": "2026-05-05T10:00:00", "amount": 99.5}
{"order_id": "O2002", "customer_id": "C002", "event_type": "order_created", "event_time": "2026-05-05T10:01:00", "amount": 149.0}
{"order_id": "O2003", "customer_id": "C003", "event_type": "order_created", "event_time": "2026-05-05T10:02:00", "amount": 75.25}
{"order_id": "O2004", "customer_id": "C001", "event_type": "order_created", "event_time": "2026-05-05T10:03:00", "amount": 210.0}
{"order_id": "O2005", "customer_id": "C004", "event_type": "order_created", "event_time": "2026-05-05T10:04:00", "amount": 55.9}

Consumer lại từ đầu:
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --from-beginning

Yêu cầu quan sát:
topic orders đọc lại được message đã gửi
message hiển thị đúng JSON

Viết file: app/producer_orders.py
Chạy: python app/producer_orders.py

Viết file: app/consumer_orders.py
Chạy: python app/consumer_orders.py

Thêm topic payments. Gửi payment event vào topic payments.
Mở producer:
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --topic payments \
  --bootstrap-server localhost:9092

Message mẫu:
{"payment_id": "P1001", "order_id": "O1001", "event_type": "payment_success", "event_time": "2026-05-05T10:05:00", "paid_amount": 120.5}
{"payment_id": "P1002", "order_id": "O1002", "event_type": "payment_success", "event_time": "2026-05-05T10:06:00", "paid_amount": 88.0}
{"payment_id": "P1003", "order_id": "O1003", "event_type": "payment_failed", "event_time": "2026-05-05T10:07:00", "paid_amount": 0}

Consumer lại:
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic payments \
  --bootstrap-server localhost:9092 \
  --from-beginning

Tạo topic nhiều partition
Xóa topic cũ:
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --delete \
  --topic orders \
  --bootstrap-server localhost:9092

Tạo lại orders với 2 paritions:
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1

Kiểm tra:
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic orders \
  --bootstrap-server localhost:9092

Gửi 10 events rồi quan sát trên Kafka UI
Topics -> orders -> Messages
Topics -> orders -> Partitions
Topics -> orders -> Offsets