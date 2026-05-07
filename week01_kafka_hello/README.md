# Week 01 - Kafka Hello World Streaming Pipeline

## Objective

Build a simple local streaming pipeline using Apache Kafka.

This project demonstrates how to produce JSON order events to Kafka and consume them using Python.

---

## Architecture

```text
Python Producer
    -> Kafka Topic: orders
        -> Python Consumer
            -> Console Output
            -> consumer_output.log
```

---

## Tech Stack

- Apache Kafka 4.1.1
- Kafka UI
- Docker Compose
- Python
- kafka-python

---

## Project Structure

```text
week01_kafka_hello/
├─ docker-compose.yml
├─ app/
│  ├─ producer_orders.py
│  ├─ consumer_orders.py
│  └─ requirements.txt
├─ consumer_output.log
└─ README.md
```

---

## Kafka Connection Notes

This project uses the following Kafka connection setup:

| Component | Bootstrap Server |
|---|---|
| Python Producer/Consumer on host machine | `localhost:9092` |
| Kafka UI inside Docker network | `kafka:19092` |
| Kafka CLI using `docker exec` | `localhost:9092` |

Kafka UI is available at:

```text
http://localhost:8080
```

---

## How to Run

### 1. Start Kafka

```bash
docker compose up -d
```

Check containers:

```bash
docker compose ps
```

Expected containers:

```text
kafka
kafka-ui
```

---

### 2. Create Kafka topics

Create topic `orders`:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

Create topic `payments`:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic payments \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

List topics:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

Expected output:

```text
orders
payments
```

> If you use Git Bash on Windows and get path errors, add `MSYS_NO_PATHCONV=1` before the command.

Example:

```bash
MSYS_NO_PATHCONV=1 docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

---

### 3. Install Python dependencies

```bash
pip install -r app/requirements.txt
```

---

### 4. Run consumer

Open terminal 1:

```bash
python app/consumer_orders.py
```

The consumer will listen to topic `orders`.

---

### 5. Run producer

Open terminal 2:

```bash
python app/producer_orders.py
```

The producer will send JSON order events to Kafka.

---

## Expected Output

Producer output:

```text
[PRODUCER] Sent event topic=orders, partition=0, offset=0, value={...}
[PRODUCER] Sent event topic=orders, partition=0, offset=1, value={...}
```

Consumer output:

```text
[CONSUMER] topic=orders, partition=0, offset=0, value={'order_id': 'O1001', ...}
[CONSUMER] topic=orders, partition=0, offset=1, value={'order_id': 'O1002', ...}
```

The consumer also writes processed messages to:

```text
consumer_output.log
```

---

## View Messages in Kafka UI

Open:

```text
http://localhost:8080
```

Check:

```text
Topics -> orders -> Messages
Topics -> orders -> Partitions
Topics -> orders -> Offsets
```

You should see:

- topic name: `orders`
- partition: `0`
- offsets increasing from `0`, `1`, `2`, ...
- JSON order events

---

## Learning Notes

Kafka is used here as an event streaming platform.

Kafka is not a database and not an analytics warehouse. It is mainly used to move events between systems in near real-time.

In this project:

```text
Producer = application that sends events
Topic = named stream of events
Partition = physical split of a topic
Offset = position of a message inside a partition
Consumer = application that reads events
Consumer group = group of consumers that share reading work
```

---

## What I Learned

- How to run Kafka locally with Docker Compose
- How to create Kafka topics using CLI
- How to produce messages using CLI
- How to consume messages using CLI
- How to write a Python Kafka Producer
- How to write a Python Kafka Consumer
- How to inspect Kafka messages in Kafka UI
- How offsets increase when new messages are produced