# W6D4 – Event-Driven Architecture (RabbitMQ + Kafka/Redpanda)

Minimal, working examples for class demos + breakout guides.

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate            # Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```text
#### RabbitMQ (Docker)
```bash
docker run -it --rm -p 5672:5672 -p 15672:15672 rabbitmq:3-management
# UI: http://localhost:15672  (guest/guest)
```text
#### Redpanda (Kafka API compatible)
```bash
docker run -it --rm -p 9092:9092 -p 9644:9644   docker.redpanda.com/redpandadata/redpanda:latest   redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M   --node-id 0 --check=false --kafka-addr 0.0.0.0:9092
```text
## Demos

#### RabbitMQ
```bash
python live-coding/01_rabbitmq_queue.py produce    # Terminal A
python live-coding/01_rabbitmq_queue.py            # Terminal B (consumer)
```text
#### Kafka/Redpanda
```bash
python live-coding/02_kafka_partitions.py produce
python live-coding/02_kafka_partitions.py C1
python live-coding/02_kafka_partitions.py C2
```text
## Breakouts

- `breakouts/01_backpressure_dlq.md` — Prefetch, Retry, DLQ, Idempotency (RabbitMQ).
- `breakouts/02_partitions_hotkeys.md` — Partitions, Consumer Groups, Hot Keys (Kafka).

## Repo Layout

```text
aise26-w6d4-classrooms/
├─ requirements.txt
├─ README.md
├─ lesson-materials/
│  ├─ pubsub-vs-queue.png
│  └─ consumer-group.png
├─ live-coding/
│  ├─ 01_rabbitmq_queue.py           # RabbitMQ demo (prefetch, retry, DLQ)
│  ├─ 02_kafka_partitions.py         # Kafka demo (partitions, groups, ordering)
│  └─ 03_inmemory_bus.py             # Brokerless fallback
└─ breakouts/
   ├─ 01_backpressure_dlq.md
   └─ 02_partitions_hotkeys.md
```text
