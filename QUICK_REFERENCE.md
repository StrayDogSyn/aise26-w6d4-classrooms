# Quick Reference Guide - Event-Driven Architecture

## 🚀 Quick Start Commands

### RabbitMQ Setup

```bash
# Start RabbitMQ container
docker run -it --rm -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Access management UI: http://localhost:15672 (guest/guest)

# Produce messages
python live-coding/01_rabbitmq_queue.py produce 20

# Consume (defaults: prefetch=5, max_retries=3)
python live-coding/01_rabbitmq_queue.py

# Consume with custom settings
python live-coding/01_rabbitmq_queue.py consume <prefetch> <max_retries>
```text
### Kafka/Redpanda Setup

```bash
# Start Redpanda container
docker run -it --rm -p 9092:9092 docker.redpanda.com/redpandadata/redpanda:latest redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M --node-id 0 --check=false --kafka-addr 0.0.0.0:9092

# Produce messages (normal distribution)
python live-coding/02_kafka_partitions.py produce 100

# Produce with hot key (50% to one key)
python live-coding/02_kafka_partitions.py produce 100 50

# Start consumers
python live-coding/02_kafka_partitions.py C1
python live-coding/02_kafka_partitions.py C2
```text
### In-Memory Bus

```bash
# No setup needed - runs standalone
python live-coding/03_inmemory_bus.py
```text
### Testing

```bash
# Run all idempotency tests
python -m pytest tests/test_idempotency.py -v

# Run specific test
python -m pytest tests/test_idempotency.py::test_ttl_deduper_basic -v
```text
---

## 📋 Key Concepts Cheat Sheet

### RabbitMQ - Back-Pressure & DLQ

| Concept | Description | When to Use |
|---------|-------------|-------------|
| **Prefetch=1** | Fair distribution, low throughput | Variable task duration, need fairness |
| **Prefetch=5** | Balanced (recommended) | Most production use cases |
| **Prefetch=20** | High throughput, hoarding risk | Fast, uniform tasks |
| **max_retries=0** | Immediate DLQ on failure | Validation errors, permanent failures |
| **max_retries=1** | Single retry | Simple network hiccups |
| **max_retries=3** | Multiple retries with backoff | Transient errors, rate limits |

#### Prefetch Formula:
```text
prefetch = (target_throughput * avg_processing_time) / num_consumers
```text
**Example:** 100 msg/sec, 50ms processing, 2 consumers = prefetch of 3-5

---

### Kafka - Partitions & Consumer Groups

| Concept | Description | Impact |
|---------|-------------|--------|
| **Partitions** | Topic split into N partitions | Max N parallel consumers per group |
| **Consumer Group** | Consumers share partitions | Load distribution, scaling |
| **Key-based routing** | Same key → same partition | Ordering guarantee per key |
| **Hot Key** | One key gets >2x avg traffic | Bottleneck on one partition |

#### Partition Count Formula:
```text
partitions = max(
    peak_throughput / consumer_throughput,
    max_consumers_needed
) * 1.5  (buffer)
```text
**Example:** 5000 msg/sec peak, 500 msg/sec per consumer = 15 partitions

---

### Idempotency Strategies

| Strategy | Storage | Speed | Distributed | Persistent | Use Case |
|----------|---------|-------|-------------|------------|----------|
| **In-Memory Set** | RAM | ⚡️ Fast | ❌ No | ❌ No | Testing, single process |
| **Redis TTL** | Redis | ⚡️ Fast | ✅ Yes | ⚠️ Ephemeral | Production, high throughput |
| **Database** | PostgreSQL/MySQL | 🐢 Slower | ✅ Yes | ✅ Yes | Audit trail, compliance |
| **Natural** | N/A | ⚡️ Fastest | ✅ Yes | ✅ Yes | Best (when possible) |

#### Natural Idempotency Examples:
```sql
-- Set operation (naturally idempotent)
UPDATE users SET status = 'active' WHERE id = 123;

-- Insert with conflict handling
INSERT INTO processed (msg_id) VALUES ('abc') 
ON CONFLICT (msg_id) DO NOTHING;

-- Conditional update
UPDATE orders SET status = 'shipped' 
WHERE id = 456 AND status = 'pending';
```text
---

## 🔍 Common Issues & Solutions

### Issue: Consumer not receiving messages

#### RabbitMQ:
```bash
# Check queue has messages
# UI: http://localhost:15672 → Queues → w6d4.tasks

# Check consumer is connected
# Look for "Consuming" message in terminal

# Verify queue name matches
QUEUE = "w6d4.tasks"  # Must match producer
```text
#### Kafka:
```bash
# Check topic exists and has messages
docker exec -it <container> rpk topic describe w6d4.events

# Verify consumer group
# Multiple consumers must use same group_id
```text
### Issue: Messages going to DLQ immediately

#### Check:
- max_retries setting (0 = immediate DLQ)
- Error type (validation errors should DLQ immediately)
- Look for "✗ DLQ" log messages with reasons

#### Fix:
```python
# Increase max_retries for transient errors
consume(prefetch=5, max_retries=3)

# Or fix validation logic to prevent errors
```text
### Issue: Hot partition detected

#### Symptoms:
- "⚠️ HOT PARTITION DETECTED" warning
- One consumer processing much more than others
- Uneven partition distribution in output

#### Solutions

1. **Salt the key** (loses ordering):

   ```python
   salt = random.randint(0, 3)
   key = f"{original_key}-{salt}"
   ```

1. **Use composite key**:

   ```python
   key = f"{user_id}:{date}:{region}"
   ```

1. **Increase partitions** (can't undo):

   ```bash
   # Create topic with more partitions
   rpk topic create my-topic --partitions 10
   ```

### Issue: Duplicate messages being processed

#### Check

- Idempotency tracking is enabled
- Look for "⚠️ DUPLICATE" messages
- Verify processed_ids set is being populated

#### Fix

```python
# Ensure idempotency check BEFORE processing
if msg_id in processed_ids:
    print("Duplicate detected")
    ch.basic_ack(method.delivery_tag)
    return

# Do work...
processed_ids.add(msg_id)
```text
---

## 📊 Monitoring & Metrics

### RabbitMQ Metrics

```python
# Key metrics to track
- Queue length (messages ready)
- Consumer count (active consumers)
- Message rate (messages/sec)
- Ack rate (processed/sec)
- DLQ count (failed messages)

# Alert thresholds
- DLQ rate > 5% → Critical alert
- Queue length > 10,000 → Warning
- Consumer count = 0 → Critical alert
```text
### Kafka Metrics

```python
# Key metrics to track
- Consumer lag (messages behind)
- Partition throughput (per partition)
- Rebalance frequency (consumer joins/leaves)
- Message rate (messages/sec)

# Alert thresholds
- Lag > 1000 and growing → Warning
- One partition > 2x avg throughput → Hot partition
- Frequent rebalances → Consumer instability
```text
---

## 🎯 Decision Trees

### When to use RabbitMQ vs Kafka?

```text
Need ordering guarantee?
├─ Yes → By key/user?
│  ├─ Yes → Kafka (partition by key)
│  └─ No → RabbitMQ (single queue)
└─ No → Either works
    ├─ High throughput (>10k msg/sec) → Kafka
    ├─ Simple use case → RabbitMQ
    └─ Need replay → Kafka (log retention)
```text
### When to retry vs immediate DLQ?

```text
Error type?
├─ Validation error (invalid format) → DLQ immediately
├─ Business rule violation → DLQ immediately
├─ Network timeout → Retry with backoff
├─ Rate limit (429) → Retry with backoff
├─ Service temporarily down → Retry with backoff
└─ Unknown error → Retry once, then DLQ
```text
### How many partitions?

```text
Start with: max(consumers, throughput_need) * 1.5

Examples:
- 3 consumers, low traffic → 5 partitions
- 10 consumers, high traffic → 15 partitions
- 1 consumer, need ordering → 1 partition

Remember: Can't decrease later!
```text
---

## 💡 Best Practices Summary

### RabbitMQ

✅ Use `prefetch=5` as default (tune based on workload)
✅ Implement idempotency for all consumers
✅ Set reasonable retry limits (3-5 max)
✅ Monitor DLQ rate and alert on high values
✅ Use persistent messages for critical data
✅ Implement exponential backoff for retries

### Kafka

✅ Key by business entity (user, order, device)
✅ Start with 1.5x expected partition need
✅ Monitor per-partition metrics for hot keys
✅ Use consumer groups for horizontal scaling
✅ Don't add more consumers than partitions
✅ Test partition distribution before production

### Idempotency

✅ Check idempotency FIRST (before any work)
✅ Use Redis with TTL for production
✅ Set TTL = 2x max message delay
✅ Use database for audit trail requirements
✅ Prefer natural idempotency when possible
✅ Handle dedup cache failures gracefully

### General

✅ Log with context (msg_id, partition, attempt #)
✅ Use structured logging for analysis
✅ Implement monitoring and alerting
✅ Test failure scenarios (consumer crash, network issues)
✅ Document retry policies and SLAs
✅ Plan for capacity (partitions, consumers, storage)

---

## 📚 Resources

### Official Documentation

- [RabbitMQ Tutorials](https://www.rabbitmq.com/getstarted.html)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Redpanda Quick Start](https://docs.redpanda.com/current/get-started/)

### Breakout Guides

- `breakouts/01_backpressure_dlq.md` - RabbitMQ experiments
- `breakouts/02_partitions_hotkeys.md` - Kafka experiments

### Code Examples

- `live-coding/01_rabbitmq_queue.py` - RabbitMQ with retry & DLQ
- `live-coding/02_kafka_partitions.py` - Kafka partitions & hot keys
- `live-coding/03_inmemory_bus.py` - Simple pub/sub demo
- `src/common/idempotency.py` - Idempotency utilities
- `tests/test_idempotency.py` - Comprehensive tests

---

## 🆘 Getting Help

### Check logs for these markers

- ✓ Success
- ⟳ Retry
- ✗ DLQ/Failed
- ⚠️ Duplicate/Warning
- 🔥 Hot partition

### Common log patterns

```bash
# Successful processing
✓ PROCESSED: id=5 in 150ms (retry=0)

# Retry scenario
⟳ RETRY: id=7 attempt=1/3 | error: Network timeout

# DLQ routing
✗ DLQ: id=9 after 3 retries | error: Invalid format

# Duplicate detection
⚠️ DUPLICATE detected: id=5 (already processed)

# Hot partition warning
⚠️ HOT PARTITION DETECTED: 85 msgs vs avg 15.0
```text
### Debug checklist

1. ✅ Docker containers running?
2. ✅ Producer sending messages?
3. ✅ Consumer connected to correct queue/topic?
4. ✅ No errors in terminal output?
5. ✅ Idempotency tracking working?
6. ✅ Retry logic configured correctly?

---

#### Happy Learning! 🎓
For detailed explanations, see the comprehensive breakout guides and code comments.
