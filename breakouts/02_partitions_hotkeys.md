# Breakout 2 — Partitions & Hot Keys (20 min)

**Use:** `live-coding/02_kafka_partitions.py`

## Learning Objectives

By the end of this breakout, you should understand:

1. How Kafka partitions enable parallel processing
2. How consumer groups distribute work across consumers
3. How to detect hot key/partition problems
4. Mitigation strategies for hot keys (salting, more partitions)
5. Trade-offs between ordering guarantees and throughput

---

## Part 1: Understanding Partitions (6 min)

### Setup

1. Start Redpanda (Kafka-compatible):

   ```bash
   docker run -it --rm -p 9092:9092 -p 9644:9644 docker.redpanda.com/redpandadata/redpanda:latest redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M --node-id 0 --check=false --kafka-addr 0.0.0.0:9092
   ```

### Experiment A: Normal Distribution

```bash
# Terminal 1: Produce messages (normal distribution across keys)
python live-coding/02_kafka_partitions.py produce 60

# Terminal 2: Consumer 1
python live-coding/02_kafka_partitions.py C1

# Terminal 3: Consumer 2  
python live-coding/02_kafka_partitions.py C2
```

**Observations to Note:**

- How many partitions does each consumer get assigned?
- Are messages with the same `user` key always on the same partition?
- Is work distributed evenly between consumers?
- Look at the "PARTITION DISTRIBUTION ANALYSIS" output

**Expected Behavior:**

✅ Each consumer gets assigned specific partitions (sticky assignment)
✅ Messages with same key → same partition (ordering preserved)
✅ Work is distributed relatively evenly across partitions
✅ Redpanda default: 3 partitions, so each consumer gets 1-2 partitions

**Key Insight:** This is the "happy path" - even distribution, good parallelism!

---

## Part 2: Hot Key Problem (8 min)

### Experiment B: Simulated Hot Key (50% traffic to one key)

```bash
# Terminal 1: Produce with 50% hot key
python live-coding/02_kafka_partitions.py produce 100 50

# Terminal 2: Consumer 1
python live-coding/02_kafka_partitions.py C1

# Terminal 3: Consumer 2
python live-coding/02_kafka_partitions.py C2
```

**Observations to Note:**

- Look at the "⚠️ HOT PARTITION DETECTED" warning
- Which consumer gets most of the work?
- What's the partition distribution? (One partition has 50+ messages)
- Does one consumer sit mostly idle?

**Expected Behavior:**

⚠️ Hot partition detected (one partition has disproportionate traffic)
⚠️ One consumer processes 50+ messages, other processes far fewer
⚠️ Uneven work distribution leads to bottleneck
⚠️ The consumer with the hot partition becomes the limiting factor

**Real-World Example:**

Imagine an e-commerce site where:

- `user-0` is a bot making thousands of orders/minute
- All other users are normal customers
- All of `user-0`'s orders go to partition 0
- One consumer is overwhelmed, others are idle

### Experiment C: Extreme Hot Key (80% traffic)

```bash
python live-coding/02_kafka_partitions.py produce 100 80
```

**Observations to Note:**

- How bad is the distribution now?
- What happens to throughput?
- Is this a realistic scenario?

**Expected Behavior:**

🔥 Severe bottleneck on one partition
🔥 One consumer processing 80+ messages, others processing <10
🔥 System throughput limited by single consumer
🔥 Realistic scenario: Celebrity user, flash sale, DDoS attack

---

## Part 3: Too Many Consumers (3 min)

### Experiment D: More Consumers Than Partitions

```bash
# Start 5 consumers with only 3 partitions
# Terminal 1-5: Start 5 different consumers
python live-coding/02_kafka_partitions.py C1
python live-coding/02_kafka_partitions.py C2
python live-coding/02_kafka_partitions.py C3
python live-coding/02_kafka_partitions.py C4
python live-coding/02_kafka_partitions.py C5
```

**Observations to Note:**

- How many consumers actually get work?
- Do some consumers sit completely idle?
- What's happening with partition assignment?

**Expected Behavior:**

⚠️ Only 3 consumers get partition assignments (one partition each)
⚠️ 2 consumers sit completely idle (no partitions assigned)
⚠️ Idle consumers still consume resources (memory, connections)

**Rule of Thumb:**

```
Max useful consumers = Number of partitions
```

**Question:** What if you need more parallelism?

**Answer:** Increase partition count (but can't decrease later!)

---

## Part 4: Hot Key Mitigation Strategies (8 min)

### Strategy 1: Key Salting

**Concept:** Add a random suffix to hot keys to distribute them across partitions

**Before (hot key):**

```python
key = "user-0"  # All messages → partition 0
```

**After (salted key):**

```python
import random
salt = random.randint(0, 3)  # 0-3
key = f"user-0-{salt}"  # Distributes across partitions
```

**Trade-off:**

✅ Distributes load across partitions
❌ **LOSES ORDERING GUARANTEE** (messages from same user can be out of order)

**When to use:**

- Ordering doesn't matter (metrics, logs, analytics)
- Partial ordering is acceptable (order within 5-minute window)
- Throughput more important than strict ordering

**When NOT to use:**

- Financial transactions (must be ordered)
- State machines (order affects outcome)
- Audit logs (must be sequential)

### Strategy 2: Increase Partition Count

**Concept:** More partitions = more parallelism

**Before:**

```bash
# Topic with 3 partitions → max 3 parallel consumers
```

**After:**

```bash
# Topic with 10 partitions → max 10 parallel consumers
# Note: Can't decrease partitions later!
```

**Trade-off:**

✅ More parallelism (more consumers can work simultaneously)
✅ Better distribution (hot key still hot, but less impact)
❌ Can't decrease later (Kafka limitation)
❌ More partitions = more resources (file handles, memory)
❌ Rebalancing takes longer

**When to use:**

- Predictable high throughput
- Need more than current partition count allows
- Can plan for future growth

**Rule of thumb:**

```
Partitions = max(
    expected_throughput / consumer_throughput,
    max_parallel_consumers_needed
)
```

**Example:**

- Target: 10,000 msg/sec
- One consumer: 1,000 msg/sec
- Partitions needed: 10,000 / 1,000 = **10 partitions**

### Strategy 3: Composite Keys

**Concept:** Use multiple attributes to create keys

**Example (e-commerce orders):**

```python
# Instead of just user_id:
key = f"{user_id}"  # Hot users create hot partitions

# Use composite key:
key = f"{user_id}:{order_date}"  # Distributes by user AND date
# or
key = f"{user_id}:{region}"  # Distributes by user AND region
```

**Trade-off:**

✅ Better distribution for hot users
✅ Maintains some ordering (within date or region)
❌ More complex key management
❌ Partial ordering only

### Strategy 4: Dedicated Topic for Hot Keys

**Concept:** Route hot keys to a separate topic with more partitions

**Architecture:**

```
Normal Topic (3 partitions)  → 90% of traffic
Hot User Topic (20 partitions) → 10% of traffic (but from 1 user)
```

**Implementation:**

```python
if is_hot_user(user_id):
    producer.send("hot-users-topic", value=event, key=user_id)
else:
    producer.send("normal-topic", value=event, key=user_id)
```

**Trade-off:**

✅ Isolates hot keys from normal traffic
✅ Can tune partitions per topic
❌ More complex (2 topics, 2 consumer groups)
❌ Need logic to identify hot users

---

## Discussion Questions (Group: 15 min)

### 1. How to detect hot keys in production?

**Metrics to monitor:**

| Metric | Tool | Alert Threshold |
|--------|------|-----------------|
| **Per-partition lag** | Kafka JMX, Datadog | Lag > 1000 and growing |
| **Per-partition throughput** | Kafka metrics | >2x average |
| **Consumer lag by partition** | Burrow, Datadog | One partition lagging behind others |
| **Processing time per partition** | Application metrics | >2x average |

**Detection Code (pseudo):**

```python
# Monitor partition-level metrics
for partition in topic.partitions:
    lag = partition.high_water_mark - partition.consumer_offset
    throughput = partition.messages_per_minute
    
    if lag > 1000 and throughput > avg_throughput * 2:
        alert(f"Hot partition detected: {partition}")
```

**Question for group:** At what point do you consider a key "hot"?

**Answer:** When one partition has >2x the average traffic and causes lag

### 2. Ordering vs Throughput: The fundamental trade-off

| Requirement | Solution | Trade-off |
|-------------|----------|-----------|
| **Strict ordering** | Single partition per key | Low throughput, hot key risk |
| **Partial ordering** | Salt keys, more partitions | Higher throughput, order within subset |
| **No ordering needed** | Round-robin, random keys | Maximum throughput, no ordering |

**Real-world examples:**

**Strict ordering needed:**

- Bank transactions (must process deposits before withdrawals)
- Blockchain (blocks must be sequential)
- State machines (state transitions depend on order)

**Partial ordering acceptable:**

- Social media posts (order within 1 minute is fine)
- Log aggregation (order within time window)
- Analytics events (order within session)

**No ordering needed:**

- Metrics/monitoring (count, sum, average)
- Image processing (each image independent)
- Email sending (order doesn't matter)

**Question for group:** What about order fulfillment system?

**Answer:** Depends on business rules:

- If orders are independent → no ordering needed
- If orders affect inventory → need ordering per SKU, not per user
- If orders have dependencies → need strict ordering

### 3. When to use more partitions?

**Increase partitions when:**

✅ Consumer lag is growing despite adding consumers
✅ You need more parallel consumers than current partition count
✅ Hot key problem persists after other mitigations
✅ Planning for future growth (but not too many too early)

**Don't increase partitions when:**

❌ Lag is due to slow consumer code (fix the code first)
❌ Already have more partitions than consumers (add consumers instead)
❌ Topic has low traffic (overhead outweighs benefits)
❌ Need strict ordering (more partitions = more partial ordering)

**Partition planning formula:**

```
Minimum partitions = max(
    peak_throughput / single_consumer_throughput,
    number_of_consumers_needed
)

Recommended = minimum * 1.5 (buffer for growth)
```

**Example:**

- Peak: 5,000 msg/sec
- Consumer throughput: 500 msg/sec
- Minimum: 5,000 / 500 = 10 partitions
- Recommended: 10 * 1.5 = **15 partitions**

### 4. Real-world hot key scenarios

**Scenario 1: Celebrity Tweet**

- Problem: Celebrity with 10M followers tweets
- Effect: 10M notifications, all with same key (celebrity_id)
- Solution: Salt the key, use composite key (celebrity_id:follower_batch)

**Scenario 2: Flash Sale**

- Problem: Product goes on sale, thousands of orders/second
- Effect: All orders for product_id="12345" go to one partition
- Solution: Key by user_id instead of product_id, or salt the key

**Scenario 3: DDoS Attack**

- Problem: Attacker floods system with requests for one user
- Effect: One partition overwhelmed, others idle
- Solution: Rate limiting, dedicated topic for suspicious traffic

**Question for group:** What about a viral video on YouTube?

**Answer:**

- Millions of views, but each view is a separate user
- Key by user_id (views distributed across partitions)
- Aggregation happens downstream (not in Kafka)
- Hot key would be if keyed by video_id (wrong choice!)

---

## Bonus Challenge (Advanced)

### Challenge 1: Implement Custom Partitioner

Instead of relying on default key-based partitioning, create custom logic:

```python
from kafka import KafkaProducer
from kafka.partitioner import Murmur2Partitioner

def custom_partitioner(key, all_partitions, available_partitions):
    """
    Custom partitioning logic to mitigate hot keys.
    """
    # Extract user from key
    user = key.decode('utf-8')
    
    # Check if hot user
    if user == "user-0":
        # Distribute hot user across multiple partitions
        import random
        return random.choice(all_partitions)
    else:
        # Normal users: use default hash
        return Murmur2Partitioner()(key, all_partitions, available_partitions)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    partitioner=custom_partitioner
)
```

### Challenge 2: Build Hot Key Detector

Create a monitoring script that detects hot partitions:

```python
from kafka import KafkaConsumer, TopicPartition

def detect_hot_partitions(topic, window_seconds=60):
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
    
    partitions = consumer.partitions_for_topic(topic)
    partition_counts = {p: 0 for p in partitions}
    
    # Count messages per partition in time window
    start = time.time()
    for msg in consumer:
        partition_counts[msg.partition] += 1
        
        if time.time() - start > window_seconds:
            break
    
    # Calculate average
    avg = sum(partition_counts.values()) / len(partition_counts)
    
    # Detect hot partitions
    for p, count in partition_counts.items():
        if count > avg * 2:
            print(f"🔥 Hot partition detected: {p} ({count} msgs vs avg {avg:.1f})")
```

### Challenge 3: Simulate and Fix Hot Key Scenario

1. Create a hot key scenario (80% traffic to one key)
2. Measure consumer lag per partition
3. Implement mitigation (salting or more partitions)
4. Measure improvement in distribution and lag

---

## Key Takeaways

✅ **Partitions enable parallelism**

- One partition = one consumer at a time (within consumer group)
- More partitions = more parallel consumers possible
- But: Can't decrease partition count later

✅ **Hot keys create bottlenecks**

- One partition gets disproportionate traffic
- One consumer becomes overwhelmed
- Other consumers sit idle
- System throughput limited by slowest consumer

✅ **Mitigation strategies**

- **Key salting**: Distribute hot keys across partitions (loses ordering)
- **More partitions**: Increase parallelism (can't undo)
- **Composite keys**: Use multiple attributes for distribution
- **Dedicated topics**: Isolate hot keys

✅ **Ordering vs Throughput trade-off**

- Strict ordering: Single partition per key (low throughput)
- Partial ordering: Salt keys, more partitions (better throughput)
- No ordering: Random/round-robin (maximum throughput)

✅ **Monitoring is critical**

- Track per-partition lag and throughput
- Alert on hot partition detection
- Use metrics to guide scaling decisions
- Test mitigation strategies with production-like load

