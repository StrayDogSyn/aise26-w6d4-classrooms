"""
Kafka Partitions Demo - Consumer Groups, Ordering, and Hot Key Detection

This demo illustrates key Kafka concepts for distributed event streaming:
1. **Partitioning**: How messages are distributed across partitions
2. **Consumer Groups**: Multiple consumers share the workload
3. **Ordering Guarantees**: Messages with same key go to same partition (ordered)
4. **Hot Keys/Partitions**: When one partition gets disproportionate traffic

Discussion Topics:
- How does Kafka guarantee ordering within a partition?
- What happens when you have more consumers than partitions?
- How to detect and mitigate hot key problems?
- When to use more partitions vs fewer partitions?

Key Concepts:
- Partition assignment is sticky (consumers get assigned specific partitions)
- Messages with the same key always go to the same partition
- Consumer groups enable horizontal scaling
- Hot keys can create bottlenecks (one partition overloaded)
"""

import os, time, json, threading, random
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

# Configuration
BOOT = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "w6d4.events"

def mk_producer():
    """
    Create a Kafka producer with JSON serialization.
    
    Note: We don't specify a key_serializer here, but we could add one
    to ensure consistent hashing of keys.
    """
    return KafkaProducer(
        bootstrap_servers=BOOT,
        value_serializer=lambda v: json.dumps(v).encode()
    )

def mk_consumer(group_id: str):
    """
    Create a Kafka consumer that belongs to a consumer group.
    
    Key parameters:
    - group_id: Consumers with the same group_id share partition assignments
    - auto_offset_reset="earliest": Start from beginning if no committed offset
    - enable_auto_commit=True: Automatically commit offsets periodically
    
    Discussion: What's the tradeoff between auto-commit and manual commit?
    - Auto: Simpler, but risk of message loss on crash
    - Manual: More control, exactly-once semantics, but more complex
    """
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOT,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode())
    )

def produce(n=50, hot_key_pct=0):
    """
    Produce n events to Kafka topic.
    
    Parameters:
    - n: Number of events to produce
    - hot_key_pct: Percentage (0-100) of messages to send to a single "hot" key
    
    Key Concepts:
    - Messages with the same key route to the same partition (ordering preserved)
    - Key is used for partition assignment: hash(key) % num_partitions
    - This demo uses user-{0..6} keys for normal distribution
    
    Hot Key Simulation (Breakout Exercise):
    - Set hot_key_pct=50 to send 50% of messages to user-0 (creates hot partition)
    - Observe: One partition gets overloaded, others sit idle
    - Mitigation: Salt the key or increase partitions
    
    Discussion:
    - What happens to ordering if we salt keys? (Lost per-user ordering)
    - How to detect hot keys in production? (Monitor per-partition lag/throughput)
    """
    p = mk_producer()
    partition_counts = {}  # Track messages per partition for analysis
    
    for i in range(n):
        # Simulate hot key scenario for breakout
        if hot_key_pct > 0 and random.randint(1, 100) <= hot_key_pct:
            key = "user-0"  # Hot key - all messages go to same partition
        else:
            key = f"user-{i % 7}"  # Normal distribution across 7 users
        
        evt = {"i": i, "user": key, "ts": time.time()}
        
        # Send message (Kafka will hash the key to determine partition)
        future = p.send(TOPIC, value=evt, key=key.encode())
        
        # Get metadata for analysis (which partition was used)
        try:
            record_metadata = future.get(timeout=10)
            partition = record_metadata.partition
            partition_counts[partition] = partition_counts.get(partition, 0) + 1
        except Exception as e:
            print(f"Error sending message {i}: {e}")
        
        print(f"→ sent: i={i} user={key}")
        time.sleep(0.03)  # Slow down for visibility
    
    p.flush()
    
    # Display partition distribution analysis
    print("\n" + "="*60)
    print("PARTITION DISTRIBUTION ANALYSIS:")
    print("="*60)
    for partition in sorted(partition_counts.keys()):
        count = partition_counts[partition]
        bar = "█" * (count // 2)  # Simple bar chart
        print(f"Partition {partition}: {count:3d} messages {bar}")
    print("="*60)
    
    # Hot key detection
    if partition_counts:
        max_count = max(partition_counts.values())
        avg_count = sum(partition_counts.values()) / len(partition_counts)
        if max_count > avg_count * 2:
            print(f"⚠️  HOT PARTITION DETECTED: {max_count} msgs vs avg {avg_count:.1f}")
            print("   Mitigation: Use salted keys or increase partition count")
    print()

def consume(name: str, group="w6d4-group"):
    """
    Consume messages from Kafka topic as part of a consumer group.
    
    Parameters:
    - name: Display name for this consumer (e.g., "C1", "C2")
    - group: Consumer group ID (consumers in same group share partitions)
    
    Key Concepts:
    - Consumers in the same group get assigned different partitions
    - One partition is consumed by only ONE consumer in a group
    - If you have more consumers than partitions, some consumers sit idle
    - Ordering is guaranteed within a partition, not across partitions
    
    Discussion Topics:
    1. What happens if a consumer in the group crashes? (Rebalance occurs)
    2. How many consumers should you run? (At most, number of partitions)
    3. Why can't two consumers in the same group read the same partition?
       (Would break offset tracking and exactly-once semantics)
    
    Breakout Experiments:
    - Run 2 consumers in same group → see partition assignment
    - Run 3+ consumers with 3 partitions → observe idle consumer
    - Send all messages with same key → one consumer gets all work (hot key)
    """
    c = mk_consumer(group)
    partition_stats = {}  # Track which partitions this consumer reads from
    
    print(f"\n[{name}] Starting consumer in group '{group}'")
    print(f"[{name}] Waiting for partition assignment...")
    
    try:
        for msg in c:
            v = msg.value
            partition = msg.partition
            offset = msg.offset
            
            # Track partition statistics
            if partition not in partition_stats:
                partition_stats[partition] = 0
            partition_stats[partition] += 1
            
            # Display message with partition and offset info
            print(f"[{name}] i={v['i']:3d} user={v['user']:8s} "
                  f"partition={partition} offset={offset:4d} "
                  f"(total from p{partition}: {partition_stats[partition]})")
            
    except KeyboardInterrupt:
        print(f"\n[{name}] Interrupted by user")
        print(f"[{name}] Final partition assignment: {list(partition_stats.keys())}")
        print(f"[{name}] Messages per partition: {partition_stats}")
    finally:
        c.close()

if __name__ == "__main__":
    """
    CLI Interface for Kafka Partitions Demo
    
    Usage:
        # Producer (normal distribution):
        python 02_kafka_partitions.py produce              # 80 messages, normal distribution
        python 02_kafka_partitions.py produce 100          # 100 messages
        
        # Producer with hot key (breakout):
        python 02_kafka_partitions.py produce 100 50       # 50% messages to hot key
        
        # Consumer:
        python 02_kafka_partitions.py C1                   # Consumer named "C1"
        python 02_kafka_partitions.py C2                   # Consumer named "C2"
        python 02_kafka_partitions.py                      # Default consumer
    
    Breakout Experiments (Partitions & Hot Keys):
    
    1. Normal Scenario:
       Terminal A: python 02_kafka_partitions.py produce 60
       Terminal B: python 02_kafka_partitions.py C1
       Terminal C: python 02_kafka_partitions.py C2
       → Observe: Work is distributed across consumers
    
    2. Hot Key Scenario (50% to one key):
       Terminal A: python 02_kafka_partitions.py produce 100 50
       Terminal B: python 02_kafka_partitions.py C1
       Terminal C: python 02_kafka_partitions.py C2
       → Observe: One consumer gets much more work (hot partition)
    
    3. Too Many Consumers:
       - Start 5 consumers with only 3 partitions
       → Observe: 2 consumers sit idle (no partition assignment)
    
    Discussion Questions:
    - How would you detect this hot key problem in production?
      (Monitor per-partition lag, messages/sec, consumer lag)
    - How would you fix it?
      (Salt the key, increase partitions, use composite keys)
    - What's the tradeoff of salting keys?
      (Lose per-user ordering guarantee)
    """
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "produce":
        count = int(sys.argv[2]) if len(sys.argv) > 2 else 80
        hot_key_pct = int(sys.argv[3]) if len(sys.argv) > 3 else 0
        
        if hot_key_pct > 0:
            print(f"\n🔥 HOT KEY MODE: {hot_key_pct}% of messages will go to 'user-0'")
            print("   This simulates a hot partition scenario\n")
        
        produce(count, hot_key_pct)
    else:
        # Consumer mode
        who = sys.argv[1] if len(sys.argv) > 1 else "C1"
        consume(who)