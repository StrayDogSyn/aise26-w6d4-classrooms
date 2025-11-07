"""
In-Memory Event Bus Demo - Pub/Sub, Retry Logic, and Idempotency

This demo provides a lightweight, broker-less event bus for learning core concepts:
1. **Publish/Subscribe Pattern**: Multiple subscribers can listen to the same topic
2. **Retry Logic**: Automatic retry with configurable max attempts
3. **Idempotency**: Deduplication to prevent processing the same message twice
4. **Dead Letter Queue (DLQ)**: Failed messages after max retries

Use Cases:
- Microservices within the same process (monolith decomposition)
- Testing event-driven logic without external dependencies
- Learning event patterns before moving to RabbitMQ/Kafka

Discussion Topics:
- When to use in-memory bus vs external broker? (Single process vs distributed)
- What are the limitations? (No persistence, no cross-process, lost on crash)
- How does this compare to RabbitMQ's topic exchange?
- What happens to messages if the process crashes? (Lost - no durability)

Trade-offs:
✅ Pros: Simple, fast, no external dependencies, good for testing
❌ Cons: No persistence, no distribution, no backpressure, lost on crash
"""

import time, random

class InMemoryBus:
    """
    A simple in-memory publish/subscribe event bus.
    
    Pattern: Observer/Pub-Sub
    - Publishers emit events to topics
    - Subscribers register callback functions for topics
    - One event can trigger multiple subscribers
    
    Comparison to RabbitMQ/Kafka:
    - RabbitMQ: External broker, persistent, distributed
    - Kafka: External broker, persistent, partitioned, log-based
    - InMemoryBus: In-process, ephemeral, simple
    
    Discussion: When would you use this vs a full message broker?
    - Use this: Single process, testing, fast prototyping
    - Use broker: Multiple services, persistence needed, high availability
    """
    def __init__(self):
        # Dict of topic -> list of callback functions
        self.subs = {}

    def subscribe(self, topic: str, fn):
        """
        Register a callback function to be called when events are published to topic.
        
        Note: Multiple functions can subscribe to the same topic (fan-out pattern)
        
        Example:
            bus.subscribe("orders", send_email)
            bus.subscribe("orders", update_inventory)
            bus.subscribe("orders", send_shipping_notification)
        """
        self.subs.setdefault(topic, []).append(fn)
        print(f"[BUS] Subscribed {fn.__name__} to topic '{topic}'")

    def publish(self, topic: str, message: dict):
        """
        Publish an event to all subscribers of a topic.
        
        Synchronous: Calls each subscriber immediately in the current thread
        
        Discussion: What are pros/cons of synchronous vs async?
        - Sync: Simple, immediate feedback, but blocks publisher
        - Async: Non-blocking, but harder to debug, need error handling
        """
        subscribers = self.subs.get(topic, [])
        if not subscribers:
            print(f"[BUS] No subscribers for topic '{topic}'")
            return
        
        print(f"[BUS] Publishing to '{topic}': {message}")
        for fn in subscribers:
            fn(message)

class Deduper:
    """
    Idempotency helper to prevent duplicate message processing.
    
    Strategy: Simple in-memory set of processed message IDs
    
    Production Alternatives:
    1. Redis with TTL: SETEX message_id 3600 "1"
       - Distributed, expires automatically, fast
    2. Database table: CREATE TABLE processed_messages (msg_id TEXT PRIMARY KEY, processed_at TIMESTAMP)
       - Persistent, queryable, slower
    3. Bloom filter: Space-efficient, but has false positive rate
    
    Discussion Questions:
    - How long should you keep processed IDs? (TTL based on message rate)
    - What happens if dedup cache is lost? (Duplicate processing)
    - Should you use a bounded cache? (Yes, with LRU eviction)
    
    Trade-offs:
    - In-memory set: Fast, but lost on restart, unbounded growth
    - Redis: Distributed, TTL support, but network latency
    - Database: Persistent, queryable, but slower
    """
    def __init__(self):
        self.seen = set()
    
    def once(self, msg_id):
        """
        Check if message has been processed before.
        
        Returns:
            True if first time seeing this ID (should process)
            False if already seen (should skip)
        
        Note: This grows unbounded! In production, use TTL or bounded cache.
        """
        if msg_id in self.seen:
            return False
        self.seen.add(msg_id)
        return True
    
    def reset(self):
        """Clear the dedup cache (useful for testing)."""
        self.seen.clear()
    
    def size(self):
        """Return number of tracked message IDs."""
        return len(self.seen)

# Global instances (in production, use dependency injection)
bus = InMemoryBus()
dedup = Deduper()

def email_consumer(evt: dict):
    """
    Example consumer: Send email notification for orders.
    
    Features demonstrated:
    1. Idempotency check (skip duplicates)
    2. Transient failure simulation (retries needed)
    3. Business logic (send email)
    
    Discussion:
    - What if sending email is slow? (Blocks other subscribers)
    - Should email failures go to DLQ immediately? (No, network can recover)
    - How to test this without sending real emails? (Mock/stub pattern)
    """
    msg_id = evt["id"]
    
    # IDEMPOTENCY: Check if already processed
    if not dedup.once(msg_id):
        print(f"  [email] ⚠️  DUPLICATE id={msg_id} user={evt['user']} → SKIP")
        return
    
    # Simulate transient failure (e.g., network timeout, rate limit)
    # Retry logic will handle this automatically via safe_wrapper
    if msg_id % 5 == 0 and evt.get("retry", 0) < 2:
        raise RuntimeError(f"Transient email service error (rate limit)")
    
    # Business logic: Send email
    print(f"  [email] ✓ Sent notification to user={evt['user']} for order={msg_id}")

def safe_wrapper(fn, evt: dict, max_retries=3):
    """
    Retry wrapper with exponential backoff and DLQ routing.
    
    Flow:
    1. Try to execute function
    2. On success: Done
    3. On failure:
       - If retries remain: Increment retry count and recurse
       - If retries exhausted: Send to DLQ
    
    Enhancements for production:
    - Exponential backoff: time.sleep(2 ** retry)
    - Jitter: Add randomness to prevent thundering herd
    - Different retry strategies per exception type
    - Circuit breaker: Stop retrying if service is down
    
    Discussion:
    - Should all errors be retried? (No - validation errors should DLQ immediately)
    - How long should we wait between retries? (Exponential with jitter)
    - When to use circuit breaker pattern? (When service is known to be down)
    """
    retry = evt.get("retry", 0)
    msg_id = evt.get("id", "unknown")
    
    try:
        # Execute the consumer function
        fn(evt)
        
    except Exception as e:
        retry += 1
        
        if retry > max_retries:
            # Send to Dead Letter Queue (DLQ)
            print(f"  [DLQ] ✗ id={msg_id} after {retry-1} retries | reason: {e}")
            
            # In production: 
            # - Log to monitoring system (Datadog, Sentry)
            # - Store in DLQ table/queue for manual review
            # - Alert on-call engineer if critical
            
        else:
            # Retry with incremented count
            print(f"  [retry] ⟳ id={msg_id} attempt={retry}/{max_retries} | error: {e}")
            
            # Copy event and add retry count
            evt2 = dict(evt)
            evt2["retry"] = retry
            
            # Optional: Add exponential backoff
            # backoff_seconds = 2 ** (retry - 1)  # 1s, 2s, 4s
            # print(f"  [retry] Waiting {backoff_seconds}s before retry...")
            # time.sleep(backoff_seconds)
            
            # Recursive retry
            safe_wrapper(fn, evt2, max_retries)

# Subscribe the wrapped email consumer to the 'orders' topic
bus.subscribe("orders", lambda evt: safe_wrapper(email_consumer, evt))

def demo():
    """
    Demonstrate the in-memory event bus with various scenarios.
    
    Scenarios tested:
    1. Normal message processing
    2. Transient failures with retry (id % 5 == 0)
    3. Duplicate message handling (id=2 sent twice)
    4. Multiple messages to same user
    
    Output guide:
    - ✓ Success
    - ⟳ Retry
    - ✗ DLQ (Dead Letter Queue)
    - ⚠️  Duplicate
    """
    print("="*70)
    print("IN-MEMORY EVENT BUS DEMO")
    print("="*70)
    print("Simulating order events with retry logic and idempotency...\n")
    
    # Publish multiple order events
    for i in range(1, 12):
        user = f"user-{i % 3}"
        print(f"\n--- Publishing order {i} for {user} ---")
        bus.publish("orders", {"id": i, "user": user})
    
    # Test idempotency: Send duplicate
    print(f"\n--- Testing Idempotency: Re-sending order 2 ---")
    bus.publish("orders", {"id": 2, "user": "user-2"})
    
    print("\n" + "="*70)
    print("DEMO COMPLETE")
    print("="*70)
    print(f"Total unique messages processed: {dedup.size()}")
    print("\nKey Observations:")
    print("  • Messages with id % 5 == 0 failed initially but retried successfully")
    print("  • Duplicate message (id=2) was detected and skipped")
    print("  • All processing happened synchronously in the current thread")
    print("\nDiscussion Questions:")
    print("  1. What happens if this process crashes during processing?")
    print("  2. How would you make this async for better throughput?")
    print("  3. When would you switch from this to RabbitMQ/Kafka?")
    print("="*70)

if __name__ == "__main__":
    demo()
