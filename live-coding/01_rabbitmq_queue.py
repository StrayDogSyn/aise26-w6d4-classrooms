"""
RabbitMQ Queue Demo - Back-Pressure, Retry Logic, Dead Letter Queue (DLQ), and Idempotency

This demo illustrates key concepts for production-ready message queue systems:
1. **Back-pressure control** via prefetch_count (QoS)
2. **Retry logic** with exponential backoff potential
3. **Dead Letter Queue (DLQ)** for poison messages
4. **Idempotency** tracking to prevent duplicate processing

Discussion Topics:
- When to use low vs high prefetch values (1 vs 5 vs 20)?
- When should messages go directly to DLQ vs retry?
- Where to store idempotency keys (in-memory, Redis, database)?
- How to set DLQ alert thresholds?
"""

import json, os, time, random
import pika

# Configuration constants
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/%2F")
QUEUE = "w6d4.tasks"
DLQ = "w6d4.tasks.dlq"
EXCHANGE = ""  # default direct exchange for simplicity

# Idempotency tracking (in production, use Redis or database with TTL)
processed_ids = set()

def mk_conn():
    """Create a blocking connection to RabbitMQ."""
    params = pika.URLParameters(RABBIT_URL)
    return pika.BlockingConnection(params)

def setup():
    """
    Initialize queues with proper configuration.
    
    Key concepts:
    - DLQ (Dead Letter Queue): Stores messages that failed all retry attempts
    - x-dead-letter-exchange: Routes failed messages to DLQ automatically
    - durable=True: Queues survive broker restarts
    
    Discussion: Should DLQ have its own retry logic or alert humans immediately?
    """
    conn = mk_conn()
    ch = conn.channel()

    # Step 1: Declare the DLQ first (no special properties needed)
    ch.queue_declare(queue=DLQ, durable=True)
    
    # Step 2: Main queue with Dead Letter Exchange (DLX) configuration
    # When a message is nack'd with requeue=False, it goes to the DLQ
    ch.queue_declare(queue=QUEUE, durable=True, arguments={
        "x-dead-letter-exchange": EXCHANGE,  # Use default exchange
        "x-dead-letter-routing-key": DLQ     # Route to DLQ queue
    })
    conn.close()
    print(f"✓ Queues setup: {QUEUE} (main) → {DLQ} (dead-letter)")

def produce(n=10):
    """
    Produce n messages to the queue.
    
    Each message includes:
    - id: Unique identifier (used for idempotency)
    - work_ms: Simulated processing time
    - delivery_mode=2: Persistent messages (survive broker restart)
    
    Discussion: When should messages be persistent vs transient?
    Answer: Persistent for critical data (orders, payments), transient for metrics/logs
    """
    conn = mk_conn()
    ch = conn.channel()
    for i in range(n):
        body = json.dumps({"id": i, "work_ms": random.randint(50, 300)})
        ch.basic_publish(
            exchange=EXCHANGE, 
            routing_key=QUEUE, 
            body=body.encode(),
            properties=pika.BasicProperties(delivery_mode=2)  # persistent
        )
        print(f"→ sent: {body}")
    conn.close()
    print(f"✓ Produced {n} messages")

def consume(prefetch=5, max_retries=3):
    """
    Consume messages from the queue with back-pressure control and retry logic.
    
    Parameters:
    - prefetch: Number of unacked messages a consumer can have at once (QoS)
                * prefetch=1: Low throughput, fair distribution, good for slow/variable tasks
                * prefetch=5: Balanced approach for most use cases
                * prefetch=20: High throughput, but risk of message hoarding
    
    - max_retries: Maximum retry attempts before sending to DLQ
                   * max_retries=0: No retries, immediate DLQ on failure
                   * max_retries=1: Single retry (good for transient network issues)
                   * max_retries=3: Multiple retries with backoff potential
    
    Discussion Topics:
    1. What happens if prefetch is too high? (Message hoarding, unfair distribution)
    2. What happens if prefetch is too low? (Underutilization, network overhead)
    3. When to use immediate DLQ vs retries? (Validation errors → DLQ, network errors → retry)
    4. Where to store idempotency tracking? (Redis with TTL, PostgreSQL, DynamoDB)
    """
    conn = mk_conn()
    ch = conn.channel()
    
    # Back-pressure control: Limit unacknowledged messages per consumer
    ch.basic_qos(prefetch_count=prefetch)
    
    RETRY_HDR = "x-retry-count"

    def handle(ch_, method, props, body):
        """
        Message handler with idempotency, retry logic, and DLQ routing.
        
        Flow:
        1. Check idempotency (skip duplicates)
        2. Try to process message
        3. On success: ACK message
        4. On failure: 
           - If retries remain: Re-publish with incremented retry count
           - If retries exhausted: NACK with requeue=False → sends to DLQ
        """
        msg = json.loads(body.decode())
        msg_id = msg["id"]
        
        # Extract retry count from message headers
        retry = int(props.headers.get(RETRY_HDR, 0)) if props and props.headers else 0

        # IDEMPOTENCY CHECK
        # In production: Use Redis with TTL or database with created_at index
        if msg_id in processed_ids:
            print(f"⚠️  DUPLICATE detected: id={msg_id} (already processed) → ACK without processing")
            ch_.basic_ack(method.delivery_tag)
            return

        try:
            # Simulate transient failures for demonstration
            # In production: Network errors, rate limits, temporary service outages
            if msg["id"] % 7 == 0 and retry < 2:
                raise RuntimeError(f"Simulated transient error (will retry)")
            
            # Simulate processing work
            time.sleep(msg["work_ms"] / 1000.0)
            
            # Mark as processed (idempotency)
            processed_ids.add(msg_id)
            
            print(f"✓ PROCESSED: id={msg_id} in {msg['work_ms']}ms (retry={retry})")
            
            # Acknowledge successful processing
            ch_.basic_ack(method.delivery_tag)
            
        except Exception as e:
            retry += 1
            
            if retry > max_retries:
                # Exhausted retries → Send to Dead Letter Queue
                print(f"✗ DLQ: id={msg_id} after {retry-1} retries | error: {e}")
                ch_.basic_nack(method.delivery_tag, requeue=False)  # Goes to DLQ
                
                # Discussion: Should we alert here? Log to monitoring system?
                # In production: Send to Datadog, PagerDuty, etc.
                
            else:
                # Retry by re-publishing with incremented retry count
                print(f"⟳ RETRY: id={msg_id} attempt={retry}/{max_retries} | error: {e}")
                
                # Re-publish with updated retry header
                ch_.basic_publish(
                    exchange=EXCHANGE, 
                    routing_key=QUEUE, 
                    body=body,
                    properties=pika.BasicProperties(
                        headers={RETRY_HDR: retry}, 
                        delivery_mode=2
                    )
                )
                
                # ACK the original message (we've re-queued manually)
                ch_.basic_ack(method.delivery_tag)
                
                # Optional: Add exponential backoff delay here
                # time.sleep(2 ** retry)  # 2s, 4s, 8s, etc.

    ch.basic_consume(queue=QUEUE, on_message_callback=handle, auto_ack=False)
    print(f" [*] Consuming from {QUEUE} (prefetch={prefetch}, max_retries={max_retries})")
    print(f" [*] Press Ctrl+C to stop...")

    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        print("\n [!] Interrupted by user")
    finally:
        conn.close()

if __name__ == "__main__":
    """
    CLI Interface for RabbitMQ Demo
    
    Usage:
        python 01_rabbitmq_queue.py produce              # Produce 15 messages
        python 01_rabbitmq_queue.py                      # Consume with defaults (prefetch=5, retries=3)
        python 01_rabbitmq_queue.py consume 1 0          # Breakout: prefetch=1, no retries
        python 01_rabbitmq_queue.py consume 20 1         # Breakout: prefetch=20, 1 retry
    
    Breakout Experiments:
    1. Try prefetch=1, max_retries=0 → What happens to failing messages?
    2. Try prefetch=20, max_retries=3 → Does one consumer hoard messages?
    3. Run multiple consumers simultaneously → How does work distribute?
    4. Send duplicate message IDs → Are they processed twice?
    """
    import sys
    setup()
    
    if len(sys.argv) > 1 and sys.argv[1] == "produce":
        count = int(sys.argv[2]) if len(sys.argv) > 2 else 15
        produce(count)
    else:
        # Parse optional prefetch and max_retries from command line
        if len(sys.argv) > 2 and sys.argv[1] == "consume":
            prefetch = int(sys.argv[2]) if len(sys.argv) > 2 else 5
            max_retries = int(sys.argv[3]) if len(sys.argv) > 3 else 3
        else:
            prefetch = 5
            max_retries = 3
        
        consume(prefetch=prefetch, max_retries=max_retries)