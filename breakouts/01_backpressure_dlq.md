# Breakout 1 — Back‑Pressure & DLQ (20 min)

**Use:** `live-coding/01_rabbitmq_queue.py`

## Learning Objectives

By the end of this breakout, you should understand:

1. How `prefetch_count` controls back-pressure and work distribution
2. When to use retry vs immediate DLQ
3. How to implement idempotency to handle duplicate messages
4. Where to store deduplication data in production

---

## Part 1: Experimenting with Prefetch Values (8 min)

### Setup

1. Start RabbitMQ:

   ```bash
   docker run -it --rm -p 5672:5672 -p 15672:15672 rabbitmq:3-management
   ```

2. Produce messages:

   ```bash
   python live-coding/01_rabbitmq_queue.py produce
   ```

### Experiment A: Low Prefetch (prefetch=1)

```bash
# Terminal 1: Consumer with prefetch=1
python live-coding/01_rabbitmq_queue.py consume 1 3

# Terminal 2: Another consumer with prefetch=1
python live-coding/01_rabbitmq_queue.py consume 1 3
```text
#### Observations to Note:
- How is work distributed between consumers?
- What happens when one consumer is slower?
- Is there any message hoarding?

#### Expected Behavior:
✅ Fair distribution (each consumer gets 1 message at a time)
✅ Slower consumer gets fewer messages overall
✅ No hoarding (messages wait in queue until consumer is ready)

### Experiment B: High Prefetch (prefetch=20)

```bash
# Terminal 1: Consumer with prefetch=20
python live-coding/01_rabbitmq_queue.py consume 20 3

# Terminal 2: Another consumer with prefetch=20
python live-coding/01_rabbitmq_queue.py consume 20 3
```text
#### Observations to Note:
- Does one consumer grab all/most messages?
- What happens if a consumer crashes with 20 unacked messages?
- How does throughput compare to prefetch=1?

#### Expected Behavior:
⚠️ Potential message hoarding (one consumer may grab most messages)
⚠️ If consumer crashes, 20 messages need redelivery
✅ Higher throughput (less network overhead)

### Experiment C: Balanced Prefetch (prefetch=5)

```bash
python live-coding/01_rabbitmq_queue.py consume 5 3
```text
#### Observations to Note:
- Does this balance fairness and throughput?
- What's a good prefetch value for your use case?

#### Expected Behavior:
✅ Good balance between fairness and throughput
✅ Reasonable buffer for network latency
✅ Acceptable risk if consumer crashes (only 5 messages lost)

---

## Part 2: Retry Logic Experiments (8 min)

### Experiment D: No Retries (max_retries=0)

```bash
python live-coding/01_rabbitmq_queue.py produce 20
python live-coding/01_rabbitmq_queue.py consume 5 0
```text
#### Observations to Note:
- Which messages go to DLQ?
- Look for messages where `id % 7 == 0` (simulated failures)

#### Expected Behavior:
✗ Messages fail on first error and go directly to DLQ
✗ No retry attempts for transient errors
⚠️ Use case: Validation errors that won't succeed on retry

### Experiment E: Single Retry (max_retries=1)

```bash
python live-coding/01_rabbitmq_queue.py produce 20
python live-coding/01_rabbitmq_queue.py consume 5 1
```text
#### Observations to Note:
- Do messages succeed on second attempt?
- Look for "⟳ RETRY" log messages

#### Expected Behavior:
⟳ Failures are retried once
✗ Still some DLQ messages (simulated transient failures need 2 retries to succeed)
✅ Use case: Simple network hiccups

### Experiment F: Multiple Retries (max_retries=3)

```bash
python live-coding/01_rabbitmq_queue.py produce 20
python live-coding/01_rabbitmq_queue.py consume 5 3
```text
#### Observations to Note:
- Do all messages eventually succeed?
- How many retry attempts before success?

#### Expected Behavior:
✓ Most/all messages succeed after retries
✓ Simulated failures need 2 retries, so max_retries=3 is sufficient
⚠️ Real scenario: Exponential backoff recommended

---

## Part 3: Idempotency Testing (4 min)

### Experiment G: Duplicate Message Handling

#### Modify producer to send duplicates:
```bash
# Send messages, then manually send duplicates
python live-coding/01_rabbitmq_queue.py produce 10
# Now open the code and run produce again - observe idempotency checks
```text
#### Or test in code:
Add this to the producer section:

```python
# After producing, send some duplicates
for i in [1, 3, 5]:
    body = json.dumps({"id": i, "work_ms": 100})
    ch.basic_publish(EXCHANGE, QUEUE, body.encode(),
        pika.BasicProperties(delivery_mode=2))
    print(f"→ sent DUPLICATE: {body}")
```text
#### Observations to Note:
- Are duplicates detected?
- Look for "⚠️ DUPLICATE detected" messages
- Is the message ACK'd without processing?

#### Expected Behavior:
✓ Duplicates are detected via `processed_ids` set
✓ Duplicate messages are ACK'd but not processed
✓ No duplicate side effects (emails, payments, etc.)

---

## Discussion Questions (Group: 15 min)

### 1. When to use immediate DLQ vs retries?

#### Immediate DLQ (max_retries=0):
- Validation errors (invalid email format, missing required field)
- Business rule violations (order already cancelled)
- Permanent failures that won't recover

#### Retry with backoff:
- Network timeouts
- Rate limits (HTTP 429)
- Database connection errors
- Downstream service temporarily down

**Question for group:** What about a payment gateway timeout?

- **Answer:** Retry with exponential backoff, but set reasonable limit (3-5 retries) to avoid charging customer multiple times

### 2. What's the right prefetch value?

#### Considerations:
- **Message processing time:** Longer processing → lower prefetch
- **Message size:** Larger messages → lower prefetch (memory usage)
- **Number of consumers:** More consumers → lower prefetch per consumer
- **Failure recovery time:** Higher prefetch → more messages lost on crash

#### Formula (rule of thumb):
```text
prefetch = (desired throughput * avg processing time) / number of consumers
```text
#### Example:
- Target: 100 msg/sec
- Processing time: 50ms
- Consumers: 2
- Prefetch = (100 * 0.05) / 2 = 2.5 ≈ **3-5**

**Question for group:** What prefetch for a batch job that processes 1000 images?

- **Answer:** Higher prefetch (20-50) if each image is independent, or prefetch=1 if ordering matters

### 3. Where to store idempotency tracking in production?

| Storage | Pros | Cons | Use Case |
|---------|------|------|----------|
| **In-memory Set** | Fast, simple | Lost on restart, not distributed | Testing, single-process |
| **Redis with TTL** | Fast, distributed, auto-expires | Requires Redis, lost if Redis crashes | High-throughput, distributed systems |
| **Database Table** | Persistent, queryable, audit trail | Slower, requires cleanup job | Critical operations, compliance |
| **Natural Idempotency** | No tracking needed | Requires careful design | Best when possible (SET, UPSERT) |

#### Real-world example - payment processing:
```python
# Option 1: Redis
redis.setex(f"payment:{payment_id}", 3600, "1")  # 1 hour TTL

# Option 2: Database with unique constraint
INSERT INTO processed_payments (payment_id, processed_at)
VALUES (?, CURRENT_TIMESTAMP)
ON CONFLICT (payment_id) DO NOTHING
RETURNING payment_id;  -- Returns NULL if duplicate
```text
**Question for group:** How long should TTL be?

- **Answer:** 2x the maximum expected message delay. If messages can be delayed up to 1 hour, use 2-hour TTL.

### 4. DLQ Alert Thresholds

#### When to alert on-call engineer?
| Scenario | Threshold | Action |
|----------|-----------|--------|
| **Normal operation** | 0-1% DLQ rate | No alert, log for analysis |
| **Warning** | 1-5% DLQ rate | Warning alert, investigate next day |
| **Critical** | >5% DLQ rate | Page on-call immediately |
| **Service down** | >50% DLQ rate | Critical page, potential outage |

#### Monitoring setup:
```python
# Pseudo-code for monitoring
if dlq_count > 100 and dlq_rate > 0.05:
    alert("High DLQ rate", severity="warning")

if dlq_rate > 0.5:
    alert("Service degradation", severity="critical")
```text
**Question for group:** Should you automatically retry DLQ messages?

- **Answer:** Depends on failure type:
  - Transient errors: Yes, with manual approval or automated retry after fix
  - Validation errors: No, fix data first
  - Unknown errors: Manual investigation required

---

## Bonus Challenge (Advanced)

### Challenge 1: Add Exponential Backoff

Modify the retry logic to wait between retries:

- 1st retry: Wait 1 second
- 2nd retry: Wait 2 seconds
- 3rd retry: Wait 4 seconds

**Hint:** Add `time.sleep(2 ** (retry - 1))` in the retry block

**Discussion:** What's the tradeoff?

- Pro: Gives downstream services time to recover
- Con: Delays message processing, holds up consumer

### Challenge 2: Track DLQ Reasons

Modify the DLQ handler to log why messages failed:

```python
# Store in a dict: dlq_reasons[msg_id] = str(exception)
```text
**Discussion:** How would you expose this to operators?

- Dashboard showing top failure reasons
- Alert when new failure type appears
- API to query DLQ by reason

### Challenge 3: Implement Redis-based Idempotency

Replace the in-memory `processed_ids` set with Redis:

```python
import redis
r = redis.Redis(host='localhost', port=6379, db=0)

def is_duplicate(msg_id):
    key = f"processed:{msg_id}"
    # SETNX returns True if key didn't exist
    is_new = r.setnx(key, "1")
    if is_new:
        r.expire(key, 3600)  # 1 hour TTL
        return False
    return True
```text
**Discussion:** What if Redis is down?

- Option 1: Fail open (accept duplicates risk)
- Option 2: Fail closed (reject all messages)
- Option 3: Fallback to database

---

## Key Takeaways

✅ **Prefetch** controls back-pressure and work distribution

- Low (1): Fair but slower
- Medium (5): Balanced
- High (20+): Fast but risky

✅ **Retry strategies** depend on error type

- Validation errors → immediate DLQ
- Transient errors → retry with backoff
- Monitor DLQ rate for alerts

✅ **Idempotency** is critical for at-least-once delivery

- In-memory: Testing only
- Redis: Production, high-throughput
- Database: Audit trail, compliance

✅ **DLQ monitoring** prevents silent failures

- Set thresholds based on business impact
- Alert on high DLQ rates
- Investigate and fix root causes
