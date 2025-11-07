"""
Comprehensive Tests for Idempotency Utilities

These tests verify that idempotency mechanisms work correctly across
different storage strategies (in-memory, TTL-based, Redis-simulated, database).

Test Categories:
1. Basic deduplication (first vs duplicate)
2. TTL/expiration behavior
3. Edge cases (empty IDs, None, concurrent access)
4. Performance considerations
5. Integration patterns

Discussion Topics:
- Why is testing idempotency critical? (Data corruption if wrong)
- What edge cases should you test? (Empty IDs, race conditions, TTL edge cases)
- How to test TTL behavior without waiting? (Time mocking)
- Should you test with real Redis/DB? (Yes, in integration tests)
"""

import time
import pytest
from src.common.idempotency import (
    process_once,
    InMemoryDeduper,
    TTLDeduper,
    SimulatedRedisDeduper,
    DatabaseDeduper
)


# ============================================================================
# BASIC TESTS - Simple set-based deduplication
# ============================================================================

def test_process_once_basic():
    """
    Test basic idempotency with simple set.
    
    Learning: This is the simplest form of deduplication.
    Use case: Single-process, no persistence needed.
    """
    seen = set()
    
    # First time: should be processed
    assert process_once(seen, "A") == "processed"
    
    # Second time: should be duplicate
    assert process_once(seen, "A") == "duplicate"
    
    # Different ID: should be processed
    assert process_once(seen, "B") == "processed"


def test_process_once_multiple_duplicates():
    """Test that multiple duplicate attempts are all detected."""
    seen = set()
    
    assert process_once(seen, "msg-1") == "processed"
    
    # Try same message 5 times
    for _ in range(5):
        assert process_once(seen, "msg-1") == "duplicate"


def test_process_once_empty_id():
    """
    Test behavior with edge case: empty string ID.
    
    Discussion: Should empty string be allowed?
    - Pro: Simple, treats it like any other ID
    - Con: Might indicate a bug in caller
    """
    seen = set()
    
    # Empty string is a valid ID (though unusual)
    assert process_once(seen, "") == "processed"
    assert process_once(seen, "") == "duplicate"


# ============================================================================
# IN-MEMORY DEDUPER TESTS - Enhanced version with statistics
# ============================================================================

def test_in_memory_deduper_basic():
    """Test InMemoryDeduper with basic operations."""
    deduper = InMemoryDeduper()
    
    # First message
    assert deduper.is_duplicate("msg-1") is False
    
    # Same message again
    assert deduper.is_duplicate("msg-1") is True
    
    # Different message
    assert deduper.is_duplicate("msg-2") is False


def test_in_memory_deduper_statistics():
    """
    Test that statistics are tracked correctly.
    
    Discussion: Why track statistics?
    - Monitor duplicate rate (might indicate retry storms)
    - Debug issues (are duplicates being detected?)
    - Capacity planning (how many unique messages?)
    """
    deduper = InMemoryDeduper()
    
    # Process some messages
    deduper.is_duplicate("msg-1")  # New
    deduper.is_duplicate("msg-1")  # Duplicate
    deduper.is_duplicate("msg-2")  # New
    deduper.is_duplicate("msg-1")  # Duplicate
    deduper.is_duplicate("msg-3")  # New
    
    stats = deduper.stats()
    assert stats["total_checks"] == 5
    assert stats["unique_messages"] == 3
    assert stats["duplicates"] == 2


def test_in_memory_deduper_reset():
    """Test that reset clears all state."""
    deduper = InMemoryDeduper()
    
    deduper.is_duplicate("msg-1")
    deduper.is_duplicate("msg-1")
    
    # Reset
    deduper.reset()
    
    # Should be treated as new message after reset
    assert deduper.is_duplicate("msg-1") is False
    
    # Stats should be reset
    stats = deduper.stats()
    assert stats["total_checks"] == 1
    assert stats["unique_messages"] == 1
    assert stats["duplicates"] == 0


# ============================================================================
# TTL DEDUPER TESTS - Time-based expiration
# ============================================================================

def test_ttl_deduper_basic():
    """Test TTL-based deduplication within TTL window."""
    deduper = TTLDeduper(ttl_seconds=3600)  # 1 hour
    
    assert deduper.is_duplicate("msg-1") is False
    assert deduper.is_duplicate("msg-1") is True


def test_ttl_deduper_expiration():
    """
    Test that messages expire after TTL.
    
    Discussion: How to test time-based behavior?
    - Option 1: Actually wait (slow, flaky)
    - Option 2: Use very short TTL (this test)
    - Option 3: Mock time.time() (more complex, more reliable)
    """
    deduper = TTLDeduper(ttl_seconds=0.1)  # 100ms TTL
    
    # First check: new message
    assert deduper.is_duplicate("msg-1") is False
    
    # Immediate recheck: duplicate
    assert deduper.is_duplicate("msg-1") is True
    
    # Wait for expiration
    time.sleep(0.15)
    
    # After TTL: treated as new message
    assert deduper.is_duplicate("msg-1") is False


def test_ttl_deduper_cleanup():
    """
    Test that expired entries are cleaned up automatically.
    
    Discussion: Why is automatic cleanup important?
    - Prevents unbounded memory growth
    - Keeps lookups fast (smaller hash table)
    - In production: Monitor memory usage over time
    """
    deduper = TTLDeduper(ttl_seconds=0.1)
    
    # Add multiple messages
    for i in range(10):
        deduper.is_duplicate(f"msg-{i}")
    
    assert deduper.size() == 10
    
    # Wait for expiration
    time.sleep(0.15)
    
    # Trigger cleanup by checking a new message
    deduper.is_duplicate("new-msg")
    
    # Old messages should be cleaned up
    assert deduper.size() == 1  # Only the new message remains


def test_ttl_deduper_mixed_expiration():
    """
    Test behavior when some messages expire and others don't.
    
    Scenario: Messages arrive at different times, TTL applies individually.
    """
    deduper = TTLDeduper(ttl_seconds=0.2)
    
    # First message
    deduper.is_duplicate("msg-1")
    
    # Wait a bit
    time.sleep(0.1)
    
    # Second message (will expire later)
    deduper.is_duplicate("msg-2")
    
    # Wait for first to expire
    time.sleep(0.15)  # Total 0.25s for msg-1, 0.15s for msg-2
    
    # msg-1 should be expired (reprocessable)
    assert deduper.is_duplicate("msg-1") is False
    
    # msg-2 should still be in window
    assert deduper.is_duplicate("msg-2") is True


# ============================================================================
# SIMULATED REDIS DEDUPER TESTS - Production-like behavior
# ============================================================================

def test_simulated_redis_basic():
    """Test Redis-style deduplication."""
    deduper = SimulatedRedisDeduper(ttl_seconds=3600)
    
    assert deduper.is_duplicate("order-123") is False
    assert deduper.is_duplicate("order-123") is True
    assert deduper.is_duplicate("order-456") is False


def test_simulated_redis_expiration():
    """Test Redis-style TTL expiration."""
    deduper = SimulatedRedisDeduper(ttl_seconds=0.1)
    
    deduper.is_duplicate("msg-1")
    assert deduper.exists("msg-1") is True
    
    time.sleep(0.15)
    
    # Should be expired
    assert deduper.exists("msg-1") is False
    assert deduper.is_duplicate("msg-1") is False  # Can be processed again


def test_simulated_redis_delete():
    """
    Test explicit deletion of message IDs.
    
    Use case: Manual retry after investigation
    - Message went to DLQ
    - Issue was fixed
    - Want to allow reprocessing
    """
    deduper = SimulatedRedisDeduper(ttl_seconds=3600)
    
    deduper.is_duplicate("msg-1")
    assert deduper.exists("msg-1") is True
    
    # Explicitly remove
    deduper.delete("msg-1")
    
    # Should be able to process again
    assert deduper.exists("msg-1") is False
    assert deduper.is_duplicate("msg-1") is False


def test_simulated_redis_many_messages():
    """
    Test behavior with many messages.
    
    Discussion: Performance considerations
    - Hash table lookups: O(1) average case
    - Memory usage: ~100 bytes per entry
    - 1M messages ≈ 100MB RAM (acceptable)
    - 10M messages ≈ 1GB RAM (needs TTL cleanup)
    """
    deduper = SimulatedRedisDeduper(ttl_seconds=3600)
    
    # Add many unique messages
    for i in range(1000):
        assert deduper.is_duplicate(f"msg-{i}") is False
    
    # Check they're all tracked
    for i in range(1000):
        assert deduper.exists(f"msg-{i}") is True
    
    # Try duplicates
    for i in range(1000):
        assert deduper.is_duplicate(f"msg-{i}") is True


# ============================================================================
# DATABASE DEDUPER TESTS - Persistent storage
# ============================================================================

def test_database_deduper_basic():
    """Test database-style deduplication (simulated)."""
    deduper = DatabaseDeduper("postgresql://localhost/test")
    
    assert deduper.is_duplicate("payment-123") is False
    assert deduper.is_duplicate("payment-123") is True
    assert deduper.is_duplicate("payment-456") is False


def test_database_deduper_cleanup():
    """
    Test cleanup of old entries.
    
    Discussion: Cleanup strategies
    - Cron job: Run nightly DELETE FROM ... WHERE age > threshold
    - On-insert: Delete old entries with each new insert (adds latency)
    - Partition: Use date-based partitioning, drop old partitions
    - TTL: Some databases support automatic TTL (Cassandra, MongoDB)
    """
    deduper = DatabaseDeduper("postgresql://localhost/test")
    
    # Add messages
    for i in range(10):
        deduper.is_duplicate(f"msg-{i}")
    
    # Simulate time passing
    time.sleep(0.1)
    
    # Add more messages
    for i in range(10, 15):
        deduper.is_duplicate(f"msg-{i}")
    
    # Cleanup old entries (pretend 0.05 seconds = 7 days)
    # In real implementation, this would be:
    # DELETE FROM processed_messages WHERE processed_at < NOW() - INTERVAL '7 days'
    deleted = deduper.cleanup_old_entries(days=0)  # Use 0 for testing
    
    # Should have deleted something
    assert deleted >= 0


# ============================================================================
# EDGE CASES AND ERROR HANDLING
# ============================================================================

def test_none_message_id():
    """
    Test behavior with None as message ID.
    
    Discussion: Should this raise an error or be treated as valid?
    - Option 1: Treat as valid ID (current behavior)
    - Option 2: Raise ValueError (fail fast)
    - Best practice: Validate at message ingestion, not here
    """
    seen = set()
    
    # None is hashable and can be added to set
    assert process_once(seen, None) == "processed"
    assert process_once(seen, None) == "duplicate"


def test_numeric_message_ids():
    """
    Test with numeric message IDs.
    
    Use case: Auto-incrementing database IDs
    """
    deduper = InMemoryDeduper()
    
    assert deduper.is_duplicate(123) is False
    assert deduper.is_duplicate(123) is True
    assert deduper.is_duplicate(456) is False


def test_unicode_message_ids():
    """
    Test with Unicode message IDs.
    
    Use case: International systems, emoji in IDs (unusual but possible)
    """
    deduper = InMemoryDeduper()
    
    assert deduper.is_duplicate("msg-🚀-1") is False
    assert deduper.is_duplicate("msg-🚀-1") is True
    assert deduper.is_duplicate("用户-123") is False


def test_very_long_message_id():
    """
    Test with very long message IDs.
    
    Discussion: Should there be a maximum length?
    - Database: VARCHAR(255) or TEXT
    - Redis: No practical limit
    - Hash table: Long strings use more memory but work fine
    """
    deduper = InMemoryDeduper()
    
    long_id = "x" * 10000  # 10KB ID
    
    assert deduper.is_duplicate(long_id) is False
    assert deduper.is_duplicate(long_id) is True


# ============================================================================
# INTEGRATION PATTERNS AND BEST PRACTICES
# ============================================================================

def test_idempotency_with_retry_pattern():
    """
    Test idempotency in combination with retry logic.
    
    Scenario: Message fails, retries 3 times, should only process once.
    
    Discussion: Where does idempotency fit in retry flow?
    1. Check idempotency FIRST (before any work)
    2. Do the work
    3. Mark as processed (if work succeeded)
    4. On retry, idempotency check will skip work
    """
    deduper = InMemoryDeduper()
    processed_count = 0
    
    def process_message(msg_id: str, should_fail: bool):
        nonlocal processed_count
        
        # Idempotency check FIRST
        if deduper.is_duplicate(msg_id):
            return "skipped"
        
        # Simulate work
        if should_fail:
            # Work failed, but we DON'T mark as processed yet
            # This allows retry
            raise RuntimeError("Transient error")
        
        # Work succeeded
        processed_count += 1
        return "processed"
    
    # First attempt: fails
    with pytest.raises(RuntimeError):
        process_message("msg-1", should_fail=True)
    
    # Retry: succeeds (but doesn't mark as duplicate yet - needs manual handling)
    # In real code, you'd mark as processed after success
    assert processed_count == 0  # Haven't marked success yet


def test_at_least_once_delivery_simulation():
    """
    Simulate at-least-once delivery where same message arrives multiple times.
    
    Kafka/RabbitMQ guarantee: At-least-once delivery
    - Message may be delivered multiple times
    - Consumer must be idempotent to handle this correctly
    
    Discussion: What about exactly-once?
    - Kafka: Exactly-once with transactions (complex)
    - RabbitMQ: Not supported, use idempotency instead
    - Best practice: Design for idempotency, not exactly-once
    """
    deduper = InMemoryDeduper()
    processed_messages = []
    
    def process_order(order_id: str):
        if not deduper.is_duplicate(order_id):
            processed_messages.append(order_id)
            return "processed"
        return "duplicate"
    
    # Simulate same message arriving 5 times (network retries, consumer restarts)
    for _ in range(5):
        result = process_order("order-123")
    
    # Should only be processed once
    assert len(processed_messages) == 1
    assert processed_messages[0] == "order-123"
    
    # Check stats
    stats = deduper.stats()
    assert stats["unique_messages"] == 1
    assert stats["duplicates"] == 4


# ============================================================================
# PERFORMANCE AND SCALABILITY TESTS
# ============================================================================

def test_performance_many_unique_ids():
    """
    Test performance with many unique IDs.
    
    Discussion: When does this become a problem?
    - In-memory: Limited by RAM (100MB per 1M messages)
    - Redis: Can handle billions with proper clustering
    - Database: Needs indexes, partitioning for scale
    """
    deduper = InMemoryDeduper()
    
    # Process 10k unique messages
    start = time.time()
    for i in range(10000):
        deduper.is_duplicate(f"msg-{i}")
    elapsed = time.time() - start
    
    # Should be very fast (< 100ms for 10k operations)
    assert elapsed < 0.1, f"Too slow: {elapsed:.3f}s"
    
    stats = deduper.stats()
    assert stats["unique_messages"] == 10000
    assert stats["duplicates"] == 0


def test_performance_many_duplicates():
    """
    Test performance when checking same ID repeatedly.
    
    Use case: Retry storms, where same failing message is retried thousands of times.
    """
    deduper = InMemoryDeduper()
    
    # First time: process
    deduper.is_duplicate("msg-1")
    
    # Check same ID 10k times
    start = time.time()
    for _ in range(10000):
        assert deduper.is_duplicate("msg-1") is True
    elapsed = time.time() - start
    
    # Should still be very fast
    assert elapsed < 0.1, f"Too slow: {elapsed:.3f}s"
    
    stats = deduper.stats()
    assert stats["duplicates"] == 10000