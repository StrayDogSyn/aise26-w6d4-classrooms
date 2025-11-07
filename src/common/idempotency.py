"""
Idempotency Utilities for Message Processing

Idempotency ensures that processing the same message multiple times has the same effect
as processing it once. This is critical in distributed systems where:
- Messages can be delivered multiple times (at-least-once delivery)
- Network issues can cause retries
- Consumer restarts can replay messages

Common Strategies:
1. In-Memory Set: Fast, but lost on restart (good for testing)
2. Redis with TTL: Distributed, auto-expires, fast (good for production)
3. Database Table: Persistent, queryable, slower (good for audit trail)
4. Idempotent Operations: Design operations to be naturally idempotent (best!)

Discussion Topics:
- How long should you store processed IDs? (Depends on message window)
- What happens if dedup cache is lost? (Duplicate processing occurs)
- Should you use different TTLs for different message types?
- When is natural idempotency better than tracking IDs?

Examples of Natural Idempotency:
- SET key=value (same result if repeated)
- INSERT ... ON CONFLICT DO NOTHING (database-level dedup)
- UPDATE status WHERE current_status='pending' (conditional update)
"""

import time
from typing import Set, Dict, Optional
from collections import OrderedDict


def process_once(cache: set, msg_id: str) -> str:
    """
    Simple idempotency check using a set.
    
    Args:
        cache: Set of already-processed message IDs
        msg_id: Unique identifier for the message
    
    Returns:
        "processed" if first time seeing this ID
        "duplicate" if already seen
    
    Use Case: Testing, single-process applications
    Limitation: No TTL, unbounded growth, lost on restart
    """
    if msg_id in cache:
        return "duplicate"
    cache.add(msg_id)
    return "processed"


class InMemoryDeduper:
    """
    In-memory idempotency tracker with basic statistics.
    
    Features:
    - Track processed message IDs
    - Count total and duplicate messages
    - Simple reset functionality
    
    Use Case: Testing, development, single-process applications
    
    Production Considerations:
    ❌ No TTL (grows unbounded)
    ❌ Lost on restart (no persistence)
    ❌ Not distributed (can't share across processes)
    ✅ Fast and simple
    """
    
    def __init__(self):
        self.seen: Set[str] = set()
        self.total_checks = 0
        self.duplicate_count = 0
    
    def is_duplicate(self, msg_id: str) -> bool:
        """Check if message has been processed before."""
        self.total_checks += 1
        
        if msg_id in self.seen:
            self.duplicate_count += 1
            return True
        
        self.seen.add(msg_id)
        return False
    
    def reset(self):
        """Clear all tracked IDs and stats."""
        self.seen.clear()
        self.total_checks = 0
        self.duplicate_count = 0
    
    def stats(self) -> dict:
        """Return statistics about deduplication."""
        return {
            "total_checks": self.total_checks,
            "unique_messages": len(self.seen),
            "duplicates": self.duplicate_count,
            "duplicate_rate": f"{self.duplicate_count / max(self.total_checks, 1) * 100:.1f}%"
        }


class TTLDeduper:
    """
    Time-based idempotency tracker with automatic expiration.
    
    Features:
    - TTL (Time To Live) for each message ID
    - Automatic cleanup of expired entries
    - Bounded memory usage
    
    Use Case: Production-like behavior without Redis dependency
    
    Discussion: How does this compare to Redis?
    - Similarities: TTL-based expiration, bounded memory
    - Differences: Not distributed, manual cleanup needed
    - When to use: Single-process or testing Redis integration
    
    Implementation Note:
    - Uses OrderedDict for efficient oldest-first cleanup
    - Stores (msg_id -> expiry_timestamp) mappings
    """
    
    def __init__(self, ttl_seconds: int = 3600):
        """
        Initialize TTL-based deduper.
        
        Args:
            ttl_seconds: How long to remember each message ID (default: 1 hour)
        
        Discussion: How to choose TTL?
        - Too short: Risk of duplicate processing
        - Too long: Memory waste, slow lookups
        - Good rule: 2x maximum expected message delay
        """
        self.ttl_seconds = ttl_seconds
        self.entries: OrderedDict[str, float] = OrderedDict()
    
    def _cleanup_expired(self):
        """Remove entries that have exceeded their TTL."""
        now = time.time()
        
        # Remove expired entries from the front (oldest first)
        expired_keys = []
        for msg_id, expiry in self.entries.items():
            if expiry < now:
                expired_keys.append(msg_id)
            else:
                break  # OrderedDict is sorted by insertion time
        
        for key in expired_keys:
            del self.entries[key]
    
    def is_duplicate(self, msg_id: str) -> bool:
        """
        Check if message has been processed within TTL window.
        
        Side effect: Cleans up expired entries
        """
        self._cleanup_expired()
        
        if msg_id in self.entries:
            return True
        
        # Add with expiry timestamp
        expiry = time.time() + self.ttl_seconds
        self.entries[msg_id] = expiry
        return False
    
    def size(self) -> int:
        """Return number of tracked message IDs (after cleanup)."""
        self._cleanup_expired()
        return len(self.entries)


class SimulatedRedisDeduper:
    """
    Simulates Redis-based idempotency tracking (for testing without Redis).
    
    Real Redis Implementation:
    
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        
        def is_duplicate(msg_id: str) -> bool:
            key = f"processed:{msg_id}"
            # SETNX returns True if key didn't exist (first time)
            is_new = r.setnx(key, "1")
            if is_new:
                r.expire(key, 3600)  # Set TTL
                return False
            return True
    
    Discussion Topics:
    - Why use Redis instead of database? (Speed, TTL support, distributed)
    - What if Redis goes down? (Fallback to database, or accept duplicate risk)
    - Should you use Redis cluster? (Yes, for high availability)
    - Alternative: Use Redis SETEX for atomic set+expire
    
    Production Best Practices:
    1. Use connection pooling
    2. Set reasonable timeouts
    3. Handle Redis failures gracefully
    4. Monitor Redis memory usage
    5. Use appropriate eviction policy (allkeys-lru)
    """
    
    def __init__(self, ttl_seconds: int = 3600):
        """
        Simulate Redis with in-memory dict.
        
        In production: Replace with actual redis.Redis() client
        """
        self.ttl_seconds = ttl_seconds
        self.store: Dict[str, float] = {}  # msg_id -> expiry_timestamp
    
    def is_duplicate(self, msg_id: str) -> bool:
        """
        Check if message has been processed (simulates Redis SETNX + EXPIRE).
        
        Redis equivalent:
            pipe = redis.pipeline()
            pipe.setnx(f"msg:{msg_id}", "1")
            pipe.expire(f"msg:{msg_id}", ttl_seconds)
            results = pipe.execute()
            return not results[0]  # SETNX returns False if key exists
        """
        now = time.time()
        
        # Check if exists and not expired
        if msg_id in self.store:
            if self.store[msg_id] > now:
                return True  # Duplicate within TTL window
            else:
                # Expired, remove it
                del self.store[msg_id]
        
        # First time seeing this ID, store with expiry
        self.store[msg_id] = now + self.ttl_seconds
        return False
    
    def delete(self, msg_id: str):
        """Remove a message ID from tracking (simulates Redis DEL)."""
        self.store.pop(msg_id, None)
    
    def exists(self, msg_id: str) -> bool:
        """Check if message ID exists without adding it."""
        if msg_id not in self.store:
            return False
        
        # Check if expired
        if self.store[msg_id] < time.time():
            del self.store[msg_id]
            return False
        
        return True


class DatabaseDeduper:
    """
    Database-backed idempotency tracker (pseudo-implementation).
    
    SQL Schema:
    
        CREATE TABLE processed_messages (
            message_id TEXT PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            handler_name TEXT,
            INDEX idx_processed_at (processed_at)
        );
        
        -- Cleanup old entries (run periodically)
        DELETE FROM processed_messages 
        WHERE processed_at < NOW() - INTERVAL '7 days';
    
    Implementation Patterns:
    
    1. Check Before Process (2 queries):
        SELECT 1 FROM processed_messages WHERE message_id = ?
        -- If not found, process message, then:
        INSERT INTO processed_messages (message_id) VALUES (?)
    
    2. Insert First (1 query, better):
        INSERT INTO processed_messages (message_id) 
        VALUES (?) 
        ON CONFLICT (message_id) DO NOTHING
        RETURNING message_id;
        -- If returns row, it's new; if empty, it's duplicate
    
    3. Optimistic Locking (for updates):
        UPDATE orders SET status='shipped', version=version+1
        WHERE order_id=? AND version=? AND status='pending'
        -- If rows_affected=0, already processed
    
    Discussion:
    - Why is INSERT...ON CONFLICT better? (Atomic, no race conditions)
    - When to cleanup old entries? (Cron job, or on each insert)
    - Should you index processed_at? (Yes, for efficient cleanup)
    - What about very high throughput? (Use Redis, or partition table)
    
    Trade-offs:
    ✅ Persistent (survives restarts)
    ✅ Queryable (audit trail)
    ✅ Distributed (multiple processes can share)
    ❌ Slower than Redis
    ❌ Requires cleanup job
    ❌ Database load
    """
    
    def __init__(self, connection_string: str):
        """
        Initialize database deduper.
        
        Args:
            connection_string: Database connection (e.g., PostgreSQL, MySQL)
        
        Note: This is a pseudo-implementation for learning purposes.
        In production, use actual database driver (psycopg2, pymysql, etc.)
        """
        self.connection_string = connection_string
        # In production: self.conn = psycopg2.connect(connection_string)
        
        # Simulate with in-memory dict
        self._simulated_db: Dict[str, float] = {}
    
    def is_duplicate(self, msg_id: str) -> bool:
        """
        Check if message has been processed using database.
        
        Real implementation:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO processed_messages (message_id)
                VALUES (%s)
                ON CONFLICT (message_id) DO NOTHING
                RETURNING message_id
            ''', (msg_id,))
            result = cursor.fetchone()
            self.conn.commit()
            return result is None  # None means conflict (duplicate)
        """
        if msg_id in self._simulated_db:
            return True
        
        self._simulated_db[msg_id] = time.time()
        return False
    
    def cleanup_old_entries(self, days: int = 7):
        """
        Remove entries older than specified days.
        
        Real implementation:
            cursor = self.conn.cursor()
            cursor.execute('''
                DELETE FROM processed_messages
                WHERE processed_at < NOW() - INTERVAL '%s days'
            ''', (days,))
            self.conn.commit()
            return cursor.rowcount
        """
        cutoff = time.time() - (days * 86400)
        before_count = len(self._simulated_db)
        
        # Remove old entries
        self._simulated_db = {
            k: v for k, v in self._simulated_db.items()
            if v >= cutoff
        }
        
        return before_count - len(self._simulated_db)
