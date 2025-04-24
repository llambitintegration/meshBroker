import os
import json
import time
import logging
import sqlite3
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict

# Configure logger
logger = logging.getLogger(__name__)

@dataclass
class StoredMessage:
    """Class for storing MQTT messages"""
    id: str  # Unique ID for the message
    topic: str  # MQTT topic
    payload: str  # Message payload (JSON string)
    qos: int  # Quality of Service level
    retain: bool  # Retain flag
    timestamp: float  # When the message was stored
    retry_count: int = 0  # Number of delivery attempts
    next_retry: Optional[float] = None  # When to retry delivery
    status: str = "pending"  # Message status: pending, delivered, failed


class MessageStore:
    """Persistent store for MQTT messages with disk backup"""
    
    def __init__(self, db_path: str = "mqtt_messages.db", max_retries: int = 5):
        """Initialize the message store"""
        self.db_path = db_path
        self.max_retries = max_retries
        self._init_db()
        logger.info(f"Message store initialized with database at {db_path}")
    
    def _init_db(self):
        """Initialize the SQLite database for message storage"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create messages table if it doesn't exist
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                topic TEXT NOT NULL,
                payload TEXT NOT NULL,
                qos INTEGER NOT NULL,
                retain INTEGER NOT NULL,
                timestamp REAL NOT NULL,
                retry_count INTEGER NOT NULL,
                next_retry REAL,
                status TEXT NOT NULL
            )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Message database initialized")
        except Exception as e:
            logger.error(f"Error initializing message database: {e}")
            raise
    
    def store_message(self, message_id: str, topic: str, payload: Any, qos: int = 0, 
                     retain: bool = False, status: str = "pending") -> bool:
        """Store a message in the database"""
        try:
            # Convert payload to string if it's not already
            if isinstance(payload, dict) or isinstance(payload, list):
                payload = json.dumps(payload)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                """
                INSERT INTO messages 
                (id, topic, payload, qos, retain, timestamp, retry_count, next_retry, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message_id,
                    topic,
                    payload,
                    qos,
                    1 if retain else 0,
                    time.time(),
                    0,
                    None,
                    status
                )
            )
            
            conn.commit()
            conn.close()
            logger.debug(f"Stored message {message_id} for topic {topic}")
            return True
        except Exception as e:
            logger.error(f"Error storing message: {e}")
            return False
    
    def update_message_status(self, message_id: str, status: str, 
                             retry_count: Optional[int] = None, 
                             next_retry: Optional[float] = None) -> bool:
        """Update the status of a message"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            update_fields = ["status = ?"]
            params = [status]
            
            if retry_count is not None:
                update_fields.append("retry_count = ?")
                params.append(retry_count)
            
            if next_retry is not None:
                update_fields.append("next_retry = ?")
                params.append(next_retry)
            
            params.append(message_id)
            
            cursor.execute(
                f"UPDATE messages SET {', '.join(update_fields)} WHERE id = ?",
                params
            )
            
            conn.commit()
            conn.close()
            logger.debug(f"Updated message {message_id} status to {status}")
            return True
        except Exception as e:
            logger.error(f"Error updating message status: {e}")
            return False
    
    def get_pending_messages(self, limit: int = 100) -> List[StoredMessage]:
        """Get pending messages that are ready for delivery"""
        try:
            current_time = time.time()
            
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute(
                """
                SELECT * FROM messages 
                WHERE status = 'pending' AND (next_retry IS NULL OR next_retry <= ?) 
                AND retry_count < ?
                ORDER BY timestamp ASC
                LIMIT ?
                """,
                (current_time, self.max_retries, limit)
            )
            
            rows = cursor.fetchall()
            conn.close()
            
            messages = []
            for row in rows:
                messages.append(StoredMessage(
                    id=row['id'],
                    topic=row['topic'],
                    payload=row['payload'],
                    qos=row['qos'],
                    retain=bool(row['retain']),
                    timestamp=row['timestamp'],
                    retry_count=row['retry_count'],
                    next_retry=row['next_retry'],
                    status=row['status']
                ))
            
            logger.debug(f"Retrieved {len(messages)} pending messages")
            return messages
        except Exception as e:
            logger.error(f"Error getting pending messages: {e}")
            return []
    
    def delete_message(self, message_id: str) -> bool:
        """Delete a message from the store"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("DELETE FROM messages WHERE id = ?", (message_id,))
            
            conn.commit()
            conn.close()
            logger.debug(f"Deleted message {message_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting message: {e}")
            return False
    
    def cleanup_old_messages(self, max_age_hours: int = 24) -> int:
        """Clean up delivered or failed messages older than max_age_hours"""
        try:
            max_age_seconds = max_age_hours * 3600
            cutoff_time = time.time() - max_age_seconds
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                """
                DELETE FROM messages 
                WHERE (status = 'delivered' OR status = 'failed') AND timestamp < ?
                """,
                (cutoff_time,)
            )
            
            deleted_count = cursor.rowcount
            conn.commit()
            conn.close()
            
            logger.info(f"Cleaned up {deleted_count} old messages")
            return deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up old messages: {e}")
            return 0
    
    def get_message_count_by_status(self) -> Dict[str, int]:
        """Get count of messages grouped by status"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT status, COUNT(*) as count FROM messages GROUP BY status"
            )
            
            results = cursor.fetchall()
            conn.close()
            
            counts = {"pending": 0, "delivered": 0, "failed": 0}
            for status, count in results:
                counts[status] = count
            
            return counts
        except Exception as e:
            logger.error(f"Error getting message counts: {e}")
            return {"pending": 0, "delivered": 0, "failed": 0}
    
    def calculate_backoff_time(self, retry_count: int, base_delay: float = 5.0) -> float:
        """Calculate exponential backoff time based on retry count"""
        # Exponential backoff: base_delay * 2^retry_count with some jitter
        import random
        max_delay = 300  # 5 minutes max
        delay = min(base_delay * (2 ** retry_count), max_delay)
        # Add jitter (±20%)
        jitter = delay * 0.2
        delay += random.uniform(-jitter, jitter)
        return time.time() + max(delay, 0.1)  # Ensure positive delay