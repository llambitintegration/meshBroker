"""
Database model for Meshtastic nodes
"""
import time
import json
import logging
from typing import Optional, Dict, List, Any, ClassVar
from dataclasses import dataclass, field, asdict
import sqlite3

# Configure logger
logger = logging.getLogger(__name__)

@dataclass
class Position:
    """Position information for a node"""
    latitude: float = 0.0
    longitude: float = 0.0
    altitude: float = 0.0
    timestamp: int = 0  # Unix timestamp
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Position':
        """Create a Position object from a dictionary"""
        return cls(
            latitude=data.get('latitude', 0.0),
            longitude=data.get('longitude', 0.0),
            altitude=data.get('altitude', 0.0),
            timestamp=data.get('timestamp', 0) or data.get('time', 0)
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return asdict(self)

@dataclass
class Message:
    """Text message from a node"""
    text: str
    from_id: str
    to_id: str
    timestamp: int  # Unix timestamp
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        """Create a Message object from a dictionary"""
        return cls(
            text=data.get('text', ''),
            from_id=data.get('from', ''),
            to_id=data.get('to', ''),
            timestamp=data.get('timestamp', 0) or data.get('time', 0)
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return asdict(self)

@dataclass
class MeshtasticNode:
    """Meshtastic node information"""
    # Core fields
    node_id: str
    last_seen: float = field(default_factory=time.time)  # Unix timestamp
    message_count: int = 0
    
    # Node identity
    name: str = "Unknown"
    short_name: str = ""
    hardware: str = ""
    
    # Grouping and categorization
    group: str = "default"
    category: str = "uncategorized"
    
    # Position information
    position: Optional[Position] = None
    
    # Messages (transient, not stored in DB directly)
    messages: List[Message] = field(default_factory=list)
    
    # Additional data fields
    telemetry: Optional[Dict[str, Any]] = None
    heartbeat: Optional[Dict[str, Any]] = None
    last_heartbeat_time: float = 0
    
    # Custom attributes
    attributes: Dict[str, Any] = field(default_factory=dict)
    
    # Status
    is_active: bool = True
    
    # Database configuration
    _db_path: ClassVar[str] = "mqtt_data/meshtastic_nodes.db"
    
    @classmethod
    def set_db_path(cls, path: str):
        """Set the database path"""
        cls._db_path = path
        
    @classmethod
    def init_db(cls):
        """Initialize the database for storing nodes"""
        try:
            conn = sqlite3.connect(cls._db_path)
            cursor = conn.cursor()
            
            # Create nodes table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                last_seen REAL NOT NULL,
                message_count INTEGER NOT NULL,
                name TEXT NOT NULL,
                short_name TEXT NOT NULL,
                hardware TEXT NOT NULL,
                group_name TEXT NOT NULL,
                category TEXT NOT NULL,
                position_json TEXT,
                telemetry_json TEXT,
                heartbeat_json TEXT,
                last_heartbeat_time REAL NOT NULL,
                attributes_json TEXT,
                is_active INTEGER NOT NULL
            )
            ''')
            
            # Create messages table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS node_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT NOT NULL,
                text TEXT NOT NULL,
                from_id TEXT NOT NULL,
                to_id TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                FOREIGN KEY (node_id) REFERENCES nodes (node_id)
            )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Meshtastic nodes database initialized")
            return True
        except Exception as e:
            logger.error(f"Error initializing nodes database: {e}")
            return False
    
    def save(self) -> bool:
        """Save the node to the database"""
        try:
            conn = sqlite3.connect(self._db_path)
            cursor = conn.cursor()
            
            position_json = json.dumps(self.position.to_dict()) if self.position else None
            telemetry_json = json.dumps(self.telemetry) if self.telemetry else None
            heartbeat_json = json.dumps(self.heartbeat) if self.heartbeat else None
            attributes_json = json.dumps(self.attributes) if self.attributes else None
            
            cursor.execute(
                """
                INSERT OR REPLACE INTO nodes
                (node_id, last_seen, message_count, name, short_name, hardware, 
                group_name, category, position_json, telemetry_json, heartbeat_json, 
                last_heartbeat_time, attributes_json, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.node_id,
                    self.last_seen,
                    self.message_count,
                    self.name,
                    self.short_name,
                    self.hardware,
                    self.group,
                    self.category,
                    position_json,
                    telemetry_json,
                    heartbeat_json,
                    self.last_heartbeat_time,
                    attributes_json,
                    1 if self.is_active else 0
                )
            )
            
            # Store messages (limited to last 100)
            if self.messages:
                # First, delete existing messages
                cursor.execute("DELETE FROM node_messages WHERE node_id = ?", (self.node_id,))
                
                # Then insert new messages (up to 100)
                for message in self.messages[-100:]:
                    cursor.execute(
                        """
                        INSERT INTO node_messages
                        (node_id, text, from_id, to_id, timestamp)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            self.node_id,
                            message.text,
                            message.from_id,
                            message.to_id,
                            message.timestamp
                        )
                    )
            
            conn.commit()
            conn.close()
            logger.debug(f"Saved node {self.node_id} to database")
            return True
        except Exception as e:
            logger.error(f"Error saving node to database: {e}")
            return False
    
    @classmethod
    def get(cls, node_id: str) -> Optional['MeshtasticNode']:
        """Get a node from the database by its ID"""
        try:
            conn = sqlite3.connect(cls._db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM nodes WHERE node_id = ?", (node_id,))
            row = cursor.fetchone()
            
            if not row:
                conn.close()
                return None
            
            # Create the node
            node = cls(
                node_id=row['node_id'],
                last_seen=row['last_seen'],
                message_count=row['message_count'],
                name=row['name'],
                short_name=row['short_name'],
                hardware=row['hardware'],
                group=row['group_name'],
                category=row['category'],
                last_heartbeat_time=row['last_heartbeat_time'],
                is_active=bool(row['is_active'])
            )
            
            # Parse optional JSON fields
            if row['position_json']:
                position_data = json.loads(row['position_json'])
                node.position = Position.from_dict(position_data)
            
            if row['telemetry_json']:
                node.telemetry = json.loads(row['telemetry_json'])
            
            if row['heartbeat_json']:
                node.heartbeat = json.loads(row['heartbeat_json'])
            
            if row['attributes_json']:
                node.attributes = json.loads(row['attributes_json'])
            
            # Load messages
            cursor.execute(
                "SELECT * FROM node_messages WHERE node_id = ? ORDER BY timestamp DESC LIMIT 100",
                (node_id,)
            )
            message_rows = cursor.fetchall()
            
            node.messages = [
                Message(
                    text=msg_row['text'],
                    from_id=msg_row['from_id'],
                    to_id=msg_row['to_id'],
                    timestamp=msg_row['timestamp']
                )
                for msg_row in message_rows
            ]
            
            conn.close()
            return node
        except Exception as e:
            logger.error(f"Error getting node from database: {e}")
            return None
    
    @classmethod
    def get_all(cls, active_only: bool = False, group: Optional[str] = None, 
               category: Optional[str] = None) -> List['MeshtasticNode']:
        """Get all nodes from the database with optional filtering"""
        try:
            conn = sqlite3.connect(cls._db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Build the query based on filters
            query = "SELECT * FROM nodes"
            params = []
            
            conditions = []
            if active_only:
                conditions.append("is_active = 1")
            
            if group:
                conditions.append("group_name = ?")
                params.append(group)
            
            if category:
                conditions.append("category = ?")
                params.append(category)
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            nodes = []
            for row in rows:
                node = cls(
                    node_id=row['node_id'],
                    last_seen=row['last_seen'],
                    message_count=row['message_count'],
                    name=row['name'],
                    short_name=row['short_name'],
                    hardware=row['hardware'],
                    group=row['group_name'],
                    category=row['category'],
                    last_heartbeat_time=row['last_heartbeat_time'],
                    is_active=bool(row['is_active'])
                )
                
                # Parse optional JSON fields
                if row['position_json']:
                    position_data = json.loads(row['position_json'])
                    node.position = Position.from_dict(position_data)
                
                if row['telemetry_json']:
                    node.telemetry = json.loads(row['telemetry_json'])
                
                if row['heartbeat_json']:
                    node.heartbeat = json.loads(row['heartbeat_json'])
                
                if row['attributes_json']:
                    node.attributes = json.loads(row['attributes_json'])
                
                nodes.append(node)
            
            conn.close()
            return nodes
        except Exception as e:
            logger.error(f"Error getting nodes from database: {e}")
            return []
    
    @classmethod
    def delete(cls, node_id: str) -> bool:
        """Delete a node from the database"""
        try:
            conn = sqlite3.connect(cls._db_path)
            cursor = conn.cursor()
            
            # Delete messages first
            cursor.execute("DELETE FROM node_messages WHERE node_id = ?", (node_id,))
            
            # Then delete the node
            cursor.execute("DELETE FROM nodes WHERE node_id = ?", (node_id,))
            
            conn.commit()
            conn.close()
            logger.debug(f"Deleted node {node_id} from database")
            return True
        except Exception as e:
            logger.error(f"Error deleting node from database: {e}")
            return False
    
    @classmethod
    def expire_inactive_nodes(cls, timeout_seconds: int = 3600) -> int:
        """Mark nodes as inactive if not seen within timeout period"""
        try:
            current_time = time.time()
            cutoff_time = current_time - timeout_seconds
            
            conn = sqlite3.connect(cls._db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "UPDATE nodes SET is_active = 0 WHERE last_seen < ? AND is_active = 1",
                (cutoff_time,)
            )
            
            expired_count = cursor.rowcount
            conn.commit()
            conn.close()
            
            logger.info(f"Expired {expired_count} inactive nodes")
            return expired_count
        except Exception as e:
            logger.error(f"Error expiring inactive nodes: {e}")
            return 0 