"""
Models for Meshtastic nodes and related data
"""
import logging
import time
import sqlite3
import json
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

# Configure logger
logger = logging.getLogger(__name__)

@dataclass
class Position:
    """Position data for a node"""
    latitude: float
    longitude: float
    altitude: float = 0
    time: int = 0
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Position':
        """Create from dictionary"""
        return cls(
            latitude=data.get("latitude", 0.0),
            longitude=data.get("longitude", 0.0),
            altitude=data.get("altitude", 0.0),
            time=data.get("time", int(time.time()))
        )

@dataclass
class Message:
    """Message data"""
    text: str
    from_id: str
    to_id: str
    time: int
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], from_id: str, to_id: str) -> 'Message':
        """Create from dictionary"""
        return cls(
            text=data.get("text", ""),
            from_id=from_id,
            to_id=to_id,
            time=data.get("time", int(time.time()))
        )

class MeshtasticNode:
    """Model representing a Meshtastic node"""
    
    _db_path = "mqtt_data/meshtastic_nodes.db"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert node to dictionary for API responses"""
        position_dict = {
            "latitude": self.position.latitude,
            "longitude": self.position.longitude,
            "altitude": self.position.altitude,
            "time": self.position.time
        } if self.position else None
        
        return {
            "node_id": self.node_id,
            "name": self.name,
            "user_short_name": self.user_short_name,
            "position": position_dict,
            "last_seen": self.last_seen,
            "battery_level": self.battery_level,
            "voltage": self.voltage,
            "snr": self.snr,
            "rssi": self.rssi,
            "group": self.group,
            "category": self.category,
            "is_active": self.is_active
        }
    
    def __init__(
        self,
        node_id: str,
        name: str = "",
        user_short_name: str = "",
        position: Optional[Position] = None,
        last_seen: int = 0,
        battery_level: int = 0,
        voltage: float = 0.0,
        snr: float = 0.0,
        rssi: int = 0,
        group: str = "default",
        category: str = "unknown"
    ):
        """Initialize a Meshtastic node"""
        self.node_id = node_id
        self.name = name
        self.user_short_name = user_short_name
        self.position = position or Position(0.0, 0.0)
        self.last_seen = last_seen or int(time.time())
        self.battery_level = battery_level
        self.voltage = voltage
        self.snr = snr
        self.rssi = rssi
        self.group = group
        self.category = category
    
    @property
    def is_active(self) -> bool:
        """Check if the node is considered active"""
        return (time.time() - self.last_seen) < 3600  # 1 hour
    
    @classmethod
    def set_db_path(cls, db_path: str):
        """Set the database path"""
        cls._db_path = db_path
    
    @classmethod
    def init_db(cls) -> bool:
        """Initialize the database"""
        try:
            # Make sure directory exists
            os.makedirs(os.path.dirname(cls._db_path), exist_ok=True)
            
            # Create connection and tables
            conn = sqlite3.connect(cls._db_path)
            cursor = conn.cursor()
            
            # Create nodes table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                name TEXT,
                user_short_name TEXT,
                position TEXT,
                last_seen INTEGER,
                battery_level INTEGER,
                voltage REAL,
                snr REAL,
                rssi INTEGER,
                group_name TEXT,
                category TEXT
            )
            ''')
            
            # Create messages table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_id TEXT,
                to_id TEXT,
                text TEXT,
                time INTEGER,
                FOREIGN KEY (from_id) REFERENCES nodes (node_id)
            )
            ''')
            
            conn.commit()
            conn.close()
            
            logger.info(f"Database initialized at {cls._db_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            return False
    
    def save(self) -> bool:
        """Save the node to the database"""
        try:
            conn = sqlite3.connect(self._db_path)
            cursor = conn.cursor()
            
            # Convert position to JSON
            position_json = json.dumps({
                "latitude": self.position.latitude,
                "longitude": self.position.longitude,
                "altitude": self.position.altitude,
                "time": self.position.time
            })
            
            cursor.execute('''
            INSERT OR REPLACE INTO nodes (
                node_id, name, user_short_name, position, last_seen, 
                battery_level, voltage, snr, rssi, group_name, category
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                self.node_id, self.name, self.user_short_name, position_json, self.last_seen,
                self.battery_level, self.voltage, self.snr, self.rssi, self.group, self.category
            ))
            
            conn.commit()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving node {self.node_id}: {e}")
            return False
    
    @classmethod
    def get(cls, node_id: str) -> Optional['MeshtasticNode']:
        """Get a node by ID"""
        try:
            conn = sqlite3.connect(cls._db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM nodes WHERE node_id = ?', (node_id,))
            row = cursor.fetchone()
            
            if not row:
                return None
                
            # Parse data
            node_id, name, user_short_name, position_json, last_seen, \
                battery_level, voltage, snr, rssi, group, category = row
                
            # Parse position
            position_data = json.loads(position_json)
            position = Position.from_dict(position_data)
            
            conn.close()
            
            return cls(
                node_id=node_id,
                name=name,
                user_short_name=user_short_name,
                position=position,
                last_seen=last_seen,
                battery_level=battery_level,
                voltage=voltage,
                snr=snr,
                rssi=rssi,
                group=group,
                category=category
            )
            
        except Exception as e:
            logger.error(f"Error getting node {node_id}: {e}")
            return None
    
    @classmethod
    def get_all(cls, active_only: bool = False, group: Optional[str] = None, 
                category: Optional[str] = None) -> List['MeshtasticNode']:
        """Get all nodes with optional filtering"""
        try:
            conn = sqlite3.connect(cls._db_path)
            cursor = conn.cursor()
            
            # Build query
            query = 'SELECT * FROM nodes'
            params = []
            
            conditions = []
            
            if active_only:
                # Calculate timestamp for 1 hour ago
                one_hour_ago = int(time.time()) - 3600
                conditions.append('last_seen > ?')
                params.append(one_hour_ago)
                
            if group:
                conditions.append('group_name = ?')
                params.append(group)
                
            if category:
                conditions.append('category = ?')
                params.append(category)
                
            if conditions:
                query += ' WHERE ' + ' AND '.join(conditions)
                
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            nodes = []
            for row in rows:
                # Parse data
                node_id, name, user_short_name, position_json, last_seen, \
                    battery_level, voltage, snr, rssi, group, category = row
                    
                # Parse position
                position_data = json.loads(position_json)
                position = Position.from_dict(position_data)
                
                nodes.append(cls(
                    node_id=node_id,
                    name=name,
                    user_short_name=user_short_name,
                    position=position,
                    last_seen=last_seen,
                    battery_level=battery_level,
                    voltage=voltage,
                    snr=snr,
                    rssi=rssi,
                    group=group,
                    category=category
                ))
                
            conn.close()
            return nodes
            
        except Exception as e:
            logger.error(f"Error getting nodes: {e}")
            return []
    
    @classmethod
    def expire_inactive_nodes(cls, expiration_seconds: int = 3600) -> int:
        """Mark nodes as inactive if not seen for a while
        
        Returns:
            Number of nodes expired
        """
        try:
            # Calculate expiration timestamp
            expiration_time = int(time.time()) - expiration_seconds
            
            # Get all nodes that are active but haven't been seen since expiration time
            active_nodes = cls.get_all(active_only=True)
            expired_count = 0
            
            for node in active_nodes:
                if node.last_seen < expiration_time:
                    # Node is expired, update last_seen to show as inactive
                    node.last_seen = 0
                    node.save()
                    expired_count += 1
                    
            return expired_count
            
        except Exception as e:
            logger.error(f"Error expiring nodes: {e}")
            return 0