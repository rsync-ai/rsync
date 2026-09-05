"""
Entity Statistics for Hot Entity Detection.

Tracks message throughput, record counts, and data sizes to determine
when an entity should use record-level partitioning for scalability.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, List
from datetime import datetime, timedelta
from threading import Lock
from .kafka_message import ConnectorCategory, CONNECTOR_CATEGORIES


@dataclass
class HotThresholds:
    """Thresholds for determining if an entity is "hot"."""
    messages_per_second: float = 1000.0
    record_count: int = 1_000_000
    size_gb: float = 10.0


# Default hot thresholds by connector category
DEFAULT_HOT_THRESHOLDS: Dict[ConnectorCategory, HotThresholds] = {
    ConnectorCategory.DATABASE: HotThresholds(
        messages_per_second=1000,
        record_count=1_000_000,
        size_gb=10,
    ),
    ConnectorCategory.NOSQL: HotThresholds(
        messages_per_second=2000,
        record_count=5_000_000,
        size_gb=20,
    ),
    ConnectorCategory.API: HotThresholds(
        messages_per_second=500,
        record_count=100_000,
        size_gb=5,
    ),
    ConnectorCategory.STORAGE: HotThresholds(
        messages_per_second=100,
        record_count=0,  # Not applicable
        size_gb=50,
    ),
    ConnectorCategory.STREAMING: HotThresholds(
        messages_per_second=5000,
        record_count=0,
        size_gb=50,
    ),
    ConnectorCategory.QUEUE: HotThresholds(
        messages_per_second=5000,
        record_count=0,
        size_gb=50,
    ),
}


@dataclass
class MessageTimestamp:
    """A message count at a specific time."""
    count: int
    timestamp: datetime


@dataclass
class EntityStats:
    """Statistics for an entity (table, collection, object type, etc.)."""
    
    entity_id: str
    connector_type: str
    
    # Metrics
    message_count: int = 0
    messages_per_second: float = 0.0
    size_bytes: int = 0
    unique_records: int = 0
    
    # Time tracking
    first_seen: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)
    
    # Rolling window for throughput (not serialized)
    _message_window: List[MessageTimestamp] = field(default_factory=list)
    _lock: Lock = field(default_factory=Lock, repr=False)
    
    @property
    def size_gb(self) -> float:
        """Return size in gigabytes."""
        return self.size_bytes / (1024 ** 3)
    
    def record_message(self, size_bytes: int = 0) -> None:
        """Record a new message."""
        with self._lock:
            now = datetime.now()
            self.message_count += 1
            self.size_bytes += size_bytes
            self.last_updated = now
            
            # Add to rolling window
            self._message_window.append(MessageTimestamp(count=1, timestamp=now))
            
            # Clean old entries
            self._clean_window(now)
            
            # Recalculate throughput
            self._calculate_throughput()
    
    def _clean_window(self, now: datetime) -> None:
        """Remove entries older than 60 seconds."""
        cutoff = now - timedelta(seconds=60)
        self._message_window = [
            entry for entry in self._message_window
            if entry.timestamp > cutoff
        ]
    
    def _calculate_throughput(self) -> None:
        """Calculate messages per second over the window."""
        if len(self._message_window) < 2:
            self.messages_per_second = 0.0
            return
        
        total_messages = sum(entry.count for entry in self._message_window)
        duration = (
            self._message_window[-1].timestamp - 
            self._message_window[0].timestamp
        ).total_seconds()
        
        if duration > 0:
            self.messages_per_second = total_messages / duration
    
    def is_hot_entity(self) -> bool:
        """Determine if this entity should use record-level partitioning."""
        category = CONNECTOR_CATEGORIES.get(
            self.connector_type.lower(),
            ConnectorCategory.API
        )
        thresholds = DEFAULT_HOT_THRESHOLDS.get(
            category,
            DEFAULT_HOT_THRESHOLDS[ConnectorCategory.API]
        )
        
        return self.is_hot_with_thresholds(thresholds)
    
    def is_hot_with_thresholds(self, thresholds: HotThresholds) -> bool:
        """Check if entity is hot using custom thresholds."""
        # High throughput
        if thresholds.messages_per_second > 0:
            if self.messages_per_second > thresholds.messages_per_second:
                return True
        
        # Many unique records
        if thresholds.record_count > 0:
            if self.unique_records > thresholds.record_count:
                return True
        
        # Large data volume
        if thresholds.size_gb > 0:
            if self.size_gb > thresholds.size_gb:
                return True
        
        return False
    
    def to_dict(self) -> Dict:
        """Convert to dictionary (for serialization)."""
        return {
            "entity_id": self.entity_id,
            "connector_type": self.connector_type,
            "message_count": self.message_count,
            "messages_per_second": self.messages_per_second,
            "size_bytes": self.size_bytes,
            "size_gb": self.size_gb,
            "unique_records": self.unique_records,
            "first_seen": self.first_seen.isoformat(),
            "last_updated": self.last_updated.isoformat(),
            "is_hot": self.is_hot_entity(),
        }


class EntityStatsRegistry:
    """Manages statistics for all entities."""
    
    def __init__(self):
        self._stats: Dict[str, EntityStats] = {}
        self._lock = Lock()
    
    def get_or_create(self, entity_id: str, connector_type: str) -> EntityStats:
        """Get existing stats or create new ones."""
        with self._lock:
            if entity_id not in self._stats:
                self._stats[entity_id] = EntityStats(
                    entity_id=entity_id,
                    connector_type=connector_type,
                )
            return self._stats[entity_id]
    
    def get(self, entity_id: str) -> Optional[EntityStats]:
        """Get stats for an entity (or None if not found)."""
        with self._lock:
            return self._stats.get(entity_id)
    
    def is_hot_entity(self, entity_id: str) -> bool:
        """Check if an entity is hot."""
        with self._lock:
            stats = self._stats.get(entity_id)
            if stats:
                return stats.is_hot_entity()
            return False
    
    def record_message(self, entity_id: str, connector_type: str, size_bytes: int = 0) -> None:
        """Record a message for an entity."""
        stats = self.get_or_create(entity_id, connector_type)
        stats.record_message(size_bytes)
    
    def get_all_stats(self) -> Dict[str, EntityStats]:
        """Get a copy of all stats."""
        with self._lock:
            return dict(self._stats)
    
    def get_hot_entities(self) -> List[str]:
        """Get all hot entity IDs."""
        with self._lock:
            return [
                entity_id
                for entity_id, stats in self._stats.items()
                if stats.is_hot_entity()
            ]
    
    def get_stats_summary(self) -> Dict:
        """Get a summary of all stats."""
        with self._lock:
            return {
                "total_entities": len(self._stats),
                "hot_entities": len(self.get_hot_entities()),
                "total_messages": sum(s.message_count for s in self._stats.values()),
                "total_size_gb": sum(s.size_gb for s in self._stats.values()),
                "entities": [s.to_dict() for s in self._stats.values()],
            }


# Global registry instance
_global_registry: Optional[EntityStatsRegistry] = None


def get_global_registry() -> EntityStatsRegistry:
    """Get the global entity stats registry."""
    global _global_registry
    if _global_registry is None:
        _global_registry = EntityStatsRegistry()
    return _global_registry
