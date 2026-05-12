import json
import hashlib
from datetime import datetime, timedelta
from typing import Any, Optional
import threading

# Simple in-memory cache (safer than Redis for production)
class CacheManager:
    def __init__(self):
        self._cache = {}
        self._timestamps = {}
        self._lock = threading.Lock()
    
    def _make_key(self, prefix: str, **kwargs) -> str:
        """Generate cache key from parameters"""
        key_data = f"{prefix}:{json.dumps(kwargs, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached data"""
        with self._lock:
            if key in self._cache:
                timestamp = self._timestamps.get(key, 0)
                if datetime.now().timestamp() - timestamp < 300:  # 5 minutes
                    return self._cache[key]
                else:
                    # Expired - remove
                    del self._cache[key]
                    del self._timestamps[key]
        return None
    
    def set(self, key: str, value: Any, ttl: int = 300) -> None:
        """Set cache data with TTL in seconds"""
        with self._lock:
            self._cache[key] = value
            self._timestamps[key] = datetime.now().timestamp()
    
    def invalidate_pattern(self, pattern: str) -> None:
        """Invalidate cache keys matching pattern"""
        with self._lock:
            keys_to_remove = []
            for key in self._cache.keys():
                if pattern in key:
                    keys_to_remove.append(key)
            for key in keys_to_remove:
                if key in self._cache:
                    del self._cache[key]
                if key in self._timestamps:
                    del self._timestamps[key]

# Global cache instance
cache = CacheManager()

from functools import wraps

def cache_result(prefix: str, ttl: int = 300):
    """Decorator to cache function results"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Filter out None values and convert to strings for consistent caching
            filtered_kwargs = {k: str(v) if v is not None else 'None' for k, v in kwargs.items()}
            
            # Generate cache key
            cache_key = cache._make_key(prefix, **filtered_kwargs)
            
            # Try to get from cache
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            cache.set(cache_key, result, ttl)
            return result
        return wrapper
    return decorator
