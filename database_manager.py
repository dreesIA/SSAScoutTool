import sqlite3
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages SQLite database operations for soccer scouting data"""
    
    def __init__(self, db_path: str = "soccer_scout.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize all database tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Players table with comprehensive stats
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id TEXT UNIQUE,
                name TEXT NOT NULL,
                age INTEGER,
                position TEXT,
                club TEXT,
                league TEXT,
                nationality TEXT,
                market_value REAL,
                rating REAL,
                goals INTEGER DEFAULT 0,
                assists INTEGER DEFAULT 0,
                matches INTEGER DEFAULT 0,
                minutes_played INTEGER DEFAULT 0,
                pass_accuracy REAL,
                shots_per_game REAL,
                key_passes REAL,
                dribbles REAL,
                aerial_duels REAL,
                tackles REAL,
                interceptions REAL,
                clearances REAL,
                yellow_cards INTEGER DEFAULT 0,
                red_cards INTEGER DEFAULT 0,
                fbref_id TEXT,
                transfermarkt_id TEXT,
                asa_id TEXT,
                sofascore_id TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Teams table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS teams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                league TEXT,
                avg_age REAL,
                market_value REAL,
                total_players INTEGER,
                stadium TEXT,
                manager TEXT,
                founded_year INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Cache table for API responses
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                value TEXT,
                expiry TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Sync history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                league TEXT,
                source TEXT,
                status TEXT,
                records_synced INTEGER,
                error_message TEXT,
                started_at TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)
        
        # User watchlist table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS watchlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                player_id INTEGER,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notes TEXT,
                FOREIGN KEY (player_id) REFERENCES players (id)
            )
        """)
        
        # Create indexes for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_league ON players(league)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_position ON players(position)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_rating ON players(rating)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cache_expiry ON cache(expiry)")
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")

    def get_connection(self) -> sqlite3.Connection:
        """Get database connection with row factory"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def cache_get(self, key: str) -> Optional[str]:
        """Get cached value if not expired"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT value FROM cache WHERE key = ? AND expiry > ?",
            (key, datetime.now())
        )
        result = cursor.fetchone()
        conn.close()
        
        return result["value"] if result else None
    
    def cache_set(self, key: str, value: str, ttl_seconds: int = 3600) -> None:
        """Set cached value with TTL"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        expiry = datetime.now() + timedelta(seconds=ttl_seconds)
        cursor.execute(
            """
            INSERT OR REPLACE INTO cache (key, value, expiry, created_at) 
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (key, value, expiry)
        )
        
        conn.commit()
        conn.close()
    
    def clear_expired_cache(self) -> int:
        """Remove expired cache entries"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        deleted = cursor.execute(
            "DELETE FROM cache WHERE expiry < ?",
            (datetime.now(),)
        ).rowcount
        
        conn.commit()
        conn.close()
        
        if deleted > 0:
            logger.info(f"Cleared {deleted} expired cache entries")
        
        return deleted
    
    def log_sync(
        self,
        league: str,
        source: str,
        status: str,
        records: int = 0,
        error: Optional[str] = None
    ) -> int:
        """Log sync operation"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO sync_history 
            (league, source, status, records_synced, error_message, started_at, completed_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (league, source, status, records, error)
        )
        
        sync_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return sync_id
    
    def get_sync_history(self, limit: int = 10) -> list[Dict[str, Any]]:
        """Get recent sync history"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT * FROM sync_history 
            ORDER BY started_at DESC 
            LIMIT ?
            """,
            (limit,)
        )
        
        history = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return history
