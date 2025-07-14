import requests
import time
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from bs4 import BeautifulSoup

from database_manager import DatabaseManager
from config import Config

# Configure module-level logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    """Base class for all scrapers"""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.config = Config()
        self.session = requests.Session()
        self.session.headers.update(self.config.DEFAULT_HEADERS)

    @abstractmethod
    def scrape_league(self, league: str) -> List[Dict[str, Any]]:
        """Scrape data for a specific league"""
        ...

    @abstractmethod
    def parse_player_data(self, html: str, league: str) -> List[Dict[str, Any]]:
        """Parse player data from HTML"""
        ...

    def make_request(self, url: str, use_cache: bool = True) -> Optional[str]:
        """Make HTTP request with caching and rate limiting"""
        if use_cache:
            cached = self.db.cache_get(url)
            if cached:
                logger.info(f"Using cached data for {url}")
                return cached

        time.sleep(self.config.RATE_LIMIT_DELAY_SECONDS)

        try:
            response = self.session.get(url, timeout=self.config.REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()

            if use_cache:
                self.db.cache_set(url, response.text, ttl_seconds=self.config.CACHE_TTL_SECONDS)

            logger.info(f"Successfully fetched {url}")
            return response.text

        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    def clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        # collapse whitespace and remove newlines/tabs
        return ' '.join(text.split())

    def parse_number(self, text: str, default: int = 0) -> int:
        """Parse integer from text, return default on failure"""
        try:
            cleaned = ''.join(ch for ch in text if ch.isdigit())
            return int(cleaned) if cleaned else default
        except Exception:
            return default

    def parse_float(self, text: str, default: float = 0.0) -> float:
        """Parse float from text (handles '%' and commas), return default on failure"""
        try:
            cleaned = text.replace('%', '').replace(',', '.')
            return float(cleaned)
        except Exception:
            return default
