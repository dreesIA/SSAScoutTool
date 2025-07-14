import os
import logging
from typing import Dict, Any

from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Configure logging for this module
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Config:
    """Application configuration"""

    # Database
    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "soccer_scout.db")

    # API Keys
    FBREF_API_KEY: str = os.getenv("FBREF_API_KEY", "")
    TRANSFERMARKT_API_KEY: str = os.getenv("TRANSFERMARKT_API_KEY", "")
    ASA_API_KEY: str = os.getenv("ASA_API_KEY", "")
    SOFASCORE_API_KEY: str = os.getenv("SOFASCORE_API_KEY", "")

    # Cache Settings
    CACHE_TTL_HOURS: int = int(os.getenv("CACHE_TTL_HOURS", "6"))
    CACHE_TTL_SECONDS: int = CACHE_TTL_HOURS * 3600
    MAX_CACHE_SIZE_MB: int = int(os.getenv("MAX_CACHE_SIZE_MB", "100"))

    # Scraping Settings
    RATE_LIMIT_DELAY_SECONDS: float = float(os.getenv("RATE_LIMIT_DELAY_SECONDS", "2"))
    MAX_CONCURRENT_REQUESTS: int = int(os.getenv("MAX_CONCURRENT_REQUESTS", "3"))
    REQUEST_TIMEOUT_SECONDS: int = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))

    # Chrome Driver Settings
    CHROME_HEADLESS: bool = os.getenv("CHROME_HEADLESS", "true").lower() == "true"
    CHROME_NO_SANDBOX: bool = os.getenv("CHROME_NO_SANDBOX", "true").lower() == "true"

    # League URLs and IDs
    LEAGUE_CONFIG: Dict[str, Dict[str, Any]] = {
        "MLS": {
            "fbref_url": "/en/comps/22/Major-League-Soccer-Stats",
            "transfermarkt_id": "major-league-soccer/startseite/wettbewerb/MLS1",
            "sofascore_id": "242",
            "asa_league_code": "mls",
        },
        "USL Championship": {
            "fbref_url": "/en/comps/123/USL-Championship-Stats",
            "transfermarkt_id": "usl-championship/startseite/wettbewerb/USC",
            "sofascore_id": "256",
            "asa_league_code": "uslc",
        },
        "USL League One": {
            "fbref_url": "/en/comps/124/USL-League-One-Stats",
            "transfermarkt_id": "usl-league-one/startseite/wettbewerb/USL1",
            "sofascore_id": "257",
            "asa_league_code": "usl1",
        },
    }

    # Request Headers
    DEFAULT_HEADERS: Dict[str, str] = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    # Export Settings
    EXPORT_PATH: str = os.getenv("EXPORT_PATH", "./exports/")
    MAX_EXPORT_ROWS: int = int(os.getenv("MAX_EXPORT_ROWS", "10000"))

    @classmethod
    def get_league_config(cls, league: str) -> Dict[str, Any]:
        """Get configuration for a specific league"""
        return cls.LEAGUE_CONFIG.get(league, {})

    @classmethod
    def validate_config(cls) -> bool:
        """Ensure required directories exist and are writable"""
        # Create export directory if it doesn't exist (no error if it does)
        os.makedirs(cls.EXPORT_PATH, exist_ok=True)
        logger.info(f"Ensured export directory exists at: {cls.EXPORT_PATH}")
        return True
