import requests
import json
import time
import logging

from datetime import datetime
from typing import List, Dict, Optional, Any

from scrapers.base_scraper import BaseScraper
from config import Config

logger = logging.getLogger(__name__)

class AmericanSoccerAnalysisAPI(BaseScraper):
    """Client for American Soccer Analysis API"""

    BASE_URL = "https://app.americansocceranalysis.com/api/v1"

    def __init__(self, db_manager: Any, api_key: str = None):
        super().__init__(db_manager)
        self.api_key = api_key or self.config.ASA_API_KEY

        if self.api_key:
            self.session.headers.update({
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            })

    def scrape_league(self, league: str) -> List[Dict[str, Any]]:
        """Get player data from ASA API for a league"""
        if not self.api_key:
            logger.warning("ASA API key not configured")
            return []

        league_config = self.config.get_league_config(league)
        asa_league_code = league_config.get("asa_league_code")
        if not asa_league_code:
            logger.error(f"No ASA league code for {league}")
            return []

        # Get current season
        current_year = datetime.now().year

        # Fetch player xG data
        xg_data = self._fetch_xg_data(asa_league_code, current_year)

        # Fetch player xPass data
        xpass_data = self._fetch_xpass_data(asa_league_code, current_year)

        # Merge data
        return self._merge_asa_data(xg_data, xpass_data, league)

    def parse_player_data(self, response_data: Dict[str, Any], league: str) -> List[Dict[str, Any]]:
        """Parse player data from API response"""
        players_data: List[Dict[str, Any]] = []

        data_list = response_data.get("data", [])
        for player in data_list:
            player_data: Dict[str, Any] = {
                "name": player.get("player_name"),
                "asa_id": player.get("player_id"),
                "team": player.get("team_name"),
                "league": league,
                "season": player.get("season"),
                "minutes_played": player.get("minutes_played", 0),
                "shots": player.get("shots", 0),
                "xg": player.get("xg", 0.0),
                "goals": player.get("goals", 0),
                "xg_per_shot": player.get("xg_per_shot", 0.0),
                "key_passes": player.get("key_passes", 0),
                "xa": player.get("xa", 0.0),
                "assists": player.get("assists", 0),
                "xgbuildup": player.get("xg_buildup", 0.0),
                "xgchain": player.get("xg_chain", 0.0),
                "source": "asa"
            }

            # Calculate per 90 stats
            minutes = player_data["minutes_played"]
            if minutes > 0:
                mins90 = minutes / 90.0
                player_data["xg_per_90"] = round(player_data["xg"] / mins90, 2)
                player_data["xa_per_90"] = round(player_data["xa"] / mins90, 2)
                player_data["shots_per_90"] = round(player_data["shots"] / mins90, 2)

            players_data.append(player_data)

        return players_data

    def _fetch_xg_data(self, league_code: str, season: int) -> List[Dict[str, Any]]:
        """Fetch expected goals data"""
        endpoint = f"{self.BASE_URL}/players/xgoals"
        params = {
            "league": league_code,
            "season": season,
            "split_by_teams": "false",
            "minimum_minutes": 900
        }

        try:
            response = self.session.get(endpoint, params=params)
            response.raise_for_status()
            return self.parse_player_data(response.json(), league_code)
        except requests.RequestException as e:
            logger.error(f"Error fetching xG data from ASA: {e}")
            return []

    def _fetch_xpass_data(self, league_code: str, season: int) -> List[Dict[str, Any]]:
        """Fetch expected assists data"""
        endpoint = f"{self.BASE_URL}/players/xpass"
        params = {
            "league": league_code,
            "season": season,
            "split_by_teams": "false",
            "minimum_minutes": 900
        }

        try:
            response = self.session.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("data", [])
        except requests.RequestException as e:
            logger.error(f"Error fetching xPass data from ASA: {e}")
            return []

    def _merge_asa_data(
        self,
        xg_data: List[Dict[str, Any]],
        xpass_data: List[Dict[str, Any]],
        league: str
    ) -> List[Dict[str, Any]]:
        """Merge different ASA data sources"""
        merged: Dict[Any, Dict[str, Any]] = {}

        # Start with xG data
        for player in xg_data:
            pid = player.get("asa_id")
            if pid:
                merged[pid] = player

        # Merge xPass data
        for player in xpass_data:
            pid = player.get("player_id")
            if pid in merged:
                merged[pid].update({
                    "xa": player.get("xa", 0.0),
                    "key_passes": player.get("key_passes", 0),
                    "pass_completion": player.get("pass_completion_percentage", 0.0)
                })
            else:
                merged[pid] = {
                    "name": player.get("player_name"),
                    "asa_id": pid,
                    "team": player.get("team_name"),
                    "league": league,
                    "xa": player.get("xa", 0.0),
                    "key_passes": player.get("key_passes", 0),
                    "pass_completion": player.get("pass_completion_percentage", 0.0),
                    "source": "asa"
                }

        return list(merged.values())

    def get_team_xg_timeline(self, team_name: str, season: int) -> List[Dict[str, Any]]:
        """Get team's xG timeline for a season"""
        endpoint = f"{self.BASE_URL}/teams/xgoals/timeline"
        params = {"team_name": team_name, "season": season}

        try:
            response = self.session.get(endpoint, params=params)
            response.raise_for_status()
            return response.json().get("data", [])
        except requests.RequestException as e:
            logger.error(f"Error fetching team xG timeline: {e}")
            return []
