import logging
import re
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from scrapers.base_scraper import BaseScraper
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FBrefScraper(BaseScraper):
    """Scraper for FBref.com statistics"""

    BASE_URL = "https://fbref.com"

    def scrape_league(self, league: str) -> List[Dict[str, Any]]:
        """Scrape player data for a specific league"""
        league_config = self.config.get_league_config(league)
        if not league_config or 'fbref_url' not in league_config:
            logger.error(f"No FBref configuration for league: {league}")
            return []

        url = urljoin(self.BASE_URL, league_config['fbref_url'])
        html = self.make_request(url)
        if not html:
            return []

        return self.parse_player_data(html, league)

    def parse_player_data(self, html: str, league: str) -> List[Dict[str, Any]]:
        """Parse player statistics from FBref HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        players_data: List[Dict[str, Any]] = []

        stats_table = soup.find('table', {'id': 'stats_standard'})
        if not stats_table:
            logger.warning("Could not find stats table in FBref page")
            return players_data

        tbody = stats_table.find('tbody')
        if not tbody:
            return players_data

        for row in tbody.find_all('tr'):
            if 'thead' in (row.get('class') or []):
                continue

            player_data = self._parse_player_row(row, league)
            if player_data:
                players_data.append(player_data)

        logger.info(f"Parsed {len(players_data)} players from FBref for {league}")
        return players_data

    def _parse_player_row(self, row, league: str) -> Optional[Dict[str, Any]]:
        """Parse individual player row"""
        try:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 20:
                return None

            player_link = cells[0].find('a')
            if not player_link:
                return None

            player_name = self.clean_text(player_link.text)
            player_href = player_link.get('href', '')
            fbref_id = player_href.split('/')[-2] if player_href else None

            data = {
                'name': player_name,
                'fbref_id': fbref_id,
                'league': league,
                'nationality': self.clean_text(cells[1].get_text()) if len(cells) > 1 else None,
                'position': self.clean_text(cells[2].get_text()) if len(cells) > 2 else None,
                'club': self.clean_text(cells[3].get_text()) if len(cells) > 3 else None,
                'age': self.parse_number(cells[4].get_text()) if len(cells) > 4 else 0,
                'birth_year': self.parse_number(cells[5].get_text()) if len(cells) > 5 else 0,
                'matches': self.parse_number(cells[6].get_text()) if len(cells) > 6 else 0,
                'starts': self.parse_number(cells[7].get_text()) if len(cells) > 7 else 0,
                'minutes': self.parse_number(cells[8].get_text()) if len(cells) > 8 else 0,
                'minutes_per_90': self.parse_float(cells[9].get_text()) if len(cells) > 9 else 0.0,
                'goals': self.parse_number(cells[10].get_text()) if len(cells) > 10 else 0,
                'assists': self.parse_number(cells[11].get_text()) if len(cells) > 11 else 0,
                'goals_assists': self.parse_number(cells[12].get_text()) if len(cells) > 12 else 0,
                'goals_minus_pk': self.parse_number(cells[13].get_text()) if len(cells) > 13 else 0,
                'penalties': self.parse_number(cells[14].get_text()) if len(cells) > 14 else 0,
                'penalties_attempted': self.parse_number(cells[15].get_text()) if len(cells) > 15 else 0,
                'yellow_cards': self.parse_number(cells[16].get_text()) if len(cells) > 16 else 0,
                'red_cards': self.parse_number(cells[17].get_text()) if len(cells) > 17 else 0,
                'xg': self.parse_float(cells[18].get_text()) if len(cells) > 18 else 0.0,
                'npxg': self.parse_float(cells[19].get_text()) if len(cells) > 19 else 0.0,
                'xa': self.parse_float(cells[20].get_text()) if len(cells) > 20 else 0.0,
                'source': 'fbref'
            }

            if data['minutes'] > 0:
                mins90 = data['minutes'] / 90.0
                data['goals_per_90'] = round(data['goals'] / mins90, 2)
                data['assists_per_90'] = round(data['assists'] / mins90, 2)
                data['xg_per_90'] = round(data['xg'] / mins90, 2)
            else:
                data['goals_per_90'] = data['assists_per_90'] = data['xg_per_90'] = 0.0

            return data

        except Exception as e:
            logger.error(f"Error parsing player row: {e}")
            return None

    def scrape_player_detailed(self, fbref_id: str) -> Optional[Dict[str, Any]]:
        """Scrape detailed stats for a specific player"""
        if not fbref_id:
            return None

        url = urljoin(self.BASE_URL, f"/en/players/{fbref_id}/")
        html = self.make_request(url)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        detailed: Dict[str, Any] = {}

        scouting_table = soup.find('table', {'id': 'scout_summary'})
        if scouting_table:
            detailed['scouting'] = self._parse_scouting_report(scouting_table)

        season_table = soup.find('table', {'id': 'stats'})
        if season_table:
            detailed['seasons'] = self._parse_season_stats(season_table)

        return detailed

    def _parse_scouting_report(self, table) -> Dict[str, Dict[str, Any]]:
        """Parse scouting report percentiles"""
        report: Dict[str, Dict[str, Any]] = {}
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 3:
                name = self.clean_text(cells[0].get_text())
                pct = self.parse_number(cells[1].get_text())
                per90 = self.parse_float(cells[2].get_text())
                report[name] = {'percentile': pct, 'per_90': per90}
        return report

    def _parse_season_stats(self, table) -> List[Dict[str, Any]]:
        """Parse historical season statistics"""
        seasons: List[Dict[str, Any]] = []
        tbody = table.find('tbody')
        if not tbody:
            return seasons
        for row in tbody.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) > 10:
                seasons.append({
                    'season': self.clean_text(cells[0].get_text()),
                    'age': self.parse_number(cells[1].get_text()),
                    'club': self.clean_text(cells[2].get_text()),
                    'league': self.clean_text(cells[4].get_text()),
                    'matches': self.parse_number(cells[5].get_text()),
                    'minutes': self.parse_number(cells[6].get_text()),
                    'goals': self.parse_number(cells[7].get_text()),
                    'assists': self.parse_number(cells[8].get_text()),
                })
        return seasons
