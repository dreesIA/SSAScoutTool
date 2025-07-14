import re
import logging
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrapers.base_scraper import BaseScraper
from config import Config

# Configure module‐level logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TransfermarktScraper(BaseScraper):
    """Scraper for Transfermarkt market values and transfer data"""
    
    BASE_URL = "https://www.transfermarkt.com"
    
    def scrape_league(self, league: str) -> List[Dict[str, Any]]:
        """Scrape market values for a league"""
        league_config = self.config.get_league_config(league)
        if not league_config or 'transfermarkt_id' not in league_config:
            logger.error(f"No Transfermarkt configuration for league: {league}")
            return []
        
        url = urljoin(self.BASE_URL, league_config['transfermarkt_id'])
        html = self.make_request(url)
        if not html:
            return []
        
        return self.parse_player_data(html, league)
    
    def parse_player_data(self, html: str, league: str) -> List[Dict[str, Any]]:
        """Parse market values from Transfermarkt HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        players_data: List[Dict[str, Any]] = []
        
        # Find all player rows in the main table
        player_rows = soup.find_all('tr', class_=['odd', 'even'])
        for row in player_rows:
            data = self._parse_player_row(row, league)
            if data:
                players_data.append(data)
        
        logger.info(f"Parsed {len(players_data)} players from Transfermarkt for {league}")
        return players_data
    
    def _parse_player_row(self, row, league: str) -> Optional[Dict[str, Any]]:
        """Parse individual player row from Transfermarkt"""
        try:
            # Find player name and link
            name_cell = row.find('td', class_='hauptlink')
            if not name_cell:
                return None
            
            name_link = name_cell.find('a')
            if not name_link:
                return None
            
            player_name = self.clean_text(name_link.text)
            player_href = name_link.get('href', '')
            
            # Extract Transfermarkt ID from URL
            tm_match = re.search(r'/spieler/(\d+)', player_href)
            tm_id = tm_match.group(1) if tm_match else None
            
            # Find position
            pos_cell = row.find('td', class_='pos')
            position = self.clean_text(pos_cell.text) if pos_cell else None
            
            # Find age
            age_cell = row.find('td', class_='zentriert', string=re.compile(r'\d+'))
            age = self.parse_number(age_cell.text) if age_cell else None
            
            # Find market value
            value_cell = row.find('td', class_='rechts hauptlink')
            market_value = self._parse_market_value(value_cell.text) if value_cell else 0
            
            # Find club (via image title attribute)
            club = None
            for cell in row.find_all('td', class_='zentriert'):
                img = cell.find('img')
                if img and img.get('title'):
                    club = img['title']
                    break
            
            return {
                'name': player_name,
                'transfermarkt_id': tm_id,
                'position': position,
                'age': age,
                'club': club,
                'league': league,
                'market_value': market_value,
                'source': 'transfermarkt'
            }
            
        except Exception as e:
            logger.error(f"Error parsing Transfermarkt player row: {e}")
            return None
    
    def _parse_market_value(self, value_text: str) -> int:
        """Parse market value from text like '€5.00m' or '€500k'"""
        try:
            # Strip currency symbols, commas, spaces
            cleaned = value_text.replace('€', '').replace(',', '').replace(' ', '').lower()
            
            if 'm' in cleaned:
                return int(float(cleaned.replace('m', '')) * 1_000_000)
            if 'k' in cleaned:
                return int(float(cleaned.replace('k', '')) * 1_000)
            
            return int(float(cleaned))
        except Exception:
            logger.warning(f"Could not parse market value: {value_text}")
            return 0
    
    def scrape_player_transfers(self, tm_id: str) -> List[Dict[str, Any]]:
        """Scrape transfer history for a specific player"""
        if not tm_id:
            return []
        
        transfers_url = urljoin(self.BASE_URL, f"/spieler/transfers/spieler/{tm_id}")
        html = self.make_request(transfers_url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        transfers: List[Dict[str, Any]] = []
        
        transfer_table = soup.find('div', class_='responsive-table')
        if not transfer_table:
            return transfers
        
        for row in transfer_table.find_all('tr', class_=['odd', 'even']):
            t = self._parse_transfer_row(row)
            if t:
                transfers.append(t)
        
        return transfers
    
    def _parse_transfer_row(self, row) -> Optional[Dict[str, Any]]:
        """Parse individual transfer record"""
        try:
            cells = row.find_all('td')
            if len(cells) < 5:
                return None
            
            transfer_data: Dict[str, Any] = {
                'season':    self.clean_text(cells[0].text),
                'date':      self.clean_text(cells[1].text),
                'from_club': self.clean_text(cells[2].text),
                'to_club':   self.clean_text(cells[3].text),
                'market_value': self._parse_market_value(cells[4].text),
                'fee':          self._parse_market_value(cells[5].text) if len(cells) > 5 else 0
            }
            return transfer_data
        except Exception as e:
            logger.error(f"Error parsing transfer row: {e}")
            return None
