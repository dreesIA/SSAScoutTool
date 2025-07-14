import json
import time
import logging
from typing import List, Dict, Optional, Any
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.base_scraper import BaseScraper
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SofascoreScraper(BaseScraper):
    """Scraper for Sofascore ratings and match data using Selenium"""
    BASE_URL = "https://www.sofascore.com"

    def __init__(self, db_manager: Any):
        super().__init__(db_manager)
        self.driver: Optional[webdriver.Chrome] = None

    def init_driver(self) -> None:
        """Initialize Selenium Chrome driver"""
        if self.driver:
            return

        chrome_options = Options()
        if self.config.CHROME_HEADLESS:
            chrome_options.add_argument('--headless')
        if self.config.CHROME_NO_SANDBOX:
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')

        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument(f"user-agent={self.config.DEFAULT_HEADERS['User-Agent']}")

        try:
            self.driver = webdriver.Chrome(
                ChromeDriverManager().install(),
                options=chrome_options
            )
            self.driver.implicitly_wait(10)
            logger.info("Chrome driver initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Chrome driver: {e}")
            raise

    def close_driver(self) -> None:
        """Close Selenium driver"""
        if self.driver:
            self.driver.quit()
            self.driver = None
            logger.info("Chrome driver closed")

    def scrape_league(self, league: str) -> List[Dict[str, Any]]:
        """Scrape player ratings for a league"""
        league_config = self.config.get_league_config(league)
        if not league_config or 'sofascore_id' not in league_config:
            logger.error(f"No Sofascore configuration for league: {league}")
            return []

        try:
            self.init_driver()
            url = f"{self.BASE_URL}/tournament/football/usa/{league.lower().replace(' ', '-')}/{league_config['sofascore_id']}"
            self.driver.get(url)
            time.sleep(3)

            try:
                stats_tab = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Statistics')]")
                )
                stats_tab.click()
                time.sleep(2)
            except TimeoutException:
                logger.warning("Statistics tab not found, continuing without click")

            return self.parse_player_data(self.driver.page_source, league)

        except Exception as e:
            logger.error(f"Error scraping Sofascore for {league}: {e}")
            return []

    def parse_player_data(self, html: str, league: str) -> List[Dict[str, Any]]:
        """Parse player data from Sofascore page"""
        players_data: List[Dict[str, Any]] = []
        try:
            if not self.driver:
                return players_data

            player_elements = self.driver.find_elements(By.CSS_SELECTOR, "[data-testid='player-statistics-row']")
            if not player_elements:
                player_elements = self.driver.find_elements(By.CSS_SELECTOR, ".player-statistics-row, .statistics-row")

            for element in player_elements:
                data = self._parse_player_element(element, league)
                if data:
                    players_data.append(data)

            if not players_data:
                players_data = self._extract_from_json_ld()

            logger.info(f"Parsed {len(players_data)} players from Sofascore for {league}")
        except Exception as e:
            logger.error(f"Error parsing Sofascore data: {e}")
        return players_data

    def _parse_player_element(self, element: Any, league: str) -> Optional[Dict[str, Any]]:
        """Parse individual player element"""
        try:
            name_el = element.find_element(By.CSS_SELECTOR, ".player-name, [data-testid='player-name']")
            rating_el = element.find_element(By.CSS_SELECTOR, ".rating, [data-testid='rating']")
            player_name = name_el.text.strip()
            rating = self.parse_float(rating_el.text)

            stats: Dict[str, int] = {}
            stat_elements = element.find_elements(By.CSS_SELECTOR, ".stat-value, [data-testid*='stat']")
            for i, stat in enumerate(stat_elements):
                val = stat.text.strip()
                if i == 0:
                    stats['matches'] = self.parse_number(val)
                elif i == 1:
                    stats['goals'] = self.parse_number(val)
                elif i == 2:
                    stats['assists'] = self.parse_number(val)
                elif i == 3:
                    stats['minutes'] = self.parse_number(val)

            sofascore_id: Optional[str] = None
            try:
                href = element.find_element(By.TAG_NAME, 'a').get_attribute('href')
                if href and '/player/' in href:
                    sofascore_id = href.split('/player/')[1].split('/')[0]
            except NoSuchElementException:
                pass

            return {
                'name': player_name,
                'sofascore_id': sofascore_id,
                'rating': rating,
                'league': league,
                'matches': stats.get('matches', 0),
                'goals': stats.get('goals', 0),
                'assists': stats.get('assists', 0),
                'minutes': stats.get('minutes', 0),
                'source': 'sofascore'
            }
        except Exception as e:
            logger.debug(f"Could not parse player element: {e}")
            return None

    def _extract_from_json_ld(self) -> List[Dict[str, Any]]:
        """Extract player data from JSON-LD structured data"""
        data_list: List[Dict[str, Any]] = []
        try:
            scripts = self.driver.find_elements(By.XPATH, "//script[@type='application/ld+json']")
            for script in scripts:
                try:
                    content = json.loads(script.get_attribute('innerHTML'))
                    # TODO: process structured data if needed
                except Exception:
                    continue
        except Exception:
            pass
        return data_list

    def scrape_player_detailed(self, sofascore_id: str) -> Optional[Dict[str, Any]]:
        """Scrape detailed stats for a specific player"""
        if not sofascore_id:
            return None
        try:
            self.init_driver()
            url = f"{self.BASE_URL}/player/{sofascore_id}"
            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='player-details']"))
            )
            return {
                'career_stats': self._extract_career_stats(),
                'season_stats': self._extract_season_stats(),
                'attributes': self._extract_player_attributes()
            }
        except Exception as e:
            logger.error(f"Error scraping player details: {e}")
            return None

    def _extract_career_stats(self) -> Dict[str, Any]:
        """Extract career statistics"""
        stats: Dict[str, Any] = {}
        try:
            section = self.driver.find_element(By.CSS_SELECTOR, "[data-testid='career-statistics']")
            items = section.find_elements(By.CSS_SELECTOR, ".stat-item")
            for item in items:
                label = item.find_element(By.CSS_SELECTOR, ".stat-label").text
                value = item.find_element(By.CSS_SELECTOR, ".stat-value").text
                stats[label.lower().replace(' ', '_')] = value
        except Exception as e:
            logger.debug(f"Could not extract career stats: {e}")
        return stats

    def _extract_season_stats(self) -> List[Dict[str, Any]]:
        """Extract season-by-season statistics"""
        seasons: List[Dict[str, Any]] = []
        try:
            table = self.driver.find_element(By.CSS_SELECTOR, "[data-testid='seasons-statistics']")
            rows = table.find_elements(By.CSS_SELECTOR, 'tr')[1:]
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, 'td')
                if len(cells) >= 5:
                    seasons.append({
                        'season': cells[0].text,
                        'team': cells[1].text,
                        'matches': self.parse_number(cells[2].text),
                        'goals': self.parse_number(cells[3].text),
                        'assists': self.parse_number(cells[4].text),
                        'rating': self.parse_float(cells[5].text) if len(cells) > 5 else 0.0
                    })
        except Exception as e:
            logger.debug(f"Could not extract season stats: {e}")
        return seasons

    def _extract_player_attributes(self) -> Dict[str, Any]:
        """Extract player attributes/characteristics"""
        attrs: Dict[str, Any] = {}
        try:
            info = self.driver.find_element(By.CSS_SELECTOR, "[data-testid='player-info']")
            for label in ['Height', 'Preferred foot', 'Position', 'Shirt number']:
                attrs[label.lower().replace(' ', '_')] = self._find_attribute_value(label)
        except Exception as e:
            logger.debug(f"Could not extract player attributes: {e}")
        return attrs

    def _find_attribute_value(self, label: str) -> Optional[str]:
        """Find attribute value by label"""
        try:
            el = self.driver.find_element(
                By.XPATH, f"//span[contains(text(), '{label}')]/following-sibling::span"
            )
            return el.text.strip()
        except Exception:
            return None