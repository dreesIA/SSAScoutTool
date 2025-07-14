import pandas as pd
import numpy as np
import hashlib
import logging
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import your modules
from database_manager import DatabaseManager
from config import Config
from scrapers.fbref_scraper import FBrefScraper
from scrapers.transfermarkt_scraper import TransfermarktScraper
from scrapers.sofascore_scraper import SofascoreScraper
from scrapers.asa_api_client import AmericanSoccerAnalysisAPI

# Configure module‐level logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataAggregator:
    """Aggregates data from multiple sources and manages synchronization"""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.config = Config()

        # Initialize scrapers
        self.fbref = FBrefScraper(db_manager)
        self.transfermarkt = TransfermarktScraper(db_manager)
        self.sofascore = SofascoreScraper(db_manager)
        self.asa = AmericanSoccerAnalysisAPI(db_manager) if self.config.ASA_API_KEY else None

    def sync_league_data(
        self,
        league: str,
        progress_callback: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Synchronize all data sources for a league"""
        sync_id = self.db.log_sync(league, 'all', 'started')
        results: Dict[str, Any] = {
            'league': league,
            'started_at': datetime.now(),
            'sources': {},
            'total_players': 0,
            'errors': []
        }

        try:
            if progress_callback:
                progress_callback(0.1, f"Starting sync for {league}")

            # Fetch data concurrently
            with ThreadPoolExecutor(max_workers=self.config.MAX_CONCURRENT_REQUESTS) as executor:
                futures: Dict[str, Any] = {}

                futures['fbref'] = executor.submit(
                    self._fetch_with_error_handling,
                    self.fbref.scrape_league,
                    league,
                    'FBref'
                )
                futures['transfermarkt'] = executor.submit(
                    self._fetch_with_error_handling,
                    self.transfermarkt.scrape_league,
                    league,
                    'Transfermarkt'
                )
                futures['sofascore'] = executor.submit(
                    self._fetch_with_error_handling,
                    self.sofascore.scrape_league,
                    league,
                    'Sofascore'
                )
                if self.asa:
                    futures['asa'] = executor.submit(
                        self._fetch_with_error_handling,
                        self.asa.scrape_league,
                        league,
                        'ASA'
                    )

                source_data: Dict[str, List[Dict[str, Any]]] = {}
                completed = 0
                total_sources = len(futures)

                for source, future in futures.items():
                    data, error = future.result()
                    source_data[source] = data

                    if error:
                        results['errors'].append(f"{source}: {error}")
                    else:
                        results['sources'][source] = len(data)

                    completed += 1
                    if progress_callback:
                        progress = 0.1 + (0.6 * completed / total_sources)
                        progress_callback(progress, f"Fetched {source} data")

            if progress_callback:
                progress_callback(0.7, "Merging data from all sources")

            merged_players = self._merge_player_data(source_data, league)
            results['total_players'] = len(merged_players)

            if progress_callback:
                progress_callback(0.9, "Saving to database")

            self._save_players_to_db(merged_players, league)

            self.db.log_sync(league, 'all', 'completed', results['total_players'])
            self.sofascore.close_driver()

            if progress_callback:
                progress_callback(1.0, f"Sync completed! {results['total_players']} players updated")

            results['completed_at'] = datetime.now()
            results['duration'] = (results['completed_at'] - results['started_at']).total_seconds()

        except Exception as e:
            logger.error(f"Error during sync: {e}")
            self.db.log_sync(league, 'all', 'failed', 0, str(e))
            results['errors'].append(f"Sync failed: {e}")
            if progress_callback:
                progress_callback(1.0, f"Sync failed: {e}")

        return results

    def _fetch_with_error_handling(
        self,
        fetch_func: Any,
        league: str,
        source_name: str
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Fetch data with error handling"""
        try:
            data = fetch_func(league)
            logger.info(f"Successfully fetched {len(data)} records from {source_name}")
            return data, None
        except Exception as e:
            error_msg = f"Failed to fetch from {source_name}: {e}"
            logger.error(error_msg)
            return [], error_msg

    def _merge_player_data(
        self,
        source_data: Dict[str, List[Dict[str, Any]]],
        league: str
    ) -> List[Dict[str, Any]]:
        """Merge player data from different sources"""
        merged_players: Dict[str, Dict[str, Any]] = {}

        for source, players in source_data.items():
            for player in players:
                key = self._generate_player_key(player.get('name', ''), player.get('club', ''))
                if key not in merged_players:
                    merged_players[key] = {
                        'league': league,
                        'sources': {},
                        'last_updated': datetime.now()
                    }
                self._merge_player_info(merged_players[key], player, source)

        final_list: List[Dict[str, Any]] = []
        for pdata in merged_players.values():
            if 'rating' not in pdata and pdata['sources']:
                ratings = [s['rating'] for s in pdata['sources'].values() if s.get('rating')]
                if ratings:
                    pdata['rating'] = round(sum(ratings) / len(ratings), 1)
            final_list.append(pdata)

        return final_list

    def _generate_player_key(self, name: str, club: str) -> str:
        """Generate unique key for player matching"""
        n = name.lower().strip().split()
        key_name = f"{n[0]}_{n[-1]}" if len(n) > 1 else n[0]
        return f"{key_name}_{club.lower().strip()}"

    def _merge_player_info(
        self,
        merged: Dict[str, Any],
        new_data: Dict[str, Any],
        source: str
    ) -> None:
        """Merge new player data into existing record"""
        merged['sources'][source] = new_data

        fields = [
            'name', 'age', 'position', 'club', 'nationality', 'market_value',
            'rating', 'goals', 'assists', 'matches', 'minutes_played',
            'yellow_cards', 'red_cards'
        ]
        for f in fields:
            if f in new_data and new_data[f] is not None:
                if f == 'market_value' and f in merged:
                    merged[f] = max(merged.get(f, 0), new_data[f])
                elif f == 'rating' and f in merged:
                    merged[f] = round((merged.get(f, 0) + new_data[f]) / 2, 1)
                else:
                    if f not in merged or merged[f] is None:
                        merged[f] = new_data[f]

        for idf in ['fbref_id', 'transfermarkt_id', 'sofascore_id', 'asa_id']:
            if idf in new_data and new_data[idf]:
                merged[idf] = new_data[idf]

        stats = ['xg', 'xa', 'xg_per_90', 'xa_per_90', 'key_passes', 'dribbles']
        for s in stats:
            if s in new_data and new_data[s] is not None:
                merged[s] = new_data[s]

    def _save_players_to_db(self, players: List[Dict[str, Any]], league: str) -> int:
        """Save merged player data to database"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        saved = 0

        for p in players:
            try:
                ext_id = hashlib.md5(f"{p.get('name','')}_{p.get('club','')}_{league}".encode()).hexdigest()
                cursor.execute(
                    """INSERT OR REPLACE INTO players (
                        external_id, name, age, position, club, league, nationality,
                        market_value, rating, goals, assists, matches, minutes_played,
                        pass_accuracy, shots_per_game, key_passes, dribbles,
                        aerial_duels, tackles, interceptions, clearances,
                        yellow_cards, red_cards, fbref_id, transfermarkt_id,
                        asa_id, sofascore_id, last_updated
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)""",
                    (
                        ext_id, p.get('name'), p.get('age'), p.get('position'),
                        p.get('club'), league, p.get('nationality'), p.get('market_value',0),
                        p.get('rating',0), p.get('goals',0), p.get('assists',0),
                        p.get('matches',0), p.get('minutes_played',0), p.get('pass_accuracy'),
                        p.get('shots_per_game'), p.get('key_passes'), p.get('dribbles'),
                        p.get('aerial_duels'), p.get('tackles'), p.get('interceptions'),
                        p.get('clearances'), p.get('yellow_cards',0), p.get('red_cards',0),
                        p.get('fbref_id'), p.get('transfermarkt_id'), p.get('asa_id'), p.get('sofascore_id')
                    )
                )
                saved += 1
            except Exception as e:
                logger.error(f"Error saving player {p.get('name','Unknown')}: {e}")
        conn.commit()
        conn.close()
        logger.info(f"Saved {saved} players to database for {league}")
        return saved

    def get_data_coverage_report(self, league: str) -> Dict[str, Any]:
        """Generate report on data coverage by source"""
        conn = self.db.get_connection()
        sources = [
            ('fbref_id', 'FBref'),
            ('transfermarkt_id', 'Transfermarkt'),
            ('sofascore_id', 'Sofascore'),
            ('asa_id', 'ASA')
        ]
        coverage: Dict[str, Dict[str, Any]] = {}

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM players WHERE league = ?", (league,))
        total = cursor.fetchone()[0]

        for idf, name in sources:
            cursor.execute(
                f"SELECT COUNT(*) FROM players WHERE league = ? AND {idf} IS NOT NULL", (league,)
            )
            count = cursor.fetchone()[0]
            coverage[name] = {
                'count': count,
                'percentage': round((count / total * 100) if total else 0, 1)
            }

        conn.close()
        return {'league': league, 'total_players': total, 'sources': coverage}
