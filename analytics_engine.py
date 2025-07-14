import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta

from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity

from database_manager import DatabaseManager
from config import Config

logger = logging.getLogger(__name__)

class AdvancedAnalytics:
    """Advanced analytics engine for player and team analysis"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.config = Config()

    def get_player_percentiles(
        self,
        player_id: int,
        comparison_group: str = 'position'
    ) -> Dict[str, Any]:
        """Calculate player percentiles against comparison group"""
        conn = self.db.get_connection()
        
        player_query = "SELECT * FROM players WHERE id = ?"
        player_df = pd.read_sql_query(player_query, conn, params=(player_id,))

        if player_df.empty:
            conn.close()
            return {}

        player = player_df.iloc[0]

        if comparison_group == 'position':
            comp_query = (
                "SELECT * FROM players "
                "WHERE position = ? AND league = ? AND minutes_played > 900 AND id != ?"
            )
            params = (player['position'], player['league'], player_id)
        elif comparison_group == 'league':
            comp_query = (
                "SELECT * FROM players "
                "WHERE league = ? AND minutes_played > 900 AND id != ?"
            )
            params = (player['league'], player_id)
        else:
            comp_query = (
                "SELECT * FROM players WHERE minutes_played > 900 AND id != ?"
            )
            params = (player_id,)

        comparison_df = pd.read_sql_query(comp_query, conn, params=params)
        conn.close()

        if comparison_df.empty:
            return {}

        metrics = {
            'rating': 'Overall Rating',
            'goals': 'Goals',
            'assists': 'Assists',
            'goals_per_90': 'Goals per 90',
            'assists_per_90': 'Assists per 90',
            'market_value': 'Market Value',
            'pass_accuracy': 'Pass Accuracy',
            'key_passes': 'Key Passes',
            'dribbles': 'Dribbles',
            'tackles': 'Tackles',
            'interceptions': 'Interceptions'
        }

        percentiles: Dict[str, Any] = {}

        for metric, name in metrics.items():
            if metric in player and pd.notna(player[metric]):
                val = player[metric]
                all_vals = comparison_df[metric].dropna().tolist() + [val]
                series = pd.Series(all_vals)
                pct = (series <= val).sum() / len(all_vals) * 100
                rank = len(all_vals) - series.rank(method='dense').iloc[-1] + 1
                percentiles[name] = {
                    'value': val,
                    'percentile': round(pct, 1),
                    'rank': int(rank),
                    'total': len(all_vals)
                }

        return {
            'player_name': player['name'],
            'comparison_group': comparison_group,
            'sample_size': len(comparison_df),
            'percentiles': percentiles
        }

    def find_similar_players(
        self,
        player_id: int,
        num_similar: int = 10,
        similarity_threshold: float = 0.7
    ) -> List[Dict[str, Any]]:
        """Find similar players using statistical similarity"""
        conn = self.db.get_connection()
        target_query = "SELECT * FROM players WHERE id = ?"
        target_df = pd.read_sql_query(target_query, conn, params=(player_id,))
        
        if target_df.empty:
            conn.close()
            return []

        target = target_df.iloc[0]
        comp_query = (
            "SELECT * FROM players "
            "WHERE position = ? AND minutes_played > 900 AND id != ?"
        )
        comparison_df = pd.read_sql_query(comp_query, conn, params=(target['position'], player_id))
        conn.close()

        if comparison_df.empty:
            return []

        features = [
            'age', 'goals_per_90', 'assists_per_90', 'rating',
            'pass_accuracy', 'key_passes', 'dribbles', 'tackles', 'interceptions'
        ]
        existing = [f for f in features if f in comparison_df.columns]
        all_players = pd.concat([target_df, comparison_df], ignore_index=True)
        matrix = all_players[existing].fillna(all_players[existing].mean())

        scaler = StandardScaler()
        norm = scaler.fit_transform(matrix)
        sim_matrix = cosine_similarity(norm)

        sims = sim_matrix[0, 1:]
        similar: List[Dict[str, Any]] = []
        for idx, sim in enumerate(sims):
            if sim >= similarity_threshold:
                p = comparison_df.iloc[idx]
                similar.append({
                    'id': p['id'],
                    'name': p['name'],
                    'club': p['club'],
                    'league': p['league'],
                    'position': p['position'],
                    'age': p['age'],
                    'rating': p['rating'],
                    'market_value': p['market_value'],
                    'similarity_score': round(sim*100,1),
                    'goals': p['goals'],
                    'assists': p['assists'],
                    'minutes': p['minutes_played']
                })

        similar.sort(key=lambda x: x['similarity_score'], reverse=True)
        return similar[:num_similar]

    def generate_player_report(self, player_id: int) -> Dict[str, Any]:
        """Generate comprehensive player scouting report"""
        conn = self.db.get_connection()
        q = "SELECT * FROM players WHERE id = ?"
        df = pd.read_sql_query(q, conn, params=(player_id,))
        if df.empty:
            conn.close()
            return {}
        player = df.iloc[0].to_dict()

        percentiles = self.get_player_percentiles(player_id)
        similar = self.find_similar_players(player_id, num_similar=5)

        strengths, weaknesses = [], []
        for metric, data in percentiles.get('percentiles', {}).items():
            pct = data['percentile']
            if pct >= 80: strengths.append({**data, 'metric': metric})
            elif pct <= 30: weaknesses.append({**data, 'metric': metric})

        rec = self._generate_recommendation(player, percentiles, strengths, weaknesses)
        val_assess = self._assess_player_value(player, percentiles)

        conn.close()
        return {
            'player': player,
            'percentiles': percentiles,
            'similar_players': similar,
            'strengths': strengths,
            'weaknesses': weaknesses,
            'recommendation': rec,
            'value_assessment': val_assess,
            'generated_at': datetime.now().isoformat()
        }

    def _generate_recommendation(
        self,
        player: Dict[str, Any],
        percentiles: Dict[str, Any],
        strengths: List[Any],
        weaknesses: List[Any]
    ) -> Dict[str, Any]:
        """Generate scouting recommendation based on analysis"""
        rec = {
            'overall_assessment': '',
            'suggested_action': '',
            'development_areas': [],
            'comparison_level': ''
        }
        rpct = percentiles.get('percentiles', {}).get('Overall Rating', {}).get('percentile', 50)
        age = player.get('age', 25)
        if rpct >= 80:
            if age <= 23:
                rec['overall_assessment'] = "Elite young talent with immediate impact"
                rec['suggested_action'] = "Priority signing"
            elif age <= 29:
                rec['overall_assessment'] = "Top-tier prime player"
                rec['suggested_action'] = "Immediate starter"
            else:
                rec['overall_assessment'] = "Experienced high-performer"
                rec['suggested_action'] = "Short-term quality addition"
        elif rpct >= 60:
            rec['overall_assessment'] = "Solid contributor"
            rec['suggested_action'] = "Depth signing"
        else:
            rec['overall_assessment'] = "Role player"
            rec['suggested_action'] = "Specific need only"

        rec['development_areas'] = [w['metric'] for w in weaknesses[:3]]
        league = player.get('league', '')
        if 'MLS' in league:
            rec['comparison_level'] = "MLS standard"
        elif 'Championship' in league:
            rec['comparison_level'] = "USL Championship standard"
        else:
            rec['comparison_level'] = "USL League One standard"
        return rec

    def _assess_player_value(
        self,
        player: Dict[str, Any],
        percentiles: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Assess player's market value"""
        cur = player.get('market_value', 0)
        age = player.get('age', 25)
        rating = player.get('rating', 6.5)
        expected = self._calculate_expected_value(rating, age, player.get('league',''))
        ratio = cur/expected if expected else 1
        va = {'current_value': cur, 'value_rating':'', 'projection':'', 'factors':[]}
        if ratio<0.7:
            va['value_rating']='Undervalued'; va['projection']='Likely to increase'
        elif ratio>1.3:
            va['value_rating']='Overvalued'; va['projection']='May decrease'
        else:
            va['value_rating']='Fair value'; va['projection']='Stable'
        if age<=23: va['factors'].append('Young age')
        if age>=30: va['factors'].append('Veteran')
        if rating>=8: va['factors'].append('Elite performance')
        return va

    def _calculate_expected_value(
        self,
        rating: float,
        age: int,
        league: str
    ) -> float:
        """Simple model to calculate expected market value"""
        multipliers = {'MLS':1_000_000, 'USL Championship':200_000, 'USL League One':50_000}
        base = multipliers.get(league,100_000)
        rm = (rating/6.5)**2
        am = 1.0
        if age<23: am=0.7+(age-20)*0.1
        elif age>29: am=1.0-(age-29)*0.08
        val = base*rm*am
        return max(val,50_000)

    def get_team_analytics(
        self,
        team_name: str,
        league: str
    ) -> Dict[str, Any]:
        """Generate team-level analytics"""
        conn = self.db.get_connection()
        q = "SELECT * FROM players WHERE club = ? AND league = ?"
        df = pd.read_sql_query(q, conn, params=(team_name, league))
        if df.empty:
            conn.close()
            return {}
        analytics = {
            'team_name': team_name,
            'league': league,
            'squad_size': len(df),
            'average_age': round(df['age'].mean(),1),
            'total_market_value': df['market_value'].sum(),
            'average_rating': round(df['rating'].mean(),2),
            'total_goals': df['goals'].sum(),
            'total_assists': df['assists'].sum(),
            'position_distribution': df['position'].value_counts().to_dict(),
            'age_distribution': {
                'U23': len(df[df['age']<23]),
                '23-29': len(df[(df['age']>=23)&(df['age']<=29)]),
                'Over30': len(df[df['age']>30])
            }
        }
        top = df.nlargest(5,'rating')[['name','position','rating','goals','assists']]
        analytics['top_performers'] = top.to_dict('records')
        weak = df.groupby('position')['rating'].mean()
        weak = weak[weak < df['rating'].mean() - 0.5]
        analytics['weak_positions'] = weak.to_dict()
        conn.close()
        return analytics
