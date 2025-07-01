import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import json

# Page configuration
st.set_page_config(
    page_title="Soccer Scout Pro",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
        font-size: 18px;
    }
    .player-card {
        background-color: #1e1e1e;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 20px;
        border: 1px solid #333;
    }
    .stat-box {
        background-color: #2d2d2d;
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 10px;
    }
    .source-badge {
        display: inline-block;
        padding: 4px 8px;
        margin: 2px;
        border-radius: 4px;
        font-size: 12px;
    }
    .source-active {
        background-color: #28a745;
        color: white;
    }
    .source-inactive {
        background-color: #6c757d;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'watchlist' not in st.session_state:
    st.session_state.watchlist = []

# Mock data
@st.cache_data
def load_players_data():
    players = [
        {
            'id': 1,
            'name': 'Diego Rossi',
            'age': 26,
            'position': 'FW',
            'club': 'Columbus Crew',
            'league': 'MLS',
            'nationality': 'Uruguay',
            'market_value': 8500000,
            'rating': 8.2,
            'goals': 15,
            'assists': 8,
            'matches': 28,
            'minutes_played': 2340,
            'pass_accuracy': 82,
            'shots_per_game': 3.2,
            'key_passes': 2.1,
            'dribbles': 2.8,
            'aerial_duels': 1.2,
            'tackles': 0.8,
            'interceptions': 0.4,
            'clearances': 0.3,
            'fbref': True,
            'transfermarkt': True,
            'asa': True,
            'sofascore': True
        },
        {
            'id': 2,
            'name': 'Tyler Adams',
            'age': 25,
            'position': 'CDM',
            'club': 'AFC Bournemouth',
            'league': 'MLS',
            'nationality': 'USA',
            'market_value': 25000000,
            'rating': 8.5,
            'goals': 2,
            'assists': 4,
            'matches': 30,
            'minutes_played': 2567,
            'pass_accuracy': 88,
            'shots_per_game': 0.8,
            'key_passes': 1.5,
            'dribbles': 1.2,
            'aerial_duels': 2.1,
            'tackles': 3.2,
            'interceptions': 2.8,
            'clearances': 1.9,
            'fbref': True,
            'transfermarkt': True,
            'asa': True,
            'sofascore': True
        },
        {
            'id': 3,
            'name': 'Ricardo Pepi',
            'age': 21,
            'position': 'ST',
            'club': 'PSV Eindhoven',
            'league': 'MLS',
            'nationality': 'USA',
            'market_value': 15000000,
            'rating': 7.8,
            'goals': 12,
            'assists': 3,
            'matches': 25,
            'minutes_played': 1890,
            'pass_accuracy': 76,
            'shots_per_game': 3.8,
            'key_passes': 1.2,
            'dribbles': 1.8,
            'aerial_duels': 2.5,
            'tackles': 0.3,
            'interceptions': 0.2,
            'clearances': 0.4,
            'fbref': True,
            'transfermarkt': True,
            'asa': False,
            'sofascore': True
        },
        {
            'id': 4,
            'name': 'Emiliano Rigoni',
            'age': 31,
            'position': 'RW',
            'club': 'Austin FC',
            'league': 'MLS',
            'nationality': 'Argentina',
            'market_value': 3500000,
            'rating': 7.5,
            'goals': 8,
            'assists': 11,
            'matches': 32,
            'minutes_played': 2456,
            'pass_accuracy': 84,
            'shots_per_game': 2.4,
            'key_passes': 2.8,
            'dribbles': 3.1,
            'aerial_duels': 0.8,
            'tackles': 0.9,
            'interceptions': 0.6,
            'clearances': 0.2,
            'fbref': True,
            'transfermarkt': True,
            'asa': True,
            'sofascore': True
        },
        {
            'id': 5,
            'name': 'Tanner Tessmann',
            'age': 23,
            'position': 'CM',
            'club': 'Venezia FC',
            'league': 'USL Championship',
            'nationality': 'USA',
            'market_value': 4000000,
            'rating': 7.6,
            'goals': 5,
            'assists': 7,
            'matches': 29,
            'minutes_played': 2234,
            'pass_accuracy': 85,
            'shots_per_game': 1.3,
            'key_passes': 2.0,
            'dribbles': 1.5,
            'aerial_duels': 1.8,
            'tackles': 2.4,
            'interceptions': 1.9,
            'clearances': 1.2,
            'fbref': True,
            'transfermarkt': True,
            'asa': False,
            'sofascore': True
        },
        {
            'id': 6,
            'name': 'Nick Lima',
            'age': 29,
            'position': 'RB',
            'club': 'New England Revolution',
            'league': 'MLS',
            'nationality': 'USA',
            'market_value': 2500000,
            'rating': 7.2,
            'goals': 1,
            'assists': 6,
            'matches': 28,
            'minutes_played': 2320,
            'pass_accuracy': 81,
            'shots_per_game': 0.6,
            'key_passes': 1.4,
            'dribbles': 1.9,
            'aerial_duels': 1.5,
            'tackles': 2.8,
            'interceptions': 2.1,
            'clearances': 2.5,
            'fbref': True,
            'transfermarkt': True,
            'asa': True,
            'sofascore': True
        },
        {
            'id': 7,
            'name': 'Jonathan Lewis',
            'age': 26,
            'position': 'LW',
            'club': 'Colorado Rapids',
            'league': 'MLS',
            'nationality': 'USA',
            'market_value': 3000000,
            'rating': 7.3,
            'goals': 9,
            'assists': 5,
            'matches': 26,
            'minutes_played': 1856,
            'pass_accuracy': 79,
            'shots_per_game': 2.6,
            'key_passes': 1.7,
            'dribbles': 3.4,
            'aerial_duels': 0.6,
            'tackles': 0.7,
            'interceptions': 0.4,
            'clearances': 0.1,
            'fbref': True,
            'transfermarkt': False,
            'asa': True,
            'sofascore': True
        },
        {
            'id': 8,
            'name': 'Hadji Barry',
            'age': 31,
            'position': 'ST',
            'club': 'Colorado Springs',
            'league': 'USL Championship',
            'nationality': 'Guinea',
            'market_value': 800000,
            'rating': 7.0,
            'goals': 18,
            'assists': 4,
            'matches': 30,
            'minutes_played': 2456,
            'pass_accuracy': 72,
            'shots_per_game': 3.9,
            'key_passes': 1.0,
            'dribbles': 1.3,
            'aerial_duels': 3.2,
            'tackles': 0.4,
            'interceptions': 0.3,
            'clearances': 0.5,
            'fbref': True,
            'transfermarkt': True,
            'asa': False,
            'sofascore': False
        },
        {
            'id': 9,
            'name': 'Milan Iloski',
            'age': 24,
            'position': 'CAM',
            'club': 'Charleston Battery',
            'league': 'USL Championship',
            'nationality': 'North Macedonia',
            'market_value': 1200000,
            'rating': 7.1,
            'goals': 7,
            'assists': 9,
            'matches': 28,
            'minutes_played': 2134,
            'pass_accuracy': 83,
            'shots_per_game': 2.1,
            'key_passes': 2.9,
            'dribbles': 2.2,
            'aerial_duels': 0.9,
            'tackles': 1.1,
            'interceptions': 0.8,
            'clearances': 0.3,
            'fbref': True,
            'transfermarkt': False,
            'asa': False,
            'sofascore': True
        },
        {
            'id': 10,
            'name': 'Arturo Rodriguez',
            'age': 22,
            'position': 'CM',
            'club': 'FC Tulsa',
            'league': 'USL Championship',
            'nationality': 'USA',
            'market_value': 900000,
            'rating': 6.9,
            'goals': 3,
            'assists': 6,
            'matches': 31,
            'minutes_played': 2567,
            'pass_accuracy': 86,
            'shots_per_game': 1.0,
            'key_passes': 1.8,
            'dribbles': 1.4,
            'aerial_duels': 1.6,
            'tackles': 2.6,
            'interceptions': 2.2,
            'clearances': 1.4,
            'fbref': True,
            'transfermarkt': True,
            'asa': False,
            'sofascore': True
        },
        {
            'id': 11,
            'name': 'Dariusz Formella',
            'age': 28,
            'position': 'RW',
            'club': 'Union Omaha',
            'league': 'USL League One',
            'nationality': 'Poland',
            'market_value': 600000,
            'rating': 6.8,
            'goals': 11,
            'assists': 8,
            'matches': 27,
            'minutes_played': 2234,
            'pass_accuracy': 80,
            'shots_per_game': 2.8,
            'key_passes': 2.3,
            'dribbles': 2.9,
            'aerial_duels': 0.7,
            'tackles': 0.8,
            'interceptions': 0.5,
            'clearances': 0.2,
            'fbref': False,
            'transfermarkt': True,
            'asa': False,
            'sofascore': False
        },
        {
            'id': 12,
            'name': 'Greg Hurst',
            'age': 27,
            'position': 'ST',
            'club': 'Chattanooga Red Wolves',
            'league': 'USL League One',
            'nationality': 'USA',
            'market_value': 450000,
            'rating': 6.7,
            'goals': 14,
            'assists': 3,
            'matches': 29,
            'minutes_played': 2345,
            'pass_accuracy': 74,
            'shots_per_game': 3.4,
            'key_passes': 0.9,
            'dribbles': 1.1,
            'aerial_duels': 2.8,
            'tackles': 0.3,
            'interceptions': 0.2,
            'clearances': 0.4,
            'fbref': False,
            'transfermarkt': False,
            'asa': False,
            'sofascore': True
        }
    ]
    return pd.DataFrame(players)

@st.cache_data
def load_teams_data():
    teams = [
        {'id': 1, 'name': 'LA Galaxy', 'league': 'MLS', 'avg_age': 26.3, 'market_value': 45200000, 'players': 28},
        {'id': 2, 'name': 'Inter Miami CF', 'league': 'MLS', 'avg_age': 27.1, 'market_value': 52300000, 'players': 30},
        {'id': 3, 'name': 'Phoenix Rising FC', 'league': 'USL Championship', 'avg_age': 24.8, 'market_value': 8900000, 'players': 26},
        {'id': 4, 'name': 'Louisville City FC', 'league': 'USL Championship', 'avg_age': 25.5, 'market_value': 7200000, 'players': 27},
        {'id': 5, 'name': 'Union Omaha', 'league': 'USL League One', 'avg_age': 23.9, 'market_value': 3100000, 'players': 24},
        {'id': 6, 'name': 'Richmond Kickers', 'league': 'USL League One', 'avg_age': 24.2, 'market_value': 2800000, 'players': 25}
    ]
    return pd.DataFrame(teams)

# Helper functions
def format_value(value):
    if value >= 1000000:
        return f"${value/1000000:.1f}M"
    elif value >= 1000:
        return f"${value/1000:.0f}K"
    return f"${value}"

def get_position_color(position):
    colors = {
        'GK': '#fbbf24',
        'CB': '#3b82f6', 
        'LB': '#60a5fa',
        'RB': '#60a5fa',
        'CDM': '#16a34a',
        'CM': '#22c55e',
        'CAM': '#f97316',
        'LW': '#f87171',
        'RW': '#f87171',
        'ST': '#dc2626',
        'FW': '#ef4444'
    }
    return colors.get(position, '#6b7280')

def get_rating_color(rating):
    if rating >= 8:
        return '#22c55e'
    elif rating >= 7:
        return '#fbbf24'
    elif rating >= 6:
        return '#f97316'
    return '#dc2626'

# Load data
players_df = load_players_data()
teams_df = load_teams_data()

# Header
st.title("⚽ Soccer Scout Pro")
st.markdown("**Multi-source scouting data from USL League One, USL Championship, and MLS**")

# Sidebar
with st.sidebar:
    st.header("🔍 Filters")
    
    # League filter
    league_filter = st.selectbox(
        "League",
        ["All Leagues", "MLS", "USL Championship", "USL League One"]
    )
    
    # Position filter
    position_filter = st.selectbox(
        "Position",
        ["All Positions", "GK", "CB", "LB", "RB", "CDM", "CM", "CAM", "LW", "RW", "ST", "FW"]
    )
    
    # Age range
    age_range = st.slider(
        "Age Range",
        min_value=16,
        max_value=40,
        value=(16, 40)
    )
    
    # Market value range
    value_range = st.slider(
        "Market Value Range",
        min_value=0,
        max_value=30000000,
        value=(0, 30000000),
        step=100000,
        format="$%d"
    )
    
    # Sort options
    sort_by = st.selectbox(
        "Sort By",
        ["rating", "age", "market_value", "goals", "assists"]
    )
    
    sort_order = st.radio(
        "Sort Order",
        ["Descending", "Ascending"]
    )
    
    st.divider()
    
    # Sync button
    if st.button("🔄 Sync Data", use_container_width=True):
        st.info("Data sync functionality would connect to FBref, Transfermarkt, ASA, and Sofascore APIs")

# Main content
tab1, tab2, tab3 = st.tabs(["📊 Players", "🏟️ Teams", "⭐ Watchlist"])

# Players Tab
with tab1:
    # Search bar
    search_query = st.text_input("🔍 Search players or clubs...", placeholder="Enter player or club name")
    
    # Apply filters
    filtered_df = players_df.copy()
    
    if league_filter != "All Leagues":
        filtered_df = filtered_df[filtered_df['league'] == league_filter]
    
    if position_filter != "All Positions":
        filtered_df = filtered_df[filtered_df['position'] == position_filter]
    
    filtered_df = filtered_df[
        (filtered_df['age'] >= age_range[0]) & 
        (filtered_df['age'] <= age_range[1])
    ]
    
    filtered_df = filtered_df[
        (filtered_df['market_value'] >= value_range[0]) & 
        (filtered_df['market_value'] <= value_range[1])
    ]
    
    if search_query:
        filtered_df = filtered_df[
            filtered_df['name'].str.contains(search_query, case=False) |
            filtered_df['club'].str.contains(search_query, case=False)
        ]
    
    # Sort
    ascending = sort_order == "Ascending"
    filtered_df = filtered_df.sort_values(by=sort_by, ascending=ascending)
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Players", len(filtered_df))
    with col2:
        st.metric("Avg Age", f"{filtered_df['age'].mean():.1f}")
    with col3:
        st.metric("Avg Rating", f"{filtered_df['rating'].mean():.1f}")
    with col4:
        st.metric("Total Goals", filtered_df['goals'].sum())
    
    st.divider()
    
    # Players grid
    for idx, player in filtered_df.iterrows():
        with st.expander(f"**{player['name']}** - {player['position']} | {player['club']} | Rating: {player['rating']}", expanded=False):
            col1, col2, col3 = st.columns([2, 2, 1])
            
            with col1:
                st.markdown("### Player Info")
                st.write(f"**Age:** {player['age']}")
                st.write(f"**Nationality:** {player['nationality']}")
                st.write(f"**Market Value:** {format_value(player['market_value'])}")
                st.write(f"**League:** {player['league']}")
                
                # Add to watchlist button
                if st.button(f"{'Remove from' if player['id'] in st.session_state.watchlist else 'Add to'} Watchlist", 
                           key=f"watchlist_{player['id']}"):
                    if player['id'] in st.session_state.watchlist:
                        st.session_state.watchlist.remove(player['id'])
                    else:
                        st.session_state.watchlist.append(player['id'])
                    st.rerun()
            
            with col2:
                st.markdown("### Performance Stats")
                stats_col1, stats_col2 = st.columns(2)
                with stats_col1:
                    st.write(f"**Goals:** {player['goals']}")
                    st.write(f"**Assists:** {player['assists']}")
                    st.write(f"**Matches:** {player['matches']}")
                    st.write(f"**Minutes:** {player['minutes_played']}")
                with stats_col2:
                    st.write(f"**Shots/Game:** {player['shots_per_game']}")
                    st.write(f"**Key Passes:** {player['key_passes']}")
                    st.write(f"**Pass Acc:** {player['pass_accuracy']}%")
                    st.write(f"**Dribbles:** {player['dribbles']}")
            
            with col3:
                st.markdown("### Data Sources")
                sources = {
                    'FBref': player['fbref'],
                    'Transfermarkt': player['transfermarkt'],
                    'ASA': player['asa'],
                    'Sofascore': player['sofascore']
                }
                for source, available in sources.items():
                    if available:
                        st.success(f"✓ {source}")
                    else:
                        st.error(f"✗ {source}")
            
            # Advanced stats visualization
            st.markdown("### Performance Breakdown")
            
            # Create radar chart
            categories = ['Goals', 'Assists', 'Pass Acc', 'Dribbles', 'Tackles', 'Interceptions']
            values = [
                player['goals'] / filtered_df['goals'].max() * 100,
                player['assists'] / filtered_df['assists'].max() * 100,
                player['pass_accuracy'],
                player['dribbles'] / filtered_df['dribbles'].max() * 100,
                player['tackles'] / filtered_df['tackles'].max() * 100,
                player['interceptions'] / filtered_df['interceptions'].max() * 100
            ]
            
            fig = go.Figure(data=go.Scatterpolar(
                r=values,
                theta=categories,
                fill='toself'
            ))
            
            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, 100]
                    )),
                showlegend=False,
                height=300
            )
            
            st.plotly_chart(fig, use_container_width=True)

# Teams Tab
with tab2:
    # Filter teams by league
    teams_league_filter = st.selectbox(
        "Filter by League",
        ["All Leagues", "MLS", "USL Championship", "USL League One"],
        key="teams_league"
    )
    
    filtered_teams = teams_df.copy()
    if teams_league_filter != "All Leagues":
        filtered_teams = filtered_teams[filtered_teams['league'] == teams_league_filter]
    
    # Display teams
    for idx, team in filtered_teams.iterrows():
        with st.container():
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.markdown(f"### {team['name']}")
                st.write(f"League: {team['league']}")
            with col2:
                st.metric("Squad Size", team['players'])
            with col3:
                st.metric("Avg Age", f"{team['avg_age']:.1f}")
            with col4:
                st.metric("Total Value", format_value(team['market_value']))
            st.divider()
    
    # Team comparison chart
    if len(filtered_teams) > 0:
        st.markdown("### Team Market Value Comparison")
        fig = px.bar(
            filtered_teams,
            x='name',
            y='market_value',
            color='league',
            title="Market Value by Team",
            labels={'market_value': 'Market Value ($)', 'name': 'Team'}
        )
        st.plotly_chart(fig, use_container_width=True)

# Watchlist Tab
with tab3:
    if len(st.session_state.watchlist) == 0:
        st.info("⭐ Your watchlist is empty. Add players from the Players tab to track them here.")
    else:
        watchlist_players = players_df[players_df['id'].isin(st.session_state.watchlist)]
        
        st.markdown(f"### Tracking {len(watchlist_players)} Players")
        
        # Watchlist summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Value", format_value(watchlist_players['market_value'].sum()))
        with col2:
            st.metric("Avg Rating", f"{watchlist_players['rating'].mean():.1f}")
        with col3:
            st.metric("Total Goals", watchlist_players['goals'].sum())
        with col4:
            st.metric("Total Assists", watchlist_players['assists'].sum())
        
        st.divider()
        
        # Display watchlist players
        for idx, player in watchlist_players.iterrows():
            col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 2, 1])
            with col1:
                st.write(f"**{player['name']}**")
                st.write(f"{player['position']} | {player['club']}")
            with col2:
                st.write(f"Rating: **{player['rating']}**")
            with col3:
                st.write(f"Value: {format_value(player['market_value'])}")
            with col4:
                st.write(f"G: {player['goals']} | A: {player['assists']}")
            with col5:
                if st.button("Remove", key=f"remove_{player['id']}"):
                    st.session_state.watchlist.remove(player['id'])
                    st.rerun()
            st.divider()
        
        # Export watchlist
        if st.button("📥 Export Watchlist to CSV"):
            csv = watchlist_players.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"soccer_scout_watchlist_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: #666;'>
    <p>Soccer Scout Pro - Aggregating data from FBref, Transfermarkt, American Soccer Analysis, and Sofascore</p>
    <p>Note: This is a demo application with mock data. In production, real-time API connections would be implemented.</p>
</div>
""", unsafe_allow_html=True)