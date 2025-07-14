# app.py (Part 1 of 6)
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import os
from io import BytesIO
import xlsxwriter

# Import our modules
from database_manager import DatabaseManager
from config import Config
from data_aggregator import DataAggregator
from analytics_engine import AdvancedAnalytics

# Page configuration
st.set_page_config(
    page_title="Soccer Scout Pro - Advanced",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main {
        padding: 0rem 1rem;
    }
    .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
        font-size: 18px;
        font-weight: 500;
    }
    .metric-card {
        background-color: #1e1e1e;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 20px;
        border: 1px solid #333;
    }
    .sync-status {
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .sync-success {
        background-color: #28a745;
        color: white;
    }
    .sync-error {
        background-color: #dc3545;
        color: white;
    }
    .player-card {
        border: 1px solid #ddd;
        border-radius: 8px;
        padding: 15px;
        margin-bottom: 10px;
        background-color: #f8f9fa;
    }
    .source-indicator {
        display: inline-block;
        width: 10px;
        height: 10px;
        border-radius: 50%;
        margin-right: 5px;
    }
    .source-active {
        background-color: #28a745;
    }
    .source-inactive {
        background-color: #dc3545;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'initialized' not in st.session_state:
    st.session_state.initialized = True
    st.session_state.watchlist = []
    st.session_state.last_sync = {}
    st.session_state.selected_player = None
    st.session_state.comparison_players = []

# Initialize components
@st.cache_resource
def init_components():
    """Initialize database and analytics components"""
    Config.validate_config()
    db_manager = DatabaseManager()
    aggregator = DataAggregator(db_manager)
    analytics = AdvancedAnalytics(db_manager)
    return db_manager, aggregator, analytics

db_manager, aggregator, analytics = init_components()

# Helper functions
def format_value(value):
    """Format market value for display"""
    if pd.isna(value) or value == 0:
        return "N/A"
    if value >= 1000000:
        return f"${value/1000000:.1f}M"
    elif value >= 1000:
        return f"${value/1000:.0f}K"
    return f"${value:,.0f}"

def format_datetime(dt):
    """Format datetime for display"""
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    return dt.strftime("%Y-%m-%d %H:%M")

def get_source_indicators(player):
    """Get HTML for source indicators"""
    sources = {
        'FBref': player.get('fbref_id') is not None,
        'TM': player.get('transfermarkt_id') is not None,
        'ASA': player.get('asa_id') is not None,
        'Sofascore': player.get('sofascore_id') is not None
    }
    
    html = ""
    for source, active in sources.items():
        status_class = "source-active" if active else "source-inactive"
        html += f'<span class="source-indicator {status_class}" title="{source}"></span>'
    
    return html

# Sidebar
with st.sidebar:
    st.header("⚙️ Data Management")
    
    # API Configuration
    with st.expander("🔑 API Configuration"):
        fbref_key = st.text_input("FBref API Key", type="password", key="fbref_key")
        tm_key = st.text_input("Transfermarkt API Key", type="password", key="tm_key")
        asa_key = st.text_input("ASA API Key", type="password", key="asa_key")
        sofascore_key = st.text_input("Sofascore API Key", type="password", key="sofascore_key")
        
        if st.button("Save API Keys"):
            # In production, save these securely
            st.success("API keys updated (in-memory only)")
    
    # Data Sync
    st.subheader("🔄 Data Synchronization")
    
    sync_league = st.selectbox(
        "Select League",
        ["MLS", "USL Championship", "USL League One"]
    )
    
    # Show last sync info
    if sync_league in st.session_state.last_sync:
        last_sync = st.session_state.last_sync[sync_league]
        st.info(f"Last sync: {format_datetime(last_sync)}")
    
    if st.button("🚀 Sync Now", use_container_width=True):
        with st.spinner(f"Syncing {sync_league} data..."):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            def update_progress(progress, message):
                progress_bar.progress(progress)
                status_text.text(message)
            
            try:
                results = aggregator.sync_league_data(sync_league, update_progress)
                
                st.session_state.last_sync[sync_league] = datetime.now()
                
                # Show results
                st.success(f"✅ Sync completed successfully!")
                st.metric("Total Players", results['total_players'])
                
                # Show source breakdown
                for source, count in results['sources'].items():
                    st.metric(source, count)
                
                if results['errors']:
                    with st.expander("⚠️ Errors"):
                        for error in results['errors']:
                            st.error(error)
                            
            except Exception as e:
                st.error(f"❌ Sync failed: {str(e)}")
    
    # Cache Management
    st.subheader("💾 Cache Management")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Clear Cache"):
            cleared = db_manager.clear_expired_cache()
            st.success(f"Cleared {cleared} entries")
    
    with col2:
        if st.button("View Stats"):
            conn = db_manager.get_connection()
            cache_size = pd.read_sql_query(
                "SELECT COUNT(*) as count, MIN(expiry) as oldest, MAX(expiry) as newest FROM cache",
                conn
            ).iloc[0]
            conn.close()
            
            st.write(f"Entries: {cache_size['count']}")
            if cache_size['oldest']:
                st.write(f"Oldest: {cache_size['oldest'][:10]}")
    
    # Sync History
    with st.expander("📊 Sync History"):
        sync_history = db_manager.get_sync_history(limit=5)
        for sync in sync_history:
            st.write(f"{sync['league']} - {sync['status']}")
            st.write(f"{sync['started_at'][:16]}")
            if sync['error_message']:
                st.error(sync['error_message'])
            st.divider()

    # app.py (Part 2 of 6)
# Main Content - continues from Part 1

# Main Content
st.title("⚽ Soccer Scout Pro - Advanced")
st.markdown("**Professional scouting platform with real-time data from FBref, Transfermarkt, ASA, and Sofascore**")

# Main tabs
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Players", "🏟️ Teams", "⭐ Watchlist", 
    "📈 Analytics", "📄 Reports", "🔍 Search"
])

# Players Tab
with tab1:
    st.subheader("Player Database")
    
    # Filters
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        league_filter = st.selectbox(
            "League",
            ["All"] + ["MLS", "USL Championship", "USL League One"],
            key="player_league_filter"
        )
    
    with col2:
        position_filter = st.selectbox(
            "Position",
            ["All"] + ["GK", "CB", "LB", "RB", "CDM", "CM", "CAM", "LW", "RW", "ST", "FW"],
            key="player_position_filter"
        )
    
    with col3:
        age_range = st.slider(
            "Age Range",
            16, 40, (20, 30),
            key="player_age_filter"
        )
    
    with col4:
        min_rating = st.slider(
            "Min Rating",
            5.0, 10.0, 6.5, 0.1,
            key="player_rating_filter"
        )
    
    # Search
    search_term = st.text_input(
        "🔍 Search players or clubs",
        placeholder="Enter name or club...",
        key="player_search"
    )
    
    # Advanced filters
    with st.expander("Advanced Filters"):
        col1, col2 = st.columns(2)
        with col1:
            min_minutes = st.number_input(
                "Min Minutes Played",
                0, 5000, 900, 100
            )
            has_market_value = st.checkbox("Has Market Value")
        with col2:
            max_value = st.number_input(
                "Max Market Value ($)",
                0, 50000000, 10000000, 100000
            )
            data_sources = st.multiselect(
                "Data Sources",
                ["FBref", "Transfermarkt", "ASA", "Sofascore"]
            )
    
    # Build query
    conn = db_manager.get_connection()
    query = "SELECT * FROM players WHERE 1=1"
    params = []
    
    if league_filter != "All":
        query += " AND league = ?"
        params.append(league_filter)
    
    if position_filter != "All":
        query += " AND position = ?"
        params.append(position_filter)
    
    query += " AND age BETWEEN ? AND ?"
    params.extend(age_range)
    
    query += " AND rating >= ?"
    params.append(min_rating)
    
    query += " AND minutes_played >= ?"
    params.append(min_minutes)
    
    if has_market_value:
        query += " AND market_value > 0"
    
    query += " AND market_value <= ?"
    params.append(max_value)
    
    if search_term:
        query += " AND (name LIKE ? OR club LIKE ?)"
        params.extend([f"%{search_term}%", f"%{search_term}%"])
    
    # Add data source filters
    if data_sources:
        source_conditions = []
        if "FBref" in data_sources:
            source_conditions.append("fbref_id IS NOT NULL")
        if "Transfermarkt" in data_sources:
            source_conditions.append("transfermarkt_id IS NOT NULL")
        if "ASA" in data_sources:
            source_conditions.append("asa_id IS NOT NULL")
        if "Sofascore" in data_sources:
            source_conditions.append("sofascore_id IS NOT NULL")
        
        if source_conditions:
            query += f" AND ({' OR '.join(source_conditions)})"
    
    # Execute query
    players_df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    # Display results
    st.write(f"Found **{len(players_df)}** players")
    
    if not players_df.empty:
        # Sort options
        col1, col2 = st.columns([3, 1])
        with col1:
            sort_by = st.selectbox(
                "Sort by",
                ["rating", "market_value", "goals", "assists", "age", "minutes_played"]
            )
        with col2:
            sort_order = st.radio("Order", ["Desc", "Asc"])
        
        players_df = players_df.sort_values(
            sort_by, 
            ascending=(sort_order == "Asc")
        )
        
        # Display players
        for idx, player in players_df.iterrows():
            with st.container():
                col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 1, 1])
                
                with col1:
                    st.markdown(f"### {player['name']}")
                    st.write(f"{player['position']} • {player['club']} • {player['league']}")
                    st.markdown(get_source_indicators(player), unsafe_allow_html=True)
                
                with col2:
                    st.metric("Rating", f"{player['rating']:.1f}")
                    st.metric("Age", player['age'])
                
                with col3:
                    st.metric("Value", format_value(player['market_value']))
                    st.metric("Minutes", f"{player['minutes_played']:,}")
                
                with col4:
                    st.metric("Goals", player['goals'])
                    st.metric("Assists", player['assists'])
                
                with col5:
                    if st.button("👁️ View", key=f"view_{player['id']}"):
                        st.session_state.selected_player = player['id']
                        st.session_state.active_tab = "analytics"
                    
                    if st.button("⭐ Watch", key=f"watch_{player['id']}"):
                        if player['id'] not in st.session_state.watchlist:
                            st.session_state.watchlist.append(player['id'])
                            st.success("Added!")
                        else:
                            st.info("Already watching")
                
                st.divider()

            # app.py (Part 3 of 6)
# Teams and Watchlist tabs

# Teams Tab
with tab2:
    st.subheader("Team Analysis")
    
    # Get teams
    conn = db_manager.get_connection()
    teams_query = """
        SELECT 
            club as team_name,
            league,
            COUNT(*) as squad_size,
            AVG(age) as avg_age,
            AVG(rating) as avg_rating,
            SUM(market_value) as total_value,
            SUM(goals) as total_goals,
            SUM(assists) as total_assists
        FROM players
        GROUP BY club, league
        HAVING COUNT(*) >= 5
        ORDER BY total_value DESC
    """
    teams_df = pd.read_sql_query(teams_query, conn)
    conn.close()
    
    # Filters
    col1, col2 = st.columns(2)
    with col1:
        team_league_filter = st.selectbox(
            "Filter by League",
            ["All"] + teams_df['league'].unique().tolist()
        )
    with col2:
        sort_metric = st.selectbox(
            "Sort by",
            ["total_value", "avg_rating", "total_goals", "squad_size"]
        )
    
    # Apply filters
    if team_league_filter != "All":
        teams_df = teams_df[teams_df['league'] == team_league_filter]
    
    teams_df = teams_df.sort_values(sort_metric, ascending=False)
    
    # Display teams
    for idx, team in teams_df.iterrows():
        with st.expander(f"{team['team_name']} - {team['league']}"):
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Squad Size", int(team['squad_size']))
                st.metric("Avg Age", f"{team['avg_age']:.1f}")
            
            with col2:
                st.metric("Avg Rating", f"{team['avg_rating']:.2f}")
                st.metric("Total Value", format_value(team['total_value']))
            
            with col3:
                st.metric("Total Goals", int(team['total_goals']))
                st.metric("Total Assists", int(team['total_assists']))
            
            with col4:
                if st.button("📊 Full Analysis", key=f"analyze_team_{idx}"):
                    team_analytics = analytics.get_team_analytics(
                        team['team_name'], 
                        team['league']
                    )
                    
                    # Display team analytics
                    st.subheader("Position Distribution")
                    if 'position_distribution' in team_analytics:
                        fig = px.pie(
                            values=list(team_analytics['position_distribution'].values()),
                            names=list(team_analytics['position_distribution'].keys()),
                            title=f"{team['team_name']} Squad Composition"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Top performers
                    if 'top_performers' in team_analytics:
                        st.subheader("Top Performers")
                        top_df = pd.DataFrame(team_analytics['top_performers'])
                        st.dataframe(top_df, use_container_width=True)

# Watchlist Tab
with tab3:
    st.subheader("⭐ Your Watchlist")
    
    if not st.session_state.watchlist:
        st.info("Your watchlist is empty. Add players from the Players tab.")
    else:
        conn = db_manager.get_connection()
        watchlist_query = f"""
            SELECT * FROM players 
            WHERE id IN ({','.join('?' * len(st.session_state.watchlist))})
        """
        watchlist_df = pd.read_sql_query(
            watchlist_query, 
            conn, 
            params=st.session_state.watchlist
        )
        conn.close()
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Players", len(watchlist_df))
        with col2:
            st.metric("Total Value", format_value(watchlist_df['market_value'].sum()))
        with col3:
            st.metric("Avg Rating", f"{watchlist_df['rating'].mean():.2f}")
        with col4:
            st.metric("Total Goals", watchlist_df['goals'].sum())
        
        # Watchlist table
        display_cols = ['name', 'position', 'club', 'league', 'age', 'rating', 
                       'market_value', 'goals', 'assists']
        
        # Format display
        display_df = watchlist_df[display_cols].copy()
        display_df['market_value'] = display_df['market_value'].apply(format_value)
        
        st.dataframe(display_df, use_container_width=True)
        
        # Actions
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🗑️ Clear Watchlist"):
                st.session_state.watchlist = []
                st.rerun()
        
        with col2:
            if st.button("📊 Compare Players"):
                st.session_state.comparison_players = st.session_state.watchlist[:4]
        
        with col3:
            if st.button("📥 Export to Excel"):
                output = BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    watchlist_df.to_excel(writer, sheet_name='Watchlist', index=False)
                    
                    # Get the workbook and worksheet
                    workbook = writer.book
                    worksheet = writer.sheets['Watchlist']
                    
                    # Format header
                    header_format = workbook.add_format({
                        'bold': True,
                        'bg_color': '#4CAF50',
                        'font_color': 'white'
                    })
                    
                    for col_num, value in enumerate(watchlist_df.columns.values):
                        worksheet.write(0, col_num, value, header_format)
                
                output.seek(0)
                st.download_button(
                    label="Download Excel",
                    data=output,
                    file_name=f"watchlist_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        
        # Player comparison
        if st.session_state.comparison_players:
            st.subheader("Player Comparison")
            
            comparison_df = watchlist_df[
                watchlist_df['id'].isin(st.session_state.comparison_players)
            ]
            
            if len(comparison_df) >= 2:
                # Radar chart
                categories = ['Rating', 'Goals', 'Assists', 'Age', 'Minutes/100']
                
                fig = go.Figure()
                
                for idx, player in comparison_df.iterrows():
                    values = [
                        player['rating'],
                        player['goals'],
                        player['assists'],
                        player['age'] / 5,  # Scale age
                        player['minutes_played'] / 100
                    ]
                    
                    fig.add_trace(go.Scatterpolar(
                        r=values,
                        theta=categories,
                        fill='toself',
                        name=player['name']
                    ))
                
                fig.update_layout(
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            range=[0, max(comparison_df['rating'].max(), 10)]
                        )
                    ),
                    showlegend=True,
                    title="Player Comparison"
                )
                
                st.plotly_chart(fig, use_container_width=True)

                # app.py (Part 4 of 6)
# Analytics Tab

# Analytics Tab
with tab4:
    st.subheader("📈 Advanced Analytics")
    
    # Player selection
    conn = db_manager.get_connection()
    all_players = pd.read_sql_query(
        "SELECT id, name, club, league FROM players ORDER BY rating DESC",
        conn
    )
    conn.close()
    
    if not all_players.empty:
        player_options = {
            f"{p['name']} ({p['club']} - {p['league']})": p['id'] 
            for _, p in all_players.iterrows()
        }
        
        selected_player_name = st.selectbox(
            "Select Player for Analysis",
            list(player_options.keys())
        )
        selected_player_id = player_options[selected_player_name]
        
        if st.button("🔍 Analyze Player"):
            # Get percentiles
            percentiles = analytics.get_player_percentiles(selected_player_id)
            
            if percentiles:
                st.subheader(f"Analysis for {percentiles['player_name']}")
                
                # Percentile chart
                if 'percentiles' in percentiles:
                    percentile_data = []
                    for metric, data in percentiles['percentiles'].items():
                        percentile_data.append({
                            'Metric': metric,
                            'Percentile': data['percentile'],
                            'Value': data['value']
                        })
                    
                    df_percentiles = pd.DataFrame(percentile_data)
                    
                    fig = px.bar(
                        df_percentiles,
                        x='Metric',
                        y='Percentile',
                        title='Performance Percentiles vs Position Peers',
                        color='Percentile',
                        color_continuous_scale='RdYlGn',
                        range_color=[0, 100]
                    )
                    fig.update_layout(yaxis=dict(range=[0, 100]))
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Display values
                    for _, row in df_percentiles.iterrows():
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.write(f"**{row['Metric']}**")
                        with col2:
                            st.write(f"Value: {row['Value']}")
                        with col3:
                            color = "🟢" if row['Percentile'] > 70 else "🟡" if row['Percentile'] > 40 else "🔴"
                            st.write(f"{color} {row['Percentile']:.1f}%ile")
                
                # Similar players
                st.subheader("Similar Players")
                similar_players = analytics.find_similar_players(selected_player_id, num_similar=8)
                
                if similar_players:
                    similar_df = pd.DataFrame(similar_players)
                    
                    # Format for display
                    display_similar = similar_df[['name', 'club', 'league', 'age', 
                                                 'rating', 'similarity_score']].copy()
                    display_similar['market_value'] = similar_df['market_value'].apply(format_value)
                    
                    st.dataframe(display_similar, use_container_width=True)

                    # app.py (Part 5 of 6)
# Reports Tab

# Reports Tab
with tab5:
    st.subheader("📄 Scouting Reports")
    
    report_type = st.selectbox(
        "Report Type",
        ["Player Scouting Report", "Team Analysis Report", 
         "League Overview", "Data Coverage Report"]
    )
    
    if report_type == "Player Scouting Report":
        # Player selection
        conn = db_manager.get_connection()
        report_players = pd.read_sql_query(
            "SELECT id, name, club, league FROM players WHERE rating > 6.5 ORDER BY rating DESC",
            conn
        )
        conn.close()
        
        if not report_players.empty:
            player_select = {
                f"{p['name']} ({p['club']})": p['id']
                for _, p in report_players.iterrows()
            }
            
            selected_for_report = st.selectbox(
                "Select Player",
                list(player_select.keys())
            )
            
            if st.button("📄 Generate Report"):
                with st.spinner("Generating comprehensive scouting report..."):
                    report = analytics.generate_player_report(
                        player_select[selected_for_report]
                    )
                    
                    if report:
                        # Player header
                        player_info = report['player']
                        col1, col2, col3 = st.columns([2, 1, 1])
                        
                        with col1:
                            st.markdown(f"# {player_info['name']}")
                            st.write(f"{player_info['position']} • {player_info['club']} • {player_info['league']}")
                        
                        with col2:
                            st.metric("Age", player_info['age'])
                            st.metric("Rating", f"{player_info['rating']:.1f}")
                        
                        with col3:
                            st.metric("Market Value", format_value(player_info['market_value']))
                            st.metric("Minutes", f"{player_info['minutes_played']:,}")
                        
                        # Strengths and Weaknesses
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("### ✅ Strengths")
                            for strength in report['strengths']:
                                st.write(f"• {strength['description']}")
                        
                        with col2:
                            st.markdown("### ⚠️ Areas for Improvement")
                            for weakness in report['weaknesses']:
                                st.write(f"• {weakness['description']}")
                        
                        # Recommendation
                        st.markdown("### 🎯 Scouting Recommendation")
                        rec = report['recommendation']
                        st.info(rec['overall_assessment'])
                        st.write(f"**Suggested Action:** {rec['suggested_action']}")
                        
                        if rec['development_areas']:
                            st.write(f"**Focus Areas:** {', '.join(rec['development_areas'])}")
                        
                        # Value Assessment
                        st.markdown("### 💰 Value Assessment")
                        value = report['value_assessment']
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.metric("Current Value", format_value(value['current_value']))
                            st.write(f"**Assessment:** {value['value_rating']}")
                        
                        with col2:
                            st.write(f"**Projection:** {value['projection']}")
                            for factor in value['factors']:
                                st.write(f"• {factor}")
                        
                        # Export options
                        st.markdown("### 📥 Export Report")
                        
                        # Create downloadable report
                        report_text = f"""
PLAYER SCOUTING REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}

PLAYER: {player_info['name']}
Position: {player_info['position']}
Club: {player_info['club']}
League: {player_info['league']}
Age: {player_info['age']}
Market Value: {format_value(player_info['market_value'])}

PERFORMANCE METRICS
Rating: {player_info['rating']}
Goals: {player_info['goals']}
Assists: {player_info['assists']}
Minutes: {player_info['minutes_played']}

RECOMMENDATION
{rec['overall_assessment']}
Suggested Action: {rec['suggested_action']}

VALUE ASSESSMENT
Current Value: {format_value(value['current_value'])}
Assessment: {value['value_rating']}
Projection: {value['projection']}
                        st.download_button(
                            label="Download Text Report",
                            data=report_text,
                            file_name=f"scouting_report_{player_info['name'].replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.txt",
                            mime="text/plain"
                        )
    
    elif report_type == "Data Coverage Report":
        st.subheader("Data Source Coverage by League")
        
        for league in ["MLS", "USL Championship", "USL League One"]:
            coverage = aggregator.get_data_coverage_report(league)
            
            if coverage:
                st.markdown(f"### {league}")
                
                col1, col2, col3, col4, col5 = st.columns(5)
                
                with col1:
                    st.metric("Total Players", coverage['total_players'])
                
                for idx, (source, data) in enumerate(coverage['sources'].items()):
                    col = [col2, col3, col4, col5][idx]
                    with col:
                        st.metric(
                            source,
                            f"{data['percentage']}%",
                            f"{data['count']} players"
                        )
                
                st.divider()

                # app.py (Part 6 of 6)
# Search Tab and Footer

# Search Tab
with tab6:
    st.subheader("🔍 Advanced Search")
    
    search_query = st.text_area(
        "Enter search criteria (SQL-like)",
        placeholder="e.g., rating > 7.5 AND age < 25 AND position IN ('ST', 'LW', 'RW')",
        height=100
    )
    
    if st.button("Execute Search"):
        try:
            # Build safe query (in production, use proper SQL sanitization)
            conn = db_manager.get_connection()
            
            # Base query with user conditions
            full_query = f"SELECT * FROM players WHERE {search_query}"
            
            results_df = pd.read_sql_query(full_query, conn)
            conn.close()
            
            st.success(f"Found {len(results_df)} players")
            
            if not results_df.empty:
                # Display results
                display_cols = ['name', 'position', 'club', 'league', 'age', 
                              'rating', 'market_value', 'goals', 'assists']
                st.dataframe(results_df[display_cols], use_container_width=True)
                
                # Export option
                csv = results_df.to_csv(index=False)
                st.download_button(
                    label="Download Results as CSV",
                    data=csv,
                    file_name=f"search_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        
        except Exception as e:
            st.error(f"Search error: {str(e)}")
            st.info("Please check your search syntax")

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: #666; padding: 20px;'>
    <p>Soccer Scout Pro - Professional Scouting Platform</p>
    <p>Data aggregated from FBref, Transfermarkt, American Soccer Analysis, and Sofascore</p>
    <p>Last updated: {}</p>
</div>
""".format(datetime.now().strftime('%Y-%m-%d %H:%M')), unsafe_allow_html=True)



                