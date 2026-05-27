"""
dashboard.py
=============
Streamlit Dashboard for Real-Time Energy Grid & Renewable Monitor

Displays all metrics in real-time:
- Green Score per zone
- Renewable production & alerts
- Energy balance (surplus/deficit)
- Pricing alerts
- System statistics
"""

import streamlit as st
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
import time

st.set_page_config(
    page_title="Energy Grid Monitor",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Real-Time Energy Grid & Renewable Monitor")

# Sidebar
st.sidebar.title("Dashboard Controls")
refresh_rate = st.sidebar.slider("Refresh rate (seconds)", 1, 30, 5)
auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)

# Helper functions
def read_jsonl(filepath, limit=100):
    """Read last N lines from JSONL file."""
    if not Path(filepath).exists():
        return []
    
    try:
        data = []
        with open(filepath, 'r') as f:
            for line in f:
                if line.strip():
                    data.append(json.loads(line))
        return data[-limit:]
    except Exception as e:
        st.error(f"Error reading {filepath}: {e}")
        return []

def get_latest_per_zone(data, zone_key="zone"):
    """Get latest record per zone."""
    latest = {}
    for record in data:
        zone = record.get(zone_key)
        if zone:
            latest[zone] = record
    return latest

def get_color_for_rating(rating):
    """Get color for green score rating."""
    colors = {
        "EXCELLENT": "#00AA00",
        "VERY_GOOD": "#22CC22",
        "GOOD": "#FFAA00",
        "FAIR": "#FF8800",
        "POOR": "#CC0000"
    }
    return colors.get(rating, "#999999")

# Main content
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Green Score", 
    "Renewable Report", 
    "Energy Balance", 
    "Pricing Alerts",
    "Zone Leaderboard",
    "System Stats"
])

# TAB 1: Green Score
with tab1:
    st.header("Green Score Per Zone")
    
    green_data = read_jsonl("output/green_score.jsonl", limit=1000)
    latest_scores = get_latest_per_zone(green_data, "zone")
    
    if latest_scores:
        col1, col2, col3, col4 = st.columns(4)
        
        for idx, (zone, data) in enumerate(sorted(latest_scores.items())):
            with [col1, col2, col3, col4][idx % 4]:
                score = data.get("green_score", 0)
                rating = data.get("rating", "UNKNOWN")
                renewable_pct = data.get("renewable_share_pct", 0)
                
                st.metric(
                    label=f"{zone} - {rating}",
                    value=f"{score:.1f}/100",
                    delta=f"{renewable_pct:.1f}% renewable"
                )
        
        # Detailed breakdown
        st.subheader("Detailed Breakdown")
        
        breakdown_data = []
        for zone, data in sorted(latest_scores.items()):
            breakdown_data.append({
                "Zone": zone,
                "Score": f"{data.get('green_score', 0):.1f}",
                "Rating": data.get("rating"),
                "Renewable %": f"{data.get('renewable_share_pct', 0):.1f}%",
                "Capacity Factor %": f"{data.get('capacity_factor_pct', 0):.1f}%",
                "Weather": data.get("weather_condition"),
                "Price EUR/MWh": f"{data.get('price_eur_mwh', 0):.2f}",
                "Renewable MW": f"{data.get('renewable_mw', 0):.0f}",
                "Consumption MW": f"{data.get('consumption_mw', 0):.0f}",
                "Surplus/Deficit MW": f"{data.get('surplus_deficit_mw', 0):.1f}",
            })
        
        df_breakdown = pd.DataFrame(breakdown_data)
        st.dataframe(df_breakdown, use_container_width=True)
        
        # Chart: Score trend
        st.subheader("Green Score Trend (Last 50 readings)")
        chart_data = []
        for record in green_data[-50:]:
            chart_data.append({
                "Timestamp": record.get("timestamp", ""),
                "Zone": record.get("zone", ""),
                "Score": record.get("green_score", 0),
            })
        
        if chart_data:
            df_chart = pd.DataFrame(chart_data)
            df_pivot = df_chart.set_index("Timestamp").pivot_table(
                values="Score", 
                index="Timestamp", 
                columns="Zone", 
                aggfunc="last"
            )
            st.line_chart(df_pivot)
    else:
        st.warning("No green score data available yet. Start the consumer to populate.")

# TAB 2: Renewable Report
with tab2:
    st.header("Renewable Production & Alerts")
    
    renewable_data = read_jsonl("output/renewable_report.jsonl", limit=500)
    latest_renewable = get_latest_per_zone(renewable_data, "country_zone")
    
    if latest_renewable:
        renewable_list = []
        for zone, data in sorted(latest_renewable.items()):
            alerts = data.get("alerts", [])
            renewable_list.append({
                "Zone": zone,
                "Solar MW": f"{data.get('solar_mw', 0):.0f}",
                "Wind MW": f"{data.get('wind_mw', 0):.0f}",
                "Total MW": f"{data.get('total_renewable_mw', 0):.0f}",
                "Capacity Factor %": f"{data.get('total_capacity_factor_pct', 0):.1f}%",
                "Dominant": data.get("dominant_source", "-"),
                "Weather": data.get("weather_label", "-"),
                "Alerts": ", ".join(alerts) if alerts else "None"
            })
        
        df_renewable = pd.DataFrame(renewable_list)
        st.dataframe(df_renewable, use_container_width=True)
        
        # Alerts summary
        st.subheader("Recent Alerts")
        alerts_data = read_jsonl("output/alerts_monitor.jsonl", limit=100)
        
        if alerts_data:
            alert_summary = []
            for alert in alerts_data[-20:]:
                alert_summary.append({
                    "Timestamp": alert.get("timestamp", "")[-8:],
                    "Zone": alert.get("zone", ""),
                    "Type": alert.get("alert_type", ""),
                    "Reason": alert.get("reason", ""),
                    "Context": alert.get("weather_context", ""),
                })
            
            df_alerts = pd.DataFrame(alert_summary)
            
            # Color code by alert type
            def alert_color(row):
                if "LOW_RENEWABLE" in row["Type"]:
                    return ["background-color: #ffcccc"] * len(row)
                elif "DOMINANT" in row["Type"]:
                    return ["background-color: #ffffcc"] * len(row)
                return [""] * len(row)
            
            st.dataframe(df_alerts, use_container_width=True)
        else:
            st.info("No alerts yet.")
    else:
        st.warning("No renewable report data available yet.")

# TAB 3: Energy Balance
with tab3:
    st.header("Energy Balance (Surplus/Deficit)")
    
    balance_data = read_jsonl("output/balancing_log.jsonl", limit=500)
    latest_balance = get_latest_per_zone(balance_data, "zone")
    
    if latest_balance:
        balance_list = []
        for zone, data in sorted(latest_balance.items()):
            status = data.get("status", "UNKNOWN")
            balance_list.append({
                "Zone": zone,
                "Renewable MW": f"{data.get('renewable_generation_mw', 0):.0f}",
                "Consumption MW": f"{data.get('consumption_mw', 0):.0f}",
                "Balance MW": f"{data.get('surplus_deficit_mw', 0):.1f}",
                "Status": status,
                "Renewable %": f"{data.get('renewable_share_pct', 0):.1f}%",
                "Solar MW": f"{data.get('solar_mw', 0):.0f}",
                "Wind MW": f"{data.get('wind_mw', 0):.0f}",
                "Price EUR/MWh": f"{data.get('spot_price_eur_mwh', 0):.2f}",
            })
        
        df_balance = pd.DataFrame(balance_list)
        
        col1, col2, col3, col4 = st.columns(4)
        
        surplus_zones = sum(1 for b in balance_list if "SURPLUS" in b["Status"])
        deficit_zones = sum(1 for b in balance_list if "DEFICIT" in b["Status"])
        avg_renewable = sum(float(b["Renewable %"].rstrip("%")) for b in balance_list) / len(balance_list) if balance_list else 0
        
        col1.metric("Surplus Zones", surplus_zones)
        col2.metric("Deficit Zones", deficit_zones)
        col3.metric("Avg Renewable %", f"{avg_renewable:.1f}%")
        col4.metric("Total Zones", len(balance_list))
        
        st.dataframe(df_balance, use_container_width=True)
    else:
        st.warning("No balance data available yet.")

# TAB 4: Pricing Alerts
with tab4:
    st.header("Pricing Alerts")
    
    pricing_data = read_jsonl("output/pricing_alerts.jsonl", limit=500)
    
    if pricing_data:
        # Latest per zone
        latest_pricing = get_latest_per_zone(pricing_data, "zone")
        
        pricing_list = []
        for zone, data in sorted(latest_pricing.items()):
            level = data.get("alert_level", "UNKNOWN")
            pricing_list.append({
                "Zone": zone,
                "Price EUR/MWh": f"{data.get('spot_price_eur_mwh', 0):.2f}",
                "Level": level,
                "Consumption MW": f"{data.get('consumption_mw', 0):.0f}",
                "Demand": data.get("demand_level", "-"),
            })
        
        df_pricing = pd.DataFrame(pricing_list)
        st.dataframe(df_pricing, use_container_width=True)
        
        # Price history chart
        st.subheader("Price Trend (Last 50 readings)")
        chart_data = []
        for record in pricing_data[-50:]:
            chart_data.append({
                "Timestamp": record.get("timestamp", "")[-8:],
                "Zone": record.get("zone", ""),
                "Price": record.get("spot_price_eur_mwh", 0),
            })
        
        if chart_data:
            df_chart = pd.DataFrame(chart_data)
            df_pivot = df_chart.set_index("Timestamp").pivot_table(
                values="Price",
                index="Timestamp",
                columns="Zone",
                aggfunc="last"
            )
            st.line_chart(df_pivot)
    else:
        st.warning("No pricing alerts data available yet.")

# TAB 5: Zone Leaderboard
with tab5:
    st.header("Live Zone Leaderboard - Trading Style")
    st.subheader("Real-time Ranking by Renewable Production")
    
    # Auto-refresh every 2 seconds for live updates
    leaderboard_data = read_jsonl("output/zone_leaderboard.jsonl", limit=500)
    
    if leaderboard_data:
        # Get latest and previous leaderboard for change detection
        latest_lb = leaderboard_data[-1]
        leaderboard_current = latest_lb.get("leaderboard", [])
        
        # Get previous if available
        previous_lb = None
        if len(leaderboard_data) > 1:
            previous_lb = leaderboard_data[-2].get("leaderboard", [])
        
        if leaderboard_current:
            # Create previous rank mapping for change detection
            previous_ranks = {}
            if previous_lb:
                for entry in previous_lb:
                    previous_ranks[entry["zone"]] = entry["rank"]
            
            # Display medals and top performers with styling
            col1, col2, col3 = st.columns(3)
            
            medal_colors = {
                1: "#FFD700",  # Gold
                2: "#C0C0C0",  # Silver
                3: "#CD7F32",  # Bronze
            }
            
            rank_data_medals = {}
            for entry in leaderboard_current[:3]:
                rank = entry["rank"]
                rank_data_medals[rank] = entry
            
            medals_config = {
                1: (col1, "🥇 1st Place - GOLD", medal_colors[1]),
                2: (col2, "🥈 2nd Place - SILVER", medal_colors[2]),
                3: (col3, "🥉 3rd Place - BRONZE", medal_colors[3]),
            }
            
            for rank, (col, title, color) in medals_config.items():
                if rank in rank_data_medals:
                    entry = rank_data_medals[rank]
                    with col:
                        prev_rank = previous_ranks.get(entry["zone"])
                        
                        # Determine if trending up/down
                        trend = ""
                        trend_color = "white"
                        if prev_rank and prev_rank != rank:
                            if rank < prev_rank:
                                trend = " 📈 UP"
                                trend_color = "green"
                            else:
                                trend = " 📉 DOWN"
                                trend_color = "red"
                        
                        st.markdown(f"""
                        <div style='background-color: {color}; padding: 15px; border-radius: 10px; text-align: center; color: black; font-weight: bold;'>
                            <div>{title}</div>
                            <div style='font-size: 24px; margin: 10px 0;'>{entry['zone']}</div>
                            <div style='font-size: 18px;'>{entry['total_renewable_mw']:.1f} MW</div>
                            <div style='font-size: 12px; color: {trend_color};'>{trend}</div>
                        </div>
                        """, unsafe_allow_html=True)
            
            st.divider()
            
            # Full ranking table with real-time styling - Using Streamlit native components
            st.subheader("Full Ranking - Live Updates")
            
            ranking_list = []
            for entry in leaderboard_current:
                rank = entry["rank"]
                zone = entry["zone"]
                total = entry["total_renewable_mw"]
                solar = entry["solar_mw"]
                wind = entry["wind_mw"]
                cf = entry["capacity_factor_pct"]
                weather = entry["weather"]
                medal = entry["medal"]
                
                # Determine trend
                prev_rank = previous_ranks.get(zone)
                if prev_rank and prev_rank != rank:
                    if rank < prev_rank:
                        trend = f"📈 +{prev_rank - rank}"
                    else:
                        trend = f"📉 -{rank - prev_rank}"
                else:
                    trend = "→ Stable"
                
                medal_display = f"[{medal}]" if medal else f"#{rank}"
                
                ranking_list.append({
                    "Rank": medal_display,
                    "Zone": zone,
                    "Total MW": f"{total:.1f}",
                    "Solar MW": f"{solar:.1f}",
                    "Wind MW": f"{wind:.1f}",
                    "Capacity %": f"{cf:.1f}%",
                    "Weather": weather,
                    "Trend": trend,
                })
            
            df_ranking = pd.DataFrame(ranking_list)
            
            # Display with conditional styling
            def style_ranking(row):
                styles = []
                rank_text = row["Rank"]
                
                if "[GOLD]" in str(rank_text):
                    styles = ["background-color: rgba(255, 215, 0, 0.2)"] * len(row)
                elif "[SILVER]" in str(rank_text):
                    styles = ["background-color: rgba(192, 192, 192, 0.2)"] * len(row)
                elif "[BRONZE]" in str(rank_text):
                    styles = ["background-color: rgba(205, 127, 50, 0.2)"] * len(row)
                else:
                    styles = [""] * len(row)
                
                return styles
            
            styled_df = df_ranking.style.apply(style_ranking, axis=1)
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
            
            st.divider()
            
            # Chart: Production with color coding
            st.subheader("Total Production Ranking")
            chart_data = []
            for entry in leaderboard_current:
                chart_data.append({
                    "Zone": entry["zone"],
                    "Total MW": entry["total_renewable_mw"],
                })
            
            df_chart_data = pd.DataFrame(chart_data).set_index("Zone").sort_values("Total MW", ascending=False)
            
            col_area, col_chart = st.columns([1, 2])
            with col_area:
                st.metric("Top Producer", df_chart_data.index[0], f"{df_chart_data.iloc[0, 0]:.1f} MW")
                st.metric("Zones Tracked", len(leaderboard_current))
                st.metric("Total Renewable", f"{df_chart_data['Total MW'].sum():.1f} MW")
            
            with col_chart:
                st.bar_chart(df_chart_data, color="#FF6B6B")
            
            # Real-time stats
            st.divider()
            st.subheader("Real-time Statistics")
            
            col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
            
            avg_total = df_chart_data['Total MW'].mean()
            max_total = df_chart_data['Total MW'].max()
            min_total = df_chart_data['Total MW'].min()
            diff = max_total - min_total
            
            col_stat1.metric("Average MW", f"{avg_total:.1f}")
            col_stat2.metric("Max MW", f"{max_total:.1f}")
            col_stat3.metric("Min MW", f"{min_total:.1f}")
            col_stat4.metric("Range MW", f"{diff:.1f}")
        else:
            st.warning("No leaderboard data available yet.")
    else:
        st.warning("Zone leaderboard consumer not running.")

# TAB 6: System Stats
with tab6:
    st.header("System Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Count records in each sink
    green_count = len(read_jsonl("output/green_score.jsonl", limit=10000))
    renewable_count = len(read_jsonl("output/renewable_report.jsonl", limit=10000))
    balance_count = len(read_jsonl("output/balancing_log.jsonl", limit=10000))
    pricing_count = len(read_jsonl("output/pricing_alerts.jsonl", limit=10000))
    alerts_count = len(read_jsonl("output/alerts_monitor.jsonl", limit=10000))
    leaderboard_count = len(read_jsonl("output/zone_leaderboard.jsonl", limit=10000))
    
    col1.metric("Green Score Records", green_count)
    col2.metric("Renewable Records", renewable_count)
    col3.metric("Balance Records", balance_count)
    col4.metric("Pricing Records", pricing_count)
    
    st.metric("Alert Monitor Records", alerts_count)
    st.metric("Leaderboard Records", leaderboard_count)
    
    # System Info
    st.subheader("System Configuration")
    
    system_info = {
        "Kafka Bootstrap": "localhost:19092,localhost:19093,localhost:19094",
        "Kafka Topics": "energy.production, energy.consumption, energy.alerts",
        "Consumer Groups": "renewable-report, alerts-monitor, balancing, pricing-alerts, storage, green-score, zone-leaderboard",
        "Data Location": "output/",
        "Format": "JSON Lines (.jsonl)",
        "Last Update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    for key, value in system_info.items():
        st.write(f"**{key}**: {value}")

# Auto-refresh
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()
