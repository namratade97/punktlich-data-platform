import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import requests
import os
import glob

st.set_page_config(page_title="Pünktlich - Berlin Hbf", layout="wide")

# Using a relative path for the database file in the repo
DB_PATH = "data/dbt.duckdb"
BRONZE_PATH = "data/bronze/*.parquet"

@st.cache_resource
def get_connection():
    # Using read_only=True is critical for web deployments
    return duckdb.connect("data/dbt.duckdb", read_only=True)

if os.path.exists(DB_PATH):
    con = get_connection()
    # Check if the gold table exists
    tables = con.execute("SHOW TABLES").fetchall()
    print(f"Tables found: {tables}")
else:
    print("Database file not found yet!")

# Updating these to match your specific GitHub details
GITHUB_REPO = "namratade97/punktlich-data-platform"
# This token is set in Hugging Face Space Secrets
GITHUB_TOKEN = os.getenv("GH_TOKEN") 

# UI HEADER
st.title("🚆 Pünktlich: Berlin Hbf Real-Time")
st.markdown("Analyzing train reliability using **Rust**, **dbt**, and **DuckDB**.")

st.markdown("""
This platform demonstrates a modern data stack pipeline:
1. **Ingest (Bronze):** Rust-based scraper fetches real-time XML from Deutsche Bahn API and saves as Parquet.
2. **Transform (Silver):** dbt cleans types, handles timezone conversion, and filters data.
3. **Analyze (Gold):** dbt aggregates performance metrics into high-performance DuckDB tables.
""")

# ACTION BUTTON
def trigger_ingestion():
    url = f"https://api.github.com/repos/{GITHUB_REPO}/dispatches"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    # This matches the 'repository_dispatch' trigger in pipeline.yml
    data = {"event_type": "run-ingestion"}
    
    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 204:
            st.success("🚀 GitHub Action triggered! Data will update in a few minutes.")
            st.info("You can monitor the progress in the 'Actions' tab of your GitHub repo.")
        else:
            st.error(f"Failed to trigger: {response.status_code} - {response.text}")
    except Exception as e:
        st.error(f"Connection error: {e}")

if st.button("🔄 Start A Fresh Batch of Ingestion from Deutsche Bahn API"):
    if not GITHUB_TOKEN:
        st.error("Missing GITHUB_TOKEN. Please add it as a Secret in Hugging Face.")
    else:
        trigger_ingestion()

st.divider()

# --- 0. LANDING ZONE (RAW XML) ---
st.header("🧱 The Landing Zone: Raw XML API Response")
with st.expander("Why show this?"):
    st.markdown("""
    Most people only see the finished chart. As an engineer, I start here. 
    The **Deutsche Bahn Timetables API** returns deeply nested XML. 
    My Rust scraper's first job is to parse this 'mess' into a structured format.
    """)

# A realistic snippet of what your scraper is actually reading:
raw_xml_sample = """
<timetable station="Berlin Hbf" eva="8011160">

<s id="8006012118938468400-2602101730-4" eva="8011160">
    <ar ct="2602101743" l="RE1"/>
    <dp ct="2602101745" l="RE1">
        <m id="r46069947" t="f" c="0" ts="2602101722" ts-tts="26-02-10 17:22:02.809"/>
    </dp>
</s>


<s id="9124848361923692803-2602101217-2" eva="8011160">
    <m id="r2415041" t="h" from="2505120800" to="2602162359" cat="Information" ts="2505112304" ts-tts="26-02-09 23:16:04.472" pr="3"/>
    <m id="r2571224" t="h" from="2602101300" to="2602101500" cat="Störung" ts="2602101348" ts-tts="26-02-10 13:48:35.860" pr="2"/>
    <m id="r2557907" t="h" from="2601190000" to="2602192359" cat="Information" ts="2601182306" ts-tts="26-02-09 23:16:04.721" pr="2"/>
    <ar ct="2602101225">
        <m id="r45976500" t="f" c="0" ts="2602100114" ts-tts="26-02-10 01:14:46.165"/>
    </ar>
    <dp ct="2602101231">
        <m id="r45976500" t="f" c="0" ts="2602100114" ts-tts="26-02-10 01:14:46.165"/>
        <m id="r46029727" t="f" c="0" ts="2602101222" ts-tts="26-02-10 12:22:07.313"/>
    </dp>
</s>
</timetable>
"""



with st.expander("Peek into an example of the raw XML (Direct from DB API)"):
        st.code(raw_xml_sample, language="xml")

st.caption("Above: A simplified example of the raw XML response from the /plan and /fchg endpoints.")


st.divider()

# --- 1. BRONZE LAYER (RAW DATA) ---
st.header("🥉 Bronze Layer: Raw Ingestion")
bronze_files = glob.glob(BRONZE_PATH)

if bronze_files:
    latest_file = max(bronze_files, key=os.path.getctime)
    st.info(f"Latest Raw Parquet: `{os.path.basename(latest_file)}`")
    
    # Read the latest parquet directly using DuckDB without needing the dbt file
    raw_sample = duckdb.query(f"SELECT * FROM read_parquet('{latest_file}') LIMIT 5").df()
    
    with st.expander("Peek into the Raw Parquet (Direct from DB API)"):
        st.write("This is exactly what the Rust scraper produced:")
        st.dataframe(raw_sample)
else:
    st.warning("No Bronze data found yet. Run an ingestion session.")

st.divider()

# --- 2. SILVER & GOLD LAYERS (DBT MODELS) ---
if os.path.exists(DB_PATH):
    try:
        con = get_connection()
        
        # Determine schema names
        schemas = con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
        schemas = [s[0] for s in schemas]
        
        silver_schema = "main_main" if "main_main" in schemas else "main"
        gold_schema = "main_gold" if "main_gold" in schemas else "gold"

        # --- SILVER SECTION ---
        st.header("🥈 Silver Layer: Cleaned Data")
        with st.expander("View Cleaned Departures (Timezones & Types handled)"):
            silver_df = con.execute(f"SELECT * FROM {silver_schema}.silver_departures LIMIT 10").df()
            st.dataframe(silver_df)

        st.divider()

        # --- GOLD SECTION ---
        st.header("🥇 Gold Layer: Business Analytics")

        with st.expander("🔍 What are we looking at?"):
            st.markdown("""
            **The Business Intelligence Layer**
            This table represents the final "Gold" stage of the Medallion Architecture. It transforms thousands of raw data points into actionable insights:
            
            * **The Grain:** Data is grouped by **Service Type**, **Hour**, and **Day**, allowing us to spot recurring patterns rather than isolated incidents.
            * **KPIs:** We calculate the **Punctuality Rate** (reliability) and **Average Delay** (severity) to measure the actual "commuter pain".
            * **Systemic Issues:** The **Disruption Count** scans for "Störung" notices to highlight technical or infrastructure failures that basic delay metrics might miss.
            * **Performance:** Pre-aggregated via **dbt** for sub-second dashboard loading.
            """)
        
        df = con.execute(f"SELECT * FROM {gold_schema}.agg_punctuality").df()

        # Sidebar Metrics
        st.sidebar.header("Filter Analytics")
        service_options = df['service_type'].unique()
        selected_service = st.sidebar.multiselect("Service Type", service_options, default=service_options)
        filtered_df = df[df['service_type'].isin(selected_service)]

        if not filtered_df.empty:
            # Top Level Stats
            col1, col2, col3 = st.columns(3)
            col1.metric("Overall Punctuality", f"{filtered_df['punctuality_rate'].mean():.1f}%")
            col2.metric("Avg Delay", f"{filtered_df['avg_delay_minutes'].mean():.1f} min")
            col3.metric("Total Disruptions", int(filtered_df['total_disruptions'].sum()))

            # Chart
            st.subheader("Punctuality Trends")
            fig = px.line(filtered_df, x="scheduled_hour", y="punctuality_rate", color="service_type", markers=True)
            st.plotly_chart(fig, use_container_width=True)
            
            st.write("### The Aggregated Gold Table")
            st.dataframe(filtered_df.sort_values('avg_delay_minutes', ascending=False))
            
    except Exception as e:
        st.error(f"Error loading refined data: {e}")
else:
    st.info("👋 Database is being built. Results will appear here shortly.")

# # DATA DISPLAY LOGIC
# if os.path.exists(DB_PATH):
#     try:
#         # Use the CACHED connection only
#         con = get_connection()
        
#         # Check which schema actually exists: 'main_gold' or 'gold'
#         schema_check = con.execute(
#             "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('gold', 'main_gold')"
#         ).fetchall()
        
#         # Pick the first one found, or default to main_gold
#         schema = schema_check[0][0] if schema_check else "main_gold"
        
#         # Fetch the data using the cached connection
#         df = con.execute(f"SELECT * FROM {schema}.agg_punctuality").df()

#         # con.close()

#         # Sidebar Metrics
#         st.sidebar.header("Filter")
#         service_options = df['service_type'].unique()
#         selected_service = st.sidebar.multiselect("Service Type", service_options, default=service_options)

#         filtered_df = df[df['service_type'].isin(selected_service)]

#         if not filtered_df.empty:
#             # Top Level Stats
#             col1, col2, col3 = st.columns(3)
#             col1.metric("Overall Punctuality", f"{filtered_df['punctuality_rate'].mean():.1f}%")
#             col2.metric("Avg Delay", f"{filtered_df['avg_delay_minutes'].mean():.1f} min")
#             col3.metric("Total Disruptions", int(filtered_df['total_disruptions'].sum()))

#             # Chart: Punctuality by Hour
#             st.subheader("Punctuality by Hour of Day")
#             fig = px.line(filtered_df, x="scheduled_hour", y="punctuality_rate", color="service_type", markers=True)
#             st.plotly_chart(fig, use_container_width=True)

#             st.write("### Raw Analytics View")
#             st.dataframe(filtered_df.sort_values('avg_delay_minutes', ascending=False))
#         else:
#             st.warning("No data matches the selected filters.")

#     except Exception as e:
#         st.error(f"Error loading database: {e}")
#         st.info("The database might be updating or empty. Try triggering ingestion.")
# else:
#     # Friendly message for the very first time the app is launched
#     st.info("👋 No data found yet. Click the **'Start New Ingestion Session'** button to populate the dashboard!")