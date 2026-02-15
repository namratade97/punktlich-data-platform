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

# Updating these to match my specific GitHub details
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

with st.sidebar:
    st.header("🛠️ Tech Stack")
    st.write("🦀 **Scraper:** Rust (reqwest + quick-xml)")
    st.write("📦 **Storage:** Apache Parquet (Bronze)")
    st.write("🦆 **Database:** DuckDB")
    st.write("🏗️ **Transformation:** dbt (Silver/Gold)")
    st.write("🚀 **CI/CD:** GitHub Actions")

    st.header("🏗️ The Architecture Story")
    
    with st.expander("🟣 Infrastructure as Code", expanded=False):
        st.markdown("""
        The foundation of this Space was provisioned using **Terraform**. 
        **[View Terraform Configuration on GitHub.](https://github.com/namratade97/punktlich-data-platform/tree/main/terraform)**
        
        By treating the UI as code, we ensure the environment is reproducible 
        and version-controlled, moving away from "manual clicks" in the console.
        """)

    with st.expander("🔗 Orchestration: GitHub Actions", expanded=False):
        st.markdown("""
        To keep the project lightweight and cost-effective, I used **GitHub Actions** as our orchestrator. 
        
        Much like **Apache Airflow**, it handles:
        * **Scheduling:** (CRON jobs for data collection).
        * **Dependency Graphs:** (Rust Scraper → dbt Transformation → HF Sync).
        * **Logs & Alerts:** Tracking pipeline health without the overhead of a dedicated Airflow server.
        """)

    with st.expander("🚧 Engineering Constraints", expanded=False):
        st.markdown("""
        **Resource-First Design** Operating on the **Free Tier** required a few specific strategic choices:
        * **DuckDB:** Chosen over PostgreSQL to avoid database hosting costs and latency.
        * **Parquet:** Used for "Bronze" storage to minimize disk space and speed up dbt runs.
        * **Transient Compute:** Using GitHub Runners to do the "heavy lifting" so the Streamlit UI remains responsive.
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
            st.info(f"""
            **What happens now?**
            1. **Scraping:** A [GitHub Action](https://github.com/{GITHUB_REPO}/actions) starts a Rust runner.
            2. **Ingestion:** It fetches real-time XML from Deutsche Bahn and saves it as Bronze Parquet.
            3. **Transformation:** dbt cleans and aggregates the data into Silver and Gold tables.
            4. **Sync:** The updated DuckDB file is pushed back to this Hugging Face Space.
            
            ⏱️ **Estimated time:** ~10 minutes. The dashboard will update automatically on your next refresh once the run is complete.
            """)
        else:
            st.error(f"Failed to trigger: {response.status_code} - {response.text}")
    except Exception as e:
        st.error(f"Connection error: {e}")

if st.button("▶️ Start A Fresh Batch of Ingestion from Deutsche Bahn API"):
    if not GITHUB_TOKEN:
        st.error("Missing GITHUB_TOKEN. Please add it as a Secret in Hugging Face.")
    else:
        trigger_ingestion()

st.divider()

# LANDING ZONE (RAW XML)
st.header("🧱 The Landing Zone: Raw XML API Response")
with st.expander("Why show this?"):
    st.markdown("""
    Most people only see the finished chart. As an engineer, I start here. 
    The **Deutsche Bahn Timetables API** returns deeply nested XML. 
    My Rust scraper's first job is to parse this into a structured format.
    """)

# A sample snippet of what the scraper is actually reading:
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

# BRONZE LAYER (RAW DATA)
st.header("🥉 Bronze Layer: Raw Ingestion")
bronze_files = glob.glob(BRONZE_PATH)

if bronze_files:
    latest_file = max(bronze_files, key=os.path.getctime)
    st.info(f"Latest Raw Parquet: `{os.path.basename(latest_file)}`")
    
    # Reading the latest parquet directly using DuckDB without needing the dbt file
    raw_sample = duckdb.query(f"SELECT * FROM read_parquet('{latest_file}') LIMIT 10").df()

    with st.expander("A glimpse of the raw Parquet ingested from DB API"):
        st.write("This is exactly what the Rust scraper produced:")
        st.dataframe(raw_sample)
else:
    st.warning("No Bronze data found yet. Run an ingestion session.")

st.divider()

# SILVER & GOLD LAYERS (DBT MODELS)
if os.path.exists(DB_PATH):
    try:
        con = get_connection()

        # Determining schema names
        schemas = con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
        schemas = [s[0] for s in schemas]
        
        silver_schema = "main_main" if "main_main" in schemas else "main"
        gold_schema = "main_gold" if "main_gold" in schemas else "gold"

        # SILVER SECTION
        st.header("🥈 Silver Layer: Cleaned Data")
        with st.expander("View Cleaned Departures (Timezones & Types handled)"):
            silver_df = con.execute(f"SELECT * FROM {silver_schema}.silver_departures LIMIT 10").df()
            st.dataframe(silver_df)

        st.divider()

        bronze_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{BRONZE_PATH}')").fetchone()[0]
        silver_count = con.execute(f"SELECT COUNT(*) FROM {silver_schema}.silver_departures").df().iloc[0,0]

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Raw Records (Bronze)", f"{bronze_count:,}")
        c2.metric("Unique Records (Silver)", f"{silver_count:,}")
        
        # Calculate deduplication percentage
        if bronze_count > 0:
            dup_rate = ((bronze_count - silver_count) / bronze_count) * 100
            c3.metric("Deduplication Effect", f"-{dup_rate:.1f}%")

        with st.expander("Why do these numbers differ?"):
            st.markdown(f"""
            **The Pipeline at work:**
            * **Bronze** counts every single row across all `{len(bronze_files)}` Parquet files from past runs ingested. Since we ideally scrape every few minutes, many trains are captured multiple times.
            * **Silver** uses dbt to 'de-duplicate' the data. It identifies unique trains by their ID and Scheduled Time, keeping only the most recent status update.
            * This ensures our **Gold** analytics aren't biased by how many times we scraped a specific hour.
            """)
        
        st.divider()

        # GOLD SECTION
        st.header("🥇 Gold Layer: Business Analytics")
        
        # Fetch data first
        df = con.execute(f"SELECT * FROM {gold_schema}.agg_punctuality").df()

        with st.expander("🔍 What are we looking at?"):
            st.markdown("""
            **The Business Intelligence Layer**
            Each row represents a "Service Bucket" (e.g., *S-Bahn* at *9:00 AM*). 
            We calculate **Punctuality Rate** and **Avg Delay** to identify recurring 
            patterns in reliability across different times of the week.
            """)

        # MAIN SECTION FILTERS
        # Creating columns to make the filter look more integrated
        col_filter, col_spacer = st.columns([2, 1]) 

        # Updated CSS for DB Red Tags with visible 'X' buttons
        st.markdown("""
            <style>
            div[data-baseweb="select"] > div {
                background-color: white !important;
                border: 2px solid #f01414 !important;
                border-radius: 4px;
            }

            div[data-baseweb="select"] svg {
                fill: #f01414 !important;
            }

            span[data-baseweb="tag"] {
                background-color: #f01414 !important;
                color: white !important;
                border-radius: 2px;
                padding-right: 5px;
            }

            span[data-baseweb="tag"] svg {
                fill: white !important;
            }

            span[data-baseweb="tag"] role[button]:hover {
                background-color: rgba(255, 255, 255, 0.2) !important;
            }

            label[data-testid="stWidgetLabel"] p {
                color: #f01414 !important;
                font-weight: bold;
            }
            </style>
            """, unsafe_allow_html=True)

        with col_filter:
            service_options = sorted(df['service_type'].unique())
            selected_service = st.multiselect(
                "Filter by Service Type:", 
                options=service_options, 
                default=service_options
            )

        # Apply Filter
        filtered_df = df[df['service_type'].isin(selected_service)]

        if not filtered_df.empty:
            # Metrics (Main Section)
            m1, m2, m3 = st.columns(3)
            m1.metric("Overall Punctuality", f"{filtered_df['punctuality_rate'].mean():.1f}%")
            m2.metric("Avg Delay", f"{filtered_df['avg_delay_minutes'].mean():.1f} min")
            m3.metric("Total Disruptions", int(filtered_df['total_disruptions'].sum()))

            # The Table
            st.write("### The Aggregated Gold Table")
            st.dataframe(
                filtered_df.sort_values(['scheduled_hour', 'day_of_week']), 
                use_container_width=True,
                hide_index=True
            )

            # Chart: Punctuality by Hour
            st.subheader("Punctuality by Hour of Day")
            chart_df = filtered_df.sort_values('scheduled_hour')

            fig = px.bar(
                chart_df, 
                x="scheduled_hour", 
                y="punctuality_rate", 
                color="service_type", 
                barmode="group",
                labels={
                    "scheduled_hour": "Hour of Day", 
                    "punctuality_rate": "Punctuality (%)",
                    "service_type": "Service"
                },
                template="plotly_white"
            )

            fig.update_layout(
                xaxis=dict(
                    tickmode='linear',
                    tick0=0,
                    dtick=1
                ),
                yaxis=dict(range=[0, 105])
            )

            st.plotly_chart(fig, use_container_width=True)

        else:
            st.warning("No data matches the selected filters.")

        
            
    except Exception as e:
        st.error(f"Error loading refined data: {e}")
else:
    st.info("⏳ Database is being built. Results will appear here shortly.")

