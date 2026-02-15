import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import requests
import os

st.set_page_config(page_title="Pünktlich - Berlin Hbf", layout="wide")

# Using a relative path for the database file in the repo
DB_PATH = "data/dbt.duckdb"

# Updating these to match your specific GitHub details
GITHUB_REPO = "nde97/punktlich-data-platform"
# This token is set in Hugging Face Space Secrets
GITHUB_TOKEN = os.getenv("GH_TOKEN") 

# UI HEADER
st.title("🚆 Pünktlich: Berlin Hbf Real-Time")
st.markdown("Analyzing train reliability using **Rust**, **dbt**, and **DuckDB**.")

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

if st.button("🔄 Start New Ingestion Session"):
    if not GITHUB_TOKEN:
        st.error("Missing GITHUB_TOKEN. Please add it as a Secret in Hugging Face.")
    else:
        trigger_ingestion()

st.divider()

# DATA DISPLAY LOGIC
if os.path.exists(DB_PATH):
    try:
        # Using read_only to prevent locking issues in a web environment
        con = duckdb.connect(DB_PATH, read_only=True)
        
        # Pulling from the schema defined in our Gold SQL
        df = con.execute("SELECT * FROM gold.agg_punctuality").df()
        con.close()

        # Sidebar Metrics
        st.sidebar.header("Filter")
        service_options = df['service_type'].unique()
        selected_service = st.sidebar.multiselect("Service Type", service_options, default=service_options)

        filtered_df = df[df['service_type'].isin(selected_service)]

        if not filtered_df.empty:
            # Top Level Stats
            col1, col2, col3 = st.columns(3)
            col1.metric("Overall Punctuality", f"{filtered_df['punctuality_rate'].mean():.1f}%")
            col2.metric("Avg Delay", f"{filtered_df['avg_delay_minutes'].mean():.1f} min")
            col3.metric("Total Disruptions", int(filtered_df['total_disruptions'].sum()))

            # Chart: Punctuality by Hour
            st.subheader("Punctuality by Hour of Day")
            fig = px.line(filtered_df, x="scheduled_hour", y="punctuality_rate", color="service_type", markers=True)
            st.plotly_chart(fig, use_container_width=True)

            st.write("### Raw Analytics View")
            st.dataframe(filtered_df.sort_values('avg_delay_minutes', ascending=False))
        else:
            st.warning("No data matches the selected filters.")

    except Exception as e:
        st.error(f"Error loading database: {e}")
        st.info("The database might be updating or empty. Try triggering ingestion.")
else:
    # Friendly message for the very first time the app is launched
    st.info("👋 No data found yet. Click the **'Start New Ingestion Session'** button to populate the dashboard!")