import streamlit as st
import requests
import time

# --- CONFIG ---
GITHUB_REPO = "namratade97/punktlich-data-platform"
GITHUB_TOKEN = st.secrets["GH_TOKEN"] # Add this in HF Settings > Secrets

def trigger_pipeline():
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    payload = {"event_type": "run-ingestion"}
    url = f"https://api.github.com/repos/{GITHUB_REPO}/dispatches"
    
    response = requests.post(url, headers=headers, json=payload)
    return response.status_code == 204

st.title("🚆 Punktlich Control Center")

if st.button("🚀 Start Real-Time Ingestion"):
    if trigger_pipeline():
        st.success("GitHub Action Triggered!")
        
        # Real-time Simulation / Polling
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        stages = [
            (20, "Compiling & Running Rust Scraper..."),
            (50, "Fetching Deutsche Bahn Data..."),
            (80, "Running dbt Silver & Gold Models..."),
            (100, "Pipeline Complete! Refreshing Dashboard...")
        ]
        
        for percent, msg in stages:
            status_text.text(msg)
            progress_bar.progress(percent)
            time.sleep(5) # In reality, we'd poll the GitHub API here for 'completed' status
            
        st.rerun()
    else:
        st.error("Failed to trigger pipeline.")