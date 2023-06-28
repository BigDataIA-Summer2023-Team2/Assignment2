import os
import json
import requests
import streamlit as st
from utils import backend_api

st.set_page_config(
    page_title="Home Page",
    page_icon="👋",
)

st.title("Transcript Insight")

st.markdown(
    """
    Application to aid research of investment analysts by searching through 
    earnings call transcripts, to periodically upload text datasets, and providing features to filter by company and year.
"""
)

backend_api.trigger_fetch_metadata()

# Run the app
# streamlit run main.py