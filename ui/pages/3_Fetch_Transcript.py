import pandas as pd
import streamlit as st
from utils import backend_api

st.title('Fetch Transcripts')

@st.cache_data
def get_companies_list():
    # Get list of companies from the database, and associated years
    metadata = backend_api.fetch_metadata()
    df = pd.DataFrame(metadata.get("company_names_years"), columns = ["CN", "YEAR"])
    return df

def run_dag():
    response = backend_api.trigger_fetch_transcript(company_name, year, quarter, word_limit, api_key, openai_api_key)
    return response.get("message")

def fetch_transcript():
    response = backend_api.fetch_transcript(company_name, year, quarter, api_key)
    return response

# data_load_state = st.text('Loading ...')
df = get_companies_list()

# select the unique companies for user to filter
company_name = st.selectbox(label='Company', options=list(
    df['CN'].sort_values().unique()))

year = st.selectbox(label='Year', options=list(
    df[df['CN']==company_name]['YEAR'].sort_values().unique()))

quarter = st.selectbox(label='Quarter', options=[1,2,3,4])

word_limit = st.number_input('Word limit per quarter', min_value=50, max_value=500)
api_key = st.text_input('API Key')

# show raw data if user wants
# if st.checkbox('Show raw data'):
#     st.subheader('Raw data')
#     st.write(df[df['CN'].isin(filter_companies) & df['YEAR'].isin(filter_years)].sort_values(by='CN'))

if st.button("Fetch Transcript"):
    response = fetch_transcript()
    st.write(response)

# data_load_state.text("Done!")