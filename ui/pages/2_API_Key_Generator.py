import streamlit as st
from utils import backend_api
API_KEY = None

def generate_api_key():
    # Call Fast API with username and password field return API key
    response = backend_api.generate_api_key(username, password)
    if response.json().get("API_ACCESS_TOKEN"):
        return f"API KEY: {response.json().get('API_ACCESS_TOKEN')}"
    else:
        return f"Unable to generate API key. {response.text}"
    

st.title('API Key Generator')

st.subheader("Get API key")

username = st.text_input('Username')
password = st.text_input('Password',type='password')

if st.button("Generate"):
    message = generate_api_key()
    st.write(message)