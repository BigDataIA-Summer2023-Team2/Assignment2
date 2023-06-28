import streamlit as st
from utils import backend_api


def register_user():
    response = backend_api.register_user(new_user, new_passwd, new_cnf_passwd)
    if response.json().get("username"):
        st.write("User registered successfully!")
    else:
        st.write(response.json())

st.title('User Registration')

st.subheader("Create an Account")

new_user = st.text_input('Username')
new_passwd = st.text_input('Password', type='password')
new_cnf_passwd = st.text_input('Confirm Password', type='password')
st.button("Sign Up", on_click=register_user)
