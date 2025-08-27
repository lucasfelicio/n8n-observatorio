import streamlit as st

def reset_chat():
    """
    Reset the Streamlit chat session state.
    """
    st.session_state.chat_history = []
    st.session_state.example = False 