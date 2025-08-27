import streamlit as st
import requests
import netifaces

from functions import reset_chat

n8n_url = "http://n8n:5678/webhook/ai-agent"

gateways = netifaces.gateways()
gateway_default = gateways['default'][netifaces.AF_INET][0]

if st.sidebar.button("Reset chat"):
    reset_chat()

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

if not st.session_state.chat_history:
    st.session_state.chat_history.append(("assistant", "Olá! Como posso te ajudar hoje?"))

for role, message in st.session_state.chat_history:
    st.chat_message(role).write(message)

user_message = st.chat_input("Faça sua pergunta...")

if user_message:
    st.chat_message("user").write(user_message)
    st.session_state.chat_history.append(("user", user_message))

    try:
        response = requests.post(n8n_url, json={"query": user_message, "ip": gateway_default})

        if response.status_code == 200:
            data = response.json()
            assistant = data["message"]

            st.chat_message("assistant").write(assistant)
            st.session_state.chat_history.append(("assistant", assistant))
        else:
            error_msg = "Desculpe, ocorreu um erro ao processar sua solicitação."
            st.chat_message("assistant").write(error_msg)
            st.session_state.chat_history.append(("assistant", error_msg))
            response = None


    except Exception as e:
        st.error("Desculpe, ocorreu um erro ao conectar ao servidor.")
        response = None