import streamlit as st
import requests
import mysql.connector
import json
import re

# Streamlit UI
st.title("Ollama LLM Query")
st.write("Select a model and enter a query to interact with the Ollama LLM.")

# Sidebar for navigation/history
st.sidebar.title("Conversation History")
if 'history' not in st.session_state:
    st.session_state.history = []

# Dropdown for selecting Ollama model
models = ["qwen3:0.6b"]  # Fixed to your specified model
selected_model = st.selectbox("Select Model", models, index=0)

# Input field
query = st.text_area("Query", placeholder="E.g., What is the capital of France?")
submit_button = st.button("Submit")

# Ollama API endpoint
OLLAMA_URL = "http://localhost:11434/api/generate"

def stream_response(model, prompt):
    """Stream and parse response from Ollama API, separating <think> content."""
    response = requests.post(
        OLLAMA_URL,
        json={"model": model, "prompt": prompt, "stream": True},
        stream=True
    )
    full_think = ""
    full_response = ""
    current_section = "response"
    for line in response.iter_lines():
        if line:
            try:
                data = json.loads(line.decode('utf-8'))
                if 'response' in data:
                    chunk = data['response']
                    if "<think>" in chunk:
                        current_section = "think"
                        full_think += re.sub(r'^<think>', '', chunk)
                    elif "</think>" in chunk:
                        current_section = "response"
                        full_think += re.sub(r'</think>$', '', chunk)
                    elif current_section == "think":
                        full_think += chunk
                    else:
                        full_response += chunk
                    yield {"think": full_think.strip(), "response": full_response.strip()}
            except json.JSONDecodeError:
                continue

if submit_button and query:
    # Store the new query and response
    response_container = st.empty()
    think_container = st.empty()
    
    try:
        full_think = ""
        full_response = ""
        for data in stream_response(selected_model, query):
            full_think = data["think"]
            full_response = data["response"]
            if full_think:
                with think_container.expander("Thinking Process", expanded=False):
                    st.markdown(full_think)
            if full_response:
                response_container.markdown(f"**Response:** {full_response}")
        
        # Add to history after response is complete
        if full_response:
            st.session_state.history.append({"query": query, "response": full_response})
        
        # Display history in sidebar
        for i, entry in enumerate(st.session_state.history):
            with st.sidebar.expander(f"Q{i+1}: {entry['query'][:30]}..."):
                st.write(f"**Question:** {entry['query']}")
                st.write(f"**Answer:** {entry['response']}")
        
        if not full_response and not full_think:
            st.error("No response received from Ollama.")
    except Exception as e:
        st.error(f"Error: {e}")