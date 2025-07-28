import streamlit as st
import requests
import json
import re
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
import os

# Streamlit UI
st.title("Ollama LLM Query with Vector Memory")
st.write("Select a model and enter a query to interact with the Ollama LLM.")

# Sidebar for navigation/history
st.sidebar.title("Conversation History")
if 'history' not in st.session_state:
    st.session_state.history = []

# Dropdown for selecting Ollama model
models = ["qwen3:0.6b"]
selected_model = st.selectbox("Select Model", models, index=0)

# Input field
query = st.text_area("Query", placeholder="E.g., What is the capital of France?")
submit_button = st.button("Submit")

# Ollama API endpoint
OLLAMA_URL = "http://localhost:11434/api/generate"

# File paths for persistent storage
DB_INDEX_FILE = "vector_index.faiss"
DB_DATA_FILE = "vector_data.json"

# Initialize vector database with persistent storage
try:
    if 'vector_index' not in st.session_state or 'vector_data' not in st.session_state:
        if os.path.exists(DB_INDEX_FILE) and os.path.exists(DB_DATA_FILE):
            st.session_state.vector_index = faiss.read_index(DB_INDEX_FILE)
            with open(DB_DATA_FILE, 'r') as f:
                st.session_state.vector_data = json.load(f)
        else:
            st.session_state.vector_index = faiss.IndexFlatL2(384)  # 384 for 'all-MiniLM-L6-v2'
            st.session_state.vector_data = []
        st.session_state.encoder = SentenceTransformer('all-MiniLM-L6-v2')
except Exception as e:
    st.session_state.vector_index = None
    st.session_state.vector_data = []
    st.warning(f"Vector database initialization failed: {e}. Using in-session context only.")

def save_vector_db():
    """Save vector database to files."""
    if st.session_state.vector_index is not None:
        faiss.write_index(st.session_state.vector_index, DB_INDEX_FILE)
        with open(DB_DATA_FILE, 'w') as f:
            json.dump(st.session_state.vector_data, f)

def add_to_vector_db(query, response):
    """Add question-answer pair to vector database if available."""
    if st.session_state.vector_index is not None:
        text = f"Q: {query}\nA: {response}"
        embedding = st.session_state.encoder.encode([text])[0]
        st.session_state.vector_data.append(text)
        st.session_state.vector_index.add(np.array([embedding]))
        save_vector_db()

def retrieve_context(query, k=3):
    """Retrieve top k relevant contexts from vector database."""
    if st.session_state.vector_index is None or len(st.session_state.vector_data) == 0:
        return ""
    query_embedding = st.session_state.encoder.encode([query])[0]
    query_embedding = np.array([query_embedding])
    distances, indices = st.session_state.vector_index.search(query_embedding, k)
    return "\n".join(st.session_state.vector_data[i] for i in indices[0] if i < len(st.session_state.vector_data))

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
    # Retrieve context from vector database or session history
    vector_context = retrieve_context(query)
    session_context = "\n".join([f"Q: {entry['query']}\nA: {entry['response']}" for entry in st.session_state.history])
    prompt_with_context = f"{vector_context}\n{session_context}\nQ: {query}\nA:" if vector_context or session_context else f"Q: {query}\nA:"

    # Placeholder for streaming output
    response_container = st.empty()
    think_container = st.empty()
    
    try:
        full_think = ""
        full_response = ""
        for data in stream_response(selected_model, prompt_with_context):
            full_think = data["think"]
            full_response = data["response"]
            if full_think:
                with think_container.expander("Thinking Process", expanded=False):
                    st.markdown(full_think)
            if full_response:
                response_container.markdown(f"**Response:** {full_response}")
        
        # Add to history and vector database after response is complete
        if full_response:
            st.session_state.history.append({"query": query, "response": full_response})
            add_to_vector_db(query, full_response)
        
        # Display history in sidebar
        for i, entry in enumerate(st.session_state.history):
            with st.sidebar.expander(f"Q{i+1}: {entry['query'][:30]}..."):
                st.write(f"**Question:** {entry['query']}")
                st.write(f"**Answer:** {entry['response']}")
        
        if not full_response and not full_think:
            st.error("No response received from Ollama.")
    except Exception as e:
        st.error(f"Error: {e}")