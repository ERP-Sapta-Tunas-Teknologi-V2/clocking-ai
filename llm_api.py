import requests
import json
from utils import get_system_prompt, extract_query_from_markdown, is_select_query

OLLAMA_API_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "qwen3:4b"

def get_sql_from_llm(user_query):
    headers = {"Content-Type": "application/json"}
    system_prompt = get_system_prompt()
    full_prompt = f"{system_prompt}\n\nUser Query: {user_query}" if system_prompt else user_query

    data = {"model": MODEL_NAME, "prompt": full_prompt}

    try:
        response = requests.post(OLLAMA_API_URL, json=data, headers=headers, stream=True)
        
        if response.status_code == 200:
            # Initialize a variable to hold the full response
            full_response = ""

            # Read the response in chunks
            for chunk in response.iter_lines():
                if chunk:
                    try:
                        # Parse each chunk as JSON
                        chunk_data = json.loads(chunk)
                        # Append the response part to the full_response string
                        full_response += chunk_data.get("response", "")
                        
                        # Check if the response is complete
                        if chunk_data.get("done", False):
                            break
                    except json.JSONDecodeError as e:
                        print(f"Error: Failed to decode JSON response chunk: {e}")
                        print(f"Raw chunk: {chunk}")
                        return None

            print(f"Full Response: {full_response}")

            if full_response:
                # Sanitize the SQL query to extract the query from markdown (backticks)
                sanitized_query = extract_query_from_markdown(full_response)
                
                if sanitized_query:
                    # Trim spaces and newlines for better validation
                    sanitized_query = sanitized_query.strip()
                    print(f"Sanitized SQL Query: {sanitized_query}")  # Print the sanitized query
                    
                    if is_select_query(sanitized_query):
                        return sanitized_query
                    else:
                        print("Invalid query: Not a SELECT query. Asking LLM to regenerate...")
                        # Regenerate the query with more precise instructions
                        return regenerate_query(user_query, "Ensure that the query is a valid SELECT query.")
                else:
                    print("Error: No SQL query found inside backticks.")
                    return None
            else:
                print("Error: No SQL query found in the response.")
                return None
        else:
            print(f"Error: Received {response.status_code} from Ollama API. Response text: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to make a request to Ollama API: {e}")
        return None

def regenerate_query(user_query, instruction="Generate a valid SQL query"):
    """Regenerate the query with more specific instructions."""
    headers = {"Content-Type": "application/json"}
    system_prompt = get_system_prompt()
    full_prompt = f"{system_prompt}\n\n{instruction}\nUser Query: {user_query}" if system_prompt else user_query

    data = {"model": MODEL_NAME, "prompt": full_prompt}

    try:
        response = requests.post(OLLAMA_API_URL, json=data, headers=headers, stream=True)
        
        if response.status_code == 200:
            # Initialize a variable to hold the full response
            full_response = ""

            # Read the response in chunks
            for chunk in response.iter_lines():
                if chunk:
                    try:
                        # Parse each chunk as JSON
                        chunk_data = json.loads(chunk)
                        # Append the response part to the full_response string
                        full_response += chunk_data.get("response", "")
                        
                        # Check if the response is complete
                        if chunk_data.get("done", False):
                            break
                    except json.JSONDecodeError as e:
                        print(f"Error: Failed to decode JSON response chunk: {e}")
                        print(f"Raw chunk: {chunk}")
                        return None

            print(f"Full Response (Regenerated): {full_response}")

            if full_response:
                # Sanitize the SQL query to extract the query from markdown (backticks)
                sanitized_query = extract_query_from_markdown(full_response)
                
                if sanitized_query:
                    # Trim spaces and newlines for better validation
                    sanitized_query = sanitized_query.strip()
                    print(f"Sanitized SQL Query (Regenerated): {sanitized_query}")  # Print the regenerated query
                    
                    if is_select_query(sanitized_query):
                        return sanitized_query
                    else:
                        print("Regenerated query is still not a valid SELECT query.")
                        return None
                else:
                    print("Error: No SQL query found inside backticks.")
                    return None
            else:
                print("Error: No SQL query found in the response.")
                return None
        else:
            print(f"Error: Received {response.status_code} from Ollama API. Response text: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to make a request to Ollama API: {e}")
        return None

def get_response_from_llm(sql_query, result):
    """Get a response from the LLM based on SQL query result."""
    headers = {"Content-Type": "application/json"}
    result_str = "\n".join([str(row) for row in result])
    data = {
        "model": MODEL_NAME,
        "prompt": f"Based on the SQL result: {result_str}, provide a summary."
    }

    try:
        response = requests.post(OLLAMA_API_URL, json=data, headers=headers)
        if response.status_code == 200:
            response_data = response.json()
            return response_data.get('answer')
        else:
            print(f"Error: Received {response.status_code} from Ollama API. Response text: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to make a request to Ollama API: {e}")
        return None
