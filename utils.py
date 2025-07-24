import re

def get_system_prompt():
    try:
        with open("guardrail/prompt.txt", "r", encoding="utf-8") as file:
            prompt = file.read().strip()
        return prompt
    except FileNotFoundError:
        print("Error: prompt.txt file not found.")
        return None
    except UnicodeDecodeError as e:
        print(f"Error decoding prompt.txt: {e}")
        return None

def extract_query_from_markdown(query):
    """Extract the SQL query from markdown (backticks) using regex."""
    match = re.search(r'```(.*?)```', query, re.DOTALL)
    if match:
        query_inside_backticks = match.group(1).strip()
        query_without_sql = re.sub(r'^\s*sql', '', query_inside_backticks).strip()
        return query_without_sql
    else:
        return None

import re

def is_select_query(query):
    """
    Returns True if the query is a safe SELECT or WITH query (read-only).
    Returns False if it contains any potentially dangerous SQL keywords.
    """
    query = query.strip().lower()

    # Allow only SELECT or WITH queries at the beginning
    if not (query.startswith("select") or query.startswith("with")):
        return False

    # Disallowed keywords (to prevent modifications)
    unsafe_keywords = [
        "insert", "update", "delete", "drop", "alter", "create", "truncate", 
        "merge", "grant", "revoke", "replace", "call", "exec", "execute", "set"
    ]

    # Pattern to match any unsafe keyword as a whole word
    pattern = r'\b(?:' + '|'.join(unsafe_keywords) + r')\b'

    return not re.search(pattern, query)
