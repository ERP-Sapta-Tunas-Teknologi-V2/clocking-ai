from llm_api import get_sql_from_llm, get_response_from_llm
from database import execute_sql_query
from utils import is_select_query

def main():
    user_query = input("Ask your question: ")

    while True:
        sql_query = get_sql_from_llm(user_query)
        
        if sql_query:
            print(f"Generated SQL: {sql_query}")
            
            if is_select_query(sql_query):
                print("Valid SELECT query. Executing...")
                result = execute_sql_query(sql_query)

                if result:
                    print(f"Database Result: {result}")
                    final_response = get_response_from_llm(sql_query, result)
                    
                    if final_response:
                        print(f"LLM Response: {final_response}")
                    else:
                        print("Error in processing LLM response.")
                else:
                    print("Error executing SQL query.")
                break
            else:
                print("Invalid query: Not a SELECT query. Asking LLM to regenerate...")
        else:
            print("Error: SQL query generation failed.")

if __name__ == "__main__":
    main()