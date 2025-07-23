import mysql.connector

def get_database_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",  # Assuming 'root' user without password
        database="clocking_reports"
    )

def execute_sql_query(sql_query):
    db_connection = get_database_connection()
    cursor = db_connection.cursor()

    try:
        cursor.execute(sql_query)
        result = cursor.fetchall()
        return result
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None
    finally:
        cursor.close()
        db_connection.close()