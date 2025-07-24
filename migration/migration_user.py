import mysql.connector

def connect_db(host, user, password, db):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=db
    )

def migrate_users():
    # Database connection config
    config = {
        "host": "localhost",
        "user": "root",
        "password": ""
    }

    source_db = connect_db(**config, db="system-smartpro")
    target_db = connect_db(**config, db="clocking_reports")

    source_cursor = source_db.cursor(dictionary=True)
    target_cursor = target_db.cursor()

    # Fetch relevant fields from source
    source_cursor.execute("""
        SELECT 
            id AS user_id,
            id_key AS user_key,
            name AS full_name,
            email,
            jabatan AS position,
            created_at,
            updated_at
        FROM ss_user
    """)
    
    users = source_cursor.fetchall()

    insert_query = """
        INSERT INTO users (
            user_id, user_key, full_name, email, position, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    for user in users:
        target_cursor.execute(insert_query, (
            user['user_id'],
            user['user_key'],
            user['full_name'],
            user['email'],
            user['position'],
            user['created_at'],
            user['updated_at'],
        ))

    target_db.commit()
    print(f"{len(users)} users migrated successfully.")

    # Cleanup
    source_cursor.close()
    target_cursor.close()
    source_db.close()
    target_db.close()

if __name__ == "__main__":
    migrate_users()
