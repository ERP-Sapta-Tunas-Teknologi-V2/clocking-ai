import mysql.connector

def connect_db(host, user, password, db):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=db
    )

def map_status(old_status):
    status_map = {
        'p': 'progress',
        'f': 'finished',
        'i': 'initial',
        'c': 'cancelled'
    }
    return status_map.get(old_status.lower(), 'unknown')

def migrate_projects():
    config = {
        "host": "localhost",
        "user": "root",
        "password": ""  # Leave empty if no password
    }

    source_db = connect_db(**config, db="system-smartpro")
    target_db = connect_db(**config, db="clocking_reports")

    source_cursor = source_db.cursor(dictionary=True)
    target_cursor = target_db.cursor()

    # Fetch data from source table
    source_cursor.execute("""
        SELECT 
            pr_project_code AS project_code,
            pr_project_name AS project_name,
            pr_customer_name AS customer_name,
            pr_pic_project AS project_manager_id,
            pr_created_by AS created_by,
            pr_created_date AS created_at,
            pr_last_update AS last_update,
            pr_status AS status
        FROM ss_project_management
    """)
    
    records = source_cursor.fetchall()

    insert_query = """
        INSERT INTO projects (
            project_code, project_name, customer_name, 
            project_manager_id, created_by, created_at, 
            last_update, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    for row in records:
        target_cursor.execute(insert_query, (
            row["project_code"],
            row["project_name"],
            row["customer_name"],
            row["project_manager_id"],
            row["created_by"],
            row["created_at"],
            row["last_update"],
            map_status(row["status"])
        ))

    target_db.commit()
    print(f"{len(records)} project records migrated successfully.")

    # Cleanup
    source_cursor.close()
    target_cursor.close()
    source_db.close()
    target_db.close()

if __name__ == "__main__":
    migrate_projects()
