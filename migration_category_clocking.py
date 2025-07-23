import mysql.connector

def connect_db(host, user, password, db):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=db
    )

def migrate_category_docking():
    config = {
        "host": "localhost",
        "user": "root",
        "password": ""  # Adjust with your credentials if needed
    }

    source_db = connect_db(**config, db="system-smartpro")
    target_db = connect_db(**config, db="clocking_reports")

    source_cursor = source_db.cursor(dictionary=True)
    target_cursor = target_db.cursor()

    source_cursor.execute("SELECT cc_id, cc_definition, cc_productive, cc_billable, cc_used, cc_direct FROM ss_category_clocking")
    rows = source_cursor.fetchall()

    insert_query = """
        INSERT INTO category_clocking (
            category_id, category_description, is_productive, is_billable, is_used, is_direct
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """

    inserted_count = 0

    for row in rows:
        target_cursor.execute(insert_query, (
            row["cc_id"],
            row["cc_definition"],
            row["cc_productive"] if row["cc_productive"] is not None else 0,  # Default to 0 if NULL
            row["cc_billable"] if row["cc_billable"] is not None else 0,      # Default to 0 if NULL
            row["cc_used"] if row["cc_used"] is not None else 0,              # Default to 0 if NULL
            row["cc_direct"] if row["cc_direct"] is not None else 0           # Default to 0 if NULL
        ))
        inserted_count += 1

    target_db.commit()
    print(f"{inserted_count} category records migrated successfully.")

    source_cursor.close()
    target_cursor.close()
    source_db.close()
    target_db.close()

if __name__ == "__main__":
    migrate_category_docking()
