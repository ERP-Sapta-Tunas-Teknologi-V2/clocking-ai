import mysql.connector
import json

def connect_db(host, user, password, db):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=db
    )

def get_user_id_from_key(cursor, user_key):
    # Explicitly reference the correct database and table
    cursor.execute("SELECT id FROM `system-smartpro`.ss_user WHERE id_key = %s", (user_key,))
    result = cursor.fetchone()
    return result[0] if result else None

def map_priority(priority):
    # Map source values to target ENUM values
    mapping = {
        'H': 'High',
        'M': 'Medium',
        'L': 'Low'
    }
    return mapping.get(priority, None)  # Return None if priority is not in the mapping

def migrate_daily_activity():
    config = {
        "host": "localhost",
        "user": "root",
        "password": ""  # Adjust with your credentials if needed
    }

    source_db = connect_db(**config, db="system-smartpro")  # Adjust source db name
    target_db = connect_db(**config, db="clocking_reports")  # Adjust target db name

    source_cursor = source_db.cursor(dictionary=True)
    target_cursor = target_db.cursor()
    user_lookup_cursor = target_db.cursor()  # For looking up user_id (cross-DB query is fully qualified)

    source_cursor.execute("SELECT * FROM ss_daily_activity")  # Adjusted table name
    rows = source_cursor.fetchall()

    insert_query = """
        INSERT INTO daily_activities (
            daily_activity_id, project_code, activity_date, priority, 
            start_time, end_time, created_by, created_at, 
            updated_at, activity_type, description, 
            activity_duration_minutes, user_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    inserted_total = 0
    inserted_with_user = 0
    inserted_without_user = 0
    skipped_parse_errors = 0

    for row in rows:
        # Extract the id_key from the da_data JSON; fall back gracefully when missing/invalid
        da_data = row.get("da_data", "{}")
        user_key = None
        try:
            da_data_json = json.loads(da_data)
            if isinstance(da_data_json, dict):
                user_key = da_data_json.get("id_key")
        except json.JSONDecodeError:
            skipped_parse_errors += 1

        user_id = None
        if user_key:
            user_id = get_user_id_from_key(user_lookup_cursor, user_key)

        # Insert regardless; if user_id is not found, use NULL to satisfy FK
        target_cursor.execute(insert_query, (
            row["da_id"],
            row["da_project_code"],
            row["da_date"],
            map_priority(row["da_priority"]),  # Map the priority value (or None)
            row["da_start_tm"],
            row["da_end_tm"],
            row["da_created_by"],
            row["da_created_date"],
            row["da_updated_date"],
            row["da_activity"],
            row["da_keterangan"],
            row["da_duration"],
            user_id  # May be None
        ))
        inserted_total += 1
        if user_id is not None:
            inserted_with_user += 1
        else:
            inserted_without_user += 1

    target_db.commit()
    print(
        f"✅ Inserted: {inserted_total} daily activity records. "
        f"(with user: {inserted_with_user}, without user: {inserted_without_user})"
    )
    if skipped_parse_errors:
        print(f"⚠️ JSON parse issues: {skipped_parse_errors} records (da_data invalid).")

    source_cursor.close()
    target_cursor.close()
    user_lookup_cursor.close()
    source_db.close()
    target_db.close()

if __name__ == "__main__":
    migrate_daily_activity()
