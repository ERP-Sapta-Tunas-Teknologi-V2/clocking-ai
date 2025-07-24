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
    user_lookup_cursor = target_db.cursor()  # For looking up user_id

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

    inserted_count = 0
    skipped_count = 0

    for row in rows:
        # Extract the id_key from the da_data JSON
        da_data = row.get("da_data", "{}")
        try:
            da_data_json = json.loads(da_data)
            user_key = da_data_json.get("id_key")

            if user_key:
                # Get user_id from ss_user table using the id_key
                user_id = get_user_id_from_key(user_lookup_cursor, user_key)

                if user_id:
                    # Insert into daily_activity table if user exists
                    target_cursor.execute(insert_query, (
                        row["da_id"],
                        row["da_project_code"],
                        row["da_date"],
                        map_priority(row["da_priority"]),  # Map the priority value
                        row["da_start_tm"],
                        row["da_end_tm"],
                        row["da_created_by"],
                        row["da_created_date"],
                        row["da_updated_date"],
                        row["da_activity"],
                        row["da_keterangan"],
                        row["da_duration"],
                        user_id  # Use the resolved user_id
                    ))
                    inserted_count += 1
                else:
                    print(f"⚠️ user_key {user_key} not found in ss_user table. Skipping project {row['da_project_code']}.")
                    skipped_count += 1
            else:
                print(f"⚠️ No id_key found in da_data for project {row['da_project_code']}. Skipping.")
                skipped_count += 1
        except json.JSONDecodeError:
            print(f"⚠️ Failed to parse da_data JSON for project {row['da_project_code']}. Skipping.")
            skipped_count += 1

    target_db.commit()
    print(f"✅ Inserted: {inserted_count} daily activity records.")
    print(f"⚠️ Skipped: {skipped_count} records due to missing or deleted users.")

    source_cursor.close()
    target_cursor.close()
    user_lookup_cursor.close()
    source_db.close()
    target_db.close()

if __name__ == "__main__":
    migrate_daily_activity()
