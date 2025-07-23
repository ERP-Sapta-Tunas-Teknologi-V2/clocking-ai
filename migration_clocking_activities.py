import mysql.connector
import json

def connect_db(host, user, password, db):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=db
    )

def migrate_clocking_activities():
    config = {
        "host": "localhost",
        "user": "root",
        "password": ""  # Adjust with your credentials if needed
    }

    source_db = connect_db(**config, db="system-smartpro")  # Adjust source db name
    target_db = connect_db(**config, db="clocking_reports")  # Adjust target db name

    source_cursor = source_db.cursor(dictionary=True)
    target_cursor = target_db.cursor()

    source_cursor.execute("SELECT da_id, da_clocking FROM ss_daily_activity")  # Fetch da_id and da_clocking (JSON) from source table
    rows = source_cursor.fetchall()

    insert_query = """
        INSERT INTO clocking_activities (
            daily_activity_id, task_id, activity_description, duration_minutes, 
            start_date, start_time, end_date, end_time, category_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    inserted_count = 0
    skipped_count = 0

    for row in rows:
        da_id = row["da_id"]
        da_clocking = row.get("da_clocking", "[]")  # Default to empty JSON if None

        # Only parse if da_clocking is not empty
        if da_clocking:
            try:
                # Parse the JSON field
                clocking_entries = json.loads(da_clocking)

                for entry in clocking_entries:
                    task_id = entry.get("task_id")
                    activity = entry.get("activity")
                    duration = entry.get("duration")
                    start_date = entry.get("start_date")
                    start_time = entry.get("start_time")
                    end_date = entry.get("end_date")
                    end_time = entry.get("end_time")
                    category_id = entry.get("task_id")  # Using task_id as category_id

                    # Insert each clocking activity into the target table
                    target_cursor.execute(insert_query, (
                        da_id, 
                        task_id, 
                        activity, 
                        duration, 
                        start_date, 
                        start_time, 
                        end_date, 
                        end_time, 
                        category_id
                    ))
                    inserted_count += 1

            except json.JSONDecodeError:
                print(f"⚠️ Failed to parse da_clocking JSON for project {da_id}. Skipping.")
                skipped_count += 1
        else:
            print(f"⚠️ No da_clocking data for project {da_id}. Skipping.")
            skipped_count += 1

    target_db.commit()
    print(f"✅ Inserted: {inserted_count} clocking activity records.")
    print(f"⚠️ Skipped: {skipped_count} records due to invalid or missing data.")

    source_cursor.close()
    target_cursor.close()
    source_db.close()
    target_db.close()

if __name__ == "__main__":
    migrate_clocking_activities()
