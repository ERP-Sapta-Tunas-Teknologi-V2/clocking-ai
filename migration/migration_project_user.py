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
    cursor.execute("SELECT user_id FROM users WHERE user_key = %s", (user_key,))
    result = cursor.fetchone()
    return result[0] if result else None

def migrate_project_users():
    config = {
        "host": "localhost",
        "user": "root",
        "password": ""
    }

    source_db = connect_db(**config, db="system-smartpro")
    target_db = connect_db(**config, db="clocking_reports")

    source_cursor = source_db.cursor(dictionary=True)
    target_cursor = target_db.cursor()
    user_lookup_cursor = target_db.cursor()

    source_cursor.execute("SELECT pr_project_code, pr_members FROM ss_project_management")
    rows = source_cursor.fetchall()

    insert_query = "INSERT INTO project_users (project_code, user_id) VALUES (%s, %s)"
    inserted_count = 0
    skipped_count = 0

    for row in rows:
        project_code = row["pr_project_code"]
        members_json = row["pr_members"]

        if not members_json or members_json.strip() == "":
            continue

        try:
            members = json.loads(members_json)
            for user_key in members.keys():
                user_id = get_user_id_from_key(user_lookup_cursor, user_key)
                if user_id:
                    target_cursor.execute(insert_query, (project_code, user_id))
                    inserted_count += 1
                else:
                    print(f"⚠️ user_key {user_key} not found in users table. Skipping.")
                    skipped_count += 1
        except json.JSONDecodeError:
            print(f"⚠️ JSON parse error in project {project_code}, skipping row.")

    target_db.commit()
    print(f"✅ Inserted: {inserted_count} rows.")
    print(f"⚠️ Skipped: {skipped_count} rows due to missing user_key or bad JSON.")

    # Cleanup
    source_cursor.close()
    target_cursor.close()
    user_lookup_cursor.close()
    source_db.close()
    target_db.close()

if __name__ == "__main__":
    migrate_project_users()
