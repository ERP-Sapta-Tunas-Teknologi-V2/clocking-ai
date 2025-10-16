import mysql.connector
import json

def connect_db(host, user, password, db):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=db
    )

def get_user_id_from_email(cursor, email):
    cursor.execute("SELECT user_id FROM users WHERE email = %s", (email,))
    result = cursor.fetchone()
    return result[0] if result else None

def project_user_exists(cursor, project_code, user_id):
    cursor.execute(
        "SELECT 1 FROM project_users WHERE project_code = %s AND user_id = %s LIMIT 1",
        (project_code, user_id),
    )
    return cursor.fetchone() is not None

def normalize_position(jabatan):
    try:
        if jabatan is None:
            return None
        s = str(jabatan).strip()
        if s == "":
            return None
        return int(s)
    except Exception:
        return None

def get_or_create_user(target_cursor, lookup_cursor, email, nickname, jabatan, id_key, next_id_func):
    # Prefer existing by email
    if email:
        existing = get_user_id_from_email(lookup_cursor, email)
        if existing:
            return existing
    # Build placeholder email if missing
    placeholder_email = email if email else f"{id_key}@placeholder.local"
    # Check again to avoid duplicate placeholder creation
    existing_pl = get_user_id_from_email(lookup_cursor, placeholder_email)
    if existing_pl:
        return existing_pl
    full_name = nickname or (email or placeholder_email)
    position = normalize_position(jabatan)
    new_user_id = next_id_func()
    target_cursor.execute(
        """
        INSERT INTO users (user_id, full_name, email, position, created_at, updated_at)
        VALUES (%s, %s, %s, %s, NOW(), NOW())
        """,
        (new_user_id, full_name, placeholder_email, position),
    )
    return new_user_id

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

    # Prepare next user_id generator for placeholder users (if table lacks AUTO_INCREMENT)
    user_lookup_cursor.execute("SELECT COALESCE(MAX(user_id), 0) FROM users")
    current_max_user_id = user_lookup_cursor.fetchone()[0] or 0
    state = {"max_user_id": current_max_user_id}
    def next_user_id():
        state["max_user_id"] += 1
        return state["max_user_id"]

    source_cursor.execute("SELECT pr_project_code, pr_members FROM ss_project_management")
    rows = source_cursor.fetchall()

    insert_query = "INSERT INTO project_users (project_code, user_id) VALUES (%s, %s)"
    inserted_count = 0
    skipped_count = 0
    duplicate_skipped = 0

    for row in rows:
        project_code = row["pr_project_code"]
        members_json = row["pr_members"]

        if not members_json or members_json.strip() == "":
            continue

        try:
            members = json.loads(members_json)
            # Expecting dict: {id_key: { email, id_key, jabatan, nickname }}
            if isinstance(members, dict):
                for id_key, member in members.items():
                    if not isinstance(member, dict):
                        skipped_count += 1
                        continue
                    email = member.get("email")
                    nickname = member.get("nickname")
                    jabatan = member.get("jabatan")
                    # Get or create user in target DB to avoid skips
                    user_id = get_or_create_user(target_cursor, user_lookup_cursor, email, nickname, jabatan, id_key, next_user_id)
                    if project_user_exists(target_cursor, project_code, user_id):
                        duplicate_skipped += 1
                        continue
                    target_cursor.execute(insert_query, (project_code, user_id))
                    inserted_count += 1
            else:
                skipped_count += 1
                print(f"⚠️ Unexpected members format for project {project_code}, expected dict. Skipping.")
        except json.JSONDecodeError:
            print(f"⚠️ JSON parse error in project {project_code}, skipping row.")

    target_db.commit()
    print(f"✅ Inserted: {inserted_count} rows.")
    print(f"⚠️ Skipped: {skipped_count} rows due to missing email/user or bad JSON.")
    print(f"ℹ️ Duplicate pairs ignored: {duplicate_skipped} rows.")

    # Cleanup
    source_cursor.close()
    target_cursor.close()
    user_lookup_cursor.close()
    source_db.close()
    target_db.close()

if __name__ == "__main__":
    migrate_project_users()
