import mysql.connector
import json
from datetime import datetime
import argparse

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

def ensure_migration_state_table(target_cursor):
    try:
        target_cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_state (
                id INT AUTO_INCREMENT PRIMARY KEY,
                job_name VARCHAR(128) UNIQUE,
                last_updated_at DATETIME NULL,
                last_id BIGINT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """
        )
    except Exception:
        pass


def get_watermark(target_cursor, job_name):
    try:
        target_cursor.execute(
            "SELECT last_updated_at, last_id FROM migration_state WHERE job_name = %s",
            (job_name,),
        )
        row = target_cursor.fetchone()
        if row:
            return row[0], row[1]
    except Exception:
        pass
    return None, None


def update_watermark(target_cursor, target_db, job_name, last_updated_at, last_id):
    try:
        target_cursor.execute(
            """
            INSERT INTO migration_state (job_name, last_updated_at, last_id)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE last_updated_at = VALUES(last_updated_at), last_id = VALUES(last_id)
            """,
            (job_name, last_updated_at, last_id),
        )
        target_db.commit()
    except Exception:
        pass


def migrate_project_users(mode: str = "incremental", since: str = None, limit: int = None, dry_run: bool = False):
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

    ensure_migration_state_table(target_cursor)
    wm_updated_at, wm_last_id = get_watermark(target_cursor, "ss_project_management_members")

    # Prepare next user_id generator for placeholder users (if table lacks AUTO_INCREMENT)
    user_lookup_cursor.execute("SELECT COALESCE(MAX(user_id), 0) FROM users")
    current_max_user_id = user_lookup_cursor.fetchone()[0] or 0
    state = {"max_user_id": current_max_user_id}

    def next_user_id():
        state["max_user_id"] += 1
        return state["max_user_id"]

    base_query = (
        """
        SELECT pr_project_code, pr_members, pr_created_date, pr_last_update
        FROM ss_project_management
        """
    )
    params = []
    where_clauses = []
    effective_since = None if mode == "full" else (since or (wm_updated_at.isoformat(sep=' ') if isinstance(wm_updated_at, datetime) else None))
    if effective_since:
        where_clauses.append(
            "((pr_last_update IS NOT NULL AND pr_last_update >= %s) OR (pr_created_date IS NOT NULL AND pr_created_date >= %s))"
        )
        params.extend([effective_since, effective_since])

    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)
    base_query += " ORDER BY pr_last_update ASC, pr_project_code ASC"
    if limit and isinstance(limit, int) and limit > 0:
        base_query += f" LIMIT {int(limit)}"

    source_cursor.execute(base_query, tuple(params) if params else None)
    rows = source_cursor.fetchall()

    insert_query = "INSERT INTO project_users (project_code, user_id) VALUES (%s, %s)"
    inserted_count = 0
    skipped_count = 0
    duplicate_skipped = 0
    max_updated_at = None

    for row in rows:
        project_code = row["pr_project_code"]
        members_json = row["pr_members"]
        if not members_json or str(members_json).strip() == "":
            continue
        try:
            members = json.loads(members_json)
            if isinstance(members, dict):
                for id_key, member in members.items():
                    if not isinstance(member, dict):
                        skipped_count += 1
                        continue
                    email = member.get("email")
                    nickname = member.get("nickname")
                    jabatan = member.get("jabatan")
                    user_id = get_or_create_user(target_cursor, user_lookup_cursor, email, nickname, jabatan, id_key, next_user_id)
                    if project_user_exists(target_cursor, project_code, user_id):
                        duplicate_skipped += 1
                        continue
                    if not dry_run:
                        target_cursor.execute(insert_query, (project_code, user_id))
                    inserted_count += 1
            else:
                skipped_count += 1
                print(f"⚠️ Unexpected members format for project {project_code}, expected dict. Skipping.")
        except json.JSONDecodeError:
            print(f"⚠️ JSON parse error in project {project_code}, skipping row.")

        upd = row.get("pr_last_update") or row.get("pr_created_date")
        try:
            if isinstance(upd, datetime):
                max_updated_at = upd if (max_updated_at is None or upd > max_updated_at) else max_updated_at
        except Exception:
            pass

    if not dry_run:
        target_db.commit()

    print(f"✅ Inserted: {inserted_count} rows.")
    print(f"⚠️ Skipped: {skipped_count} rows due to missing email/user or bad JSON.")
    print(f"ℹ️ Duplicate pairs ignored: {duplicate_skipped} rows.")

    if (mode == "incremental" or effective_since) and not dry_run:
        update_watermark(target_cursor, target_db, "ss_project_management_members", max_updated_at, None)

    # Cleanup
    source_cursor.close()
    target_cursor.close()
    user_lookup_cursor.close()
    source_db.close()
    target_db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate project-users links with incremental mode")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    parser.add_argument("--since", type=str, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    migrate_project_users(mode=args.mode, since=args.since, limit=args.limit, dry_run=args.dry_run)
