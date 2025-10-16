import mysql.connector
from datetime import datetime
import argparse

def connect_db(host, user, password, db):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=db
    )

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


def normalize_position(value):
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def user_exists(cursor, user_id):
    try:
        cursor.execute("SELECT 1 FROM users WHERE user_id = %s LIMIT 1", (user_id,))
        return cursor.fetchone() is not None
    except Exception:
        return False


def migrate_users(mode: str = "incremental", since: str = None, limit: int = None, dry_run: bool = False):
    config = {
        "host": "localhost",
        "user": "root",
        "password": ""
    }

    source_db = connect_db(**config, db="system-smartpro")
    target_db = connect_db(**config, db="clocking_reports")

    source_cursor = source_db.cursor(dictionary=True)
    target_cursor = target_db.cursor()

    ensure_migration_state_table(target_cursor)
    wm_updated_at, wm_last_id = get_watermark(target_cursor, "ss_user")

    base_query = (
        """
        SELECT 
            id AS user_id,
            id_key AS user_key,
            name AS full_name,
            email,
            jabatan AS position,
            created_at,
            updated_at
        FROM ss_user
        """
    )
    params = []
    where_clauses = []

    effective_since = None if mode == "full" else (since or (wm_updated_at.isoformat(sep=' ') if isinstance(wm_updated_at, datetime) else None))
    if effective_since:
        where_clauses.append(
            "((updated_at IS NOT NULL AND updated_at >= %s) OR (created_at IS NOT NULL AND created_at >= %s))"
        )
        params.extend([effective_since, effective_since])

    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)
    base_query += " ORDER BY updated_at ASC, id ASC"
    if limit and isinstance(limit, int) and limit > 0:
        base_query += f" LIMIT {int(limit)}"

    source_cursor.execute(base_query, tuple(params) if params else None)
    rows = source_cursor.fetchall()

    upsert_query = """
        INSERT INTO users (
            user_id, full_name, email, position, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            full_name = VALUES(full_name),
            email = VALUES(email),
            position = VALUES(position),
            created_at = VALUES(created_at),
            updated_at = VALUES(updated_at)
    """

    inserted_new = 0
    updated_existing = 0
    skipped = 0
    max_updated_at = None
    max_id = None

    for row in rows:
        exists = user_exists(target_cursor, row["user_id"])
        if not dry_run:
            target_cursor.execute(
                upsert_query,
                (
                    row["user_id"],
                    row["full_name"],
                    row.get("email"),
                    normalize_position(row.get("position")),
                    row.get("created_at"),
                    row.get("updated_at"),
                ),
            )
        if exists:
            updated_existing += 1
        else:
            inserted_new += 1

        upd = row.get("updated_at") or row.get("created_at")
        try:
            if isinstance(upd, datetime):
                max_updated_at = upd if (max_updated_at is None or upd > max_updated_at) else max_updated_at
            uid = row.get("user_id")
            max_id = uid if (max_id is None or (uid is not None and uid > max_id)) else max_id
        except Exception:
            pass

    if not dry_run:
        target_db.commit()

    total = inserted_new + updated_existing
    print(
        f"✅ Users processed: {total}. Inserted: {inserted_new}, Updated: {updated_existing}, Skipped: {skipped}."
    )

    if (mode == "incremental" or effective_since) and not dry_run:
        update_watermark(target_cursor, target_db, "ss_user", max_updated_at, max_id)

    # Cleanup
    source_cursor.close()
    target_cursor.close()
    source_db.close()
    target_db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate users with incremental mode and idempotent upsert")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    parser.add_argument("--since", type=str, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    migrate_users(mode=args.mode, since=args.since, limit=args.limit, dry_run=args.dry_run)
