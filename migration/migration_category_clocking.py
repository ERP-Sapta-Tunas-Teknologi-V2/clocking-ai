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


def category_exists(cursor, category_id):
    try:
        cursor.execute("SELECT 1 FROM category_clocking WHERE category_id = %s LIMIT 1", (category_id,))
        return cursor.fetchone() is not None
    except Exception:
        return False


def migrate_category_docking(mode: str = "incremental", since: str = None, limit: int = None, dry_run: bool = False):
    config = {
        "host": "localhost",
        "user": "root",
        "password": ""  # Adjust with your credentials if needed
    }

    source_db = connect_db(**config, db="system-smartpro")
    target_db = connect_db(**config, db="clocking_reports")

    source_cursor = source_db.cursor(dictionary=True)
    target_cursor = target_db.cursor()

    ensure_migration_state_table(target_cursor)
    wm_updated_at, wm_last_id = get_watermark(target_cursor, "ss_category_clocking")

    base_query = "SELECT cc_id, cc_definition, cc_productive, cc_billable, cc_used, cc_direct FROM ss_category_clocking"
    params = []
    where_clauses = []

    # ss_category_clocking tidak punya tanggal update; gunakan last_id untuk incremental
    if mode == "incremental" and wm_last_id is not None:
        where_clauses.append("cc_id > %s")
        params.append(wm_last_id)

    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)
    base_query += " ORDER BY cc_id ASC"
    if limit and isinstance(limit, int) and limit > 0:
        base_query += f" LIMIT {int(limit)}"

    source_cursor.execute(base_query, tuple(params) if params else None)
    rows = source_cursor.fetchall()

    upsert_query = """
        INSERT INTO category_clocking (
            category_id, category_description, is_productive, is_billable, is_used, is_direct
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            category_description = VALUES(category_description),
            is_productive = VALUES(is_productive),
            is_billable = VALUES(is_billable),
            is_used = VALUES(is_used),
            is_direct = VALUES(is_direct)
    """

    inserted = 0
    updated = 0
    max_id = wm_last_id or 0

    for row in rows:
        exists = category_exists(target_cursor, row["cc_id"])
        payload = (
            row["cc_id"],
            row["cc_definition"],
            row["cc_productive"] if row["cc_productive"] is not None else 0,
            row["cc_billable"] if row["cc_billable"] is not None else 0,
            row["cc_used"] if row["cc_used"] is not None else 0,
            row["cc_direct"] if row["cc_direct"] is not None else 0,
        )
        if not dry_run:
            target_cursor.execute(upsert_query, payload)
        if exists:
            updated += 1
        else:
            inserted += 1
        cid = row["cc_id"]
        if cid is not None and cid > max_id:
            max_id = cid

    if not dry_run:
        target_db.commit()
    total = inserted + updated
    print(f"✅ Categories processed: {total}. Inserted: {inserted}, Updated: {updated}.")

    if mode == "incremental" and not dry_run:
        update_watermark(target_cursor, target_db, "ss_category_clocking", None, max_id)

    source_cursor.close()
    target_cursor.close()
    source_db.close()
    target_db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate category_clocking with incremental mode and idempotent upsert")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    parser.add_argument("--since", type=str, default=None)  # diabaikan karena tidak ada timestamp di sumber
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    migrate_category_docking(mode=args.mode, since=args.since, limit=args.limit, dry_run=args.dry_run)
