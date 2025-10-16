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

def map_status(old_status):
    status_map = {
        'p': 'progress',
        'f': 'finished',
        'i': 'initial',
        'c': 'cancelled'
    }
    return status_map.get(old_status.lower(), 'unknown')

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


def project_exists(cursor, project_code):
    try:
        cursor.execute("SELECT 1 FROM projects WHERE project_code = %s LIMIT 1", (project_code,))
        return cursor.fetchone() is not None
    except Exception:
        return False


def migrate_projects(mode: str = "incremental", since: str = None, limit: int = None, dry_run: bool = False):
    config = {
        "host": "localhost",
        "user": "root",
        "password": ""  # Leave empty if no password
    }

    source_db = connect_db(**config, db="system-smartpro")
    target_db = connect_db(**config, db="clocking_reports")

    source_cursor = source_db.cursor(dictionary=True)
    target_cursor = target_db.cursor()

    ensure_migration_state_table(target_cursor)
    wm_updated_at, wm_last_id = get_watermark(target_cursor, "ss_project_management")

    base_query = (
        """
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
    records = source_cursor.fetchall()

    insert_query = """
        INSERT INTO projects (
            project_code, project_name, customer_name,
            project_manager_id, created_by, created_at,
            last_update, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    update_query = """
        UPDATE projects SET 
            project_name = %s,
            customer_name = %s,
            project_manager_id = %s,
            created_by = %s,
            created_at = %s,
            last_update = %s,
            status = %s
        WHERE project_code = %s
    """

    inserted = 0
    updated = 0
    max_updated_at = None
    max_id = None

    for row in records:
        exists = project_exists(target_cursor, row["project_code"])
        if not dry_run:
            if exists:
                target_cursor.execute(
                    update_query,
                    (
                        row["project_name"],
                        row["customer_name"],
                        row["project_manager_id"],
                        row["created_by"],
                        row["created_at"],
                        row["last_update"],
                        map_status(row["status"]),
                        row["project_code"],
                    ),
                )
            else:
                target_cursor.execute(
                    insert_query,
                    (
                        row["project_code"],
                        row["project_name"],
                        row["customer_name"],
                        row["project_manager_id"],
                        row["created_by"],
                        row["created_at"],
                        row["last_update"],
                        map_status(row["status"]),
                    ),
                )
        if exists:
            updated += 1
        else:
            inserted += 1

        upd = row.get("last_update") or row.get("created_at")
        try:
            if isinstance(upd, datetime):
                max_updated_at = upd if (max_updated_at is None or upd > max_updated_at) else max_updated_at
        except Exception:
            pass

    if not dry_run:
        target_db.commit()

    total = inserted + updated
    print(f"✅ Projects processed: {total}. Inserted: {inserted}, Updated: {updated}.")

    if (mode == "incremental" or effective_since) and not dry_run:
        update_watermark(target_cursor, target_db, "ss_project_management", max_updated_at, max_id)

    # Cleanup
    source_cursor.close()
    target_cursor.close()
    source_db.close()
    target_db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate projects with incremental mode and idempotent upsert")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    parser.add_argument("--since", type=str, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    migrate_projects(mode=args.mode, since=args.since, limit=args.limit, dry_run=args.dry_run)
