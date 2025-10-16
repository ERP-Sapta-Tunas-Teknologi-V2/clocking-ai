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

DEFAULT_CATEGORY_ID = 1  # Fallback category when da_clocking JSON is empty and category not found
TASK_ID_MAP = {
    # Assumptive task mapping based on common activity labels
    # Adjust as needed to match your real task catalog
    "remote": 1,
    "wfh": 1,
    "onsite": 7,
    "wfo": 7,
}

def map_priority(priority):
    mapping = {
        'H': 'High',
        'M': 'Medium',
        'L': 'Low'
    }
    try:
        return mapping.get(str(priority))
    except Exception:
        return None

# --- Helpers for Daily Activities migration ---
def get_user_id_from_key(cursor, user_key):
    """Lookup source ss_user.id by id_key."""
    try:
        cursor.execute("SELECT id FROM ss_user WHERE id_key = %s", (user_key,))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception:
        return None

def daily_activity_exists(target_cursor, da_id):
    try:
        target_cursor.execute("SELECT 1 FROM daily_activities WHERE daily_activity_id = %s LIMIT 1", (da_id,))
        return target_cursor.fetchone() is not None
    except Exception:
        return False

def get_target_user_id_from_id_key(cursor, id_key):
    try:
        if not id_key:
            return None
        placeholder_email = f"{id_key}@placeholder.local"
        cursor.execute("SELECT user_id FROM users WHERE email = %s", (placeholder_email,))
        res = cursor.fetchone()
        return res[0] if res else None
    except Exception:
        return None

def compute_diff_minutes(start_date, start_time, end_date, end_time):
    try:
        if not (start_date and start_time and end_date and end_time):
            return None
        start_dt = datetime.fromisoformat(f"{start_date} {start_time}")
        end_dt = datetime.fromisoformat(f"{end_date} {end_time}")
        delta = end_dt - start_dt
        return int(delta.total_seconds() // 60)
    except Exception:
        return None


def backfill_clocking_fields(target_cursor, target_db):
    # 1) Isi duration_minutes dari start/end ketika tersedia
    target_cursor.execute(
        """
        UPDATE clocking_activities
        SET duration_minutes = TIMESTAMPDIFF(
            MINUTE,
            TIMESTAMP(start_date, start_time),
            TIMESTAMP(end_date, end_time)
        )
        WHERE duration_minutes IS NULL
          AND start_date IS NOT NULL AND start_time IS NOT NULL
          AND end_date IS NOT NULL AND end_time IS NOT NULL
        """
    )

    # 2) Set task_id via mapping umum, hanya untuk yang NULL/0
    target_cursor.execute(
        """
        UPDATE clocking_activities
        SET task_id = CASE
            WHEN LOWER(activity_description) = 'remote' THEN 1
            WHEN LOWER(activity_description) = 'wfh' THEN 1
            WHEN LOWER(activity_description) = 'onsite' THEN 7
            WHEN LOWER(activity_description) = 'wfo' THEN 7
            ELSE NULL
        END
        WHERE (task_id IS NULL OR task_id = 0)
        """
    )

    # 3) Pastikan sisa task_id=0 menjadi NULL
    target_cursor.execute(
        """
        UPDATE clocking_activities
        SET task_id = NULL
        WHERE task_id = 0
        """
    )

    # 4) Untuk sisa duration NULL, set ke 0 sebagai default aman
    target_cursor.execute(
        """
        UPDATE clocking_activities
        SET duration_minutes = 0
        WHERE duration_minutes IS NULL
        """
    )

    target_db.commit()


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
        # Best-effort; if fails, incremental mode will fall back to full scan
        pass


def get_watermark(target_cursor, job_name):
    try:
        target_cursor.execute(
            "SELECT last_updated_at, last_id FROM migration_state WHERE job_name = %s",
            (job_name,)
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
            (job_name, last_updated_at, last_id)
        )
        target_db.commit()
    except Exception:
        # Best-effort; do not break migration because watermark update fails
        pass


def migrate_daily_activity(mode: str = "incremental", since: str = None, limit: int = None, dry_run: bool = False):
    config = {
        "host": "localhost",
        "user": "root",
        "password": ""  # Adjust with your credentials if needed
    }

    source_db = connect_db(**config, db="system-smartpro")  # Source db name
    target_db = connect_db(**config, db="clocking_reports")  # Target db name

    source_cursor = source_db.cursor(dictionary=True)
    target_cursor = target_db.cursor()
    user_lookup_cursor = source_db.cursor()  # Lookup id from ss_user in source DB

    # Ensure watermark table exists
    ensure_migration_state_table(target_cursor)
    wm_updated_at, wm_last_id = get_watermark(target_cursor, "ss_daily_activity")

    # Build incremental query
    base_query = "SELECT * FROM ss_daily_activity"
    params = []
    where_clauses = []

    effective_since = since or (wm_updated_at.isoformat(sep=' ') if isinstance(wm_updated_at, datetime) else None)
    if effective_since:
        where_clauses.append("((da_updated_date IS NOT NULL AND da_updated_date >= %s) OR (da_created_date IS NOT NULL AND da_created_date >= %s))")
        params.extend([effective_since, effective_since])

    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)
    base_query += " ORDER BY da_updated_date ASC, da_id ASC"
    if limit and isinstance(limit, int) and limit > 0:
        base_query += f" LIMIT {int(limit)}"

    source_cursor.execute(base_query, tuple(params) if params else None)
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
    max_updated_at = None
    max_id = None

    for row in rows:
        # Extract id_key from da_data JSON
        da_data = row.get("da_data", "{}")
        user_key = None
        try:
            da_data_json = json.loads(da_data) if isinstance(da_data, str) else da_data
            if isinstance(da_data_json, dict):
                user_key = da_data_json.get("id_key")
        except json.JSONDecodeError:
            skipped_parse_errors += 1

        user_id = None
        if user_key:
            user_id = get_user_id_from_key(user_lookup_cursor, user_key)

        # Track maxima
        da_id = row.get("da_id")
        upd = row.get("da_updated_date") or row.get("da_created_date")
        try:
            if isinstance(upd, datetime):
                max_updated_at = upd if (max_updated_at is None or upd > max_updated_at) else max_updated_at
            max_id = da_id if (max_id is None or (da_id is not None and da_id > max_id)) else max_id
        except Exception:
            pass

        # Avoid duplicate if already present
        if daily_activity_exists(target_cursor, da_id):
            continue

        if not dry_run:
            target_cursor.execute(insert_query, (
                da_id,
                row.get("da_project_code"),
                row.get("da_date"),
                map_priority(row.get("da_priority")),
                row.get("da_start_tm"),
                row.get("da_end_tm"),
                row.get("da_created_by"),
                row.get("da_created_date"),
                row.get("da_updated_date"),
                row.get("da_activity"),
                row.get("da_keterangan"),
                row.get("da_duration"),
                user_id,
            ))
        inserted_total += 1
        if user_id is not None:
            inserted_with_user += 1
        else:
            inserted_without_user += 1

    if not dry_run:
        target_db.commit()
    print(
        f"✅ Inserted: {inserted_total} daily activity records. "
        f"(with user: {inserted_with_user}, without user: {inserted_without_user})"
    )
    if skipped_parse_errors:
        print(f"⚠️ JSON parse issues: {skipped_parse_errors} records (da_data invalid).")

    # Update watermark if running incremental and not dry-run
    if (mode == "incremental" or effective_since) and not dry_run:
        update_watermark(target_cursor, target_db, "ss_daily_activity", max_updated_at, max_id)

    source_cursor.close()
    target_cursor.close()
    user_lookup_cursor.close()
    source_db.close()
    target_db.close()


def migrate_clocking_activities(mode: str = "incremental", since: str = None, limit: int = None, dry_run: bool = False):
    config = {
        "host": "localhost",
        "user": "root",
        "password": ""  # Adjust with your credentials if needed
    }

    source_db = connect_db(**config, db="system-smartpro")  # Adjust source db name
    target_db = connect_db(**config, db="clocking_reports")  # Adjust target db name

    source_cursor = source_db.cursor(dictionary=True)
    target_cursor = target_db.cursor()

    # Ensure watermark table exists
    ensure_migration_state_table(target_cursor)
    wm_updated_at, wm_last_id = get_watermark(target_cursor, "ss_daily_activity")

    # Fetch required fields including fallbacks when da_clocking is empty
    base_query = (
        """
        SELECT 
            da_id,
            da_clocking,
            da_activity,
            da_duration,
            da_date,
            da_start_tm,
            da_end_tm,
            da_project_code,
            da_priority,
            da_created_by,
            da_created_date,
            da_updated_date,
            da_keterangan,
            da_data
        FROM ss_daily_activity
        """
    )
    params = []
    where_clauses = []

    effective_since = since or (wm_updated_at.isoformat(sep=' ') if isinstance(wm_updated_at, datetime) else None)
    if effective_since:
        where_clauses.append("((da_updated_date IS NOT NULL AND da_updated_date >= %s) OR (da_created_date IS NOT NULL AND da_created_date >= %s))")
        params.extend([effective_since, effective_since])

    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)
    base_query += " ORDER BY da_updated_date ASC, da_id ASC"
    if limit and isinstance(limit, int) and limit > 0:
        base_query += f" LIMIT {int(limit)}"

    source_cursor.execute(base_query, tuple(params) if params else None)
    rows = source_cursor.fetchall()

    # Preload existing daily_activity IDs from target to satisfy FK constraints
    target_cursor.execute("SELECT daily_activity_id FROM daily_activities")
    existing_daily_ids = {row[0] for row in target_cursor.fetchall()}

    # Preload valid category IDs to guard FK constraints
    target_cursor.execute("SELECT category_id FROM category_clocking")
    valid_category_ids = {row[0] for row in target_cursor.fetchall()}

    # Track which daily_activity_ids already have clocking entries to avoid duplicate fallbacks
    target_cursor.execute("SELECT DISTINCT daily_activity_id FROM clocking_activities")
    daily_ids_with_clockings = {row[0] for row in target_cursor.fetchall()}

    insert_query = """
        INSERT INTO clocking_activities (
            daily_activity_id, task_id, activity_description, duration_minutes, 
            start_date, start_time, end_date, end_time, category_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Prepare insert for missing parent daily_activities (auto-create)
    daily_insert_query = """
        INSERT INTO daily_activities (
            daily_activity_id, project_code, activity_date, priority,
            start_time, end_time, created_by, created_at,
            updated_at, activity_type, description,
            activity_duration_minutes, user_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    inserted_total = 0
    inserted_from_json = 0
    inserted_from_fallback = 0
    skipped_count = 0
    category_fixed_count = 0
    max_updated_at = None
    max_id = None

    for row in rows:
        da_id = row["da_id"]

        # Normalize da_clocking: treat None/empty string/'null' as empty list
        da_clocking_raw = row.get("da_clocking")
        if da_clocking_raw is None or str(da_clocking_raw).strip() in ("", "null", "NULL"):
            da_clocking = []
        else:
            try:
                # If stored as JSON string, parse; if already list/dict, keep
                da_clocking = (
                    json.loads(da_clocking_raw)
                    if isinstance(da_clocking_raw, str)
                    else da_clocking_raw
                )
                if da_clocking is None:
                    da_clocking = []
            except json.JSONDecodeError:
                # Fall back to empty list on invalid JSON
                da_clocking = []

        # Track maxima
        upd = row.get("da_updated_date") or row.get("da_created_date")
        try:
            if isinstance(upd, datetime):
                max_updated_at = upd if (max_updated_at is None or upd > max_updated_at) else max_updated_at
            max_id = da_id if (max_id is None or (da_id is not None and da_id > max_id)) else max_id
        except Exception:
            pass

        # Ensure parent daily_activities exists; auto-create minimal row if missing
        if da_id not in existing_daily_ids:
            # Try map user via id_key -> placeholder email
            id_key = None
            try:
                da_data_raw = row.get("da_data")
                if da_data_raw:
                    da_json = json.loads(da_data_raw) if isinstance(da_data_raw, str) else da_data_raw
                    if isinstance(da_json, dict):
                        id_key = da_json.get("id_key")
            except json.JSONDecodeError:
                id_key = None
            user_id = get_target_user_id_from_id_key(target_cursor, id_key)
            if not dry_run:
                target_cursor.execute(
                    daily_insert_query,
                    (
                        da_id,
                        row.get("da_project_code"),
                        row.get("da_date"),
                        map_priority(row.get("da_priority")),
                        row.get("da_start_tm"),
                        row.get("da_end_tm"),
                        row.get("da_created_by"),
                        row.get("da_created_date"),
                        row.get("da_updated_date"),
                        row.get("da_activity"),
                        row.get("da_keterangan"),
                        row.get("da_duration"),
                        user_id,
                    ),
                )
            # Update local set to avoid double counting even in dry-run
            existing_daily_ids.add(da_id)

        if isinstance(da_clocking, list) and len(da_clocking) > 0:
            for entry in da_clocking:
                task_id = entry.get("task_id")
                activity = entry.get("activity")
                duration = entry.get("duration")
                start_date = entry.get("start_date")
                start_time = entry.get("start_time")
                end_date = entry.get("end_date")
                end_time = entry.get("end_time")
                # If duration missing, try compute from start/end
                if duration is None:
                    duration = compute_diff_minutes(start_date, start_time, end_date, end_time)
                # If task_id missing, try mapping from activity label
                if task_id is None and activity:
                    task_id = TASK_ID_MAP.get(str(activity).strip().lower())
                # Leave task_id as None (NULL) if no mapping
                # Prefer explicit category_id; otherwise fall back to task_id; else default
                category_id = (
                    entry.get("category_id")
                    if entry.get("category_id") is not None
                    else (entry.get("task_id") if entry.get("task_id") is not None else DEFAULT_CATEGORY_ID)
                )

                # Guard against invalid category ids
                if category_id not in valid_category_ids:
                    category_id = DEFAULT_CATEGORY_ID
                    category_fixed_count += 1

                if not dry_run:
                    target_cursor.execute(insert_query, (
                        da_id,
                        task_id,
                        activity,
                        duration,
                        start_date,
                        start_time,
                        end_date,
                        end_time,
                        category_id,
                    ))
                inserted_total += 1
                inserted_from_json += 1
        else:
            # Fallback: build a single clocking activity record from ss_daily_activity columns
            # This reduces skipped rows and creates usable data even without JSON
            activity_desc = row.get("da_activity") or None
            duration_minutes = row.get("da_duration") or None

            # Extract date/time parts from timestamps (if present)
            start_ts = row.get("da_start_tm")
            end_ts = row.get("da_end_tm")

            def split_dt(ts):
                if ts is None:
                    return None, None
                if isinstance(ts, datetime):
                    return ts.date().isoformat(), ts.time().isoformat()
                # If ts is a string, try to parse
                try:
                    parsed = datetime.fromisoformat(str(ts))
                    return parsed.date().isoformat(), parsed.time().isoformat()
                except ValueError:
                    return None, None

            start_date, start_time = split_dt(start_ts)
            end_date, end_time = split_dt(end_ts)

            # If duration missing, compute from start/end
            if duration_minutes is None:
                duration_minutes = compute_diff_minutes(start_date, start_time, end_date, end_time)
            # Fallback to 0 if still None (ensure non-null)
            if duration_minutes is None:
                duration_minutes = 0

            # Only insert fallback if this daily_activity_id has no existing clocking entries
            if da_id not in daily_ids_with_clockings:
                if not dry_run:
                    target_cursor.execute(insert_query, (
                        da_id,
                        None,  # task_id unknown when no JSON
                        activity_desc,
                        duration_minutes,
                        start_date,
                        start_time,
                        end_date,
                        end_time,
                        DEFAULT_CATEGORY_ID,  # use default category
                    ))
                inserted_total += 1
                inserted_from_fallback += 1
                daily_ids_with_clockings.add(da_id)

    if not dry_run:
        target_db.commit()
    print(
        f"✅ Inserted: {inserted_total} clocking activity records. "
        f"(JSON: {inserted_from_json}, Fallback: {inserted_from_fallback}, "
        f"Category fixed: {category_fixed_count})"
    )
    print(f"⚠️ Skipped: {skipped_count} records due to insufficient data.")

    # Jalankan backfill setelah migrasi untuk merapikan kolom
    if not dry_run:
        backfill_clocking_fields(target_cursor, target_db)

    # Verifikasi singkat
    if not dry_run:
        target_cursor.execute("SELECT COUNT(*) FROM clocking_activities WHERE task_id=0")
        task_zero = target_cursor.fetchone()[0]
        target_cursor.execute("SELECT COUNT(*) FROM clocking_activities WHERE task_id IS NULL")
        task_null = target_cursor.fetchone()[0]
        target_cursor.execute("SELECT COUNT(*) FROM clocking_activities WHERE duration_minutes IS NULL")
        duration_null = target_cursor.fetchone()[0]
        print(f"🔎 Backfill check — task_id=0: {task_zero}, task_id NULL: {task_null}, duration NULL: {duration_null}")

    # Update watermark if running incremental and not dry-run
    if (mode == "incremental" or effective_since) and not dry_run:
        update_watermark(target_cursor, target_db, "ss_daily_activity", max_updated_at, max_id)

    source_cursor.close()
    target_cursor.close()
    source_db.close()
    target_db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate daily and clocking activities with incremental support")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental", help="Run mode")
    parser.add_argument("--since", type=str, default=None, help="Process records updated/created since this DATETIME (YYYY-MM-DD[ HH:MM:SS])")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of source rows to process")
    parser.add_argument("--dry-run", action="store_true", help="Do not insert/update; only compute and print counts")
    args = parser.parse_args()

    print(f"🚀 Running migration (mode={args.mode}, since={args.since}, limit={args.limit}, dry_run={args.dry_run})")
    migrate_daily_activity(mode=args.mode, since=args.since, limit=args.limit, dry_run=args.dry_run)
    migrate_clocking_activities(mode=args.mode, since=args.since, limit=args.limit, dry_run=args.dry_run)
