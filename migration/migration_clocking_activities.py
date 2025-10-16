import mysql.connector
import json
from datetime import datetime

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

    # Fetch required fields including fallbacks when da_clocking is empty
    source_cursor.execute(
        """
        SELECT 
            da_id,
            da_clocking,
            da_activity,
            da_duration,
            da_date,
            da_start_tm,
            da_end_tm
        FROM ss_daily_activity
        """
    )
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

    inserted_total = 0
    inserted_from_json = 0
    inserted_from_fallback = 0
    skipped_count = 0
    category_fixed_count = 0

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

        # If we have entries from JSON, insert them
        if da_id not in existing_daily_ids:
            # Cannot insert due to missing parent record in daily_activities
            skipped_count += 1
            continue

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

    target_db.commit()
    print(
        f"✅ Inserted: {inserted_total} clocking activity records. "
        f"(JSON: {inserted_from_json}, Fallback: {inserted_from_fallback}, "
        f"Category fixed: {category_fixed_count})"
    )
    print(f"⚠️ Skipped: {skipped_count} records due to insufficient data.")

    # Jalankan backfill setelah migrasi untuk merapikan kolom
    backfill_clocking_fields(target_cursor, target_db)

    # Verifikasi singkat
    target_cursor.execute("SELECT COUNT(*) FROM clocking_activities WHERE task_id=0")
    task_zero = target_cursor.fetchone()[0]
    target_cursor.execute("SELECT COUNT(*) FROM clocking_activities WHERE task_id IS NULL")
    task_null = target_cursor.fetchone()[0]
    target_cursor.execute("SELECT COUNT(*) FROM clocking_activities WHERE duration_minutes IS NULL")
    duration_null = target_cursor.fetchone()[0]
    print(f"🔎 Backfill check — task_id=0: {task_zero}, task_id NULL: {task_null}, duration NULL: {duration_null}")

    source_cursor.close()
    target_cursor.close()
    source_db.close()
    target_db.close()

if __name__ == "__main__":
    migrate_clocking_activities()
