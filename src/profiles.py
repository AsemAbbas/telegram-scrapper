"""
Profile-based scraping system.
Supports multiple channels with different scraping strategies,
date ranges, daily limits, and auto-resume capabilities.
"""
import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any

DB_PATH = Path(__file__).parent.parent / "data" / "scraper.db"


def get_connection():
    """Get database connection."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_profiles_db():
    """Initialize the profiles database tables."""
    conn = get_connection()
    cursor = conn.cursor()

    # Channels table - independent channel entities
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            title TEXT,
            description TEXT,
            is_verified INTEGER DEFAULT 0,
            member_count INTEGER,
            last_scraped_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            created_by INTEGER
        )
    """)

    # Profiles table - one per channel
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            channel_username TEXT NOT NULL,
            channel_title TEXT,
            description TEXT,
            export_format TEXT DEFAULT 'xlsx',
            export_location TEXT DEFAULT 'default',
            export_custom_path TEXT,
            push_to_sheets INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            is_active INTEGER DEFAULT 1
        )
    """)

    # Processes table - multiple per profile
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            process_type TEXT NOT NULL,

            from_date TEXT,
            to_date TEXT,

            hours_back INTEGER DEFAULT 24,

            daily_limit INTEGER,
            batch_delay REAL DEFAULT 1.0,

            schedule_enabled INTEGER DEFAULT 0,
            schedule_time TEXT,
            schedule_interval_hours INTEGER,

            status TEXT DEFAULT 'idle',
            messages_scraped INTEGER DEFAULT 0,
            last_message_id INTEGER,
            last_message_date TEXT,
            current_position_date TEXT,
            today_scraped INTEGER DEFAULT 0,
            today_date TEXT,

            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_run_at TEXT,
            next_run_at TEXT,
            error_message TEXT,

            FOREIGN KEY (profile_id) REFERENCES profiles(id) ON DELETE CASCADE
        )
    """)

    # Scraped messages tracking (for duplicate detection)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scraped_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER NOT NULL,
            process_id INTEGER NOT NULL,
            channel_username TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            message_date TEXT,
            scraped_at TEXT NOT NULL,

            UNIQUE(channel_username, message_id),
            FOREIGN KEY (profile_id) REFERENCES profiles(id) ON DELETE CASCADE,
            FOREIGN KEY (process_id) REFERENCES processes(id) ON DELETE CASCADE
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sm_chan_msg ON scraped_messages(channel_username, message_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_proc_profile ON processes(profile_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_proc_status ON processes(status)")

    # Migrations: add new columns
    try:
        cursor.execute("ALTER TABLE profiles ADD COLUMN channel_id INTEGER REFERENCES channels(id)")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE profiles ADD COLUMN user_id INTEGER")
    except sqlite3.OperationalError:
        pass

    conn.commit()

    # Migrate existing profiles to channels table
    _migrate_profiles_to_channels(conn)

    conn.close()


def _migrate_profiles_to_channels(conn):
    """Migrate existing profiles to the channels table (one-time migration)."""
    cursor = conn.cursor()
    # Check for profiles with no channel_id set
    rows = cursor.execute(
        "SELECT id, channel_username, channel_title FROM profiles WHERE channel_id IS NULL AND channel_username IS NOT NULL"
    ).fetchall()
    if not rows:
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for row in rows:
        username = row["channel_username"].lstrip("@")
        # Check if channel already exists
        existing = cursor.execute("SELECT id FROM channels WHERE username = ?", (username,)).fetchone()
        if existing:
            channel_id = existing["id"]
        else:
            cursor.execute(
                "INSERT INTO channels (username, title, created_at, updated_at) VALUES (?, ?, ?, ?)",
                (username, row["channel_title"], now, now)
            )
            channel_id = cursor.lastrowid
        cursor.execute("UPDATE profiles SET channel_id = ? WHERE id = ?", (channel_id, row["id"]))
    conn.commit()


# ═══════════════════════════════════════════════════════════════
# CHANNEL CRUD
# ═══════════════════════════════════════════════════════════════

def create_channel(username: str, title: str = None, description: str = None,
                   created_by: int = None) -> Dict:
    """Create a new channel entity."""
    conn = get_connection()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    username = username.lstrip("@")
    try:
        conn.execute("""
            INSERT INTO channels (username, title, description, created_by, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (username, title, description, created_by, now, now))
        conn.commit()
        channel_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()
        return {"success": True, "id": channel_id}
    except sqlite3.IntegrityError:
        conn.close()
        return {"success": False, "error": f"Channel @{username} already exists"}


def get_channel(channel_id: int) -> Optional[Dict]:
    """Get a channel by ID."""
    conn = get_connection()
    row = conn.execute("SELECT * FROM channels WHERE id = ?", (channel_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_channel_by_username(username: str) -> Optional[Dict]:
    """Get a channel by username."""
    conn = get_connection()
    row = conn.execute("SELECT * FROM channels WHERE username = ?", (username.lstrip("@"),)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_channels() -> List[Dict]:
    """Get all channels with usage stats."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT c.*,
               COUNT(DISTINCT p.id) as profile_count,
               COALESCE(SUM(pr.messages_scraped), 0) as total_messages
        FROM channels c
        LEFT JOIN profiles p ON p.channel_id = c.id
        LEFT JOIN processes pr ON pr.profile_id = p.id
        GROUP BY c.id
        ORDER BY c.updated_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_channel(channel_id: int, **kwargs) -> Dict:
    """Update a channel."""
    conn = get_connection()
    allowed = {"title", "description", "is_verified", "member_count", "last_scraped_at"}
    updates, values = [], []
    for k, v in kwargs.items():
        if k in allowed:
            updates.append(f"{k} = ?")
            values.append(v)
    if not updates:
        conn.close()
        return {"success": False, "error": "No valid fields"}
    updates.append("updated_at = ?")
    values.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    values.append(channel_id)
    conn.execute(f"UPDATE channels SET {', '.join(updates)} WHERE id = ?", values)
    conn.commit()
    conn.close()
    return {"success": True}


def delete_channel(channel_id: int) -> Dict:
    """Delete a channel (only if no profiles reference it)."""
    conn = get_connection()
    count = conn.execute("SELECT COUNT(*) as c FROM profiles WHERE channel_id = ?", (channel_id,)).fetchone()["c"]
    if count > 0:
        conn.close()
        return {"success": False, "error": f"Cannot delete: {count} profile(s) still use this channel"}
    conn.execute("DELETE FROM channels WHERE id = ?", (channel_id,))
    conn.commit()
    conn.close()
    return {"success": True}


# ═══════════════════════════════════════════════════════════════
# PROFILE CRUD
# ═══════════════════════════════════════════════════════════════

def create_profile(name: str, channel_username: str, description: str = None,
                   export_format: str = "xlsx", export_location: str = "default",
                   export_custom_path: str = None, push_to_sheets: bool = False,
                   channel_id: int = None, user_id: int = None) -> Dict:
    conn = get_connection()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    username = channel_username.lstrip("@") if channel_username else ""

    # Auto-create or find channel if channel_id not provided
    if not channel_id and username:
        existing = conn.execute("SELECT id FROM channels WHERE username = ?", (username,)).fetchone()
        if existing:
            channel_id = existing["id"]
        else:
            conn.execute(
                "INSERT INTO channels (username, created_at, updated_at, created_by) VALUES (?, ?, ?, ?)",
                (username, now, now, user_id)
            )
            channel_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Get channel username from channel_id if not provided
    if channel_id and not username:
        ch_row = conn.execute("SELECT username FROM channels WHERE id = ?", (channel_id,)).fetchone()
        if ch_row:
            username = ch_row["username"]

    try:
        conn.execute("""
            INSERT INTO profiles (name, channel_username, description, export_format,
                                  export_location, export_custom_path, push_to_sheets,
                                  channel_id, user_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (name, username, description, export_format,
              export_location, export_custom_path, 1 if push_to_sheets else 0,
              channel_id, user_id, now, now))
        conn.commit()
        return {"success": True, "id": conn.execute("SELECT last_insert_rowid()").fetchone()[0]}
    except sqlite3.IntegrityError as e:
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def get_profile(profile_id: int) -> Optional[Dict]:
    conn = get_connection()
    row = conn.execute("SELECT * FROM profiles WHERE id = ?", (profile_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_profiles(user_id: int = None) -> List[Dict]:
    conn = get_connection()
    query = """
        SELECT p.*,
               COUNT(pr.id) as process_count,
               COALESCE(SUM(pr.messages_scraped), 0) as total_messages,
               (SELECT COUNT(*) FROM processes WHERE profile_id = p.id AND status = 'running') as running_count,
               c.title as channel_title_from_channels
        FROM profiles p
        LEFT JOIN processes pr ON p.id = pr.profile_id
        LEFT JOIN channels c ON p.channel_id = c.id
    """
    params = []
    if user_id is not None:
        query += " WHERE p.user_id = ?"
        params.append(user_id)
    query += " GROUP BY p.id ORDER BY p.updated_at DESC"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_profile(profile_id: int, **kwargs) -> Dict:
    conn = get_connection()
    allowed = {"name", "description", "export_format", "export_location",
               "export_custom_path", "is_active", "channel_title", "push_to_sheets", "channel_id"}
    updates, values = [], []
    for k, v in kwargs.items():
        if k in allowed:
            if k == "push_to_sheets":
                v = 1 if v else 0
            updates.append(f"{k} = ?")
            values.append(v)
    if not updates:
        conn.close()
        return {"success": False, "error": "No valid fields"}
    updates.append("updated_at = ?")
    values.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    values.append(profile_id)
    conn.execute(f"UPDATE profiles SET {', '.join(updates)} WHERE id = ?", values)
    conn.commit()
    conn.close()
    return {"success": True}


def delete_profile(profile_id: int) -> Dict:
    conn = get_connection()
    conn.execute("DELETE FROM scraped_messages WHERE profile_id = ?", (profile_id,))
    conn.execute("DELETE FROM processes WHERE profile_id = ?", (profile_id,))
    conn.execute("DELETE FROM profiles WHERE id = ?", (profile_id,))
    conn.commit()
    conn.close()
    return {"success": True}


# ═══════════════════════════════════════════════════════════════
# PROCESS CRUD
# ═══════════════════════════════════════════════════════════════

def create_process(profile_id: int, name: str, process_type: str,
                   from_date: str = None, to_date: str = None,
                   hours_back: int = 24, daily_limit: int = None,
                   batch_delay: float = 1.0,
                   schedule_enabled: bool = False, schedule_time: str = None,
                   schedule_interval_hours: int = None) -> Dict:
    conn = get_connection()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_position = from_date if process_type == "date_range" else None
    conn.execute("""
        INSERT INTO processes (profile_id, name, process_type, from_date, to_date,
                               hours_back, daily_limit, batch_delay, schedule_enabled,
                               schedule_time, schedule_interval_hours,
                               current_position_date, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (profile_id, name, process_type, from_date, to_date, hours_back,
          daily_limit, batch_delay, 1 if schedule_enabled else 0,
          schedule_time, schedule_interval_hours, current_position, now, now))
    pid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()
    return {"success": True, "id": pid}


def get_process(process_id: int) -> Optional[Dict]:
    conn = get_connection()
    row = conn.execute("""
        SELECT pr.*, p.channel_username, p.name as profile_name,
               p.export_format, p.export_location, p.export_custom_path, p.push_to_sheets
        FROM processes pr
        JOIN profiles p ON pr.profile_id = p.id
        WHERE pr.id = ?
    """, (process_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_profile_processes(profile_id: int) -> List[Dict]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM processes WHERE profile_id = ? ORDER BY created_at DESC",
        (profile_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_process(process_id: int, **kwargs) -> Dict:
    conn = get_connection()
    allowed = {"name", "process_type", "from_date", "to_date", "hours_back",
               "daily_limit", "batch_delay", "schedule_enabled", "schedule_time",
               "schedule_interval_hours", "status", "messages_scraped",
               "last_message_id", "last_message_date", "current_position_date",
               "today_scraped", "today_date", "last_run_at", "next_run_at", "error_message"}
    updates, values = [], []
    for k, v in kwargs.items():
        if k in allowed:
            if k == "schedule_enabled":
                v = 1 if v else 0
            updates.append(f"{k} = ?")
            values.append(v)
    if not updates:
        conn.close()
        return {"success": False, "error": "No valid fields"}
    updates.append("updated_at = ?")
    values.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    values.append(process_id)
    conn.execute(f"UPDATE processes SET {', '.join(updates)} WHERE id = ?", values)
    conn.commit()
    conn.close()
    return {"success": True}


def delete_process(process_id: int) -> Dict:
    conn = get_connection()
    conn.execute("DELETE FROM scraped_messages WHERE process_id = ?", (process_id,))
    conn.execute("DELETE FROM processes WHERE id = ?", (process_id,))
    conn.commit()
    conn.close()
    return {"success": True}


# ═══════════════════════════════════════════════════════════════
# PROGRESS & DUPLICATE TRACKING
# ═══════════════════════════════════════════════════════════════

def is_message_scraped(channel_username: str, message_id: int) -> bool:
    conn = get_connection()
    row = conn.execute(
        "SELECT 1 FROM scraped_messages WHERE channel_username = ? AND message_id = ?",
        (channel_username.lstrip("@"), message_id)
    ).fetchone()
    conn.close()
    return row is not None


def batch_check_scraped(channel_username: str, message_ids: List[int]) -> set:
    """Check which message IDs are already scraped. Returns set of already-scraped IDs."""
    if not message_ids:
        return set()
    conn = get_connection()
    channel = channel_username.lstrip("@")
    already_scraped = set()
    # SQLite has a variable limit, process in chunks of 500
    for i in range(0, len(message_ids), 500):
        chunk = message_ids[i:i+500]
        placeholders = ",".join(["?"] * len(chunk))
        rows = conn.execute(
            f"SELECT message_id FROM scraped_messages WHERE channel_username = ? AND message_id IN ({placeholders})",
            [channel] + chunk
        ).fetchall()
        already_scraped.update(r["message_id"] for r in rows)
    conn.close()
    return already_scraped


def mark_messages_scraped(profile_id: int, process_id: int, channel_username: str,
                          messages: List[Dict]) -> int:
    """Mark messages as scraped. Returns count of NEW (non-duplicate) messages inserted."""
    conn = get_connection()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    channel = channel_username.lstrip("@")
    new_count = 0
    for msg in messages:
        try:
            conn.execute("""
                INSERT INTO scraped_messages (profile_id, process_id, channel_username,
                                              message_id, message_date, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (profile_id, process_id, channel, msg.get("msg_id"),
                  msg.get("date_utc"), now))
            new_count += 1
        except sqlite3.IntegrityError:
            pass  # Already scraped - UNIQUE constraint prevents duplicates
    conn.commit()
    conn.close()
    return new_count


def get_channel_scraped_stats(channel_username: str) -> Dict:
    """Get stats about what's been scraped for a channel."""
    conn = get_connection()
    channel = channel_username.lstrip("@")
    row = conn.execute("""
        SELECT COUNT(*) as total_unique,
               MIN(message_date) as earliest_date,
               MAX(message_date) as latest_date,
               MIN(message_id) as min_msg_id,
               MAX(message_id) as max_msg_id
        FROM scraped_messages WHERE channel_username = ?
    """, (channel,)).fetchone()
    conn.close()
    return dict(row) if row else {"total_unique": 0, "earliest_date": None, "latest_date": None}


def increment_process_count(process_id: int, count: int, last_msg_id: int = None,
                            last_msg_date: str = None, current_position: str = None):
    conn = get_connection()
    today = datetime.now().strftime("%Y-%m-%d")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = conn.execute("SELECT today_date, today_scraped FROM processes WHERE id = ?", (process_id,)).fetchone()
    if row:
        new_today = (row["today_scraped"] or 0) + count if row["today_date"] == today else count
        conn.execute("""
            UPDATE processes
            SET messages_scraped = messages_scraped + ?,
                today_scraped = ?, today_date = ?,
                last_message_id = COALESCE(?, last_message_id),
                last_message_date = COALESCE(?, last_message_date),
                current_position_date = COALESCE(?, current_position_date),
                updated_at = ?
            WHERE id = ?
        """, (count, new_today, today, last_msg_id, last_msg_date, current_position, now, process_id))
    conn.commit()
    conn.close()


def get_daily_remaining(process_id: int) -> Optional[int]:
    """How many messages can still be scraped today. None = no limit."""
    conn = get_connection()
    row = conn.execute("SELECT daily_limit, today_scraped, today_date FROM processes WHERE id = ?", (process_id,)).fetchone()
    conn.close()
    if not row or not row["daily_limit"]:
        return None
    today = datetime.now().strftime("%Y-%m-%d")
    used = row["today_scraped"] or 0 if row["today_date"] == today else 0
    return max(0, row["daily_limit"] - used)


def get_process_progress(process_id: int) -> Optional[Dict]:
    proc = get_process(process_id)
    if not proc:
        return None
    conn = get_connection()
    stats = dict(conn.execute(
        "SELECT COUNT(*) as count FROM scraped_messages WHERE process_id = ?",
        (process_id,)
    ).fetchone())
    conn.close()

    progress_percent = 0
    if proc["process_type"] == "date_range" and proc["from_date"] and proc["to_date"]:
        try:
            from_dt = datetime.strptime(proc["from_date"], "%Y-%m-%d")
            to_dt = datetime.strptime(proc["to_date"], "%Y-%m-%d")
            total_days = (to_dt - from_dt).days
            if total_days > 0 and proc["current_position_date"]:
                cur_dt = datetime.strptime(proc["current_position_date"], "%Y-%m-%d")
                days_done = (cur_dt - from_dt).days
                progress_percent = min(100, int((days_done / total_days) * 100))
        except ValueError:
            pass

    remaining = get_daily_remaining(process_id)

    return {
        **proc,
        "unique_messages": stats["count"],
        "progress_percent": progress_percent,
        "daily_remaining": remaining
    }


def get_due_processes() -> List[Dict]:
    """Get processes that should run now."""
    conn = get_connection()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute("""
        SELECT pr.*, p.channel_username, p.name as profile_name,
               p.export_format, p.export_location, p.export_custom_path, p.push_to_sheets
        FROM processes pr
        JOIN profiles p ON pr.profile_id = p.id
        WHERE pr.schedule_enabled = 1
          AND pr.status NOT IN ('running', 'completed')
          AND p.is_active = 1
          AND (pr.next_run_at IS NULL OR pr.next_run_at <= ?)
    """, (now,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def calculate_next_run(process: Dict) -> str:
    now = datetime.now()
    if process.get("schedule_interval_hours"):
        nxt = now + timedelta(hours=process["schedule_interval_hours"])
    elif process.get("schedule_time"):
        h, m = process["schedule_time"].split(":")
        nxt = now.replace(hour=int(h), minute=int(m), second=0)
        if nxt <= now:
            nxt += timedelta(days=1)
    else:
        nxt = now + timedelta(days=1)
    return nxt.strftime("%Y-%m-%d %H:%M:%S")


# Initialize on import
init_profiles_db()
