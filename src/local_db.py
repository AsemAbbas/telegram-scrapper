"""
Local SQLite database for backup storage and audit logging.
Provides fallback when Google Sheets quota is exceeded.
"""
import sqlite3
import json
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "scraper.db"


def get_connection():
    """Get database connection, creating tables if needed."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    _init_tables(conn)
    return conn


def _init_tables(conn):
    """Initialize database tables."""
    cursor = conn.cursor()
    
    # Audit/Activity log table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            action TEXT NOT NULL,
            details TEXT,
            status TEXT DEFAULT 'success'
        )
    """)
    
    # Scraped messages backup table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scrape_date TEXT,
            channel TEXT,
            username TEXT,
            msg_id INTEGER,
            date_utc TEXT,
            time_utc TEXT,
            text TEXT,
            views INTEGER,
            forwards INTEGER,
            replies_count INTEGER,
            reactions TEXT,
            reactions_sum INTEGER,
            media_type TEXT,
            urls TEXT,
            url_count INTEGER,
            hashtags TEXT,
            mentions TEXT,
            is_forward INTEGER,
            fwd_from_name TEXT,
            post_link TEXT UNIQUE,
            synced_to_sheets INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # App settings table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()


# ═══════════════════════════════════════════════════════════════
# Audit Log Functions
# ═══════════════════════════════════════════════════════════════

def log_audit(action: str, details: str = None, status: str = "success"):
    """Log an action to the audit trail."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO audit_log (timestamp, action, details, status) VALUES (?, ?, ?, ?)",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), action, details, status)
    )
    conn.commit()
    conn.close()


def get_audit_log(limit: int = 100):
    """Get recent audit log entries."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM audit_log ORDER BY id DESC LIMIT ?",
        (limit,)
    )
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def clear_audit_log():
    """Clear all audit log entries."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM audit_log")
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════
# Local Message Storage Functions
# ═══════════════════════════════════════════════════════════════

def save_messages_locally(rows: list):
    """Save scraped messages to local database."""
    if not rows:
        return 0
    
    conn = get_connection()
    cursor = conn.cursor()
    
    saved_count = 0
    for row in rows:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO messages (
                    scrape_date, channel, username, msg_id, date_utc, time_utc,
                    text, views, forwards, replies_count, reactions, reactions_sum,
                    media_type, urls, url_count, hashtags, mentions, is_forward,
                    fwd_from_name, post_link
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get("scrape_date", ""),
                row.get("channel", ""),
                row.get("username", ""),
                row.get("msg_id", 0),
                row.get("date_utc", ""),
                row.get("time_utc", ""),
                row.get("text", ""),
                row.get("views", 0),
                row.get("forwards", 0),
                row.get("replies_count", 0),
                row.get("reactions", ""),
                row.get("reactions_sum", 0),
                row.get("media_type", ""),
                row.get("urls", ""),
                row.get("url_count", 0),
                row.get("hashtags", ""),
                row.get("mentions", ""),
                1 if row.get("is_forward") else 0,
                row.get("fwd_from_name", ""),
                row.get("post_link", "")
            ))
            if cursor.rowcount > 0:
                saved_count += 1
        except sqlite3.IntegrityError:
            pass  # Duplicate, skip
    
    conn.commit()
    conn.close()
    return saved_count


def get_unsynced_messages(limit: int = 500):
    """Get messages that haven't been synced to Google Sheets."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM messages WHERE synced_to_sheets = 0 ORDER BY id LIMIT ?",
        (limit,)
    )
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def mark_messages_synced(post_links: list):
    """Mark messages as synced to Google Sheets."""
    if not post_links:
        return
    
    conn = get_connection()
    cursor = conn.cursor()
    placeholders = ",".join("?" * len(post_links))
    cursor.execute(
        f"UPDATE messages SET synced_to_sheets = 1 WHERE post_link IN ({placeholders})",
        post_links
    )
    conn.commit()
    conn.close()


def get_local_message_count():
    """Get count of locally stored messages."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as total, SUM(CASE WHEN synced_to_sheets = 0 THEN 1 ELSE 0 END) as unsynced FROM messages")
    row = cursor.fetchone()
    conn.close()
    return {"total": row["total"] or 0, "unsynced": row["unsynced"] or 0}


# ═══════════════════════════════════════════════════════════════
# Settings Functions
# ═══════════════════════════════════════════════════════════════

def get_setting(key: str, default=None):
    """Get a setting value."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        try:
            return json.loads(row["value"])
        except:
            return row["value"]
    return default


def set_setting(key: str, value):
    """Set a setting value."""
    conn = get_connection()
    cursor = conn.cursor()
    value_str = json.dumps(value) if not isinstance(value, str) else value
    cursor.execute(
        "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
        (key, value_str, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )
    conn.commit()
    conn.close()


def get_all_settings():
    """Get all settings."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT key, value FROM settings")
    settings = {}
    for row in cursor.fetchall():
        try:
            settings[row["key"]] = json.loads(row["value"])
        except:
            settings[row["key"]] = row["value"]
    conn.close()
    return settings
