"""
TeleDrive Authentication System.
Handles user registration, login, sessions, and role management.
Supports email/password and Google OAuth.
"""
import sqlite3
import hashlib
import secrets
import json
import bcrypt
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List


# ═══════════════════════════════════════════════════════════════
# ROLE & PERMISSION SYSTEM
# ═══════════════════════════════════════════════════════════════

ROLES = {
    "admin": {
        "display_name": "Administrator",
        "description": "Full access to everything, can manage users and roles",
        "color": "amber",
        "permissions": ["*"],
    },
    "editor": {
        "display_name": "Settings Editor",
        "description": "Can modify system settings, credentials, and scheduler",
        "color": "blue",
        "permissions": [
            "view_dashboard", "run_scraper", "manage_profiles",
            "manage_settings", "manage_scheduler", "view_audit",
            "test_connections", "manage_exports",
        ],
    },
    "tester": {
        "display_name": "Tester",
        "description": "Can test connections and run scrapers, but not modify settings",
        "color": "purple",
        "permissions": [
            "view_dashboard", "run_scraper", "manage_profiles",
            "test_connections", "view_audit", "manage_exports",
        ],
    },
    "user": {
        "display_name": "Normal User",
        "description": "Basic access to own scraping and profiles",
        "color": "slate",
        "permissions": [
            "view_dashboard", "run_scraper", "manage_profiles",
            "manage_exports",
        ],
    },
}


def has_permission(user: dict, permission: str) -> bool:
    """Check if a user has a specific permission based on their role."""
    role = user.get("role", "user")
    role_def = ROLES.get(role, ROLES["user"])
    perms = role_def["permissions"]
    return "*" in perms or permission in perms


def get_available_roles() -> list:
    """Get all available roles with their metadata."""
    return [
        {
            "name": name,
            "display_name": data["display_name"],
            "description": data["description"],
            "color": data["color"],
            "permissions": data["permissions"],
        }
        for name, data in ROLES.items()
    ]

DB_PATH = Path(__file__).parent.parent / "data" / "teledrive.db"


def get_connection():
    """Get database connection."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_auth_db():
    """Initialize authentication database tables."""
    conn = get_connection()
    cursor = conn.cursor()

    # Users table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT,
            name TEXT NOT NULL,
            avatar_url TEXT,
            role TEXT NOT NULL DEFAULT 'user',
            auth_provider TEXT DEFAULT 'email',
            google_id TEXT UNIQUE,
            is_active INTEGER DEFAULT 1,
            subscription_plan TEXT DEFAULT 'free',
            subscription_expires_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_login_at TEXT
        )
    """)

    # User credentials (per-user Telegram/Google Sheets/etc.)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            credential_type TEXT NOT NULL,
            credential_key TEXT NOT NULL,
            credential_value TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(user_id, credential_type, credential_key),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
    """)

    # Subscription plans
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS subscription_plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            description TEXT,
            price REAL DEFAULT 0,
            currency TEXT DEFAULT 'USD',
            interval TEXT DEFAULT 'monthly',
            max_profiles INTEGER DEFAULT 3,
            max_processes INTEGER DEFAULT 5,
            max_messages_per_day INTEGER DEFAULT 10000,
            can_export_excel INTEGER DEFAULT 0,
            can_push_sheets INTEGER DEFAULT 0,
            can_schedule INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            sort_order INTEGER DEFAULT 0
        )
    """)

    # System settings (admin-controlled)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS system_settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT
        )
    """)

    # Sessions table for persistent login
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            token TEXT NOT NULL UNIQUE,
            expires_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
    """)

    conn.commit()

    # Migrations: add new columns if they don't exist
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'active'")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN security_question TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN security_answer_hash TEXT")
    except sqlite3.OperationalError:
        pass
    conn.commit()

    # Seed default subscription plans if none exist
    existing = cursor.execute("SELECT COUNT(*) as c FROM subscription_plans").fetchone()["c"]
    if existing == 0:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        plans = [
            ("free", "Free", "Get started with basic scraping", 0, "USD", "monthly",
             3, 3, 5000, 0, 0, 0, 1, 0),
            ("pro", "Pro", "For power users who need more", 9.99, "USD", "monthly",
             20, 50, 100000, 1, 1, 1, 1, 1),
            ("enterprise", "Enterprise", "Unlimited everything for teams", 29.99, "USD", "monthly",
             -1, -1, -1, 1, 1, 1, 1, 2),
        ]
        cursor.executemany("""
            INSERT INTO subscription_plans 
            (name, display_name, description, price, currency, interval,
             max_profiles, max_processes, max_messages_per_day,
             can_export_excel, can_push_sheets, can_schedule, is_active, sort_order)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, plans)
        conn.commit()

    # Seed default system settings
    defaults = {
        "subscriptions_enabled": "false",
        "registration_enabled": "true",
        "google_oauth_enabled": "false",
        "google_oauth_client_id": "",
        "google_oauth_client_secret": "",
        "app_name": "TeleDrive",
        "app_tagline": "Telegram Channel Intelligence Platform",
        # Theme colors
        "theme_primary": "#6366f1",
        "theme_primary_hover": "#4f46e5",
        "theme_secondary": "#8b5cf6",
        "theme_bg": "#0b0f1a",
        "theme_bg_sidebar": "#0f1629",
        "theme_bg_card": "#1e293b",
        "theme_bg_input": "#0f172a",
        "theme_border": "#334155",
        "theme_text": "#e2e8f0",
        "theme_text_muted": "#94a3b8",
        "theme_success": "#22c55e",
        "theme_danger": "#ef4444",
        "theme_warning": "#f59e0b",
        "allow_user_themes": "false",
        "require_approval": "false",
    }
    for key, value in defaults.items():
        cursor.execute("""
            INSERT OR IGNORE INTO system_settings (key, value, updated_at) 
            VALUES (?, ?, ?)
        """, (key, value, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit()

    # Create default admin if no users exist
    user_count = cursor.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"]
    if user_count == 0:
        create_user("admin@teledrive.app", "admin123", "Admin", role="admin")

    conn.close()


# ═══════════════════════════════════════════════════════════════
# PASSWORD HELPERS
# ═══════════════════════════════════════════════════════════════

def hash_password(password: str) -> str:
    """Hash password using bcrypt."""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, password_hash: str) -> bool:
    """Verify password against hash. Supports both bcrypt and legacy SHA-256."""
    try:
        if password_hash.startswith("$2b$") or password_hash.startswith("$2a$"):
            return bcrypt.checkpw(password.encode(), password_hash.encode())
        # Legacy SHA-256 format: salt:hash
        salt, hashed = password_hash.split(":")
        return hashlib.sha256((salt + password).encode()).hexdigest() == hashed
    except (ValueError, AttributeError):
        return False


def _needs_rehash(password_hash: str) -> bool:
    """Check if a password hash needs to be upgraded to bcrypt."""
    return not (password_hash.startswith("$2b$") or password_hash.startswith("$2a$"))


# ═══════════════════════════════════════════════════════════════
# USER CRUD
# ═══════════════════════════════════════════════════════════════

def create_user(email: str, password: str = None, name: str = "",
                role: str = "user", auth_provider: str = "email",
                google_id: str = None, avatar_url: str = None,
                status: str = "active",
                security_question: str = None, security_answer: str = None) -> Dict:
    """Create a new user."""
    if role not in ROLES:
        return {"success": False, "error": f"Invalid role: {role}"}
    conn = get_connection()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    password_hash = hash_password(password) if password else None
    answer_hash = hash_password(security_answer.lower().strip()) if security_answer else None
    try:
        conn.execute("""
            INSERT INTO users (email, password_hash, name, avatar_url, role,
                             auth_provider, google_id, status,
                             security_question, security_answer_hash,
                             created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (email.lower().strip(), password_hash, name, avatar_url,
              role, auth_provider, google_id, status,
              security_question, answer_hash, now, now))
        conn.commit()
        user_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()
        return {"success": True, "id": user_id}
    except sqlite3.IntegrityError:
        conn.close()
        return {"success": False, "error": "Email already registered"}


def authenticate_user(email: str, password: str) -> Optional[Dict]:
    """Authenticate user with email/password. Returns user dict, {"pending": True} if pending, or None."""
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM users WHERE email = ? AND is_active = 1",
        (email.lower().strip(),)
    ).fetchone()
    conn.close()

    if not row:
        return None
    if not row["password_hash"]:
        return None  # Google-only account
    if not verify_password(password, row["password_hash"]):
        return None

    # Check user status
    status = row["status"] if "status" in row.keys() else "active"
    if status == "pending":
        return {"pending": True}
    if status == "rejected":
        return None

    # Auto-rehash legacy SHA-256 passwords to bcrypt on successful login
    if _needs_rehash(row["password_hash"]):
        new_hash = hash_password(password)
        conn2 = get_connection()
        conn2.execute("UPDATE users SET password_hash = ? WHERE id = ?", (new_hash, row["id"]))
        conn2.commit()
        conn2.close()

    # Update last login
    update_user(row["id"], last_login_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return dict(row)


def authenticate_google(google_id: str, email: str, name: str,
                        avatar_url: str = None) -> Optional[Dict]:
    """Authenticate or create user via Google OAuth."""
    conn = get_connection()
    # Check if user exists by google_id
    row = conn.execute(
        "SELECT * FROM users WHERE google_id = ? AND is_active = 1",
        (google_id,)
    ).fetchone()

    if row:
        conn.close()
        update_user(row["id"], last_login_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return dict(row)

    # Check if email exists (link accounts)
    row = conn.execute(
        "SELECT * FROM users WHERE email = ? AND is_active = 1",
        (email.lower().strip(),)
    ).fetchone()

    if row:
        conn.execute("UPDATE users SET google_id = ?, avatar_url = COALESCE(?, avatar_url) WHERE id = ?",
                      (google_id, avatar_url, row["id"]))
        conn.commit()
        conn.close()
        update_user(row["id"], last_login_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return dict(row)

    # Create new user
    conn.close()
    result = create_user(email, None, name, "user", "google", google_id, avatar_url)
    if result["success"]:
        return get_user(result["id"])
    return None


def get_user(user_id: int) -> Optional[Dict]:
    """Get user by ID."""
    conn = get_connection()
    row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    conn.close()
    if row:
        user = dict(row)
        user.pop("password_hash", None)
        return user
    return None


def get_user_by_email(email: str) -> Optional[Dict]:
    """Get user by email."""
    conn = get_connection()
    row = conn.execute("SELECT * FROM users WHERE email = ?", (email.lower().strip(),)).fetchone()
    conn.close()
    if row:
        user = dict(row)
        user.pop("password_hash", None)
        return user
    return None


def get_all_users() -> List[Dict]:
    """Get all users (admin function)."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT id, email, name, avatar_url, role, auth_provider, is_active,
               COALESCE(status, 'active') as status,
               subscription_plan, subscription_expires_at, created_at, last_login_at
        FROM users ORDER BY created_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_user(user_id: int, **kwargs) -> Dict:
    """Update user fields."""
    conn = get_connection()
    allowed = {"name", "avatar_url", "role", "is_active", "status", "subscription_plan",
               "subscription_expires_at", "last_login_at", "email"}
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
    values.append(user_id)
    conn.execute(f"UPDATE users SET {', '.join(updates)} WHERE id = ?", values)
    conn.commit()
    conn.close()
    return {"success": True}


def change_password(user_id: int, new_password: str) -> Dict:
    """Change user password."""
    conn = get_connection()
    password_hash = hash_password(new_password)
    conn.execute("UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?",
                 (password_hash, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user_id))
    conn.commit()
    conn.close()
    return {"success": True}


def delete_user(user_id: int) -> Dict:
    """Delete a user and all their data."""
    conn = get_connection()
    conn.execute("DELETE FROM user_credentials WHERE user_id = ?", (user_id,))
    conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
    conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()
    return {"success": True}


def get_user_count() -> int:
    """Get total user count."""
    conn = get_connection()
    count = conn.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"]
    conn.close()
    return count


# ═══════════════════════════════════════════════════════════════
# USER CREDENTIALS (per-user API keys, etc.)
# ═══════════════════════════════════════════════════════════════

def set_user_credential(user_id: int, cred_type: str, key: str, value: str):
    """Set a user credential."""
    conn = get_connection()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("""
        INSERT INTO user_credentials (user_id, credential_type, credential_key, credential_value, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id, credential_type, credential_key) 
        DO UPDATE SET credential_value = excluded.credential_value, updated_at = excluded.updated_at
    """, (user_id, cred_type, key, value, now, now))
    conn.commit()
    conn.close()


def get_user_credential(user_id: int, cred_type: str, key: str, default: str = "") -> str:
    """Get a user credential value."""
    conn = get_connection()
    row = conn.execute(
        "SELECT credential_value FROM user_credentials WHERE user_id = ? AND credential_type = ? AND credential_key = ?",
        (user_id, cred_type, key)
    ).fetchone()
    conn.close()
    return row["credential_value"] if row else default


def get_user_credentials(user_id: int, cred_type: str = None) -> Dict:
    """Get all credentials for a user, optionally filtered by type."""
    conn = get_connection()
    if cred_type:
        rows = conn.execute(
            "SELECT credential_key, credential_value FROM user_credentials WHERE user_id = ? AND credential_type = ?",
            (user_id, cred_type)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT credential_type, credential_key, credential_value FROM user_credentials WHERE user_id = ?",
            (user_id,)
        ).fetchall()
    conn.close()
    result = {}
    for r in rows:
        if cred_type:
            result[r["credential_key"]] = r["credential_value"]
        else:
            key = f"{r['credential_type']}.{r['credential_key']}"
            result[key] = r["credential_value"]
    return result


def delete_user_credentials(user_id: int, cred_type: str = None):
    """Delete user credentials."""
    conn = get_connection()
    if cred_type:
        conn.execute("DELETE FROM user_credentials WHERE user_id = ? AND credential_type = ?",
                      (user_id, cred_type))
    else:
        conn.execute("DELETE FROM user_credentials WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════
# SESSION MANAGEMENT
# ═══════════════════════════════════════════════════════════════

def create_session(user_id: int, days: int = 30) -> str:
    """Create a login session token."""
    conn = get_connection()
    token = secrets.token_urlsafe(48)
    now = datetime.now()
    expires = now + timedelta(days=days)
    conn.execute("""
        INSERT INTO sessions (user_id, token, expires_at, created_at)
        VALUES (?, ?, ?, ?)
    """, (user_id, token, expires.strftime("%Y-%m-%d %H:%M:%S"),
          now.strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit()
    conn.close()
    return token


def validate_session(token: str) -> Optional[Dict]:
    """Validate session token and return user if valid."""
    conn = get_connection()
    row = conn.execute("""
        SELECT s.*, u.id as uid, u.email, u.name, u.role, u.is_active,
               u.avatar_url, u.subscription_plan, u.auth_provider
        FROM sessions s
        JOIN users u ON s.user_id = u.id
        WHERE s.token = ? AND u.is_active = 1 AND COALESCE(u.status, 'active') = 'active'
    """, (token,)).fetchone()
    conn.close()

    if not row:
        return None

    # Check expiry
    expires = datetime.strptime(row["expires_at"], "%Y-%m-%d %H:%M:%S")
    if datetime.now() > expires:
        invalidate_session(token)
        return None

    return {
        "id": row["uid"],
        "email": row["email"],
        "name": row["name"],
        "role": row["role"],
        "avatar_url": row["avatar_url"],
        "subscription_plan": row["subscription_plan"],
        "auth_provider": row["auth_provider"],
    }


def invalidate_session(token: str):
    """Delete a session."""
    conn = get_connection()
    conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
    conn.commit()
    conn.close()


def cleanup_expired_sessions():
    """Remove all expired sessions."""
    conn = get_connection()
    conn.execute("DELETE FROM sessions WHERE expires_at < ?",
                 (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════
# SYSTEM SETTINGS
# ═══════════════════════════════════════════════════════════════

def get_system_setting(key: str, default: str = "") -> str:
    """Get a system setting."""
    conn = get_connection()
    row = conn.execute("SELECT value FROM system_settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def set_system_setting(key: str, value: str):
    """Set a system setting."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO system_settings (key, value, updated_at) VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
    """, (key, value, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit()
    conn.close()


def get_all_system_settings() -> Dict:
    """Get all system settings."""
    conn = get_connection()
    rows = conn.execute("SELECT key, value FROM system_settings").fetchall()
    conn.close()
    return {r["key"]: r["value"] for r in rows}


# ═══════════════════════════════════════════════════════════════
# SUBSCRIPTION PLANS
# ═══════════════════════════════════════════════════════════════

def get_all_plans() -> List[Dict]:
    """Get all subscription plans."""
    conn = get_connection()
    rows = conn.execute("SELECT * FROM subscription_plans ORDER BY sort_order").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_plan(plan_name: str) -> Optional[Dict]:
    """Get a plan by name."""
    conn = get_connection()
    row = conn.execute("SELECT * FROM subscription_plans WHERE name = ?", (plan_name,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_user_plan_limits(user_id: int) -> Dict:
    """Get the effective limits for a user based on their subscription."""
    user = get_user(user_id)
    if not user:
        return {}

    # If subscriptions are disabled, give everyone pro-level access
    subs_enabled = get_system_setting("subscriptions_enabled", "false") == "true"
    if not subs_enabled:
        return {
            "max_profiles": -1,
            "max_processes": -1,
            "max_messages_per_day": -1,
            "can_export_excel": True,
            "can_push_sheets": True,
            "can_schedule": True,
        }

    # Admin always gets unlimited
    if user["role"] == "admin":
        return {
            "max_profiles": -1,
            "max_processes": -1,
            "max_messages_per_day": -1,
            "can_export_excel": True,
            "can_push_sheets": True,
            "can_schedule": True,
        }

    plan = get_plan(user.get("subscription_plan", "free"))
    if not plan:
        plan = get_plan("free")

    return {
        "max_profiles": plan["max_profiles"],
        "max_processes": plan["max_processes"],
        "max_messages_per_day": plan["max_messages_per_day"],
        "can_export_excel": bool(plan["can_export_excel"]),
        "can_push_sheets": bool(plan["can_push_sheets"]),
        "can_schedule": bool(plan["can_schedule"]),
    }


# ═══════════════════════════════════════════════════════════════
# THEME SYSTEM
# ═══════════════════════════════════════════════════════════════

DEFAULT_THEME = {
    "primary": "#6366f1",
    "primary_hover": "#4f46e5",
    "secondary": "#8b5cf6",
    "bg": "#0b0f1a",
    "bg_sidebar": "#0f1629",
    "bg_card": "#1e293b",
    "bg_input": "#0f172a",
    "border": "#334155",
    "text": "#e2e8f0",
    "text_muted": "#94a3b8",
    "success": "#22c55e",
    "danger": "#ef4444",
    "warning": "#f59e0b",
}

PRESET_THEMES = {
    "indigo_night": {
        "display_name": "Indigo Night",
        "description": "Default dark theme with indigo and purple accents",
        "theme": {**DEFAULT_THEME},
    },
    "ocean_blue": {
        "display_name": "Ocean Blue",
        "description": "Cool blue tones with teal accents",
        "theme": {
            "primary": "#0ea5e9", "primary_hover": "#0284c7", "secondary": "#06b6d4",
            "bg": "#0c1222", "bg_sidebar": "#0e1a2e", "bg_card": "#172554",
            "bg_input": "#0c1222", "border": "#1e3a5f", "text": "#e0f2fe",
            "text_muted": "#7dd3fc", "success": "#22c55e", "danger": "#ef4444", "warning": "#f59e0b",
        },
    },
    "emerald_dark": {
        "display_name": "Emerald Dark",
        "description": "Rich green tones for a natural feel",
        "theme": {
            "primary": "#10b981", "primary_hover": "#059669", "secondary": "#34d399",
            "bg": "#0a1410", "bg_sidebar": "#0c1a14", "bg_card": "#14332a",
            "bg_input": "#0a1410", "border": "#1a4d3e", "text": "#d1fae5",
            "text_muted": "#6ee7b7", "success": "#22c55e", "danger": "#ef4444", "warning": "#f59e0b",
        },
    },
    "rose_twilight": {
        "display_name": "Rose Twilight",
        "description": "Warm rose and pink tones with a dark backdrop",
        "theme": {
            "primary": "#f43f5e", "primary_hover": "#e11d48", "secondary": "#fb7185",
            "bg": "#180a10", "bg_sidebar": "#1f0c16", "bg_card": "#3b1529",
            "bg_input": "#180a10", "border": "#5c1e3e", "text": "#fce7f3",
            "text_muted": "#f9a8d4", "success": "#22c55e", "danger": "#ef4444", "warning": "#f59e0b",
        },
    },
}

SECURITY_QUESTIONS = [
    "What is your pet's name?",
    "What city were you born in?",
    "What is your mother's maiden name?",
    "What was the name of your first school?",
    "What is your favorite movie?",
    "What was your childhood nickname?",
]


def get_site_theme() -> Dict:
    """Get the global site theme from system settings."""
    settings = get_all_system_settings()
    theme = {}
    for key, default in DEFAULT_THEME.items():
        theme[key] = settings.get(f"theme_{key}", default)
    return theme


def get_user_theme(user_id: int) -> Optional[Dict]:
    """Get a user's custom theme, or None if not set."""
    creds = get_user_credentials(user_id, "theme")
    if not creds:
        return None
    theme = {}
    for key in DEFAULT_THEME:
        if key in creds and creds[key]:
            theme[key] = creds[key]
    return theme if theme else None


def set_user_theme(user_id: int, theme: Dict):
    """Save a user's custom theme."""
    for key, value in theme.items():
        if key in DEFAULT_THEME and value:
            set_user_credential(user_id, "theme", key, value)


def clear_user_theme(user_id: int):
    """Remove a user's custom theme."""
    delete_user_credentials(user_id, "theme")


def get_effective_theme(user_id: int = None) -> Dict:
    """Get the effective theme: user theme (if allowed and set) or site theme."""
    site_theme = get_site_theme()
    if user_id:
        allow = get_system_setting("allow_user_themes", "false") == "true"
        # Also check per-user flag
        user_allowed = get_user_credential(user_id, "theme", "_enabled", "false") == "true"
        if allow and user_allowed:
            user_theme = get_user_theme(user_id)
            if user_theme:
                merged = {**site_theme, **user_theme}
                return merged
    return site_theme


def theme_to_css(theme: Dict) -> str:
    """Convert a theme dict to CSS custom properties string."""
    lines = []
    for key, value in theme.items():
        css_var = f"--td-{key.replace('_', '-')}"
        lines.append(f"{css_var}: {value};")
    # Generate computed colors (alpha variants)
    p = theme.get("primary", "#6366f1")
    lines.append(f"--td-primary-rgb: {_hex_to_rgb(p)};")
    s = theme.get("secondary", "#8b5cf6")
    lines.append(f"--td-secondary-rgb: {_hex_to_rgb(s)};")
    return "\n".join(lines)


def _hex_to_rgb(hex_color: str) -> str:
    """Convert #RRGGBB to 'R, G, B' string."""
    h = hex_color.lstrip('#')
    if len(h) == 3:
        h = ''.join(c*2 for c in h)
    try:
        return f"{int(h[0:2], 16)}, {int(h[2:4], 16)}, {int(h[4:6], 16)}"
    except (ValueError, IndexError):
        return "99, 102, 241"


# ═══════════════════════════════════════════════════════════════
# PENDING USER APPROVAL
# ═══════════════════════════════════════════════════════════════

def get_pending_users() -> List[Dict]:
    """Get all users with pending status."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT id, email, name, role, created_at
        FROM users WHERE COALESCE(status, 'active') = 'pending'
        ORDER BY created_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def approve_user(user_id: int) -> Dict:
    """Approve a pending user."""
    return update_user(user_id, status="active")


def reject_user(user_id: int) -> Dict:
    """Reject a pending user."""
    return update_user(user_id, status="rejected")


# ═══════════════════════════════════════════════════════════════
# SECURITY QUESTION / FORGOT PASSWORD
# ═══════════════════════════════════════════════════════════════

def set_security_question(user_id: int, question: str, answer: str):
    """Set user's security question and hashed answer."""
    conn = get_connection()
    answer_hash = hash_password(answer.lower().strip())
    conn.execute(
        "UPDATE users SET security_question = ?, security_answer_hash = ?, updated_at = ? WHERE id = ?",
        (question, answer_hash, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user_id)
    )
    conn.commit()
    conn.close()


def get_security_question(email: str) -> Optional[str]:
    """Get the security question for a user by email. Returns None if not set."""
    conn = get_connection()
    row = conn.execute(
        "SELECT security_question FROM users WHERE email = ? AND is_active = 1",
        (email.lower().strip(),)
    ).fetchone()
    conn.close()
    if row and row["security_question"]:
        return row["security_question"]
    return None


def verify_security_answer(email: str, answer: str) -> bool:
    """Verify a security answer for a user."""
    conn = get_connection()
    row = conn.execute(
        "SELECT security_answer_hash FROM users WHERE email = ? AND is_active = 1",
        (email.lower().strip(),)
    ).fetchone()
    conn.close()
    if not row or not row["security_answer_hash"]:
        return False
    return verify_password(answer.lower().strip(), row["security_answer_hash"])


def reset_password_with_security(email: str, answer: str, new_password: str) -> Dict:
    """Reset password after verifying security answer."""
    if not verify_security_answer(email, answer):
        return {"success": False, "error": "Incorrect security answer"}
    conn = get_connection()
    row = conn.execute("SELECT id FROM users WHERE email = ? AND is_active = 1",
                       (email.lower().strip(),)).fetchone()
    conn.close()
    if not row:
        return {"success": False, "error": "User not found"}
    return change_password(row["id"], new_password)


def get_preset_themes() -> Dict:
    """Get all preset themes."""
    return PRESET_THEMES


# Initialize on import
init_auth_db()
