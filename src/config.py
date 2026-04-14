import os
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

# ═══════════════════════════════════════════════════════════════
# Paths
# ═══════════════════════════════════════════════════════════════
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CHANNELS_FILE = PROJECT_ROOT / "channels.json"
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# Telegram API
# ═══════════════════════════════════════════════════════════════
TG_API_ID = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH", "")
TG_PHONE = os.getenv("TG_PHONE", "")

# Session name resolves to data/ directory so sessions aren't scattered in CWD
_raw_session_name = os.getenv("TG_SESSION_NAME", "scraper_session")
if os.path.isabs(_raw_session_name):
    TG_SESSION_NAME = _raw_session_name
else:
    TG_SESSION_NAME = str(DATA_DIR / _raw_session_name)

# ═══════════════════════════════════════════════════════════════
# Google Sheets
# ═══════════════════════════════════════════════════════════════
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "service-account.json")
SHEET_ID = os.getenv("SHEET_ID", "")


def get_session_path(session_name: str) -> str:
    """Return absolute path for a Telethon session file (without .session extension).
    If session_name is already absolute, return as-is. Otherwise resolve to data/ directory.
    """
    if os.path.isabs(session_name):
        return session_name
    return str(DATA_DIR / session_name)


def load_channels():
    """Load channel list and config from channels.json."""
    with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    default_hours = data.get("default_hours", 24)
    channels = []

    for ch in data.get("channels", []):
        hours = ch.get("hours_back", default_hours)
        use_date_range = ch.get("use_date_range", False)
        
        if use_date_range and ch.get("from_date_str") and ch.get("to_date_str"):
            # Parse custom date range
            from_date_str = ch["from_date_str"]
            to_date_str = ch["to_date_str"]
            
            # Parse datetime-local format (YYYY-MM-DDTHH:MM)
            from_date = datetime.strptime(from_date_str, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)
            to_date = datetime.strptime(to_date_str, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)
            
            channels.append({
                "name": ch["name"],
                "hours_back": hours,
                "use_date_range": True,
                "from_date_str": from_date_str,
                "to_date_str": to_date_str,
                "from_date": from_date,
                "to_date": to_date,
            })
        else:
            # Use hours_back mode
            channels.append({
                "name": ch["name"],
                "hours_back": hours,
                "use_date_range": False,
                "from_date_str": "",
                "to_date_str": "",
                "from_date": datetime.now(timezone.utc) - timedelta(hours=hours),
                "to_date": datetime.now(timezone.utc),
            })

    return channels


def get_sheet_columns():
    """Column order for Google Sheet output."""
    return [
        "scrape_date",
        "channel",
        "username",
        "msg_id",
        "date_utc",
        "time_utc",
        "text",
        "views",
        "forwards",
        "replies_count",
        "reactions",
        "reactions_sum",
        "media_type",
        "urls",
        "url_count",
        "hashtags",
        "mentions",
        "is_forward",
        "fwd_from_name",
        "post_link",
    ]
