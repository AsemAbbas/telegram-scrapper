import os
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

# ═══════════════════════════════════════════════════════════════
# Telegram API
# ═══════════════════════════════════════════════════════════════
TG_API_ID = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH", "")
TG_PHONE = os.getenv("TG_PHONE", "")
TG_SESSION_NAME = os.getenv("TG_SESSION_NAME", "scraper_session")

# ═══════════════════════════════════════════════════════════════
# Google Sheets
# ═══════════════════════════════════════════════════════════════
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "service-account.json")
SHEET_ID = os.getenv("SHEET_ID", "")

# ═══════════════════════════════════════════════════════════════
# Paths
# ═══════════════════════════════════════════════════════════════
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CHANNELS_FILE = PROJECT_ROOT / "channels.json"


def load_channels():
    """Load channel list and config from channels.json."""
    with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    default_hours = data.get("default_hours", 24)
    channels = []

    for ch in data.get("channels", []):
        hours = ch.get("hours_back", default_hours)
        channels.append({
            "name": ch["name"],
            "hours_back": hours,
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
