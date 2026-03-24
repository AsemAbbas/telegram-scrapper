 # Telegram Daily Scraper → Google Sheets

يسحب بيانات قنوات تلغرام يومياً ويحطها بـ Google Sheets — يشتغل تلقائياً حتى لو جهازك مطفي عبر GitHub Actions.

---

## 📋 Table of Contents

- [How It Works](#how-it-works)
- [Features](#features)
- [Project Structure](#project-structure)
- [Setup Guide](#setup-guide)
- [Running the Scraper](#running-the-scraper)
- [Export Options](#export-options)
- [⚠️ Important Warnings & Limitations](#️-important-warnings--limitations)
- [Troubleshooting](#troubleshooting)

---

## How It Works

1. **Connects to Telegram** using the Telethon library with your API credentials
2. **Fetches messages** from configured channels within a specified time range
3. **Processes data** extracting text, views, reactions, media, URLs, hashtags, etc.
4. **Exports data** to Google Sheets and/or local files (CSV, JSON, Excel)
5. **Runs automatically** via GitHub Actions on a schedule, or manually via the Web GUI

### Data Flow
```
Telegram Channels → Telethon API → Process Messages → Export to:
                                                      ├── Google Sheets
                                                      ├── Local CSV/JSON/Excel
                                                      └── SQLite Backup (automatic)
```

---

## Features

- **🌐 Web GUI** - Beautiful dark-themed interface at `http://localhost:5000`
- **📊 Google Sheets Export** - Push data directly to Google Sheets
- **💾 Local File Export** - Save to CSV, JSON, or Excel (.xlsx)
- **📁 Custom Save Locations** - Save to Desktop, Documents, or any custom folder
- **⏰ Scheduler** - Set up automatic scraping at specific times
- **📋 Audit History** - Track all actions with timestamps
- **🛑 Kill Switch** - Stop running scrapes immediately
- **🔄 Reset Settings** - Restore defaults while keeping channels
- **✅ Enable/Disable Toggle** - Prevent all scraping when disabled
- **💾 Auto-Backup** - SQLite backup of all scraped data (protects against Google Sheets failures)

---

## Project Structure

```
telegram-scraper/
├── .github/workflows/daily-scrape.yml   # GitHub Actions cron (daily 06:00 UTC)
├── src/
│   ├── config.py      # Settings & channel loader
│   ├── scraper.py     # Telethon scraper
│   ├── sheets.py      # Google Sheets writer
│   ├── local_db.py    # SQLite backup & audit log
│   └── local_export.py # CSV/JSON/Excel export
├── templates/
│   └── index.html     # Web GUI template
├── data/
│   └── scraper.db     # SQLite database (auto-created)
├── exports/           # Local export files (auto-created)
├── channels.json      # Channel list configuration
├── web_app.py         # Flask web GUI server
├── main.py            # CLI entry point
├── auth_session.py    # One-time local auth script
├── requirements.txt
└── .env.example
```

---

## Setup Guide

### 1. Telegram API Credentials

1. Go to https://my.telegram.org
2. Log in → API Development Tools
3. Create an app → get **API ID** and **API Hash**

### 2. Google Service Account

1. Go to https://console.cloud.google.com
2. Create a project (or use existing)
3. Enable **Google Sheets API**
4. Create a **Service Account** → download the JSON key
5. Create a blank Google Sheet
6. Share it with the service account email (e.g. `scraper@project.iam.gserviceaccount.com`)
7. Copy the Sheet ID from the URL: `https://docs.google.com/spreadsheets/d/SHEET_ID_HERE/edit`

### 3. Local Authentication (one-time)

```bash
# Install dependencies
pip install -r requirements.txt

# Copy env template
cp .env.example .env
# Fill in TG_API_ID, TG_API_HASH, TG_PHONE, GOOGLE_CREDS_JSON, SHEET_ID

# Authenticate with Telegram (enter the code they send you)
python auth_session.py

# Base64 encode the session for GitHub
python -c "import base64; print(base64.b64encode(open('scraper_session.session','rb').read()).decode())"
```

### 4. GitHub Setup (for automated scraping)

1. Create a new GitHub repo
2. Push this project to it
3. Go to **Settings → Secrets and variables → Actions**
4. Add these secrets:

| Secret | Value |
|--------|-------|
| `TG_API_ID` | Your Telegram API ID |
| `TG_API_HASH` | Your Telegram API Hash |
| `TG_PHONE` | Your phone number (+...) |
| `TG_SESSION` | Base64-encoded session file |
| `GOOGLE_CREDS` | Base64-encoded service-account.json |
| `SHEET_ID` | Your Google Sheet ID |

To base64 encode the Google creds:
```bash
python -c "import base64; print(base64.b64encode(open('service-account.json','rb').read()).decode())"
```

### 5. Configure Channels

Edit `channels.json`:
```json
{
  "default_hours": 24,
  "channels": [
    {"name": "DarbMirror", "hours_back": 24},
    {"name": "AnotherChannel", "hours_back": 48}
  ]
}
```

- **name**: Channel username (without @)
- **hours_back**: How many hours of messages to fetch

---

## Running the Scraper

### Web GUI (Recommended)
```bash
python web_app.py
# Open http://localhost:5000
```

### Command Line
```bash
python main.py
```

### GitHub Actions (Automated)
- Runs daily at **06:00 UTC** (09:00 UTC+3) automatically
- Trigger manually: GitHub repo → Actions tab → "Daily Telegram Scraper" → Run workflow

---

## Export Options

### Google Sheets
- Data is pushed directly to your configured Google Sheet
- Each channel gets its own tab
- Duplicate detection prevents re-adding same messages

### Local File Export
| Format | Extension | Best For |
|--------|-----------|----------|
| CSV | `.csv` | Excel, data analysis, large datasets |
| JSON | `.json` | Programming, APIs, nested data |
| Excel | `.xlsx` | Google Drive, sharing, formatted viewing |

### Save Locations
- **Project Folder** - `./exports/` directory
- **Desktop** - Your Desktop folder
- **Documents** - Your Documents folder
- **Custom Path** - Any folder you specify

---

## ⚠️ Important Warnings & Limitations

### 🔴 Telegram Rate Limits & Account Risks

**Telegram has strict rate limits. Scraping too aggressively can result in:**

| Risk Level | Action | Consequence |
|------------|--------|-------------|
| 🟡 Low | Scraping a few channels, <1000 messages | Safe, no issues |
| 🟠 Medium | Scraping 10+ channels, 10K-50K messages | Temporary slowdowns, FloodWait errors |
| 🔴 High | Scraping 50+ channels, 100K+ messages | Account restrictions, temporary bans |
| ⛔ Critical | Scraping 1M+ messages rapidly | **Permanent account ban possible** |

**Best Practices:**
- ✅ Scrape during off-peak hours
- ✅ Use reasonable `hours_back` values (24-72 hours)
- ✅ Add delays between channels (built-in)
- ✅ Don't scrape the same channel repeatedly
- ❌ Don't scrape 1M+ messages in one session
- ❌ Don't run multiple instances simultaneously
- ❌ Don't use your primary Telegram account

### 🔴 Google Sheets Limitations

| Limit | Value | What Happens |
|-------|-------|--------------|
| Max cells per sheet | 10,000,000 | Sheet becomes read-only |
| Max rows per sheet | ~5,000,000 | Cannot add more data |
| API quota (free) | 300 requests/min | Rate limiting, 429 errors |
| API quota (daily) | 500,000,000 cells read/written | Quota exceeded errors |

**For Large Datasets (>100K rows):**
- ✅ Use local file export (CSV/Excel) instead
- ✅ Create multiple sheets (one per month/week)
- ✅ Use the SQLite backup (automatic)
- ❌ Don't push 1M+ rows to a single sheet

### 🔴 Google Drive Limitations

| Limit | Value |
|-------|-------|
| Max file size | 5TB (but 50MB for Sheets conversion) |
| Storage (free) | 15GB shared across Gmail, Drive, Photos |
| Upload limit | 750GB/day |

**If uploading large Excel files to Drive:**
- Files >50MB won't convert to Google Sheets format
- They'll remain as .xlsx files (still viewable)

### 🔴 GitHub Actions Limitations

| Limit | Value | Risk |
|-------|-------|------|
| Job timeout | 6 hours max | Long scrapes may fail |
| Storage | 500MB artifacts | Large exports may fail |
| Minutes (free) | 2,000/month | May run out with frequent scrapes |
| Concurrent jobs | 20 | Not usually an issue |

**Will scraping 1M messages block my GitHub?**
- ❌ No, GitHub won't block your account
- ⚠️ But the job may timeout after 6 hours
- ⚠️ Large artifacts (>500MB) will fail to upload
- ✅ Use local scraping for massive datasets

### 🟢 Safe Limits Summary

| What | Safe Limit | Risky |
|------|------------|-------|
| Messages per scrape | <50,000 | >100,000 |
| Channels per scrape | <20 | >50 |
| Scrapes per day | 2-3 | >10 |
| Google Sheets rows | <500,000 | >1,000,000 |

---

## Troubleshooting

### FloodWaitError
```
telethon.errors.FloodWaitError: A wait of X seconds is required
```
**Solution:** Wait the specified time. Reduce scraping frequency.

### Google Sheets Quota Exceeded
```
APIError: Quota exceeded for quota metric 'Write requests'
```
**Solution:** Wait 1 minute, or use local file export instead.

### Session Expired
```
telethon.errors.AuthKeyUnregisteredError
```
**Solution:** Re-run `python auth_session.py` and update GitHub secrets.

### openpyxl Not Installed (Excel export)
```
openpyxl not installed. Run: pip install openpyxl
```
**Solution:** `pip install openpyxl`

---

## Google Sheet Output

Each channel gets its own tab. Columns:

| Column | Description |
|--------|-------------|
| scrape_date | When this scrape ran |
| channel | Channel title |
| msg_id | Message ID |
| date_utc / time_utc | Message date & time |
| text | Message text |
| views / forwards / replies_count | Engagement metrics |
| reactions | Reaction breakdown |
| media_type | photo/video/document/etc |
| urls / hashtags / mentions | Extracted entities |
| is_forward / fwd_from_name | Forward info |
| post_link | Direct link to message |

---

## License

MIT License - Use at your own risk. The authors are not responsible for any account bans or data loss.
