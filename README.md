# Telegram Daily Scraper → Google Sheets

يسحب بيانات قنوات تلغرام يومياً ويحطها بـ Google Sheets — يشتغل تلقائياً حتى لو جهازك مطفي عبر GitHub Actions.

## Project Structure

```
telegram-scraper/
├── .github/workflows/daily-scrape.yml   # GitHub Actions cron (daily 06:00 UTC)
├── src/
│   ├── config.py      # Settings & channel loader
│   ├── scraper.py     # Telethon scraper
│   └── sheets.py      # Google Sheets writer
├── channels.json      # Channel list (edit this to add/remove channels)
├── main.py            # Entry point
├── auth_session.py    # One-time local auth script
├── requirements.txt
└── .env.example
```

## Setup (خطوة بخطوة)

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

### 3. Local Authentication (مرة واحدة)

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

### 4. GitHub Setup

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
- **hours_back**: How many hours of messages to fetch (overrides default_hours)

## Running

### Manual (local)
```bash
python main.py
```

### Automated (GitHub Actions)
- Runs daily at **06:00 UTC** (09:00 UTC+3) automatically
- Trigger manually: GitHub repo → Actions tab → "Daily Telegram Scraper" → Run workflow

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
