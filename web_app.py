"""
Telegram Scraper Web GUI
A simple Flask web interface for the Telegram scraper.
"""
import asyncio
import json
import os
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from src.config import load_channels, TG_API_ID, TG_API_HASH, TG_PHONE, TG_SESSION_NAME, SHEET_ID
from src.sheets import push_to_sheets
from src.local_db import (
    log_audit, get_audit_log, clear_audit_log,
    save_messages_locally, get_local_message_count,
    get_setting, set_setting
)
from src.local_export import export_data, get_export_files, get_available_formats

from telethon import TelegramClient
from telethon.tl import types

app = Flask(__name__)
app.config['SECRET_KEY'] = 'telegram-scraper-secret'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Global state
scrape_status = {
    "running": False,
    "progress": 0,
    "current_channel": "",
    "messages": [],
    "results": [],
    "error": None,
    "kill_requested": False
}

# App enabled state (global kill switch)
app_enabled = True

# Scheduler state
scheduler = BackgroundScheduler()
scheduler_config = {
    "enabled": False,
    "time": "08:00",
    "interval_hours": None,  # None = daily at time, number = every X hours
    "last_run": None,
    "next_run": None
}
SCHEDULER_CONFIG_FILE = Path(__file__).parent / "scheduler_config.json"


def load_scheduler_config():
    """Load scheduler config from file."""
    global scheduler_config
    if SCHEDULER_CONFIG_FILE.exists():
        try:
            with open(SCHEDULER_CONFIG_FILE, 'r') as f:
                saved = json.load(f)
                scheduler_config.update(saved)
        except:
            pass
    return scheduler_config


def save_scheduler_config():
    """Save scheduler config to file."""
    with open(SCHEDULER_CONFIG_FILE, 'w') as f:
        json.dump(scheduler_config, f, indent=2)


def scheduled_scrape():
    """Run scraper on schedule."""
    global app_enabled
    
    if not app_enabled:
        print("[Scheduler] App is disabled, skipping scheduled scrape...")
        log_audit("scheduled_scrape_skipped", "App is disabled", "warning")
        return
    
    if scrape_status["running"]:
        print("[Scheduler] Scraper already running, skipping...")
        return
    
    print(f"[Scheduler] Starting scheduled scrape at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_audit("scheduled_scrape_started", f"Scheduled scrape initiated")
    scheduler_config["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save_scheduler_config()
    
    channels = load_channels()
    if channels:
        # Default export options for scheduled scrapes
        export_options = {"push_to_sheets": True}
        thread = threading.Thread(target=run_scraper_async, args=(channels, export_options))
        thread.daemon = True
        thread.start()


def setup_scheduler():
    """Setup the scheduler based on config."""
    global scheduler
    
    # Remove existing jobs
    scheduler.remove_all_jobs()
    
    if not scheduler_config["enabled"]:
        scheduler_config["next_run"] = None
        return
    
    if scheduler_config["interval_hours"]:
        # Interval mode: every X hours
        scheduler.add_job(
            scheduled_scrape,
            'interval',
            hours=scheduler_config["interval_hours"],
            id='scrape_job'
        )
        next_run = datetime.now() + timedelta(hours=scheduler_config["interval_hours"])
    else:
        # Daily mode: at specific time
        hour, minute = map(int, scheduler_config["time"].split(':'))
        scheduler.add_job(
            scheduled_scrape,
            CronTrigger(hour=hour, minute=minute),
            id='scrape_job'
        )
        # Calculate next run
        now = datetime.now()
        next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
    
    scheduler_config["next_run"] = next_run.strftime("%Y-%m-%d %H:%M:%S")
    save_scheduler_config()


def log_message(msg, msg_type="info"):
    """Send a log message to the frontend."""
    scrape_status["messages"].append({"type": msg_type, "text": msg, "time": datetime.now().strftime("%H:%M:%S")})
    socketio.emit('log', {"type": msg_type, "text": msg, "time": datetime.now().strftime("%H:%M:%S")})


async def scrape_channel_simple(client, channel_name, from_date, to_date, log_fn):
    """Simplified scraper that works within the web context."""
    # Check kill switch
    if scrape_status["kill_requested"]:
        log_fn("Kill switch activated - stopping", "warning")
        return []
    
    entity = await client.get_entity(channel_name)
    username = getattr(entity, "username", None) or str(channel_name)
    title = getattr(entity, "title", username)
    
    log_fn(f"Channel: {title} (@{username})")
    
    all_messages = []
    offset_id = 0
    batch_num = 0
    BATCH_SIZE = 200
    
    while True:
        # Check kill switch on each batch
        if scrape_status["kill_requested"]:
            log_fn("Kill switch activated - stopping mid-scrape", "warning")
            break
        
        batch_num += 1
        try:
            kwargs = {"limit": BATCH_SIZE}
            if offset_id == 0:
                kwargs["offset_date"] = to_date
            else:
                kwargs["offset_id"] = offset_id
            msgs = await client.get_messages(entity, **kwargs)
        except Exception as e:
            log_fn(f"Batch error: {e}, retrying...", "warning")
            await asyncio.sleep(3)
            continue
        
        if not msgs:
            break
        
        done = False
        for m in msgs:
            if m.date.astimezone(timezone.utc) < from_date:
                done = True
                break
            all_messages.append(m)
        
        offset_id = msgs[-1].id
        if batch_num % 3 == 0:
            log_fn(f"  Fetched {len(all_messages)} messages so far...")
        if done:
            break
        await asyncio.sleep(0.3)
    
    log_fn(f"  Total: {len(all_messages)} messages")
    
    # Process messages into rows
    scrape_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    rows = []
    
    for msg in all_messages:
        dt_utc = msg.date.astimezone(timezone.utc)
        
        # Simple reactions summary
        reactions_str = ""
        reactions_sum = 0
        if hasattr(msg, 'reactions') and msg.reactions and hasattr(msg.reactions, 'results'):
            parts = []
            for rc in msg.reactions.results:
                count = getattr(rc, "count", 0) or 0
                reactions_sum += count
                r = getattr(rc, "reaction", None)
                if isinstance(r, types.ReactionEmoji):
                    parts.append(f"{r.emoticon}:{count}")
                else:
                    parts.append(f"[r]:{count}")
            reactions_str = ", ".join(parts)
        
        # Extract URLs, mentions, hashtags
        urls, mentions, hashtags = [], [], []
        for e in (getattr(msg, "entities", None) or []):
            text = msg.text or ""
            fragment = text[e.offset:e.offset + e.length] if text else ""
            etype = type(e).__name__
            if "Url" in etype:
                urls.append(getattr(e, 'url', fragment))
            elif "Mention" in etype:
                mentions.append(fragment)
            elif "Hashtag" in etype:
                hashtags.append(fragment)
        
        # Media type
        media = getattr(msg, "media", None)
        mtype = ""
        if media:
            mtype = type(media).__name__.replace("MessageMedia", "").lower()
        
        # Forward info
        fwd = getattr(msg, "fwd_from", None)
        is_forward = bool(fwd)
        fwd_from_name = getattr(fwd, "from_name", "") if fwd else ""
        
        row = {
            "scrape_date": scrape_date,
            "channel": title,
            "username": f"@{username}",
            "msg_id": msg.id,
            "date_utc": dt_utc.strftime("%Y-%m-%d"),
            "time_utc": dt_utc.strftime("%H:%M:%S"),
            "text": (msg.text or "").replace("\r", "")[:50000],
            "views": getattr(msg, "views", None) or 0,
            "forwards": getattr(msg, "forwards", None) or 0,
            "replies_count": (getattr(msg.replies, "replies", None) if getattr(msg, "replies", None) else 0) or 0,
            "reactions": reactions_str,
            "reactions_sum": reactions_sum,
            "media_type": mtype,
            "urls": " | ".join(urls) if urls else "",
            "url_count": len(urls),
            "hashtags": " | ".join(hashtags) if hashtags else "",
            "mentions": " | ".join(mentions) if mentions else "",
            "is_forward": is_forward,
            "fwd_from_name": fwd_from_name,
            "post_link": f"https://t.me/{username}/{msg.id}",
        }
        rows.append(row)
    
    return rows


async def run_scraper_coroutine(channels_config, push_to_sheet, log_fn):
    """Main async scraper coroutine."""
    log_fn(f"Starting scraper for {len(channels_config)} channel(s)...")
    
    log_fn("Connecting to Telegram...")
    client = TelegramClient(TG_SESSION_NAME, TG_API_ID, TG_API_HASH)
    await client.start(phone=TG_PHONE)
    me = await client.get_me()
    log_fn(f"Logged in as: {me.first_name}", "success")
    
    all_rows = []
    total_channels = len(channels_config)
    
    for i, ch in enumerate(channels_config):
        scrape_status["current_channel"] = ch["name"]
        progress = int((i / total_channels) * 100)
        scrape_status["progress"] = progress
        socketio.emit('status', {"running": True, "progress": progress, "channel": ch["name"]})
        
        if ch.get('use_date_range'):
            log_fn(f"Scraping: {ch['name']} ({ch['from_date'].strftime('%Y-%m-%d %H:%M')} → {ch['to_date'].strftime('%Y-%m-%d %H:%M')})...")
        else:
            log_fn(f"Scraping: {ch['name']} (last {ch['hours_back']}h)...")
        
        try:
            rows = await scrape_channel_simple(client, ch["name"], ch["from_date"], ch["to_date"], log_fn)
            all_rows.extend(rows)
            
            scrape_status["results"].append({
                "channel": ch["name"],
                "messages": len(rows),
                "hours_back": ch["hours_back"]
            })
            
            log_fn(f"  ✓ {len(rows)} messages from {ch['name']}", "success")
            socketio.emit('channel_done', {"channel": ch["name"], "count": len(rows)})
            
        except Exception as e:
            log_fn(f"  ✗ Error scraping {ch['name']}: {str(e)}", "error")
            scrape_status["results"].append({
                "channel": ch["name"],
                "messages": 0,
                "error": str(e)
            })
    
    await client.disconnect()
    
    # Always save to local database first (backup)
    if all_rows:
        local_saved = save_messages_locally(all_rows)
        log_fn(f"Saved {local_saved} new messages to local database", "info")
    
    # Push to Google Sheets
    if push_to_sheet and all_rows and SHEET_ID:
        log_fn("Pushing to Google Sheets...")
        try:
            push_to_sheets(all_rows)
            log_fn(f"Pushed {len(all_rows)} rows to Google Sheets", "success")
            log_audit("sheets_push_success", f"Pushed {len(all_rows)} rows")
        except Exception as e:
            log_fn(f"Error pushing to Sheets: {str(e)} - Data saved locally!", "error")
            log_audit("sheets_push_failed", str(e), "error")
    elif not all_rows:
        log_fn("No messages to push", "warning")
    elif not SHEET_ID:
        log_fn("SHEET_ID not configured, skipping Google Sheets", "warning")
    
    return all_rows


def run_scraper_async(channels_config, export_options=None):
    """Run the scraper in a background thread.
    
    Args:
        channels_config: List of channel configurations
        export_options: Dict with export settings:
            - push_to_sheets: bool
            - export_local: bool
            - local_format: "csv" or "json"
            - local_filename: str or None (auto-generate)
            - local_append: bool
    """
    global scheduler_config
    
    if export_options is None:
        export_options = {"push_to_sheets": True}
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    all_rows = []
    channel_names = [ch.get("name", "unknown") for ch in channels_config]
    
    log_audit("scrape_started", f"Channels: {', '.join(channel_names)}")
    
    push_to_sheets_flag = export_options.get("push_to_sheets", True)
    export_local = export_options.get("export_local", False)
    local_format = export_options.get("local_format", "csv")
    local_filename = export_options.get("local_filename", None)
    local_append = export_options.get("local_append", False)
    save_location = export_options.get("save_location", "default")
    custom_path = export_options.get("custom_path", None)
    
    try:
        scrape_status["running"] = True
        scrape_status["progress"] = 0
        scrape_status["messages"] = []
        scrape_status["results"] = []
        scrape_status["error"] = None
        scrape_status["kill_requested"] = False  # Reset kill switch
        
        socketio.emit('status', {"running": True, "progress": 0})
        
        all_rows = loop.run_until_complete(
            run_scraper_coroutine(channels_config, push_to_sheets_flag, log_message)
        )
        
        scrape_status["progress"] = 100
        log_message(f"Done! Total: {len(all_rows)} messages from {len(channels_config)} channel(s)", "success")
        log_audit("scrape_completed", f"Total: {len(all_rows)} messages from {len(channels_config)} channels")
        
        # Export to local file if requested
        if export_local and all_rows:
            log_message(f"Exporting to local {local_format.upper()} file...")
            result = export_data(all_rows, local_format, local_filename, local_append, save_location, custom_path)
            if result["success"]:
                log_message(f"Exported {result['rows']} rows to {result['filepath']} ({result['mode']})", "success")
                log_audit("local_export_success", f"{result['filepath']} - {result['rows']} rows ({result['mode']})")
            else:
                log_message(f"Export failed: {result['error']}", "error")
                log_audit("local_export_failed", result['error'], "error")
        
        # Update last_run timestamp
        scheduler_config["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_scheduler_config()
        socketio.emit('scheduler_update', scheduler_config)
        
    except Exception as e:
        scrape_status["error"] = str(e)
        log_message(f"Error: {str(e)}", "error")
        log_audit("scrape_failed", str(e), "error")
    
    finally:
        scrape_status["running"] = False
        socketio.emit('status', {"running": False, "progress": 100})
        socketio.emit('done', {"total": len(all_rows), "results": scrape_status["results"]})
        loop.close()


@app.route('/')
def index():
    """Main page."""
    channels = load_channels()
    return render_template('index.html', 
                         channels=channels,
                         sheet_id=SHEET_ID,
                         status=scrape_status)


@app.route('/api/channels', methods=['GET'])
def get_channels():
    """Get current channel configuration."""
    channels = load_channels()
    return jsonify(channels)


@app.route('/api/channels', methods=['POST'])
def save_channels():
    """Save channel configuration."""
    data = request.json
    channels_path = Path(__file__).parent / "channels.json"
    
    # Clean up channel data for saving
    channels_to_save = []
    for ch in data.get("channels", []):
        channel_data = {
            "name": ch.get("name", ""),
            "hours_back": ch.get("hours_back", 24),
            "use_date_range": ch.get("use_date_range", False),
        }
        if ch.get("use_date_range"):
            channel_data["from_date_str"] = ch.get("from_date_str", "")
            channel_data["to_date_str"] = ch.get("to_date_str", "")
        channels_to_save.append(channel_data)
    
    config = {
        "default_hours": data.get("default_hours", 24),
        "channels": channels_to_save
    }
    
    with open(channels_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    
    return jsonify({"success": True})


@app.route('/api/scrape', methods=['POST'])
def start_scrape():
    """Start a scrape job."""
    global app_enabled
    
    if not app_enabled:
        log_audit("scrape_blocked", "App is disabled", "warning")
        return jsonify({"error": "App is disabled. Enable it first."}), 400
    
    if scrape_status["running"]:
        return jsonify({"error": "Scraper is already running"}), 400
    
    data = request.json or {}
    
    # Build export options
    export_options = {
        "push_to_sheets": data.get("push_to_sheets", True),
        "export_local": data.get("export_local", False),
        "local_format": data.get("local_format", "csv"),
        "local_filename": data.get("local_filename", None),
        "local_append": data.get("local_append", False),
        "save_location": data.get("save_location", "default"),
        "custom_path": data.get("custom_path", None)
    }
    
    # Load channels and prepare config
    channels = load_channels()
    if not channels:
        return jsonify({"error": "No channels configured"}), 400
    
    # Start scraper in background thread
    thread = threading.Thread(target=run_scraper_async, args=(channels, export_options))
    thread.daemon = True
    thread.start()
    
    return jsonify({"success": True, "message": "Scraper started"})


@app.route('/api/status', methods=['GET'])
def get_status():
    """Get current scraper status."""
    return jsonify(scrape_status)


@app.route('/api/scheduler', methods=['GET'])
def get_scheduler():
    """Get scheduler configuration."""
    load_scheduler_config()
    return jsonify(scheduler_config)


@app.route('/api/scheduler', methods=['POST'])
def update_scheduler():
    """Update scheduler configuration."""
    global scheduler_config
    data = request.json
    
    scheduler_config["enabled"] = data.get("enabled", False)
    scheduler_config["time"] = data.get("time", "08:00")
    scheduler_config["interval_hours"] = data.get("interval_hours")
    
    save_scheduler_config()
    setup_scheduler()
    
    return jsonify({
        "success": True,
        "config": scheduler_config
    })


@app.route('/api/scheduler/run-now', methods=['POST'])
def run_scheduler_now():
    """Manually trigger a scheduled scrape."""
    if scrape_status["running"]:
        return jsonify({"error": "Scraper is already running"}), 400
    
    scheduled_scrape()
    return jsonify({"success": True, "message": "Scrape started"})


@app.route('/api/kill', methods=['POST'])
def kill_scrape():
    """Kill switch - stop all running scrape processes."""
    if not scrape_status["running"]:
        return jsonify({"error": "No scraper is running"}), 400
    
    scrape_status["kill_requested"] = True
    log_message("🛑 KILL SWITCH ACTIVATED - Stopping all processes...", "error")
    log_audit("kill_switch_activated", "User stopped running scrape")
    
    return jsonify({"success": True, "message": "Kill signal sent"})


# ═══════════════════════════════════════════════════════════════
# Audit Log API
# ═══════════════════════════════════════════════════════════════

@app.route('/api/audit', methods=['GET'])
def get_audit():
    """Get audit log entries."""
    limit = request.args.get('limit', 100, type=int)
    logs = get_audit_log(limit)
    return jsonify(logs)


@app.route('/api/audit', methods=['DELETE'])
def clear_audit():
    """Clear audit log."""
    clear_audit_log()
    log_audit("audit_log_cleared", "User cleared audit log")
    return jsonify({"success": True})


# ═══════════════════════════════════════════════════════════════
# App Enable/Disable API
# ═══════════════════════════════════════════════════════════════

@app.route('/api/app/status', methods=['GET'])
def get_app_status():
    """Get app enabled status."""
    global app_enabled
    local_stats = get_local_message_count()
    return jsonify({
        "enabled": app_enabled,
        "local_messages": local_stats
    })


@app.route('/api/app/toggle', methods=['POST'])
def toggle_app():
    """Toggle app enabled/disabled state."""
    global app_enabled
    app_enabled = not app_enabled
    set_setting("app_enabled", app_enabled)
    
    status = "enabled" if app_enabled else "disabled"
    log_audit(f"app_{status}", f"User {status} the app")
    
    return jsonify({"enabled": app_enabled})


# ═══════════════════════════════════════════════════════════════
# Reset Settings API
# ═══════════════════════════════════════════════════════════════

@app.route('/api/reset', methods=['POST'])
def reset_settings():
    """Reset all settings to defaults (keeps channels)."""
    global scheduler_config, app_enabled
    
    # Reset scheduler config
    scheduler_config = {
        "enabled": False,
        "time": "08:00",
        "interval_hours": None,
        "last_run": None,
        "next_run": None
    }
    save_scheduler_config()
    setup_scheduler()
    
    # Reset app enabled
    app_enabled = True
    set_setting("app_enabled", True)
    
    log_audit("settings_reset", "User reset all settings to defaults")
    
    return jsonify({
        "success": True,
        "message": "Settings reset to defaults (channels preserved)"
    })


# ═══════════════════════════════════════════════════════════════
# Local Database Stats API
# ═══════════════════════════════════════════════════════════════

@app.route('/api/local-stats', methods=['GET'])
def get_local_stats():
    """Get local database statistics."""
    stats = get_local_message_count()
    return jsonify(stats)


# ═══════════════════════════════════════════════════════════════
# Export Files API
# ═══════════════════════════════════════════════════════════════

@app.route('/api/exports', methods=['GET'])
def list_exports():
    """Get list of export files."""
    location = request.args.get('location', 'default')
    custom_path = request.args.get('custom_path', None)
    files = get_export_files(location, custom_path)
    return jsonify(files)


@app.route('/api/export-formats', methods=['GET'])
def get_formats():
    """Get available export formats."""
    return jsonify(get_available_formats())


if __name__ == '__main__':
    print("=" * 50)
    print("  Telegram Scraper Web GUI")
    print("  Open http://localhost:5000 in your browser")
    print("=" * 50)
    
    # Load app enabled state from database
    app_enabled = get_setting("app_enabled", True)
    
    # Load and start scheduler
    load_scheduler_config()
    scheduler.start()
    setup_scheduler()
    
    if scheduler_config["enabled"]:
        print(f"  Scheduler: ENABLED - Next run: {scheduler_config['next_run']}")
    else:
        print("  Scheduler: DISABLED")
    print("=" * 50)
    
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)
