"""Scraper execution, scheduler, and profile-based scraping engine."""
import asyncio
import json
import re
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

from flask import Blueprint, request, jsonify

from src.config import load_channels, TG_API_ID, TG_API_HASH, TG_PHONE, TG_SESSION_NAME, SHEET_ID
from src.sheets import push_to_sheets
from src.local_db import log_audit, save_messages_locally, set_setting
from src.local_export import export_data
from src.profiles import (
    get_process, get_profile, update_process, update_profile,
    mark_messages_scraped, increment_process_count, get_daily_remaining,
    get_due_processes, calculate_next_run, batch_check_scraped,
)
from routes.shared import (
    login_required, permission_required, audit, log_message,
    scrape_status, profile_scrape_status, socketio,
    get_app_enabled, set_app_enabled,
)

from telethon import TelegramClient
from telethon.tl import types
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

scraper_bp = Blueprint('scraper', __name__)


# ═══════════════════════════════════════════════════════════════
# SCHEDULER STATE
# ═══════════════════════════════════════════════════════════════

scheduler = BackgroundScheduler()
scheduler_config = {
    "enabled": False,
    "time": "08:00",
    "interval_hours": None,
    "last_run": None,
    "next_run": None,
}
SCHEDULER_CONFIG_FILE = Path(__file__).parent.parent / "scheduler_config.json"


def load_scheduler_config():
    global scheduler_config
    if SCHEDULER_CONFIG_FILE.exists():
        try:
            with open(SCHEDULER_CONFIG_FILE, 'r') as f:
                saved = json.load(f)
                scheduler_config.update(saved)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"[Scheduler] Error loading config: {e}")
    return scheduler_config


def save_scheduler_config():
    with open(SCHEDULER_CONFIG_FILE, 'w') as f:
        json.dump(scheduler_config, f, indent=2)


def scheduled_scrape():
    if not get_app_enabled():
        print("[Scheduler] App is disabled, skipping scheduled scrape...")
        log_audit("scheduled_scrape_skipped", "App is disabled", "warning")
        return
    if scrape_status["running"]:
        print("[Scheduler] Scraper already running, skipping...")
        return
    print(f"[Scheduler] Starting scheduled scrape at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_audit("scheduled_scrape_started", "Scheduled scrape initiated")
    scheduler_config["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save_scheduler_config()
    channels = load_channels()
    if channels:
        export_options = {"push_to_sheets": True}
        thread = threading.Thread(target=run_scraper_async, args=(channels, export_options))
        thread.daemon = True
        thread.start()


def setup_scheduler():
    global scheduler
    scheduler.remove_all_jobs()
    if not scheduler_config["enabled"]:
        scheduler_config["next_run"] = None
        return
    if scheduler_config["interval_hours"]:
        scheduler.add_job(scheduled_scrape, 'interval',
                          hours=scheduler_config["interval_hours"], id='scrape_job')
        next_run = datetime.now() + timedelta(hours=scheduler_config["interval_hours"])
    else:
        hour, minute = map(int, scheduler_config["time"].split(':'))
        scheduler.add_job(scheduled_scrape, CronTrigger(hour=hour, minute=minute), id='scrape_job')
        now = datetime.now()
        next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
    scheduler_config["next_run"] = next_run.strftime("%Y-%m-%d %H:%M:%S")
    save_scheduler_config()


def init_scheduler():
    """Initialize and start the scheduler. Called from web_app.py startup."""
    load_scheduler_config()
    scheduler.start()
    setup_scheduler()
    scheduler.add_job(check_profile_schedules, 'interval', seconds=60,
                      id='profile_scheduler', replace_existing=True)
    from src.auth import cleanup_expired_sessions
    scheduler.add_job(cleanup_expired_sessions, 'interval', hours=6,
                      id='session_cleanup', replace_existing=True)


# ═══════════════════════════════════════════════════════════════
# QUICK SCRAPE (Dashboard)
# ═══════════════════════════════════════════════════════════════

async def scrape_channel_simple(client, channel_name, from_date, to_date, log_fn):
    """Simplified scraper that works within the web context."""
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
    rows = _process_messages(all_messages, username, title)
    return rows


def _process_messages(all_messages, username, title):
    """Convert Telethon messages to row dicts."""
    scrape_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    rows = []
    for msg in all_messages:
        dt_utc = msg.date.astimezone(timezone.utc)
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
        media = getattr(msg, "media", None)
        mtype = type(media).__name__.replace("MessageMedia", "").lower() if media else ""
        fwd = getattr(msg, "fwd_from", None)
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
            "is_forward": bool(fwd),
            "fwd_from_name": getattr(fwd, "from_name", "") if fwd else "",
            "post_link": f"https://t.me/{username}/{msg.id}",
        }
        rows.append(row)
    return rows


async def run_scraper_coroutine(channels_config, push_to_sheet, log_fn):
    """Main async scraper coroutine for quick scrape."""
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
        if socketio:
            socketio.emit('status', {"running": True, "progress": progress, "channel": ch["name"]})
        if ch.get('use_date_range'):
            log_fn(f"Scraping: {ch['name']} ({ch['from_date'].strftime('%Y-%m-%d %H:%M')} -> {ch['to_date'].strftime('%Y-%m-%d %H:%M')})...")
        else:
            log_fn(f"Scraping: {ch['name']} (last {ch['hours_back']}h)...")
        try:
            rows = await scrape_channel_simple(client, ch["name"], ch["from_date"], ch["to_date"], log_fn)
            all_rows.extend(rows)
            scrape_status["results"].append({"channel": ch["name"], "messages": len(rows), "hours_back": ch["hours_back"]})
            log_fn(f"  {len(rows)} messages from {ch['name']}", "success")
            if socketio:
                socketio.emit('channel_done', {"channel": ch["name"], "count": len(rows)})
        except Exception as e:
            log_fn(f"  Error scraping {ch['name']}: {str(e)}", "error")
            scrape_status["results"].append({"channel": ch["name"], "messages": 0, "error": str(e)})
    await client.disconnect()
    if all_rows:
        local_saved = save_messages_locally(all_rows)
        log_fn(f"Saved {local_saved} new messages to local database", "info")
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
    """Run the scraper in a background thread."""
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
        scrape_status["kill_requested"] = False
        if socketio:
            socketio.emit('status', {"running": True, "progress": 0})
        all_rows = loop.run_until_complete(
            run_scraper_coroutine(channels_config, push_to_sheets_flag, log_message)
        )
        scrape_status["progress"] = 100
        log_message(f"Done! Total: {len(all_rows)} messages from {len(channels_config)} channel(s)", "success")
        log_audit("scrape_completed", f"Total: {len(all_rows)} messages from {len(channels_config)} channels")
        if export_local and all_rows:
            log_message(f"Exporting to local {local_format.upper()} file...")
            result = export_data(all_rows, local_format, local_filename, local_append, save_location, custom_path)
            if result["success"]:
                log_message(f"Exported {result['rows']} rows to {result['filepath']} ({result['mode']})", "success")
                log_audit("local_export_success", f"{result['filepath']} - {result['rows']} rows ({result['mode']})")
            else:
                log_message(f"Export failed: {result['error']}", "error")
                log_audit("local_export_failed", result['error'], "error")
        scheduler_config["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_scheduler_config()
        if socketio:
            socketio.emit('scheduler_update', scheduler_config)
    except Exception as e:
        scrape_status["error"] = str(e)
        log_message(f"Error: {str(e)}", "error")
        log_audit("scrape_failed", str(e), "error")
    finally:
        scrape_status["running"] = False
        if socketio:
            socketio.emit('status', {"running": False, "progress": 100})
            socketio.emit('done', {"total": len(all_rows), "results": scrape_status["results"]})
        loop.close()


# ═══════════════════════════════════════════════════════════════
# PROFILE-BASED SMART SCRAPER ENGINE
# ═══════════════════════════════════════════════════════════════

async def run_profile_scrape_coroutine(process_data, log_fn):
    """Smart scraper for a profile process."""
    proc_id = process_data["id"]
    profile_id = process_data["profile_id"]
    channel = process_data["channel_username"]
    proc_type = process_data["process_type"]
    daily_limit = process_data.get("daily_limit")
    batch_delay = process_data.get("batch_delay", 1.0) or 1.0

    if proc_type == "date_range":
        resume_date = process_data.get("current_position_date") or process_data.get("from_date")
        from_date = datetime.strptime(process_data["from_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        to_date = datetime.strptime(process_data["to_date"], "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        if resume_date and resume_date != process_data["from_date"]:
            effective_from = datetime.strptime(resume_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            log_fn(f"Resuming from {resume_date} (originally from {process_data['from_date']})")
        else:
            effective_from = from_date
    elif proc_type == "rolling":
        hours = process_data.get("hours_back", 24) or 24
        to_date = datetime.now(timezone.utc)
        effective_from = to_date - timedelta(hours=hours)
    else:
        hours = process_data.get("hours_back", 24) or 24
        to_date = datetime.now(timezone.utc)
        effective_from = to_date - timedelta(hours=hours)

    remaining = get_daily_remaining(proc_id)
    if remaining is not None and remaining <= 0:
        log_fn(f"Daily limit reached for today ({daily_limit} messages). Will resume tomorrow.", "warning")
        return []

    log_fn("Connecting to Telegram...")
    client = TelegramClient(TG_SESSION_NAME, TG_API_ID, TG_API_HASH)
    await client.start(phone=TG_PHONE)
    me = await client.get_me()
    log_fn(f"Logged in as: {me.first_name}", "success")

    try:
        entity = await client.get_entity(channel)
    except Exception as e:
        log_fn(f"Cannot find channel @{channel}: {e}", "error")
        await client.disconnect()
        return []

    username = getattr(entity, "username", None) or str(channel)
    title = getattr(entity, "title", username)
    update_profile(profile_id, channel_title=title)

    log_fn(f"Channel: {title} (@{username})")
    if proc_type == "date_range":
        log_fn(f"Range: {effective_from.strftime('%Y-%m-%d')} -> {to_date.strftime('%Y-%m-%d')}")
    if daily_limit:
        log_fn(f"Daily limit: {daily_limit} | Remaining today: {remaining if remaining is not None else 'unlimited'}")
    log_fn(f"Batch delay: {batch_delay}s")

    all_messages = []
    offset_id = 0
    batch_num = 0
    BATCH_SIZE = 200
    scraped_today = 0
    last_msg_date_str = None

    while True:
        status_key = f"process_{proc_id}"
        if profile_scrape_status.get(status_key, {}).get("kill"):
            log_fn("Stop signal received - pausing process", "warning")
            break
        if remaining is not None and scraped_today >= remaining:
            log_fn(f"Daily limit reached ({daily_limit}). Pausing until tomorrow.", "warning")
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
            err_str = str(e)
            if "FloodWait" in err_str or "flood" in err_str.lower():
                wait_match = re.search(r'(\d+)', err_str)
                wait_secs = int(wait_match.group(1)) if wait_match else 60
                log_fn(f"FloodWait: Telegram requires {wait_secs}s wait. Waiting...", "warning")
                await asyncio.sleep(wait_secs + 5)
                continue
            log_fn(f"Batch error: {e}, retrying in 5s...", "warning")
            await asyncio.sleep(5)
            continue
        if not msgs:
            break
        done = False
        batch_ids = [m.id for m in msgs]
        already_scraped = batch_check_scraped(channel, batch_ids)
        dupes_in_batch = len(already_scraped)
        for m in msgs:
            msg_date = m.date.astimezone(timezone.utc)
            if msg_date < effective_from:
                done = True
                break
            if m.id in already_scraped:
                continue
            all_messages.append(m)
            scraped_today += 1
            if remaining is not None and scraped_today >= remaining:
                done = True
                break
        offset_id = msgs[-1].id
        last_msg_date_str = msgs[-1].date.astimezone(timezone.utc).strftime("%Y-%m-%d")
        if batch_num % 3 == 0 or len(all_messages) % 1000 < BATCH_SIZE:
            dupe_note = f" ({dupes_in_batch} dupes skipped)" if dupes_in_batch > 0 else ""
            log_fn(f"  Batch #{batch_num}: {len(all_messages)} new msgs, reached {last_msg_date_str}{dupe_note}")
            if socketio:
                socketio.emit('profile_progress', {
                    "process_id": proc_id, "messages": len(all_messages),
                    "batch": batch_num, "current_date": last_msg_date_str,
                    "dupes_skipped": dupes_in_batch,
                })
        if done:
            break
        await asyncio.sleep(batch_delay)

    await client.disconnect()
    log_fn(f"Total new messages: {len(all_messages)}")
    rows = _process_messages(all_messages, username, title)

    if rows:
        new_count = mark_messages_scraped(profile_id, proc_id, channel, rows)
        increment_process_count(proc_id, new_count,
                                last_msg_id=all_messages[-1].id if all_messages else None,
                                last_msg_date=last_msg_date_str,
                                current_position=last_msg_date_str)
        log_fn(f"Tracked {new_count} new unique messages in database", "info")
    return rows


def run_profile_process(proc_id: int):
    """Run a single profile process in a background thread."""
    proc = get_process(proc_id)
    if not proc:
        return
    profile = get_profile(proc["profile_id"])
    if not profile:
        return
    status_key = f"process_{proc_id}"
    profile_scrape_status[status_key] = {"running": True, "kill": False, "messages": 0}
    update_process(proc_id, status="running", error_message=None,
                   last_run_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log_audit("process_started", f"Process '{proc['name']}' for @{proc['channel_username']}")

    def log_fn(msg, msg_type="info"):
        if socketio:
            socketio.emit('profile_log', {
                "process_id": proc_id, "profile_id": proc["profile_id"],
                "type": msg_type, "text": msg,
                "time": datetime.now().strftime("%H:%M:%S"),
            })

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        rows = loop.run_until_complete(run_profile_scrape_coroutine(proc, log_fn))
        if rows:
            save_messages_locally(rows)
            fmt = profile.get("export_format", "xlsx")
            location = profile.get("export_location", "default")
            custom_path = profile.get("export_custom_path")
            filename = f"{profile['channel_username']}_{proc['name'].replace(' ', '_')}"
            result = export_data(rows, fmt, filename, True, location, custom_path)
            if result["success"]:
                log_fn(f"Exported {result['rows']} rows -> {result['filepath']}", "success")
                log_audit("process_export", f"{result['filepath']} ({result['rows']} rows)")
            else:
                log_fn(f"Export failed: {result['error']}", "error")
            # Push to Google Sheets (per-profile sheet_id or global fallback)
            profile_sheet_id = profile.get("sheet_id") or SHEET_ID
            if (profile.get("push_to_sheets") or profile.get("sheet_id")) and profile_sheet_id:
                try:
                    user_id = profile.get("user_id")
                    push_to_sheets(rows, user_id=user_id, sheet_id=profile_sheet_id)
                    log_fn(f"Pushed {len(rows)} rows to Google Sheets", "success")
                except Exception as e:
                    log_fn(f"Sheets push failed: {e}", "error")
        total = len(rows) if rows else 0
        log_fn(f"Process complete: {total} new messages", "success")
        log_audit("process_completed", f"'{proc['name']}': {total} messages")
        final_status = "idle"
        if proc["process_type"] == "date_range":
            remaining = get_daily_remaining(proc_id)
            if remaining is not None and remaining <= 0:
                final_status = "paused"
            elif total == 0:
                final_status = "completed"
        elif proc["process_type"] == "one_time":
            final_status = "completed"
        next_run = None
        if proc.get("schedule_enabled") and final_status != "completed":
            next_run = calculate_next_run(proc)
        update_process(proc_id, status=final_status, next_run_at=next_run)
    except Exception as e:
        log_fn(f"Process error: {str(e)}", "error")
        log_audit("process_error", f"'{proc['name']}': {str(e)}", "error")
        update_process(proc_id, status="error", error_message=str(e))
    finally:
        profile_scrape_status.pop(status_key, None)
        if socketio:
            socketio.emit('profile_process_done', {
                "process_id": proc_id, "profile_id": proc["profile_id"],
            })
        loop.close()


def check_profile_schedules():
    """Check and run due profile processes. Called by APScheduler."""
    if not get_app_enabled():
        return
    due = get_due_processes()
    for proc in due:
        status_key = f"process_{proc['id']}"
        if status_key not in profile_scrape_status:
            thread = threading.Thread(target=run_profile_process, args=(proc['id'],))
            thread.daemon = True
            thread.start()


# ═══════════════════════════════════════════════════════════════
# API ROUTES
# ═══════════════════════════════════════════════════════════════

@scraper_bp.route('/api/scrape', methods=['POST'])
@login_required
def start_scrape():
    if not get_app_enabled():
        audit("scrape_blocked", "App is disabled", "warning")
        return jsonify({"error": "App is disabled. Enable it first."}), 400
    if scrape_status["running"]:
        return jsonify({"error": "Scraper is already running"}), 400
    data = request.json or {}
    export_options = {
        "push_to_sheets": data.get("push_to_sheets", True),
        "export_local": data.get("export_local", False),
        "local_format": data.get("local_format", "csv"),
        "local_filename": data.get("local_filename", None),
        "local_append": data.get("local_append", False),
        "save_location": data.get("save_location", "default"),
        "custom_path": data.get("custom_path", None),
    }
    channels = load_channels()
    if not channels:
        return jsonify({"error": "No channels configured"}), 400
    thread = threading.Thread(target=run_scraper_async, args=(channels, export_options))
    thread.daemon = True
    thread.start()
    return jsonify({"success": True, "message": "Scraper started"})


@scraper_bp.route('/api/status', methods=['GET'])
@login_required
def get_status():
    return jsonify(scrape_status)


@scraper_bp.route('/api/kill', methods=['POST'])
@login_required
def kill_scrape():
    if not scrape_status["running"]:
        return jsonify({"error": "No scraper is running"}), 400
    scrape_status["kill_requested"] = True
    log_message("KILL SWITCH ACTIVATED - Stopping all processes...", "error")
    audit("kill_switch_activated", "User stopped running scrape")
    return jsonify({"success": True, "message": "Kill signal sent"})


@scraper_bp.route('/api/scheduler', methods=['GET'])
@login_required
def get_scheduler():
    load_scheduler_config()
    return jsonify(scheduler_config)


@scraper_bp.route('/api/scheduler', methods=['POST'])
@permission_required("manage_scheduler")
def update_scheduler():
    data = request.json
    scheduler_config["enabled"] = data.get("enabled", False)
    scheduler_config["time"] = data.get("time", "08:00")
    scheduler_config["interval_hours"] = data.get("interval_hours")
    save_scheduler_config()
    setup_scheduler()
    return jsonify({"success": True, "config": scheduler_config})


@scraper_bp.route('/api/scheduler/run-now', methods=['POST'])
@permission_required("manage_scheduler")
def run_scheduler_now():
    if scrape_status["running"]:
        return jsonify({"error": "Scraper is already running"}), 400
    scheduled_scrape()
    return jsonify({"success": True, "message": "Scrape started"})


@scraper_bp.route('/api/processes/<int:proc_id>/run', methods=['POST'])
@login_required
def api_run_process(proc_id):
    from routes.shared import _check_profile_ownership
    if not get_app_enabled():
        return jsonify({"error": "App is disabled"}), 400
    proc = get_process(proc_id)
    if not proc:
        return jsonify({"error": "Process not found"}), 404
    profile = get_profile(proc.get("profile_id"))
    if not _check_profile_ownership(profile, request.user):
        return jsonify({"error": "Access denied"}), 403
    if proc["status"] == "running":
        return jsonify({"error": "Process is already running"}), 400
    thread = threading.Thread(target=run_profile_process, args=(proc_id,))
    thread.daemon = True
    thread.start()
    return jsonify({"success": True, "message": f"Process '{proc['name']}' started"})


@scraper_bp.route('/api/processes/<int:proc_id>/stop', methods=['POST'])
@login_required
def api_stop_process(proc_id):
    from routes.shared import _check_profile_ownership
    proc = get_process(proc_id)
    if proc:
        profile = get_profile(proc.get("profile_id"))
        if not _check_profile_ownership(profile, request.user):
            return jsonify({"error": "Access denied"}), 403
    status_key = f"process_{proc_id}"
    if status_key in profile_scrape_status:
        profile_scrape_status[status_key]["kill"] = True
        update_process(proc_id, status="paused")
        audit("process_stopped", f"User stopped process #{proc_id}")
        return jsonify({"success": True, "message": "Stop signal sent"})
    return jsonify({"error": "Process is not running"}), 400
