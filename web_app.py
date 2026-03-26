"""
TeleDrive — Telegram Channel Intelligence Platform
A multi-user web application for scraping, archiving, and exporting Telegram channel data.
"""
import asyncio
import json
import os
import threading
import functools
from datetime import datetime, timezone, timedelta
from pathlib import Path

from flask import Flask, render_template, jsonify, request, redirect, url_for, make_response
from flask_socketio import SocketIO, emit
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from src.config import load_channels, TG_API_ID, TG_API_HASH, TG_PHONE, TG_SESSION_NAME, SHEET_ID, GOOGLE_CREDS_JSON
from src.sheets import push_to_sheets
from src.local_db import (
    log_audit, get_audit_log, clear_audit_log,
    save_messages_locally, get_local_message_count,
    get_setting, set_setting
)
from src.local_export import export_data, get_export_files, get_available_formats
from src.profiles import (
    create_profile, get_profile, get_all_profiles, update_profile, delete_profile,
    create_process, get_process, get_profile_processes, update_process, delete_process,
    mark_messages_scraped, increment_process_count, get_daily_remaining,
    get_process_progress, get_due_processes, calculate_next_run,
    is_message_scraped, batch_check_scraped, get_channel_scraped_stats,
    create_channel, get_channel, get_all_channels, update_channel, delete_channel,
    get_channel_by_username,
)
from src.auth import (
    authenticate_user, authenticate_google, create_user, get_user, get_user_by_email,
    get_all_users, update_user, delete_user, change_password, get_user_count,
    create_session, validate_session, invalidate_session, cleanup_expired_sessions,
    set_user_credential, get_user_credential, get_user_credentials, delete_user_credentials,
    get_system_setting, set_system_setting, get_all_system_settings,
    get_all_plans, get_plan, get_user_plan_limits,
    get_effective_theme, get_site_theme, get_user_theme, set_user_theme, clear_user_theme,
    theme_to_css, DEFAULT_THEME, PRESET_THEMES, SECURITY_QUESTIONS,
    has_permission, get_available_roles, ROLES, verify_password,
    get_pending_users, approve_user, reject_user,
    set_security_question, get_security_question, verify_security_answer,
    reset_password_with_security, get_preset_themes,
)

from telethon import TelegramClient
from telethon.tl import types

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'teledrive-dev-only-change-in-production')
IS_PRODUCTION = os.getenv('FLASK_ENV') == 'production' or not os.getenv('FLASK_DEBUG')
COOKIE_SECURE = os.getenv('COOKIE_SECURE', 'true' if IS_PRODUCTION else 'false') == 'true'
socketio = SocketIO(app, cors_allowed_origins=os.getenv('CORS_ORIGIN', '*'), async_mode='threading')

# Threading lock for scraper state
scrape_lock = threading.Lock()


@app.context_processor
def inject_theme():
    """Inject theme CSS variables into all templates."""
    user = get_current_user()
    user_id = user["id"] if user else None
    theme = get_effective_theme(user_id)
    css_vars = theme_to_css(theme)
    allow_user_themes = get_system_setting("allow_user_themes", "false") == "true"
    user_theme_enabled = False
    if user_id and allow_user_themes:
        user_theme_enabled = get_user_credential(user_id, "theme", "_enabled", "false") == "true"
    return {
        "theme_css": css_vars,
        "theme": theme,
        "allow_user_themes": allow_user_themes,
        "user_theme_enabled": user_theme_enabled,
    }


# ═══════════════════════════════════════════════════════════════
# AUTH MIDDLEWARE
# ═══════════════════════════════════════════════════════════════

def get_current_user():
    """Get current user from session cookie."""
    token = request.cookies.get("td_session")
    if not token:
        return None
    return validate_session(token)


def audit(action: str, details: str = None, status: str = "success"):
    """Log audit with current user context if available."""
    user = getattr(request, 'user', None) if request else None
    uid = user["id"] if user else None
    email = user["email"] if user else None
    log_audit(action, details, status, user_id=uid, user_email=email)


def _check_profile_ownership(profile, user):
    """Check if user owns the profile. Admins can access any profile."""
    if user["role"] == "admin":
        return True
    return profile and profile.get("user_id") == user["id"]


def login_required(f):
    """Decorator to require authentication."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            if request.is_json or request.path.startswith('/api/'):
                return jsonify({"error": "Authentication required"}), 401
            return redirect('/login')
        request.user = user
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    """Decorator to require admin role."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            if request.is_json or request.path.startswith('/api/'):
                return jsonify({"error": "Authentication required"}), 401
            return redirect('/login')
        if user["role"] != "admin":
            if request.is_json or request.path.startswith('/api/'):
                return jsonify({"error": "Admin access required"}), 403
            return redirect('/dashboard')
        request.user = user
        return f(*args, **kwargs)
    return decorated


def permission_required(*permissions):
    """Decorator to require specific permissions based on role."""
    def decorator(f):
        @functools.wraps(f)
        def decorated(*args, **kwargs):
            user = get_current_user()
            if not user:
                if request.is_json or request.path.startswith('/api/'):
                    return jsonify({"error": "Authentication required"}), 401
                return redirect('/login')
            for perm in permissions:
                if not has_permission(user, perm):
                    if request.is_json or request.path.startswith('/api/'):
                        return jsonify({"error": f"Permission denied: requires {perm}"}), 403
                    return redirect('/dashboard')
            request.user = user
            return f(*args, **kwargs)
        return decorated
    return decorator

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
        except (json.JSONDecodeError, KeyError) as e:
            print(f"[Scheduler] Error loading config: {e}")
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


@app.route('/setup-guide')
def setup_guide_page():
    """Admin setup guide — accessible without login for initial setup."""
    return render_template('setup-guide.html')


@app.route('/')
def index():
    """Landing page for unauthenticated users, redirect to dashboard for logged-in users."""
    user = get_current_user()
    if user:
        return redirect('/dashboard')
    return render_template('landing.html')


@app.route('/dashboard')
@login_required
def dashboard():
    """Main dashboard page."""
    channels = load_channels()
    return render_template('dashboard.html',
                         user=request.user,
                         channels=channels,
                         sheet_id=SHEET_ID,
                         status=scrape_status)


# ═══════════════════════════════════════════════════════════════
# AUTH ROUTES
# ═══════════════════════════════════════════════════════════════

@app.route('/login')
def login_page():
    """Login page."""
    user = get_current_user()
    if user:
        return redirect('/dashboard')
    google_enabled = get_system_setting("google_oauth_enabled", "false") == "true"
    reg_enabled = get_system_setting("registration_enabled", "true") == "true"
    return render_template('login.html', google_enabled=google_enabled, registration_enabled=reg_enabled)


@app.route('/forgot-password')
def forgot_password_page():
    """Forgot password page."""
    user = get_current_user()
    if user:
        return redirect('/dashboard')
    return render_template('forgot-password.html', security_questions=SECURITY_QUESTIONS)


@app.route('/register')
def register_page():
    """Registration page."""
    user = get_current_user()
    if user:
        return redirect('/dashboard')
    reg_enabled = get_system_setting("registration_enabled", "true") == "true"
    if not reg_enabled:
        return redirect('/login')
    google_enabled = get_system_setting("google_oauth_enabled", "false") == "true"
    return render_template('register.html', google_enabled=google_enabled, security_questions=SECURITY_QUESTIONS)


@app.route('/api/auth/login', methods=['POST'])
def api_login():
    """Login with email/password."""
    data = request.json or {}
    email = data.get("email", "").strip()
    password = data.get("password", "")
    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400
    user = authenticate_user(email, password)
    if not user:
        return jsonify({"error": "Invalid email or password"}), 401
    if isinstance(user, dict) and user.get("pending"):
        return jsonify({"error": "Your account is pending admin approval", "pending": True}), 403
    token = create_session(user["id"])
    resp = make_response(jsonify({"success": True, "user": {
        "id": user["id"], "email": user["email"], "name": user["name"],
        "role": user["role"], "avatar_url": user.get("avatar_url")
    }}))
    resp.set_cookie("td_session", token, max_age=30*24*3600, httponly=True, samesite="Lax", secure=COOKIE_SECURE)
    log_audit("user_login", f"{user['email']} logged in", user_id=user["id"], user_email=user["email"])
    return resp


@app.route('/api/auth/register', methods=['POST'])
def api_register():
    """Register a new account."""
    reg_enabled = get_system_setting("registration_enabled", "true") == "true"
    if not reg_enabled:
        return jsonify({"error": "Registration is currently disabled"}), 403
    data = request.json or {}
    email = data.get("email", "").strip()
    name = data.get("name", "").strip()
    password = data.get("password", "")
    security_question = data.get("security_question", "")
    security_answer = data.get("security_answer", "")
    if not email or not name or not password:
        return jsonify({"error": "All fields are required"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400

    # Determine status based on admin approval setting
    require_approval = get_system_setting("require_approval", "false") == "true"
    status = "pending" if require_approval else "active"

    result = create_user(email, password, name, status=status,
                         security_question=security_question or None,
                         security_answer=security_answer or None)
    if not result["success"]:
        return jsonify({"error": result["error"]}), 400

    if status == "pending":
        log_audit("user_registered_pending", f"{email} registered (pending approval)")
        return jsonify({"success": True, "pending": True,
                        "message": "Your account has been submitted for admin approval."})

    user = get_user(result["id"])
    token = create_session(result["id"])
    resp = make_response(jsonify({"success": True, "user": {
        "id": user["id"], "email": user["email"], "name": user["name"], "role": user["role"]
    }}))
    resp.set_cookie("td_session", token, max_age=30*24*3600, httponly=True, samesite="Lax", secure=COOKIE_SECURE)
    log_audit("user_registered", f"{email} registered", user_id=result["id"], user_email=email)
    return resp


@app.route('/api/auth/google', methods=['POST'])
def api_google_login():
    """Login/register via Google OAuth token verification."""
    google_enabled = get_system_setting("google_oauth_enabled", "false") == "true"
    if not google_enabled:
        return jsonify({"error": "Google login is not enabled"}), 403
    data = request.json or {}
    google_id = data.get("google_id")
    email = data.get("email")
    name = data.get("name")
    avatar_url = data.get("avatar_url")
    if not google_id or not email:
        return jsonify({"error": "Invalid Google credentials"}), 400
    user = authenticate_google(google_id, email, name, avatar_url)
    if not user:
        return jsonify({"error": "Authentication failed"}), 401
    token = create_session(user["id"])
    resp = make_response(jsonify({"success": True, "user": {
        "id": user["id"], "email": user["email"], "name": user["name"],
        "role": user["role"], "avatar_url": user.get("avatar_url")
    }}))
    resp.set_cookie("td_session", token, max_age=30*24*3600, httponly=True, samesite="Lax", secure=COOKIE_SECURE)
    audit("google_login", f"{email} logged in via Google")
    return resp


@app.route('/api/auth/logout', methods=['POST'])
def api_logout():
    """Logout current user."""
    token = request.cookies.get("td_session")
    if token:
        invalidate_session(token)
    resp = make_response(jsonify({"success": True}))
    resp.delete_cookie("td_session")
    return resp


@app.route('/api/auth/me', methods=['GET'])
@login_required
def api_me():
    """Get current user info."""
    user = request.user
    limits = get_user_plan_limits(user["id"])
    return jsonify({**user, "limits": limits})


@app.route('/api/auth/profile', methods=['PUT'])
@login_required
def api_update_user_profile():
    """Update current user's profile."""
    data = request.json or {}
    user = request.user
    allowed_updates = {}
    if "name" in data and data["name"].strip():
        allowed_updates["name"] = data["name"].strip()
    if "email" in data and data["email"].strip():
        allowed_updates["email"] = data["email"].strip()
    if allowed_updates:
        result = update_user(user["id"], **allowed_updates)
        return jsonify(result)
    return jsonify({"success": True})


@app.route('/api/auth/password', methods=['PUT'])
@login_required
def api_change_password():
    """Change password (requires current password verification)."""
    data = request.json or {}
    current_pass = data.get("current_password", "")
    new_pass = data.get("new_password", "")
    if not current_pass:
        return jsonify({"error": "Current password is required"}), 400
    if len(new_pass) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400
    # Verify current password
    from src.auth import get_connection as get_auth_conn
    conn = get_auth_conn()
    row = conn.execute("SELECT password_hash FROM users WHERE id = ?", (request.user["id"],)).fetchone()
    conn.close()
    if not row or not row["password_hash"] or not verify_password(current_pass, row["password_hash"]):
        return jsonify({"error": "Current password is incorrect"}), 403
    result = change_password(request.user["id"], new_pass)
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
# ADMIN ROUTES
# ═══════════════════════════════════════════════════════════════

@app.route('/admin')
@admin_required
def admin_page():
    """Admin dashboard."""
    return render_template('admin.html', user=request.user)


@app.route('/api/admin/users', methods=['GET'])
@admin_required
def api_admin_users():
    """Get all users with theme_enabled flag."""
    users = get_all_users()
    for u in users:
        u["theme_enabled"] = get_user_credential(u["id"], "theme", "_enabled", "false") == "true"
    return jsonify(users)


@app.route('/api/admin/users/<int:uid>', methods=['PUT'])
@admin_required
def api_admin_update_user(uid):
    """Update a user (admin)."""
    data = request.json or {}
    result = update_user(uid, **data)
    if result["success"]:
        audit("admin_user_updated", f"Admin updated user #{uid}: {data}")
    return jsonify(result)


@app.route('/api/admin/users/<int:uid>', methods=['DELETE'])
@admin_required
def api_admin_delete_user(uid):
    """Delete a user (admin)."""
    if uid == request.user["id"]:
        return jsonify({"error": "Cannot delete yourself"}), 400
    user = get_user(uid)
    result = delete_user(uid)
    if result["success"] and user:
        audit("admin_user_deleted", f"Admin deleted user {user['email']}")
    return jsonify(result)


@app.route('/api/admin/users', methods=['POST'])
@admin_required
def api_admin_create_user():
    """Create a user (admin)."""
    data = request.json or {}
    result = create_user(
        email=data.get("email", ""),
        password=data.get("password", ""),
        name=data.get("name", ""),
        role=data.get("role", "user")
    )
    if result["success"]:
        audit("admin_user_created", f"Admin created user {data.get('email')}")
    return jsonify(result), 200 if result["success"] else 400


@app.route('/api/admin/settings', methods=['GET'])
@admin_required
def api_admin_get_settings():
    """Get all system settings."""
    settings = get_all_system_settings()
    plans = get_all_plans()
    user_count = get_user_count()
    return jsonify({"settings": settings, "plans": plans, "user_count": user_count})


@app.route('/api/admin/settings', methods=['PUT'])
@admin_required
def api_admin_update_settings():
    """Update system settings."""
    data = request.json or {}
    for key, value in data.items():
        set_system_setting(key, str(value))
    audit("admin_settings_updated", f"Updated: {', '.join(data.keys())}")
    return jsonify({"success": True})


@app.route('/api/admin/theme', methods=['GET'])
@admin_required
def api_admin_get_theme():
    """Get the current site theme."""
    theme = get_site_theme()
    allow_user = get_system_setting("allow_user_themes", "false") == "true"
    return jsonify({"theme": theme, "allow_user_themes": allow_user, "defaults": DEFAULT_THEME})


@app.route('/api/admin/theme', methods=['PUT'])
@admin_required
def api_admin_update_theme():
    """Update the site theme colors. Accepts preset name or individual colors."""
    data = request.json or {}
    # Apply preset if specified
    preset_name = data.get("preset")
    if preset_name and preset_name in PRESET_THEMES:
        for key, value in PRESET_THEMES[preset_name]["theme"].items():
            set_system_setting(f"theme_{key}", value)
        audit("theme_preset_applied", f"Applied preset: {preset_name}")
        return jsonify({"success": True})
    for key, value in data.items():
        if key == "allow_user_themes":
            set_system_setting("allow_user_themes", "true" if value else "false")
        elif key in DEFAULT_THEME:
            set_system_setting(f"theme_{key}", value)
    audit("theme_updated", f"Updated: {', '.join(data.keys())}",
              user_id=request.user["id"], user_email=request.user["email"])
    return jsonify({"success": True})


@app.route('/api/admin/theme/reset', methods=['POST'])
@admin_required
def api_admin_reset_theme():
    """Reset site theme to defaults."""
    for key, value in DEFAULT_THEME.items():
        set_system_setting(f"theme_{key}", value)
    audit("theme_reset", "Theme reset to defaults")
    return jsonify({"success": True})


@app.route('/api/admin/users/<int:uid>/theme', methods=['PUT'])
@admin_required
def api_admin_toggle_user_theme(uid):
    """Enable/disable custom theme for a specific user."""
    data = request.json or {}
    enabled = data.get("enabled", False)
    set_user_credential(uid, "theme", "_enabled", "true" if enabled else "false")
    audit("user_theme_toggled", f"User {uid}: custom theme {'enabled' if enabled else 'disabled'}")
    return jsonify({"success": True})


@app.route('/api/user/theme', methods=['GET'])
@login_required
def api_user_get_theme():
    """Get current user's theme settings."""
    user = request.user
    user_theme = get_user_theme(user["id"])
    site_theme = get_site_theme()
    enabled = get_user_credential(user["id"], "theme", "_enabled", "false") == "true"
    allow = get_system_setting("allow_user_themes", "false") == "true"
    return jsonify({
        "site_theme": site_theme,
        "user_theme": user_theme,
        "enabled": enabled,
        "allowed": allow,
    })


@app.route('/api/user/theme', methods=['PUT'])
@login_required
def api_user_update_theme():
    """Update current user's custom theme."""
    user = request.user
    allow = get_system_setting("allow_user_themes", "false") == "true"
    enabled = get_user_credential(user["id"], "theme", "_enabled", "false") == "true"
    if not allow or not enabled:
        return jsonify({"error": "Custom themes not enabled for your account"}), 403
    data = request.json or {}
    set_user_theme(user["id"], data)
    return jsonify({"success": True})


@app.route('/api/user/theme/reset', methods=['POST'])
@login_required
def api_user_reset_theme():
    """Reset user theme to site defaults."""
    clear_user_theme(request.user["id"])
    return jsonify({"success": True})


@app.route('/api/admin/stats', methods=['GET'])
@admin_required
def api_admin_stats():
    """Get admin dashboard stats."""
    users = get_all_users()
    profiles = get_all_profiles()
    local_stats = get_local_message_count()
    return jsonify({
        "total_users": len(users),
        "active_users": sum(1 for u in users if u["is_active"]),
        "admin_count": sum(1 for u in users if u["role"] == "admin"),
        "total_profiles": len(profiles),
        "total_messages": local_stats.get("total", 0),
        "subs_enabled": get_system_setting("subscriptions_enabled", "false") == "true",
    })


@app.route('/api/admin/roles', methods=['GET'])
@admin_required
def api_admin_roles():
    """Get all available roles with permissions."""
    return jsonify(get_available_roles())


# ═══════════════════════════════════════════════════════════════
# PENDING USER APPROVAL ROUTES
# ═══════════════════════════════════════════════════════════════

@app.route('/api/admin/pending-users', methods=['GET'])
@admin_required
def api_admin_pending_users():
    """Get all pending users."""
    return jsonify(get_pending_users())


@app.route('/api/admin/users/<int:uid>/approve', methods=['POST'])
@admin_required
def api_admin_approve_user(uid):
    """Approve a pending user."""
    user = get_user(uid)
    result = approve_user(uid)
    if result["success"] and user:
        audit("user_approved", f"Admin approved user {user['email']}")
    return jsonify(result)


@app.route('/api/admin/users/<int:uid>/reject', methods=['POST'])
@admin_required
def api_admin_reject_user(uid):
    """Reject a pending user."""
    user = get_user(uid)
    result = reject_user(uid)
    if result["success"] and user:
        audit("user_rejected", f"Admin rejected user {user['email']}")
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
# FORGOT PASSWORD (SECURITY QUESTION) ROUTES
# ═══════════════════════════════════════════════════════════════

@app.route('/api/auth/forgot-password/verify', methods=['POST'])
def api_forgot_verify():
    """Get the security question for an email."""
    data = request.json or {}
    email = data.get("email", "").strip()
    if not email:
        return jsonify({"error": "Email is required"}), 400
    question = get_security_question(email)
    if not question:
        return jsonify({"error": "No security question set for this account. Contact your administrator."}), 404
    return jsonify({"question": question})


@app.route('/api/auth/forgot-password/reset', methods=['POST'])
def api_forgot_reset():
    """Reset password using security answer."""
    data = request.json or {}
    email = data.get("email", "").strip()
    answer = data.get("answer", "").strip()
    new_password = data.get("new_password", "")
    if not email or not answer or not new_password:
        return jsonify({"error": "All fields are required"}), 400
    if len(new_password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400
    result = reset_password_with_security(email, answer, new_password)
    if result["success"]:
        log_audit("password_reset_security", f"Password reset via security question for {email}")
    return jsonify(result), 200 if result["success"] else 400


@app.route('/api/auth/security-question', methods=['PUT'])
@login_required
def api_update_security_question():
    """Update current user's security question."""
    data = request.json or {}
    question = data.get("question", "")
    answer = data.get("answer", "")
    if not question or not answer:
        return jsonify({"error": "Question and answer are required"}), 400
    set_security_question(request.user["id"], question, answer)
    audit("security_question_updated", "User updated security question")
    return jsonify({"success": True})


@app.route('/api/auth/security-questions', methods=['GET'])
def api_get_security_questions():
    """Get list of available security questions."""
    return jsonify(SECURITY_QUESTIONS)


# ═══════════════════════════════════════════════════════════════
# THEME PRESETS ROUTES
# ═══════════════════════════════════════════════════════════════

@app.route('/api/themes/presets', methods=['GET'])
@login_required
def api_get_theme_presets():
    """Get all preset themes."""
    return jsonify(get_preset_themes())


@app.route('/api/user/theme/preset', methods=['PUT'])
@login_required
def api_user_apply_preset():
    """Apply a preset theme to user's custom theme."""
    user = request.user
    allow = get_system_setting("allow_user_themes", "false") == "true"
    enabled = get_user_credential(user["id"], "theme", "_enabled", "false") == "true"
    if not allow or not enabled:
        return jsonify({"error": "Custom themes not enabled for your account"}), 403
    data = request.json or {}
    preset_name = data.get("preset")
    if preset_name not in PRESET_THEMES:
        return jsonify({"error": "Invalid preset"}), 400
    set_user_theme(user["id"], PRESET_THEMES[preset_name]["theme"])
    return jsonify({"success": True})


# ═══════════════════════════════════════════════════════════════
# SHARED CREDENTIALS (read-only for users)
# ═══════════════════════════════════════════════════════════════

@app.route('/api/settings/shared-credentials', methods=['GET'])
@login_required
def api_get_shared_credentials():
    """Get shared credentials (always masked) for all users."""
    result = {}
    for key in SETTINGS_KEYS:
        val = get_setting(key, "")
        if not val:
            if key == "tg_api_id": val = str(TG_API_ID) if TG_API_ID else ""
            elif key == "tg_api_hash": val = TG_API_HASH or ""
            elif key == "tg_phone": val = TG_PHONE or ""
            elif key == "tg_session_name": val = TG_SESSION_NAME or "scraper_session"
            elif key == "google_creds_json": val = GOOGLE_CREDS_JSON or ""
            elif key == "sheet_id": val = SHEET_ID or ""
        # Always mask sensitive values for non-admin
        if val and len(str(val)) > 4:
            masked = str(val)[:2] + "\u2022" * min(len(str(val)) - 4, 20) + str(val)[-2:]
            result[key] = masked
        else:
            result[key] = "\u2022\u2022\u2022\u2022" if val else ""
        result[key + "_set"] = bool(val)
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
# DASHBOARD STATS API
# ═══════════════════════════════════════════════════════════════

@app.route('/api/dashboard/stats', methods=['GET'])
@login_required
def api_dashboard_stats():
    """Get dashboard statistics for the current user."""
    user = request.user
    from src.profiles import get_connection as get_prof_conn
    conn = get_prof_conn()

    # Get user's profiles or all if admin
    if user["role"] == "admin":
        profiles = conn.execute("SELECT COUNT(*) as c FROM profiles").fetchone()["c"]
        processes = conn.execute("SELECT * FROM processes ORDER BY last_run_at DESC LIMIT 1").fetchone()
        total_msgs = conn.execute("SELECT COALESCE(SUM(messages_scraped), 0) as c FROM processes").fetchone()["c"]
        total_procs = conn.execute("SELECT COUNT(*) as c FROM processes").fetchone()["c"]
        completed = conn.execute("SELECT COUNT(*) as c FROM processes WHERE status IN ('completed', 'idle')").fetchone()["c"]
        errored = conn.execute("SELECT COUNT(*) as c FROM processes WHERE status = 'error'").fetchone()["c"]
        active_schedules = conn.execute("SELECT COUNT(*) as c FROM processes WHERE schedule_enabled = 1").fetchone()["c"]
        today = datetime.now().strftime("%Y-%m-%d")
        today_msgs = conn.execute("SELECT COALESCE(SUM(today_scraped), 0) as c FROM processes WHERE today_date = ?", (today,)).fetchone()["c"]
    else:
        profiles = conn.execute("SELECT COUNT(*) as c FROM profiles WHERE user_id = ?", (user["id"],)).fetchone()["c"]
        processes = conn.execute("""
            SELECT pr.* FROM processes pr JOIN profiles p ON pr.profile_id = p.id
            WHERE p.user_id = ? ORDER BY pr.last_run_at DESC LIMIT 1
        """, (user["id"],)).fetchone()
        total_msgs = conn.execute("""
            SELECT COALESCE(SUM(pr.messages_scraped), 0) as c FROM processes pr
            JOIN profiles p ON pr.profile_id = p.id WHERE p.user_id = ?
        """, (user["id"],)).fetchone()["c"]
        total_procs = conn.execute("""
            SELECT COUNT(*) as c FROM processes pr JOIN profiles p ON pr.profile_id = p.id WHERE p.user_id = ?
        """, (user["id"],)).fetchone()["c"]
        completed = conn.execute("""
            SELECT COUNT(*) as c FROM processes pr JOIN profiles p ON pr.profile_id = p.id
            WHERE p.user_id = ? AND pr.status IN ('completed', 'idle')
        """, (user["id"],)).fetchone()["c"]
        errored = conn.execute("""
            SELECT COUNT(*) as c FROM processes pr JOIN profiles p ON pr.profile_id = p.id
            WHERE p.user_id = ? AND pr.status = 'error'
        """, (user["id"],)).fetchone()["c"]
        active_schedules = conn.execute("""
            SELECT COUNT(*) as c FROM processes pr JOIN profiles p ON pr.profile_id = p.id
            WHERE p.user_id = ? AND pr.schedule_enabled = 1
        """, (user["id"],)).fetchone()["c"]
        today = datetime.now().strftime("%Y-%m-%d")
        today_msgs = conn.execute("""
            SELECT COALESCE(SUM(pr.today_scraped), 0) as c FROM processes pr
            JOIN profiles p ON pr.profile_id = p.id WHERE p.user_id = ? AND pr.today_date = ?
        """, (user["id"], today)).fetchone()["c"]

    # Last process details
    last_process = None
    if processes:
        last_process = dict(processes)

    # Success rate
    success_rate = round((completed / total_procs * 100), 1) if total_procs > 0 else 0

    conn.close()
    return jsonify({
        "total_profiles": profiles,
        "total_messages": total_msgs,
        "total_processes": total_procs,
        "success_rate": success_rate,
        "errored_count": errored,
        "active_schedules": active_schedules,
        "messages_today": today_msgs,
        "last_process": last_process,
    })


# ═══════════════════════════════════════════════════════════════
# CHANNEL CRUD API
# ═══════════════════════════════════════════════════════════════

@app.route('/api/managed-channels', methods=['GET'])
@login_required
def api_get_managed_channels():
    """Get all channels (shared across users)."""
    return jsonify(get_all_channels())


@app.route('/api/managed-channels', methods=['POST'])
@login_required
def api_create_managed_channel():
    """Create a new channel."""
    data = request.json or {}
    username = data.get("username", "").strip()
    if not username:
        return jsonify({"error": "Channel username is required"}), 400
    result = create_channel(
        username=username,
        title=data.get("title"),
        description=data.get("description"),
        created_by=request.user["id"]
    )
    if result["success"]:
        audit("channel_created", f"Channel @{username.lstrip('@')} created",
                  user_id=request.user["id"], user_email=request.user["email"])
    return jsonify(result), 200 if result["success"] else 400


@app.route('/api/managed-channels/<int:cid>', methods=['GET'])
@login_required
def api_get_managed_channel(cid):
    """Get a single channel with stats."""
    ch = get_channel(cid)
    if not ch:
        return jsonify({"error": "Channel not found"}), 404
    stats = get_channel_scraped_stats(ch["username"])
    ch["stats"] = stats
    return jsonify(ch)


@app.route('/api/managed-channels/<int:cid>', methods=['PUT'])
@login_required
def api_update_managed_channel(cid):
    """Update a channel."""
    data = request.json or {}
    result = update_channel(cid, **data)
    return jsonify(result)


@app.route('/api/managed-channels/<int:cid>', methods=['DELETE'])
@login_required
def api_delete_managed_channel(cid):
    """Delete a channel."""
    ch = get_channel(cid)
    if ch:
        audit("channel_deleted", f"Channel @{ch['username']} deleted")
    result = delete_channel(cid)
    return jsonify(result), 200 if result["success"] else 400


# ═══════════════════════════════════════════════════════════════
# USER CREDENTIALS API (per-user Telegram/Google/etc.)
# ═══════════════════════════════════════════════════════════════

@app.route('/api/user/credentials', methods=['GET'])
@login_required
def api_get_user_creds():
    """Get current user's credentials (masked)."""
    user = request.user
    creds = get_user_credentials(user["id"])
    result = {}
    for key, val in creds.items():
        # Mask sensitive values
        if any(s in key for s in ["hash", "token", "secret"]) and val and len(val) > 6:
            result[key] = val[:4] + "\u2022" * (len(val) - 6) + val[-2:]
            result[key + "_set"] = True
        elif "creds_json" in key and val:
            result[key] = val if val.endswith(".json") else "(inline JSON)"
            result[key + "_set"] = True
        else:
            result[key] = val
            result[key + "_set"] = bool(val)
    # Check if Telegram session exists for this user
    session_name = get_user_credential(user["id"], "telegram", "session_name", f"td_user_{user['id']}")
    result["tg_session_exists"] = Path(session_name + ".session").exists()
    return jsonify(result)


@app.route('/api/user/credentials', methods=['PUT'])
@login_required
def api_set_user_creds():
    """Update current user's credentials."""
    user = request.user
    data = request.json or {}
    updated = []
    for key, value in data.items():
        if value is not None and not str(value).startswith("\u2022"):
            parts = key.split(".", 1)
            if len(parts) == 2:
                set_user_credential(user["id"], parts[0], parts[1], str(value).strip())
                updated.append(key)
    if updated:
        audit("user_creds_updated", f"User {user['email']} updated: {', '.join(updated)}")
    return jsonify({"success": True, "updated": updated})


@app.route('/api/user/test-telegram', methods=['POST'])
@login_required
def api_user_test_telegram():
    """Test Telegram connection with user's credentials."""
    import asyncio as _asyncio
    user = request.user
    api_id = get_user_credential(user["id"], "telegram", "api_id", str(TG_API_ID))
    api_hash = get_user_credential(user["id"], "telegram", "api_hash", TG_API_HASH)
    phone = get_user_credential(user["id"], "telegram", "phone", TG_PHONE)
    session = get_user_credential(user["id"], "telegram", "session_name", f"td_user_{user['id']}")
    if not api_id or not api_hash or not phone:
        return jsonify({"success": False, "error": "Telegram credentials not configured. Add your API ID, API Hash, and phone number first."})
    try:
        loop = _asyncio.new_event_loop()
        client = TelegramClient(session, int(api_id), api_hash)
        loop.run_until_complete(client.start(phone=phone))
        me = loop.run_until_complete(client.get_me())
        loop.run_until_complete(client.disconnect())
        loop.close()
        name = f"{me.first_name or ''} {me.last_name or ''}".strip()
        return jsonify({"success": True, "user": name, "phone": phone})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/user/test-google', methods=['POST'])
@login_required
def api_user_test_google():
    """Test Google Sheets connection with user's credentials."""
    user = request.user
    creds = get_user_credential(user["id"], "google", "creds_json", GOOGLE_CREDS_JSON)
    sid = get_user_credential(user["id"], "google", "sheet_id", SHEET_ID)
    if not creds or not sid:
        return jsonify({"success": False, "error": "Google credentials or Sheet ID not configured"})
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        if creds.endswith(".json"):
            credentials = Credentials.from_service_account_file(creds, scopes=scopes)
        else:
            credentials = Credentials.from_service_account_info(json.loads(creds), scopes=scopes)
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key(sid)
        title = sheet.title
        worksheets = [ws.title for ws in sheet.worksheets()]
        return jsonify({"success": True, "sheet_title": title, "worksheets": worksheets})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/channels', methods=['GET'])
@login_required
def get_channels():
    """Get current channel configuration."""
    channels = load_channels()
    return jsonify(channels)


@app.route('/api/channels', methods=['POST'])
@login_required
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
@login_required
def start_scrape():
    """Start a scrape job."""
    global app_enabled
    
    if not app_enabled:
        audit("scrape_blocked", "App is disabled", "warning")
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
@login_required
def get_status():
    """Get current scraper status."""
    return jsonify(scrape_status)


@app.route('/api/scheduler', methods=['GET'])
@login_required
def get_scheduler():
    """Get scheduler configuration."""
    load_scheduler_config()
    return jsonify(scheduler_config)


@app.route('/api/scheduler', methods=['POST'])
@permission_required("manage_scheduler")
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
@permission_required("manage_scheduler")
def run_scheduler_now():
    """Manually trigger a scheduled scrape."""
    if scrape_status["running"]:
        return jsonify({"error": "Scraper is already running"}), 400
    
    scheduled_scrape()
    return jsonify({"success": True, "message": "Scrape started"})


@app.route('/api/kill', methods=['POST'])
@login_required
def kill_scrape():
    """Kill switch - stop all running scrape processes."""
    if not scrape_status["running"]:
        return jsonify({"error": "No scraper is running"}), 400
    
    scrape_status["kill_requested"] = True
    log_message("🛑 KILL SWITCH ACTIVATED - Stopping all processes...", "error")
    audit("kill_switch_activated", "User stopped running scrape")
    
    return jsonify({"success": True, "message": "Kill signal sent"})


# ═══════════════════════════════════════════════════════════════
# Audit Log API
# ═══════════════════════════════════════════════════════════════

@app.route('/api/audit', methods=['GET'])
@login_required
def get_audit():
    """Get audit log entries. Admins see all, users see only their own."""
    limit = request.args.get('limit', 100, type=int)
    user = request.user
    if user["role"] == "admin":
        logs = get_audit_log(limit)
    else:
        logs = get_audit_log(limit, user_id=user["id"])
    return jsonify(logs)


@app.route('/api/audit', methods=['DELETE'])
@admin_required
def clear_audit():
    """Clear audit log."""
    clear_audit_log()
    audit("audit_log_cleared", "User cleared audit log")
    return jsonify({"success": True})


# ═══════════════════════════════════════════════════════════════
# App Enable/Disable API
# ═══════════════════════════════════════════════════════════════

@app.route('/api/app/status', methods=['GET'])
@login_required
def get_app_status():
    """Get app enabled status."""
    global app_enabled
    local_stats = get_local_message_count()
    return jsonify({
        "enabled": app_enabled,
        "local_messages": local_stats
    })


@app.route('/api/app/toggle', methods=['POST'])
@admin_required
def toggle_app():
    """Toggle app enabled/disabled state. When disabling, kill all running processes."""
    global app_enabled
    app_enabled = not app_enabled
    set_setting("app_enabled", app_enabled)
    
    status = "enabled" if app_enabled else "disabled"
    audit(f"app_{status}", f"User {status} the app")
    
    # When disabling, kill any running scrape AND profile processes
    if not app_enabled:
        if scrape_status["running"]:
            scrape_status["kill_requested"] = True
        for sk, sv in profile_scrape_status.items():
            sv["kill"] = True
        if scrape_status["running"] or profile_scrape_status:
            log_message("🛑 APP DISABLED — All running processes killed.", "error")
        socketio.emit('app_disabled', {"enabled": False})
    else:
        socketio.emit('app_enabled', {"enabled": True})
    
    return jsonify({"enabled": app_enabled})


# ═══════════════════════════════════════════════════════════════
# MANAGER / OVERVIEW API
# ═══════════════════════════════════════════════════════════════

@app.route('/api/manager/overview', methods=['GET'])
@login_required
def api_manager_overview():
    """Aggregate stats: profiles, processes, cron jobs, messages. Scoped by user for non-admins."""
    from src.profiles import get_connection as get_prof_conn
    conn = get_prof_conn()
    user = request.user
    is_admin = user["role"] == "admin"

    # Build WHERE clause for user scoping
    profile_where = "" if is_admin else f"WHERE p.user_id = {user['id']}"
    process_join_where = "" if is_admin else f"AND p.user_id = {user['id']}"

    # Profile stats
    profiles = [dict(r) for r in conn.execute(f"""
        SELECT p.id, p.name, p.channel_username, p.channel_title, p.is_active, p.created_at,
               COUNT(pr.id) as process_count,
               COALESCE(SUM(pr.messages_scraped), 0) as total_messages,
               (SELECT COUNT(*) FROM processes WHERE profile_id = p.id AND status = 'running') as running_count
        FROM profiles p
        LEFT JOIN processes pr ON p.id = pr.profile_id
        {profile_where}
        GROUP BY p.id ORDER BY p.updated_at DESC
    """).fetchall()]

    # All processes (the "cron jobs")
    all_processes = [dict(r) for r in conn.execute(f"""
        SELECT pr.id, pr.profile_id, pr.name, pr.process_type, pr.status,
               pr.schedule_enabled, pr.schedule_time, pr.schedule_interval_hours,
               pr.messages_scraped, pr.today_scraped, pr.today_date,
               pr.daily_limit, pr.from_date, pr.to_date, pr.current_position_date,
               pr.last_run_at, pr.next_run_at, pr.error_message, pr.created_at,
               p.channel_username, p.name as profile_name
        FROM processes pr
        JOIN profiles p ON pr.profile_id = p.id {process_join_where}
        ORDER BY pr.status = 'running' DESC, pr.schedule_enabled DESC, pr.updated_at DESC
    """).fetchall()]

    # Aggregate counts (scoped)
    profile_ids = [p["id"] for p in profiles]
    if profile_ids:
        id_list = ",".join(str(i) for i in profile_ids)
        total_msgs = conn.execute(f"SELECT COUNT(*) as c FROM scraped_messages WHERE profile_id IN ({id_list})").fetchone()["c"]
    else:
        total_msgs = 0 if not is_admin else conn.execute("SELECT COUNT(*) as c FROM scraped_messages").fetchone()["c"]

    status_counts = {}
    for row in conn.execute("SELECT status, COUNT(*) as c FROM processes GROUP BY status").fetchall():
        status_counts[row["status"]] = row["c"]

    scheduled_count = conn.execute("SELECT COUNT(*) as c FROM processes WHERE schedule_enabled = 1").fetchone()["c"]

    conn.close()

    # Global scheduler info
    global_sched = {
        "enabled": scheduler_config.get("enabled", False),
        "time": scheduler_config.get("time"),
        "interval_hours": scheduler_config.get("interval_hours"),
        "next_run": scheduler_config.get("next_run"),
        "last_run": scheduler_config.get("last_run"),
    }

    return jsonify({
        "profiles": profiles,
        "profile_count": len(profiles),
        "active_profiles": sum(1 for p in profiles if p["is_active"]),
        "processes": all_processes,
        "process_count": len(all_processes),
        "status_counts": status_counts,
        "scheduled_count": scheduled_count,
        "total_messages": total_msgs,
        "global_scheduler": global_sched,
        "app_enabled": app_enabled,
    })


# ═══════════════════════════════════════════════════════════════
# ACCOUNTS / CREDENTIALS SETTINGS API
# ═══════════════════════════════════════════════════════════════

SETTINGS_KEYS = {
    "tg_api_id", "tg_api_hash", "tg_phone", "tg_session_name",
    "google_creds_json", "sheet_id",
    "github_token", "github_repo"
}

@app.route('/api/settings/accounts', methods=['GET'])
@permission_required("manage_settings")
def api_get_accounts():
    """Get all account/credential settings (masked)."""
    result = {}
    for key in SETTINGS_KEYS:
        val = get_setting(key, "")
        if not val:
            # Fallback to env/config
            if key == "tg_api_id": val = str(TG_API_ID) if TG_API_ID else ""
            elif key == "tg_api_hash": val = TG_API_HASH or ""
            elif key == "tg_phone": val = TG_PHONE or ""
            elif key == "tg_session_name": val = TG_SESSION_NAME or "scraper_session"
            elif key == "google_creds_json": val = GOOGLE_CREDS_JSON or ""
            elif key == "sheet_id": val = SHEET_ID or ""
        # Mask sensitive values
        if key in ("tg_api_hash", "github_token") and val and len(val) > 6:
            result[key] = val[:4] + "•" * (len(val) - 6) + val[-2:]
            result[key + "_set"] = True
        elif key == "google_creds_json" and val:
            result[key] = val if val.endswith(".json") else "(inline JSON)"
            result[key + "_set"] = True
        else:
            result[key] = val
            result[key + "_set"] = bool(val)
    # Add connection status flags
    result["tg_session_exists"] = Path(get_setting("tg_session_name", TG_SESSION_NAME) + ".session").exists()
    return jsonify(result)


@app.route('/api/settings/accounts', methods=['PUT'])
@permission_required("manage_settings")
def api_update_accounts():
    """Update account/credential settings."""
    data = request.json or {}
    updated = []
    for key in SETTINGS_KEYS:
        if key in data and data[key] is not None:
            val = str(data[key]).strip()
            if val and not val.startswith("•"):  # Don't save masked values
                set_setting(key, val)
                updated.append(key)
    if updated:
        audit("accounts_updated", f"Updated: {', '.join(updated)}")
    return jsonify({"success": True, "updated": updated})


@app.route('/api/settings/test-telegram', methods=['POST'])
@permission_required("test_connections")
def api_test_telegram():
    """Test Telegram connection with current credentials."""
    import asyncio as _asyncio
    api_id = get_setting("tg_api_id", str(TG_API_ID))
    api_hash = get_setting("tg_api_hash", TG_API_HASH)
    phone = get_setting("tg_phone", TG_PHONE)
    session = get_setting("tg_session_name", TG_SESSION_NAME)
    if not api_id or not api_hash or not phone:
        return jsonify({"success": False, "error": "Telegram credentials not configured"})
    try:
        loop = _asyncio.new_event_loop()
        client = TelegramClient(session, int(api_id), api_hash)
        loop.run_until_complete(client.start(phone=phone))
        me = loop.run_until_complete(client.get_me())
        loop.run_until_complete(client.disconnect())
        loop.close()
        name = f"{me.first_name or ''} {me.last_name or ''}".strip()
        return jsonify({"success": True, "user": name, "phone": phone})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/settings/test-google', methods=['POST'])
@permission_required("test_connections")
def api_test_google():
    """Test Google Sheets connection with current credentials."""
    creds = get_setting("google_creds_json", GOOGLE_CREDS_JSON)
    sid = get_setting("sheet_id", SHEET_ID)
    if not creds or not sid:
        return jsonify({"success": False, "error": "Google credentials or Sheet ID not configured"})
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        if creds.endswith(".json"):
            credentials = Credentials.from_service_account_file(creds, scopes=scopes)
        else:
            import json as _json
            credentials = Credentials.from_service_account_info(_json.loads(creds), scopes=scopes)
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key(sid)
        title = sheet.title
        worksheets = [ws.title for ws in sheet.worksheets()]
        return jsonify({"success": True, "sheet_title": title, "worksheets": worksheets})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════
# Reset Settings API
# ═══════════════════════════════════════════════════════════════

@app.route('/api/reset', methods=['POST'])
@admin_required
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
    
    audit("settings_reset", "User reset all settings to defaults")
    
    return jsonify({
        "success": True,
        "message": "Settings reset to defaults (channels preserved)"
    })


# ═══════════════════════════════════════════════════════════════
# Local Database Stats API
# ═══════════════════════════════════════════════════════════════

@app.route('/api/local-stats', methods=['GET'])
@login_required
def get_local_stats():
    """Get local database statistics."""
    stats = get_local_message_count()
    return jsonify(stats)


# ═══════════════════════════════════════════════════════════════
# Export Files API
# ═══════════════════════════════════════════════════════════════

@app.route('/api/exports', methods=['GET'])
@login_required
def list_exports():
    """Get list of export files."""
    location = request.args.get('location', 'default')
    custom_path = request.args.get('custom_path', None)
    files = get_export_files(location, custom_path)
    return jsonify(files)


@app.route('/api/export-formats', methods=['GET'])
@login_required
def get_formats():
    """Get available export formats."""
    return jsonify(get_available_formats())


# ═══════════════════════════════════════════════════════════════
# PROFILE API ENDPOINTS
# ═══════════════════════════════════════════════════════════════

@app.route('/api/profiles', methods=['GET'])
@login_required
def api_get_profiles():
    """Get profiles. Admins see all, users see only their own."""
    user = request.user
    if user["role"] == "admin":
        return jsonify(get_all_profiles())
    return jsonify(get_all_profiles(user_id=user["id"]))


@app.route('/api/profiles', methods=['POST'])
@login_required
def api_create_profile():
    """Create a new profile. Auto-creates a scrape process if date settings provided."""
    data = request.json or {}
    result = create_profile(
        name=data.get("name", ""),
        channel_username=data.get("channel_username", ""),
        description=data.get("description", ""),
        export_format=data.get("export_format", "xlsx"),
        export_location=data.get("export_location", "default"),
        export_custom_path=data.get("export_custom_path"),
        push_to_sheets=data.get("push_to_sheets", False),
        channel_id=data.get("channel_id"),
        user_id=request.user["id"],
    )
    if result["success"]:
        profile_id = result["id"]
        audit("profile_created", f"Profile '{data.get('name')}' for @{data.get('channel_username')}")

        # Auto-create a process if scrape settings are provided
        from_date = data.get("from_date")
        to_date = data.get("to_date")
        daily_limit = data.get("daily_limit")
        batch_delay = data.get("batch_delay", 1.0)

        if from_date and to_date:
            proc_result = create_process(
                profile_id=profile_id,
                name=f"Scrape {from_date} to {to_date}",
                process_type="date_range",
                from_date=from_date,
                to_date=to_date,
                daily_limit=daily_limit,
                batch_delay=batch_delay or 1.0,
            )
            if proc_result["success"]:
                audit("process_auto_created", f"Auto-created date_range process for profile #{profile_id}")
                result["process_id"] = proc_result["id"]

    return jsonify(result), 200 if result["success"] else 400


@app.route('/api/profiles/<int:pid>', methods=['GET'])
@login_required
def api_get_profile(pid):
    """Get a single profile with processes."""
    profile = get_profile(pid)
    if not profile:
        return jsonify({"error": "Profile not found"}), 404
    if not _check_profile_ownership(profile, request.user):
        return jsonify({"error": "Access denied"}), 403
    profile["processes"] = get_profile_processes(pid)
    return jsonify(profile)


@app.route('/api/profiles/<int:pid>', methods=['PUT'])
@login_required
def api_update_profile(pid):
    """Update a profile."""
    profile = get_profile(pid)
    if not profile:
        return jsonify({"error": "Profile not found"}), 404
    if not _check_profile_ownership(profile, request.user):
        return jsonify({"error": "Access denied"}), 403
    data = request.json or {}
    result = update_profile(pid, **data)
    return jsonify(result)


@app.route('/api/profiles/<int:pid>', methods=['DELETE'])
@login_required
def api_delete_profile(pid):
    """Delete a profile."""
    profile = get_profile(pid)
    if not profile:
        return jsonify({"error": "Profile not found"}), 404
    if not _check_profile_ownership(profile, request.user):
        return jsonify({"error": "Access denied"}), 403
    audit("profile_deleted", f"Deleted profile '{profile['name']}'")
    result = delete_profile(pid)
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
# PROCESS API ENDPOINTS
# ═══════════════════════════════════════════════════════════════

@app.route('/api/profiles/<int:pid>/processes', methods=['GET'])
@login_required
def api_get_processes(pid):
    """Get all processes for a profile."""
    profile = get_profile(pid)
    if not profile:
        return jsonify({"error": "Profile not found"}), 404
    if not _check_profile_ownership(profile, request.user):
        return jsonify({"error": "Access denied"}), 403
    return jsonify(get_profile_processes(pid))


@app.route('/api/profiles/<int:pid>/processes', methods=['POST'])
@login_required
def api_create_process(pid):
    """Create a new process for a profile."""
    profile = get_profile(pid)
    if not profile:
        return jsonify({"error": "Profile not found"}), 404
    if not _check_profile_ownership(profile, request.user):
        return jsonify({"error": "Access denied"}), 403
    data = request.json or {}
    result = create_process(
        profile_id=pid,
        name=data.get("name", ""),
        process_type=data.get("process_type", "rolling"),
        from_date=data.get("from_date"),
        to_date=data.get("to_date"),
        hours_back=data.get("hours_back", 24),
        daily_limit=data.get("daily_limit"),
        batch_delay=data.get("batch_delay", 1.0),
        schedule_enabled=data.get("schedule_enabled", False),
        schedule_time=data.get("schedule_time"),
        schedule_interval_hours=data.get("schedule_interval_hours")
    )
    if result["success"]:
        audit("process_created", f"Process '{data.get('name')}' for profile '{profile['name']}'")
    return jsonify(result), 200 if result["success"] else 400


@app.route('/api/processes/<int:proc_id>', methods=['GET'])
@login_required
def api_get_process(proc_id):
    """Get process with detailed progress info."""
    progress = get_process_progress(proc_id)
    if not progress:
        return jsonify({"error": "Process not found"}), 404
    # Check ownership via parent profile
    profile = get_profile(progress.get("profile_id"))
    if not _check_profile_ownership(profile, request.user):
        return jsonify({"error": "Access denied"}), 403
    channel = progress.get("channel_username", "")
    if channel:
        progress["channel_stats"] = get_channel_scraped_stats(channel)
    return jsonify(progress)


@app.route('/api/processes/<int:proc_id>', methods=['PUT'])
@login_required
def api_update_process(proc_id):
    """Update a process."""
    proc = get_process(proc_id)
    if not proc:
        return jsonify({"error": "Process not found"}), 404
    profile = get_profile(proc.get("profile_id"))
    if not _check_profile_ownership(profile, request.user):
        return jsonify({"error": "Access denied"}), 403
    data = request.json or {}
    result = update_process(proc_id, **data)
    return jsonify(result)


@app.route('/api/processes/<int:proc_id>', methods=['DELETE'])
@login_required
def api_delete_process(proc_id):
    """Delete a process."""
    proc = get_process(proc_id)
    if not proc:
        return jsonify({"error": "Process not found"}), 404
    profile = get_profile(proc.get("profile_id"))
    if not _check_profile_ownership(profile, request.user):
        return jsonify({"error": "Access denied"}), 403
    if proc["status"] == "running":
        return jsonify({"error": "Cannot delete a running process. Stop it first."}), 400
    audit("process_deleted", f"Deleted process '{proc['name']}'")
    result = delete_process(proc_id)
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
# PROFILE-BASED SMART SCRAPER ENGINE
# ═══════════════════════════════════════════════════════════════

# Track running profile processes
profile_scrape_status = {}


async def run_profile_scrape_coroutine(process_data, log_fn):
    """
    Smart scraper for a profile process.
    Respects daily limits, resumes from last position, deduplicates.
    """
    proc_id = process_data["id"]
    profile_id = process_data["profile_id"]
    channel = process_data["channel_username"]
    proc_type = process_data["process_type"]
    daily_limit = process_data.get("daily_limit")
    batch_delay = process_data.get("batch_delay", 1.0) or 1.0

    # Calculate date range
    if proc_type == "date_range":
        # Resume from current_position_date if available
        resume_date = process_data.get("current_position_date") or process_data.get("from_date")
        from_date = datetime.strptime(process_data["from_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        to_date = datetime.strptime(process_data["to_date"], "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)

        if resume_date and resume_date != process_data["from_date"]:
            # Resume: scrape from resume_date instead of from_date
            effective_from = datetime.strptime(resume_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            log_fn(f"Resuming from {resume_date} (originally from {process_data['from_date']})")
        else:
            effective_from = from_date
    elif proc_type == "rolling":
        hours = process_data.get("hours_back", 24) or 24
        to_date = datetime.now(timezone.utc)
        effective_from = to_date - timedelta(hours=hours)
    else:
        # one_time: same as rolling but default 24h
        hours = process_data.get("hours_back", 24) or 24
        to_date = datetime.now(timezone.utc)
        effective_from = to_date - timedelta(hours=hours)

    # Check daily remaining
    remaining = get_daily_remaining(proc_id)
    if remaining is not None and remaining <= 0:
        log_fn(f"Daily limit reached for today ({daily_limit} messages). Will resume tomorrow.", "warning")
        return []

    log_fn(f"Connecting to Telegram...")
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

    # Update profile with channel title
    update_profile(profile_id, channel_title=title)

    log_fn(f"Channel: {title} (@{username})")
    if proc_type == "date_range":
        log_fn(f"Range: {effective_from.strftime('%Y-%m-%d')} → {to_date.strftime('%Y-%m-%d')}")
    if daily_limit:
        log_fn(f"Daily limit: {daily_limit} | Remaining today: {remaining if remaining is not None else 'unlimited'}")
    log_fn(f"Batch delay: {batch_delay}s")

    all_messages = []
    new_messages = []
    offset_id = 0
    batch_num = 0
    BATCH_SIZE = 200
    scraped_today = 0
    last_msg_date_str = None

    while True:
        # Check kill
        status_key = f"process_{proc_id}"
        if profile_scrape_status.get(status_key, {}).get("kill"):
            log_fn("Stop signal received - pausing process", "warning")
            break

        # Check daily limit
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
                # Extract wait time
                import re
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

        # Batch duplicate check - one query for the whole batch instead of per-message
        batch_ids = [m.id for m in msgs]
        already_scraped = batch_check_scraped(channel, batch_ids)
        dupes_in_batch = len(already_scraped)

        for m in msgs:
            msg_date = m.date.astimezone(timezone.utc)
            if msg_date < effective_from:
                done = True
                break

            # Skip already-scraped messages (batch-checked above)
            if m.id in already_scraped:
                continue

            all_messages.append(m)

            # Track daily limit
            scraped_today += 1
            if remaining is not None and scraped_today >= remaining:
                done = True
                break

        offset_id = msgs[-1].id
        last_msg_date_str = msgs[-1].date.astimezone(timezone.utc).strftime("%Y-%m-%d")

        if batch_num % 3 == 0 or len(all_messages) % 1000 < BATCH_SIZE:
            dupe_note = f" ({dupes_in_batch} dupes skipped)" if dupes_in_batch > 0 else ""
            log_fn(f"  Batch #{batch_num}: {len(all_messages)} new msgs, reached {last_msg_date_str}{dupe_note}")
            socketio.emit('profile_progress', {
                "process_id": proc_id,
                "messages": len(all_messages),
                "batch": batch_num,
                "current_date": last_msg_date_str,
                "dupes_skipped": dupes_in_batch
            })

        if done:
            break
        await asyncio.sleep(batch_delay)

    await client.disconnect()

    log_fn(f"Total new messages: {len(all_messages)}")

    # Process messages into rows
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

    # Mark messages as scraped and update progress
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
        socketio.emit('profile_log', {
            "process_id": proc_id,
            "profile_id": proc["profile_id"],
            "type": msg_type,
            "text": msg,
            "time": datetime.now().strftime("%H:%M:%S")
        })

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        rows = loop.run_until_complete(run_profile_scrape_coroutine(proc, log_fn))

        # Export data
        if rows:
            # Save to local DB backup
            save_messages_locally(rows)

            # Export to local file
            fmt = profile.get("export_format", "xlsx")
            location = profile.get("export_location", "default")
            custom_path = profile.get("export_custom_path")
            filename = f"{profile['channel_username']}_{proc['name'].replace(' ', '_')}"

            result = export_data(rows, fmt, filename, True, location, custom_path)
            if result["success"]:
                log_fn(f"Exported {result['rows']} rows → {result['filepath']}", "success")
                audit("process_export", f"{result['filepath']} ({result['rows']} rows)")
            else:
                log_fn(f"Export failed: {result['error']}", "error")

            # Push to Google Sheets if configured
            if profile.get("push_to_sheets") and SHEET_ID:
                try:
                    push_to_sheets(rows)
                    log_fn(f"Pushed {len(rows)} rows to Google Sheets", "success")
                except Exception as e:
                    log_fn(f"Sheets push failed: {e}", "error")

        total = len(rows) if rows else 0
        log_fn(f"Process complete: {total} new messages", "success")
        audit("process_completed", f"'{proc['name']}': {total} messages")

        # Check if date_range process is complete
        final_status = "idle"
        if proc["process_type"] == "date_range":
            remaining = get_daily_remaining(proc_id)
            if remaining is not None and remaining <= 0:
                final_status = "paused"  # Will resume tomorrow
            elif total == 0:
                final_status = "completed"
        elif proc["process_type"] == "one_time":
            final_status = "completed"

        # Calculate next run
        next_run = None
        if proc.get("schedule_enabled") and final_status != "completed":
            next_run = calculate_next_run(proc)

        update_process(proc_id, status=final_status, next_run_at=next_run)

    except Exception as e:
        log_fn(f"Process error: {str(e)}", "error")
        audit("process_error", f"'{proc['name']}': {str(e)}", "error")
        update_process(proc_id, status="error", error_message=str(e))

    finally:
        profile_scrape_status.pop(status_key, None)
        socketio.emit('profile_process_done', {
            "process_id": proc_id,
            "profile_id": proc["profile_id"]
        })
        loop.close()


@app.route('/api/processes/<int:proc_id>/run', methods=['POST'])
@login_required
def api_run_process(proc_id):
    """Start running a process."""
    global app_enabled
    if not app_enabled:
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


@app.route('/api/processes/<int:proc_id>/stop', methods=['POST'])
@login_required
def api_stop_process(proc_id):
    """Stop a running process."""
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


def check_profile_schedules():
    """Check and run due profile processes. Called by APScheduler."""
    global app_enabled
    if not app_enabled:
        return

    due = get_due_processes()
    for proc in due:
        status_key = f"process_{proc['id']}"
        if status_key not in profile_scrape_status:
            thread = threading.Thread(target=run_profile_process, args=(proc['id'],))
            thread.daemon = True
            thread.start()


if __name__ == '__main__':
    print()
    print("  \033[95m╔══════════════════════════════════════════╗\033[0m")
    print("  \033[95m║\033[0m  \033[1m\033[96mTeleDrive\033[0m — Channel Intelligence     \033[95m║\033[0m")
    print("  \033[95m║\033[0m  http://localhost:5000                  \033[95m║\033[0m")
    print("  \033[95m║\033[0m  Default: admin@teledrive.app / admin123 \033[95m║\033[0m")
    print("  \033[95m╚══════════════════════════════════════════╝\033[0m")
    print()
    
    # Load app enabled state from database
    app_enabled = get_setting("app_enabled", True)
    
    # Load and start scheduler
    load_scheduler_config()
    scheduler.start()
    setup_scheduler()
    
    # Add profile process scheduler (checks every 60 seconds)
    scheduler.add_job(check_profile_schedules, 'interval', seconds=60, id='profile_scheduler',
                      replace_existing=True)
    
    # Cleanup expired sessions periodically
    scheduler.add_job(cleanup_expired_sessions, 'interval', hours=6, id='session_cleanup',
                      replace_existing=True)
    
    from src.auth import get_user_count
    user_count = get_user_count()
    profiles = get_all_profiles()
    print(f"  Users: {user_count} | Profiles: {len(profiles)}")
    if scheduler_config["enabled"]:
        print(f"  Scheduler: ENABLED — Next: {scheduler_config['next_run']}")
    else:
        print("  Scheduler: OFF")
    print()
    
    debug = os.getenv('FLASK_DEBUG', '1') == '1'
    socketio.run(app, host='0.0.0.0', port=int(os.getenv('PORT', 5000)), debug=debug, allow_unsafe_werkzeug=debug)
