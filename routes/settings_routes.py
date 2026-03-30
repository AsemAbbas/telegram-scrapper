"""Settings, credentials, dashboard stats, audit, exports, app toggle routes."""
import json
from datetime import datetime
from pathlib import Path

from flask import Blueprint, render_template, request, jsonify

from src.config import TG_API_ID, TG_API_HASH, TG_PHONE, TG_SESSION_NAME, SHEET_ID, GOOGLE_CREDS_JSON
from src.auth import get_user_credential, set_user_credential, get_user_credentials
from src.local_db import (
    get_setting, set_setting, get_local_message_count,
    get_audit_log, clear_audit_log,
)
from src.local_export import get_export_files, get_available_formats
from src.profiles import get_all_profiles
from routes.shared import (
    login_required, admin_required, permission_required, audit,
    scrape_status, profile_scrape_status, socketio,
    get_app_enabled, set_app_enabled, log_message,
)

settings_bp = Blueprint('settings', __name__)


SETTINGS_KEYS = {
    "tg_api_id", "tg_api_hash", "tg_phone", "tg_session_name",
    "google_creds_json", "sheet_id",
    "github_token", "github_repo",
}


# ═══════════════════════════════════════════════════════════════
# SHARED CREDENTIALS (read-only for users)
# ═══════════════════════════════════════════════════════════════

@settings_bp.route('/api/settings/shared-credentials', methods=['GET'])
@login_required
def api_get_shared_credentials():
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
        if val and len(str(val)) > 4:
            masked = str(val)[:2] + "\u2022" * min(len(str(val)) - 4, 20) + str(val)[-2:]
            result[key] = masked
        else:
            result[key] = "\u2022\u2022\u2022\u2022" if val else ""
        result[key + "_set"] = bool(val)
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
# USER CREDENTIALS
# ═══════════════════════════════════════════════════════════════

@settings_bp.route('/api/settings/credentials/effective', methods=['GET'])
@login_required
def api_effective_credentials():
    """Get effective credentials for the current user.
    Shows whether each credential type uses 'default' (admin's) or 'custom' (user's own)."""
    user = request.user
    result = {}

    # Telegram credentials
    tg_keys = ["api_id", "api_hash", "phone", "session_name"]
    for key in tg_keys:
        user_val = get_user_credential(user["id"], "telegram", key, "")
        if user_val:
            result[f"telegram.{key}"] = {"source": "custom", "set": True}
        else:
            # Check if admin has defaults
            env_map = {"api_id": str(TG_API_ID) if TG_API_ID else "",
                       "api_hash": TG_API_HASH or "",
                       "phone": TG_PHONE or "",
                       "session_name": TG_SESSION_NAME or ""}
            default_val = get_setting(f"tg_{key}", env_map.get(key, ""))
            result[f"telegram.{key}"] = {"source": "default" if default_val else "none", "set": bool(default_val)}

    # Google
    user_google = get_user_credential(user["id"], "google_oauth", "access_token", "")
    result["google"] = {"source": "oauth" if user_google else "default", "connected": bool(user_google)}

    return jsonify(result)


@settings_bp.route('/api/user/credentials', methods=['GET'])
@login_required
def api_get_user_creds():
    user = request.user
    creds = get_user_credentials(user["id"])
    result = {}
    for key, val in creds.items():
        if any(s in key for s in ["hash", "token", "secret"]) and val and len(val) > 6:
            result[key] = val[:4] + "\u2022" * (len(val) - 6) + val[-2:]
            result[key + "_set"] = True
        elif "creds_json" in key and val:
            result[key] = val if val.endswith(".json") else "(inline JSON)"
            result[key + "_set"] = True
        else:
            result[key] = val
            result[key + "_set"] = bool(val)
    session_name = get_user_credential(user["id"], "telegram", "session_name", f"td_user_{user['id']}")
    result["tg_session_exists"] = Path(session_name + ".session").exists()
    return jsonify(result)


@settings_bp.route('/api/user/credentials', methods=['PUT'])
@login_required
def api_set_user_creds():
    user = request.user
    data = request.json or {}
    updated = []
    import re
    for key, value in data.items():
        if value is not None and not str(value).startswith("\u2022"):
            parts = key.split(".", 1)
            if len(parts) == 2:
                val = str(value).strip()
                # Sanitize session names to prevent path traversal
                if parts[1] == "session_name":
                    val = re.sub(r'[^a-zA-Z0-9_\-]', '_', val)
                set_user_credential(user["id"], parts[0], parts[1], val)
                updated.append(key)
    if updated:
        audit("user_creds_updated", f"User {user['email']} updated: {', '.join(updated)}")
    return jsonify({"success": True, "updated": updated})


# ═══════════════════════════════════════════════════════════════
# TEST CONNECTIONS (User)
# ═══════════════════════════════════════════════════════════════

@settings_bp.route('/api/user/test-telegram', methods=['POST'])
@login_required
def api_user_test_telegram():
    import asyncio as _asyncio
    from telethon import TelegramClient
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


@settings_bp.route('/api/user/test-google', methods=['POST'])
@login_required
def api_user_test_google():
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


# ═══════════════════════════════════════════════════════════════
# ACCOUNT SETTINGS (Admin/Editor)
# ═══════════════════════════════════════════════════════════════

@settings_bp.route('/api/settings/accounts', methods=['GET'])
@permission_required("manage_settings")
def api_get_accounts():
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
        if key in ("tg_api_hash", "github_token") and val and len(val) > 6:
            result[key] = val[:4] + "\u2022" * (len(val) - 6) + val[-2:]
            result[key + "_set"] = True
        elif key == "google_creds_json" and val:
            result[key] = val if val.endswith(".json") else "(inline JSON)"
            result[key + "_set"] = True
        else:
            result[key] = val
            result[key + "_set"] = bool(val)
    result["tg_session_exists"] = Path(get_setting("tg_session_name", TG_SESSION_NAME) + ".session").exists()
    return jsonify(result)


@settings_bp.route('/api/settings/accounts', methods=['PUT'])
@permission_required("manage_settings")
def api_update_accounts():
    data = request.json or {}
    updated = []
    for key in SETTINGS_KEYS:
        if key in data and data[key] is not None:
            val = str(data[key]).strip()
            if val and not val.startswith("\u2022"):
                set_setting(key, val)
                updated.append(key)
    if updated:
        audit("accounts_updated", f"Updated: {', '.join(updated)}")
    return jsonify({"success": True, "updated": updated})


# ═══════════════════════════════════════════════════════════════
# TEST CONNECTIONS (Admin/Editor)
# ═══════════════════════════════════════════════════════════════

@settings_bp.route('/api/settings/test-telegram', methods=['POST'])
@permission_required("test_connections")
def api_test_telegram():
    import asyncio as _asyncio
    from telethon import TelegramClient
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


@settings_bp.route('/api/settings/test-google', methods=['POST'])
@permission_required("test_connections")
def api_test_google():
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
            credentials = Credentials.from_service_account_info(json.loads(creds), scopes=scopes)
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key(sid)
        title = sheet.title
        worksheets = [ws.title for ws in sheet.worksheets()]
        return jsonify({"success": True, "sheet_title": title, "worksheets": worksheets})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════
# DASHBOARD STATS
# ═══════════════════════════════════════════════════════════════

@settings_bp.route('/api/dashboard/stats', methods=['GET'])
@login_required
def api_dashboard_stats():
    user = request.user
    from src.profiles import get_connection as get_prof_conn
    conn = get_prof_conn()
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
    last_process = dict(processes) if processes else None
    success_rate = round((completed / total_procs * 100), 1) if total_procs > 0 else 0
    conn.close()
    return jsonify({
        "total_profiles": profiles, "total_messages": total_msgs,
        "total_processes": total_procs, "success_rate": success_rate,
        "errored_count": errored, "active_schedules": active_schedules,
        "messages_today": today_msgs, "last_process": last_process,
    })


# ═══════════════════════════════════════════════════════════════
# MANAGER OVERVIEW
# ═══════════════════════════════════════════════════════════════

@settings_bp.route('/api/manager/overview', methods=['GET'])
@login_required
def api_manager_overview():
    from src.profiles import get_connection as get_prof_conn
    from routes.scraper_routes import scheduler_config
    conn = get_prof_conn()
    user = request.user
    is_admin = user["role"] == "admin"
    uid = user["id"]

    # Use parameterized queries to prevent SQL injection
    if is_admin:
        profiles = [dict(r) for r in conn.execute("""
            SELECT p.id, p.name, p.channel_username, p.channel_title, p.is_active, p.created_at,
                   COUNT(pr.id) as process_count,
                   COALESCE(SUM(pr.messages_scraped), 0) as total_messages,
                   (SELECT COUNT(*) FROM processes WHERE profile_id = p.id AND status = 'running') as running_count
            FROM profiles p LEFT JOIN processes pr ON p.id = pr.profile_id
            GROUP BY p.id ORDER BY p.updated_at DESC
        """).fetchall()]
        all_processes = [dict(r) for r in conn.execute("""
            SELECT pr.id, pr.profile_id, pr.name, pr.process_type, pr.status,
                   pr.schedule_enabled, pr.schedule_time, pr.schedule_interval_hours,
                   pr.messages_scraped, pr.today_scraped, pr.today_date,
                   pr.daily_limit, pr.from_date, pr.to_date, pr.current_position_date,
                   pr.last_run_at, pr.next_run_at, pr.error_message, pr.created_at,
                   p.channel_username, p.name as profile_name
            FROM processes pr JOIN profiles p ON pr.profile_id = p.id
            ORDER BY pr.status = 'running' DESC, pr.schedule_enabled DESC, pr.updated_at DESC
        """).fetchall()]
    else:
        profiles = [dict(r) for r in conn.execute("""
            SELECT p.id, p.name, p.channel_username, p.channel_title, p.is_active, p.created_at,
                   COUNT(pr.id) as process_count,
                   COALESCE(SUM(pr.messages_scraped), 0) as total_messages,
                   (SELECT COUNT(*) FROM processes WHERE profile_id = p.id AND status = 'running') as running_count
            FROM profiles p LEFT JOIN processes pr ON p.id = pr.profile_id
            WHERE p.user_id = ? GROUP BY p.id ORDER BY p.updated_at DESC
        """, (uid,)).fetchall()]
        all_processes = [dict(r) for r in conn.execute("""
            SELECT pr.id, pr.profile_id, pr.name, pr.process_type, pr.status,
                   pr.schedule_enabled, pr.schedule_time, pr.schedule_interval_hours,
                   pr.messages_scraped, pr.today_scraped, pr.today_date,
                   pr.daily_limit, pr.from_date, pr.to_date, pr.current_position_date,
                   pr.last_run_at, pr.next_run_at, pr.error_message, pr.created_at,
                   p.channel_username, p.name as profile_name
            FROM processes pr JOIN profiles p ON pr.profile_id = p.id
            WHERE p.user_id = ?
            ORDER BY pr.status = 'running' DESC, pr.schedule_enabled DESC, pr.updated_at DESC
        """, (uid,)).fetchall()]

    profile_ids = [p["id"] for p in profiles]
    if profile_ids:
        placeholders = ",".join(["?"] * len(profile_ids))
        total_msgs = conn.execute(
            f"SELECT COUNT(*) as c FROM scraped_messages WHERE profile_id IN ({placeholders})",
            profile_ids
        ).fetchone()["c"]
    else:
        total_msgs = 0 if not is_admin else conn.execute("SELECT COUNT(*) as c FROM scraped_messages").fetchone()["c"]
    status_counts = {}
    for row in conn.execute("SELECT status, COUNT(*) as c FROM processes GROUP BY status").fetchall():
        status_counts[row["status"]] = row["c"]
    scheduled_count = conn.execute("SELECT COUNT(*) as c FROM processes WHERE schedule_enabled = 1").fetchone()["c"]
    conn.close()
    global_sched = {
        "enabled": scheduler_config.get("enabled", False),
        "time": scheduler_config.get("time"),
        "interval_hours": scheduler_config.get("interval_hours"),
        "next_run": scheduler_config.get("next_run"),
        "last_run": scheduler_config.get("last_run"),
    }
    return jsonify({
        "profiles": profiles, "profile_count": len(profiles),
        "active_profiles": sum(1 for p in profiles if p["is_active"]),
        "processes": all_processes, "process_count": len(all_processes),
        "status_counts": status_counts, "scheduled_count": scheduled_count,
        "total_messages": total_msgs, "global_scheduler": global_sched,
        "app_enabled": get_app_enabled(),
    })


# ═══════════════════════════════════════════════════════════════
# AUDIT LOG
# ═══════════════════════════════════════════════════════════════

@settings_bp.route('/api/audit', methods=['GET'])
@login_required
def get_audit():
    limit = min(request.args.get('limit', 100, type=int), 1000)
    user = request.user
    if user["role"] == "admin":
        logs = get_audit_log(limit)
    else:
        logs = get_audit_log(limit, user_id=user["id"])
    return jsonify(logs)


@settings_bp.route('/api/audit', methods=['DELETE'])
@admin_required
def clear_audit():
    clear_audit_log()
    audit("audit_log_cleared", "User cleared audit log")
    return jsonify({"success": True})


# ═══════════════════════════════════════════════════════════════
# APP ENABLE/DISABLE
# ═══════════════════════════════════════════════════════════════

@settings_bp.route('/api/app/status', methods=['GET'])
@login_required
def get_app_status():
    local_stats = get_local_message_count()
    return jsonify({"enabled": get_app_enabled(), "local_messages": local_stats})


@settings_bp.route('/api/app/toggle', methods=['POST'])
@admin_required
def toggle_app():
    enabled = not get_app_enabled()
    set_app_enabled(enabled)
    set_setting("app_enabled", enabled)
    status = "enabled" if enabled else "disabled"
    audit(f"app_{status}", f"User {status} the app")
    if not enabled:
        if scrape_status["running"]:
            scrape_status["kill_requested"] = True
        for sk, sv in profile_scrape_status.items():
            sv["kill"] = True
        if scrape_status["running"] or profile_scrape_status:
            log_message("APP DISABLED - All running processes killed.", "error")
        if socketio:
            socketio.emit('app_disabled', {"enabled": False})
    else:
        if socketio:
            socketio.emit('app_enabled', {"enabled": True})
    return jsonify({"enabled": enabled})


# ═══════════════════════════════════════════════════════════════
# RESET / LOCAL STATS / EXPORTS
# ═══════════════════════════════════════════════════════════════

@settings_bp.route('/api/reset', methods=['POST'])
@admin_required
def reset_settings():
    from routes.scraper_routes import scheduler_config, save_scheduler_config, setup_scheduler
    scheduler_config.update({
        "enabled": False, "time": "08:00",
        "interval_hours": None, "last_run": None, "next_run": None,
    })
    save_scheduler_config()
    setup_scheduler()
    set_app_enabled(True)
    set_setting("app_enabled", True)
    audit("settings_reset", "User reset all settings to defaults")
    return jsonify({"success": True, "message": "Settings reset to defaults (channels preserved)"})


@settings_bp.route('/api/local-stats', methods=['GET'])
@login_required
def get_local_stats():
    return jsonify(get_local_message_count())


@settings_bp.route('/api/exports', methods=['GET'])
@login_required
def list_exports():
    location = request.args.get('location', 'default')
    custom_path = request.args.get('custom_path', None)
    files = get_export_files(location, custom_path)
    return jsonify(files)


@settings_bp.route('/api/export-formats', methods=['GET'])
@login_required
def get_formats():
    return jsonify(get_available_formats())
