"""Admin routes: user management, system settings, themes, stats."""
from flask import Blueprint, render_template, request, jsonify

from src.auth import (
    get_all_users, get_user, create_user, update_user, delete_user, get_user_count,
    get_system_setting, set_system_setting, get_all_system_settings,
    get_all_plans, get_user_credential, set_user_credential,
    get_site_theme, DEFAULT_THEME, PRESET_THEMES,
    get_available_roles, get_pending_users, approve_user, reject_user,
    add_telegram_number, get_all_telegram_numbers, update_telegram_number,
    delete_telegram_number, mark_telegram_number_failed, reset_telegram_number_fails,
    create_plan, update_plan, delete_plan, assign_user_plan,
)
from src.profiles import get_all_profiles
from src.local_db import get_local_message_count
from routes.shared import login_required, admin_required, audit

admin_bp = Blueprint('admin', __name__)


@admin_bp.route('/admin')
@admin_required
def admin_page():
    return render_template('admin.html', user=request.user)


# ═══════════════════════════════════════════════════════════════
# USER MANAGEMENT
# ═══════════════════════════════════════════════════════════════

@admin_bp.route('/api/admin/users', methods=['GET'])
@admin_required
def api_admin_users():
    users = get_all_users()
    for u in users:
        u["theme_enabled"] = get_user_credential(u["id"], "theme", "_enabled", "false") == "true"
    return jsonify(users)


@admin_bp.route('/api/admin/users/<int:uid>', methods=['PUT'])
@admin_required
def api_admin_update_user(uid):
    data = request.json or {}
    result = update_user(uid, **data)
    if result["success"]:
        audit("admin_user_updated", f"Admin updated user #{uid}: {data}")
    return jsonify(result)


@admin_bp.route('/api/admin/users/<int:uid>', methods=['DELETE'])
@admin_required
def api_admin_delete_user(uid):
    if uid == request.user["id"]:
        return jsonify({"error": "Cannot delete yourself"}), 400
    user = get_user(uid)
    result = delete_user(uid)
    if result["success"] and user:
        audit("admin_user_deleted", f"Admin deleted user {user['email']}")
    return jsonify(result)


@admin_bp.route('/api/admin/users', methods=['POST'])
@admin_required
def api_admin_create_user():
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


# ═══════════════════════════════════════════════════════════════
# SYSTEM SETTINGS
# ═══════════════════════════════════════════════════════════════

@admin_bp.route('/api/admin/settings', methods=['GET'])
@admin_required
def api_admin_get_settings():
    settings = get_all_system_settings()
    plans = get_all_plans()
    user_count = get_user_count()
    return jsonify({"settings": settings, "plans": plans, "user_count": user_count})


@admin_bp.route('/api/admin/settings', methods=['PUT'])
@admin_required
def api_admin_update_settings():
    data = request.json or {}
    for key, value in data.items():
        set_system_setting(key, str(value))
    audit("admin_settings_updated", f"Updated: {', '.join(data.keys())}")
    return jsonify({"success": True})


# ═══════════════════════════════════════════════════════════════
# THEME MANAGEMENT
# ═══════════════════════════════════════════════════════════════

@admin_bp.route('/api/admin/theme', methods=['GET'])
@admin_required
def api_admin_get_theme():
    theme = get_site_theme()
    allow_user = get_system_setting("allow_user_themes", "false") == "true"
    return jsonify({"theme": theme, "allow_user_themes": allow_user, "defaults": DEFAULT_THEME})


@admin_bp.route('/api/admin/theme', methods=['PUT'])
@admin_required
def api_admin_update_theme():
    data = request.json or {}
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


@admin_bp.route('/api/admin/theme/reset', methods=['POST'])
@admin_required
def api_admin_reset_theme():
    for key, value in DEFAULT_THEME.items():
        set_system_setting(f"theme_{key}", value)
    audit("theme_reset", "Theme reset to defaults")
    return jsonify({"success": True})


@admin_bp.route('/api/admin/users/<int:uid>/theme', methods=['PUT'])
@admin_required
def api_admin_toggle_user_theme(uid):
    data = request.json or {}
    enabled = data.get("enabled", False)
    set_user_credential(uid, "theme", "_enabled", "true" if enabled else "false")
    audit("user_theme_toggled", f"User {uid}: custom theme {'enabled' if enabled else 'disabled'}")
    return jsonify({"success": True})


# ═══════════════════════════════════════════════════════════════
# STATS & MISC
# ═══════════════════════════════════════════════════════════════

@admin_bp.route('/api/admin/stats', methods=['GET'])
@admin_required
def api_admin_stats():
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


@admin_bp.route('/api/admin/roles', methods=['GET'])
@admin_required
def api_admin_roles():
    return jsonify(get_available_roles())


# ═══════════════════════════════════════════════════════════════
# PENDING USER APPROVAL
# ═══════════════════════════════════════════════════════════════

@admin_bp.route('/api/admin/pending-users', methods=['GET'])
@admin_required
def api_admin_pending_users():
    return jsonify(get_pending_users())


@admin_bp.route('/api/admin/users/<int:uid>/approve', methods=['POST'])
@admin_required
def api_admin_approve_user(uid):
    user = get_user(uid)
    result = approve_user(uid)
    if result["success"] and user:
        audit("user_approved", f"Admin approved user {user['email']}")
    return jsonify(result)


@admin_bp.route('/api/admin/users/<int:uid>/reject', methods=['POST'])
@admin_required
def api_admin_reject_user(uid):
    user = get_user(uid)
    result = reject_user(uid)
    if result["success"] and user:
        audit("user_rejected", f"Admin rejected user {user['email']}")
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
# TELEGRAM BACKUP NUMBERS
# ═══════════════════════════════════════════════════════════════

@admin_bp.route('/api/admin/telegram-numbers', methods=['GET'])
@admin_required
def api_get_telegram_numbers():
    return jsonify(get_all_telegram_numbers())


@admin_bp.route('/api/admin/telegram-numbers', methods=['POST'])
@admin_required
def api_create_telegram_number():
    data = request.json or {}
    phone = data.get("phone", "").strip()
    if not phone:
        return jsonify({"error": "Phone number is required"}), 400
    session_name = data.get("session_name", f"tg_backup_{phone.replace('+', '').replace(' ', '')}")
    result = add_telegram_number(
        phone=phone,
        session_name=session_name,
        api_id=data.get("api_id"),
        api_hash=data.get("api_hash"),
        is_primary=data.get("is_primary", False),
    )
    if result["success"]:
        audit("telegram_number_added", f"Added backup number {phone}")
    return jsonify(result), 200 if result["success"] else 400


@admin_bp.route('/api/admin/telegram-numbers/<int:num_id>', methods=['PUT'])
@admin_required
def api_update_telegram_number(num_id):
    data = request.json or {}
    result = update_telegram_number(num_id, **data)
    if result["success"]:
        audit("telegram_number_updated", f"Updated Telegram number #{num_id}")
    return jsonify(result)


@admin_bp.route('/api/admin/telegram-numbers/<int:num_id>', methods=['DELETE'])
@admin_required
def api_delete_telegram_number(num_id):
    result = delete_telegram_number(num_id)
    if result["success"]:
        audit("telegram_number_deleted", f"Deleted Telegram number #{num_id}")
    return jsonify(result)


@admin_bp.route('/api/admin/telegram-numbers/<int:num_id>/reset-fails', methods=['POST'])
@admin_required
def api_reset_telegram_fails(num_id):
    result = reset_telegram_number_fails(num_id)
    audit("telegram_fails_reset", f"Reset fail count for number #{num_id}")
    return jsonify(result)


@admin_bp.route('/api/admin/telegram-numbers/<int:num_id>/test', methods=['POST'])
@admin_required
def api_test_telegram_number(num_id):
    """Test connection for a specific backup number."""
    import asyncio as _asyncio
    from telethon import TelegramClient
    from src.config import TG_API_ID, TG_API_HASH
    numbers = get_all_telegram_numbers()
    num = next((n for n in numbers if n["id"] == num_id), None)
    if not num:
        return jsonify({"success": False, "error": "Number not found"})
    api_id = num.get("api_id") or str(TG_API_ID)
    api_hash = num.get("api_hash") or TG_API_HASH
    try:
        loop = _asyncio.new_event_loop()
        from src.config import get_session_path
        client = TelegramClient(get_session_path(num["session_name"]), int(api_id), api_hash)
        loop.run_until_complete(client.start(phone=num["phone"]))
        me = loop.run_until_complete(client.get_me())
        loop.run_until_complete(client.disconnect())
        loop.close()
        name = f"{me.first_name or ''} {me.last_name or ''}".strip()
        reset_telegram_number_fails(num_id)
        return jsonify({"success": True, "user": name, "phone": num["phone"]})
    except Exception as e:
        mark_telegram_number_failed(num_id, str(e))
        return jsonify({"success": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════
# SUBSCRIPTION PLAN MANAGEMENT
# ═══════════════════════════════════════════════════════════════

@admin_bp.route('/api/admin/plans', methods=['POST'])
@admin_required
def api_create_plan():
    data = request.json or {}
    name = data.get("name", "").strip()
    display_name = data.get("display_name", "").strip()
    if not name or not display_name:
        return jsonify({"error": "Plan name and display name are required"}), 400
    result = create_plan(name=name, display_name=display_name, **{
        k: v for k, v in data.items() if k not in ("name", "display_name")
    })
    if result["success"]:
        audit("plan_created", f"Created subscription plan: {display_name}")
    return jsonify(result), 200 if result["success"] else 400


@admin_bp.route('/api/admin/plans/<int:plan_id>', methods=['PUT'])
@admin_required
def api_update_plan(plan_id):
    data = request.json or {}
    result = update_plan(plan_id, **data)
    if result["success"]:
        audit("plan_updated", f"Updated plan #{plan_id}")
    return jsonify(result)


@admin_bp.route('/api/admin/plans/<int:plan_id>', methods=['DELETE'])
@admin_required
def api_delete_plan(plan_id):
    result = delete_plan(plan_id)
    audit("plan_deactivated", f"Deactivated plan #{plan_id}")
    return jsonify(result)


@admin_bp.route('/api/admin/users/<int:uid>/plan', methods=['PUT'])
@admin_required
def api_assign_user_plan(uid):
    data = request.json or {}
    plan_name = data.get("plan", "free")
    expires_at = data.get("expires_at")
    result = assign_user_plan(uid, plan_name, expires_at)
    if result["success"]:
        audit("user_plan_assigned", f"Assigned plan '{plan_name}' to user #{uid}")
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
# USER-FACING SUBSCRIPTION PLANS
# ═══════════════════════════════════════════════════════════════

@admin_bp.route('/api/subscriptions/plans', methods=['GET'])
@login_required
def api_get_subscription_plans():
    """Get subscription plans, user's current plan, and usage info."""
    from src.auth import get_plan, get_system_setting, get_connection
    plans = get_all_plans()
    active_plans = [p for p in plans if p.get("is_active")]
    subs_enabled = get_system_setting("subscriptions_enabled", "false") == "true"
    user_plan_name = request.user.get("subscription_plan", "free")
    current_plan = get_plan(user_plan_name)
    # Get today's message count for usage (scraped_messages via profile ownership)
    from datetime import date
    today = date.today().isoformat()
    messages_today = 0
    try:
        from src.profiles import get_connection as get_td_conn
        conn2 = get_td_conn()
        row = conn2.execute(
            """SELECT COUNT(*) as cnt FROM scraped_messages sm
               JOIN profiles p ON sm.profile_id = p.id
               WHERE sm.scraped_at >= ? AND p.user_id = ?""",
            (today, request.user["id"])
        ).fetchone()
        conn2.close()
        messages_today = row["cnt"] if row else 0
    except Exception:
        pass
    return jsonify({
        "plans": active_plans,
        "current_plan": current_plan,
        "enabled": subs_enabled,
        "usage": {"messages_today": messages_today}
    })
