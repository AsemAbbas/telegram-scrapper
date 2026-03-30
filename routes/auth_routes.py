"""Authentication routes: login, register, Google OAuth, password, security questions, theme."""
from flask import Blueprint, render_template, request, jsonify, redirect, make_response

from src.auth import (
    authenticate_user, authenticate_google, create_user, get_user,
    create_session, invalidate_session, change_password, verify_password,
    get_system_setting, get_user_plan_limits, update_user,
    get_user_credential, set_user_credential,
    get_site_theme, get_user_theme, set_user_theme, clear_user_theme,
    PRESET_THEMES, SECURITY_QUESTIONS,
    set_security_question, get_security_question, reset_password_with_security,
    get_preset_themes,
)
from src.local_db import log_audit
from routes.shared import get_current_user, login_required, audit
import os

auth_bp = Blueprint('auth', __name__)

COOKIE_SECURE = os.getenv('COOKIE_SECURE', 'true') == 'true'


# ═══════════════════════════════════════════════════════════════
# PAGE ROUTES
# ═══════════════════════════════════════════════════════════════

@auth_bp.route('/login')
def login_page():
    user = get_current_user()
    if user:
        return redirect('/dashboard')
    google_enabled = get_system_setting("google_oauth_enabled", "false") == "true"
    reg_enabled = get_system_setting("registration_enabled", "true") == "true"
    return render_template('login.html', google_enabled=google_enabled, registration_enabled=reg_enabled)


@auth_bp.route('/forgot-password')
def forgot_password_page():
    user = get_current_user()
    if user:
        return redirect('/dashboard')
    return render_template('forgot-password.html', security_questions=SECURITY_QUESTIONS)


@auth_bp.route('/register')
def register_page():
    user = get_current_user()
    if user:
        return redirect('/dashboard')
    reg_enabled = get_system_setting("registration_enabled", "true") == "true"
    if not reg_enabled:
        return redirect('/login')
    google_enabled = get_system_setting("google_oauth_enabled", "false") == "true"
    return render_template('register.html', google_enabled=google_enabled, security_questions=SECURITY_QUESTIONS)


# ═══════════════════════════════════════════════════════════════
# AUTH API
# ═══════════════════════════════════════════════════════════════

@auth_bp.route('/api/auth/login', methods=['POST'])
def api_login():
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


@auth_bp.route('/api/auth/register', methods=['POST'])
def api_register():
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


@auth_bp.route('/api/auth/google', methods=['POST'])
def api_google_login():
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


@auth_bp.route('/api/auth/logout', methods=['POST'])
def api_logout():
    token = request.cookies.get("td_session")
    if token:
        invalidate_session(token)
    resp = make_response(jsonify({"success": True}))
    resp.delete_cookie("td_session")
    return resp


@auth_bp.route('/api/auth/me', methods=['GET'])
@login_required
def api_me():
    user = request.user
    limits = get_user_plan_limits(user["id"])
    # Whitelist fields to prevent leaking internal data
    safe_fields = {"id", "email", "name", "role", "avatar_url", "auth_provider",
                   "subscription_plan", "subscription_expires_at", "is_active",
                   "created_at", "last_login_at"}
    safe_user = {k: v for k, v in user.items() if k in safe_fields}
    return jsonify({**safe_user, "limits": limits})


@auth_bp.route('/api/auth/profile', methods=['PUT'])
@login_required
def api_update_user_profile():
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


@auth_bp.route('/api/auth/password', methods=['PUT'])
@login_required
def api_change_password():
    data = request.json or {}
    current_pass = data.get("current_password", "")
    new_pass = data.get("new_password", "")
    if not current_pass:
        return jsonify({"error": "Current password is required"}), 400
    if len(new_pass) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400
    from src.auth import get_connection as get_auth_conn
    conn = get_auth_conn()
    row = conn.execute("SELECT password_hash FROM users WHERE id = ?", (request.user["id"],)).fetchone()
    conn.close()
    if not row or not row["password_hash"] or not verify_password(current_pass, row["password_hash"]):
        return jsonify({"error": "Current password is incorrect"}), 403
    result = change_password(request.user["id"], new_pass)
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
# FORGOT PASSWORD / SECURITY QUESTIONS
# ═══════════════════════════════════════════════════════════════

@auth_bp.route('/api/auth/forgot-password/verify', methods=['POST'])
def api_forgot_verify():
    data = request.json or {}
    email = data.get("email", "").strip()
    if not email:
        return jsonify({"error": "Email is required"}), 400
    question = get_security_question(email)
    if not question:
        return jsonify({"error": "No security question set for this account. Contact your administrator."}), 404
    return jsonify({"question": question})


@auth_bp.route('/api/auth/forgot-password/reset', methods=['POST'])
def api_forgot_reset():
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


@auth_bp.route('/api/auth/security-question', methods=['PUT'])
@login_required
def api_update_security_question():
    # Check if user already has a security question set - if so, block changes
    from src.auth import get_connection
    conn = get_connection()
    row = conn.execute("SELECT security_question FROM users WHERE id = ?", (request.user["id"],)).fetchone()
    conn.close()
    if row and row["security_question"]:
        return jsonify({"error": "Security question is already set and cannot be changed."}), 403
    data = request.json or {}
    question = data.get("question", "")
    answer = data.get("answer", "")
    if not question or not answer:
        return jsonify({"error": "Question and answer are required"}), 400
    set_security_question(request.user["id"], question, answer)
    audit("security_question_set", "User set security question")
    return jsonify({"success": True})


@auth_bp.route('/api/auth/security-questions', methods=['GET'])
@login_required
def api_get_security_questions():
    """Returns question list + whether user already has one set."""
    from src.auth import get_connection
    conn = get_connection()
    row = conn.execute("SELECT security_question FROM users WHERE id = ?", (request.user["id"],)).fetchone()
    conn.close()
    has_question = bool(row and row["security_question"])
    current_question = row["security_question"] if has_question else None
    return jsonify({"questions": SECURITY_QUESTIONS, "has_question": has_question, "current_question": current_question})


# ═══════════════════════════════════════════════════════════════
# USER THEME ROUTES
# ═══════════════════════════════════════════════════════════════

@auth_bp.route('/api/user/theme', methods=['GET'])
@login_required
def api_user_get_theme():
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


@auth_bp.route('/api/user/theme', methods=['PUT'])
@login_required
def api_user_update_theme():
    user = request.user
    allow = get_system_setting("allow_user_themes", "false") == "true"
    enabled = get_user_credential(user["id"], "theme", "_enabled", "false") == "true"
    if not allow or not enabled:
        return jsonify({"error": "Custom themes not enabled for your account"}), 403
    data = request.json or {}
    set_user_theme(user["id"], data)
    return jsonify({"success": True})


@auth_bp.route('/api/user/theme/reset', methods=['POST'])
@login_required
def api_user_reset_theme():
    clear_user_theme(request.user["id"])
    return jsonify({"success": True})


@auth_bp.route('/api/themes/presets', methods=['GET'])
@login_required
def api_get_theme_presets():
    return jsonify(get_preset_themes())


@auth_bp.route('/api/user/theme/preset', methods=['PUT'])
@login_required
def api_user_apply_preset():
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
