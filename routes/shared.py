"""
Shared state, middleware, and helpers used across all route blueprints.
"""
import functools
import threading
from datetime import datetime
from pathlib import Path

from flask import request, jsonify, redirect
from flask_socketio import SocketIO

from src.auth import (
    validate_session, has_permission, get_system_setting,
    get_user_credential, get_effective_theme, theme_to_css,
)
from src.local_db import log_audit
from src.profiles import get_profile


# ═══════════════════════════════════════════════════════════════
# SHARED STATE
# ═══════════════════════════════════════════════════════════════

# SocketIO instance - set by web_app.py after creation
socketio: SocketIO = None

# Threading lock for scraper state
scrape_lock = threading.Lock()

# Global scraper status (for quick scrape from dashboard)
scrape_status = {
    "running": False,
    "progress": 0,
    "current_channel": "",
    "messages": [],
    "results": [],
    "error": None,
    "kill_requested": False,
}

# Track running profile processes
profile_scrape_status = {}

# App enabled state (global kill switch)
app_enabled = True


def set_app_enabled(value: bool):
    global app_enabled
    app_enabled = value


def get_app_enabled() -> bool:
    return app_enabled


# ═══════════════════════════════════════════════════════════════
# AUTH MIDDLEWARE
# ═══════════════════════════════════════════════════════════════

def get_current_user():
    """Get current user from session cookie."""
    token = request.cookies.get("td_session")
    if not token:
        return None
    return validate_session(token)


def audit(action: str, details: str = None, status: str = "success", **kwargs):
    """Log audit with current user context if available."""
    user = getattr(request, 'user', None) if request else None
    uid = kwargs.get('user_id', user["id"] if user else None)
    email = kwargs.get('user_email', user["email"] if user else None)
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


def log_message(msg, msg_type="info"):
    """Send a log message to the frontend via SocketIO."""
    entry = {"type": msg_type, "text": msg, "time": datetime.now().strftime("%H:%M:%S")}
    scrape_status["messages"].append(entry)
    if socketio:
        socketio.emit('log', entry)
