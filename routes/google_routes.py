"""Google OAuth connect, sheet listing, and management routes."""
import json
from flask import Blueprint, request, jsonify, redirect, url_for, session

from src.auth import (
    get_system_setting, get_user_credential, set_user_credential,
    delete_user_credentials,
)
from src.sheets import (
    list_user_spreadsheets, create_spreadsheet, get_sheet_tabs,
    check_google_connection,
)
from routes.shared import login_required, audit

google_bp = Blueprint('google', __name__)

SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


# ═══════════════════════════════════════════════════════════════
# OAUTH CONNECT FLOW
# ═══════════════════════════════════════════════════════════════

@google_bp.route('/auth/google/connect')
@login_required
def google_connect():
    """Initiate Google OAuth flow with Sheets/Drive scopes."""
    client_id = get_system_setting("google_oauth_client_id", "")
    if not client_id:
        return jsonify({"error": "Google OAuth not configured by admin"}), 400

    from google_auth_oauthlib.flow import Flow

    flow = Flow.from_client_config(
        {
            "web": {
                "client_id": client_id,
                "client_secret": get_system_setting("google_oauth_client_secret", ""),
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [_get_redirect_uri()],
            }
        },
        scopes=SCOPES,
    )
    flow.redirect_uri = _get_redirect_uri()

    authorization_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true',
        prompt='consent',
    )
    # Store state in session for CSRF protection
    session['google_oauth_state'] = state
    return redirect(authorization_url)


@google_bp.route('/auth/google/callback')
@login_required
def google_callback():
    """Handle Google OAuth callback, store tokens."""
    # Validate CSRF state parameter
    stored_state = session.pop('google_oauth_state', None)
    received_state = request.args.get('state')
    if not stored_state or stored_state != received_state:
        return jsonify({"error": "Invalid OAuth state. Please try connecting again."}), 403

    client_id = get_system_setting("google_oauth_client_id", "")
    client_secret = get_system_setting("google_oauth_client_secret", "")

    from google_auth_oauthlib.flow import Flow

    flow = Flow.from_client_config(
        {
            "web": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [_get_redirect_uri()],
            }
        },
        scopes=SCOPES,
    )
    flow.redirect_uri = _get_redirect_uri()

    # Exchange authorization code for tokens
    flow.fetch_token(authorization_response=request.url)
    credentials = flow.credentials

    user_id = request.user["id"]

    # Store tokens in user_credentials
    set_user_credential(user_id, "google_oauth", "access_token", credentials.token)
    if credentials.refresh_token:
        set_user_credential(user_id, "google_oauth", "refresh_token", credentials.refresh_token)
    if credentials.expiry:
        set_user_credential(user_id, "google_oauth", "expires_at", credentials.expiry.isoformat())
    set_user_credential(user_id, "google_oauth", "scopes", json.dumps(list(credentials.scopes or [])))

    audit("google_connected", "User connected Google account for Sheets access")

    # Redirect back to dashboard settings
    return redirect('/dashboard#settings')


def _get_redirect_uri():
    """Build the OAuth redirect URI."""
    return request.url_root.rstrip('/') + '/auth/google/callback'


# ═══════════════════════════════════════════════════════════════
# GOOGLE STATUS & DISCONNECT
# ═══════════════════════════════════════════════════════════════

@google_bp.route('/api/google/status', methods=['GET'])
@login_required
def api_google_status():
    """Check if current user has valid Google OAuth connection."""
    result = check_google_connection(request.user["id"])
    return jsonify(result)


@google_bp.route('/api/google/disconnect', methods=['POST'])
@login_required
def api_google_disconnect():
    """Remove user's Google OAuth tokens."""
    user_id = request.user["id"]
    from src.auth import get_connection
    conn = get_connection()
    conn.execute(
        "DELETE FROM user_credentials WHERE user_id = ? AND credential_type = 'google_oauth'",
        (user_id,)
    )
    conn.commit()
    conn.close()
    audit("google_disconnected", "User disconnected Google account")
    return jsonify({"success": True})


# ═══════════════════════════════════════════════════════════════
# SHEET PICKER ENDPOINTS
# ═══════════════════════════════════════════════════════════════

@google_bp.route('/api/google/sheets', methods=['GET'])
@login_required
def api_list_sheets():
    """List user's Google Sheets."""
    result = list_user_spreadsheets(request.user["id"])
    if isinstance(result, dict) and "error" in result:
        return jsonify(result), 400
    return jsonify(result)


@google_bp.route('/api/google/sheets', methods=['POST'])
@login_required
def api_create_sheet():
    """Create a new Google Sheet."""
    data = request.json or {}
    title = data.get("title", "").strip()
    if not title:
        return jsonify({"error": "Sheet title is required"}), 400
    result = create_spreadsheet(request.user["id"], title)
    if result.get("success"):
        audit("google_sheet_created", f"Created sheet: {title}")
    return jsonify(result), 200 if result.get("success") else 400


@google_bp.route('/api/google/sheets/<sheet_id>/tabs', methods=['GET'])
@login_required
def api_get_sheet_tabs(sheet_id):
    """Get worksheet tabs for a specific sheet."""
    result = get_sheet_tabs(request.user["id"], sheet_id)
    if isinstance(result, dict) and "error" in result:
        return jsonify(result), 400
    return jsonify(result)
