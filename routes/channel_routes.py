"""Channel management routes."""
import json
from pathlib import Path
from flask import Blueprint, request, jsonify

from src.config import load_channels
from src.profiles import (
    create_channel, get_channel, get_all_channels, update_channel, delete_channel,
    get_channel_scraped_stats,
)
from routes.shared import login_required, admin_required, audit

channel_bp = Blueprint('channels', __name__)


# ═══════════════════════════════════════════════════════════════
# MANAGED CHANNELS (Database-backed)
# ═══════════════════════════════════════════════════════════════

@channel_bp.route('/api/managed-channels', methods=['GET'])
@login_required
def api_get_managed_channels():
    user = request.user
    if user["role"] == "admin":
        return jsonify(get_all_channels())
    return jsonify(get_all_channels(user_id=user["id"]))


@channel_bp.route('/api/managed-channels', methods=['POST'])
@login_required
def api_create_managed_channel():
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


@channel_bp.route('/api/managed-channels/<int:cid>', methods=['GET'])
@login_required
def api_get_managed_channel(cid):
    ch = get_channel(cid)
    if not ch:
        return jsonify({"error": "Channel not found"}), 404
    stats = get_channel_scraped_stats(ch["username"])
    ch["stats"] = stats
    return jsonify(ch)


@channel_bp.route('/api/managed-channels/<int:cid>', methods=['PUT'])
@login_required
def api_update_managed_channel(cid):
    ch = get_channel(cid)
    if not ch:
        return jsonify({"error": "Channel not found"}), 404
    # Only admin or the creator can modify
    if request.user["role"] != "admin" and ch.get("created_by") != request.user["id"]:
        return jsonify({"error": "Access denied"}), 403
    data = request.json or {}
    result = update_channel(cid, **data)
    return jsonify(result)


@channel_bp.route('/api/managed-channels/<int:cid>', methods=['DELETE'])
@login_required
def api_delete_managed_channel(cid):
    ch = get_channel(cid)
    if not ch:
        return jsonify({"error": "Channel not found"}), 404
    # Only admin or the creator can delete
    if request.user["role"] != "admin" and ch.get("created_by") != request.user["id"]:
        return jsonify({"error": "Access denied"}), 403
    audit("channel_deleted", f"Channel @{ch['username']} deleted")
    result = delete_channel(cid)
    return jsonify(result), 200 if result["success"] else 400


# ═══════════════════════════════════════════════════════════════
# CHANNELS CONFIG (JSON file-based, for quick scrape)
# ═══════════════════════════════════════════════════════════════

@channel_bp.route('/api/channels', methods=['GET'])
@login_required
def get_channels_config():
    channels = load_channels()
    return jsonify(channels)


@channel_bp.route('/api/channels', methods=['POST'])
@admin_required
def save_channels_config():
    data = request.json
    channels_path = Path(__file__).parent.parent / "channels.json"
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
