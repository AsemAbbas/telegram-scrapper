"""Profile and Process CRUD routes."""
from flask import Blueprint, request, jsonify

from src.profiles import (
    create_profile, get_profile, get_all_profiles, update_profile, delete_profile,
    create_process, get_process, get_profile_processes, update_process, delete_process,
    get_process_progress, get_channel_scraped_stats,
)
from src.auth import get_user_plan_limits
from routes.shared import login_required, audit, _check_profile_ownership

profile_bp = Blueprint('profiles', __name__)


# ═══════════════════════════════════════════════════════════════
# PROFILE CRUD
# ═══════════════════════════════════════════════════════════════

@profile_bp.route('/api/profiles', methods=['GET'])
@login_required
def api_get_profiles():
    user = request.user
    if user["role"] == "admin":
        return jsonify(get_all_profiles())
    return jsonify(get_all_profiles(user_id=user["id"]))


@profile_bp.route('/api/profiles', methods=['POST'])
@login_required
def api_create_profile():
    # Check subscription limits
    limits = get_user_plan_limits(request.user["id"])
    max_profiles = limits.get("max_profiles", -1)
    if max_profiles != -1:
        current = len(get_all_profiles(user_id=request.user["id"]))
        if current >= max_profiles:
            return jsonify({"error": f"Profile limit reached ({max_profiles}). Upgrade your plan for more."}), 403
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
        sheet_id=data.get("sheet_id"),
        sheet_url=data.get("sheet_url"),
        sheet_tab_name=data.get("sheet_tab_name"),
    )
    if result["success"]:
        profile_id = result["id"]
        audit("profile_created", f"Profile '{data.get('name')}' for @{data.get('channel_username')}")

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


@profile_bp.route('/api/profiles/<int:pid>', methods=['GET'])
@login_required
def api_get_profile(pid):
    profile = get_profile(pid)
    if not profile:
        return jsonify({"error": "Profile not found"}), 404
    if not _check_profile_ownership(profile, request.user):
        return jsonify({"error": "Access denied"}), 403
    profile["processes"] = get_profile_processes(pid)
    return jsonify(profile)


@profile_bp.route('/api/profiles/<int:pid>', methods=['PUT'])
@login_required
def api_update_profile(pid):
    profile = get_profile(pid)
    if not profile:
        return jsonify({"error": "Profile not found"}), 404
    if not _check_profile_ownership(profile, request.user):
        return jsonify({"error": "Access denied"}), 403
    data = request.json or {}
    result = update_profile(pid, **data)
    return jsonify(result)


@profile_bp.route('/api/profiles/<int:pid>', methods=['DELETE'])
@login_required
def api_delete_profile(pid):
    profile = get_profile(pid)
    if not profile:
        return jsonify({"error": "Profile not found"}), 404
    if not _check_profile_ownership(profile, request.user):
        return jsonify({"error": "Access denied"}), 403
    audit("profile_deleted", f"Deleted profile '{profile['name']}'")
    result = delete_profile(pid)
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
# PROCESS CRUD
# ═══════════════════════════════════════════════════════════════

@profile_bp.route('/api/profiles/<int:pid>/processes', methods=['GET'])
@login_required
def api_get_processes(pid):
    profile = get_profile(pid)
    if not profile:
        return jsonify({"error": "Profile not found"}), 404
    if not _check_profile_ownership(profile, request.user):
        return jsonify({"error": "Access denied"}), 403
    return jsonify(get_profile_processes(pid))


@profile_bp.route('/api/profiles/<int:pid>/processes', methods=['POST'])
@login_required
def api_create_process(pid):
    profile = get_profile(pid)
    if not profile:
        return jsonify({"error": "Profile not found"}), 404
    if not _check_profile_ownership(profile, request.user):
        return jsonify({"error": "Access denied"}), 403
    # Check subscription limits
    limits = get_user_plan_limits(request.user["id"])
    max_procs = limits.get("max_processes", -1)
    if max_procs != -1:
        from src.profiles import get_connection as get_prof_conn
        conn = get_prof_conn()
        current = conn.execute("""
            SELECT COUNT(*) as c FROM processes pr JOIN profiles p ON pr.profile_id = p.id
            WHERE p.user_id = ?
        """, (request.user["id"],)).fetchone()["c"]
        conn.close()
        if current >= max_procs:
            return jsonify({"error": f"Process limit reached ({max_procs}). Upgrade your plan for more."}), 403
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


@profile_bp.route('/api/processes/<int:proc_id>', methods=['GET'])
@login_required
def api_get_process(proc_id):
    progress = get_process_progress(proc_id)
    if not progress:
        return jsonify({"error": "Process not found"}), 404
    profile = get_profile(progress.get("profile_id"))
    if not _check_profile_ownership(profile, request.user):
        return jsonify({"error": "Access denied"}), 403
    channel = progress.get("channel_username", "")
    if channel:
        progress["channel_stats"] = get_channel_scraped_stats(channel)
    return jsonify(progress)


@profile_bp.route('/api/processes/<int:proc_id>', methods=['PUT'])
@login_required
def api_update_process(proc_id):
    proc = get_process(proc_id)
    if not proc:
        return jsonify({"error": "Process not found"}), 404
    profile = get_profile(proc.get("profile_id"))
    if not _check_profile_ownership(profile, request.user):
        return jsonify({"error": "Access denied"}), 403
    data = request.json or {}
    result = update_process(proc_id, **data)
    return jsonify(result)


@profile_bp.route('/api/processes/<int:proc_id>', methods=['DELETE'])
@login_required
def api_delete_process(proc_id):
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
