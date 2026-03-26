"""
WSGI entry point for cPanel Passenger deployment.
cPanel's "Setup Python App" feature uses this file automatically.
"""
import os
import sys

# Add project directory to path
INTERP = os.path.expanduser("~/virtualenv/teledrive/3.11/bin/python")  # Adjust to your venv path
if sys.executable != INTERP:
    os.execl(INTERP, INTERP, *sys.argv)

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Production settings
os.environ.setdefault('FLASK_ENV', 'production')

from web_app import app, socketio, scheduler, setup_scheduler, load_scheduler_config
from web_app import check_profile_schedules, cleanup_expired_sessions
from src.local_db import get_setting

# Initialize scheduler for persistent process
app_enabled = get_setting("app_enabled", True)
load_scheduler_config()

if not scheduler.running:
    scheduler.start()
    setup_scheduler()
    scheduler.add_job(check_profile_schedules, 'interval', seconds=60,
                      id='profile_scheduler', replace_existing=True)
    scheduler.add_job(cleanup_expired_sessions, 'interval', hours=6,
                      id='session_cleanup', replace_existing=True)

# Passenger expects 'application' variable
application = app
