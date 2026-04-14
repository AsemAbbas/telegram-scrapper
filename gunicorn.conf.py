"""
Gunicorn configuration for TeleDrive.
Usage: gunicorn -c gunicorn.conf.py "web_app:app"
"""
import os

# Bind to localhost — Nginx reverse-proxies from port 80/443
bind = "127.0.0.1:" + os.getenv("PORT", "5000")

# Single worker required:
#   - SQLite doesn't handle concurrent writers from multiple processes
#   - Flask-SocketIO with threading mode needs a single process
workers = 1

# gthread matches Flask-SocketIO async_mode='threading'
worker_class = "gthread"

# Threads for handling concurrent requests within the single worker
threads = 4

# Telegram scraping can be slow — allow up to 5 minutes
timeout = 300
graceful_timeout = 30

# Keep-alive for Nginx reverse proxy connection reuse
keepalive = 5

# Logging
accesslog = "-"
errorlog = "-"
loglevel = os.getenv("LOG_LEVEL", "info")

# PID file
pidfile = "/tmp/teledrive.pid"
