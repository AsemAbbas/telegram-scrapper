#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# TeleDrive — cPanel VPS Deployment Script
# Run this via SSH on your VPS after uploading files.
# ═══════════════════════════════════════════════════════════════

set -e

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$APP_DIR/venv"
SERVICE_NAME="teledrive"

echo "═══════════════════════════════════════"
echo "  TeleDrive — Deployment"
echo "═══════════════════════════════════════"
echo ""

# 1. Create virtual environment
if [ ! -d "$VENV_DIR" ]; then
    echo "[1/6] Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
else
    echo "[1/6] Virtual environment exists."
fi

source "$VENV_DIR/bin/activate"

# 2. Install dependencies
echo "[2/6] Installing dependencies..."
pip install --upgrade pip
pip install -r "$APP_DIR/requirements.txt"
pip install gunicorn eventlet

# 3. Create data directory
echo "[3/6] Setting up data directory..."
mkdir -p "$APP_DIR/data"
mkdir -p "$APP_DIR/exports"
chmod 700 "$APP_DIR/data"

# 4. Generate SECRET_KEY if not in .env
if ! grep -q "^SECRET_KEY=" "$APP_DIR/.env" 2>/dev/null; then
    echo "[4/6] Generating SECRET_KEY..."
    SECRET=$(python3 -c "import secrets; print(secrets.token_hex(32))")
    echo "SECRET_KEY=$SECRET" >> "$APP_DIR/.env"
else
    echo "[4/6] SECRET_KEY already set."
fi

# 5. Create systemd service (requires root)
echo "[5/6] Creating systemd service..."
if [ "$(id -u)" -eq 0 ] || command -v sudo &>/dev/null; then
    SUDO=""
    [ "$(id -u)" -ne 0 ] && SUDO="sudo"

    $SUDO tee /etc/systemd/system/${SERVICE_NAME}.service > /dev/null <<EOF
[Unit]
Description=TeleDrive — Telegram Channel Intelligence
After=network.target

[Service]
User=$(whoami)
Group=$(whoami)
WorkingDirectory=$APP_DIR
Environment="PATH=$VENV_DIR/bin"
EnvironmentFile=$APP_DIR/.env
ExecStart=$VENV_DIR/bin/gunicorn -w 1 -b 127.0.0.1:5000 --worker-class eventlet --timeout 120 "web_app:app"
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

    $SUDO systemctl daemon-reload
    $SUDO systemctl enable ${SERVICE_NAME}
    $SUDO systemctl restart ${SERVICE_NAME}
    echo "  Service started! Check: sudo systemctl status ${SERVICE_NAME}"
else
    echo "  [SKIP] No sudo access. Create the systemd service manually."
fi

# 6. Show status
echo "[6/6] Done!"
echo ""
echo "═══════════════════════════════════════"
echo "  Next steps:"
echo "  1. Point your domain to 127.0.0.1:5000 via cPanel's Apache/Nginx proxy"
echo "     OR add a reverse proxy in Apache:"
echo ""
echo "     ProxyPass / http://127.0.0.1:5000/"
echo "     ProxyPassReverse / http://127.0.0.1:5000/"
echo ""
echo "  2. Set up SSL via cPanel > SSL/TLS"
echo "  3. Login: admin@teledrive.app / admin123"
echo "  4. CHANGE THE ADMIN PASSWORD IMMEDIATELY"
echo "═══════════════════════════════════════"
