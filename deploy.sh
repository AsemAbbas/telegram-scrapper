#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# TeleDrive — Ubuntu VPS Deployment Script (Hetzner / DigitalOcean / Linode)
#
# Usage:
#   sudo bash deploy.sh                          # defaults to /opt/teledrive
#   sudo bash deploy.sh --domain yourdomain.com  # with SSL via Certbot
#   sudo bash deploy.sh --app-dir /srv/teledrive # custom install path
#
# Idempotent — safe to run multiple times.
# ═══════════════════════════════════════════════════════════════

set -euo pipefail

# ── Parse arguments ──
APP_DIR="/opt/teledrive"
DOMAIN=""
SERVICE_NAME="teledrive"
SERVICE_USER="teledrive"

while [[ $# -gt 0 ]]; do
    case $1 in
        --app-dir)  APP_DIR="$2";  shift 2 ;;
        --domain)   DOMAIN="$2";   shift 2 ;;
        *)          echo "Unknown option: $1"; exit 1 ;;
    esac
done

VENV_DIR="$APP_DIR/venv"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo ""
echo "  ╔══════════════════════════════════════════╗"
echo "  ║  TeleDrive — VPS Deployment              ║"
echo "  ║  Target: $APP_DIR"
[ -n "$DOMAIN" ] && echo "  ║  Domain: $DOMAIN"
echo "  ╚══════════════════════════════════════════╝"
echo ""

# ── Must run as root ──
if [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: Run this script with sudo or as root."
    exit 1
fi

# ═══════════════════════════════════════════════════════════════
# 1. System packages
# ═══════════════════════════════════════════════════════════════
echo "[1/8] Installing system packages..."
apt-get update -qq
apt-get install -y -qq python3 python3-venv python3-pip nginx certbot python3-certbot-nginx > /dev/null

# ═══════════════════════════════════════════════════════════════
# 2. Create system user
# ═══════════════════════════════════════════════════════════════
echo "[2/8] Setting up system user..."
if ! id "$SERVICE_USER" &>/dev/null; then
    useradd --system --shell /usr/sbin/nologin --home-dir "$APP_DIR" --create-home "$SERVICE_USER"
    echo "  Created user: $SERVICE_USER"
else
    echo "  User $SERVICE_USER already exists."
fi

# ═══════════════════════════════════════════════════════════════
# 3. Copy application files
# ═══════════════════════════════════════════════════════════════
echo "[3/8] Copying application files..."
mkdir -p "$APP_DIR"

# Copy project files (exclude dev/git/deploy artifacts)
rsync -a --delete \
    --exclude='.git' \
    --exclude='.github' \
    --exclude='venv' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='.env' \
    --exclude='data/' \
    --exclude='exports/' \
    --exclude='*.session' \
    --exclude='*.session-journal' \
    --exclude='*_SECRET.txt' \
    --exclude='*_CLEAN.txt' \
    --exclude='github_secrets.txt' \
    --exclude='n8n-telegram-scrapper-*.json' \
    --exclude='start.bat' \
    --exclude='start.vbs' \
    --exclude='create-shortcut.*' \
    --exclude='setup-guide.html' \
    --exclude='passenger_wsgi.py' \
    "$SCRIPT_DIR/" "$APP_DIR/"

# ═══════════════════════════════════════════════════════════════
# 4. Virtual environment + dependencies
# ═══════════════════════════════════════════════════════════════
echo "[4/8] Setting up Python environment..."
if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
fi
"$VENV_DIR/bin/pip" install --upgrade pip -q
"$VENV_DIR/bin/pip" install -r "$APP_DIR/requirements.txt" -q
"$VENV_DIR/bin/pip" install gunicorn -q

# ═══════════════════════════════════════════════════════════════
# 5. Data directories + permissions
# ═══════════════════════════════════════════════════════════════
echo "[5/8] Setting up directories and permissions..."
mkdir -p "$APP_DIR/data" "$APP_DIR/exports"
chown -R "$SERVICE_USER:$SERVICE_USER" "$APP_DIR"
chmod 750 "$APP_DIR/data"

# ═══════════════════════════════════════════════════════════════
# 6. Environment file
# ═══════════════════════════════════════════════════════════════
echo "[6/8] Configuring environment..."
if [ ! -f "$APP_DIR/.env" ]; then
    cp "$APP_DIR/.env.example" "$APP_DIR/.env"
    # Set production defaults
    sed -i 's/^FLASK_DEBUG=1/FLASK_DEBUG=0/' "$APP_DIR/.env"
    sed -i 's/^FLASK_ENV=development/FLASK_ENV=production/' "$APP_DIR/.env"
    sed -i 's/^COOKIE_SECURE=false/COOKIE_SECURE=true/' "$APP_DIR/.env"
    # Generate SECRET_KEY
    SECRET=$("$VENV_DIR/bin/python3" -c "import secrets; print(secrets.token_hex(32))")
    sed -i "s/^SECRET_KEY=change-me-in-production/SECRET_KEY=$SECRET/" "$APP_DIR/.env"
    chown "$SERVICE_USER:$SERVICE_USER" "$APP_DIR/.env"
    chmod 600 "$APP_DIR/.env"
    echo "  Created .env with generated SECRET_KEY"
    echo "  >>> Edit $APP_DIR/.env to add your Telegram and Google credentials <<<"
else
    echo "  .env already exists (not overwritten)."
    # Ensure SECRET_KEY is set
    if ! grep -q "^SECRET_KEY=" "$APP_DIR/.env" || grep -q "^SECRET_KEY=change-me" "$APP_DIR/.env"; then
        SECRET=$("$VENV_DIR/bin/python3" -c "import secrets; print(secrets.token_hex(32))")
        if grep -q "^SECRET_KEY=" "$APP_DIR/.env"; then
            sed -i "s/^SECRET_KEY=.*/SECRET_KEY=$SECRET/" "$APP_DIR/.env"
        else
            echo "SECRET_KEY=$SECRET" >> "$APP_DIR/.env"
        fi
        echo "  Generated new SECRET_KEY"
    fi
fi

# ═══════════════════════════════════════════════════════════════
# 7. Systemd service
# ═══════════════════════════════════════════════════════════════
echo "[7/8] Installing systemd service..."
# Substitute APP_DIR in the service file
sed "s|/opt/teledrive|$APP_DIR|g" "$APP_DIR/systemd/teledrive.service" \
    > /etc/systemd/system/${SERVICE_NAME}.service

systemctl daemon-reload
systemctl enable ${SERVICE_NAME} --quiet
systemctl restart ${SERVICE_NAME}
echo "  Service installed and started."

# ═══════════════════════════════════════════════════════════════
# 8. Nginx
# ═══════════════════════════════════════════════════════════════
echo "[8/8] Configuring Nginx..."

# Substitute APP_DIR in Nginx config
sed "s|/opt/teledrive|$APP_DIR|g" "$APP_DIR/nginx/teledrive.conf" \
    > /etc/nginx/sites-available/teledrive

# Substitute domain if provided
if [ -n "$DOMAIN" ]; then
    sed -i "s/server_name _;/server_name $DOMAIN;/" /etc/nginx/sites-available/teledrive
fi

# Enable site
ln -sf /etc/nginx/sites-available/teledrive /etc/nginx/sites-enabled/teledrive

# Remove default site if it exists
rm -f /etc/nginx/sites-enabled/default

# Test and reload
nginx -t
systemctl reload nginx

echo "  Nginx configured and reloaded."

# ── Optional SSL ──
if [ -n "$DOMAIN" ]; then
    echo ""
    echo "  Setting up SSL for $DOMAIN..."
    certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos --register-unsafely-without-email || {
        echo "  SSL setup failed. Run manually: sudo certbot --nginx -d $DOMAIN"
    }
fi

# ── Firewall ──
if command -v ufw &>/dev/null && ufw status | grep -q "active"; then
    ufw allow 'Nginx Full' > /dev/null 2>&1
    echo "  Firewall: Nginx Full allowed."
fi

# ═══════════════════════════════════════════════════════════════
# Done
# ═══════════════════════════════════════════════════════════════
echo ""
echo "  ╔══════════════════════════════════════════╗"
echo "  ║  Deployment complete!                    ║"
echo "  ╠══════════════════════════════════════════╣"
echo "  ║  App:     $APP_DIR"
echo "  ║  Service: sudo systemctl status $SERVICE_NAME"
echo "  ║  Logs:    sudo journalctl -u $SERVICE_NAME -f"
if [ -n "$DOMAIN" ]; then
echo "  ║  URL:     https://$DOMAIN"
else
echo "  ║  URL:     http://$(hostname -I | awk '{print $1}')"
fi
echo "  ╠══════════════════════════════════════════╣"
echo "  ║  Default login:                          ║"
echo "  ║    admin@teledrive.app / admin123         ║"
echo "  ║    CHANGE THE PASSWORD IMMEDIATELY!       ║"
echo "  ╠══════════════════════════════════════════╣"
echo "  ║  Next steps:                             ║"
echo "  ║  1. Edit $APP_DIR/.env"
echo "  ║     (add Telegram & Google credentials)  ║"
echo "  ║  2. sudo systemctl restart $SERVICE_NAME"
if [ -z "$DOMAIN" ]; then
echo "  ║  3. sudo bash deploy.sh --domain YOUR_DOMAIN"
fi
echo "  ╚══════════════════════════════════════════╝"
echo ""
