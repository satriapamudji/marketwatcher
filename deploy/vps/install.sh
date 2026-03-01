#!/bin/bash
# MarketWatcher VPS Installation Script
# Usage: sudo ./install.sh

set -e

PROJECT_DIR="/opt/marketwatcher"
SERVICE_NAME="marketwatcher"

echo "=== MarketWatcher VPS Installer ==="

# Check for root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root (sudo ./install.sh)"
    exit 1
fi

# Install dependencies
echo "[1/6] Installing system dependencies..."
apt-get update
apt-get install -y python3 python3-pip python3-venv

# Create project directory
echo "[2/6] Creating project directory..."
mkdir -p $PROJECT_DIR
mkdir -p $PROJECT_DIR/logs
mkdir -p $PROJECT_DIR/config

# Copy files (assuming you're in the project root)
echo "[3/6] Copying application files..."
cp -r src $PROJECT_DIR/
cp -r config $PROJECT_DIR/
cp pyproject.toml $PROJECT_DIR/
cp .env.example $PROJECT_DIR/.env.example

# Create virtual environment
echo "[4/6] Setting up Python virtual environment..."
cd $PROJECT_DIR
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .

# Create .env from example if not exists
if [ ! -f .env ]; then
    echo "[5/6] Creating .env file..."
    cp .env.example .env
    echo ""
    echo "IMPORTANT: Edit $PROJECT_DIR/.env and add your Telegram credentials:"
    echo "  TELEGRAM_BOT_TOKEN=your_token"
    echo "  TELEGRAM_CHAT_ID=your_chat_id"
    echo ""
fi

# Install systemd service
echo "[6/6] Installing systemd service..."
cp deploy/vps/marketwatcher.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable marketwatcher

echo ""
echo "=== Installation Complete ==="
echo ""
echo "Next steps:"
echo "1. Edit $PROJECT_DIR/.env with your Telegram credentials"
echo "2. Run 'marketwatcher tui' to configure scheduler jobs"
echo "3. Start the service: systemctl start marketwatcher"
echo "4. Check logs: journalctl -u marketwatcher -f"
echo ""
