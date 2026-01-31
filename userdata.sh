#!/bin/bash
set -e

LOG=/var/log/trading-bot-bootstrap.log
exec > >(tee -a $LOG) 2>&1

echo "🚀 Bootstrapping Trading Bot EC2"

REGION="ap-south-1"
SSM_REPO_PARAM="/trading-bot/github_repo"
APP_USER="ec2-user"
APP_HOME="/home/ec2-user"

S3_BUCKET="s3://dhan-trading-data"
S3_PREFIX="trading-bot"

# -----------------------------
# System update & deps
# -----------------------------
sudo yum update -y
sudo timedatectl set-timezone Asia/Kolkata
sudo yum install -y git python3.11 python3-pip awscli



# -----------------------------
# Get repo URL from SSM
# -----------------------------
REPO_URL=$(aws ssm get-parameter \
  --name "$SSM_REPO_PARAM" \
  --region "$REGION" \
  --query "Parameter.Value" \
  --output text)

cd "$APP_HOME"

# -----------------------------
# Clone repo (idempotent)
# -----------------------------
REPO_NAME=$(basename "$REPO_URL" .git)
if [ ! -d "$REPO_NAME" ]; then
  git clone "$REPO_URL"
fi

cd "$REPO_NAME"

# -----------------------------
# Python venv
# -----------------------------
if [ ! -d "venv" ]; then
  python3 -m venv venv
fi

source venv/bin/activate
pip install --upgrade pip
[ -f requirements.txt ] && pip install -r requirements.txt

# -----------------------------
# Runtime dirs
# -----------------------------
mkdir -p logs outputs
chmod -R 755 logs outputs
chown -R $APP_USER:$APP_USER logs outputs

# -----------------------------
# PYTHONPATH
# -----------------------------
export PYTHONPATH=$PWD
grep -q "export PYTHONPATH=" /home/$APP_USER/.bashrc || \
  echo "export PYTHONPATH=$PWD" >> /home/$APP_USER/.bashrc

# -----------------------------
# Upload ONLY /var/log/trading-bot.log to S3
# -----------------------------
sudo tee /usr/local/bin/upload-trading-log.sh > /dev/null <<EOF
#!/bin/bash
aws s3 cp /var/log/trading-bot.log \
  $S3_BUCKET/$S3_PREFIX/logs/trading-bot.log \
  --region $REGION || true
EOF
sudo chmod +x /usr/local/bin/upload-trading-log.sh

# -----------------------------
# systemd uploader service
# -----------------------------
sudo tee /etc/systemd/system/trading-log-upload.service > /dev/null <<EOF
[Unit]
Description=Upload trading-bot.log to S3

[Service]
Type=oneshot
ExecStart=/usr/local/bin/upload-trading-log.sh
EOF

# -----------------------------
# systemd uploader timer (5 min)
# -----------------------------
sudo tee /etc/systemd/system/trading-log-upload.timer > /dev/null <<EOF
[Unit]
Description=Upload trading-bot.log to S3 every 5 minutes

[Timer]
OnBootSec=2min
OnUnitActiveSec=5min
Persistent=true

[Install]
WantedBy=timers.target
EOF

# -----------------------------
# Trading bot service
# -----------------------------
sudo tee /etc/systemd/system/trading-bot.service > /dev/null <<EOF
[Unit]
Description=Trading Bot Service
After=network-online.target
Wants=network-online.target

[Service]
User=$APP_USER
WorkingDirectory=$APP_HOME/$REPO_NAME
Environment=PYTHONPATH=$APP_HOME/$REPO_NAME
Environment=PYTHONUNBUFFERED=1
ExecStart=$APP_HOME/$REPO_NAME/venv/bin/python app/main.py
Restart=always
RestartSec=10
StandardOutput=append:/var/log/trading-bot.log
StandardError=append:/var/log/trading-bot.log
ExecStopPost=/usr/local/bin/upload-trading-log.sh

[Install]
WantedBy=multi-user.target
EOF

# -----------------------------
# Enable & start
# -----------------------------
sudo systemctl daemon-reload
sudo systemctl enable trading-bot
sudo systemctl enable --now trading-log-upload.timer
sudo systemctl restart trading-bot

echo "✅ Trading Bot started; /var/log/trading-bot.log uploads to S3 only"
