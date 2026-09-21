#!/bin/bash
set -e

LOG=/var/log/trading-bot-algo-bootstrap.log
exec > >(tee -a $LOG) 2>&1

echo "🚀 Bootstrapping Trading Bot Algo EC2"

REGION="ap-south-1"
SSM_REPO_PARAM="/trading-bot-algo/github_repo"
APP_USER="ec2-user"
APP_HOME="/home/ec2-user"

S3_BUCKET="s3://dhan-trading-data"
S3_PREFIX="trading-bot"

# -----------------------------
# System update & deps
# -----------------------------
sudo yum update -y
sudo timedatectl set-timezone Asia/Kolkata
sudo yum install -y git python3.11 python3.11-pip python3.11-devel awscli
echo "✅ Installed Python 3.11"
/usr/bin/python3.11 --version
python3 --version  # should remain system 3.9

# -----------------------------
# Self-associate the pre-reserved "dhan" Elastic IP — Dhan's order-
# placement API only accepts calls from this whitelisted address, and
# every fresh launch otherwise gets a random dynamic IP. The instance
# profile (EC2-AI-Agent-Role) already grants ec2:AssociateAddress via
# the Assign_ipaddress inline policy.
# -----------------------------
DHAN_EIP_ALLOCATION_ID="eipalloc-0daf98ed664e3827f"
IMDS_TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
SELF_INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" \
  http://169.254.169.254/latest/meta-data/instance-id)
aws ec2 associate-address \
  --instance-id "$SELF_INSTANCE_ID" \
  --allocation-id "$DHAN_EIP_ALLOCATION_ID" \
  --region "$REGION"
echo "✅ Associated dedicated Dhan IP (allocation $DHAN_EIP_ALLOCATION_ID) to $SELF_INSTANCE_ID"

# -----------------------------
# Safe python aliases (user only)
# -----------------------------
BASHRC="$APP_HOME/.bashrc"
grep -q "alias python=python3.11" "$BASHRC" || echo "alias python=python3.11" >> "$BASHRC"
grep -q "alias pip=pip3.11" "$BASHRC" || echo "alias pip=pip3.11" >> "$BASHRC"

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
# Clone repo (idempotent) — wrapped with a timeout+retry. The EIP
# self-association above needs a brief moment to fully propagate at
# the network level, and the first substantial outbound HTTPS
# connection through it (this clone, to github.com) can hang
# indefinitely if issued in that narrow window — confirmed
# reproducible twice: a stuck clone, killed and retried a few seconds
# later, completes instantly. `set -e` at the top of this script means
# an unhandled hang here would otherwise block the whole boot forever.
# -----------------------------
REPO_NAME=$(basename "$REPO_URL" .git)
if [ ! -d "$REPO_NAME" ]; then
  CLONE_OK=0
  for attempt in 1 2 3; do
    if timeout 30 git clone "$REPO_URL"; then
      CLONE_OK=1
      break
    fi
    echo "⚠️ git clone attempt $attempt timed out/failed, retrying..."
    rm -rf "$REPO_NAME"
    sleep 5
  done
  if [ "$CLONE_OK" -ne 1 ]; then
    echo "❌ git clone failed after 3 attempts, aborting bootstrap"
    exit 1
  fi
fi

cd "$REPO_NAME"

# -----------------------------
# Python venv using 3.11
# -----------------------------
if [ ! -d "venv" ]; then
  /usr/bin/python3.11 -m venv venv
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
# Upload ONLY /var/log/trading-bot-algo.log to S3
# -----------------------------
sudo tee /usr/local/bin/upload-trading-bot-algo-log.sh > /dev/null <<EOF
#!/bin/bash
aws s3 cp /var/log/trading-bot-algo.log \
  $S3_BUCKET/$S3_PREFIX/logs/trading-bot-algo.log \
  --region $REGION || true
EOF
sudo chmod +x /usr/local/bin/upload-trading-bot-algo-log.sh

# -----------------------------
# systemd uploader service
# -----------------------------
sudo tee /etc/systemd/system/trading-bot-algo-log-upload.service > /dev/null <<EOF
[Unit]
Description=Upload trading-bot-algo.log to S3

[Service]
Type=oneshot
ExecStart=/usr/local/bin/upload-trading-bot-algo-log.sh
EOF

# -----------------------------
# systemd uploader timer (5 min)
# -----------------------------
sudo tee /etc/systemd/system/trading-bot-algo-log-upload.timer > /dev/null <<EOF
[Unit]
Description=Upload trading-bot-algo.log to S3 every 5 minutes

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
sudo tee /etc/systemd/system/trading-bot-algo.service > /dev/null <<EOF
[Unit]
Description=Trading Bot Algo Service
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
StandardOutput=append:/var/log/trading-bot-algo.log
StandardError=append:/var/log/trading-bot-algo.log
ExecStopPost=/usr/local/bin/upload-trading-bot-algo-log.sh

[Install]
WantedBy=multi-user.target
EOF

# -----------------------------
# Enable & start
# -----------------------------
sudo systemctl daemon-reload
sudo systemctl enable trading-bot-algo
sudo systemctl enable --now trading-bot-algo-log-upload.timer
sudo systemctl restart trading-bot-algo

echo "✅ Trading Bot Algo started; /var/log/trading-bot-algo.log uploads to S3 only"
