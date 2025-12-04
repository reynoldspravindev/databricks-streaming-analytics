#!/bin/bash

################################################################################
# SFTP Server Configuration Script
# Configures Ubuntu VM for secure SFTP access with dedicated telco user
# Run this script on the GCE VM instance
################################################################################

set -e

# Configuration
SFTP_USER="telco_user"
SFTP_PASSWORD="${SFTP_PASSWORD:-CHANGE_ME_IN_PRODUCTION}"  # Set via environment variable or change this!
SFTP_GROUP="sftpusers"
SFTP_BASE="/sftp"
SFTP_TELCO_BASE="/sftp/telco"
SFTP_SYSLOG_DIR="/sftp/telco/syslog"
SFTP_SNMP_DIR="/sftp/telco/snmp"

echo "=========================================="
echo "SFTP Server Configuration"
echo "=========================================="

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root (use sudo)" 
   exit 1
fi

# Update system packages
echo "Updating system packages..."
apt-get update
apt-get upgrade -y

# Install OpenSSH server if not already installed
echo "Installing OpenSSH server..."
apt-get install -y openssh-server

# Create SFTP group
echo "Creating SFTP group: $SFTP_GROUP..."
if ! getent group $SFTP_GROUP > /dev/null 2>&1; then
    groupadd $SFTP_GROUP
else
    echo "Group $SFTP_GROUP already exists."
fi

# Create SFTP user
echo "Creating SFTP user: $SFTP_USER..."
if ! id -u $SFTP_USER > /dev/null 2>&1; then
    useradd -m -g $SFTP_GROUP -s /bin/bash $SFTP_USER
    echo "$SFTP_USER:$SFTP_PASSWORD" | chpasswd
    echo "User created with password: $SFTP_PASSWORD"
else
    echo "User $SFTP_USER already exists."
    echo "$SFTP_USER:$SFTP_PASSWORD" | chpasswd
    echo "Password updated."
fi

# Create directory structure
echo "Creating SFTP directory structure..."
mkdir -p $SFTP_BASE
mkdir -p $SFTP_TELCO_BASE
mkdir -p $SFTP_SYSLOG_DIR
mkdir -p $SFTP_SNMP_DIR

# Set permissions (root owns base, user owns subdirectories)
chown root:root $SFTP_BASE
chmod 755 $SFTP_BASE

chown root:root $SFTP_TELCO_BASE
chmod 755 $SFTP_TELCO_BASE

chown $SFTP_USER:$SFTP_GROUP $SFTP_SYSLOG_DIR
chmod 755 $SFTP_SYSLOG_DIR

chown $SFTP_USER:$SFTP_GROUP $SFTP_SNMP_DIR
chmod 755 $SFTP_SNMP_DIR

# Backup original SSH config
if [ ! -f /etc/ssh/sshd_config.backup ]; then
    cp /etc/ssh/sshd_config /etc/ssh/sshd_config.backup
    echo "Original SSH config backed up to /etc/ssh/sshd_config.backup"
fi

# Configure SSH for SFTP with password authentication
echo "Configuring SSH server for SFTP..."

# Remove any existing Match block for SFTP group
sed -i '/^Match Group sftpusers/,/^$/d' /etc/ssh/sshd_config

# Add SFTP configuration
cat >> /etc/ssh/sshd_config <<'EOF'

# SFTP Configuration for Telco Demo
Match Group sftpusers
    ChrootDirectory /sftp
    ForceCommand internal-sftp
    PasswordAuthentication yes
    PermitTunnel no
    AllowAgentForwarding no
    AllowTcpForwarding no
    X11Forwarding no
EOF

# Ensure password authentication is enabled globally
sed -i 's/^PasswordAuthentication no/PasswordAuthentication yes/' /etc/ssh/sshd_config
if ! grep -q "^PasswordAuthentication yes" /etc/ssh/sshd_config; then
    echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config
fi

# Validate SSH configuration
echo "Validating SSH configuration..."
sshd -t

# Restart SSH service
echo "Restarting SSH service..."
systemctl restart ssh
systemctl enable ssh

# Install Python and required packages for data generator
echo "Installing Python and dependencies..."
apt-get install -y python3 python3-pip python3-venv

# Create Python virtual environment for data generator
echo "Creating Python virtual environment..."
mkdir -p /opt/telco-generator
python3 -m venv /opt/telco-generator/venv

# Install Python packages for GCS support
echo "Installing Python packages for GCS..."
/opt/telco-generator/venv/bin/pip install --upgrade pip
/opt/telco-generator/venv/bin/pip install google-cloud-storage

# Display status
echo ""
echo "=========================================="
echo "SFTP Server Configuration Complete!"
echo "=========================================="
echo "SFTP User: $SFTP_USER"
echo "SFTP Password: $SFTP_PASSWORD"
echo "Syslog Directory: $SFTP_SYSLOG_DIR"
echo "SNMP Directory: $SFTP_SNMP_DIR"
echo ""
echo "SSH Service Status:"
systemctl status ssh --no-pager | head -n 5
echo ""
echo "Test SFTP connection with:"
echo "  sftp $SFTP_USER@\$(hostname -I | awk '{print \$1}')"
echo ""
echo "Next Steps:"
echo "1. Copy the data generator script to /opt/telco-generator/"
echo "2. Install the systemd service"
echo "3. Start the data generator"
echo "=========================================="

# Create a test file to verify SFTP works
sudo -u $SFTP_USER bash -c "echo 'SFTP server ready - $(date)' > $SFTP_SYSLOG_DIR/test_connection.txt"
sudo -u $SFTP_USER bash -c "echo 'timestamp,device_id,test' > $SFTP_SNMP_DIR/test_connection.csv"

echo "Test files created in syslog and snmp directories."

