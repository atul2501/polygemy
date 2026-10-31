#!/usr/bin/env bash
# One-time server hardening (run as root on the VPS; review before running).
# Inbound access is also restricted in the AWS security group: allow TCP 22 from your IP only.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run as root"; exit 1; }
# SSH: keys only, no root login.
sed -i -E 's/^#?PasswordAuthentication .*/PasswordAuthentication no/; s/^#?PermitRootLogin .*/PermitRootLogin no/; s/^#?KbdInteractiveAuthentication .*/KbdInteractiveAuthentication no/' /etc/ssh/sshd_config
systemctl reload sshd 2>/dev/null || systemctl reload ssh
if command -v apt-get >/dev/null; then
  apt-get update -q && apt-get install -yq chrony ufw unattended-upgrades docker.io docker-compose-v2 git make
  ufw default deny incoming && ufw default allow outgoing && ufw allow OpenSSH && ufw --force enable
  dpkg-reconfigure -f noninteractive unattended-upgrades
else # Amazon Linux 2023
  dnf install -yq chrony docker git make && systemctl enable --now docker
  mkdir -p /usr/local/lib/docker/cli-plugins
  echo "install the docker compose plugin: https://docs.docker.com/compose/install/linux/"
fi
systemctl enable --now chrony 2>/dev/null || systemctl enable --now chronyd
# Docker must not publish ports beyond what compose binds to 127.0.0.1.
echo "hardening done: verify with 'ss -tlnp' that only sshd listens on 0.0.0.0"
