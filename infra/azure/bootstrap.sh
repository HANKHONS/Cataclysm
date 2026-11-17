#!/usr/bin/env bash
# Run on the provisioned Ubuntu VM; no Azure resource creation occurs here.
set -euo pipefail
if [ "$(id -u)" -ne 0 ]; then
  echo 'Run with sudo after copying the repository to the VM.' >&2
  exit 1
fi
apt-get update
apt-get install -y ca-certificates curl
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
. /etc/os-release
printf 'deb [arch=%s signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu %s stable\n' "$(dpkg --print-architecture)" "$VERSION_CODENAME" > /etc/apt/sources.list.d/docker.list
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
systemctl enable --now docker
docker compose version
echo 'Docker installed. Mount the data disk, configure ignored .env, then run docker compose up -d --build from the repository.'
