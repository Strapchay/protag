#!/usr/bin/env bash
set -euo pipefail

UNIT_NAME="aion-kernel.service"
UNIT_DIR="/etc/systemd/system"
CGROUP_ROOT="/sys/fs/cgroup/aion"
ENV_DIR="/etc/aion-kernel"
ENV_FILE="${ENV_DIR}/aion.env"

if [[ $# -lt 3 ]]; then
  echo "usage: $0 <binary-path> <workdir> <config-path> [user]" >&2
  exit 1
fi

if [[ "${EUID}" -ne 0 ]]; then
  echo "run this installer as root (or via sudo)" >&2
  exit 1
fi

BIN_PATH="$1"
WORKDIR="$2"
CONFIG_PATH="$3"
SERVICE_USER="${4:-${SUDO_USER:-$(id -un)}}"
SERVICE_GROUP="${SERVICE_USER}"

mkdir -p "${ENV_DIR}"

if [[ ! -d "${CGROUP_ROOT}" ]]; then
  mkdir -p "${CGROUP_ROOT}"
fi
chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "${CGROUP_ROOT}"

cat >"${ENV_FILE}" <<EOF
AION_CGROUPS_MODE=systemd
AION_CGROUPS_BASE_PATH=${CGROUP_ROOT}
EOF

cat >"${UNIT_DIR}/${UNIT_NAME}" <<EOF
[Unit]
Description=Aion Kernel Orchestrator
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
WorkingDirectory=${WORKDIR}
EnvironmentFile=-${ENV_FILE}
ExecStart=${BIN_PATH} server --workdir ${WORKDIR} --config ${CONFIG_PATH}
Restart=on-failure
RestartSec=2
Delegate=yes
NoNewPrivileges=yes
KillMode=mixed

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now "${UNIT_NAME}"
