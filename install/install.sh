#!/usr/bin/env bash
set -euo pipefail

UNIT_NAME="aion-kernel.service"
UNIT_DIR="/etc/systemd/system"
CGROUP_ROOT="/sys/fs/cgroup/aion"
ENV_DIR="/etc/aion-kernel"
ENV_FILE="${ENV_DIR}/aion.env"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PI_EXTENSION_SRC="${SCRIPT_DIR}/pi/extensions/aion-gateway-provider.ts"

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
SERVICE_HOME="$(getent passwd "${SERVICE_USER}" | cut -d: -f6)"

mkdir -p "${ENV_DIR}"
if [[ -n "${SERVICE_HOME}" && -f "${PI_EXTENSION_SRC}" ]]; then
  PI_EXTENSION_DIR="${SERVICE_HOME}/.pi/agent/extensions"
  mkdir -p "${PI_EXTENSION_DIR}"
  cp "${PI_EXTENSION_SRC}" "${PI_EXTENSION_DIR}/aion-gateway-provider.ts"
  chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "${PI_EXTENSION_DIR}"
fi

if [[ ! -d "${CGROUP_ROOT}" ]]; then
  mkdir -p "${CGROUP_ROOT}"
fi
chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "${CGROUP_ROOT}"

cat >"${ENV_FILE}" <<EOF
AION_CGROUPS_MODE=systemd
AION_CGROUPS_BASE_PATH=${CGROUP_ROOT}
AION_PI_GATEWAY_EXTENSION_PATH=${SERVICE_HOME}/.pi/agent/extensions/aion-gateway-provider.ts
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
