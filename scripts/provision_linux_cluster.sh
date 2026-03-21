#!/bin/bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <guest-ip-or-hostname> [guest-ip-or-hostname ...]" >&2
  exit 1
fi

if ! command -v sshpass >/dev/null 2>&1; then
  echo "ERROR: sshpass is required on the host to provision password-only Ubuntu guests." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REMOTE_USER="${WOS_LINUX_USER:-user}"
REMOTE_PASS="${WOS_LINUX_PASSWORD:-1234}"
SSH_PORT_BASE="${WOS_LINUX_SSH_PORT_BASE:-}"
WAIT_TIMEOUT="${WOS_LINUX_SSH_WAIT_TIMEOUT:-120}"
WAIT_INTERVAL="${WOS_LINUX_SSH_WAIT_INTERVAL:-5}"

wait_for_ssh() {
  local ssh_target="$1"
  shift
  local -a ssh_port_args=("$@")
  local waited=0

  while (( waited < WAIT_TIMEOUT )); do
    echo "  waiting for SSH on ${ssh_target} (${waited}/${WAIT_TIMEOUT}s)"
    if sshpass -p "$REMOTE_PASS" ssh \
      -o StrictHostKeyChecking=no \
      -o UserKnownHostsFile=/dev/null \
      -o ConnectTimeout=5 \
      "${ssh_port_args[@]}" \
      "${REMOTE_USER}@${ssh_target}" \
      true >/dev/null 2>&1; then
      return 0
    fi

    sleep "$WAIT_INTERVAL"
    waited=$((waited + WAIT_INTERVAL))
  done

  return 1
}

for target in "$@"; do
  ssh_target="$target"
  ssh_port_args=()

  if [[ -n "$SSH_PORT_BASE" && "$target" =~ ^[0-9]+$ ]]; then
    ssh_target="127.0.0.1"
    ssh_port_args=(-p "$((SSH_PORT_BASE + target))")
  fi

  echo "=== Provisioning ${ssh_target} ==="
  if ! wait_for_ssh "$ssh_target" "${ssh_port_args[@]}"; then
    echo "ERROR: ${ssh_target} did not become reachable over SSH within ${WAIT_TIMEOUT}s" >&2
    exit 1
  fi

  echo "  SSH is reachable on ${ssh_target}; starting provisioning"

  sshpass -p "$REMOTE_PASS" ssh \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    -o ConnectTimeout=5 \
    "${ssh_port_args[@]}" \
    "${REMOTE_USER}@${ssh_target}" \
    "echo '$REMOTE_PASS' | sudo -S bash -s" < "${SCRIPT_DIR}/provision_ubuntu_bench.sh"
done
