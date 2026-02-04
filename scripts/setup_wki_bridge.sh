#!/bin/bash
set -e

# -- setup_wki_bridge.sh — Create WKI bridge and TAP devices ----------------
#
# Usage:  sudo ./scripts/setup_wki_bridge.sh [N]
#
# Creates:
#   - wos-wki-br0         — Isolated Linux bridge for WKI inter-kernel traffic
#   - wos-wki-tap0..tapN-1 — TAP devices attached to the bridge (one per VM)
#
# The bridge is intentionally NOT assigned an IP address — it is an isolated
# L2 segment that only WKI kernels communicate over.
#
# To tear down:  sudo ./scripts/setup_wki_bridge.sh --teardown
# ----------------------------------------------------------------------------

if [ "$EUID" -ne 0 ]; then
  echo "ERROR: This script must be run as root (sudo)." >&2
  exit 1
fi

BRIDGE="wos-wki-br0"
REAL_USER="${SUDO_USER:-$USER}"

# -- Teardown mode -----------------------------------------------------------
if [ "$1" = "--teardown" ]; then
  echo "=== Tearing down WKI bridge ==="

  # Delete all wos-wki-tap* devices
  for dev in $(ip -o link show | grep -oP 'wos-wki-tap\d+'); do
    echo "  Deleting ${dev}"
    ip link set "$dev" down 2>/dev/null || true
    ip tuntap del dev "$dev" mode tap 2>/dev/null || true
  done

  # Delete the bridge
  if ip link show "$BRIDGE" &>/dev/null; then
    echo "  Deleting ${BRIDGE}"
    ip link set "$BRIDGE" down
    ip link delete "$BRIDGE" type bridge
  fi

  echo "=== Teardown complete ==="
  exit 0
fi

# -- Setup mode --------------------------------------------------------------
NUM_VMS="${1:-2}"

echo "=== Setting up WKI bridge for ${NUM_VMS} VMs ==="

# Create bridge if it doesn't exist
if ! ip link show "$BRIDGE" &>/dev/null; then
  echo "  Creating bridge: ${BRIDGE}"
  ip link add "$BRIDGE" type bridge
  ip link set "$BRIDGE" up
  # Disable STP — small isolated segment, no loops
  echo 0 > /sys/class/net/${BRIDGE}/bridge/stp_state 2>/dev/null || true
else
  echo "  Bridge ${BRIDGE} already exists"
fi

# Create TAP devices
for ((i = 0; i < NUM_VMS; i++)); do
  TAP="wos-wki-tap${i}"

  if ip link show "$TAP" &>/dev/null; then
    echo "  TAP ${TAP} already exists — skipping"
    continue
  fi

  echo "  Creating TAP: ${TAP} (owner: ${REAL_USER})"
  ip tuntap add dev "$TAP" mode tap user "$REAL_USER"
  ip link set "$TAP" master "$BRIDGE"
  ip link set "$TAP" up
done

echo ""
echo "=== WKI bridge ready ==="
echo "    Bridge: ${BRIDGE}"
echo "    TAPs:   wos-wki-tap{0..$(($NUM_VMS - 1))}"
echo ""
echo "To tear down: sudo $0 --teardown"
