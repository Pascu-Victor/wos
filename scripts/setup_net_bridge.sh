#!/bin/bash
set -e

# -- setup_net_bridge.sh — Create network bridge and TAP devices ------------
#
# Usage:  sudo ./scripts/setup_net_bridge.sh [N]
#
# Creates:
#   - wos-br0             — Linux bridge for VM network traffic
#   - wos-tap0..tapN-1    — TAP devices attached to the bridge (one per VM)
#
# This script moves the IP configuration from NET_LAN to the bridge so that
# both the host and VMs share the same network segment with internet access.
#
# To tear down:  sudo ./scripts/setup_net_bridge.sh --teardown
# ----------------------------------------------------------------------------

if [ "$EUID" -ne 0 ]; then
  echo "ERROR: This script must be run as root (sudo)." >&2
  exit 1
fi

BRIDGE="wos-br0"
NET_LAN="${NET_LAN:-enp7s0}"
REAL_USER="${SUDO_USER:-$USER}"
STATE_FILE="/var/run/wos-bridge-state"

# -- Teardown mode -----------------------------------------------------------
if [ "$1" = "--teardown" ]; then
  echo "=== Tearing down network bridge ==="

  # Restore IP configuration to NET_LAN
  if [ -f "$STATE_FILE" ]; then
    source "$STATE_FILE"
    echo "  Restoring IP config to ${NET_LAN}"

    # Remove NET_LAN from bridge first
    ip link set "$NET_LAN" nomaster 2>/dev/null || true

    # Restore IP address to physical interface
    if [ -n "$SAVED_IP" ]; then
      ip addr add "$SAVED_IP" dev "$NET_LAN" 2>/dev/null || true
    fi

    # Restore default route
    if [ -n "$SAVED_GW" ]; then
      ip route add default via "$SAVED_GW" dev "$NET_LAN" 2>/dev/null || true
    fi

    rm -f "$STATE_FILE"
  else
    # No state file, just remove from bridge
    if ip link show "$NET_LAN" &>/dev/null; then
      echo "  Removing ${NET_LAN} from bridge"
      ip link set "$NET_LAN" nomaster 2>/dev/null || true
      echo "  WARNING: No saved state - you may need to run 'dhclient ${NET_LAN}' to restore connectivity"
    fi
  fi

  # Delete all wos-tap* devices
  for dev in $(ip -o link show | grep -oP 'wos-tap\d+'); do
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
NUM_VMS="${1:-1}"

echo "=== Setting up network bridge for ${NUM_VMS} VMs ==="

# Capture current IP configuration from NET_LAN before bridging
CURRENT_IP=$(ip -4 addr show "$NET_LAN" 2>/dev/null | grep -oP 'inet \K[\d./]+' | head -1)
CURRENT_GW=$(ip route show default 2>/dev/null | grep -oP 'via \K[\d.]+' | head -1)

if [ -z "$CURRENT_IP" ]; then
  echo "  WARNING: No IP address found on ${NET_LAN}"
fi

# Create bridge if it doesn't exist
if ! ip link show "$BRIDGE" &>/dev/null; then
  echo "  Creating bridge: ${BRIDGE}"
  ip link add "$BRIDGE" type bridge
  # Disable STP — small isolated segment, no loops
  echo 0 > /sys/class/net/${BRIDGE}/bridge/stp_state 2>/dev/null || true
else
  echo "  Bridge ${BRIDGE} already exists"
  # Ensure STP is disabled
  echo 0 > /sys/class/net/${BRIDGE}/bridge/stp_state 2>/dev/null || true
fi

# Disable bridge netfilter — prevents iptables from filtering bridged traffic (including ICMP)
if modprobe br_netfilter 2>/dev/null; then
  echo "  Disabling bridge netfilter (allows ICMP through bridge)"
  sysctl -q -w net.bridge.bridge-nf-call-iptables=0
  sysctl -q -w net.bridge.bridge-nf-call-ip6tables=0
  sysctl -q -w net.bridge.bridge-nf-call-arptables=0
fi

# Add LAN interface to bridge and move IP configuration
if ip link show "$NET_LAN" &>/dev/null; then
  # Save state for teardown
  echo "SAVED_IP=\"$CURRENT_IP\"" > "$STATE_FILE"
  echo "SAVED_GW=\"$CURRENT_GW\"" >> "$STATE_FILE"
  echo "NET_LAN=\"$NET_LAN\"" >> "$STATE_FILE"

  # Remove IP from physical interface
  if [ -n "$CURRENT_IP" ]; then
    echo "  Moving IP config from ${NET_LAN} to ${BRIDGE}"
    ip addr del "$CURRENT_IP" dev "$NET_LAN" 2>/dev/null || true
  fi

  # Add physical interface to bridge
  echo "  Adding ${NET_LAN} to bridge"
  ip link set "$NET_LAN" master "$BRIDGE"

  # Bring up the bridge and assign IP
  ip link set "$BRIDGE" up
  if [ -n "$CURRENT_IP" ]; then
    ip addr add "$CURRENT_IP" dev "$BRIDGE"
  fi

  # Restore default route via bridge
  if [ -n "$CURRENT_GW" ]; then
    ip route del default 2>/dev/null || true
    ip route add default via "$CURRENT_GW" dev "$BRIDGE"
  fi
else
  echo "  WARNING: LAN interface ${NET_LAN} not found"
  ip link set "$BRIDGE" up
fi

# Create TAP devices
for ((i = 0; i < NUM_VMS; i++)); do
  TAP="wos-tap${i}"

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
echo "=== Network bridge ready ==="
echo "    Bridge: ${BRIDGE} (${CURRENT_IP:-no IP})"
echo "    LAN:    ${NET_LAN}"
echo "    Gateway: ${CURRENT_GW:-none}"
echo "    TAPs:   wos-tap{0..$(($NUM_VMS - 1))}"
echo ""
echo "To tear down: sudo $0 --teardown"
