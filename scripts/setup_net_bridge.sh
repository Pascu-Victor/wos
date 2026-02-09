#!/bin/bash
set -e

# -- setup_net_bridge.sh — Create network bridge and TAP devices ------------
#
# Usage:  sudo ./scripts/setup_net_bridge.sh [N]
#         sudo ./scripts/setup_net_bridge.sh --no-lan [N]
#         sudo ./scripts/setup_net_bridge.sh --vm [N]
#
# Creates:
#   - wos-br0             — Linux bridge for VM network traffic
#   - wos-tap0..tapN-1    — TAP devices attached to the bridge (one per VM)
#
# By default, moves the IP configuration from NET_LAN to the bridge so that
# both the host and VMs share the same network segment with internet access.
#
# With --no-lan flag, only creates bridge and TAP devices without linking to
# any physical interface. Useful when connecting to an external router VM.
#
# With --vm flag, connects vmnet1 to wos-br0 and configures host IP for
# accessing a router VM network (removes vmnet1's IP, bridges it, and assigns
# 10.10.0.100/16 to wos-br0).
#
# To tear down:  sudo ./scripts/setup_net_bridge.sh --teardown
# ----------------------------------------------------------------------------

if [ "$EUID" -ne 0 ]; then
  echo "ERROR: This script must be run as root (sudo)." >&2
  exit 1
fi

BRIDGE="wos-br0"
NET_LAN="${NET_LAN:-wlp2s0}"
REAL_USER="${SUDO_USER:-$USER}"
STATE_FILE="/var/run/wos-bridge-state"

# -- Parse flags -------------------------------------------------------------
SKIP_LAN=0
USE_VMNET=0
TEARDOWN=0
VMNET_DEV="vmnet1"
VMNET_BRIDGE_IP="10.10.0.100/16"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --teardown)
      TEARDOWN=1
      shift
      ;;
    --no-lan)
      SKIP_LAN=1
      shift
      ;;
    --vm)
      USE_VMNET=1
      shift
      ;;
    *)
      break
      ;;
  esac
done

# -- Teardown mode -----------------------------------------------------------
if [ "${TEARDOWN:-0}" -eq 1 ]; then
  if [ -f "$STATE_FILE" ]; then
    source "$STATE_FILE"

    if [ "${USE_VMNET:-0}" -eq 1 ]; then
      # Restore vmnet1 configuration
      echo "  Restoring vmnet configuration"

      # Remove vmnet from bridge
      if [ -n "$VMNET_DEV" ]; then
        ip link set "$VMNET_DEV" nomaster 2>/dev/null || true

        # Restore vmnet IP
        if [ -n "$SAVED_VMNET_IP" ]; then
          echo "  Restoring ${SAVED_VMNET_IP} to ${VMNET_DEV}"
          ip addr add "$SAVED_VMNET_IP" dev "$VMNET_DEV" 2>/dev/null || true
        fi
      fi
    else
      # Restore regular NET_LAN configuration
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
# For --no-lan mode, skip IP migration (external router handles DHCP)
if [ "$SKIP_LAN" -eq 0 ]; then
  CURRENT_IP=$(ip -4 addr show "$NET_LAN" 2>/dev/null | grep -oP 'inet \K[\d./]+' | head -1)
  CURRENT_GW=$(ip route show default 2>/dev/null | grep -oP 'via \K[\d.]+' | head -1)

  if [ -z "$CURRENT_IP" ]; then
    echo "  WARNING: No IP address found on ${NET_LAN}"
  fi
else
  echo "  No-LAN mode: bridge will have no master interface"
  CURRENT_IP=""
  CURRENT_GW=""
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

# Handle VM mode or regular LAN mode
if [ "$USE_VMNET" -eq 1 ]; then
  # VM mode: bridge vmnet1 to wos-br0 for router VM access
  if ip link show "$VMNET_DEV" &>/dev/null; then
    # Save original vmnet1 IP for teardown
    VMNET_IP=$(ip -4 addr show "$VMNET_DEV" 2>/dev/null | grep -oP 'inet \K[\d./]+' | head -1)
    echo "SAVED_VMNET_IP=\"$VMNET_IP\"" > "$STATE_FILE"
    echo "VMNET_DEV=\"$VMNET_DEV\"" >> "$STATE_FILE"
    echo "USE_VMNET=\"1\"" >> "$STATE_FILE"

    # Remove vmnet1's IP so it doesn't conflict with router
    if [ -n "$VMNET_IP" ]; then
      echo "  Removing ${VMNET_IP} from ${VMNET_DEV}"
      ip addr del "$VMNET_IP" dev "$VMNET_DEV" 2>/dev/null || true
    fi

    # Add vmnet1 to bridge
    echo "  Adding ${VMNET_DEV} to bridge"
    ip link set "$VMNET_DEV" master "$BRIDGE"

    # Bring up the bridge
    ip link set "$BRIDGE" up

    # Assign host IP on wos-br0 for accessing router VM network
    echo "  Assigning ${VMNET_BRIDGE_IP} to ${BRIDGE}"
    ip addr add "$VMNET_BRIDGE_IP" dev "$BRIDGE"
  else
    echo "  ERROR: ${VMNET_DEV} not found"
    exit 1
  fi
fi

# Add LAN interface to bridge (unless --no-lan)
if [ "$SKIP_LAN" -eq 0 ]; then
  if ip link show "$NET_LAN" &>/dev/null; then
    # Save state for teardown
    echo "SAVED_IP=\"$CURRENT_IP\"" > "$STATE_FILE"
    echo "SAVED_GW=\"$CURRENT_GW\"" >> "$STATE_FILE"
    echo "NET_LAN=\"$NET_LAN\"" >> "$STATE_FILE"

    # Move IP from physical interface to bridge
    if [ -n "$CURRENT_IP" ]; then
      echo "  Moving IP config from ${NET_LAN} to ${BRIDGE}"
      ip addr del "$CURRENT_IP" dev "$NET_LAN" 2>/dev/null || true
    fi

    # Bring up the interface before adding to bridge
    ip link set "$NET_LAN" up

    # Add physical interface to bridge
    echo "  Adding ${NET_LAN} to bridge"
    ip link set "$NET_LAN" master "$BRIDGE"

    # Bring up the bridge
    ip link set "$BRIDGE" up

    # Assign IP to bridge and restore gateway
    if [ -n "$CURRENT_IP" ]; then
      ip addr add "$CURRENT_IP" dev "$BRIDGE"
    fi

    if [ -n "$CURRENT_GW" ]; then
      ip route del default 2>/dev/null || true
      ip route add default via "$CURRENT_GW" dev "$BRIDGE"
    fi
  else
    echo "  WARNING: LAN interface ${NET_LAN} not found"
    ip link set "$BRIDGE" up
  fi
else
  # No-LAN mode: just bring up the bridge without any master interface
  echo "  No master interface - bridge is isolated"
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
  ip tuntap add dev "$TAP" mode tap
  chown "$REAL_USER" /dev/net/tun
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
