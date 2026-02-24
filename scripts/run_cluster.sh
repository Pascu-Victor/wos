#!/bin/bash
set -e

# -- run_cluster.sh — Launch an N-node WKI cluster --------------------------
#
# Usage:  ./scripts/run_cluster.sh [N] [--skip N]
#
# Launches N QEMU VMs (default 2) configured for WKI inter-kernel communication.
# --skip N    Start VM IDs from N instead of 0 (useful for adding to existing cluster)
# Each VM gets:
#   - A CoW overlay disk (preserves base images)
#   - Unique VM ID, MAC addresses, serial/QEMU logs
#   - A dedicated WKI NIC (e1000e) on wos-wki-tap<N>
#   - Shared ivshmem region for RDMA transport
#
# Prerequisites:
#   - Base disk images: disk.qcow2, test_fat32.qcow2
#   - WKI bridge + taps created: ./scripts/setup_wki_bridge.sh <N>
#   - (Optional) Regular networking bridge wos-br0 + wos-wki-tap<N> for each VM
#
# Environment:
#   WOS_NET=user   — Use QEMU user-mode networking instead of TAP for eth0
# ----------------------------------------------------------------------------

NUM_VMS=2
SKIP=0
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --skip)
      SKIP="$2"
      shift 2
      ;;
    *)
      NUM_VMS="$1"
      shift
      ;;
  esac
done

SPAWNED=$((NUM_VMS - SKIP))
echo "=== WKI Cluster: launching ${SPAWNED} VMs (total cluster size: ${NUM_VMS}, starting from VM ID ${SKIP}) ==="

# Verify base disk images exist
if [ ! -f disk.qcow2 ]; then
  echo "ERROR: disk.qcow2 not found in $(pwd). Run from the project root." >&2
  exit 1
fi
if [ ! -f test_fat32.qcow2 ]; then
  echo "ERROR: test_fat32.qcow2 not found in $(pwd). Run from the project root." >&2
  exit 1
fi

# Verify the WKI bridge exists
if ! ip link show wos-wki-br0 &>/dev/null; then
  echo "ERROR: wos-wki-br0 bridge not found. Run: sudo ./scripts/setup_wki_bridge.sh ${NUM_VMS}" >&2
  exit 1
fi

# Create ivshmem backing file
IVSHMEM_PATH="/dev/shm/wos-ivshmem"
if [ ! -f "$IVSHMEM_PATH" ]; then
  echo "Creating ivshmem backing file: ${IVSHMEM_PATH} (16M)"
  dd if=/dev/zero of="$IVSHMEM_PATH" bs=1M count=16 2>/dev/null
  chmod 666 "$IVSHMEM_PATH"
fi

# Create overlay directory
OVERLAY_DIR="cluster-overlays"
mkdir -p "$OVERLAY_DIR"

PIDS=()

cleanup() {
  echo ""
  echo "=== Shutting down cluster ==="
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  wait 2>/dev/null || true
  echo "=== Cluster stopped ==="
}
trap cleanup EXIT INT TERM

for ((i = SKIP; i < NUM_VMS; i++)); do
  echo "--- Preparing VM${i} ---"

  # Create CoW overlay disks (recreate each run for clean state)
  OVERLAY0="${OVERLAY_DIR}/disk-vm${i}.qcow2"
  OVERLAY1="${OVERLAY_DIR}/fat32-vm${i}.qcow2"
  qemu-img create -f qcow2 -b "$(pwd)/disk.qcow2" -F qcow2 "$OVERLAY0" >/dev/null
  qemu-img create -f qcow2 -b "$(pwd)/test_fat32.qcow2" -F qcow2 "$OVERLAY1" >/dev/null

  # Verify WKI tap device exists
  WKI_TAP="wos-wki-tap${i}"
  if ! ip link show "$WKI_TAP" &>/dev/null; then
    echo "WARNING: ${WKI_TAP} not found — VM${i} will not have WKI NIC"
    WKI_TAP=""
  fi

  # Launch VM in background (no monitor stdio — use serial log)
  # Default to user-mode networking for eth0 (WKI bridge handles inter-kernel on eth1)
  unset WOS_NET
  export WOS_MEM="${WOS_MEM:-4G}"
  export WOS_VM_ID="$i"
  export WOS_DISK0="$OVERLAY0"
  export WOS_DISK1="$OVERLAY1"
  export WOS_WKI_TAP="$WKI_TAP"
  export WOS_IVSHMEM="$IVSHMEM_PATH"

  bash "${SCRIPT_DIR}/run_kern.sh" &
  PIDS+=($!)

  echo "--- VM${i} launched (PID ${PIDS[-1]}) ---"
done

echo ""
echo "=== All ${SPAWNED} VMs launched (IDs ${SKIP}-$((NUM_VMS - 1))) ==="
echo "    Serial logs: serial-vm{$SKIP..$(($NUM_VMS - 1))}.log"
echo "    Overlays:    ${OVERLAY_DIR}/"
echo "    ivshmem:     ${IVSHMEM_PATH}"
echo ""
echo "Press Ctrl+C to stop all VMs."

# Wait for all VMs
wait
