#!/bin/bash
set -e

# -- Multi-VM environment variables ------------------------------------------
# WOS_VM_ID     — VM identifier (0, 1, 2, …). Default: 0. Derives unique MACs and log names.
# WOS_NET       — "user" for QEMU user-mode networking, otherwise TAP (default).
# WOS_WKI_TAP   — TAP device for dedicated WKI NIC (e1000e). Omit to skip.
# WOS_IVSHMEM   — Shared memory path for ivshmem (e.g. /dev/shm/wos-ivshmem). Omit to skip.
# WOS_DISK0     — Path to primary disk image.  Default: disk.qcow2
# WOS_DISK1     — Path to secondary disk image. Default: test_fat32.qcow2
# WOS_MEM       — Guest RAM size. Default: 4G (single VM), cluster script overrides to 4G.
# WOS_ENABLE_GFX — Set to enable graphical display. Default: display none.
# ----------------------------------------------------------------------------

VM_ID="${WOS_VM_ID:-0}"
MEM="${WOS_MEM:-4G}"
DISK0="${WOS_DISK0:-disk.qcow2}"
DISK1="${WOS_DISK1:-test_fat32.qcow2}"

# Derive a per-VM hex byte for MAC uniqueness (0x56 + VM_ID, matching legacy default)
MAC_BYTE=$(printf '%02x' $((0x56 + VM_ID)))

# Per-VM log file names
SERIAL_LOG="serial-vm${VM_ID}.log"
QEMU_LOG="qemu-vm${VM_ID}.%d.log"

# remove old log files for this VM
rm -f "$SERIAL_LOG"
rm -f qemu-vm${VM_ID}.*log

# Define the character device to output to a file
CHARDEV="-chardev file,id=char0,path=${SERIAL_LOG} -serial chardev:char0 -monitor stdio"

# Define graphics mode - disable if WOS_ENABLE_GFX is not set
if [ -z "$WOS_ENABLE_GFX" ]; then
  GFX_ARGS="-display none"
else
  GFX_ARGS=""
fi

# Check for debug flag
DEBUG_ARGS=""
IS_DEBUG=0
for arg in "$@"
do
    if [ "$arg" == "--debug" ]; then
        IS_DEBUG=1
        # Create debug-specific overlays independent of cluster overlays
        OVERLAY_DIR="cluster-overlays"
        mkdir -p "$OVERLAY_DIR"

        DEBUG_DISK0="${OVERLAY_DIR}/debug-disk-vm${VM_ID}.qcow2"
        DEBUG_DISK1="${OVERLAY_DIR}/debug-fat32-vm${VM_ID}.qcow2"

        # Remove old debug overlays for clean state
        rm -f "$DEBUG_DISK0" "$DEBUG_DISK1"

        # Create fresh debug overlays
        qemu-img create -f qcow2 -b "$(pwd)/disk.qcow2" -F qcow2 "$DEBUG_DISK0" >/dev/null
        qemu-img create -f qcow2 -b "$(pwd)/test_fat32.qcow2" -F qcow2 "$DEBUG_DISK1" >/dev/null

        # Override disk paths to use debug overlays
        DISK0="$DEBUG_DISK0"
        DISK1="$DEBUG_DISK1"

        DEBUG_PORT=$((1234 + VM_ID))
        DEBUGCON_PORT=$((23456 + VM_ID))
        MONITOR_PORT=$((3002 + VM_ID))
        # Explicit GDB bind to IPv4 loopback
        DEBUG_ARGS="-gdb tcp:127.0.0.1:${DEBUG_PORT} -S"
        # ISA-DebugCon (telnet)
        DEBUG_ARGS="$DEBUG_ARGS -chardev socket,id=debugger,port=${DEBUGCON_PORT},host=0.0.0.0,server=on,wait=off,telnet=on -device isa-debugcon,iobase=0x402,chardev=debugger"
        # Additional Monitor
        DEBUG_ARGS="$DEBUG_ARGS -monitor tcp:0.0.0.0:${MONITOR_PORT},server,nowait"
    fi
done

# -- Primary NIC (eth0 — virtio-net, regular networking) ---------------------
# Set WOS_NET=user for QEMU user-mode networking (built-in DHCP server at 10.0.2.2)
# Default: TAP device wos-tap<VM_ID> attached to bridge wos-br0
if [ "$WOS_NET" = "user" ]; then
  NET_ARGS="-netdev user,id=net0"
  NET_ARGS="$NET_ARGS -device virtio-net-pci,netdev=net0,mac=52:54:00:12:34:${MAC_BYTE}"
  echo "[VM${VM_ID}] eth0: QEMU user-mode networking (DHCP: 10.0.2.2)"
else
  TAP_DEV="wos-lan-N${VM_ID}"
  NET_ARGS="-netdev tap,id=net0,ifname=${TAP_DEV},script=no,downscript=no"
  NET_ARGS="$NET_ARGS -device virtio-net-pci,netdev=net0,mac=52:54:00:12:34:${MAC_BYTE}"
  echo "[VM${VM_ID}] eth0: TAP networking via ${TAP_DEV} MAC: 52:54:00:12:34:${MAC_BYTE} (set WOS_NET=user for built-in DHCP)"
fi

# -- WKI NIC (eth1 — virtio-net, dedicated inter-kernel bridge) --------------
# Auto-detect: if WOS_WKI_TAP is not set but debug mode and bridge exists, use wos-wki-tap<VM_ID>
if [ -z "$WOS_WKI_TAP" ] && [ "$IS_DEBUG" -eq 1 ]; then
  AUTO_WKI_TAP="wos-wki-tap${VM_ID}"
  if ip link show "$AUTO_WKI_TAP" &>/dev/null 2>&1; then
    WOS_WKI_TAP="$AUTO_WKI_TAP"
    echo "[VM${VM_ID}] Auto-detected WKI TAP: ${WOS_WKI_TAP}"
  fi
fi

if [ -n "$WOS_WKI_TAP" ]; then
  NET_ARGS="$NET_ARGS -netdev tap,id=net1,ifname=${WOS_WKI_TAP},script=no,downscript=no"
  NET_ARGS="$NET_ARGS -device virtio-net-pci,netdev=net1,mac=52:54:00:12:35:${MAC_BYTE}"
  echo "[VM${VM_ID}] eth1: WKI bridge via ${WOS_WKI_TAP}"
fi

# # -- ivshmem RDMA transport --------------------------------------------------
# IVSHMEM_ARGS=""
# if [ -n "$WOS_IVSHMEM" ]; then
#   IVSHMEM_ARGS="-object memory-backend-file,size=16M,share=on,mem-path=${WOS_IVSHMEM},id=hmem -device ivshmem-plain,memdev=hmem"
#   echo "[VM${VM_ID}] ivshmem: ${WOS_IVSHMEM}"
# fi

echo "[VM${VM_ID}] STARTING BOOT:"

LOG_ARGS="-d cpu_reset,int,tid,in_asm,nochain,guest_errors,page,trace:ps2_keyboard_set_translation -D ${QEMU_LOG}"

qemu-system-x86_64 -M q35 -cpu max -m ${MEM} \
  -drive file=${DISK0},if=none,id=drive0,format=qcow2 \
  -device ahci,id=ahci \
  -device ide-hd,drive=drive0,bus=ahci.0 \
  -drive file=${DISK1},if=none,id=drive1,format=qcow2 \
  -device ide-hd,drive=drive1,bus=ahci.1 \
  $NET_ARGS \
  $IVSHMEM_ARGS \
  -bios /usr/share/OVMF/x64/OVMF.4m.fd $CHARDEV $GFX_ARGS $DEBUG_ARGS $LOG_ARGS -no-reboot -smp 2
