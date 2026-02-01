#!/bin/bash
set -e

# remove old log files
rm -f serial.log
rm -f qemu.*log

# Define the character device to output to a file
CHARDEV="-chardev file,id=char0,path=serial.log -serial chardev:char0 -monitor stdio"

# Define graphics mode - disable if WOS_ENABLE_GFX is not set
if [ -z "$WOS_ENABLE_GFX" ]; then
  GFX_ARGS="-display none"
else
  GFX_ARGS=""
fi

# Check for debug flag
DEBUG_ARGS=""
for arg in "$@"
do
    if [ "$arg" == "--debug" ]; then
        # Explicit GDB bind to IPv4 loopback
        DEBUG_ARGS="-gdb tcp:127.0.0.1:1234 -S"
        # ISA-DebugCon on port 23456 (telnet)
        DEBUG_ARGS="$DEBUG_ARGS -chardev socket,id=debugger,port=23456,host=0.0.0.0,server=on,wait=off,telnet=on -device isa-debugcon,iobase=0x402,chardev=debugger"
        # Additional Monitor on port 3002
        DEBUG_ARGS="$DEBUG_ARGS -monitor tcp:0.0.0.0:3002,server,nowait"
    fi
done

# Network configuration
# Set WOS_NET=user for QEMU user-mode networking (built-in DHCP server at 10.0.2.2)
# Default: TAP device wos-tap0 attached to bridge wos-br0 (requires host DHCP server)
if [ "$WOS_NET" = "user" ]; then
  NET_ARGS="-netdev user,id=net0"
  NET_ARGS="$NET_ARGS -device virtio-net-pci,netdev=net0,mac=52:54:00:12:34:56"
  echo "Using QEMU user-mode networking (DHCP: 10.0.2.2, IP: 10.0.2.15)"
else
  # Setup: sudo ip tuntap add dev wos-tap0 mode tap user $USER
  #        sudo ip link set wos-tap0 master wos-br0
  #        sudo ip link set wos-tap0 up
  NET_ARGS="-netdev tap,id=net0,ifname=wos-tap0,script=no,downscript=no"
  NET_ARGS="$NET_ARGS -device virtio-net-pci,netdev=net0,mac=52:54:00:12:34:56"
  echo "Using TAP networking via wos-tap0 (set WOS_NET=user for built-in DHCP)"
fi
# NET_ARGS="$NET_ARGS -netdev user,id=net1,hostfwd=tcp::8081-:81"
# NET_ARGS="$NET_ARGS -device e1000e,netdev=net1,mac=52:54:00:12:34:57"
# NET_ARGS="$NET_ARGS -device qemu-xhci,id=xhci"
# NET_ARGS="$NET_ARGS -netdev user,id=net2"
# NET_ARGS="$NET_ARGS -device usb-net,netdev=net2,bus=xhci.0"
# Optional ivshmem for inter-VM DMA networking (requires two QEMU instances sharing /dev/shm/wos-ivshmem):
# NET_ARGS="$NET_ARGS -object memory-backend-file,size=16M,share=on,mem-path=/dev/shm/wos-ivshmem,id=hmem -device ivshmem-plain,memdev=hmem"

echo "STARTING BOOT:"

LOG_ARGS="-d cpu_reset,int,tid,in_asm,guest_errors,page,trace:ps2_keyboard_set_translation -D qemu.%d.log"

qemu-system-x86_64 -M q35 -cpu host -enable-kvm -m 24G \
  -drive file=disk.qcow2,if=none,id=drive0,format=qcow2 \
  -device ahci,id=ahci \
  -device ide-hd,drive=drive0,bus=ahci.0 \
  -drive file=test_fat32.qcow2,if=none,id=drive1,format=qcow2 \
  -device ide-hd,drive=drive1,bus=ahci.1 \
  $NET_ARGS \
  -bios /usr/share/OVMF/x64/OVMF.4m.fd $CHARDEV $GFX_ARGS $DEBUG_ARGS $LOG_ARGS -no-reboot -smp 8
