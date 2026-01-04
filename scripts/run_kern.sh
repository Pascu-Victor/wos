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

echo "STARTING BOOT:"

qemu-system-x86_64 -M q35 -cpu host -enable-kvm -m 8G \
  -drive file=disk.qcow2,if=none,id=drive0,format=qcow2 \
  -device ahci,id=ahci \
  -device ide-hd,drive=drive0,bus=ahci.0 \
  -drive file=test_fat32.qcow2,if=none,id=drive1,format=qcow2 \
  -device ide-hd,drive=drive1,bus=ahci.1 \
  -bios /usr/share/OVMF/x64/OVMF.4m.fd $CHARDEV $GFX_ARGS $DEBUG_ARGS -d cpu_reset,int,tid,in_asm,guest_errors,page,trace:ps2_keyboard_set_translation -D qemu.%d.log -no-reboot -smp 10
