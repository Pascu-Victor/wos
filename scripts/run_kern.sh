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

echo "STARTING BOOT:"

qemu-system-x86_64 -M q35 -cpu max -enable-kvm -m 32G \
  -drive file=disk.qcow2,if=none,id=drive0,format=qcow2 \
  -device ahci,id=ahci \
  -device ide-hd,drive=drive0,bus=ahci.0 \
  -drive file=test_fat32.qcow2,if=none,id=drive1,format=qcow2 \
  -device ide-hd,drive=drive1,bus=ahci.1 \
  -bios /usr/share/OVMF/x64/OVMF.4m.fd $CHARDEV $GFX_ARGS -d cpu_reset,int,tid,in_asm,guest_errors,page,trace:ps2_keyboard_set_translation -D qemu.%d.log -no-reboot -smp 3
