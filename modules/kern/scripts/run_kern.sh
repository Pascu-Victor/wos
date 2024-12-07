#!/bin/bash
set -e
# run  scripts/check_headers.sh
sh scripts/check_headers.sh
if [ "$?" != "0" ]; then
    echo "Error: check_headers.sh failed"
    exit 1
else
    echo "all headers are good"
fi
make -j32

# run scripts/make_image.sh
result=$(sh scripts/make_image.sh)
if [ "$?" != "0" ]; then
    echo "Error: make_image.sh failed"
    exit 1
else
    echo "image created successfully"
fi

# remove old log files
rm -f serial.log
rm -f qemu.*log

# Define the character device to output to a file
CHARDEV="-chardev file,id=char0,path=serial.log -serial chardev:char0 -monitor stdio"

echo "STARTING BOOT:"
qemu-system-x86_64 -m 1G -drive file=disk.qcow2 -bios /usr/share/OVMF/x64/OVMF.4m.fd $CHARDEV -s -S -d cpu_reset,int,tid,in_asm,guest_errors,page,trace:ps2_keyboard_set_translation -D qemu.%d.log -no-reboot -M q35 -cpu max -smp 4
