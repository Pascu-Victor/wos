#!/bin/bash
set -e

function check_headers() {
    sh scripts/check_headers.sh $1
    if [ "$?" != "0" ]; then
        echo "Error: check_headers.sh failed for $1"
        exit 1
    else
        echo "$1 headers are good"
    fi
}
# run check_headers
check_headers "modules/kern"
check_headers "modules/init"

cmake -B build -GNinja .
cmake --build build

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
qemu-system-x86_64 -cpu max -s -S -m 1G -drive file=disk.qcow2 -bios /usr/share/OVMF/x64/OVMF.4m.fd $CHARDEV -d cpu_reset,int,tid,in_asm,guest_errors,page,trace:ps2_keyboard_set_translation -D qemu.%d.log -no-reboot -smp 1
