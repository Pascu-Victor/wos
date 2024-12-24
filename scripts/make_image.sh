# Create an empty zeroed-out 64MiB image file.
set -e

if [ -e disk.qcow2 ]; then
    rm disk.qcow2
fi

CWD=$(pwd)

qemu-img create -f qcow2 disk.qcow2 64M
guestfish --rw -a disk.qcow2 <<_EOF_
run
part-init /dev/sda gpt
part-add /dev/sda p 2048 126976
mkfs fat /dev/sda1
mount /dev/sda1 /
mkdir /EFI
mkdir /EFI/BOOT
mkdir /boot
mkdir /boot/limine
copy-in $CWD/build/modules/kern/wos /boot
copy-in $CWD/modules/kern/limine.conf /boot/limine
copy-in /usr/share/limine/BOOTX64.EFI /EFI/BOOT
copy-in $CWD/build/modules/init/init /boot
umount /
_EOF_
