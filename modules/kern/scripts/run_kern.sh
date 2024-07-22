set -e
sh scripts/check_headers.sh && echo "All headers are good"
make
sh scripts/make_image.sh
qemu-system-x86_64 -drive file=image.hdd,format=raw -bios /usr/share/OVMF/x64/OVMF.fd -s -S