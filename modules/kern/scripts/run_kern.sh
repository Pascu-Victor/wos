set -e
sh scripts/check_headers.sh && echo "All headers are good"
make -j16
sh scripts/make_image.sh
echo "STARTING BOOT:"
qemu-system-x86_64 -m 256M -drive file=image.hdd,format=raw -bios /usr/share/OVMF/x64/OVMF.fd -chardev stdio,id=char0,mux=on,logfile=serial.log,signal=off \
-serial chardev:char0 -mon chardev=char0 -s -S -d in_asm,op,guest_errors -D qemu.log -no-reboot