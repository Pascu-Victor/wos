# WOS Script Agent Notes

Public user-facing commands are exposed through `bin/`; implementation lives
under this `scripts/` tree. Keep new implementation scripts in the matching
role directory and add a `bin/` symlink only when the command is intended to be
part of the public workflow.

## Isolated KTEST workflow

`bin/wos-ktest` is implemented by `scripts/test/ktest_setup.py`. It owns the
isolated diagnostic selftest workflow and must not mutate the normal WOS build
or VM images.

Default KTEST roots:

- `build-ktest/`
- `ktest-data/sysroot`
- `ktest-data/mlibc-build`
- `ktest-data/busybox-build`
- `ktest-data/busybox-install`
- `ktest-data/dropbear-build`
- `ktest-data/make-build`
- `ktest-data/python-build`
- `ktest-data/disk.qcow2`
- `ktest-data/mountfs.qcow2`
- `ktest-data/overlays/`

When changing KTEST packaging helpers, preserve the environment overrides used
by `ktest_setup.py`: `WOS_BUILD_DIR`, `WOS_SYSROOT_PATH`,
`WOS_BUSYBOX_INSTALL_DIR`, `WOS_BOOT_DISK`, `WOS_ROOTFS_DISK`, and
`WOS_KERNEL_CMDLINE`.

`scripts/build/make_image.sh` must place boot files at exact target paths:
`/EFI/BOOT/BOOTX64.EFI`, `/limine/limine.conf`, `/wos`, and
`/initramfs.cpio`. Do not use source basenames for generated Limine configs.

## Checks

For script changes, prefer:

- `python3 -m py_compile scripts/cluster/node_setup.py scripts/cluster/cluster_setup.py scripts/test/ktest_setup.py`
- `bash -n scripts/build/*.sh`
- `bin/wos-ktest --help`
- `bin/wos-ktest --build-only`

Runtime VM confirmation is user-run unless explicitly requested and available.
