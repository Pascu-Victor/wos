# Config Agent Notes

`node.json` is the single-node VM spec used by `bin/wos-ktest`. Keep its layout
compatible with `scripts/cluster/node_setup.py` and the cluster node spec shape:
top-level `build`, `package`, and `node` sections, with `node.vm` and
`node.nics` carrying the VM and network configuration.

KTEST defaults should remain isolated from the normal WOS VM:

- `build.dir`: `build-ktest`
- `build.sysroot`: `ktest-data/sysroot`
- `node.vm.disk0`: `ktest-data/disk.qcow2`
- `node.vm.disk1`: `ktest-data/mountfs.qcow2`
- `node.vm.overlay_dir`: `ktest-data/overlays`
- `package.kernel_cmdline`: normally `--selftest`

The KTEST node is intentionally large for diagnostics: 32 vCPUs, 32 GiB RAM, a
LAN NIC, and a WKI NIC. Preserve those defaults unless the user asks for a
different selftest machine profile.

`cluster.json` remains the multi-node cluster topology. Keep cluster and KTEST
VM specs aligned through shared field names rather than adding one-off config
keys for the same QEMU behavior.

`disks.conf` supports `WOS_BOOT_DISK` and `WOS_ROOTFS_DISK` environment
overrides so isolated packaging can generate fstab entries for KTEST disks
without touching `disk.qcow2` or `mountfs.qcow2`.
