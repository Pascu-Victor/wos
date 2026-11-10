# Rootless VFIO reset guard

`scripts/test/vfio_reset_guard.py` installs a Linux x86-64 seccomp filter and
then executes a program. It denies `VFIO_DEVICE_RESET` and
`VFIO_DEVICE_PCI_HOT_RESET` on every `ioctl` FD with `EPERM`. The latter is the
explicit slot/bus reset. Linux PCI reset-method selection can also choose a
bus method for the former, so both requests are denied. Other syscalls and
ioctls continue to the kernel. The filter persists across `exec` and child
processes. An installation failure stops the launch.

Pass the wrapper as the executable in the process that owns QEMU's inherited
VFIO and iommufd descriptors:

```text
/usr/bin/python3 scripts/test/vfio_reset_guard.py -- /usr/bin/qemu-system-x86_64 ...
```

This is a launch constraint, not a reset-domain lease. It cannot prevent
kernel-initiated resets on VFIO device close, resets requested by another
process, driver actions, or host power transitions. The host owner must hold
every affected function and retain a tested recovery path. Query the exact
physical dependency set with `vfio_hot_reset_info.query()` while the VFIO
cdev is bound; a guest PCI tree or an IOMMU group is insufficient evidence.

The Linux VFIO cdev path admits only one active binding per device. A GPU FD
already bound by a host reset broker cannot simply be handed to stock QEMU:
QEMU attempts its own bind. Closing the broker's GPU FD can itself trigger
VFIO's close reset path. A future host lease must explicitly qualify this
transition, preserve sibling ownership, and recheck the dependency set after
QEMU binds. The guard does not make that transition safe by itself. If QEMU
requires either denied ioctl during initialization, treat the launch as
failed and recover the host; do not remove the filter to force it through.

The host unit test uses `/dev/null` and a child `exec`; it does not require
root, open VFIO, or operate a GPU.

One selected physical AMD VFIO guest also booted WOS through the guard under
an existing single-function host lease. Its read-only GPU discovery and
verified firmware-header diagnostic passed, followed by natural shutdown,
QEMU exit zero, disposable-overlay cleanup and native host recovery. The
physical hot-reset query still reported six unowned sibling functions, so the
run did not attempt a shared reset or qualify a full-domain lease, GPU DMA or
interrupt delivery. The private result and serial SHA-256 values are recorded
in Goal 41's local evidence ledger.
