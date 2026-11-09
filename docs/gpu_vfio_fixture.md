# External VFIO GPU fixture

This fixture runs WOS with a **physical** PCI GPU function assigned by an
external Linux KVM/QEMU host. It is a reproducible test layout for Goal 41,
not a claim that any GPU driver or smoke test is complete. A VFIO guest result
qualifies that guest environment only; bare-metal behavior requires a separate
run. Do not substitute an emulated display device, virtio-gpu, a mediated
device, or an SR-IOV virtual function.

## Host and device record

Before each run, record the host kernel, QEMU version, boot identity, GPU PCI
domain:bus:device.function, vendor/device/subsystem/revision IDs, IOMMU group
membership, bound driver for every group member, and reset methods. Preserve the
read-only `lspci -nnvv -s "$GPU_BDF"` and sysfs group inventory. Record the
physical function's BAR/ROM layout, Resizable BAR state, capability chain,
MSI/MSI-X table and PBA locations, and companion audio/USB functions. A host
operator must make the entire required group/reset domain available to VFIO
without resetting an unrelated device. Do not begin if group isolation or
recovery is uncertain.

Use one WOS guest at a time. Keep immutable, hash-identified QCOW2 boot/rootfs
bases and create private QCOW2 overlays. Measure allocated bytes and free space
before creation and after cleanup; set a bound before the run. Never delete a
base, an unrelated image, or an image held by a live process. Keep serial logs
bounded and preserve small failure receipts after removing exact-owned inactive
overlays.

## Guest launch shape

The following is the external-host equivalent of the two-disk AHCI layout in
`scripts/cluster/node_setup.py`. Supply absolute paths. The host must already
have assigned the selected physical function to VFIO; the command does not
change host drivers. Add a `-device vfio-pci,host=...` for each required
companion function after validating its group and reset relationship.
Set `BOOT_BASE`, `ROOT_BASE`, `BOOT_OVERLAY`, `ROOT_OVERLAY`, `GPU_BDF`,
`SERIAL_LOG`, `QMP_SOCKET`, and `OVMF_FD` to the recorded fixture values.

```sh
qemu-img create -f qcow2 -b "$BOOT_BASE" -B qcow2 "$BOOT_OVERLAY"
qemu-img create -f qcow2 -b "$ROOT_BASE" -B qcow2 "$ROOT_OVERLAY"
qemu-img info --backing-chain "$BOOT_OVERLAY"
qemu-img info --backing-chain "$ROOT_OVERLAY"

qemu-system-x86_64 \
  -M q35 -cpu host,migratable=no,+invtsc --enable-kvm -m 8192 -smp 4 \
  -drive "file=$BOOT_OVERLAY,if=none,id=drive0,format=qcow2" \
  -device ahci,id=ahci -device ide-hd,drive=drive0,bus=ahci.0 \
  -drive "file=$ROOT_OVERLAY,if=none,id=drive1,format=qcow2" \
  -device ide-hd,drive=drive1,bus=ahci.1 \
  -device "vfio-pci,host=$GPU_BDF" \
  -chardev "file,id=char0,path=$SERIAL_LOG" -serial chardev:char0 \
  -display none -vga none -nic none -bios "$OVMF_FD" \
  -qmp "unix:$QMP_SOCKET,server=on,wait=off"
```

This command is the **no guest vIOMMU** mode. For the separate virtual-IOMMU
mode, add exactly one of the following devices and record which one was used:

```text
-device intel-iommu,intremap=off,caching-mode=on,aw-bits=48
-device amd-iommu,dma-remap=on,dma-translation=on,intremap=off,xtsup=off,device-iotlb=off
```

The host IOMMU used by VFIO and the IOMMU presented to WOS are different
layers. A run with QEMU's Intel vIOMMU on an AMD host does not qualify native
WOS AMD-Vi hardware. Interrupt remapping is disabled in these two guest
profiles. Under WOS strict policy, an unsupported/no-vIOMMU mode must fail
closed instead of being counted as isolated DMA. Do not enable ATS, PRI, or
PASID as a workaround for a failed capability check.

Record the guest kernel command line together with the QEMU device list. For a
read-only physical PCI diagnostic, use these distinct profiles:

| Guest mode | QEMU IOMMU device | WOS DMA policy | Expected isolation claim |
| --- | --- | --- | --- |
| No vIOMMU compatibility | None | `--iommu-off` | Explicit software passthrough; no guest DMA isolation. |
| Intel vIOMMU translation | `intel-iommu` | `--iommu-strict` | Only a successfully admitted translated guest domain; host VFIO isolation is separate. |
| No vIOMMU negative control | None | `--iommu-strict` | Boot must refuse requested translation with bus mastering inhibited. |

Use the same selected physical function and immutable guest artifacts for a
paired comparison where possible. A successful `--iommu-off` boot establishes
mode distinction and PCI inventory only; it does not authorize DMA submission
or satisfy the BAR, firmware, IRQ, reset, or full GPU smoke tests below.

Treat the AMD virtual-IOMMU option as a compatibility probe until the guest
trace shows a matching IVRS owner, a supported PCI IOMMU function/capability,
and translated-domain activation. An IVRS table alone is insufficient: if the
virtual model omits the required PCI interface, WOS must stop strict boot with
bus mastering inhibited. Record that as an unsupported fixture, not AMD-Vi
qualification; do not fabricate PCI capability bytes or relax strict policy.

## Evidence and exit checks

Freeze the kernel ELF, boot/rootfs hashes, WOS command line, QEMU command and
version, and guest mode for each result. Capture serial output and QMP process
exit. Join the runtime kernel identity to the held ELF. In the WOS PCI trace,
join the selected **physical** vendor/device/subsystem/revision and complete
visible function group to the host inventory. Compare assigned BAR/ROM,
capability and MSI/MSI-X descriptions, allowing documented QEMU/VFIO guest
address relocation while retaining physical-function identity. Record any
missing companion or unsupported capability as a failure of that claim.

Run and report each smoke test separately: bounded BAR read/write with cache
attributes and generation checks; a pinned named/hash firmware request; an
owned DMA map/submit/unmap with accounting return; and MSI/MSI-X delivery,
reset, late-interrupt and teardown checks. These are acceptance targets, not
implicit effects of a PCI enumeration or selftest boot. Count only tests that
the current WOS image actually implements and executes on the assigned device.

After either success or failure, verify the guest shut down or was stopped,
QEMU exited, and no process still references its overlays or socket. Recheck
the host GPU and all companions, driver recovery, and the absence of a new
unowned reset. Remove only this run's verified inactive overlays and socket;
retain bounded logs, hashes and a receipt with explicit pass/fail per claim.
Keep the no-vIOMMU and virtual-IOMMU receipts separate. A cold PCI inventory
alone gives no BAR, firmware, DMA, IRQ, reset, AMD-Vi or bare-metal credit.
