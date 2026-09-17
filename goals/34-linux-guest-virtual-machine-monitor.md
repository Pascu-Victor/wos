# Goal 34: A WOS Virtual Machine Monitor for Linux Guests

## Copy-pastable goal command

```text
/goal Deliver a WOS-hosted userspace VMM that uses both Intel VMX/EPT and AMD SVM/NPT through the same native hypervisor ABI to boot an unmodified pinned x86_64 Linux kernel with SMP, serial console, virtio block and network, and complete VM lifecycle control through a shared client library, per-VM VMM process, local vmmd manager and vmctl. Implement the userspace contract in goals/VIRTUALIZATION-ARCHITECTURE.md. Verify all criteria in goals/34-linux-guest-virtual-machine-monitor.md through device parser tests, target integration, Normal Build WOS, and user-run Linux guest workload evidence on both CPU vendors. Preserve guest/host isolation and bounded device queues. Limit work to the specified machine model, VMM, ABI extensions, packaging and tests; iterate with bounded subagents and an evidence ledger, and apply the file's blocked rule for missing prerequisites or runtime evidence.
```

## Outcome and scale

Make WOS useful as a virtualization host: a packaged WOS utility launches and
controls a Linux VM through goal 33's kernel API, with working disk, network,
console, and SMP. **Target: one to two continuous weeks with an integration
agent and up to three active subagents.** A guest printing one serial message
is a milestone; the finish line is a usable, stress-tested machine.

**Hard prerequisite:** goal 33's accepted VM execution ABI, or equivalent
validated local implementation supporting both Intel and AMD. Read its actual
source and evidence rather than assuming its completion from numbering. The
[userspace architecture contract](VIRTUALIZATION-ARCHITECTURE.md) is a required
part of this goal. Both vendor backends must pass the same guest fixtures;
missing evidence on one vendor leaves the goal incomplete.

## Baseline and impact map

Investigated 2026-09-08. `modules/kern/src/dev/virtio/virtio.hpp`,
`virtio.cpp`, and `virtio_net.cpp` implement WOS consuming virtual devices.
They are reference guest drivers, not a host-side device emulator.
`scripts/cluster/node_setup.py::build_qemu_args()` launches WOS under host QEMU;
it is not a WOS VMM.

Read goal 33's implementation, `modules/CMakeLists.txt`,
`modules/kern/src/vfs/file.hpp`, `vfs/file_operations.hpp`,
`dev/block_device.hpp`, `net/netdevice.hpp`, `net/netpoll.hpp`,
`platform/acpi/apic/apic.cpp`, `platform/ktime/ktime.hpp`,
`configs/rootfs/aliases.tsv`, and `scripts/cluster/node_setup.py`.
Before extending wrappers, read `.github/instructions/mlibc.instructions.md`
and the corresponding local sysdeps.

Deliver `libwosvirt` in proposed `shared/virtualization/`, `wos-vmm` in
`modules/vmm/`, the local node manager in `modules/vmmd/`, and `vmctl` in
`modules/vmctl/`, with CMake/rootfs/service integration. All are proposed new
components. Use a common supported ISA build policy on Intel and AMD rather
than silently inheriting build-host-only `-march=native` requirements.

Freeze the `wos-pc-v1` machine model: direct Linux kernel/initramfs boot,
ACPI/PCI description, SMP startup, serial, kernel virtual LAPIC, userspace I/O
APIC and necessary legacy timer/interrupt devices, plus modern virtio devices.
Pin kernel/initramfs/disk hashes and guest configuration. Arbitrary firmware
and installer ISO boot remain a separate future capability.

Flow: VM config → validated image loader → memory/vCPU setup → run loop →
MMIO/PIO exit → device queue validation → WOS file/network I/O → completion and
virtual interrupt. Guest addresses always translate through retained VM slots;
never treat them as WOS userspace or host physical pointers.

## Required userspace implementation

1. `libwosvirt` wraps versioned VM/vCPU handles, checked memory slots, run/exit
   completion records, event injection and teardown. It must exercise actual
   WOS ioctl/usercopy/FD semantics. A mapped run page is not assumed available.
2. One `wos-vmm` process owns each VM, with one thread per vCPU, a device/control
   event loop and a bounded I/O worker pool. It allocates/registers guest RAM,
   loads Linux through its documented boot protocol, configures initial CPU
   state and enters `VCPU_RUN`. The kernel executes the guest with VMX or SVM;
   normalized PIO/MMIO exits return to the common userspace device model.
3. CPU exceptions, second-level translation and virtual LAPIC/event injection
   remain in the kernel. The VMM emulates UART, PCI, I/O APIC and device registers,
   validates virtqueues, performs backing I/O and signals completion. Explicitly
   define instruction completion, IRQ level/EOI and reset-generation behavior.
4. Block requests use authorized positional I/O and flush on raw-image/block
   handles. Network requests require a new bounded, pollable Ethernet endpoint
   and explicit uplink attachment policy. Existing `/dev/net` statistics nodes
   and ordinary TCP sockets do not provide that interface. Implement bidirectional
   frames, backpressure, MAC/MTU checks and close/fence handling.
5. `vmmd` owns authorized definitions and local supervision, while `vmctl`
   supplies validate/define/start/list/inspect/console/pause/resume/stop/destroy.
   The manager preflights resources, launches the VMM with limited authority and
   reconciles live identities on restart. Definitions and runtime status are
   separate; `destroy` does not implicitly delete guest disks.
6. Implement the architecture document's boot sequence, address translation,
   device data flow, local CLI semantics and lifetime rules. State/export hooks
   must allow later migration to capture userspace device state, while remote
   control/transport itself belongs to goal 35.

## Checkpoints and parallel ownership

1. Freeze library/ABI, boot/machine/exit and packet-endpoint contracts; subagents
   own loader/run loop, device models/tests, and manager/CLI/packaging in separate
   directories. The primary owns kernel integrations and common headers.
2. Integrate boot, serial, virtual timers and interrupts; prove secondary CPU
   startup before adding heavy I/O.
3. Integrate virtio block and network, pause/resume/reset/shutdown, process death,
   queue backpressure and bounded guest memory access.
4. Run hostile descriptor tests and user-run guest workloads; audit the whole
   queue-to-page-pin lifetime and reconcile any required kernel API extensions.

## Acceptance criteria

1. An unmodified pinned Linux kernel boots to a shell with 1 and 4 vCPUs, using
   declared supported kernel configuration and reproducible rootfs artifacts,
   independently on Intel VMX/EPT and AMD SVM/NPT. Use identical guest artifacts.
2. Linux enumerates virtio block and network; guest file creation, fsync, reboot,
   and readback preserve checksums. Guest TCP transfers to a fixture endpoint
   preserve payload hashes in both directions.
3. Console attach/detach and management commands work from WOS. Malformed
   config, inaccessible image, missing CPU feature, or RAM/quota shortage gives a
   precise failure with no partially running VM.
4. Virtio feature negotiation, device status, reset, descriptor lengths, indirect
   chains if supported, cycles, out-of-slot buffers, overflow, overlapping
   descriptors, and queue wrap are covered by executable tests and fuzz seeds.
   Unsupported features are never advertised.
5. Freeze descriptor metadata before asynchronous use; retain or copy referenced
   data safely. Concurrent guest mutation cannot redirect host memory access.
6. Pause stops every vCPU and defines in-flight device behavior; reset rejects
   late completions from the old queue generation. Destroy and VMM crash join
   workers before slot/pin release, including a blocked disk or full network queue.
7. A user-run 2-hour concurrent CPU/disk/network workload completes without host
   panic or corruption. Repeat 100 boot/shutdown cycles with no retained VM,
   queue, FD, or pinned-memory growth after warmup.
8. Report cold boot time, VM exit distribution, CPU throughput, disk throughput,
   network throughput, and host memory overhead on a fixed fixture. Measurements
   establish a baseline; do not claim KVM performance parity without evidence.

9. The same VMM source, shared ABI and machine definition pass criteria 1–8 on
   both vendors, including 100 boot cycles and the 2-hour stress run per vendor.
   Support is not established by a CPUID branch or a build-only AMD target.
10. Local `vmctl` workflows, manager restart/reattach, unauthorized requests,
    malformed control frames, dead clients and VMM crashes have executable and
    user-run evidence. Manager reconciliation never launches a duplicate VM.
11. Guest memory registration prevents stale userspace aliases after unmap/COW;
    the real run/completion ABI rejects wrong-vCPU and old-generation replies.
    Test multi-vCPU device access and read completion racing reset/slot teardown.
12. The guest reaches a configured endpoint over the actual Ethernet attachment,
    using stock virtio drivers. Show TX/RX frame and queue evidence; a fake guest
    socket adapter or host-only loopback test does not establish guest networking.
13. The installed library, VMM, manager, CLI, service entry and example definition
    operate together in an isolated WOS image. Publish an actual command transcript
    and document the guest boot/device compatibility matrix and unsupported modes.

## Boundaries, invariants, and rollback

No PC hardware zoo, arbitrary firmware boot, Windows guest promise, GPU/PCI
passthrough, nested guest hypervisor, or Linux `/dev/kvm` binary compatibility.
Kernel entry and device worker locks never surround blocking filesystem/network
operations. Retain ordinary WOS process limits and FD semantics. Shared machine
schemas and kernel changes belong to the primary integration agent.

Use the [OASIS virtio 1.2 specification](https://docs.oasis-open.org/virtio/virtio/v1.2/virtio-v1.2.html)
for the selected device model, explicitly recording its implemented subset.
Keep the VMM optional; rollback removes its exposure without altering existing
WOS virtio guest drivers or normal node packaging.

## Execution, evidence, and stop rules

Read `AGENTS.md`, the subsystem index, applicable kernel/WKI/critical-path/build
instructions, and the source entrypoints below before changing code. Source
wins over this dated plan. Make an impact map and the full repository patch
contract, including locks, unsafe contexts, ownership, ABI, validation, and
rollback, before each materially different critical patch.

Use a primary integration agent and up to three active subagents with bounded,
non-overlapping assignments; rotate assignments between checkpoints. The primary
owns shared headers, integration, and acceptance. Keep
`goals/progress/34/ledger.md` and `goals/progress/34/evidence.md`:
checkpoint, hypothesis, change, command, result,
artifact/build ID, remaining failure, next action. Preserve existing user edits.

Run Normal `Build WOS`; if the task is unavailable, mirror the current
`.vscode/tasks.json` command/environment. Register executable host tests with
the existing CMake test infrastructure and run their generated CTest entries.
Use `scripts/dev/format_repo.sh --check <changed-code-paths>`.
Prepare isolated diagnostics with `bin/wos-ktest --build-only`, retaining
`build-ktest/` and `ktest-data/` isolation. Read
`docs/kernel_debug_flags.md` before changing diagnostic flags. Never combine
report-mode KUBSan with the UBSan trap profile.

Kernel/cluster runtime execution and debugging are user-run under the current
instructions. Supply exact fixture-specific commands, expected observations,
and log locations early; analyze returned evidence while continuing independent
implementation and host validation. Runtime requirements in this file are mandatory,
not replaced by model tests, source-string tests, a successful build, or a demo
script that has not run.

Finish only when **all** acceptance criteria in this file have supporting evidence.
The one-to-two-week horizon is a scope estimate, not an enforced delay, token
budget, autonomy guarantee, or alternative finish line. Do not pad work or
silently shrink the contract. If a premise changes, record the discrepancy and
reconcile the contract with the actual tree before dependent edits. If an
indispensable blocker persists for three consecutive goal turns and no useful
authorized work remains, report the attempts, evidence, exact missing input,
and restart step, and mark the active goal blocked. Missing prerequisites or
hardware are never completion. Budget and pause controls remain user/system
controlled.
