# WOS virtualization architecture and userspace contract

This is the proposed implementation contract for [goal 33](33-native-hypervisor.md),
[goal 34](34-linux-guest-virtual-machine-monitor.md) and
[goal 35](35-wki-distributed-virtualization.md), with extension points for
[checkpoints/migration](36-wki-vm-checkpoint-migration.md),
[replicated disks](37-wki-replicated-vm-volumes.md) and
[remote RAM](38-wki-disaggregated-guest-memory.md).
It expands the 2026-09-08 investigation following the user's requirement for
**both Intel and AMD**. Module names, device names, operations and CLI examples
below are proposed deliverables, not commands or facilities already in WOS.
The numbered goals retain their individual acceptance and stop rules.

Read the [local manual corpus guide](../docs/virtualization-manuals.md) before
fetching architecture references again. Intel's complete SDM, AMD APM Volume 2
and the KVM, Linux boot and virtio references are locally searchable under
`reference/virtualization/`. The manifest records hashes and source availability;
AMD Volume 2 revision 3.45 was imported from the user's saved PDF. PDF text uses Docling with
physical-page provenance; verify tables and diagrams in the original document.

## What actually executes the guest

Build a WOS-native hardware VMM. WOS runs on an Intel VMX/EPT or AMD SVM/NPT
host; a WOS userspace process manages each VM. Its vCPU threads enter the WOS
kernel, which loads that vCPU's architectural state and enters hardware guest
mode. Most guest instructions execute on the physical processor. Intercepted
operations return to the kernel; device accesses that require emulation return
a normalized exit to the VMM. This is the selected design, not a QEMU launch
wrapper or a full software CPU interpreter.

The guest runs its own kernel, page tables, scheduler, drivers and applications.
Guest system calls normally remain inside the guest OS. Address translation is
guest virtual → guest physical through guest page tables → retained host
physical RAM through EPT or NPT. Guest privilege never becomes WOS host privilege.

Intel and AMD implement the same WOS-visible VM operations. Vendor-specific
VMCS/VMCB state stays inside the kernel and is never the userspace machine
description or a portable snapshot. The
[KVM API](https://www.kernel.org/doc/html/latest/virt/kvm/api.html) is a reference
for the separation of VM/vCPU control from a userspace monitor. WOS defines its
own versioned ABI; Linux `/dev/kvm`, libvirt and QEMU compatibility are not
implied.

## Deliverables and ownership

| Component | Proposed location | Owns |
| --- | --- | --- |
| Common kernel hypervisor | `modules/kern/src/platform/virt/` | VM/vCPU objects, slot/pin ownership, architectural state, event injection, virtual LAPIC, scheduling/kicks, accounting |
| Intel backend | `platform/virt/vmx/` under kernel source | VMX setup, VMCS entry/exit, EPT, vendor controls and invalidation |
| AMD backend | `platform/virt/svm/` under kernel source | SVM setup, VMCB entry/exit, host-save handling, NPT, ASIDs and vendor controls |
| `libwosvirt` | `shared/virtualization/` plus module packaging | Typed ABI wrappers, versioned config/control codecs, handle ownership, checked guest-memory helpers |
| `wos-vmm` | `modules/vmm/` | One VM's RAM mappings, vCPU threads, Linux loader, device models, event loop, bounded I/O workers, console and device snapshots |
| `vmmd` | `modules/vmmd/` | Node inventory, owner authorization, capability/admission checks, one-VMM-per-VM supervision, durable definitions and reconciliation |
| `vmctl` | `modules/vmctl/` | Validate/define/start/list/inspect/console/pause/resume/stop/destroy; later checkpoint/migrate/volume commands |
| WKI VM service bridge | kernel WKI service plus scoped userspace endpoint | Authenticated remote envelopes, bounded queues, exact request/session identity and communication with the local manager |
| VM network endpoint | scoped kernel network device API | Authorized guest Ethernet frame ingress/egress and bounded pollable queues; no host-memory access |

Goal 33 supplies the kernel interface, backend parity and a minimal monitor.
Goal 34 supplies `libwosvirt`, `wos-vmm`, local `vmmd`/`vmctl`, network
endpoint and packaging. Goal 35 adds distributed control and remote resource
adapters. Goal 36 adds coordinated kernel-state and userspace-device snapshots.
The shared library's interfaces are separated from the single writer of the
kernel ABI so subagents can work without concurrently changing shared headers.

Run one VMM process per VM, with one host thread per vCPU, one management/device
event loop and a bounded pool of blocking I/O workers. `vmmd` runs as a
supervised WOS service and communicates through a local authenticated control
endpoint. It retains VM identity independently of terminal/CLI lifetimes.
The per-VM process has only the authority, file/device handles and quotas needed
for its assigned VM. VM control permission does not grant arbitrary disk, NIC,
other-VM or kernel-memory access.

Current [module build rules](../modules/CMakeLists.txt) apply
`-march=native` to userspace. These new targets and their runtime dependencies
need an explicit supported ISA build policy and load tests on both vendors;
a successful build on one host says nothing about deployment on the other.
Add target lists, library/runtime installation, rootfs aliases, service config
and isolated image packaging together. Do not depend on Linux host libraries.

## Kernel/userspace ABI

Propose an authorized `/dev/wosvirt` control endpoint, VM handles and vCPU
handles implemented through WOS device ioctls and typed file objects. Names
such as `VM_CREATE` and `VCPU_RUN` below describe operation families; allocate
actual constants and struct layouts only in the implementation patch contract.

| Operation family | Contract |
| --- | --- |
| Query/create | ABI version, capabilities, supported limits and machine requirements; create an empty VM with assigned owner and quotas |
| Memory slots | Register validated aligned userspace RAM against guest-physical ranges; reject overlaps/holes/overflow; charge and retain backing until all hardware and worker references drain |
| vCPU state | Create vCPU, set/get checked architectural registers, CPUID policy and XSAVE state; CPU controls never accept raw guest-provided host values |
| Run/complete | Enter execution; return a bounded typed exit with vCPU/run generation; accept only a matching pending completion before resuming |
| Events | Kick a running vCPU, inject validated virtual IRQ/NMI, change interrupt-line levels and inspect pending state |
| Lifecycle | Quiesce all vCPUs, reset generations, cancel pending work and join execution before releasing pins/handles |
| Later migration/RAM | Dirty tracking and consistent state capture; explicit page resolve/invalidate interfaces for remotely backed slots |

Start with checked copy-in/copy-out run and completion records. A mapped run
page is optional and requires a separately implemented mapping/lifetime contract:
[FileOperations](../modules/kern/src/vfs/file_operations.hpp) has ioctl and poll
hooks but no generic device-mmap callback. Do not assume Linux-style vCPU mmap
already works. Likewise, integrate the actual WOS devfs and syscall dispatch
paths rather than merely defining unused ioctl numbers.

Anonymous userspace RAM registration needs a lifecycle barrier: pinning a page
alone does not prevent userspace from remapping the slot's virtual address.
Prevent conflicting unmap/mprotect/COW replacement while registered, or make
replacement synchronously quiesce and update all hardware and VMM references.
Helpers use validated slot offsets and retained generations, not unchecked
addresses supplied by the guest. Remote-backed slots in goal 38 replace that
all-resident assumption with explicit residency-aware access.

Handles are owner-process-bound, created close-on-exec, with explicit dup/fork
rules. A foreign inherited descriptor must not permit running or modifying the
VM; it can be closed safely. Owner death/exec revokes admission and stops its
vCPUs, while remaining references only retain objects needed for safe teardown.
Do not permit stale handles to pin a vanished address space indefinitely.

## Running and completing exits

```text
vCPU host thread:
  submit completion for the previous exit, if any
  VCPU_RUN -> WOS saves host state -> Intel entry or AMD VMRUN
    guest executes until hardware exit or host kick
  WOS restores host state and handles kernel-owned exits
  if userspace work is required:
    publish normalized exit with VM/vCPU/run generation
    dispatch to device model or queue bounded asynchronous work
    record completion once; re-enter only when valid
```

The kernel owns CPU-state validation, guest exceptions, second-level translation,
host scheduling, virtual LAPIC state and interrupt injection. The VMM owns
serial, PCI and other device register models. CPUID/MSR policy is configured
through validated common interfaces, not arbitrary userspace register access.

Normalize PIO and MMIO with address/port, direction, width, bounded data and
completion semantics. A common checked x86 decoder must cover the instructions
used by the supported guest/device model, including page-boundary fetch and
string/repeated I/O where supported. Intel and AMD exit metadata differ; do not
assume either provides a ready-to-use MMIO value or identical next-RIP behavior.
Separate decoded completed operations from operations requiring emulation;
advance architectural state exactly once and deliver real guest faults.
Unsupported accesses produce a defined guest-visible fault or VM error rather
than guessed success.

HLT parks the vCPU until a virtual event or management kick. Pause/kick interrupts
a spinning guest even when its IF is clear. Host interrupt/NMI restoration and
scheduler fairness remain kernel responsibilities on both vendors. Userspace
never polls by repeatedly running a halted VM.

## Constructing the reference Linux machine

The mandatory first guest is an unmodified pinned x86_64 Linux kernel with
pinned initramfs/root disk and a configuration supporting the chosen devices.
The VMM implements a versioned `wos-pc-v1` machine definition; guest-visible
hardware and addresses are frozen in an executable machine-layout fixture.

Boot through the documented Linux 64-bit direct-entry protocol: validate image
capabilities and sizes, allocate non-overlapping kernel/initramfs/command-line
regions, build `boot_params` and the memory map, construct startup page tables
and GDT, and set entry registers/control state. The exact entry address and
requirements come from the pinned kernel's protocol, not guesses from its ELF
headers. Reference:
[Linux x86 boot protocol](https://docs.kernel.org/arch/x86/boot.html).

Supply ACPI tables and PCI configuration describing the virtual machine rather
than copying host tables. The machine includes local APICs, an I/O APIC,
minimal legacy PIC/PIT/RTC behavior needed by the chosen Linux profile, a serial
UART, virtual reset/power control, and modern virtio PCI block/network devices.
The primary agent must freeze addresses, routing, EOI handling, timer source,
ACPI power registers and required table set before guest integration.

The kernel models local APIC and INIT/SIPI state; userspace owns I/O APIC,
legacy device/timer register models and routing configuration. Device line and
EOI APIs must preserve level-triggered interrupts across mask/unmask, pause and
snapshot. Secondary vCPUs boot through guest INIT/SIPI and mode transitions;
prestarting all CPUs at the bootstrap kernel entry is not SMP boot support.

Firmware and a BIOS/UEFI boot manager are unnecessary for this direct Linux
boot path. Arbitrary ISO boot, an unmodified WOS Limine/UEFI guest, and Windows
installation require additional firmware/machine/driver work and are not
claimed by goal 34. The machine/loader boundary should allow a later firmware
loader without moving device emulation into the kernel.

## How guest devices reach real storage and networks

Use the selected subset of the
[virtio 1.2 device contract](https://docs.oasis-open.org/virtio/virtio/v1.2/virtio-v1.2.html):
modern PCI discovery, feature negotiation, status/reset, split queues,
notifications and interrupt delivery. Begin with a small explicit feature set;
only advertise indirect descriptors, event index, offloads or multiqueue when
their full behavior is implemented and tested.

For virtio block, the guest driver places a request in guest RAM and notifies
the device. A userspace model snapshots the bounded descriptor chain, checks
direction/ranges, retains slot access, and submits positional read/write/flush
to the authorized backing handle. Completion copies read data and status back,
publishes the used ring with the required ordering, and raises the virtual
interrupt. Reset or VM death cancels the old generation; a late disk callback
cannot modify a newly reused queue or slot.

Initially use raw image files or explicitly authorized block handles. Read-only,
capacity, alignment, short I/O and flush behavior must reach the guest correctly.
An image filename is not permission to open arbitrary host paths. In goal 35,
a remote disk uses a scoped WKI proxy/adapter exposing the same semantics;
goal 37 can supply replicated backing without changing the guest device model.

Virtio network is layer-2 Ethernet, so a normal host TCP socket cannot substitute
for its packet interface. The current
[devfs network nodes](../modules/kern/src/vfs/fs/devfs.cpp) expose statistics
with no write handler; they are not TAP devices. Goal 34 must implement an
authorized pollable packet endpoint with bounded RX/TX frames, MTU checks,
MAC/link policy, queue credits and close/teardown semantics. Define a bounded
virtual Ethernet attachment and explicit forwarding policy to a configured
physical or virtual uplink; verify bidirectional guest traffic. NAT, DHCP and a
general virtual switch are not silently assumed. A static-address fixture is
acceptable. WKI transport NICs remain excluded from guest attachment.

Userspace copies or safely retains guest buffers while transmitting, and copies
received Ethernet frames into posted guest buffers before signaling completion.
In goal 35 the endpoint may bind an authorized remote NIC resource; the guest
still sees the same virtual NIC. All guest DMA is emulated memory access in
these goals; no guest programs real device DMA or host interrupt controllers.

## Local and WKI management flow

Goal 34 implements local definitions and supervision first. A definition contains
schema/machine version, owner, vCPU count, CPU feature profile, RAM size, image
identities/hashes, console policy, explicit disk/network attachments and quotas.
Validate and preflight it before publishing a VM. A process exit becomes a
terminal state with an inspectable reason; manager restart reconciles live VMM
identities instead of blindly launching duplicate processes. Automatic restart
after executor loss is outside the initial contract.

Proposed goal-34 workflow after installation:

```sh
vmctl validate ./linux.json
vmctl define ./linux.json
vmctl start linux
vmctl console linux
vmctl inspect linux
vmctl pause linux
vmctl resume linux
vmctl stop linux
vmctl destroy linux
```

`stop` attempts the supported guest shutdown and exposes an explicit force
option/deadline. `destroy` removes VM runtime/definition state; deleting backing
images requires a separate explicit operation. These are proposed CLI semantics
whose help, parser and end-to-end tests must match the delivered commands.

For goal 35, extend the same API with placement and remote resource references:

```text
vmctl on A -> local vmmd -> typed WKI request bridge
  -> authenticated WKI session -> bridge on B -> executor vmmd
  -> authorized per-VM wos-vmm on B -> local Intel or AMD kernel backend
  -> optional disk/network backing through WKI to C
```

There is no assumed raw-WKI userspace socket. Add a narrow service endpoint:
kernel RX admits bounded typed envelopes tagged with authenticated origin,
permission scope, request ID and session/channel generation; `vmmd` reads them
from a pollable queue and replies using an opaque validated request token.
Userspace cannot forge source identity or submit arbitrary privileged WKI
frames. Apply kernel admission and manager object-level authorization.

Keep reliable transport ACK distinct from VM operation acceptance/commit.
Queue exhaustion is retry/backpressure before a false operation success.
Manager death, request cancellation and session rotation invalidate exact tokens.
Remote VM start is not ordinary `TASK_SUBMIT`; VM ownership survives controller
disconnect according to goal 35's contract.

At steady state, guest instructions and ordinary VM exits stay on B. WKI carries
management, remote resource I/O, console streams and later migration/page traffic.
Do not send every CPU instruction or exit across the network. An Intel or AMD
controller can manage either vendor's executor; admission follows executor CPU
capabilities, not the controller's CPU.

## Later state and memory extensions

Goal 36 quiesces both kernel vCPUs and userspace devices. Capture CPU state plus
queue generations, UART buffers, virtual interrupt/timer state, clock offsets,
disk-generation identity and defined in-flight I/O disposition. A register dump
alone is not a VM snapshot. Rebuild vendor hardware state on restore.
Require Intel→compatible Intel and AMD→compatible AMD migration tests;
Intel↔AMD live migration is initially rejected before cutover. Reusing a Linux
image on either host and restarting it from an ordinary disk are separate from
transferring live CPU state.

Goal 38 adds remote guest page providers behind common EPT/NPT memory operations.
The VMM's device helpers must use residency-aware reads/writes rather than assume
all registered RAM is mapped and resident. A fault parks its vCPU while a bounded
worker fetches data; a device access may likewise wait outside hot locks.
Both vendors need identical page-version, dirty tracking and invalidation
semantics, including writes from emulated devices.

## Evidence required

The native monitor must test the actual ABI on Intel and AMD. The Linux machine,
local manager workflow, SMP, disk/network and stress suites must pass on both
with the same guest artifact hashes. WKI tests cover both executor vendors and
mixed-vendor controller/resource nodes. Migration and remote RAM have their
additional vendor-specific acceptance gates in goals 36 and 38.

Document proposed versus implemented operations as development proceeds.
Publish actual commands, CPU capabilities, build/guest hashes, logs and failures.
Host parser/model tests are necessary but cannot stand in for real guest boot or
device completion. Runtime execution remains user-run under the repository
instructions. The mandatory two-vendor result must not be waived to match a
planning duration.

## Architecture references and retrieval limits

Use the [Intel architecture manuals](https://www.intel.com/content/www/us/en/developer/articles/technical/intel-sdm.html)
and AMD Architecture Programmer's Manual Volume 2, listed by the
[AMD publisher](https://www.amd.com/en/developer/sev.html), when implementing the
backend contract. Both manuals are now pinned in the local corpus: Intel SDM
revision 092 and AMD APM Volume 2 revision 3.45 (July 2026). The earlier AMD
automated download returned HTML; the user's saved PDF was subsequently imported
and indexed with Docling across all 875 pages. Acquisition and indexing do not
constitute a detailed architectural audit. Validate vendor control bits and
architectural corner cases against these pinned editions during implementation.
