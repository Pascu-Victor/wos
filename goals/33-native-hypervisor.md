# Goal 33: Native WOS Hardware Virtualization

## Copy-pastable goal command

```text
/goal Implement an opt-in native x86_64 WOS hypervisor with one versioned VM/vCPU/memory interface and fully supported Intel VMX/EPT and AMD SVM/NPT backends, interruptible vCPU scheduling, complete host-state restoration, and bounded teardown. Verify every acceptance criterion in goals/33-native-hypervisor.md on both vendors with executable tests, Normal Build WOS, isolated diagnostics, and user-run hardware guest traces. Preserve WOS ABI, scheduler, memory ownership, and unsupported-host boot. Work only within the listed virtualization integration surfaces, using checkpointed bounded subagents and evidence-led iterations. Follow the file's blocked rule when essential hardware or runtime evidence cannot be obtained.
```

## Outcome and scale

Deliver the kernel execution foundation for a KVM-like WOS platform: a WOS
process can create an isolated VM, supply guest RAM, create vCPUs, enter guest
execution, receive typed exits, interrupt execution, and destroy everything.
**Planning horizon: 168–336 continuous hours for the integration agent, with
bounded subagents active during independent work.** This includes architecture,
assembly, ABI, hostile-input validation, concurrency, and hardware verification.

**Intel VMX/EPT and AMD SVM/NPT are both required support targets**, with equal
functional acceptance. Implementing only one vendor does not complete the goal.
Require capability discovery and fail-closed admission; nested virtualization
exposed to guests, device passthrough, confidential guests (TDX/SEV), full Linux
KVM ioctl compatibility, and cross-node execution are excluded. An unsupported
CPU must still boot and run ordinary WOS. Missing hardware from either vendor
is a validation blocker, not permission to substitute a simulated backend and
declare completion. The original time horizon is an estimate; completing the
two-vendor contract takes priority over fitting its original schedule.

Read the [virtualization architecture contract](VIRTUALIZATION-ARCHITECTURE.md)
for the kernel/userspace split, proposed handle ABI and guest execution flow.

## Baseline and impact map

Investigated 2026-09-08. No VMX/SVM execution backend was found in the kernel
source scan. Existing KVM mentions concern the host QEMU launcher. Read:

- `modules/kern/src/platform/asm/cpu.hpp`, `cpu.cpp`, and `msr.hpp`:
  CPUID, control registers, per-CPU layout, XSAVE support.
- `modules/kern/src/platform/smt/smt.cpp`: `cpu_param_init()`;
  `platform/interrupt/gates.asm`, `platform/sys/context_switch.hpp`,
  `platform/sched/scheduler.hpp`, `scheduler.cpp`, and `task.hpp`.
- `modules/kern/src/platform/mm/virt.hpp`, `virt.opt.cpp`,
  `paging.hpp`, `phys.hpp`, and `platform/sched/resource.hpp`.
- `modules/kern/src/vfs/file.hpp`, `vfs/fs/devfs.cpp`,
  `platform/sys/syscall.cpp`, `abi/callnums.hpp`, and `modules/kern/CMakeLists.txt`.
- `tests/host/CMakeLists.txt`, `modules/kern/src/test/ktest.cpp`,
  and `scripts/cluster/node_setup.py`.

Proposed new areas: `platform/virt/` below the kernel source, an explicitly
versioned userspace ABI, and a minimal target test monitor. Names are proposals.

Flow: process handle → validated VM/vCPU object → retained guest memory slots →
EPT/NPT → VM entry → bounded exit classification → host restoration → userspace
exit record or scheduler handoff. Intel VMCS ownership and AMD VMCB/host-save/ASID
state have different CPU-local rules; migration must honor each. Runqueue,
task/FD, page-table, page-pin, and proposed
VM locks need an explicit order. Never enter arbitrary guest code while holding
a runqueue lock. Never wait for memory or I/O in the assembly exit trampoline.

## Common interface and vendor backends

Keep VM objects, handles, memory slots, vCPU state, CPUID policy, virtual LAPIC,
interrupt queues, charging, and normalized exits in common code. Proposed
`platform/virt/vmx/` and `platform/virt/svm/` implement a checked backend interface:
probe, enable/disable CPU, construct/destroy vCPU, load/put, enter, decode exit,
inject event, map/protect/unmap second-level pages, invalidate, and collect dirty
state. Dispatch is selected from the execution CPU's verified capabilities.

| Area | Intel backend | AMD backend |
| --- | --- | --- |
| Admission/setup | VMX capability MSRs, firmware permission, legal controls, VMXON regions | SVM/NPT CPUID features, firmware permission, EFER.SVME, host-save area |
| Execution state | VMCS lifecycle, VM entry/resume, VM instruction failure | VMCB lifecycle, VMRUN and required host/guest save/restore, invalid-state exits |
| Interception | Execution controls plus I/O/MSR bitmaps | Intercept controls plus I/O/MSR permission maps |
| Memory | EPT permissions/faults and architecture-correct invalidation; VPID if enabled | Nested page tables/faults and architecture-correct ASID/TLB invalidation |
| Events | Interrupt/NMI windows, pending injection, exit reinjection | Virtual interrupt controls, GIF-related host restoration, pending injection/reinjection |
| Later state transfer | Architectural state exported without raw VMCS | Architectural state exported without raw VMCB/host-save data |

These are work items, not interchangeable register layouts. Audit capability
requirements against each vendor manual. The supported machine needs real,
protected and long-mode guest execution, including secondary CPU startup;
Intel admission must require unrestricted-guest support for this baseline.
Hardware dirty bits, APIC acceleration and timer scaling are optional only when
a tested common fallback supplies the promised semantics. Never assume an Intel
preemption timer exists on AMD: host timer/IPI kicks must bound guest runtime on
both. Do not execute instructions from the wrong backend while probing.

The userspace API exposes architectural state and normalized exits, with vendor
details confined to versioned diagnostics/capability data. The same VMM source
and machine definition must work on both hosts. Declare the supported x86 ISA
baseline for its binaries; current userspace `-march=native` build settings must
not accidentally require build-host-only features on the other vendor.

## Checkpoints and parallel ownership

1. Days 1–3: primary fixes the common backend/ABI contract; subagents own Intel
   backend reconnaissance, AMD backend reconnaissance, and common guest fixtures.
2. Days 3–7: backend subagents implement independently beneath that interface;
   primary integrates VM lifecycle, slots and common exits; a third subagent
   tests state validation and architectural guest parity. Both vendors must
   reach the freestanding guest milestone.
3. Days 7–10: add multiple vCPUs, interrupt injection, timer/kick semantics,
   resource charging, slot replacement exclusion, and kill/close races.
4. Days 10–14 and longer if required: replay negative-state and stress tests on
   user-run Intel and AMD hardware; rotate an independent subagent to audit each
   backend's save/restore and unwind paths. Preserve both acceptance gates.

## Acceptance criteria

1. Document supported CPU controls, CPUID/MSR exposure, VM/vCPU handle ownership,
   bounds, version negotiation, errno, run/exit ABI, and unknown-field rejection.
2. At least two VMs with at least two vCPUs each execute deterministic guest
   payloads concurrently; guest pages and register state remain disjoint.
3. Tests cover general/segment/control/debug registers, FS/GS, XSAVE/XCR0,
   exceptions, external interrupts, HLT wakeup, interrupt windows, and invalid
   entry state. Host sentinel workloads detect state leakage after exits.
4. Timer preemption and explicit kicks stop a guest spinning with interrupts
   disabled; cancellation has a finite documented deadline measured by the
   hardware fixture. No hung guest can monopolize a host CPU indefinitely.
5. Memory slot admission rejects overlap, overflow, unmapped or forbidden host
   ranges, wrong permissions, and stale handles. EPT/NPT changes invalidate cached
   translations before the old backing can be freed.
6. VM memory is explicitly charged and pinned while reachable by hardware.
   Reclaim, COW, unmap, process death, partial construction, and concurrent close
   cannot release or replace live backing. No host/kernel mappings are exposed.
7. Across 1,000 create/run/kill/destroy cycles, tracked VM objects, pins, and
   physical owners return to the warmed baseline. Test forced allocation failure
   at each construction stage and close while a vCPU is running.
8. Normal WOS workloads and unsupported-host boot pass. Required hardware traces
   identify CPU capability masks, build IDs, test payload hashes, and outcomes.
9. Criteria 1–8 pass independently on an Intel VMX/EPT host and an AMD SVM/NPT
   host with the same common API tests and payload hashes. Firmware-disabled,
   missing second-level paging, incompatible XSAVE, invalid controls and
   unsupported acceleration paths reject or take a documented tested fallback.
10. Vendor-specific tests cover CPU migration/load-put, TLB tag reuse, nested
    translation faults, invalid entry, NMI/reinjection and host state restoration.
    A compile-only backend or nested-emulator run cannot substitute for required
    vendor hardware evidence. Record bare-metal versus nested execution clearly.
11. The minimal userspace test monitor uses the real public handle/run interface
    to create memory slots, enter both backends, service PIO/MMIO exits, inject
    interrupts and close a running VM; an internal kernel-only test is insufficient.

## Verification and rollback

Start with the [local manual index](../docs/virtualization-manuals.md) and its
source manifest. Use the pinned Intel SDM revision 092 and AMD APM Volume 2
revision 3.45 before implementing architectural controls. Search results identify
candidate pages, not a substitute for verifying original tables and diagrams.

Use the [KVM API](https://www.kernel.org/doc/html/latest/virt/kvm/api.html) as a
reference for VM/vCPU/memory capability separation, not an ABI compatibility
promise. Consult the [Intel manuals](https://www.intel.com/content/www/us/en/developer/articles/technical/intel-sdm.html)
and AMD's Architecture Programmer's Manual Volume 2, listed in the
[AMD documentation catalog](https://www.amd.com/en/developer/sev.html), for each
backend's control legality and state transitions; record the revisions used.
Host tests prove parsers/state models, while actual VM entry requires runtime
evidence. Keep the feature opt-in and preserve an off build/profile for rollback.
Coordinate Task/VM work with ongoing goals 19–22 and future goal 40.

## Execution, evidence, and stop rules

Read `AGENTS.md`, the subsystem index, applicable kernel/WKI/critical-path/build
instructions, and the source entrypoints below before changing code. Source
wins over this dated plan. Make an impact map and the full repository patch
contract, including locks, unsafe contexts, ownership, ABI, validation, and
rollback, before each materially different critical patch.

Use a primary integration agent and up to three active subagents with bounded,
non-overlapping assignments; rotate assignments between checkpoints. The primary
owns shared headers, integration, and acceptance. Keep
`goals/progress/33/ledger.md` and `goals/progress/33/evidence.md`:
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
