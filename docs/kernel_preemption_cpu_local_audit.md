# PREEMPT_NOBLOCK CPU-local lifetime audit

This ledger records the source audit that qualifies default ordinary
process-kernel preemption. It covers direct CPU-ID reads, current-task lookups,
`PerCpuCrossAccess::this_cpu()` pointers, GS-base operations, and assembly GS
accesses. The host audit test fails when a new source file uses one of those
surfaces without being added here.

The classifications have precise meanings:

- **re-read**: the CPU value is consumed immediately for attribution or a
  single operation. Code needing it again performs a new read after calls.
- **migration-protected**: a `MigrationGuard`, scheduler CPU pin, or immutable
  boot CPU assignment keeps the CPU stable across the lifetime.
- **preemption-protected**: IRQ masking, preempt-disable, a spinlock, or the
  scheduler handoff boundary prevents a switch across the lifetime.
- **refactored**: `get_current_task()` yields the executing task's identity,
  which remains valid when that task resumes; the pointer is not retained as a
  proxy for a runqueue or CPU ID.

## Guarded-stack qualification inventory reconciliation

These entries classify existing call sites discovered during the epoch-progress
qualification. They describe CPU-local lifetimes; they do not replace each
subsystem's object ownership, shutdown or reservation tests.

- `modules/kern/src/dev/e1000e/e1000e.cpp` — **re-read / refactored**. Live-test CPU values are immediate scalar attribution; current-task pointers identify the NAPI worker or inspect the executing holder's shutdown flag after sleep.
- `modules/kern/src/dev/virtio/virtio_net.cpp` — **re-read / refactored**. Live-test receive/sender CPU samples are scalar attribution. Worker comparisons and holder shutdown checks use task identity, without retaining a runqueue or CPU-local pointer.
- `modules/kern/src/dev/ivshmem/selftest.cpp` — **refactored**. The ownership holder re-reads its executing task after sleep to inspect shutdown state.
- `modules/kern/src/dev/usb/xhci.cpp` — **refactored**. The held-publication test re-reads its executing task after each yield for shutdown accounting.
- `modules/kern/src/net/wki/dev_server.cpp` — **refactored**. Metadata-batch handling retains the executing task as the explicit VFS credential/namespace owner across operations; it does not retain CPU-local state.
- `modules/kern/src/net/wki/peer.cpp` — **refactored**. Lifecycle writer ownership is published and compared as task identity after acquisition; yielding does not reinterpret the owner as a CPU.
- `modules/kern/src/platform/dbg/coredump.cpp` — **refactored**. Output opening receives the executing task's identity, rather than a CPU-local pointer.
- `modules/kern/src/platform/sched/file_growth.cpp` — **refactored**. The caller identity supplies resource policy and signal delivery; process resource state is accessed under its resource lock through `Owner`.
- `modules/kern/src/platform/sched/resource.cpp` — **refactored**. Resource syscalls retain the executing caller identity for usercopy and policy; canonical process state is accessed through `Owner` and existing locks.
- `modules/kern/src/platform/sched/resource_process.cpp` — **refactored / preemption-protected**. Reservation executor identity is linked and unlinked under the population IRQ-save lock. It is not a retained runqueue pointer.
- `modules/kern/src/platform/sched/service_resource.cpp` — **refactored**. Service syscalls pass executing task identity to policy/usercopy helpers and use the service lock for shared state.
- `modules/kern/src/test/credential_ktest.cpp` — **refactored**. The bounded selftest checks whether an ambient task exists to distinguish boot ESRCH from backend EROFS; no CPU-local pointer is retained.
- `modules/kern/src/vfs/fd_reservation.cpp` — **refactored / preemption-protected**. The executing task is a reservation-cleanup identity. Table slots and executor links are changed under the canonical table's IRQ-save lock, with the existing retained table reference.

## Mixed scheduler, return, and epoch paths

- `modules/kern/src/platform/sched/scheduler.cpp` — **migration-protected / preemption-protected**. Epoch read-side regions now begin inside `MigrationGuard`; raw current-runqueue pointers are confined to IRQ-off, per-CPU-lock, idle, or handoff code. Cross-CPU publication consumes the packed migration owner.
- `modules/kern/src/platform/sched/scheduler.hpp` — **preemption-protected / refactored**. Inline wait helpers retain only the current `Task*`; halt and runqueue boundaries mask interrupts, and guard APIs do not expose a CPU-local pointer.
- `modules/kern/src/platform/sched/epoch.cpp` — **migration-protected**. Returning `EpochGuard` call sites hold `MigrationGuard`; manual non-returning epoch ownership is confined to IRQ-disabled scheduler handoff paths.
- `modules/kern/src/syscalls_impl/process/waitpid.cpp` — **refactored**. The waiter remains a task identity across park/resume, and the child-event API pins the canonical owner. CPU observations are diagnostic scalars sampled before begin and terminal telemetry; no CPU-local pointer or epoch lifetime is retained.
- `modules/kern/src/platform/sys/context_switch.cpp` — **preemption-protected**. CPU/GS/TSS values are read and installed only inside interrupt, syscall-handoff, or context-switch boundaries where switching is already excluded.
- `modules/kern/src/platform/sys/context_switch.asm` — **preemption-protected**. GS changes and scratch-frame restoration are within the assembly handoff before return-boundary ownership commits.
- `modules/kern/src/platform/sys/syscall.cpp` — **preemption-protected**. GS diagnostics execute on syscall entry/exit or fatal paths; ordinary return repair does not retain a CPU ID across a preemptible call.
- `modules/kern/src/platform/sys/syscall.asm` — **preemption-protected**. `swapgs`, GS scratch fields, and stack selection are entry/exit state-machine operations, not general preemptible C++ lifetimes.
- `modules/kern/src/platform/interrupt/gates.cpp` — **preemption-protected**. CPU IDs are captured in interrupt/exception context; panic epoch ownership runs after interrupts are disabled and peer CPUs are halted.
- `modules/kern/src/platform/interrupt/gates.asm` — **preemption-protected**. Same-CPL normalization and `swapgs` execute entirely inside interrupt entry/return.
- `modules/kern/src/sanitizer/kcov_trace_pc.asm` — **preemption-protected**. GS coverage-buffer access is an interrupt-safe leaf instrumentation path and does not retain a pointer across a call.

## Per-CPU storage and allocator paths

- `modules/kern/src/platform/smt/smt.cpp` — **migration-protected / preemption-protected**. CPU-local objects are initialized on immutable boot CPUs; runtime cross-access uses per-CPU locks.
- `modules/kern/src/platform/smt/smt.hpp` — **preemption-protected**. Locked accessors mask interrupts. Unlocked `this_cpu()` is restricted to the scheduler's audited IRQ-off/idle paths; its contract now forbids retention across a preemptible call without `MigrationGuard`.
- `modules/kern/src/platform/asm/cpu.cpp` — **re-read**. The safe CPU-ID helper returns a scalar snapshot and exposes no CPU-local pointer.
- `modules/kern/src/platform/asm/cpu.hpp` — **re-read / preemption-protected**. GS leaf helpers return scalar state; direct base writes are used only by audited boot and handoff code.
- `modules/kern/src/platform/asm/segment.hpp` — **preemption-protected**. Raw GS-base leaf operations are limited to audited context setup and switch paths.
- `modules/kern/src/platform/asm/msr.hpp` — **preemption-protected**. The GS MSR identifiers are scalar constants consumed only by audited boot, exec-commit, and handoff paths.
- `modules/kern/src/platform/mm/dyn/kmalloc.opt.cpp` — **preemption-protected**. Magazine CPU selection and access occur with interrupts disabled; the slow path releases the CPU-local reference first.
- `modules/kern/src/platform/mm/phys.opt.cpp` — **preemption-protected**. Per-CPU page caches are selected before taking IRQ-save cache locks; references do not escape unlock. Statistics are atomic attribution counters.
- `modules/kern/src/platform/mm/virt.opt.cpp` — **preemption-protected**. TLB gate ownership pairs IRQ masking with preempt-disable; IPI service and active-pagemap slots are interrupt or scalar operations.
- `modules/kern/src/mod/io/serial/serial.cpp` — **preemption-protected**. Normal serial ownership preempt-disables the owning task; panic/early-boot CPU ownership cannot migrate.
- `modules/kern/src/platform/sys/mutex.cpp` — **preemption-protected / refactored**. Lock ownership retains a task identity, and spinlock/preemption state excludes unsafe owner transitions.

## Pinned CPU workers and topology

- `modules/kern/src/net/backlog.cpp` — **migration-protected**. Each handler caches its queue index only after `post_task_pinned_cpu()` permanently binds it to that CPU; inline producers use immediate scalar CPU reads.
- `modules/kern/src/net/netpoll.cpp` — **migration-protected / re-read**. IRQ-affine NAPI workers are posted pinned; watchdog/rescue comparisons re-read the current CPU and retain task identity rather than a CPU-local pointer.
- `modules/kern/src/test/mm_ktest.cpp` — **migration-protected / re-read**. The KTEST CPU value selects a diagnostic snapshot on the test CPU and is not retained across scheduler work.
- `modules/kern/src/test/vfs_ktest.cpp` — **refactored**. Current-task use is task identity inside a bounded selftest, not CPU-local ownership.

## Immediate CPU snapshots

- `modules/kern/src/net/packet.cpp` — **re-read**. CPU is sampled only for packet diagnostics.
- `modules/kern/src/net/loopback.cpp` — **re-read**. The scalar CPU chooses one enqueue operation; later work owns its explicit queue index.
- `modules/kern/src/net/wki/event.cpp` — **re-read**. CPU is immediate performance-event attribution.
- `modules/kern/src/net/wki/dev_proxy.cpp` — **re-read / refactored**. CPU values are immediate diagnostics; current task pointers are retained as task identities under existing object locks/references.
- `modules/kern/src/net/wki/chaos_workload.cpp` — **re-read**. The bounded test-only worker checks the current task's daemon type immediately and does not retain CPU-local state across a migration point.
- `modules/kern/src/net/wki/wki.cpp` — **re-read**. CPU is immediate WKI performance attribution.
- `modules/kern/src/platform/dbg/dbg.cpp` — **preemption-protected**. CPU read occurs only in the panic writer after normal scheduling has stopped.
- `modules/kern/src/platform/dbg/journal.cpp` — **re-read**. Record creation samples CPU into record metadata; no CPU-local pointer is retained.
- `modules/kern/src/platform/loader/elf_loader.cpp` — **re-read**. CPU is immediate local-loader performance attribution.
- `modules/kern/src/platform/sched/resource_as.cpp` — **re-read**. Address-space observation samples the current CPU for immediate performance-event attribution after observation; it retains no CPU-local pointer or CPU selection across calls.
- `modules/kern/src/syscalls_impl/multiproc/threadInfo.cpp` — **re-read**. The syscall returns a scalar CPU observation.
- `modules/kern/src/syscalls_impl/process/exec.cpp` — **re-read / refactored**. CPU is immediate event attribution; GS-base writes are at the non-preemptible image-commit return boundary.
- `modules/kern/src/syscalls_impl/process/exit.cpp` — **re-read / refactored**. CPU is immediate exit attribution; task identity is retained through exit ownership, not as CPU-local state.
- `modules/kern/src/syscalls_impl/process/process.cpp` — **re-read / refactored**. Child scratch CPU fields are construction hints before publication and are overwritten by `install_task_cpu_bases()` on execution; parent/current pointers are task identities.
- `modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp` — **re-read / refactored**. CPU is immediate VMEM event attribution and current-task use is task identity.
- `modules/kern/src/platform/sched/task.cpp` — **re-read**. Construction-time CPU/scratch values precede publication and switch code rewrites them on every installed CPU.

## Current-task identity without a CPU-local lifetime

Each file below uses `get_current_task()` as the executing task's stable identity.
The task cannot be reclaimed or become a different task while its kernel call
resumes, and none of these sites retains a runqueue pointer or treats a cached
`task->cpu` as the current CPU across a preemptible call.

- `modules/kern/src/dev/ahci.cpp` — **refactored**.
- `modules/kern/src/dev/console.cpp` — **refactored**.
- `modules/kern/src/dev/pty.cpp` — **refactored**.
- `modules/kern/src/net/proto/tcp.cpp` — **refactored**.
- `modules/kern/src/net/socket.cpp` — **refactored**.
- `modules/kern/src/net/wki/remote_compute.cpp` — **refactored**.
- `modules/kern/src/net/wki/remote_ipc.cpp` — **refactored**.
- `modules/kern/src/net/wki/remote_ipc_socket.cpp` — **refactored**.
- `modules/kern/src/net/wki/remote_vfs.cpp` — **refactored**.
- `modules/kern/src/platform/debug/ptrace.cpp` — **refactored / preemption-protected**. Target scratch pointers are target-owned storage; direct GS reads are bounded syscall snapshots.
- `modules/kern/src/platform/mm/reclaim.cpp` — **refactored**. The current task is converted to a stable identity token solely to distinguish recursive reclaim from cross-task lease contention; it is never used as a CPU or runqueue proxy.
- `modules/kern/src/platform/power/power.cpp` — **refactored**.
- `modules/kern/src/platform/sys/signal.cpp` — **refactored / preemption-protected**. Task scratch storage is task-owned; GS-sensitive return work occurs at the validated return boundary.
- `modules/kern/src/syscalls_impl/futex/futex.cpp` — **refactored**.
- `modules/kern/src/syscalls_impl/log/sys_log.cpp` — **refactored**.
- `modules/kern/src/syscalls_impl/multiproc/threadControl.cpp` — **refactored**.
- `modules/kern/src/syscalls_impl/net/sys_net.cpp` — **refactored**.
- `modules/kern/src/syscalls_impl/process/init_control.cpp` — **refactored**. Current-task use is stable caller identity for PID/euid authorization and usercopy; no CPU-local pointer is retained.
- `modules/kern/src/syscalls_impl/process/getpid.cpp` — **refactored**.
- `modules/kern/src/syscalls_impl/process/getppid.cpp` — **refactored**.
- `modules/kern/src/syscalls_impl/shm/shm.cpp` — **refactored**.
- `modules/kern/src/syscalls_impl/time/time.cpp` — **refactored**.
- `modules/kern/src/syscalls_impl/vfs/sys_vfs.cpp` — **refactored**.
- `modules/kern/src/vfs/buffer_cache.cpp` — **refactored**.
- `modules/kern/src/vfs/core.cpp` — **refactored**.
- `modules/kern/src/vfs/epoll.cpp` — **refactored**.
- `modules/kern/src/vfs/fs/procfs.cpp` — **refactored**.
- `modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp` — **refactored**.
- `modules/kern/src/vfs/mount.cpp` — **refactored**.

## Additional current-source acquisition and lifetime inventory

The current-task getter masks IRQs before sampling its CPU/runqueue and copies
only the executing Task identity before restoring entry IF. The entries below
classify how callers use that identity; they do not grant a raw CPU/runqueue
pointer lifetime, replace object ownership or imply NMI context-switch support.

- `modules/kern/src/dev/block_request.cpp` — **refactored**. Caller, completion and worker pointers are Task identities. Queue IRQ-save locking and retained waiter/service/worker references protect publication; backend callbacks and parks run after queue unlock, with shutdown ownership held.
- `modules/kern/src/net/wki/secure_rdma.cpp` — **refactored**. The retained worker and PROCESS/DAEMON frontend checks use executing Task identity. Engine locking protects worker publication; frontend parks retain no CPU-local pointer or engine lock.
- `modules/kern/src/platform/mm/lifetime_retirement_executor.cpp` — **refactored**. Current-worker comparison and shutdown controller qualification use Task identity; the IRQ-save queue lock protects the retained worker/join reference, and callbacks or sleeps run unlocked.
- `modules/kern/src/platform/mm/managed_huge.cpp` — **refactored**. Preparation and current-root admission compare the explicit executing actor; actual root/PTE access has separate short preempt/PUB/reader guards, with no retained CPU-local pointer.
- `modules/kern/src/platform/mm/numa_alloc.cpp` — **preemption-protected**. CPU-to-immutable-NUMA-node capture is inside PreemptGuard. Later allocation consumes copied placement facts, not a retained CPU/runqueue reference.
- `modules/kern/src/platform/sched/workqueue.cpp` — **refactored**. Executing worker/context identity is stable across callbacks and parks. Queue IRQ-save locking and retained published Task references protect ownership; callbacks and waits run unlocked.
- `modules/kern/src/platform/sys/syscall_preparation.hpp` — **refactored**. Preparation qualifies its explicit executing actor and restores entry IF; it retains no CPU ID, runqueue pointer or CPU-local ownership across fallible work.
- `modules/kern/src/test/anonymous_usercopy_runtime.cpp` — **refactored**. Admission and cleanup use the explicit current actor/root incarnation; mapped usercopy and prepared cleanup retain Task identity rather than CPU-local state.
- `modules/kern/src/test/compartment_ktest.cpp` — **refactored**. Pending-submission checks use the executing executor identity, or the fixture-owned subject before task context exists; no CPU-local pointer survives submission/cancellation.
- `modules/kern/src/test/compartment_lifecycle_runtime.cpp` — **refactored**. Actor/controller/NTP comparisons use Task identity under the IRQ-save state lock. Retained subjects and census/evidence leases protect park/observation lifetime; waits do not retain that lock.
- `modules/kern/src/test/file_mapping_lifetime_runtime_ktest.cpp` — **refactored**. Real daemon/controller Task identity owns its retained mailbox and unpublished-child operations across ordinary waits; shutdown ownership and explicit DEAD joins protect retirement.
- `modules/kern/src/test/numa_demand_ktest.cpp` — **preemption-protected**. The test converts a CPU snapshot to an immutable topology node inside PreemptGuard, then retains only the copied node value.
- `modules/kern/src/test/numa_lifecycle_census.cpp` — **refactored**. Census scope/admission compare the explicit executing actor and root incarnation. Resource/session locks and census permissions govern observation, without a retained CPU-local pointer.
- `modules/kern/src/test/numa_pressure_runtime_ktest.cpp` — **re-read / refactored**. Independent before/after CPU samples are diagnostic scalars; the initial scalar selects copied immutable NUMA placement. The executing daemon is a Task identity, not a cached CPU proxy.
- `modules/kern/src/vfs/advisory.hpp` — **refactored**. The scanner match is comment-only: the declaration takes an explicit retained subject and states that it never resolves executing-task identity itself. No runtime CPU-local access occurs here.

Source-known follow-up: PerCpuCrossAccess::this_cpu_locked() and
this_cpu_locked_void() still sample CPU before acquiring their IRQ-save lock.
This416 change closes only Task acquisition, not that separate selection
interval or all CPU-local lifetimes. No observed failure or broader preemption
certification is inferred from this ledger or the focused native test.

## Enforcement and qualified migration boundary

`tests/host/unit/kernel_preemption_cpu_local_audit_test.py` reconstructs the
inventory from current source and requires every matching file to have a
classified ledger entry. A timer-preempted process kernel frame may now migrate
only through the centralized cross-CPU policy, which revalidates its durable
frame class, task resources, migration/preemption guards, return-transition
state, affinity/domain constraints, and WKI ownership. Current and handoff
tasks remain on their owner CPU. The same-CPL return assembly switches to the
incoming task's kernel stack before publishing the handoff; distinct tasks
with the same kernel-stack top are rejected before switch preparation and
again at the final ownership commit. `switch_to()` has already validated the
frame and installed target-CPU TSS, GS/FS, pagemap, and debug-register state.
User FPU restore and signal delivery remain deferred to a validated
userspace-return boundary.
