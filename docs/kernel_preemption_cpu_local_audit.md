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

## Mixed scheduler, return, and epoch paths

- `modules/kern/src/platform/sched/scheduler.cpp` — **migration-protected / preemption-protected**. Epoch read-side regions now begin inside `MigrationGuard`; raw current-runqueue pointers are confined to IRQ-off, per-CPU-lock, idle, or handoff code. Cross-CPU publication consumes the packed migration owner.
- `modules/kern/src/platform/sched/scheduler.hpp` — **preemption-protected / refactored**. Inline wait helpers retain only the current `Task*`; halt and runqueue boundaries mask interrupts, and guard APIs do not expose a CPU-local pointer.
- `modules/kern/src/platform/sched/epoch.cpp` — **migration-protected**. Returning `EpochGuard` call sites hold `MigrationGuard`; manual non-returning epoch ownership is confined to IRQ-disabled scheduler handoff paths.
- `modules/kern/src/syscalls_impl/process/waitpid.cpp` — **migration-protected**. Its epoch lifetime is nested inside `MigrationGuard`; its current task pointer is task identity.
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
- `modules/kern/src/net/wki/wki.cpp` — **re-read**. CPU is immediate WKI performance attribution.
- `modules/kern/src/platform/dbg/dbg.cpp` — **preemption-protected**. CPU read occurs only in the panic writer after normal scheduling has stopped.
- `modules/kern/src/platform/dbg/journal.cpp` — **re-read**. Record creation samples CPU into record metadata; no CPU-local pointer is retained.
- `modules/kern/src/platform/loader/elf_loader.cpp` — **re-read**. CPU is immediate local-loader performance attribution.
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
