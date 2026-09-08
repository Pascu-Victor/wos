# Goal 22: Kernel-Enforced Resource Governance

**Status: Done (2026-09-08).** See the completion record at the end for the final contract audit and live evidence.

## Planning metadata

- **Target horizon:** one to two continuous weeks with process/VM, VFS, init, and WKI workstreams.
- **Critical surfaces:** task lifecycle, FD/process creation, VM/stack/core, file growth, scheduler CPU accounting, service spawn, procfs, and remote execution.
- **Completion rule:** limits must be inherited, privileged, atomically enforced, observable, and tested at boundaries; libc-only bookkeeping is not completion.

## Copy-pastable goal command

```text
/goal Outcome: Replace WOS libc-local resource-limit bookkeeping with inherited, kernel-enforced per-process limits and declarative per-service budgets for open files, address space, stack, core size, process count, CPU time, and file growth, with coherent procfs/init/WKI accounting. | Verification surface: ABI and state-model tests, denial-side-effect tests for each resource, Normal Build WOS, isolated KTEST exhaustion/race cases, service-manifest tests, TESTD/userland scenarios, and user-run multi-node budget evidence. | Constraints: Preserve fork/exec inheritance, soft/hard-limit privilege rules, errno, fixed emergency reserves, scheduler/IRQ nonblocking rules, file durability, WKI compatibility, and KTEST isolation; a failed reservation must leave no partial object or charge. | Boundaries: Add kernel rlimit state/syscalls, reservations/charges, procfs and init status, service manifest budgets, and remote propagation; do not build cgroups, containers, a new allocator, or general scheduling classes. | Iteration policy: Define the limit/reservation model and ABI first, migrate one resource with boundary and rollback tests at a time, then add service and WKI propagation, using denial telemetry to drive the next uncovered path. | Blocked stop condition: Stop only after the same indispensable runtime blocker repeats for three goal turns, safe host/model/KTEST work is exhausted, and the exact missing scenario and unlock action are documented.
```

## Local source evidence (baseline before execution)

`toolchain/src/mlibc/sysdeps/wos/generic/sysdeps.cpp` keeps `getrlimit`/`setrlimit` state in a libc-static resource-limit array. `Task` has credentials, scheduling, VM, and descriptor state but no inherited kernel resource-limit object. Descriptor exhaustion is governed by the fixed FD table, and the init `ServiceSpec` has priority/restart/readiness fields but no resource budgets.

The kernel already has memory, CPU, child-process, file, and procfs accounting seams, but they are not tied to one reservation/commit model. Remote compute also needs a versioned policy so placement cannot silently escape the submitter's limits.

## Measurable completion contract

1. A versioned kernel ABI implements get/set limit with validated infinity, soft/hard values, privilege checks, and libc parity.
2. Fork/clone inherit one coherent snapshot; exec preserves limits; credential changes cannot illegitimately raise hard limits.
3. FD allocation, process/thread creation, address-space reservation, stack growth, core generation, CPU consumption, and file growth reserve before irreversible effects and release exactly once.
4. Concurrent reservations cannot oversubscribe a limit or strand charges on failure, cancellation, exec, or exit.
5. CPU-time exhaustion and stack/address-space failure have documented signal/errno behavior that is safe at interrupt/fault boundaries.
6. Procfs and service status expose configured, current, peak, denied, and reason counters without leaking unrelated process data.
7. Init manifest budgets validate ranges and apply before service exec; restart loops cannot evade aggregate policy accidentally.
8. Remote placement propagates a capability/versioned budget contract and reports explicit rejection when the target cannot enforce it.
9. Boundary tests cover zero, one-below, exact, one-above, infinity, privilege changes, concurrent consumers, and every unwind path.

## Invariants, boundaries, and overlap

Enforcement in scheduler, exception, allocator, and VFS paths cannot allocate or block where their context forbids it. Limits count logical resources consistently across shared VM/FD tables and WKI proxy objects. In scope are POSIX-style per-process limits and per-service defaults; cgroups, quotas across arbitrary groups, bandwidth control, and new schedulers are out of scope. Coordinate `Task` and `ServiceSpec` changes with Goal 19.

## Parallel workstreams

The primary agent owns ABI, charge semantics, inheritance, and integration. Subagent A handles process/CPU/VM/stack/core. Subagent B handles FD and file-growth enforcement. Subagent C handles init/procfs/WKI propagation and integration scenarios.

## Verification, iteration, and rollback

Run formatting, Normal `Build WOS`, ABI parity, host state models, focused KTEST exhaustion/fault cases, manifest tests, and user-run local/remote workloads. Land each resource behind complete accounting and denial tests. Keep old libc defaults only as a compatibility bridge until the kernel query succeeds; never silently fall back after enforcement is advertised. Use the `/goal` blocked condition exactly.

## Execution ledger (2026-09-07)

- Status: In progress. Original goal text loaded exactly, followed by:
  You are able to run and debug WOS live by appending --no-setup to the
  wos-ktest and wos-cluster scripts to not need root access.
- Source confirms libc-static limits and no kernel enforcement. Threads resolve
  descriptor ownership using owner_pid and a retained scheduler lookup.
- Impact map: process/process.cpp dispatch, fork and CLONE_VM_PROC;
  process/exec.cpp create-process spawn and in-place execve; sched/task.hpp
  fixed state and task.cpp thread creation; VFS core.cpp allocation, duplication,
  pipe/open and write/truncate; syscalls_impl/vmem/sys_vmem.cpp mmap reservations,
  unmap/remap; sched/threading and loader initial stack; dbg/coredump generation;
  scheduler CPU accounting; init ServiceSpec/manifest/spawn/status; procfs
  per-process output; WKI remote_compute request/target admission.
- Flow: libc get/set -> PROCESS dispatch -> retained process owner -> fixed
  state under resource_lock. Fork/clone/spawn snapshot policy before publication;
  execve retains the existing Task. Enforcement will reserve before backend
  side effects and release at rollback/close/unmap/exit.
- Stage 1 patch contract:
  Problem: libc-only limits reset on exec and cannot constrain kernel work.
  Expected files: abi/resource.hpp, callnums/process.h and libc mirror,
  sched/resource{,_model}.{hpp,cpp}, task.hpp, process/process.cpp and exec.cpp,
  host/kernel tests and later libc sysdeps.
  Files read: the listed ABI/dispatch/task/process and VFS entrypoints; usercopy,
  spinlock, module CMake files and local task command; applicable instructions.
  Invariants: old selectors/register ordering unchanged, coherent inheritance,
  no usercopy under locks, no allocation in policy operations, no owner fallback.
  Locks: scheduler registry only for retained owner lookup; resource_lock only
  for bounded state reads/writes. Release owner after resource_lock unlock.
  Unsafe contexts: IRQ/scheduler integration deferred to its own patch contract.
  Allocation/blocking: syscall usercopy outside spinlock; bounded state itself
  cannot allocate, block, log, or invoke callbacks.
  Compatibility: append selectors 48-50; version argument 1; fixed-width structs.
  Build/test plan: state boundaries, privilege, snapshot and reservation rollback;
  ABI parity, Normal Build WOS; isolated KTEST and target probes in later stages.
  Rollback: remove only this stage's additive selectors/state/calls; preserve
  unrelated existing VM artifacts and untracked routing evidence.
- Pending: resource enforcement and telemetry; init and WKI propagation; complete
  denial-side-effect/runtime matrix. No completion claim from model tests alone.

### Stage 2 patch contract: descriptor enforcement

Problem: open/create/truncate and accept consume resources before FD exhaustion
is discovered, and direct descriptor-install paths can bypass policy.
Expected files: sched/fd_table.hpp (extracted production fixed table), task.hpp;
vfs/fd_reservation.{hpp,cpp}, vfs/core.cpp; net/sys_net.cpp; process exit and
thread exit cleanup; process/exec.cpp direct installs; libc sysdeps/ABI mirror;
resource host/KTEST tests. The listed functions and callers were read locally.
Invariants: numeric RLIMIT_NOFILE bounds, no hidden reserved File objects,
no reservation copying at fork, failed reservation has no backend effects,
accepted reservations survive later soft-limit lowering, no leaked Task refs.
Locks: FD table -> resource state; no backend work or usercopy under either.
Unsafe contexts: no network RX or IRQ blocking; reservation list belongs solely
to the executor and is canceled before fatal exit discards the syscall stack.
Allocation/blocking: fixed reservation slots; stack nodes registered on Task;
backend open/accept may block outside locks. Explicit exit cleanup is mandatory.
ABI: old operations unchanged; new libc calls use fixed 16-byte rlimit and
version 1, without silent fallback; strace names all three appended operations.
Validation: host table reservation/overflow/privilege/race tests; KTEST actual
reservation cleanup and denied create/truncate; then target inheritance/exec,
fd syscall matrix and rootless VM evidence. Rollback this stage as a unit.

Evidence so far: Clang host state tests (ASan/UBSan) passed six cases before
FD-table cases were added; ABI parity and host manifest passed. Initial build
caught missing exhaustive strace decoder cases (fixed). A subsequent libc build
caught freestanding cstdint typedef conflict in the new mirror; switched its
include to libc's stdint.h. Full build and new tests still pending.
Remaining FD audit: spawn actions/stdio, internal exec-loader temporary FDs,
WKI direct installs, atomic pipe publication, denial telemetry and live tests.
Remaining resources and init/WKI contracts are not implemented yet.

### Live checkpoint

- First isolated rootless KTEST completed: 8,357 passed, 0 failed, including
  all four Resource cases (reservation lifecycle, fatal-exit cancellation,
  coherent inheritance, denied create/truncate). Command:
  `bin/wos-ktest --config configs/node_ktest_rootless.json --no-setup`.
  Saved serial: `/tmp/wos-goal22-ktest-stage2.log`, SHA-256
  `05a914ef935c5dfdaf21438cf2532d55a2cd840fb95efe0adc322f2b396c6b57`.
  Kernel SHA-256: `3197ed3943738196141047ee52f929788939540336e3a543d799a9790762ac23`.
  This run predates the atomic pipe-pair publication and final thread-fork
  FD-owner audit; rerun KTEST on final source before completion.
- Eight host ASan/UBSan tests passed; ABI parity and source/manifest checks
  passed. Normal Build WOS succeeded with sandbox escalation for libguestfs
  local socket binding; no WOS VMs were running during image packaging.
- Latest normal image built with `testprog resource-limits` and launched using
  `/tmp/wos-goal22-node.json`, `--launch --no-setup --debug-node 0`, temporary
  `/tmp/wos-goal22-vm` overlays and live descriptor. GDB connected to localhost
  port 1234, read RIP=0xfff0, then detached to resume boot. Capture:
  `/tmp/wos-goal22-live-gdb.log`. Userspace matrix is still pending.
- Internal ELF reads now use retained File objects without consuming user FDs.
  Source tests retain sparse/lazy ELF reader and file-lifetime checks. Fork and
  spawn copy a thread's process-owner FD table and CLOEXEC bitmap under lock.
- The libc rlimits conformance test now queries the existing hard limit rather
  than assuming the host can raise it to 4096; WOS's fixed table is 256 slots.

### Descriptor validation checkpoint (final stage-2 source)

- Normal Build WOS succeeded. Isolated rootless KTEST completed with 8,368
  passed, 0 failed, including atomic descriptor-pair publication. Saved serial:
  `/tmp/wos-goal22-ktest-final-fd-serial.log`.
- Rootless normal VM `/tmp/wos-goal22-vm2` passed all 38 userspace descriptor
  checks; installed libc `posix/rlimits` conformance case exited successfully.
  Logs: `/tmp/wos-goal22-userspace2.log`, `/tmp/wos-goal22-libc-rlimits.log`.
- An earlier test incorrectly expected a dynamically linked executable to
  initialize libc with NOFILE=0. Replaced that assertion with a static raw-ABI
  exec fixture; dynamic exec inheritance is separately checked with limit 16.
- Live WOSDBG opened a build-ID-verified pause/read session and captured an idle
  backtrace during an earlier VM shutdown stall at final userspace sync. It
  closed its pause lease and resumed successfully. That shutdown issue is not
  attributed to this patch without evidence; the owned VM was then stopped via
  its QMP socket before rebuilding disks. No unrelated VM was stopped.

### Stage 3 patch contract: file-size bounds

Problem: file writes and truncate currently ignore RLIMIT_FSIZE, and append
must choose EOF and enforce the bound within one inode serialization region.
Expected files: sched file-growth policy helper/model; tmpfs, XFS and FAT32
write/truncate implementations; remote VFS consumer paths after their full
entrypoint audit; resource host/KTEST and userspace tests.
Files already read: VFS core write/pwrite/sendfile/ftruncate paths, File and
FileOperations, tmpfs write/append/truncate, XFS write/append/truncate and inode
locking, FAT32 write/truncate, scheduler signal-pending API, resource ABI/model.
Invariants: no file data, size, allocation, or cache mutation on full denial;
short writes stop exactly at the bound; zero writes do not allocate; append EOF
and bound check share the inode lock; no charge persists across failed I/O.
Locks involved: policy snapshot before inode mutex; bounded arithmetic under
inode mutex; telemetry and signal publication after backend locks are released.
Unsafe contexts: no new scheduler/IRQ work; no usercopy or blocking with resource
spinlock held; no retained owner reference across faultable backend I/O.
Allocation/blocking assumptions: guard contains fixed scalars, retains no task,
and snapshots through a short-lived owner lookup before filesystem locks.
ABI/wire/syscall compatibility impact: SIGXFSZ and EFBIG for a fully denied
write/resize; successful prefixes return their byte count. Wire integration
remains a later explicit patch contract, not implied by local enforcement.
Build/test plan: overflow/zero/exact/short-write model tests, actual TMPFS append
bound/unchanged bytes tests, live TMPFS/XFS write/pwrite/truncate/append races,
Normal Build WOS and isolated KTEST.
Rollback plan: remove the file-growth helper and its backend calls as one stage;
retain independently validated descriptor implementation.

Stage 3 remote-consumer impact map/contract extension: read WKI instructions,
wire header/capabilities, WkiState/peer/public APIs, wki_init/send/RX entrypoints,
RemoteFileContext and remote write/truncate/SEEK_END, write-behind flush,
proxy request construction/wait ownership, and direct server write handlers.
The consumer sends explicit offsets; write-behind owns accepted bytes and keeps
partial flush tails. Apply the caller's bound before taking ctx->io_lock,
invalidating caches, allocating write-behind, flushing, or sending a truncate.
No wire fields or server resource authority change in this step. Deferred flush
may complete previously admitted bytes after the caller lowers its limit. No
resource lock spans ctx->io_lock or ACK waiting. Remote compute policy and
multinode denial evidence remain pending.

Stale navigation claim found: wki.instructions.md/cache describes an older
8954-byte payload and RoCE initialization. Local secure WKI v3 subtracts the
40-byte authentication trailer (payload 8914) and disables raw RoCE startup.
Use current constants and source, not those cache values, for this goal.

Stage 3 validation checkpoint:
- Ten host ASan/UBSan model tests pass. XFS write/metadata-lock and remote VFS
  source checks pass after following the new policy wrapper to its unchanged
  serialized implementation. Existing usercopy/exec/network checks pass.
- First stage-3 KTEST found a real early-init null scheduler access while TMPFS
  unpacked initramfs. Symbolized stack: get_current_task -> FileGrowth ctor ->
  tmpfs_write -> unpack_initramfs. Saved panic:
  `/tmp/wos-goal22-fsize-early-boot-panic.log`. Fixed by using has_run_queues()
  before obtaining a current task; pre-scheduler writes have no process policy.
- Normal Build WOS succeeded with that fix. Isolated KTEST rerun and the new
  live TMPFS/XFS boundary matrix are pending; do not treat compilation as proof
  of runtime enforcement. Remote consumer checks also need multinode evidence.

### Stage 4 patch contract: procfs resource observations

Problem: the resource ABI exposes self usage, but there is no process/thread
procfs limits view for budget diagnosis.
Expected files: vfs/fs/procfs.{hpp,cpp}, resource userspace tests, resource docs.
Files already read: procfs enum/data types, open/self/pid/task routing, readdir,
stat, lazy read generation and retained task lookup, resource state/FD locks.
Invariants: append directory entries without renumbering existing entries;
coherent policy/usage snapshot; no usercopy, formatting, allocation or retained
Task across blocking allocation while resource locks are held; recheck access
on each read, including an already generated snapshot. Root and the subject's
own process can inspect limits; unrelated unprivileged processes cannot.
Locks involved: retained scheduler lookups outside FD/resource locks; FD table
then resource state for a bounded snapshot, released before formatting/copy.
Unsafe contexts: procfs normal read path only, no IRQ/NAPI work.
Allocation/blocking assumptions: fixed snapshot on stack; existing procfs lazy
4-KiB content allocation after all owner references and spinlocks are released.
ABI/wire/syscall compatibility impact: new /proc/PID/limits and task aliases,
no existing file/selector changes. Show enforcement status for staged resources
so configured-only policies cannot be mistaken for enforced budgets.
Build/test plan: live self/thread visibility, denial/peak rows, denied unrelated
read after dropping UID; Normal Build WOS and existing procfs source checks.
Rollback plan: remove only the added procfs node/route/formatter and tests.

- Corrected stage-3 isolated KTEST: 8,387 passed, 0 failed; saved serial
  `/tmp/wos-goal22-ktest-fsize-fixed-serial.log`. Normal Build WOS passed.
- Fresh rootless normal VM passed the expanded 88-check resource matrix,
  including TMPFS and XFS zero/exact/partial/denied writes and truncates,
  preserved bytes, four concurrent appenders, and infinity restoration.
  Saved output: `/tmp/wos-goal22-userspace-fsize.log`.
- Procfs snapshot implementation is now pending build/live checks. Read access
  is checked against the current process identity or root on every read; this
  also covers a cached File inherited by a child that later drops privileges.

Stage-4 lock audit addition: simultaneous pread calls on one newly opened limits
file serialize its lazy snapshot with a per-file sleeping mutex. That mutex is
acquired before retained task lookup, then FD/resource locks are used only for
the snapshot. SEEK_END observes the generated length under the same mutex;
formatting/allocation happens after task references and spinlocks are released.
The target test starts four concurrent initial readers and compares all bytes.

- Stage 4 Normal Build WOS succeeded and a fresh rootless VM passed all 101
  resource checks, including four concurrent first readers, coherent cached
  observations, current/peak/denial fields, and foreign cached-file access after
  dropping UID. Saved output: `/tmp/wos-goal22-userspace-proc.log`.

### Stage 5 patch contract: deferred core-dump size

Problem: the coredump daemon currently writes with its own unlimited policy,
so a crashing process cannot constrain CORE or FSIZE output.
Expected files: dbg/coredump.cpp; resource extent model/helper; resource tests,
static fault fixture and rootfs alias; procfs enforcement status/docs.
Files already read: coredump public API, producer/ring/worker/output writer and
packed header/segment/module layouts; CPL3 gate call site; task refcount API;
resource owner/state; signal and exec source tests; testprog fixture build.
Invariants: no output file for a denied complete dump; fixed ring/pin emergency
reserves unchanged; no partial request publication, stolen ELF/file reference,
or pinned-page leak on denial; current crashing Task stays retained as before.
Limits apply to the complete binary dump, including tables/pages/embedded ELF.
Locks involved: bounded scheduler owner lookup and resource_lock only from the
CPL3 fault path (which holds no kernel spinlock); release before page capture.
Final admission repeats policy check under resource_lock before publication;
the asynchronous request retains a policy owner for completion observations.
Unsafe contexts: no allocation, VFS, waiting or logging added to the producer;
all output remains on the daemon's stack. Kernel panic paths remain excluded.
Allocation/blocking assumptions: fixed scalar sizing arithmetic and fixed ring;
size/overflow/limit denial releases already captured pins before returning.
ABI/wire/syscall compatibility impact: coredump file version/layout unchanged.
Finite CORE and FSIZE admit a whole valid dump or suppress output; zero disables
capture. Accepted dumps retain their admission if limits are subsequently
lowered. CORE observations describe the last successful binary output size.
Build/test plan: pure overflow and extent boundaries; actual zero/small/infinite
static userspace fault fixtures; KTEST ring/unwind checks; Normal Build WOS and
existing coredump redaction/ELF ownership source checks; inspect emitted dump.
Rollback plan: remove producer admission/completion accounting and internal
request additions together; preserve existing ring and file ABI.

Stage-5 implementation checkpoint: complete-size admission precedes transferring
ELF/File ownership and READY publication. A rejected capture releases pinned
pages and the crashing-task reference, returns the ring slot to EMPTY, and
creates no file. An accepted request retains a separate policy owner until the
worker records successful CORE/FSIZE output and releases it. The writer verifies
its immutable planned size before opening output and removes failed output.
Packed coredump header/module/segment formats and fixed ring capacity remain
unchanged. Eleven host ASan/UBSan tests pass; signal/exec source checks pass.
The KTEST core admission/owner-lifetime case has passed in the running suite;
aggregate result and actual static-fault output validation remain pending.

Stage-5 isolated KTEST completed: 8,388 passed, 0 failed; saved serial
`/tmp/wos-goal22-ktest-core-serial.log`. Normal Build WOS also succeeded.
The fresh normal VM under `/tmp/wos-goal22-vm5` is now reserved for denied and
admitted static crash fixtures and WOSDBG parsing of the resulting dump.


Stage-5 live validation: the complete matrix passed 111 checks, 0 failures.
Zero/small CORE and small FSIZE produced no partial output; the unlimited
static fixture produced a 33,296-byte v4 binary. WOSDBG parsed five segments,
matched embedded executable build ID fcc36ff307213386bba9f34b719b4c7ae06ac89f,
and decoded `ud2` at `_start+0x4`. Its compact heuristic labels the fault an
instruction-fetch fault, while parsed interrupt 6 correctly identifies invalid
opcode; use the trap/decode evidence rather than that heuristic label.
Evidence: `/tmp/wos-goal22-userspace-core.log`,
`/tmp/wos-goal22-vm5/resource-core.bin`, and
`/tmp/wos-goal22-vm5/analyze-core-result.json`.
CORE is now marked enforced in the procfs source. CPU/AS/STACK/NPROC, init
budgets, WKI compute policy/FD attachment audit and remote matrix remain open.

Next resource reconnaissance (STACK, not edited yet):
- Kernel-managed initial stacks reserve USER_STACK_SIZE=64 MiB with a 256-KiB
  initial top window and a separate bottom page used for GS/bootstrap metadata.
  TLS/SafeStack/guards share the immutable initial layout reservation. Retain
  that reservation geometry so increasing a soft limit can permit later growth.
- `threading::create_thread` is called by Task's inline-ELF constructor,
  initialize_process_image, and in-place exec staging. Legacy inline spawn
  currently initializes its image before the later resource::inherit call;
  policy must move before construction rather than enforcing against defaults.
- `ensure_stack_backing` is used for initial windows, argv/env writes in both
  exec paths and remote compute, and signal-frame writes. Lazy stack growth is
  reached after anonymous-swap and lazy-VM fault handling in virt.opt.cpp.
- Existing backing loops can map a prefix before an allocation failure. Proposed
  rollback-safe backing: allocate missing pages into a temporary chain stored
  within the not-yet-visible pages, free the whole chain on failure, and only
  zero/map after all pages exist. Initial stack PTEs are already reserved.
- Thread::stack_lowest_backed records downward growth. The bootstrap bottom
  metadata page is not the main stack's usable growth window and must retain
  its existing special treatment. Lowered limits do not revoke mapped pages.
- Libc-provided pthread stacks are ordinary explicit VM mappings, inferred from
  lazy ranges; they are distinct from automatic main-stack growth and are
  governed by AS. A STACK-only check on inferred whole VM ranges would reject
  valid caller-supplied stacks because the thread syscall carries no stack size.
- Still to read before a STACK patch contract: all initial constructor callers,
  fork/CLONE_VM thread metadata copying, mapped-page allocation/free assumptions,
  signal frame preparation, and init argument-limit failure propagation.

STACK reconnaissance caveat: scheduler blocking boundaries explicitly call
exit_current_on_pending_fatal_default_signal(). A temporary page chain cannot
rely solely on a C++ destructor while reclaim-capable page allocation can block.
It needs an executor-owned fatal-exit cleanup registration (like FD reservations),
or a proven nonblocking allocation/rollback scheme. Do not implement an
unregistered temporary chain or retain an owner across those blocking points.
The successful VM5 was stopped through its owned QMP socket after the host dump
and WOSDBG report were preserved; no normal validation VM is currently running.

Stage-6 patch contract (STACK):
Problem: STACK policy is inherited but automatic main-stack construction and
fault/signal/argv growth do not consult it.
Expected files to change: sched threading/task/resource model, process exec/fork,
MM fault routing, signal frame preparation, remote argv call site, tests/docs.
Files already read: these entrypoints plus phys allocation, user layout, TLB
shootdown, scheduler preemption/fatal handoff, boot Task constructor callers.
Invariants to preserve: 64-MiB reservation, guard/TLS/SafeStack/GS layout; existing
mapped pages after lowering; fork/exec policy; caller-supplied pthread VM stacks;
no mapped prefix or charge on rejected growth/allocation failure.
Locks involved: bounded process resource lock for policy/telemetry, existing
page-table/physical-page/TLB locks; preemption guard around temporary page chain.
Unsafe contexts: user fault and signal paths with IRQs disabled; early boot;
normal exec syscall construction and WKI worker argv construction.
Allocation/blocking assumptions: stack backing batches use non-reclaiming
page_alloc_may_fail while preemption is disabled. Allocate the entire missing
batch before mapping, using page-local chain nodes; free all on failure. No
blocking or retained owner across reclaim. Existing PTE reservation means final
mapping does not allocate page-table levels; TLB completion only spins/services
IPIs. This replaces the unsafe reclaim-capable temporary-chain proposal above.
ABI/wire/syscall compatibility impact: no layout or wire changes; finite STACK
becomes enforced for automatic main stack, page-granular backing rounded down;
explicit pthread mappings remain a separate AS surface. Zero prevents new images.
Build/test plan: host boundary model, KTEST denied/accepted backing and unchanged
mapping checks, Normal Build WOS, isolated KTEST, rootless exec/fault/raise/lower
userspace scenarios. STACK current is main-stack downward committed extent;
staged exec only updates usage after image publication.
Rollback plan: revert this stage's stack hooks and constructor extraction,
retaining previously validated NOFILE/FSIZE/CORE behavior and ABI.

Stage-6 implementation checkpoint: explicit policy subjects at every stack
construction/backing caller; inline spawn now uses an owned skeleton and
inherits before either ELF initializer. Backing batches preflight every missing
page, then allocate with no reclaim under a preemption guard and publish only
after the complete batch exists. Existing backing is grandfathered. Pthread
explicit mappings no longer fall through to automatic stack backing after a
failed lazy-VM fault. Twelve host ASan/UBSan cases pass; Normal Build WOS passes.
The first KTEST image lacked the new allocation failure hook's actual injection
branch (the editing match missed a formatted line). Three rollback assertions
correctly failed while all admission assertions passed. Hook corrected; rerun
required. Source VM/exec/signal/WKI checks pass; scheduler source assertion
updated for the explicit policy-owner argument while retaining lazy layout checks.
Anonymous swap currently selects only lazy anonymous VM ranges, excluding the
kernel-managed initial stack, so nonpresent initial stack leaves represent new
backing rather than swapped-out committed pages. VM source checks confirm mmap,
munmap and mprotect cannot replace the protected initial layout/PTE hierarchy.

Corrected STACK KTEST admission/rollback case passes in the running suite;
aggregate result pending. The live static fixture expects a process-lifetime
peak at least as large as its current extent (forked old-image backing remains
part of that peak across exec), not an incorrectly reset per-image peak.
CPU reconnaissance started while builds run: scheduler::account_runtime_slice
subtracts IRQ time, then account_task_runtime_delta consumes syscall-precharged
microseconds; charge_syscall_time_until separately adds only new syscall time.
Any CPU hook must follow both seams without double-charging and cannot perform
registry lookup/release while a runqueue lock is held. Thread policy lifetime
requires a stable retained owner/account before adding an IRQ accounting hook.
No CPU implementation or stage-7 patch contract yet.

Stage-6 isolated KTEST complete: 8,428 passed, 0 failed. Saved serial at
/tmp/wos-goal22-ktest-stack-fixed-serial.log. Formatting/tidy completed with
advisory include-cleaner warnings. Normal Build WOS succeeded. A fixture-only
rebuild exposed missing rootfs dependency entries for the three new static
probes; added their TARGET_FILE entries alongside testprog so packaging cannot
silently retain an older probe after its source changes.

Stage-6 live result: all four STACK cases passed (exact/lower/raise/telemetry,
SIGSEGV beyond boundary, zero-limit exec rollback, E2BIG argv rollback). The
full matrix reported 114 checks, one failure because unlimited core never left
READY. Live GDB established CPU3 idle with 3 heap entries: PID121 testprog
EXITING/has_exited/pagemap=null at index0, followed by ndp_timer and coredump.
The idle picker returns on the invalid earliest candidate without removing it;
the running-task picker already removes such stale candidates. Saved evidence:
/tmp/wos-goal22-userspace-stack.log and /tmp/wos-goal22-vm6/inspect-{core,heap,queue2}.log.

Stage-6b patch contract (observed scheduler obstruction):
Problem: invalid earliest runnable candidate prevents idle CPU from selecting
valid later work, stranding the admitted core request indefinitely.
Expected files to change: scheduler.cpp, scheduler_ktest.cpp, scheduler source test.
Files already read: scheduler idle/running/exit pickers, RunHeap, transition
helpers, task exit/reclamation, context_switch timer return, live heap snapshot.
Invariants: preserve heap ownership, sums/load bookkeeping, existing invalid
candidate cleanup semantics, ptrace handling, return-frame handoff and IRQ rules.
Locks: existing local runqueue lock; no new locks, allocation or callbacks in picker.
Unsafe contexts: timer IRQ/idle selection. Regression fixture runs only in KTEST.
Allocation/blocking assumptions: reuse the existing bounded fixed-heap pruning
helper already used on running-task selections; no allocation or blocking.
ABI/wire/syscall impact: none.
Build/test plan: deterministic exiting-root/healthy-daemon regression, scheduler
source tests, Normal Build WOS, isolated KTEST, fresh rootless full matrix.
Rollback: revert idle-picker helper call and its regression test only.

The first idle-picker regression fixture used Task{} without setting heap_index
(the runtime constructors set it to -1). Scheduler publication correctly rejected
that invalid fixture before the picker was tested. Fixed the fixture to use -1;
saved /tmp/wos-goal22-idle-fixture-init-panic.log and restarted isolated KTEST.
No production behavior changed in this correction. Normal Build WOS and source
checks pass for the idle picker change; live VM6 was stopped via owned QMP.

CPU/AS follow-up reconnaissance (no edits):
- Only Task::create_user_thread sets is_thread=true/owner_pid in production.
  A task-owned retained policy-owner member could give IRQ CPU accounting a
  stable pointer; Task::release only decrements an atomic, and every unpublished
  thread failure/reclamation ultimately deletes Task. Bind before publication;
  do not use registry lookup from the runqueue-locked CPU charge hook.
- Thread-group fatal signal exit already requests every sibling's exit. CPU
  accounting should only enqueue SIGXCPU/SIGKILL and let existing return paths
  deliver it; no logging, allocation, or group iteration in the IRQ hook.
- AS entrypoints are sys_vmem.cpp anon_allocate/free, file_allocate, mremap,
  shm attach/detach and ELF/thread initial layout. SharedVmemPublicationGuard
  stabilizes pagemap publisher membership. collect_user_memory_stats reports
  logical virtual pages separately from resident/shared/swap/table pages.
  A pagemap is exactly a hardware table page; do not append accounting fields
  to that hardware layout. Charges must cover reservations and unique logical
  pages, including protected initial stack/TLS/guards, rather than RSS.

Stage-6b isolated KTEST completed: 8,429 passed, 0 failed, including the exiting
heap-root regression. Evidence /tmp/wos-goal22-ktest-idle-prune-fixed-serial.log.
Normal Build WOS succeeded and formatting/tidy/source checks passed. Fresh VM7
now runs the complete resource matrix with the production idle-picker fix.

Stage-6/6b complete live matrix: 115 checks, 0 failures. The fresh VM7 produced
its admitted 33,296-byte core immediately, with cdmp completion in serial.
All four stack checks passed, and procfs now advertises STACK as enforced.
Evidence: /tmp/wos-goal22-userspace-idle-prune.log,
/tmp/wos-goal22-vm7/serial-vm0.log, /tmp/wos-goal22-vm7/resource-core.bin.
The owned VM was stopped via QMP after preserving evidence. CPU, AS, NPROC,
init budgets/status, WKI propagation/attachment audit, and distributed runtime
validation remain open; Goal 22 is not complete.

Stage-7 patch contract (CPU):
Problem: inherited CPU policy has no effect; user/system CPU accounting already
has two non-overlapping charging seams but threads need stable shared ownership.
Expected files: resource model/lifetime/implementation, Task/thread construction,
scheduler CPU accounting, resource ABI reason mirror/procfs, host/KTEST/userspace.
Files read: scheduler account_runtime_slice/account_task_runtime_delta and syscall
precharge logic, Task reference/GC/unpublished thread cleanup, signal enqueue and
default/group exit, all production is_thread assignments, procfs resource rows.
Invariants: preserve existing user/system accounting and subtract precharged time
exactly once; fork resets consumption and exec retains it; aggregate sibling CPU;
seconds ABI, IRQ exclusion as defined by existing accounting; no new scheduling
class, blocking, registry lookup, allocation or group walk in charging hooks.
Locks: existing runqueue lock may be held; only resource_lock nested below it.
Limit syscalls never acquire a runqueue lock under resource_lock. Signal enqueue
is a Task atomic OR after the policy lock is released.
Unsafe contexts: timer IRQ, syscall completion, early boot. Ignore non-PROCESS.
Allocation/blocking assumptions: immutable retained process-owner reference binds
before thread publication and releases through Task member destruction; charging
uses that pointer directly, without lookup or changing refs in IRQ context.
ABI/wire/syscall impact: CPU uses existing seconds limits/observations; append
hard_limit denial reason without changing packed sizes/selectors. Consumption is
measured in internal microseconds, displayed as whole CPU seconds. SIGXCPU on
soft threshold, at most once per further consumed second; SIGKILL at hard bound.
Enforcement occurs at existing accounting/return boundaries, with scheduler
quantum latency (not a promise of zero instruction overshoot on concurrent CPUs).
Build/test plan: pure zero/exact/repeated-soft/hard/overflow/fork tests; KTEST
owner lifetime and shared-thread charge; Normal Build WOS and diagnostic KTEST;
live signal/default/ignore-hard/multithread/exec/inheritance scenarios.
Rollback: remove CPU hooks and lifetime binding, leaving prior enforced resources.

Stage-7 model/integration checkpoint: 16 host ASan/UBSan cases pass. The new
thread member retains only process metadata and releases by atomic decrement on
Task destruction, including unpublished cleanup. IRQ charging uses no registry
or refcount operation. CPU total follows syscall charge plus timer UNCHARGED_US
(after consuming syscall precharge), and only sets pending signal bits.
The existing scheduler timer decision already keeps a periodic tick for active
processes even without competitors, so CPU exhaustion cannot disappear merely
because a process is the sole runnable task. No new timer/scheduling policy added.
Live fixtures cover zero/default soft, zero hard, ignored-soft hard, threads,
repeated caught soft, fork reset and exec-preserved consumption; not run yet.

Stage-7 safety addendum before signal integration edits: ordinary pending fatal
signals may be consumed by exit_current_on_pending_fatal_default_signal on a
kernel-stack handoff, including a blocked syscall that still owns locks. CPU
exhaustion must not newly trigger that path. Store CPU signal events in a
separate Task atomic, and consume them only for the current task after validated
USER_RETURN gates or completed syscall return. Incoming handoffs retain the
buffer until their own next user-return boundary. Hard exhaustion is handled
before the generic in_signal_handler deferral, so an infinite SIGXCPU handler
cannot evade SIGKILL. This adds bounded atomic transfer at existing return
entrypoints; no change to external-signal/group-exit semantics or wire layout.

Initial CPU KTEST completed 8,446 passed, 0 failed (saved cpu-initial serial),
but that image predates the safe-user-return signal buffer and is not validation
of the final CPU delivery path. The updated source checks verify IRQ charging
only buffers events, kernel-stack fatal handoffs never consume them, and user
frame validation precedes CPU delivery, which precedes in-handler deferral.
The new live looping-soft-handler fixture must terminate via SIGKILL at hard
expiry. Normal Build WOS and ABI parity passed before this delivery refinement;
both kernel/runtime profiles are being rebuilt for the final path.

Stage-7 final delivery-path isolated KTEST: 8,448 passed, 0 failed; saved
/tmp/wos-goal22-ktest-cpu-boundary-serial.log. Normal Build WOS succeeded, ABI
parity and 16 model cases passed, and formatting/tidy/source checks passed.
Fresh rootless VM8 is next for the nine CPU runtime scenarios and full matrix.

Stage 7b live handoff correction contract:
Problem: VM8 full matrix 124 checks / 2 failures; CPU-only repetition caught a
looping SIGXCPU handler consuming 330 seconds with hard=1 and SIGKILL buffered.
GDB cpu-return.log proves every observed outgoing timer chose a different task;
noncurrent incoming user handoffs never flushed the CPU buffer. Requiring a
same-task timer return is not a bounded latency guarantee under competition.
Expected files: signal.cpp/hpp, scheduler.cpp, CPU/source tests and ledger/docs.
Files read: signal delivery gates, context_switch.cpp/.asm user and kernel return
macros, frame_class.hpp, scheduler timer/deferred/commit/exit selection paths.
Invariants: never terminate a task on another task's stack; CPU expiry must not
kill a blocked syscall on a kernel return; preserve validated frame/GS/FPU order.
Locks: existing runqueue commit lock released before signal actions; no new locks.
Unsafe contexts: timer/handoff; no allocation in event transfer; existing mapped
signal-frame construction and process exit only at established user boundaries.
Allocation/blocking: signal transfer atomic only, hard action after own-stack
commit and validated USER_RETURN context; existing exit cleanup rules preserved.
ABI: unchanged. Soft events may enter ordinary pending state only after a user
return is validated; hard events remain buffered until own-stack user commit.
Tests: source ordering checks, CPU model/KTEST, Normal Build, fresh rootless VM
CPU/full matrix including looping handler and competing threads.
Rollback: remove only handoff event transfer/hard-user-commit gate.

Stage 7b validation: Normal Build WOS passed
(/tmp/wos-goal22-build-cpu-handoff.log); formatting/tidy/source checks passed.
Fresh isolated rootless KTEST: 8,448 passed, 0 failed, preserved as
/tmp/wos-goal22-ktest-cpu-handoff-serial.log. Fresh VM9 full live matrix:
124 checks, 0 failed (/tmp/wos-goal22-userspace-cpu-handoff.log), followed by
three CPU-only runs, each 9 checks / 0 failures
(/tmp/wos-goal22-cpu-handoff-repeat.log). No debugger pauses in these passing
runs. CORE fixture remained complete at 33,296 bytes. CPU procfs row now marked
enforced for the next build. Prior VM8 failures and GDB evidence remain saved;
they were implementation bugs, not indispensable runtime blockers.

Stage 8 AS reconnaissance and patch contract:
Files: sched resource/model and Task initialization; MM virt/paging; ELF loader;
VMEM mmap/munmap/mremap; SysV shmat/shmdt; exec publication; procfs; host/KTEST/live.
Entrypoints read: anon_allocate/file_allocate/anon_free/anon_mremap and shared-VM
publication/metadata helpers, shmat/shmdt and segment pins, create_thread and both
Task image initializers, all load_elf callers, PT_LOAD lazy/eager and metadata
mapping, exec staged root swap, Task usercopy quiescence, registry snapshots,
collect_user_memory_stats and root access gates. PageTable is exactly 4096 bytes.
Flow: check rounded logical growth under SharedVmemPublicationGuard before PTE
reservation or MAP_FIXED removal; existing transaction owns accepted ranges and
unwinds them. Private exec roots check inherited policy before each image/layout
allocation and publish observations only on commit. Fork inherits existing VA
(including mappings grandfathered after lowering), without resetting the policy.
AS counts virtual occupied pages, including guards, PROT_NONE, NORESERVE, lazy
file/anonymous, stack/TLS, and shared-memory attachments, rather than RSS.
Lazy ELF currently exists only in side metadata: install reserved PTE markers
before loading so lazy image pages cannot disappear from logical accounting.
Shared-root mutations and queries serialize through the existing publication
mutex. Use an allocation-free sparse page-table walk; exclude kernel-half and
supervisor mappings and include reserved leaves. Root-access/usercopy pins protect
teardown. No new allocator, page-table ABI field, or per-fault resource lock.
WOS permits a thread exec to retain siblings' old roots: AS current/peak describe
the queried task's address space, with process-owner policy and denial telemetry.
Update every live publisher's AS observations after shared-root mutations, so
CLONE_VM peers cannot miss a transient peak. Threads that still share a root
report the same current; exec preserves that task's historical peak.
MAP_FIXED admission uses net logical growth and fails before removing old data.
WOS mremap currently allocates/copies then removes: both live virtual ranges count
during relocation; failure must preserve the source and release the destination.
Locks: shared-VM publication -> registry (publisher observations) -> resource;
query content mutex -> shared-VM -> descriptor/resource. No resource -> registry
or runqueue nesting; no reference destruction/usercopy under spinlocks.
Unsafe contexts: new walks/admission only process construction/VM syscalls/procfs,
not IRQ/fault allocation. Existing reserved-page materialization is not AS growth.
Allocation/blocking: no new heap allocation in admission/observation; loader union
planning uses bounded existing ELF metadata context before any user mappings.
ABI impact: same AS selector/bytes and usage layout; errno ENOMEM on AS denial.
Build/test: boundary/overflow/net-replacement model, sparse page count + reservations
KTEST, lazy/eager/file/shm/thread-race/MAP_FIXED/exec preservation live tests,
source rollback/order tests, Normal Build and isolated rootless KTEST.
Unknowns: remote publication paths will receive the same policy during the later
WKI capability/propagation stage; AS is not advertised enforced until integrated.
Rollback: remove AS admission/observation hooks and retain earlier resource stages.
AS lifetime refinement: root walks and observation hold PreemptGuard while their
nonallocating Task/root pins are live, preventing non-unwinding fatal handoffs
from stranding pin counters. They do not mask IRQs for the full walk. Existing
ELF validation proves byte-disjoint PT_LOAD intervals; count shared rounded
first/last pages once without a heap planning buffer. This preserves program
header order and the validator's existing O(phnum squared) bound.
AS procfs lock correction before validation: procfs uses the AS current/peak
published at completed VM transactions, rather than walking under the shared-VM
mutex while holding its File content mutex. File mmap can itself read procfs
while holding the shared-VM mutex; taking it again inside procfs would deadlock
(and taking content then VM would invert that order across readers). Explicit
RESOURCE_USAGE snapshots can take shared-VM and refresh from the tree normally.
AS initial integration checkpoint: 18 host ASan/UBSan model cases passed
(/tmp/wos-goal22-as-model.log). Normal Build WOS passed first integration and
then final initial AS/test source (/tmp/wos-goal22-build-as-tests.log). Formatting,
clang-tidy and VM/exec/scheduler/signal source checks passed. The first isolated
AS KTEST is now running; AS remains advertised pending until runtime validation.
Live matrix now contains fixed/lazy/file/shared-memory bounds, replacement and
remap data preservation, thread races, and failed/successful finite exec cases.
First AS KTEST: 8,478 passed / 3 failed, then expected selftest-failure panic.
All failures were the new one-page lazy-ELF fixture: lazy_load_page_range
intentionally keeps page_no=0 eager. Corrected the fixture to two pages and
asserts eager first/reserved second plus the same exact total budget. Production
code unchanged by this correction. Failed serial preserved at
/tmp/wos-goal22-ktest-as-fixture-serial.log; rerun is required.
AS corrected-fixture isolated KTEST completed: 8,482 passed, 0 failed; both ELF
admission tests and sparse reservation counting passed. Preserved serial:
/tmp/wos-goal22-ktest-as-fixed-serial.log. VM powered off and its live descriptor
was removed. Next validation is the full resource userspace matrix in fresh VM10.

Stage 8b live accounting correction contract:
Problem: VM10 reports 159 checks / 6 failures. Lazy anonymous/file mmap ranges
are metadata reservations, without PTE markers. A PTE-only walk undercounts
both current use and admission; supervisor leaves also encode resident PROT_NONE.
Expected files: resource_as.cpp/hpp, virt.opt.cpp, resource KTEST, VM source tests,
userspace AS-only runner, documentation. Files read: mmap reservation/lazy/eager
paths, metadata update/clone/release, fault mapping, root gates, kernel PML4 copy.
Invariants: retain sparse lazy mappings without allocating page tables merely
for accounting. Count the union of disjoint lazy VM metadata and occupied lower
PTEs, walking only gaps outside metadata to avoid concurrent-fault double count.
Private staged roots must never include the subject's old-image metadata.
Locks/context: existing shared-VM transaction or exclusively owned unpublished
Task; no new lock, allocation, usercopy, logging or blocking in counting. Current
Task metadata cannot be released by another task; shared metadata writers already
serialize on the publication guard. Root access gates remain nonblocking.
ABI: unchanged. Lower-half resident PROT_NONE counts regardless of PAGE_USER;
kernel mappings occupy only the excluded upper PML4 half.
Tests: metadata/PTE overlap, unsorted and adjacent ranges, partial replacement,
private staged root isolation, PROT_NONE, live AS-only and complete matrix,
Normal Build WOS and isolated KTEST. Rollback this correction independently.

Stage 9 NPROC reconnaissance and patch contract:
Problem: fork, CLONE_VM, pthread creation and spawn ignore inherited NPROC.
Expected files: resource_process.{hpp,cpp}, Task, fork/clone/spawn/thread/exit,
boot task seed, WKI receiver task seed, resource syscall/procfs, model/KTEST/live.
Files read: all production Task creators, process/thread exit, unpublished Task
teardown and FD reservation fatal cancellation; SETUID/SETEUID and fork credentials;
WKI buffer/file task construction and recovery ownership. Init/WKI policy remains
a later stage; receiver tasks must already participate in the local population.
Semantics: NPROC bounds live user processes plus threads for the creator's real
UID. Root is subject to finite limits so service budgets remain useful. Policy
and denial belong to the process owner; current/peak describe the queried real
UID's local population, including pending creation reservations. Kernel daemons
and idle tasks are excluded. Zombies cease charging after logical exit cleanup.
A real-UID change moves its existing charge atomically and fails EAGAIN if the
destination would exceed this process's soft bound; effective-UID-only changes
neither move a charge nor raise hard limits. This explicit immediate-failure
policy avoids silently bypassing admission through setuid.
Model: one intrusive node per pending stack reservation or charged Task, no heap
UID table or arbitrary UID capacity. A population spinlock serializes count,
admission, transfer and release; successful creation replaces its pending node
with the Task node without another increment. Failed construction and fatal
cancellation unlink once. Task member destruction is the unpublished fallback;
normal logical exit releases before zombie publication, independent of GC refs.
Locks: population -> resource; owner lookup/ref acquisition before population.
No resource -> population nesting; no callbacks/allocations/blocking/usercopy in
population operations. Pending nodes register on their executor for non-unwinding
fatal exit. Attach only when child recovery ownership exists or immediately
before nonblocking publication; do not strand a charge on an abandoned stack.
ABI: existing NPROC slot/count units, EAGAIN on admission/UID transfer denial;
no new selector or structure. Pending and live population are node-local until
the explicit WKI policy stage, which must not claim a cross-node UID global quota.
Build/test: exact/zero/infinity/lowering/UID-transfer model and actual lifecycle
KTEST, cancellation/unpublished cleanup, fork/thread races, credential changes,
exec inheritance, Normal Build WOS, isolated rootless KTEST and fresh VM matrix.
Rollback: remove stage-9 hooks and node member together; retain prior resources.

Stage 8c observed live return-boundary obstruction contract:
AS union KTEST passed 8,498 / 0. VM11 AS-only reached thread setup with no AS
failures, then stalled. GDB proves PID117 in user-mode spin with interrupts on,
preempt depth 0, deferred_task_switch=true, yield_switch=true, no wait channel,
syscall_account_start_us=0; timer diagnostic 0x601 is USER/RETURN_TRANSITION.
PID109 remains runnable with the earliest deadline on CPU0. Timers keep firing.
Evidence: /tmp/wos-goal22-vm11/as-{gates,heap,state,pick,clear-deferred-clean}.log.
Problem: defer_process_reschedule_to_syscall_exit accepts any process-owned
kernel stack, including late user handoff/return callbacks after syscall accounting
has finished. It can enqueue a deferred switch after the assembly deferred gate,
then user timer preemption refuses that unfinished transition forever.
Expected files: context_switch.cpp, scheduler source-order check, ledger.
Files read: request_reschedule/defer helper; preempt enable pending service;
commit_handoff/requeue; USER eligibility policy; syscall.cpp accounting finish and
syscall.asm deferred/signal/final return ordering. No assembly changes planned.
Invariant: only an actively accounted syscall may defer to its future syscall
exit. When paused or already finished, keep the existing local timer-poke path.
Explicit blocking syscall deferred flags and WKI exec handoff remain unchanged.
Locks/unsafe contexts: syscall, timer/handoff; scalar phase test, no allocation,
blocking, locks or new callbacks. Same-stack ownership validation retained.
ABI unchanged. Tests: source guard ordering, scheduler tests and complete KTEST;
fresh AS race/full matrix plus repeated CPU signal tests, without debug mutation.
Rollback: remove only the active-syscall guard. An intervened VM is diagnosis,
not a passing validation run.
Stage 8c exact origin confirmed by hardware watchpoint: clearing PID117's flag
caused it to be set again in the very next timer, through MigrationGuard ctor ->
preempt_enable_token_at -> request_local_reschedule -> defer_process_reschedule_to_syscall_exit.
This was an interrupt of userspace on the task's TSS stack, with syscall accounting
inactive, not an active syscall. /tmp/wos-goal22-vm11/as-watch-deferred.log is the
bounded backtrace. Added the active-accounting guard so this path pokes the timer
instead of creating an unreachable syscall-exit deferral. VM11 remains diagnosis
only; fresh unmodified runtime validation is required.

Stage 9 libc failure-unwind addendum (before edits):
Source inspection found thread_create returns Clone errors without releasing its
new TCB/TLS allocation, and WOS Clone leaves the automatic 8 MiB stack mapping.
NPROC denial would therefore leak userspace AS even though the kernel reservation
unwinds. Read generic threads, rtld allocateTcb/initTlsObjects and allocator,
WOS PrepareStack/Clone and Linux comparison, plus mlibc instructions.
Plan: keep the raw allocation in a private unpublished-TCB token returned through
an additive rtld helper; release TCB-local keys/DTV/backing only before a thread
has been created. Preserve Tcb size/offsets and the existing allocate entrypoint.
WOS Clone owns automatic stack rollback on failure (its nonzero guard marks
that ownership; explicit caller stack forces guard=0). PrepareStack already
unmaps on protection failure. Successful thread/join lifetime is unchanged.
No kernel/syscall/TLS-layout ABI changes, no cross-platform stack-base assumptions.
Validation: repeated failed pthread creation has stable AS and NPROC population;
caller-supplied stacks survive denial; full libc/Normal/KTEST builds and live tests.
Stage 9 first integration: 19 ASan/UBSan host model cases passed, Normal Build
WOS passed (/tmp/wos-goal22-build-nproc-initial.log), isolated rootless KTEST
passed 8,537 / 0 (/tmp/wos-goal22-ktest-nproc-initial-serial.log). That KTEST
includes the AS union and active-syscall reschedule correction. Subsequent local
source audit added receiver UID charge transfer and proxy completion release,
plus caller-owned-stack and CLONE_VM denial probes; Normal Build is running for
those additions. No NPROC or corrected-AS live success claimed yet.

Stage 9b live result and MM wait patch contract (before editing):
- Final NPROC audit Normal Build and formatting/tidy passed. Fresh VM12 AS-only
  passed all 35 checks without debugger intervention. NPROC progressed through
  denied fork/pthread/CLONE_VM probes, then exposed two issues: its nonroot
  fixture could not create in root-owned /tmp; concurrent fork and nanosleep
  deadlocked at the shared user-pagemap access gate.
- Bounded live GDB shows CPU0 fork draining gate readers, CPU4 usercopy COW
  waiting for remote TLB acknowledgements, six other CPUs waiting for the gate.
  Every CPU has IF=0. Evidence: /tmp/wos-goal22-vm12/nproc-backtraces.log and
  nproc-flags.log. CPU-only SSH could not connect; no CPU/NPROC pass claimed.
Problem: raw page-map lifetime-gate spins do not service TLB requests required
by the reader/exclusive holder they await.
Expected files: mm/virt.opt.cpp, tlb_shootdown_source_test.py; testprog fixture.
Files read: gate reader/writer acquisition, COW fork, pin/fault path, TLB
service and completion, raw spinlock service convention, kernel/critical docs.
Invariants: pin lifetime exclusion, source COW publication and final TLB flush,
IRQ state, fixed allocation-free/nonblocking TLB service, mapped-only fail-closed.
Locks: no new locks; cooperative service uses existing atomics and local TLB
invalidation only. Service periodically only while IF is already masked; when
IF is enabled the IPI handler can run. No scheduler/preemption operation.
Unsafe contexts: gate acquisition can occur with IF=0 during usercopy/syscall,
fork or teardown; early boot service is a no-op before TLB initialization.
Allocation/blocking: no allocation, logging, callbacks or sleeping added.
ABI/wire/syscall impact: none.
Build/test plan: raw-wait source invariants, usercopy/VM tests, Normal Build,
fresh live NPROC concurrency then AS/CPU/full matrix; diagnostic KTEST.
Rollback: remove just cooperative-service helper/calls and source assertion.

Stage 9b: Normal Build and formatting passed. Fresh VM13 NPROC completed
28 checks with one failure, and subsequent CPU 9/0 and AS 35/0 passed without
GDB intervention. Concurrent fork/thread release, repeated pthread allocation
rollback, CLONE_VM TID retention and UID denial all passed. Remaining failure
is posix_spawn returning EPERM instead of EAGAIN. Source proves its generic
fork fallback does `ec = -pid` on libc fork's -1 result, losing errno.
Libc patch contract: change options/posix/generic/spawn.cpp to snapshot errno
immediately after failed fork before close, and return that positive error.
Read fast-spawn selection, complete fork fallback and WOS Spawn/Fork sysdeps.
Preserve signal-mask/cancellation restoration, pipe cleanup and successful PID
publication. No locks, allocator, ABI, successful-spawn or kernel changes.
Add a forced-fallback denial probe alongside the normal fast-spawn denial.
Validate full libc Normal build and live tests; rollback these narrow hunks.
VM13 full matrix completed 160 checks with only the expected NPROC spawn errno
failure (the nested NPROC suite was 28 checks / 1). CORE fixture 33296 bytes was
produced; this is no new debugger-format assertion. The VM was then stopped
through its owned QMP socket. AS/NPROC procfs labels now reflect active kernel
enforcement. docs/resource_governance.md documents union accounting, UID task
population, failed pthread ownership and pending service/WKI integration.

Stage 9b validation: isolated diagnostic KTEST passed 8537 / 0 and powered off
(/tmp/wos-goal22-ktest-nproc-tlb-serial.log). This includes receiver UID transfer,
proxy charge release and cooperative TLB waits, but predates the libc spawn
errno fix and procfs label update. Final Normal Build and formatting/tidy for
those last changes passed; fresh VM14 is ready for the complete live matrix.

Stage 10a init manifest reconnaissance and bounded patch contract:
Read service_manifest.{h,cpp}, supervisor_model.h, services.cpp runtime/spawn/
reap/status, init_control ABI/kernel publisher and servicectl status, init host
tests, relevant subsystem/kernel/critical/ABI/build instructions. No path-specific
init instructions exist. Flow: bounded v1 parser -> validated dependency graph
-> pure supervisor model actions -> fork/stdio/pgid/priority/exec -> error pipe
-> per-generation reap/quiescence -> status publication and restart policy.
The manifest has no resource fields; restart-count/window resets are separate
from the persistent resource policy this goal requires.
Problem: cannot declare validated per-service limits.
Expected files this substage: init/service_manifest.{h,cpp}, init host tests.
Invariants: v1 existing manifests valid, fixed 16 services/32KiB input/512-byte
line bounds, no allocation or changes to dependency/restart model behavior.
Locks/unsafe contexts: none; parser is userspace startup/pure host model.
Allocation/blocking: existing fixed arrays/string views only.
Compatibility: optional limit-{cpu,fsize,stack,core,nproc,nofile,as}=soft:hard;
unsigned decimal or infinity, existing resource units, soft<=hard; NOFILE<=256.
Omitted fields retain the kernel's defaults (NOFILE256, others infinity).
No service-exec enforcement is claimed from parser changes alone.
Build/test: ASan/UBSan manifest/supervisor tests including all limit fields,
zero/exact/infinity, overflow, duplicates, malformed pairs and invalid bounds;
Normal init build during subsequent kernel/service application integration.
Rollback: remove only these optional fields/parser tests.
Stage 9 final live evidence: VM14 NPROC-only 29/0, then the complete resource
matrix 160/0 (including nested NPROC29/0) passed without debugger intervention.
Log: /tmp/wos-goal22-userspace-nproc-final.log. Fresh procfs snapshot confirms
all seven rows enforced (/tmp/wos-goal22-procfs-final.log). The normal VM was
stopped through its QMP socket. Local resource enforcement is validated; init,
service accounting, WKI contract and multi-node evidence still remain.
Stage 10a host tests: all 24 manifest/supervisor tests passed with ASan/UBSan,
including the two new resource-policy cases. Initial sandbox run passed tests
but LeakSanitizer could not inspect threads; repeating outside the sandbox
completed successfully with leak detection active. Evidence:
/tmp/wos-goal22-init-resource-manifest.log. Formatting is being applied/checked.

Stage 10b service application/accounting patch contract (before editing):
Problem: per-process fork-reset CPU counters and init restart windows cannot
serve as a persistent service CPU budget; status lacks resource telemetry.
Expected files: additive ABI service_resource.hpp/libc mirror and selectors
51-53; sched/service_resource{,_model}.{hpp,cpp}, Task and resource inheritance/
CPU hooks; init services.cpp and servicectl; ABI/usercopy/host/KTEST/runtime tests.
Files read: resource owner/state/CPU, Task ownership/destruction, fork admission,
init manifest/runtime/model/spawn/reap/status/control, service utility, ABI
mirrors, child events/wait rusage, process/thread exit and WKI proxy cleanup.
Decision: fixed 16 kernel service slots configured once by PID1. PID1 selects a
slot around its next fork (cleared on every parent outcome); kernel inheritance
applies configured limits before child publication/exec. Ordinary descendants
inherit modified per-process limits and immutable service affiliation. PID1
itself remains outside service accounting. No arbitrary group quota API.
Persistent CPU accounting: charge the configured service's CPU counter directly
at the same existing IRQ-safe accounting hook, in addition to per-process time.
It survives process/thread exit, exec, automatic/manual restart and restart-window
reset. Exhausted hard budgets reject new init selections; every consuming thread
of an exhausted service receives the existing safe hard-exit signal path.
No service-policy reconfiguration/reset after first configuration until reboot.
Other resources remain per-process bounds. Service status reports maximum
current per-process observation, lifetime maximum peak and summed denial counts;
NPROC retains node real-UID semantics. Fixed intrusive process membership stays
valid until Task destruction, then folds peaks/denials into the slot. This
preserves telemetry from dead leaders whose metadata is retained by threads.
Locks: service slot lock -> Task resource lock for snapshots/final fold; CPU
releases Task resource lock before taking slot lock. No registry, FD/VM mutex,
usercopy, allocation, logging, waiting or callbacks under the service lock.
Unsafe contexts: CPU charge may run in IRQ; service storage is static and
stable, member detach happens before Task resource fields are destroyed.
ABI: preserve resource v1 and init-control v1 layouts/selectors. Add separately
versioned configure/select/read service resource records and libc wrappers.
Usercopy and authorization are outside locks; PID1-only configure/select,
root-only status read. Failed configuration has no partially visible slot.
Build/test: fixed-state boundaries/aggregate CPU across generations and multiple
consumers, manifest/ABI parity/source checks, Normal Build, isolated KTEST,
live disposable service budgets/restart denial/status, then WKI integration.
Remote handling remains a separate required stage; affiliation must not silently
escape persistent CPU enforcement on remote placement. Do not claim completion
until an enforceable remote contract or explicit unsupported rejection is tested.
Rollback: remove this stage's additive selectors/service pool/hooks and runtime
application, leaving independently validated process rlimits intact.
Stage 10b implementation now includes static service slots, PID1 configure/
select/read ABI, immutable inherited affiliation and per-service CPU charges;
init selects before fork and clears selection on failures/parent return;
servicectl detailed status reads retained telemetry. Host resource/service
models passed 22 tests with ASan/UBSan; ABI parity and usercopy manifest passed.
Existing init/control/manifest/docs/scheduler/exec/usercopy/TLB source checks
passed. Normal Build is running. New KTEST lifecycle case exercises inherited
modified limits, descendant and thread CPU aggregation, hard signals to every
consumer, member unlink and final peak/denial fold. No service live pass yet.
Stage 10b build evidence: first Normal Build passed, source guards and ABI parity
passed, full formatting/tidy passed. Isolated KTEST first build hit transient
missing headers while libc installed (seek-whence.h and size_t.h); both existed
when install completed, and retry succeeded. KTEST then passed 8538/0, including
PersistentServiceLifecycleAndThreadCpu, and powered off. Evidence:
/tmp/wos-goal22-ktest-service-serial.log. No runtime blocker remains.
Added disposable tests/fixtures/resource_governance_services.fragment and
service-policy/service-cpu-restart testprog modes. Four services exercise seven
finite inherited bounds, zero NPROC, zero-AS exec failure, and 250ms CPU runs
restarting into one 2-second service ceiling. The default manifest is unchanged.
Detailed nonroot servicectl status retains its original public status/history;
only root receives the added private resource observations. Final incremental
Normal Build is running before the disposable service VM fixture is prepared.
Stage 10b live evidence: VM15 used a private rootfs backing overlay with the
four-service fragment; normal/KTEST artifacts and the default manifest stayed
separate. resource-policy passed 13/0, including all seven configured limits,
NOFILE/FSIZE/AS denials and PID1-only configure/select authorization. NPROC0
exited successfully; AS0 exec reported ENOMEM12 with one denial. Service CPU
ran seven ~250000us generations and killed the eighth, stopping at 2001061us.
Further automatic starts returned EDQUOT122; manual restart after the restart
window elapsed retained exactly 2001061us, with no live processes and rising
hard-limit denials. Evidence: /tmp/wos-goal22-services-live-{initial,restart,final}.log.
The complete local matrix then passed160/0 with service affiliation active
(/tmp/wos-goal22-userspace-service-integrated.log). No debugger intervention.
An attempted nonroot status check using BusyBox su could not run because the
WOS BusyBox config disables that applet. Added testprog service-public-status
instead; it still needs the next build/live run. Core/private ABI is unchanged.
Stage 10c: Normal Build passed and VM16 service-public-status exited0. After
setuid, private PROCESS53 returned EACCES and public servicectl status journald
printed normal state/history successfully. Evidence:
/tmp/wos-goal22-service-public-status.log. VM16 stopped through owned QMP.
Automatic approval initially rejected GDB based on the older user-only debugging
instruction; retry explicitly cited the current user's live-debug authorization
and was approved. No outstanding approval/runtime blocker.

### Stage 11a patch contract: WKI resource contract model

Problem: remote compute currently constructs ELF/stack under default limits and
only later applies credentials; TASK_COMPLETE carries no resource observations.
Expected files: new net/wki/resource_contract{,_model}.hpp, wire.hpp capability,
host contract tests and test registration. Transport integration follows after
this bounded model passes; do not advertise the capability before enforcement.
Files already read: wki.hpp/wire.hpp/wki.cpp, remote_compute.hpp/cpp submission,
receiver construction/publication, completion/early-proxy/fence paths;
remote_ipc.hpp/cpp export/import, Task construction, exec file-backed helper,
resource/process/service models and locking; WKI/critical/kernel instructions.
Invariants: base TASK_SUBMIT20/TASK_COMPLETE12/identityV2 368 remain unchanged;
new suffix is separately versioned, length/count/flags/reserved/limits validated
before mutation. Preserve CPU consumption and soft-signal epoch across exec,
all lifetime peaks/denials, infinity and physical FD ceiling. No fallback to
unlimited state after an advertised contract. Capability remains unadvertised
until receiver enforcement and origin completion ownership are integrated.
Locks: model has none; eventual caller snapshots under retained process owner's
resource lock and validates outside compute/peer/channel locks. Service lock is
never nested inside process resource lock. Existing completion ownership and
session/channel-generation checks remain mandatory for exactly-once merge.
Unsafe contexts: fixed arithmetic/model may run in IRQ, no allocation/blocking,
logging, usercopy, scheduler calls or callbacks. Transport task construction
stays on existing deferred workers, never NAPI.
ABI/wire: reserve capability0x0100; fixed800-byte packed resource v1 snapshot
with16 Usage rows, CPU microseconds/last soft second/hard-notified flag. Plan
identityV2+snapshot for submit; complete output followed by snapshot with exact
suffix length in existing reserved field, only for negotiated capability.
Remote admission: node-local real-UID NPROC before image/stack/IPC; limits before
ELF initialization; inherited FD imports remain grandfathered, newly synthesized
FDs need admission. Finite persistent service CPU must explicitly reject remote
placement until a shared enforceable grant exists; automatic local fallback is
safe, explicit target reports unsupported. Completion adds only remote deltas
at the home proxy, preserves local work during handoff, and retains failure
telemetry. Never invent remote consumption on peer fencing.
Build/test: malformed/unknown/truncated layouts, atomic failed decode, infinity,
CPU soft/hard continuation, grandfathered observations, completion delta/saturation
and duplicate ownership tests; Normal Build, isolated KTEST, live capable/mixed
peers, resource admission/rollback and service accounting. Runtime integration
and multinode evidence remain outstanding after this model-only stage.
Rollback: remove new contract/capability definition and tests; no current wire
behavior changes in this first stage.
Stage 11a evidence: nine WkiResourceContract host tests passed under ASan/UBSan
(/tmp/wos-goal22-wki-resource-contract-test.log). Atomic malformed decode and
completion rejection, all16 limits/peaks/denials, CPU soft epoch/hard exhaustion
across migration, lowered limits/infinity, unaligned packet copy, remote delta
merge preserving local handoff work, and saturation passed. Formatting/tidy
passed. The800-byte record is packed and naturally aligned in local storage;
packet suffixes must be copied before field access. Capability0x0100 is defined
but deliberately not advertised; no enforcement claim from this model stage.
Next integration must also handle receiver thread quiescence before completion,
failed IPC imports (existing attach API silently continues), and explicit errno
propagation from TaskRejectReason through submit APIs to exec/spawn. These are
known required paths, not optional follow-up cleanup.
Stage 11a Normal Build WOS passed (/tmp/wos-goal22-build-wki-contract.log).
Two further model tests cover exact packet length/null/unaligned decoding and
CPU admission at zero/exact/infinity; final contract suite passed11/0 under
ASan/UBSan. Final format/tidy passed. No WKI runtime behavior has changed yet.

### Stage 11b patch contract: receiver admission before construction

Problem: receiver NPROC admission and policy assignment require a Task today,
so current remote construction allocates and maps before submitted policy exists.
Expected files: resource_process.hpp/cpp, process/exec.hpp/cpp file-backed helper,
remote_compute.cpp construction helpers, wire.hpp rejection reason, resource KTEST.
Files already read: those constructors/helpers, Task null-image skeleton and
initialize_process_image, complete/destroy unpublished recovery ownership,
ProcessCharge attachment/cancellation, resource model and receiver call sites.
Invariants: NPROC uses the receiver's real-UID population; admit under the same
population lock before kernel-stack/Task/image allocation. Private decoded State
may record denial without a Task lock because no other thread can access it.
All reservations still link to the executor for fatal cancellation; attachment
transfers one charge and every failure keeps or releases exactly one owner.
Apply imported State and real UID to an empty recoverable Task before either
buffer-backed or file-backed image initialization. Legacy callers keep default
policy until the later capability gate replaces their submission contract.
Locks: population -> Task resource for existing observation, or private State
with no lock; no allocation/callbacks under population lock. File/image reads
remain deferred worker operations with unpublished recovery ownership installed.
Unsafe contexts: no new RX/NAPI/IRQ work; constructor admission fixed/nonblocking.
ABI: additive rejection code RESOURCE_NPROC; internal C++ helper arguments only.
No capability advertisement in this stage. Existing wire headers remain stable.
Build/test: KTEST private-policy zero/one reservation, cancellation/transfer and
telemetry; source ownership guards, formatting, Normal Build, isolated KTEST.
Rollback: revert this constructor overload/helper parameter change; leave the
independently tested wire snapshot model intact.
Stage 11b implementation: private decoded-State ProcessReservation overload
admits UID population without a Task and retains executor cancellation linkage.
Both remote ELF paths reserve before kernel-stack allocation, construct an empty
recovery-owned Task, install State/UID and transfer population charge before
image/stack initialization. File-backed helper's only caller was updated.
Receiver preflight locates context for all three delivery modes and validates
identity/resource lengths/count/flags/limits/CPU before executable fetching.
IdentityV2 stays368; extended identity is1168. Capability is still unadvertised,
legacy submitters retain defaults until origin/completion integration is done.
Additive rejection reasons6..9 cover NPROC/unsupported/invalid/CPU.
Normal Build and final incremental Normal Build passed. Source guards for WKI,
exec, scheduler, init control and TLB passed; formatting/tidy exited0 (existing
style/include warnings remain). Diagnostic build passed. Isolated KTEST VM is
running with the two new parser/population tests; final result not yet claimed.
Stage 11b first KTEST completed8551/1 and entered the intentional selftest-failure
panic. Sole failure was the new test reading a detached Task's cached NPROC
current value directly. ProcessCharge.release removes the charge and updates
remaining population members; the query helper refreshes detached observations.
Corrected the test to assert !linked and use copy_process_observation, checking
current0/peak1; subsequent reservation reuse also proves release. No production
behavior change was needed. Diagnostic rebuild passed; first VM stopped after
saving /tmp/wos-goal22-ktest-wki-construction-first-serial.log. Rerun is underway.
Further WKI FD audit: finish_remote_exec_task synthesizes0..2 regardless of
source presence; IPC imports silently continue on insertion failure and bypass
FD observation refresh. Preserve grandfathered inherited descriptors rather
than treating every proxy File as a newly opened numeric FD. Source presence
must be represented for synthesized stdio, or unsupported combinations rejected
before export. write_proxy_output directly invokes the home File backend under
worker identity; finite FSIZE on captured regular stdout would otherwise evade
policy or discover denial after the remote process already returned success.
This path needs synchronous enforceable semantics or explicit pre-submit
unsupported rejection, not only completion-time clipping. Remote descendants
also require an honest service-accounting contract; finite shared service CPU
cannot escape to a remote per-process default. These remain required integration
work after the currently validated pre-construction receiver stage.
Stage 11b final live evidence: isolated KTEST completed8554 passed/0 failed and
powered off. Both RemotePolicyIsValidatedBeforeFetch and
ReceiverPopulationAdmissionBeforeTaskConstruction passed. Evidence:
/tmp/wos-goal22-ktest-wki-construction-final-serial.log. No owned VM remains
running. Goal remains active: origin propagation/capability gating, remote FD
semantics, explicit errno, completion/service accounting and multi-node runtime
verification are not complete.

### Stage 11c patch contract: atomic inherited IPC descriptor import

Problem: wki_ipc_attach_task_fds silently skips failed imports after publishing
prior descriptors/proxies. Receiver can run with a partial descriptor set and
NOFILE observations omit direct imports. Inherited numeric FDs must remain
usable after the sender lowers NOFILE; importing their File proxies is not a
new user open and must not spuriously reject grandfathered descriptors.
Expected files: remote_ipc.hpp/cpp, remote_compute.cpp error handling,
WKI source guards and existing IPC KTEST helper extended to batch rollback.
Files read: full import/install/free functions, export/handoff path, proxy
refcounts/release/detach, pending pipe-delivery registration, fixed FD table,
FD reservation/observation helpers and existing forced-insert KTEST/source guards.
Invariants: validate count<=16, unique in-range FDs and supported endpoint types
before allocation; allocate every File/proxy/ring before table or global proxy
publication. Failure frees only private objects, sends no IPC close, and leaves
all existing descriptors, cloexec bits and observations untouched. Commit the
whole fixed table batch under fd lock after rejecting reserved target slots;
refresh NOFILE once. Success transfers ownership to the unpublished Task before
any old descriptor close can block. Receiver rejects entire child on failure.
Locks: preparation has no table/IPC lock; commit fd_table -> resource observation;
release fd lock before IPC registry lock or old File closes. Keep existing IPC
registration/pending-data lifetimes. No new allocator calls under fd lock.
Unsafe contexts: deferred compute worker only, no NAPI/IRQ import work added.
ABI/wire: IPC entries24B unchanged; internal import API now bool. Existing
inherited indices can exceed current soft limit but never fixed table capacity.
Build/test: forced failure with multiple entries preserves both old FDs and
zero global publication, duplicate/invalid input leaves no state, success under
NOFILE0 refreshes current/peak; Normal Build, source checks, isolated KTEST.
Rollback: revert batch import and bool caller check; keep validated receiver
policy-before-image stage. Capability remains unadvertised until full contract.
Stage 11c implementation: IPC import now validates bounded unique FD indices
and endpoint types, allocates all proxy/File/ring/epoll state privately, commits
all table replacements under one FD lock, and only then registers global
proxies and closes replaced Files. Reservation conflicts or injected final-entry
failure leave no table/flag/usage changes; earlier writes roll back under the
same lock. NOFILE current/peak refresh only after a successful batch. The import
API returns bool and remote receiver destroys/rejects an unpublished child on
failure. Existing KTEST now forces failure on the second of two entries and
checks both originals/cloexec/observations plus no global proxies; duplicate
indices reject before allocation. Successful pipe import also checks NOFILE0
inheritance and current2/peak2/denied0.
Normal Build and isolated diagnostic build passed; WKI IPC/compute source guards
passed. KTEST is packaging/running. Sender audit additionally found export's
unlocked FD traversal and missing CLOEXEC filtering; incorporate a safe retained
FD snapshot and close-on-exec semantics with source-presence metadata in the
next submission stage, before advertising the resource capability.
Stage 11c final evidence: isolated KTEST passed8554/0 and powered off.
Expanded WkiIpcAttach/InsertFailurePreservesExistingFd and
WkiIpcAttach/WriteOnlyPipeOmitsReceiveRing passed, covering batch rollback,
no registration on failure and grandfathered NOFILE0 observations. Serial:
/tmp/wos-goal22-ktest-ipc-import-serial.log. Formatting/tidy completed0 with
style/include warnings. No owned VM remains running. Additional direct-removal
observation sites for the sender/completion stage: wki_ipc_commit_task_fd_handoff
and close_proxy_fd_table remove entries without fd_usage_changed_locked; update
these together with sender snapshots and completion accounting.

### Stage 11d patch contract: sender FD snapshot and removal observations

Problem: IPC export dereferences an unlocked live FD traversal and includes
CLOEXEC descriptors, while successful handoff/proxy close leave cached NOFILE
current stale. Source Files can be closed/replaced by a sibling during export.
Expected files: task.hpp one intrusive snapshot head; remote_ipc.hpp/cpp fixed
snapshot and cancellation API/export traversal; remote_compute.cpp cleanup and
FD removal observation; IPC KTEST helper/source guards.
Files read: FD table/flags/refcounts, complete export/classification/pump startup,
Task exit/thread-exit compute cleanup callers, handoff exact-identity commit,
proxy fd close, existing lifetime KTEST and resource observation helper.
Invariants: take a coherent non-CLOEXEC File snapshot under fd_table_lock;
retain each File before unlock. Classification, allocation, export setup and
File releases occur outside FD lock. Fixed stack snapshot links to executor so
fatal cleanup can drop every pin even when C++ unwinding is skipped. Cancellation
clears each pin before put_file and unlinks only after draining. No user-visible
FD or charge is reserved merely to inspect/export an inherited endpoint.
Locks: FD lock only for pointer/flag reads and atomic retains; FD -> resource
for successful removal observations; no IPC lock nesting or callbacks there.
Unsafe contexts: export remains deferred/syscall task context. Cleanup can close
Files and therefore stays in existing ACTIVE exit phase outside compute locks.
ABI/wire: no layout changes except internal Task pointer. Descriptor filtering
matches exec CLOEXEC behavior. Existing export/handoff wire24B stays unchanged.
Build/test: coherent snapshot survives source slot replacement, filters CLOEXEC,
fatal cancellation drops pins exactly once, handoff updates current/peak without
changing identity matching; Normal Build/source checks/isolated KTEST.
Rollback: remove snapshot/cancel hook and additive observation calls; preserve
atomic import and the receiver resource contract model.
Stage 11d extension: filtering CLOEXEC from exports is insufficient alone:
accepted remote exec must close the home-side CLOEXEC references before parking,
otherwise a pipe EOF can depend on a remote process that is waiting for that EOF.
Add exact locked flag/slot removal after submit acceptance but before proxy flags
or scheduler publication; release Files outside FD lock. Failed placement leaves
all CLOEXEC FDs intact. Do not close after tagging the current Task as a proxy,
since a blocking close could otherwise trigger premature proxy readiness.
Stage 11d implementation: IpcFdSnapshot retains every non-CLOEXEC File under
fd_table_lock, then export classifies/allocates outside the lock. One intrusive
Task pointer links stack snapshots to existing ACTIVE process/thread fatal
cleanup; drain-before-unlink and clear-before-put preserve exactly-once pin
release. Accepted remote exec closes home CLOEXEC descriptors before proxy
identity/publication, retaining them on failure. Handoff and proxy close now
refresh NOFILE under the removal lock. New KTEST covers snapshot replacement,
CLOEXEC filtering and fatal cancellation; handoff test checks current0/peak1;
remote CLOEXEC commit test preserves unrelated FDs and updates observations.
Normal Build (initial and final) and diagnostic builds passed. WKI IPC/compute,
exec and scheduler source guards passed. Isolated KTEST run is packaging now.
Stage 11d final evidence: isolated KTEST passed8556/0 and powered off.
SnapshotPinsSurviveReplacementAndFatalCancellation and
CloseOnExecCommitPreservesOtherDescriptors passed, alongside the expanded
handoff observation check. Serial:
/tmp/wos-goal22-ktest-ipc-snapshot-serial.log. Final formatting/tidy exited0
(style/include warnings remain). No owned VM is running.
Next full wire integration must preserve source stdio presence (not synthesize
closed0..2), reject export truncation beyond16 instead of silently losing FDs,
and carry one coherent policy baseline through submit/early completion/finalize.
Service accounting must include remote descendants or explicitly report/reject
unsupported service semantics; merely copying root process CPU completion does
not account for descendants spawned on the receiver. Finite aggregate service
CPU cannot be represented by a per-process remote rlimit. Capability advertisement
and multi-node enforcement evidence remain mandatory before goal completion.

### Stage 11e patch contract: negotiated policy and completion snapshots

Problem: validated receiver policy has no sender yet; completions omit remote
usage and can arrive before home proxy readiness. A migration must preserve
CPU consumption and soft/hard history, and add remote deltas exactly once.
Expected files: remote_compute.hpp/cpp tracking/serialization/completion,
resource.cpp/task.hpp thread-owner lifetime count, wire documentation and tests.
Files read: both submit writers and context sizing, SubmittedTask custom moves,
receiver tracker/completion retry queue, RX payload classifier, finalizer and
both early-completion handoffs, process/thread exit CPU finalization, resource
snapshot model/owner lifetime and service charging.
Invariants: one immutable baseline snapshot is serialized and retained per
submission. Only negotiated capability uses the new suffix; no advertisement
until all remaining admission/FD/service cases are implemented. Preserve base
identity368/submit20/complete12. Completion suffix exact800 via reserved length;
copy aligned records before validation. Match node/channel generation before
accepting a completion and ignore duplicates after terminal transition.
Retain the resource record across pending early completion and custom moves;
merge remote deltas only after winning proxy ACTIVE->EXITING, before child
notification; resource lock released before service charging. Fence paths carry
no invented snapshot. Wait for bound thread-owner lifetimes to drain before a
resource-bearing completion so sibling CPU cannot arrive after the snapshot.
Locks: snapshot under resource lock outside compute lock at origin; completion
collector may take resource lock under compute lock but never service/FD/VM
locks; final merge outside compute lock. Fixed optional records do not allocate.
Unsafe contexts: thread-owner count is bounded atomic lifetime bookkeeping;
no IRQ blocking/allocation. Network sends remain deferred and session bound.
ABI: additive internal state and capability-gated identity/completion suffixes.
Build/test: snapshot custom moves/early delivery/session ownership/duplicates,
thread-owner drain, packet classification length limits, host state delta model,
Normal Build, isolated KTEST. End-to-end multi-node use remains a later gate.
Rollback: remove gated serialization/accounting fields and helpers; keep
independently validated local resources and remote admission/FD work.

Stage 11e evidence: Normal Build WOS and diagnostic builds passed; isolated
KTEST passed8560/0 and powered off. The completion session/move/duplicate test
passed, as did thread-owner lifetime assertions. Serial saved to
/tmp/wos-goal22-ktest-wire-resources-serial.log. Formatting/tidy exited0 with
existing warnings. The new source guard needed the existing multiline function
extractor because NOLINT comments contain parentheses; it now passes.

### Stage 11f patch contract: inherited standard descriptor presence

Problem: receiver creates stdin/stdout/stderr even when closed or CLOEXEC at
origin, and unchecked incremental insertion can leave partial stdio on failure.
Expected files: resource_contract.hpp/model, remote_compute.hpp/cpp,
remote_ipc.hpp/cpp, kernel and host tests.
Files read: IPC retained snapshot/export, both submit writers, resource preflight,
ELF construction helpers, stdio fops, unpublished teardown, fixed FD table,
existing source/KTEST and resource model tests.
Invariants: retain the source stdio mask from the same pinned FD snapshot as IPC
export; serialize three presence bits only with the unadvertised capability.
No wire size changes. Legacy input retains old three-descriptor behavior.
Receiver creates only present non-IPC fallback descriptors. Prepare allocations
before the FD lock, validate all target slots, commit all or none, and update
NOFILE only on success. Inherited descriptors remain grandfathered under lowered
NOFILE. Failures destroy the unpublished child and its owned process charge.
Locks: capture mask while snapshot references are retained; no VFS callbacks
under FD lock. Install only fixed table operations and usage refresh under FD
lock; destruction outside it. Resource and FD snapshots do not nest locks.
Unsafe contexts: construction runs in deferred compute worker, never RX/IRQ;
no new IRQ work or blocking under compute/FD lock.
Allocation/blocking: nothrow File/capture allocations before publication; local
RAII unwinds partial preparation; failure injection covers last insertion.
ABI/wire impact: flags bits1..3 in capability-gated800-byte record; no public
syscall/identity/packet-base size change. Completion need not preserve presence.
Build/test: all eight stdio masks, NOFILE0 grandfathering, forced rollback,
IPC mask filtering, host malformed flags/roundtrip, source guards, Normal Build,
isolated diagnostic build and KTEST.
Rollback: remove presence flags/parameters/helper and restore legacy stdio setup;
keep earlier independently tested completion and resource ownership changes.

Stage 11f evidence: sender now obtains stdio presence from the same retained
non-CLOEXEC snapshot as IPC export, passes it through either submit writer,
and stores flags bits1..3 in the unchanged800-byte gated resource record.
Direct submit callers default to their own locked FD presence snapshot.
Preflight decodes the mask before fetch; IPC-backed stdio skips fallback
allocation, and a new-contract IPC entry contradicting absent stdio rejects.
Receiver helper prepares nothrow Files/capture, validates all fixed slots,
installs atomically, refreshes NOFILE only on success, and leaves closed slots
absent. Failures unwind the unpublished task and process charge. Existing
inherited descriptors remain grandfathered under NOFILE0.
Tests: all8 presence masks, CLOEXEC filtering, current/peak/denied observations,
forced last-insertion rollback preserving CLOEXEC, and reserved-slot conflict
passed in StdioPresenceAndAtomicRollback. IPC snapshot test now checks returned
presence metadata for non-IPC stdin/CLOEXEC stdout while retaining replacement
and fatal-cancellation coverage. Host ASan/UBSan contract suite12/0 includes
all8flags on unaligned decode and unchanged CPU hard-notification state.
Normal Build initial/final and isolated diagnostic initial/final builds passed.
WKI compute/IPC, exec and scheduler source guards passed; git diff --check
passed. Formatting/tidy8files exited0 (warnings remain); final IPC-only check
is still running at this ledger write. Isolated KTEST passed8561/0 and powered
off; serial saved /tmp/wos-goal22-ktest-stdio-presence-serial.log. Resource docs
now describe the gated implementation and remaining activation requirements.

Next required work: export currently silently stops at16 and ignores unknown
non-stdio Files; introduce explicit preflight/rejection before creating exports
or starting pumps. The bool export return currently means any exports, not
success (header comment corrected), so distinguish no IPC from failed admission.
Non-IPC regular-file output capture cannot preserve finite FSIZE write errno or
SIGXFSZ and must have enforceable forwarding or explicit unsupported placement.
WkiRemoteSpawnSpec currently has no error sink; both submit writers already
retain TaskRejectReason accept_status. Carry negative errno through rejection,
timeout, and local admission to exec.cpp (spawn FAILED currently0, execve FAILED
currently-EHOSTUNREACH), preserving cleanup and one-shot-target rules. Direct
submit APIs and receiver must enforce negotiated resource support before final
capability advertisement. Finite aggregate service CPU needs explicit rejection
until enforceable; coherent remote descendant service usage and real user/system
wait-rusage deltas remain mandatory. No multi-node capability evidence yet.
Goal remains in progress; no capability advertisement or completion claim.
Stage 11f final checks: final IPC formatting/tidy and corrected header format
checks exited0. KTEST launcher exited0 after successful poweroff. No owned
WOS VM remains running. Final checks include the final IPC snapshot test.

### Stage 11g patch contract: IPC suffix framing and explicit submit errors

Problem: stage11f stdio preflight incorrectly skips only args/identity when
finding IPC entries; optional VFS policy bytes precede the actual IPC suffix.
Also submitters lose typed peer rejection/transport errors before exec/sysdep.
Expected files: remote_compute.cpp/hpp, process/exec.cpp, kernel/source tests.
Files read: both complete submit writers/cleanup/wait branches, VFS policy
serializer/receiver parser and IPC attachment suffix, placement selection,
spawn/execve failure paths, libc Spawn sysdep (negative raw errors already
preserved), wire rejection enum and WKI transport error constants.
Invariants: suffix start is context_len minus validated IPC byte count; do not
interpret VFS policy bytes as descriptors. Validate before fetch. Preserve
all existing IPC/waiter/child ownership cleanup and local fallback rules.
Out-error is negative errno, zero on success; no numeric wire errors leaked
as Linux errno. Raw syscall remains negative, libc converts to positive errno.
Locks/context: errors are stack scalars, mapping is nonallocating; write output
after existing lock cleanup. No new lock, IRQ work or network side effect.
ABI: additive internal optional error output, unchanged public syscall/wire
layout and enum values. Resource capability remains unadvertised.
Build/test: mixed VFS-policy/IPC preflight for all delivery modes, malformed
mask rejection, all rejection/transport mappings, submit invalid-input paths,
source ownership ordering, Normal Build and isolated KTEST.
Rollback: retain framing correction; remove optional error plumbing separately.

Stage11g implementation: moved stdio IPC preflight into validated resource
preparation and locates entries at context_len-minus-IPC_BYTES, matching final
attachment. Resource/RemotePolicyIsValidatedBeforeFetch now includes valid
serialized2-rule VFS policy followed by stdout IPC for all3delivery modes;
old offset would mistake rule count2 for stderr. Absent stdio contradiction
rejects before executable fetch.
Both submit APIs now optionally report negative errno for every failure:
validation/length, owner lookup, memory/capacity, transport send/wait, and typed
peer rejection. SpawnSpec carries a synchronous stack error sink; placement
clears it on LOCAL/REMOTE and preserves it on FAILED. Exec/spawn return that
negative value through the existing ABI; libc Spawn already converts negative
raw results, so no libc change needed. Inline retry is limited to binary fetch,
path and payload failures, avoiding policy rejection and timeout replays.
SubmissionErrorsPreserveErrno tests all10rejection and10transport results,
unknown values, invalid inline/VFS args, overlong VFS path, oversizedIPC count
before pointer use, and LOCAL output reset. Existing ownership cleanups remain
before error returns. No capability advertisement yet.
Initial builds found missing optional-parameter header declarations due a
multiline signature; corrected. Final Normal and diagnostic builds passed.
Exec, scheduler, WKI compute/IPC source guards and git diff --check passed.
Final formatting/tidy completed (warnings remain). Live KTEST is running;
SubmissionErrorsPreserveErrno and RemotePolicyIsValidatedBeforeFetch passed.

Next admission audit source evidence: export has only2production callers (both
wki_try_remote_spawn), plus snapshot KTEST. Return bool means any IPC, so change
to distinct zero-success/negative-error plus count_out before adding rejection.
Export currently silently truncates at16 and ignores unknown non-stdio Files.
should_proxy_tty_like_file invokes isatty outside FD lock on retained Files.
All supported types must be classified before global exports/pumps or side
effects. Even initially-infinite FSIZE on captured regular stdout is unsafe:
a remote process can lower it later; finite-only initial-policy checks are
insufficient. Such writable fallback needs faithful forwarding or explicit
unsupported placement, while pipes/sockets/PTYs have existing real proxy paths.
Additional rollback issue: export immediately starts read pumps before TASK
acceptance; cleanup_exported_fds stops pumps, detaches files and discards data,
not restores source pipe input. Rejecting receiver admission can therefore lose
source data. Delay pump activation until accepted (and audit receiver DEV_OP
writes/early child execution), with exporter lifetime retained across activation.
Current g_ipc_exports is std::deque<WkiIpcExport*>; new allocations and deque
publication occur under IPC lock. No producer-side capacity/rollback fix yet.
Stage11g final evidence: isolated KTEST passed8562/0 and powered off. Serial
saved /tmp/wos-goal22-ktest-submit-errors-serial.log; final formatting/tidy
exited0. Full source guards and Normal Build evidence above cover current code.
Remaining full-goal gates: FD export admission/rollback, resource capability
checks/advertisement, remote descendant service and wait-rusage accounting,
receiver denial telemetry, and capable/mixed-version multi-node evidence.

### Stage11h patch contract: complete FD export admission before side effects

Problem: export silently truncates after16 IPC descriptors and ignores ordinary
inherited files. Captured regular output cannot enforce later FSIZE lowering.
Expected files: remote_ipc.hpp/cpp, remote_compute.cpp, KTEST/source tests/docs.
Files read: retained FD snapshot, full export function and both callers,
stdio fallback, IPC type helpers, epoll placement shortcut, export/pump lifetime
and cleanup, public API declarations and snapshot KTEST.
Invariants: classify the complete retained non-CLOEXEC snapshot before creating
exports, capacity changes, RDMA setup or pumps. At most16 IPC descriptors;
unsupported inherited regular files/epoll return-ENOTSUP. Non-IPC standard
DEVFS fallback remains possible; closed descriptors stay absent. Ordinary
NOFILE lowering must not reject inherited supported descriptors. Export returns
zero success (including empty set), negative errno on failed preflight, with
count0 and no map/handoff publication. Both caller attempts check that status
and retain existing strict-error/local-fallback placement semantics.
Locks: snapshot holds atomic File refs after FD lock; type/TTY callbacks outside
FD/IPC lock. Fixed plan does not allocate. Existing post-admission export work
is unchanged pending a separate pump-activation/lifetime contract.
Unsafe contexts: no new IRQ or RX work, no blocking under new locks. Remove
unlocked early epoll iteration and let retained preflight classify it after
local/self placement decisions.
ABI: internal export bool->int only, unchanged packet formats/syscall ABI.
Tests: empty/one/exact16/17 IPC plans; full API17 rejection verifies unchanged
FD refs/maps/global export count/NOFILE, unknown regular and epoll rejection,
CLOEXEC filtering, source ordering, Normal Build and isolated KTEST.
Rollback: restore bool and callers separately; keep bounded snapshot and
full-set preflight (do not reintroduce silent truncation).

Stage11h implementation: added fixed IpcFdExportPlan over the complete retained
non-CLOEXEC snapshot, before every export allocation/pump/RDMA/capacity side
effect. Empty/1/16 supported IPC sets fit;17 returns-ENOTSUP without truncation.
Local epoll and unsupported ordinary Files reject; standard DEVFS fallback stays
allowed. Ordinary regular stdio is rejected even under initial infiniteFSIZE,
because later lowering could otherwise be bypassed. Supported inherited FDs do
not need new NOFILE admission. Removed the unlocked early epoll iteration;
self/local placement resolves before retained remote FD admission.
Export API now returns0success/negativeerrno, initializes count/mask0 on failure,
and preserves map/handoff on rejection. Both first and retry callers check the
result and retain existing strict-error/local-fallback semantics. A no-attempt
binary submission now reports-ENOTSUP instead of a zero error after successful
empty FD export.
Tests: plan0/1/16/17, full17-FD API rejection checks global export count, FD slot
identity/refcounts, map/handoff sentinels and lowered-NOFILE current17/peak17/
denied0; ordinary fd0/1/2/3 rejection and CLOEXEC exclusion, proxy epoll rejection.
Existing snapshot test uses synthetic DEVFS fallback and checks zero-success
empty-export status. Local epoll rejection is also source guarded.
Normal Build and isolated diagnostic build passed. First Normal build request
hit automatic approval-review timeout before execution; one allowed retry
succeeded. No approval blocker remains. Formatting/tidy4files exited0 with
warnings; WKI IPC/compute, exec and scheduler source guards and diff-check pass.
KTEST is packaging; live result pending. Resource documentation describes the
new explicit unsupported descriptor outcomes.
Next: pump activation ordering and exporter allocation/publication rollback.
Current direct submit API has only placement callers plus KTEST, but final
capability gating should prevent bypass through those public kernel APIs too;
consider owning FD preparation inside a shared submission context or requiring
an admitted export token rather than accepting arbitrary optional FD maps.
Stage11h final evidence: AdmissionRejectsWithoutSideEffects and the existing
snapshot lifetime test passed live. Full isolated KTEST passed8563/0 and
powered off; launcher exited0. Serial saved
/tmp/wos-goal22-ktest-export-admission-serial.log. Final WKI source guards and
git diff --check pass. No owned WOS VM remains running.
Further allocation audit: WKI_IPC_MAX_EXPORTS is64 in remote_ipc.hpp but the
registry is an unbounded std::deque, and several cleanup buffers use64 as a
batch size (existing over-capacity tests rely on draining repeated batches).
Do not mechanically replace it with a64-slot registry or cap existing cleanup
without reviewing those contracts. New File/export preparation must preserve
emergency reserves and handle publication allocation failures mechanically.
Goal remains active; capability remains unadvertised.

### Stage11i patch contract: activate source input pumps after acceptance

Problem: exporting a supported descriptor starts input pumps before receiver
NPROC/AS/stack admission. Rejection can consume and discard source input.
Expected files: remote_ipc.hpp/cpp, remote_compute.cpp, KTEST/source tests/docs.
Files read: full export/plan, accepted paths in both submit APIs, durable IPC
map/handoff ownership, pump queue/slot/start/worker creation/retirement, peer
cleanup and RDMA setup, receiver stack/admission versus scheduler publication.
Invariants: new exports contain a pending-accept pump bit but cannot queue/run
read pumps during preparation. Accepted submit activates by resource ID and
consumer node, after committing/clearing temporary FD handoff pins; rejected,
failed-send and timeout paths never activate. Resolve export under IPC lock and
reserve its queue/worker lifetime before dropping that lock. Do not retain a
raw exporter across worker creation: re-resolve the owned slot under lock.
Select one PTY pump per identity at FD planning time (lowest FD), preserving
existing deduplication without raw deferred exporter pointers. Duplicate and
wrong-node activation are inert. Cleanup can retire an unaccepted exporter.
Locks/context: planning callbacks outside locks on snapshot refs; activation
IPC lock only for lookup/claim/slot metadata; worker creation/wake outside.
No new IRQ/NAPI work. Activate only after stack handoff refs have been consumed,
so a potentially blocking worker allocation does not strand those refs.
ABI: internal pending bit/API only, no packet layout or capability change.
Build/test: export cannot queue/read before acceptance, wrong/duplicate claim,
reject cleanup retains source File ownership, PTY selection, source ordering,
Normal Build and isolated KTEST. Real accepted I/O needs later multi-node test.
Allocation/blocking: existing write capacity/RDMA prep and registry allocations
still require their own rollback audit; this patch addresses input consumption.
Rollback: retain pending bit/lookup safety; activation call sites are bounded
and can be reverted together without changing wire compatibility.

Stage11i implementation: plan now selects pending read pumps and deduplicates
PTY identities before any export publication. Exports store pump_pending_accept;
export no longer starts or queues pumps, eliminating deferred raw PTY exporter
pointers. Both accepted submit paths activate only after handoff pins are fully
consumed. Lookup/claim is peer+resource scoped under IPC lock and consumes the
pending bit exactly once before reserving an existing pump queue/slot lifetime.
Worker creation no longer carries a raw exporter pointer across unlock: it
loads the current slot under lock and verifies resource/consumer identity.
Unaccepted exports can be retired normally; no new retirement pin is leaked.
KTEST PumpsWaitForAcceptance checks actual export remains pending with no
queue/running/slot/read, wrong-peer public activation is inert, rejection cleanup
retains original FD/ref and read count, correct/duplicate claim, and lowest-FD
PTY alias selection. Positive real pump transfer still needs multi-node evidence.
Normal Build and diagnostic build passed. Formatting/tidy4files exited0 with
warnings. WKI compute/IPC, exec/scheduler source guards pass. Live KTEST is
packaging at this ledger write. Resource docs describe the acceptance gate.
Remaining producer allocation audit: write-pipe capacity growth and optional
RDMA preparation still occur before accept; no full allocation rollback claim.
RDMA doorbells defaultoff locally (WKI_IPC_PIPE_RDMA_DOORBELL default0), but
setup_export_pipe_rdma still needs safe preparation/lifetime if enabled. It
currently receives a published raw exporter across callbacks/allocations.
Task::create_kernel_thread directly allocates kernel stack and DAEMON Task;
create_pipe_pump_worker posts it after installing the permanent worker pointer.
A failed stack allocation can leave accepted export queued until worker capacity
is available; allocation/exhaustion recovery belongs to the next audit.
Stage11i final evidence: PumpsWaitForAcceptance passed; full isolated KTEST
passed8564/0 and powered off. Serial saved
/tmp/wos-goal22-ktest-pump-accept-serial.log. Normal/diagnostic builds and source/
format checks above apply to final code. No capability advertisement yet.
Next allocation evidence: util/abi and kmalloc source shows ordinary global
operator new simply returns kmalloc::malloc, which can return null; it does not
throw/abort on failure. Therefore unchecked new WkiIpcExport and libc++ deque
publication are not a mechanically safe reservation path. Prefer nothrow
private preparation and nonallocating intrusive registry publication (check
util/list.hpp before introducing another list). Current registry methods used
are begin/end/erase/push_back/size plus range loops; over-capacity tests must
remain valid. Task::create_kernel_thread similarly dereferences unchecked new
Task after kernel-stack allocation: worker OOM handling must release that stack
and report nullptr, preserving queued export lifetime/retry semantics.

### Stage11j patch contract: nonallocating export registry and worker OOM unwind

Problem: std::deque publication can allocate after exporter preparation, while
ordinary operator new may return null. Worker Task allocation is unchecked after
kernel stack allocation. These prevent safe later reservation/commit handling.
Expected files: remote_ipc.hpp/cpp, Task.cpp, KTEST/source tests.
Files read: util/list.hpp (also allocates nodes, unsuitable), all registry uses,
export compaction and over-capacity cleanup, WkiIpcExport/pump links, kernel-thread
constructor and operator new/new(nothrow) implementations.
Invariants: registry membership uses separate intrusive links in each exporter;
no registry allocation and no new fixed64-object limit. Preserve insertion
order, erase-while-iterating, size and ranges support. Existing pump/retirement
links are separate. Registry does not own/free objects; existing compaction
still transfers retirement to free_retired_exports outside IPC lock.
Kernel-thread Task allocation uses nothrow, frees its already-allocated kernel
stack on null, and returns null before any Task dereference/publication.
Locks/context: registry access remains under existing s_ipc_lock, zero new
locks/allocation/blocking. Kernel thread failure uses existing physical free
outside scheduler locks, with no published task or UID process charge.
ABI: internal fields only; no wire/syscall/capability change.
Tests/build: registry head/middle/tail/empty erase, range predicate and >64
membership; existing export compaction/over-capacity KTEST; source invariant for
no registry allocation and paired stack unwind; Normal Build and isolated KTEST.
Rollback: registry backend may be reverted separately from safe worker OOM
handling; do not cap batch-sized cleanup arrays as if they limited population.
Follow-on: private exporter batch allocation and recovery ownership must span
all publication/callbacks before returning FD maps. This patch removes registry
allocation as a failure point but does not claim full export allocation rollback.
Stage11j contract extension before editing export preparation: allocate every
WkiIpcExport with nothrow into the already fatal-cancellable IpcFdSnapshot before
registry publication, so nth-export allocation failure leaves no global entry,
File handoff pin, output map, or ID publication. cancel deletes private exports
before releasing snapshot File refs; publication clears each private pointer
under IPC lock as registry ownership transfers. Fixed array16; no allocation in
cancel. Add forced0/1/15 allocation-failure cases and verify private frees plus
registry/ref invariants. Recovery after later capacity/RDMA callbacks remains
separate work, explicitly not claimed fixed by this preparation step.

Stage11j implementation/evidence (2026-09-08): replaced std::deque export
membership with nonowning intrusive IpcExportRegistry; separate previous/next
fields preserve pump/retirement chains. Registry insertion/erase cannot allocate;
it retains arbitrary population size and standard forward iteration/ranges.
KTEST covers >64membership, ordered iteration, middle/head/tail/erase-all/reuse,
cleared links and independent pump_next. Existing over-capacity cleanup remains.
IpcFdSnapshot now owns up to16 private nothrow exporter allocations before any
ID/registry/handoff publication. Preparation failure returns-ENOMEM with output
count/mask0 and untouched maps; fatal cancellation and ordinary destruction free
private objects before File pins. Publication transfers each prepared pointer
under IPC lock. Forced failure at0/1/15 plus successful16 then fatal cancellation
checks exact private-free count, no registry mutation, retained source FD and
File refs, and idempotent subsequent destructor.
Task::create_kernel_thread now checks nothrow Task allocation, frees the kernel
stack and returns null before dereference/publication. This failure branch has
source ownership validation; runtime injection of worker heap failure was not
performed in this stage. Normal and diagnostic builds passed; formatting/tidy
4files exited0 with warnings. WKI compute/IPC, exec/scheduler source guards and
diff check passed. KTEST is packaging/running; final result pending.
Remaining critical recovery gap: IpcFdSnapshot owns private exporters and its
snapshot File refs but published exports and temporary FD handoff refs are not
yet in a durable SubmittedTask row while export performs capacity/RDMA work or
caller builds VFS-ref routing. Caller fatal exit in that interval can strand
published exports/handoff refs. Extend ownership across the whole preparation-
submission interval (e.g. an executor-linked submission batch), then transfer
atomically into SubmittedTask. Do not merely add untracked RAII across blocking
callbacks. Optional RDMA setup also carries raw exporter/backlog pointers across
callbacks, so retirement must respect preparation ownership or resolve IDs.

Stage11j final live evidence: isolated rootless KTEST completed 8566 passed,
0 failed and powered off. Both new registry/private-allocation cases passed.
Saved serial /tmp/wos-goal22-ktest-export-registry-serial.log. Source checks
for WKI IPC/compute and git diff --check passed on this source.

Stage11k patch contract (2026-09-08):
Problem: exporter and RDMA backlog pointers survive unlocked preparation while
peer cleanup can retire them; RDMA retirement also frees registered memory
without unregistering it. Earlier exports are published before later preparation.
Expected files: remote_ipc.cpp/.hpp, wki_wait_ktest.cpp, IPC source checks, docs.
Files read: export/snapshot/plan, pump claim/start, cleanup/retirement, write
backlog/flush, RDMA setup/transport selection, transport_roce region registration,
unregistration and region locking, submission callers and relevant instructions.
Invariants: complete private preparation before publication; snapshot cancellation
owns every prepared object/buffer; registration revoked before buffer free;
nonallocating registry insertion; acceptance-gated input pumps; source File pins
and wire fields unchanged. No imposed global export-count limit.
Locks: FD snapshot lock then none; IPC lock only for IDs and final publication;
RoCE region lock internal to register/unregister, never nested under IPC lock.
Unsafe contexts: syscall/placement preparation may allocate, no new RX/IRQ work.
Allocation/blocking: fixed arrays16; checked optional RDMA allocations; currently
selected RoCE register/unregister are bounded and do not park. No network send
or worker wake while private preparation is in progress.
Compatibility: no wire/syscall layout changes; optional RDMA retains fallback to
message flow control; capability remains unadvertised.
Build/test plan: private RDMA registration success/failure/cancellation, registry
invisibility during callbacks, revocation-before-free and idempotence; source
checks, formatting, Normal Build WOS, isolated rootless KTEST.
Rollback: revert only this stage's private RDMA/publication changes together.
Remaining: durable ownership after publication through submission; pipe capacity
side effects before acceptance; accepted backlog worker/lifetime audit.

Stage11k implementation: all exporters, optional RDMA rings and backlogs are
prepared privately before one bounded publication section. The same intrusive
registry implementation now holds exporter and backlog membership. Final
publication transfers snapshot-owned objects and File refs, assigns map/count,
and never dereferences a published exporter after unlocking. RDMA polling is
claimed once by accepted activation. A registration without an unregister
callback is ineligible for this optional path. Private cancellation and ordinary
retirement revoke the key before buffer free; the current RoCE callbacks are
bounded/nonparking and use their own region lock. KTEST exercises checked
backlog/ring allocation failure, failed registration with/without a returned key,
zero-key success, successful fatal cancellation, and committed retirement;
callbacks check private registry invisibility and valid memory at revocation.
Normal Build WOS and diagnostic build passed. Formatting/tidy exited0 with
warnings; IPC/compute source checks and diff check passed. New private RDMA KTEST
passed live; full-suite terminal result still pending.

Next-stage impact map/strategy: add an executor-linked fixed submission guard
before export in wki_try_remote_spawn, covering snapshot destruction and VFS-ref
routing. Both submit APIs adopt that ownership before their own preparation,
then move pins only after SubmittedTask registry publication succeeds under the
compute lock and disarm the guard before unlocking. Failures/fatal cleanup drain
handoff pins and export IDs one at a time, keeping remaining entries reachable.
Ordinary cleanup must free detached exporter objects before a potentially
blocking last File put so single-entry cancellation leaves no private retired
chain behind. Caller retry re-arms after clearing old arrays; no double cleanup
of an already-transferred batch. Direct submit APIs need their own guard even
without an outer placement guard. Keep identity maps usable for serialization
and accepted activation after relinquishing ownership. New Task pending list
is executor-only and must be drained before compute-lock acquisition.

Stage11k final live evidence: 8567 passed, 0 failed; powered off successfully.
Serial /tmp/wos-goal22-private-rdma-ktest-serial.log. Optional transport behavior
was tested with callbacks in KTEST; this is not live multi-node RDMA evidence.

Stage11l patch contract:
Problem: after export publication, fatal caller exit before SubmittedTask
publication strands exported endpoints and temporary handoff File pins.
Expected files: remote_ipc.hpp/.cpp, remote_compute.hpp/.cpp, task.hpp,
wki_wait_ktest.cpp, source tests and resource docs/ledger.
Files read: all listed next-stage entrypoints/callers in the impact map above;
SubmittedTask move/publication and cleanup helpers, FD handoff commit/release,
export cleanup/retirement, Task pending reservation fields and exit cleanup.
Invariants: a single discoverable owner for every handoff ref/export ID across
blocking work; complete map/count published under IPC lock; no user FD removal
before acceptance; fixed max16; no new wire/ABI fields or cap advertisement.
Locks: executor-only pending guard list; IPC during export; compute during
SubmittedTask ownership transfer; cancellation before compute lock; puts outside.
Unsafe contexts: no new RX/IRQ work, guard bookkeeping cannot allocate/block.
Allocation/blocking: bounded stack node links into Task before export; drain
single entries while keeping remaining ownership linked across blocking cleanup.
Compatibility: append internal optional guard parameter; direct submit owns a
local guard; old default call shape remains valid; wire unchanged.
Build/test plan: fatal cancellation at prepared/published boundary, nested guard
adoption/idempotence/re-arm, exact File refs/FD preservation/registry restoration,
source guards, formatting, Normal Build WOS, isolated rootless KTEST.
Rollback: revert this stage's guard and submit transfer changes together.

Stage11l implementation/evidence: WkiIpcSubmissionGuard links a bounded caller
map/count/handoff view into the executing Task before export. Both direct-submit
APIs first adopt into their own guard; placement retry re-arms after clearing
old arrays. The count is already published under IPC lock at final export
publication. Normal/fatal cancellation clears individual pins/IDs before puts or
single-export cleanup, remains linked across callbacks, and handles re-entry.
Single-export cleanup frees detached exporters before the last File put.
SubmittedTask receives pins only after successful durable registry publication,
then disarms the guard under compute lock. Send failure, timeout, and rejection
restore row-owned pins/IDs into the guard before consuming the row; this covers
blocking cancellation sends as well. Fatal cleanup drains submission guards
before FD snapshots and before compute-lock acquisition.
Tests: actual 1/16-export cancellation preserves source FDs/refcounts and restores
registry population; empty cancellation/re-arm and nested guard adoption;
reentrant final-File-close cleanup drains remaining pins exactly once; actual
SubmittedTask publication/return-to-guard/fatal cleanup covers0/1/16 refs.
Normal Build WOS passed. First diagnostic build found the new compute KTEST was
in a file without the compute declarations; moved it to the existing
wki_remote_compute_ktest.cpp. Diagnostic rebuild passed. Formatting/tidy exited0
with warnings, source guards and diff check passed. All three new KTEST cases
have passed live; full-suite terminal result pending.

Remaining ownership work after11l:
- Accepted handoff still takes SubmittedIpcHandoffCleanup onto the caller stack
  before wki_ipc_commit_task_fd_handoff. Reuse the new guard with map=nullptr and
  the cleanup's count/array, registered before taking the row pins, to cover
  fatal exit during a final File put. First put of a removed FD cannot be last
  because the identity pin is still held; guard must retain all later entries.
- Pipe capacity growth still occurs before acceptance and remains after a
  receiver rejection. Candidate defer to export_pipe_write_nonblocking's first
  actual pipe write, whose deferred worker already owns the File. This moves
  growth after receiver execution has started; audit repeated growth attempts,
  existing pipe write tests/perf accounting and daemon context before editing.
- Optional RDMA accepted-worker lifetime: flush takes rdma_data_ptr from the
  ring under IPC lock, then uses it unlocked. Peer cleanup detaches backlog->exp
  and can retire the exporter/ring while that write is in progress. Unregister
  now protects transport RX, not that local worker reference. Need an explicit
  worker/retirement pin or backlog-owned ring lifetime through drain. Do not
  advertise capability or claim this race fixed by private preparation.
- SubmittedTask publication storage, pending queues and deferred backlog chunk
  queues still allocate through standard containers in existing paths; audit
  checked allocation/recovery as required, without imposing a64-export cap.
- Complete capability gating, unsupported-target errors, real user/system/child
  wait accounting, remote service descendants/aggregate CPU policy, rejected
  receiver denial telemetry, and capable/mixed-peer multi-node evidence remain.

Stage11l final live evidence: isolated rootless KTEST completed8570 passed,
0 failed, then powered off with launcher exit0. Saved serial:
/tmp/wos-goal22-submission-guard-ktest-serial.log. Production source was unchanged
after the successful Normal Build WOS. Final IPC/compute source and diff checks
passed. Goal22 remains active; no WKI capability advertisement/completion claim.

Stage11m patch contract (2026-09-08):
Problem: accepted handoff pins temporarily leave durable tracking during
potentially blocking File closes; rejected preparation grows local pipe capacity.
Expected files: remote_compute.cpp, remote_ipc.cpp/.hpp, VFS core.cpp/vfs.hpp
(test fixture only), wki_wait_ktest.cpp, IPC source checks and resource docs.
Files read: accepted paths in both submitters, guard cancellation/commit helper,
pipe write flush/deferred PTY call sites, export preparation, nonblocking write
view, VFS pipe creation/reserve/growth/copy/accounting and existing pipe KTESTs.
Invariants: register accepted cleanup guard before taking row pins; preserve
exact-FD identity commit and clear-before-put; zero/invalid writes cannot grow;
preparation/rejection preserves capacity and bytes; preserve O_NONBLOCK and wire.
Locks: compute only for pin transfer; FD for removal; no puts under either;
pipe growth uses pipe lock internally outside IPC/FD locks in deferred workers.
Unsafe contexts: no allocation in RX/IRQ; actual writes already run in deferred
worker context with a File lifetime reference, never under IPC lock.
Allocation/blocking: growth is optional, failure keeps existing buffer and write
semantics; successful growth makes later attempts a bounded capacity check.
No extra File ref or allocation during accepted-guard bookkeeping.
ABI/wire impact: none; test-only pipe fixture exposes existing creation helper.
Build/test plan: reentrant accepted commit cleanup preserves later source FDs and
releases their temporary pins; actual native pipe export/cancel unchanged
capacity/contents; first remote-side write grows and preserves buffered bytes;
repeated/zero/invalid writes; source/format, Normal Build WOS, isolated KTEST.
Rollback: revert this stage's accepted guard and deferred capacity changes as a
unit; keep previously verified preparation/submission ownership fixes.

Stage11m implementation: both accepted submit paths register a map-less guard
before taking row handoff pins, so final-close cancellation can drain later
pins. Pipe reserve_capacity moved from export preparation to deferred actual
write; zero/invalid guards precede it. Native pipe KTEST fixture uses existing
vfs_pipe_for_task with O_NONBLOCK; production VFS behavior unchanged. Tests
cover accepted commit re-entry/source FD identity and a real native pipe with
buffered prefix, unchanged preparation/rejection capacity, zero/invalid writes,
first/repeated actual write growth, preserved byte sequence and cleanup totals.
Normal and diagnostic builds passed; IPC/compute source and diff checks passed.
Formatting/tidy and live KTEST are still running at this checkpoint.

Stage11n patch contract:
Problem: peer/task cleanup can retire an RDMA ring while a deferred write worker
uses a pointer into that ring outside the IPC lock.
Expected files: remote_ipc.cpp/.hpp, wki_wait_ktest.cpp, source tests/docs.
Files read: full deferred write-flush loop, retirement predicate/compaction/free,
peer and task-export cleanup, backlog registry/queue/erase/free, RDMA front and
region revocation, existing export lifetime/failed write KTESTs.
Invariants: each backlog has exactly one shard worker; a live write pins its
export/ring until it returns under IPC lock; cleanup may detach immediately but
must defer ring retirement. File ownership stays separately reference-counted.
Locks: pin and return under IPC lock; VFS write, revoke, free and File puts outside.
Unsafe contexts: no new RX waits or allocations; no peer cleanup waiting for
this writer. The shard worker completes deferred retirement after its write.
Allocation/blocking: one bool per exporter; no allocation in pin operations;
worker is an existing kernel daemon and owns its File across write/growth.
ABI/wire impact: none; local-only lifetime field and helper.
Validation: fake registered ring with actual deferred write helper after both
peer and per-submission detach; no free/revoke until worker returns; exact-once
revocation, File release, registry cleanup. Source, format, both builds, KTEST.
Rollback: revert pin/predicate/worker finish changes as one stage.

Stage11m final evidence: Normal/diagnostic builds and formatting/tidy passed
(tidy warnings remain). Both new cases passed; full isolated rootless KTEST
8572 passed,0 failed, powered off/launcher exit0. Saved serial
/tmp/wos-goal22-accepted-capacity-ktest-serial.log. This is the11m kernel,
prior to the subsequently prepared11n deferred-writer retirement pin.

Stage11n final evidence/implementation: WkiIpcExport now has an IPC-lock guarded
pipe_write_in_flight lifetime pin for the single backlog shard worker. The
retirement predicate respects it. The worker pins before unlocking for VFS and
uses finish_export_pipe_write_locked with its original owner after returning;
that helper clears the pin and completes detached retirement under IPC lock.
No peer-cleanup wait was added. Backlog/export/ring are freed outside IPC lock
before dropping the worker's independent File reference on the detached path.
KTEST uses a fake registered ring with the actual deferred write helper after
both public per-submission and peer cleanup. It verifies still-readable bytes,
no early revocation/free, worker-triggered retirement, and exact-once revocation
and File close. Both builds passed; formatting/tidy exited0 with warnings;
IPC/compute source guards and final diff check passed. Full isolated rootless
KTEST:8573 passed,0 failed, powered off/launcher exit0. Saved serial:
/tmp/wos-goal22-rdma-worker-ktest-serial.log. This is callback/lifetime evidence,
not live multi-node RDMA transport evidence.

Next allocation/publication audit from current source:
- g_ipc_proxies is still deque<ProxyIpcState*>; the importer installs File slots
  then push_back can allocate. Reuse the existing intrusive IpcRegistry with
  independent registry links in ProxyIpcState; current uses need begin/end/erase
  and push_back only. Preserve refcount ownership and over64 cleanup tests.
- publish_submitted_task_locked still emplaces a SubmittedTaskSlot into
  deque<SubmittedTaskSlot> when the free list is empty. Standard-container
  allocation cannot express checked kernel heap failure. Preserve stable slots,
  insertion order, free-list reuse, ID buckets and high-water tests with checked
  slot storage. Four traversals inspect g_submitted_tasks (task cleanup, running
  query, peer failure, diagnostic snapshot). Publication allocation failure
  should return actual -ENOMEM to both submitters, not generic -EAGAIN; invalid
  IDs/duplicates and resource constraints need separate results. Failed local
  st Task lifetime refs must unwind before potentially blocking IPC cleanup.
- Both submit writers keep oversized wire frames in stack unique_ptr large_buf
  across interruptible send/wait. Fatal stack abandonment can strand that owned
  allocation. Track buffer lifetime through Task cleanup independently of the
  IPC ownership that moves into SubmittedTask. Avoid disarming a buffer when
  its IPC pins transfer; do not merely add another untracked RAII object.
- Receiver RunningRemoteTask publication and deferred pending/chunk queues also
  need their relevant allocation/lifetime audit before final capability enable.
Remaining full-goal gates: capability/unsupported-target behavior, actual
user/system/child wait usage, remote service descendants and aggregate policy,
receiver denial telemetry, and capable/mixed-peer multi-node verification.
Goal22 remains active; capability is not advertised. No current runtime blocker.

Stage11o patch contract:
Problem: imported FD publication and submitted-task slot growth use standard
containers that cannot report a null-returning kernel allocation safely.
Expected files: remote_ipc.hpp/.cpp, remote_compute.cpp/.hpp, WKI compute/IPC
KTEST registrations and source checks, docs/ledger.
Files read: proxy attach/install/refcount/lookup/unregister/peer cleanup,
IpcRegistry implementation; SubmittedTaskSlot/free-list/ID lookup/publication/
reclaim, all four pool traversals, both submit callers and existing slot tests.
Invariants: proxy registry publication cannot allocate; separate links preserve
refcounts and waiter ownership; no64 population cap. Stable slot addresses,
insertion order, ID buckets, free-list reuse and high-water accounting remain.
Failure preserves the unpublished input, all registries/counts and Task refs;
caller releases unpublished Task ref before potentially blocking IPC cleanup.
Locks: IPC for proxy membership; compute for slot pool/ID/free-list publication.
Unsafe contexts: no new RX/IRQ allocation; slot allocation stays in the existing
submitter-side context, under the same compute lock as before, now checked.
Allocation/blocking: use nothrow Slot creation and append-only storage links;
no extra generic allocator or scheduler class. Reclaimed slots remain reusable.
ABI/wire impact: none. Both submitters report -ENOMEM for slot allocation;
invalid/duplicate slot IDs report -EINVAL internally.
Validation: forced empty-pool allocation failure with unchanged refs/metadata,
invalid/duplicate IDs, growth ordering/high-water/reuse; proxy >64 cleanup and
attach rollback remain covered; source/format, Normal Build WOS, isolated KTEST.
Rollback: revert checked slot storage/publication and proxy membership together.

Stage11o implementation: proxy membership now uses existing nonallocating
IpcRegistry with independent ProxyIpcState links. SubmittedTaskSlot storage is
an append-only list with checked nothrow growth; ID index/free-list reuse and
insertion order remain unchanged. All four traversals use storage_next, and
high-water tests use the explicit storage count. Publication failure preserves
input and reports -ENOMEM (invalid/duplicate IDs -EINVAL). Both submitters
release the unpublished Task ref before IPC cleanup and preserve that errno.
KTEST injects empty-free-list OOM, validates unchanged refs/metadata/counters/
indices, then tests real growth/order, duplicate rejection and allocation-free
reuse. Normal/diagnostic builds and source guards passed; live suite running.

Stage11p patch contract:
Problem: oversized inline/VFS_REF frames stay in stack unique_ptr storage across
interruptible send/wait; fatal stack abandonment can strand that allocation.
Expected files: remote_compute.cpp/.hpp, task.hpp, compute KTEST/source checks,
resource docs/ledger.
Files read: both submit writers, send_task_submit_payload fragmentation,
wki_send_impl/copy_wki_payload_segments/retransmit publication, Task pending
cleanup lists and wki_remote_compute_cleanup_for_task.
Invariants: a buffer belongs to one executor-linked cleanup node independently
of IPC ownership transferred to SubmittedTask; exact-once release on normal
return and fatal exit. Small frames retain stack storage/no heap allocation.
Locks: buffer list executor-only; cleanup before compute lock. No network or
File callbacks in buffer release; ordinary kernel allocation/free semantics.
Unsafe contexts: submitter syscall/placement only, no new IRQ/NAPI allocation.
Allocation/blocking: link before checked heap allocation; zero size and failure
leave no linked node; same size bounds as existing wire framing. Transport and
retransmit queues own copies, so fatal cleanup does not free their storage.
ABI/wire impact: none, one local Task pending-list pointer and a private class.
Validation: zero/failed allocation, nested buffers, out-of-order release,
remaining-list cleanup via actual fatal-task cleanup, exact free counts; source,
format, both builds and isolated rootless KTEST.
Rollback: revert tracked buffer class/list and both writer substitutions together.

Stage11o final evidence: full isolated rootless KTEST8574 passed,0 failed;
SlotAllocationFailurePreservesInput passed, as did existing >64 IPC peer
cleanup and attach rollback coverage. Powered off/launcher exit0. Saved serial
/tmp/wos-goal22-publication-storage-ktest-serial.log. Formatting/tidy exited0
with warnings. This kernel predates11p tracked frame storage.

Stage11p implementation: TaskSubmitBuffer independently links into the current
executor before allocating an oversized frame. Zero-size stack-frame cases
allocate/link nothing; allocation failure unlinks. Normal destruction or fatal
wki_remote_compute_cleanup_for_task clears/frees each allocation exactly once,
without compute locks or File/network callbacks. Both INLINE and VFS_REF use it;
IPC guard disarm does not unlink buffer ownership. KTEST covers sizes8915/65535,
zero/forced failed allocation, nested/out-of-order cleanup, IPC ownership
independence and actual fatal-task cleanup with exact free counts. Both builds
and formatting/tidy currently running; source/diff checks passed.

Stage11p final evidence: Normal Build WOS and diagnostic build passed;
formatting/tidy exited0 with warnings. SubmitBuffersFollowExecutor passed live;
full isolated rootless KTEST8575 passed,0 failed, powered off/launcher exit0.
Saved serial /tmp/wos-goal22-submit-buffer-ktest-serial.log. Final IPC/compute
source checks and diff check passed. Goal22 remains active; no current blocker.

Next receiver-publication investigation (read only, not implemented):
g_running_remote_tasks remains deque<RunningRemoteTask> and its sole production
push_back(rt) occurs in handle_task_submit after child/proxy preparation and
final peer/session/cancel/deadline check, before post_task_balanced. Failure
must unlock, release worker_task_ref and completion_task_ref, destroy the
unpublished process/output, and send TASK_REJECT NO_MEM through the same session.
RunningRemoteTask is a plain copyable local record of flags/IDs/raw owned refs;
existing compaction already relocates rows. All current readers are under
compute lock or copy/ref their needed data before unlocking; verify the full
callers when implementing. Existing util/SmallVec.hpp provides checked bool
push_back, indexed at, range iteration and move construction/assignment. It
can retain indexed bounded/fair TASK_ACCEPT retries without a custom allocator
or an O(n^2) linked-list at(). Use one stable forward compaction pass plus
pop_back for inactive rows (remove_at repeatedly would be quadratic). Confirm
inactive rows have already relinquished Task/output ownership before removal.
The accept-retry KTEST currently swaps local deque fixtures into the registry;
convert those fixtures to the same checked container and std::swap via move.
Audit/find_running_task callers, completion/cancellation/peer cleanup, diagnostic
rows, completion eligibility, post-publication relookup, and pending accept
cursor before changing storage. Add actual allocation-failure/unpublished-child
cleanup evidence and retain retry fairness/exit-ready/session tests.
Remaining full goal: receiver/deferred allocation recovery, capability gating,
real user/system/child wait accounting, remote service descendant/aggregate
policy and denial telemetry, then capable/mixed-peer multi-node verification.

Stage11q patch contract:
Problem: receiver running-task registry allocation cannot report failure after
child/FD preparation; rejection must unwind before scheduler publication.
Expected files: remote_compute.cpp/.hpp, compute KTEST/source checks, docs.
Files read: complete running-task lookup/retire/peer termination/completion/
accept retry/exit-ready/cancel/diagnostic/publication paths; SmallVec checked
growth/move/indexing; unpublished-process teardown/ownership tests; NPROC
reservation/attach/observation; native pipe KTEST creation helper.
Invariants: running rows may move only under compute lock, as during existing
compaction; retained Task refs/scalar copies survive unlock, never row pointers.
Preserve insertion order, indexed/fair accept cursor, no fixed active-task cap.
On publication failure, release monitor/worker refs then destroy the owned child
and output; no scheduler publication, leaked FD, process charge, or monitor row.
Locks: compute around registry operations; release before teardown/File puts and
wire rejection. Existing peer lifecycle lease still serializes final publication.
Unsafe contexts: no new NAPI/IRQ work; receiver submit worker only allocates.
Allocation/blocking: existing checked SmallVec with16 inline rows, heap growth
returns false. Compact in one stable linear pass, not repeated remove_at shifts.
ABI/wire impact: none; allocation failure sends existing TASK_REJECT NO_MEM.
Validation: >inline/>64 growth and stable compaction; existing retry fairness;
forced publication failure with actual owned child, native pipe FDs and NPROC
charge; exact teardown/ref/registry/pipe/population observations; source/format,
Normal Build WOS and isolated rootless KTEST.
Rollback: revert checked receiver registry/failure path/tests together.

Stage11q build evidence: Normal Build WOS and isolated diagnostic build passed.
Formatting/tidy exited0 with warnings, compute/IPC source checks and diff check
passed. Rootless KTEST currently packaging; no live result claimed yet.

Stage11r patch contract:
Problem: pending completion deque allocates unchecked both when collecting an
exited receiver and when requeueing a failed send; accounting/output must survive.
Expected files: remote_compute.cpp/.hpp, compute KTEST/source checks, docs.
Files read: complete collector, pending-completion storage/compaction, both peer
cleanup modes, diagnostic enumeration, peer timer call site, source guards;
existing WKI/task/lifecycle and wire entrypoints from preceding stages.
Invariants: preserve FIFO bounded-per-tick retry and session-generation checks;
transfer Task/output ownership only after checked queue allocation succeeds.
Failed allocation leaves the exited running row intact for a later timer tick.
Requeue the detached node itself without allocation; peer cleanup cannot free
an in-flight detached node and session recheck decides its final disposition.
Locks: compute protects head/tail/count and all publication/removal; sends remain
outside compute. Retain existing output deletion contexts (plain capture data).
Unsafe contexts: timer worker allocation already present, no new IRQ/NAPI work.
Allocation/blocking: checked nothrow node allocation at initial collection only;
no cap, no allocation for retry or removal; periodic timer retries OOM collection.
ABI/wire compatibility: unchanged; exact retained resource/output payload.
Build/test plan: forced enqueue failure, FIFO beyond64, detached node retry
identity and accounting/output preservation, peer cleanup safety; source/format,
Normal Build WOS, diagnostic build and rootless isolated KTEST.
Rollback: revert queue storage/helpers/collector and corresponding tests together.

Stage11q final evidence: RunningRegistryCompactsStably and
RunningPublicationFailureUnwinds passed live; full isolated rootless KTEST8577
passed,0 failed, powered off/launcher exit0. Serial saved at
/tmp/wos-goal22-receiver-registry-ktest-serial.log. Failure is a forced checked
publication result, not a live kernel heap allocator fault; actual child/ref/
pipe/process-charge teardown is exercised. Normal and diagnostic builds passed,
format/tidy exited0 with warnings; source/diff checks passed.

Stage11r implementation: replace pending completion deque with owned linked
nodes and compute-locked head/tail/count. Initial nothrow allocation failure
continues before releasing any running Task/capture ownership. Detached sends
own their node; failed current-session sends append that same node with no
allocation, stale sends destroy it/output. Both peer-cleanup loops and diagnostic
enumeration traverse the queue; compaction preserves order and fixes tail/count.
Retry logs copy scalars before publishing the node, avoiding a new dangling read
if peer cleanup removes it after unlock. Wire output/resource bytes unchanged.
KTEST exercises forced enqueue failure on empty/nonempty queues,69-node FIFO
rotation, exact node identity during retry, detached output+800-byte accounting
survival across invalidation/compaction, empty-to-nonempty reuse and cleanup.
Source checks guard no allocation in retry and retention before monitor release.
Both builds/format checks running; no Stage11r live result yet.

Stage11r build evidence: Normal Build WOS and isolated diagnostic build passed;
formatting/tidy exited0 with warnings; compute/IPC source checks passed. The
rootless diagnostic VM is packaging/running; live evidence still pending.

Next accounting investigation (read only): current resource State stores only
aggregate cpu_time_us. Scheduler charge_syscall_time_until and
account_task_runtime_delta separately update per-Task user_time_us/system_time_us;
resource::charge_cpu currently receives only elapsed time and aggregates threads
through immutable LifetimeOwner. Receiver completion waits resource_thread_owners
zero and snapshots State, while finalize_proxy_task merges resource totals and
service CPU after winning ACTIVE->EXITING, then publishes child_events exit.
Child events snapshot task_rusage_user_time_us/system_time_us (Task own plus
reaped children); waitpid consumes that immutable snapshot into parent child_*
fields. Merely merging resource CPU leaves remote wait/rusage zero. Do not label
all remote CPU as user time. Investigate maintaining split process CPU alongside
resource accounting under resource_lock (scheduler already knows the split),
carrying both counters in a versioned wire record and merging only remote deltas
before exit-event publication. Also preserve reaped child user/system totals;
consider exec migrations and thread accounting. This is not yet a patch contract
or implementation. Capability still not advertised. Remaining service aggregate/
descendant support, policy-denial telemetry and capable/mixed-peer evidence must
be resolved before Goal22 completion; no current blocker.

Stage11r final evidence: CompletionQueuePreservesOwnership passed live; full
isolated rootless KTEST8578 passed,0 failed, powered off/launcher exit0. Saved
serial /tmp/wos-goal22-completion-queue-ktest-serial.log. Normal Build WOS and
diagnostic build passed, formatting/tidy exited0 with warnings, final compute/
IPC source and diff checks passed. Initial-allocation failure uses a forced
checked-helper result, not global heap fault injection. FIFO69, retry identity,
real output and exact resource snapshot lifetime are exercised in the kernel.
Goal22 remains active, no current runtime blocker. Latest source includes both
Stage11q checked receiver registry and Stage11r checked completion queue.

Stage11s patch contract:
Problem: resource CPU total reaches origin, but user/system and reaped-child CPU
are missing from remote wait accounting. Total cannot be relabeled user time.
Expected files: sched resource State/API/scheduler accounting; WKI resource
contract/model/compute; host model/source tests, resource/compute KTEST, docs.
Files read: scheduler syscall precharge/runtime split, resource charge and owner
lifetime, wire snapshot/restore/merge and sender/receiver/finalizer, child event
snapshot/commit and Task wait helpers, relevant kernel/WKI/critical instructions.
Invariants: same chargeable runtime after IRQ/precharge removal, same single
policy charge and signal boundaries, no new locks/allocations in scheduler.
Store user/system counters alongside total under resource_lock. Preserve soft/
hard history and fork reset; exec migration imports counters and merges deltas.
Wire v2 extends previously unadvertised resource suffix with own split counters
and completion-only reaped-child deltas; submissions require those deltas zero.
Receiver Task child counters start at zero, so completion returns only this
execution segment's reaped work. Nested migration adds returned child deltas
once before its own completion. Home's existing child counters remain intact.
Locks: existing resource lock for CPU split; same proxy ACTIVE->EXITING winner
before merge/child event publication; completion waits all resource thread
owners gone, making exited receiver child counters stable. No lock nesting added.
Unsafe contexts: IRQ/scheduler updates are fixed arithmetic only; wire work is
existing submit/timer context. No new blocking, allocation or callbacks.
Compatibility: resource suffix version2/832 bytes; legacy identity remains368.
Capability stays unadvertised; v1 resource records rejected explicitly by version
and exact length, no interpretation of mismatched layouts. Syscall ABI unchanged.
Validation: user/system split across thread owners and precharged slices;
wire roundtrip/malformed/reset/overflow/delta/nested-child accounting; actual
proxy child event and exact-once wait; source/format, host sanitizers, Normal
Build WOS, diagnostic build and rootless isolated KTEST.
Rollback: revert split state/scheduler and v2 wire/merge/tests together.

Stage11s implementation: State now records cpu_user_time_us and
cpu_system_time_us beside aggregate CPU. Scheduler runtime computes the same
user portion for Task stats and one resource charge; syscall precharge remains
all system, consumed before any next-tick charge. Thread charges retain the same
immutable owner and resource lock. Saturating counters preserve total consistency;
fork inherit_limits resets both modes, imported exec State retains them.
Resource wire version2 is832 bytes: mode counters at792/800, reaped-child
completion deltas at808/816, reserved824. Legacy368 identity is unchanged,
resource-bearing identity is1200. Decode checks saturated sum equals total;
completion also checks each mode is monotonic and baseline child deltas zero.
Restore rejects nonzero completion-only child deltas without mutating State.
Collector reads stable exited child totals after all thread owners drain.
One winning finalizer calls merge_proxy_resource_completion, which validates
before updating resource state, Task modes and reaped totals, then charges the
existing service CPU path before publishing child exit. No capability advertised.
The existing proxy child-event fixture now uses the production merge helper:
invalid completion leaves all counters intact; real merged own+reaped totals
351 user/588 system survive release/reclaim and one parent event consumption.
New scheduler KTEST covers user/kernel classification, partial and wholly
precharged ticks, and blocked-time exclusion. Existing thread CPU KTEST now
checks600000 user/400000 system across two owners without changed signal timing.
Host15 wire tests and22 resource/service model tests passed ASan/UBSan. Normal
Build WOS and diagnostic build passed. Format/tidy running with warnings; live
rootless KTEST currently packaging, no Stage11s runtime result yet.

Next observation investigation (read only, not implemented): procfs
collect_proc_task_stats sums active-thread Task.user/system counters and
process_cpu_time_ns in syscalls_impl/time/time.cpp does the same. TIMES reads
only the calling Task's own/child fields. These can lose exited-thread history
or show thread-only process times, unlike the new stable process resource CPU
split. Task wait helpers still use own Task fields plus child fields. The two
remote ELF constructors import resource State but leave per-Task mode counters
at constructor zero; process views should consume the imported process State
rather than accidentally treating exec migration as a new CPU lifetime.
Audit these consumers and child-event snapshot lock ordering before changing
observation helpers. Keep per-thread views per-thread. Do not double-count
remote mode deltas already merged into resource State and proxy Task fields.
Local child-event publication precedes thread-object GC, so full process wait
accounting may need a thread accounting/exit readiness rule, not an assumption
that object reclamation has already finished. This is separate from the already
implemented receiver completion gate on resource_thread_owners zero.

Stage11s final evidence: isolated rootless KTEST8581 passed,0 failed; VM powered
off/launcher exit0. ResourceCpuSplitMatchesTask, existing thread owner CPU test,
and merged proxy child-event fixture all passed. Saved serial:
/tmp/wos-goal22-cpu-split-ktest-serial.log. Host ASan/UBSan15 resource-contract
and22 resource/service model tests passed. Normal Build WOS and diagnostic
build passed; formatting/tidy exited0 with warnings. Final compute/IPC/scheduler
source checks and diff check passed. No capability advertisement yet. Goal22
remains active with local observation consistency, distributed service policy/
accounting, admission denial telemetry, capability activation and multi-node
validation outstanding. No current blocker.

Stage11t patch contract:
Problem: process procfs/clock reads sum active thread counters and lose reclaimed
thread/remote-imported history; TIMES/GetRusage reads only the caller. Procfs
child CPU columns are hardcoded zero despite available reaped accounting.
Expected files: resource CPU snapshot helper; child_events bounded reaped reader;
time.cpp/.hpp; procfs.cpp/.hpp; resource KTEST, source checks, target probe/docs.
Files read: complete time syscall dispatch and libc ClockGet/GetRusage wrapper,
process procfs status/stat generation and top parser, resource owner/split charge,
child_events lock/commit/reaped fields, scheduler accounting from Stage11s;
kernel/critical/mlibc/subsystem instructions and syscall time dispatcher.
Invariants: process views use persistent resource user/system counters once;
thread views stay per-thread; resource CPU excludes children. Borrow an immutable
thread owner through its existing lifetime reference, never stale PID lookup.
No usercopy under resource/lifecycle locks. Reaped totals read as one pair under
existing lifecycle lock. Preserve syscall ABI, clock/tick units and procfs layout.
Locks: resource snapshot then unlock before lifecycle snapshot; no nested locks,
no runqueue/WKI lock at these callers. Procfs keeps its Task ref during reads.
Allocation/blocking: snapshot fixed scalars only; no allocation/registry lookup.
Existing procfs active scan remains solely for thread count/state. Convert CPU
microseconds directly to timespec to avoid nanosecond multiplication overflow.
Unsafe contexts: syscall/procfs observation only; no scheduler/IRQ mutations.
Validation: retired sibling history, inherited remote State, stale thread-local
copies, missing owner failure, child totals, process/thread distinction, max-us
conversion and TIMES ticks; source/format, Normal Build WOS, diagnostic build,
rootless live KTEST and target observation probe when artifacts are ready.
Rollback: revert snapshot/readers/tests together; enforcement and wire unchanged.
Stage11t lock refinement: WKI finalizer currently writes reaped counters without
the lifecycle lock. Route its addition through a bounded child_events helper,
sharing saturated addition with local commit_claim. Finalizer already holds no
compute/runqueue/resource lock there. This makes the new paired read coherent
with both actual production writers. The old inline Task accumulation helper
has no production call sites and is unchanged. Include remote_compute.cpp in
expected files; wire semantics and exact-once exit winner remain unchanged.

Stage11t implementation: copy_cpu_observation borrows immutable thread owner,
reads retained process mode counters under resource_lock, releases it, then reads
reaped mode pair under lifecycle lock. Missing owner fails without output change.
CLOCK_PROCESS_CPUTIME and TIMES use this helper; thread clock keeps own Task
counters. CPU timespec conversion divides microseconds directly, avoiding
nanosecond overflow. Procfs process status/stat uses retained counters and reaped
fields; active scan now determines only count/state, thread views stay local.
WKI reaped additions and local wait commit saturate under lifecycle lock, so the
new paired reader is synchronized with actual writers. No ABI/wire changes.
KTEST covers imported State, retired owner eligibility/sibling reclamation,
thread-local stale State, missing owner, reaped saturation, process/thread view,
CLOCK/TIMES units and UINT64_MAX microsecond conversion. New target command
resource-limits cpu-observation compares process clock/rusage/procfs after three
joined CPU workers and one reaped child. Normal/diagnostic builds and format/tidy
running; source/diff checks passed. No Stage11t live result claimed yet.

Stage11t validation so far: diagnostic build passed. Normal build compiled but
sandboxed packaging failed at libguestfs bind (Operation not permitted), after
FUSE delta was unavailable. Retried Normal Build WOS with host access and no
normal WOS VM running; retry passed and completed the disk image. This was host
packaging permission, not a code/cluster failure; no approval rejection occurred.
Formatting/tidy exited0 with warnings. Time/waitpid/compute/scheduler source
checks passed. All three new CPU observation KTESTs passed; full suite still
running. Normal VM17 config prepared at/tmp/wos-goal22-node17.json with isolated
/tmp/wos-goal22-vm17 overlays for the target probe after KTEST exits.

Stage11t final evidence: full rootless isolated KTEST8599 passed,0 failed;
powered off/launcher exit0, saved serial
/tmp/wos-goal22-cpu-observation-ktest-serial.log. All three new observation tests
passed. Normal Build WOS passed with host-access packaging retry described
above. Formatting/tidy exited0 with warnings; time/waitpid/compute/scheduler
source and diff checks passed. Normal rootless VM17 resumed through GDB and ran
resource-limits cpu-observation:14 checks,0 failed, workers240948us,
process267343us, rusage250000us, proc250000us, reaped children70000us. Mode tick
truncation explains the allowed<20ms difference. Log
/tmp/wos-goal22-cpu-observation-live.log; stopped owned VM17 via verified QMP.

Next critical accounting gap: local publish_exit snapshots Task own+child CPU,
so a reaped process's already-joined threads still do not contribute to its
parent's rusage, even though its own process clock/procfs now preserve them.
Process exit calls request_thread_group_exit, does teardown, finishes syscall
accounting and immediately publishes exit_notify_ready/child event; sibling
requests are asynchronous. Need audit group-exit completion and final scheduler
accounting before substituting resource State into immutable child-event
snapshots. Must include terminating live siblings and joined/reclaimed siblings,
not just the current simple one-thread child probe. WKI already waits resource
thread owners for its final resource record. Keep this separate from object-GC
waits that could deadlock waitpid/lifetimes. Also remaining distributed service
accounting/policy, resource-denial reporting and capable/mixed-node verification.
Goal22 active, no current blocker; resource capability still unadvertised.

Stage11u patch contract:
Problem: child exit snapshots omit joined-thread CPU and race live sibling CPU;
thread exit also lacks a final syscall/runtime accounting flush. Refcount/GC
completion cannot be used as the process wait condition.
Expected files: resource LifetimeOwner/task counters and close gate; scheduler
final CPU flush; process/thread exit; child_events CPU snapshot; tests/probe/docs.
Files read: complete process/thread exit flow, thread constructor/publication and
late request inheritance, LifetimeOwner/destruction, scheduler runtime/precharge/
last_tick paths, process-safe yield, child-event enqueue/publish/GC ownership;
critical-path/kernel instructions and earlier subsystem context.
Invariants: close CPU-thread admission under resource_lock before group scan.
Each successful bind owns one pending CPU contribution until final thread exit,
or unpublished rollback. Keep lifetime ref count independent and unchanged.
Leader waits while ACTIVE, yielding without locks, for CPU contributions only;
no wait for thread-object GC. After all cleanup, flush current runtime slice
with its still-active syscall classification, then freeze future Task charges.
Only after frozen accounting does a thread release its CPU contribution. Leader
publishes resource user/system + reaped totals after its own final flush.
Locks: existing runqueue -> resource for final slice; lifecycle -> resource for
bounded exit-event snapshot, no resource->lifecycle nesting. Close gate uses
resource only; counter retirement is atomic. No locks across yields or cleanup.
Unsafe contexts: final flush runs in exiting syscall on current CPU under its
runqueue lock, no allocation/blocking/callback beyond existing CPU charge path.
Waiting stays at existing PROCESS-safe kern_yield before EXITING; no IRQ waits.
Allocation/rollback: fixed fields only; failed late bind drops its new lifetime
ref, destructor retires unpublished contribution without wake/GC dependency.
ABI/wire: unchanged; wait rusage now includes complete process CPU. Thread and
ptrace-stop snapshots retain thread scope. Kernel daemon fixtures retain own time.
Tests: admission-close/bind/retire/rollback independent of Task refs; final slice
classification and idempotent freeze; child-event aggregate/exact-once; live
joined-thread child and live-sibling group exit. Normal/diagnostic builds,
format/source checks, rootless KTEST and normal target probe. Rollback together.

Stage11u implementation: resource_cpu_threads counts pending thread CPU
contributions independently of resource_thread_owners/Task refs. LifetimeOwner
bind closes under owner resource_lock; late bind drops its acquired ref. Its
finish_cpu is idempotent, and destructor handles unpublished rollback. Process
exit closes admission before requesting sibling exit and yields while ACTIVE
until contributions drain. Thread exit flushes/finalizes CPU then retires its
contribution after descriptor/WKI/wait cleanup. Leader finalizes before exit
notification. finalize_task_cpu_locked consumes IRQ/precharged time, accounts
remaining slice while syscall classification is still active, finishes it, and
sets cpu_accounting_finalized; scheduler/resource charge paths skip frozen Tasks.
Child-event EXIT snapshots process resource modes plus reaped modes under
lifecycle->resource; thread/non-process snapshots retain prior scope.
KTEST adds final partial slice+IRQ/precharge/freeze idempotence, admission close/
failed late bind/retirement-before-metadata/destructor rollback, and retired-thread
child event/exact-once reap. Target cpu-wait-observation covers joined vs active
workers at group exit. First builds caught missing resource.hpp in exit.cpp and
this_cpu_locked's nonvoid callback requirement; fixed and diagnostic rebuild
passed. Normal rebuild and format/tidy retry running, rootless KTEST packaging.
Source checks passed; no Stage11u live result yet.

Stage11u runtime evidence so far: first isolated suite8611 passed,1 failed and
entered deliberate selftest-failure panic. Only new ExitIncludesRetiredThreadCpu
failed: initialize_fixture marked child EXITING before LifetimeOwner::attach,
whose try_acquire correctly refuses EXITING. Fixed fixture to bind/charge while
ACTIVE and restore EXITING before event publication. Saved failed serial
/tmp/wos-goal22-child-cpu-fixture-failure-serial.log. No production workaround.
Final diagnostic fixture rebuild passed; stopped verified failed KTEST VM.
Normal Build WOS had passed. Rootless normal VM18 then ran CPU wait probe:
10 checks,0 failed; joined workers wait4=242488us,parentdelta230000us;
live group-exit workers wait4=248523us,parentdelta250000us. Existing CPU
signal/limit matrix9 checks,0 failed. Logs/tmp/wos-goal22-child-cpu-live.log
and/tmp/wos-goal22-child-cpu-signal-live.log. Verified/quit normal VM18.
Formatting/tidy retry and final fixture check exited0 with warnings. Corrected
full isolated KTEST currently packaging; no successful final result yet.

Next WKI lifetime investigation (read only): receiver completion still waits
resource_thread_owners (metadata/GC), introduced before explicit CPU contribution
retirement existed. Consider switching to resource_cpu_threads after confirming
all semantic resource mutations finish before that retirement, so a pinned dead
thread object cannot indefinitely delay remote wait. Current thread exit retires
CPU just before process_charge.release; move that release before retirement if
this boundary is to mean all relevant teardown complete. Later Thread/pagemap GC
can lower physical AS current, but completion merge intentionally preserves home
physical current and only merges limits/peaks/denials/CPU; prove no post-boundary
policy/denial changes before relying on this. Preserve monitor/output references,
session checks, and actual exit_notify_ready gating. Service aggregates and
resource admission denial telemetry remain outstanding, followed by capability
activation and capable/mixed-peer multi-node evidence. Goal remains active.

Stage11u final validation: corrected isolated KTEST completed 8612 passed,
0 failed and powered off; launcher exited0. Full serial saved at
/tmp/wos-goal22-child-cpu-ktest-serial.log. Normal Build WOS, diagnostic build,
format/tidy (warnings present), source guards, CPU wait live10/0 and CPU signal
live9/0 all passed as recorded above. No owned VM remains running. Local child
exit CPU aggregation is complete; remaining WKI contracts keep Goal22 active.

Stage11v patch contract (receiver accounting completion):
Problem: resource-bearing receiver completion waits for thread metadata GC even
though Stage11u establishes an explicit CPU-retirement boundary. Nested proxy
finalization also publishes exit_notify_ready before descriptor/ELF cleanup.
Expected files: remote_compute.cpp, threadControl.cpp, existing WKI/wait source
checks and resource_governance.md; existing completion-wake KTEST helper.
Files read: receiver completion collector, publication/session predicates, proxy
finalizer, thread/process exit, LifetimeOwner, ProcessCharge release, GC cleanup,
lazy range release, resource snapshot/merge and existing tests/docs. WKI core/
wire/task entrypoints were read in preceding stages; instructions rechecked.
Invariants: ready still requires has_exited and release/acquire publication;
resource completion waits for all CPU contributions, not metadata destruction.
Final thread UID release occurs before its CPU contribution retirement. Later
GC frees physical ranges and File refs; completion preserves home current values
and consumes only policy/peak/denial/CPU observations. Population release may
update other active UID members but cannot add a denial or raise the departing
member's peak. Session/accept gates and durable queue ownership unchanged.
Locks: existing compute lock and bounded atomic loads; thread UID release keeps
existing population->resource order and occurs before final runqueue flush.
Unsafe contexts: no new IRQ callbacks, waits, allocations or logging. Proxy
cleanup remains in existing permitted context, ahead of readiness publication.
ABI/wire compatibility: none; capability still unadvertised.
Tests: actual bound thread keeps metadata ref after CPU retirement; completion
predicate blocks before retirement and readiness, permits after retirement with
ref retained; legacy readiness unchanged. Source guards cover cleanup/publication
order. Normal/diagnostic builds, formatting, full isolated rootless KTEST.
Rollback: revert this stage's helper/predicate/order changes and tests together.

Stage11v implementation: receiver predicate now observes exit publication plus
resource_cpu_threads==0, independent of metadata refs; existing wake KTEST binds
a real LifetimeOwner, verifies pending contribution blocks, retires it and
verifies completion proceeds with metadata retained, while readiness/has_exited
still gate. Proxy readiness moved after descriptor/ELF/UID cleanup; thread UID
release moved before final CPU flush. Normal and diagnostic builds and full
format/tidy exited0 (warnings present); WKI/wait/scheduler source checks passed.
Full isolated KTEST is running. First source-check attempt used an unavailable
forbid_tokens helper; replaced with the file's ordinary explicit assertion.

Stage11w reconnaissance/patch contract (admission denial telemetry):
Problem: receiver NPROC denial mutates its private State, but rejected ELF/stack/
IPC construction loses Task observations on teardown; TASK_REJECT sends only a
reason. Home procfs/service denial counters cannot explain receiver admission.
Expected files: resource_contract_model.hpp, remote_compute.cpp, wire.hpp comment,
process/exec.cpp/.hpp file-backed construction optional failure observations,
host wire tests, WKI KTEST helper, source tests and docs.
Files read: both ELF constructors, unpublished destruction helper and every
receiver rejection site, file-backed constructor, ProcessReservation, response
sender/handler/session/waiter gates, SubmittedTask baseline and merge model.
Flow: preserve unpublished child State before destruction into worker-local
submitted_resources; once input contract validates, rejection can append one
832-byte snapshot. Three reserved response bytes encode suffix size low/high,
last byte0. Receiver accepted/legacy response remains16 bytes. Origin validates
exact length, record version, immutable baseline, unchanged policy/CPU/reaped
before claiming response; merge only max peaks and saturating denied deltas.
Invariants: no imported current charge/limit/CPU after failed execution; failed
reservation leaves no partial child or charge; duplicate/stale/session-mismatched
rejections cannot merge again. Original errno preserved. Output/Task refs and
unpublished recovery ownership unchanged. No capability advertised yet.
Locks: compute->process resource for bounded rejection merge, same order as
existing completion snapshots; no service/registry lookup, waits or usercopy.
Unsafe contexts: RX only fixed stack decode/validation, atomics and resource
spinlock. Child observation copy before permitted-context teardown, no callbacks
under resource lock. Optional failure output is borrowed only within ordinary
function call; no stack pointer retained in Task.
ABI/wire impact: optional capability-bound suffix using reserved length bytes;
base response layout unchanged. Malformed suffix causes no waiter/state mutation.
Tests: model rejection updates only peaks/denials, saturation, malformed/reset/
changed-policy/CPU refusal; actual handler exact session and duplicate behavior;
construction source guards and rootless full KTEST. Normal and diagnostic builds,
format/tidy and host ASan/UBSan. Rollback all stage changes together.

Stage11v final evidence: isolated KTEST8612 passed,0 failed, powered off and
launcher exited0. Full serial/tmp/wos-goal22-completion-retirement-ktest-serial.log.
No owned VM running. Receiver completion retirement/order change validated.

Stage11w implementation: receiver's destruction helper optionally copies resource
State under its lock before teardown, threaded through all9 submit rejection
sites, registry-publication discard, inline construction/interpreter/stdout
failure, and file-backed constructor's borrowed failure_observations output.
After valid input (including CPU-exhausted preflight), TASK_REJECT appends the
832-byte resource record, length encoded in original reserved bytes. CPU
preflight records HARD_LIMIT denial. Earlier malformed/cancel/timeout requests
remain16-byte replies. Exact-session handler validates framing and unchanged
policy/CPU/reaped baseline before response claim, then merges only saturating
denial deltas and max peaks under the immutable owner's resource lock. No charge,
limit, execution CPU or ref ownership imported. Duplicates see response_pending
false. Existing errno paths unchanged.
Model tests now18/18 pass ASan/UBSan, log/tmp/wos-goal22-rejection-contract-test.log.
Normal Build WOS and diagnostic build passed; format/tidy exited0 with warnings,
logs/tmp/wos-goal22-rejection-observation-{normal-build,ktest-build,check}.log.
WKI/exec/wait source guards and diff check passed. Source guard maintenance fixed
old exact function-call expectations and an undefined EXEC_CPP test constant.
New KTEST invokes the actual rejection handler with truncated/invalid policy/
reserved-byte/session and node mismatch frames, then a valid frame and duplicate.
Full isolated KTEST currently packaging; no live result for Stage11w yet.

Service propagation reconnaissance: init configures a service slot for every
manifest service, including default-unlimited services; dropbear descendants
therefore retain membership. Rejecting every service-affiliated remote request
would disable normal shell remote placement and is not an acceptable shortcut.
Copying a finite aggregate CPU budget as a per-process limit permits siblings,
restarts and concurrent placements to overspend it. Unsupported finite aggregate
policy can be explicitly rejected where a receiver cannot enforce it (completion
contract8), but unlimited service accounting still needs descendant coverage.
Current root completion charges home service only for root own-CPU delta; reaped
child counters cannot substitute for service CPU (unreaped/orphan descendants,
nested remote execution and double counting). Need a defined remote accounting
lifetime beyond root waitability, or explicitly bounded supported service domain,
before capability activation. No new service-wire design committed in this
stage. Remaining strict capability gates and capable/mixed-node evidence also
remain; Goal22 active, no indispensable runtime blocker.

Stage11w final validation: isolated KTEST8613 passed,0 failed, including actual
ResourceRejectionPreservesSessionAndDenials handler test. VM powered off and
launcher exited0. Full serial/tmp/wos-goal22-rejection-observation-ktest-serial.log.
No owned VM remains running. Final WKI/exec/wait source checks and diff check
passed. Host18/18 ASan/UBSan, both builds and format/tidy results recorded above.
Docs updated with rejection suffix and corrected remaining activation work:
inherited-FD/completion/rejection implementation is covered; distributed service
accounting, strict capability gates and capable/mixed-peer live evidence remain.

Next bounded task: map service membership inheritance/retirement and all remote
spawn/nested-exec/descendant exit paths, then define remote service observation
lifetime independently of root process waitability. Invariants: CPU is counted
once at execution site, including un-reaped descendants; root wait must not wait
for background descendants; local restart history persists. Finite aggregate CPU
must never become a copied per-process allowance; explicitly reject unsupported
finite aggregate placement if no enforceable shared budget exists. Do not reject
all default-unlimited service descendants (that disables ordinary WKI shells).
Keep capability unadvertised until this and strict negotiated-policy gates have
complete tests. Goal22 remains active, no repeated indispensable blocker.

Stage11x reconnaissance/patch contract (remote service accounting lifetime):
Previous turn classified progress: Stage11v/w code plus passed builds/host/KTEST.
Current source confirms resource::inherit snapshots process owner then calls
inherit_service; threads retain immutable process owner and charge its service.
Local slots have boot lifetime; membership retires nonCPU totals only at Task
metadata destruction, CPU is charged as execution occurs. Remote constructors
currently restore State but attach no service, so descendants lose affiliation.
Expected files: service_resource.hpp/.cpp, resource KTEST, resource docs/ledger.
Files read: full service implementation/model/ABI, resource inherit/CPU path,
Task member layout, remote construction/finalization and parent/thread exit from
preceding stages; subsystem index/kernel/WKI/critical instructions reviewed.
Problem: a future wire report cannot safely use root Task lifetime for service
observations; root wait must return while background descendants keep executing.
Implementation: checked heap RemoteServiceAccounting with intrusive references,
immutable per-process membership copied by existing fork/clone inheritance.
Every attached process owns a reference; independent monitor owns initial ref.
Threads use retained process owner; no extra membership or accounting charge.
Snapshot accumulates actual CPU charges, nonCPU max current/peak and denial
deltas; imported root denial baselines are subtracted so migration does not
repeat historical denials. Retired members fold once; no wait on root task.
Invariants: local service slots/ABI/limits unchanged; remote object only observes
usage, and does not pretend a copied limit enforces aggregate finite CPU. No WKI
capability activation yet. Root import attach must precede scheduler publication;
failed attach leaves membership/refcounts unchanged. Last release destroys only
after members are gone; monitor ref permits observing empty final state.
Locks: existing service_lock -> subject.resource_lock for snapshots/retirement;
IRQ charge takes service_lock after resource charge released its lock. Atomic
reference changes bounded; destruction occurs after releasing service_lock.
Unsafe contexts: no allocation/blocking/network/callback/log in charge/inherit/
snapshot; create uses checked nothrow allocation only in permitted receiver work.
ABI/wire: none in this stage. WKI registration/reporting remains next integration.
Tests: imported baseline denial excluded; fork/grandchild/thread inheritance;
root retired before descendant continues charging; snapshots preserve retired
observations; duplicate/late root attach refusal and no leaked refs; normal and
isolated diagnostic builds, formatting/source checks, full rootless KTEST.
Rollback: remove remote-only fields/class/branches and test together.

Stage11x implementation: checked RemoteServiceAccounting allocation owns initial
atomic ref; each immutable remote ServiceMember retains another. Existing
resource::inherit/inherit_service carries the object to process descendants,
threads use bound process owner, charge_service_cpu records execution-site CPU.
Snapshot merges live/current/max peaks and summed denials; imported per-member
denial baselines are subtracted. Task retirement folds observations once under
service->resource locks, unlinks, then releases accounting outside the lock.
Monitor may release first without invalidating members. Public root attach
rejects threads, already-published/non-ACTIVE/already-affiliated Tasks without
mutation. No WKI attachment/reporting/finite aggregate support advertised yet.
Normal and diagnostic builds passed; format/tidy exited0 with warnings; existing
WKI/exec/wait source guards and diff check passed. Full isolated KTEST packaging,
with new real inheritance/root-retirement/monitor-first lifetime test.
Logs/tmp/wos-goal22-remote-service-lifetime-{normal-build,ktest-build,check}.log.

Next integration design candidate (requires source mapping before edits): use
hierarchical reports along the existing submit session, not global origin-node
service IDs. Each accepted service-bearing remote submission has one receiver
accounting object and one retained source-side report destination (local boot
service slot or enclosing remote accounting object). Descendant forks remain in
the receiver object. Nested remote placement creates another report stream to
its immediate submitter, whose accounting object remains alive while that stream
is pending. This covers background descendants without delaying root wait.
A service flag can use the resource record's remaining flag bits; no service slot
identity need cross the wire. Reports carry task_id, version/sequence, cumulative
CPU/denials/peaks and current/live values. Validate exact session and monotonic
observations; add only deltas. Keep the SubmittedTask slot unreclaimable while
reporting remains pending (existing submitted_task_can_reclaim_locked central
predicate), but release its root Task ref at ordinary wait completion. Task IDs
then cannot be reused before the final report. Receiver final report needs an
explicit durable acknowledgment and same-node retry ownership, independent of
TASK_COMPLETE. An enclosing accounting object needs a pending-remote-child
counter so no-members is not mistaken for final while a nested stream survives.
Avoid double counting: service-bearing TASK_COMPLETE must not also call existing
root CPU service merge, and its per-Task imported remote denial deltas must be
excluded from ServiceMember aggregation when the report stream counts them.
Rejection before scheduler publication has no accepted report stream: existing
TASK_REJECT denial telemetry remains ordinary task/service accounting and clears
the source reporting hold. Capture original submitted denial baseline when root
attachment follows construction so admission observations are counted once.
Finite aggregate CPU cannot be enforced by this observation protocol; strict
remote placement must explicitly reject an unsupported finite service aggregate
policy before exporting FDs (completion contract8), with ordinary allowed local
fallback. Unlimited service defaults stay remotely usable and fully observed.
No source/WKI protocol changes for this candidate yet. Next turn must verify
ACK/cancel/fence lifetimes and report retention/reclamation before implementation.

Additional integration constraints from current source: SubmittedTask result
consumption releases root Task refs and requests centralized reclaim; a separate
service-report hold can preserve the slot without retaining the root. Its move
constructor/assignment/destructor must transfer/release the report destination
exactly once. Peer failure/session-retirement paths must include report-pending
inactive rows, not only currently running tasks. A lost report stream cannot be
presented as complete zero usage: retain known lower-bound counters and expose
an explicit incomplete-accounting observation through service status, with no
claim of complete remote telemetry after fencing. An orphaned receiver accounting
object can safely survive monitor cancellation via its process-held references;
report cancellation must not introduce new process-tree kill semantics. Existing
peer/process termination behavior should be preserved unless source proves a
resource enforcement requirement needs a separate change.

Stage11x final validation: full isolated KTEST8614 passed,0 failed, including
RemoteServiceAccountingOutlivesRootProcess. VM powered off and launcher exited0;
serial saved/tmp/wos-goal22-remote-service-lifetime-ktest-serial.log. No owned VM
running. Both builds, format/tidy (warnings), source checks and final diff check
passed as above. Remote accounting lifetime/inheritance foundation is validated;
WKI reporter attachment, session-scoped cumulative report merge, cancellation/
fence handling, finite aggregate admission rejection, capability gates and
capable/mixed-node live evidence remain. Goal22 active; no blocking condition.

Stage11y patch contract (source report destination and merge):
Previous goal turn was progress: retained accounting code and KTEST8614/0.
Current read: service classes/model/ABI, slot snapshot/retirement, SubmittedTask
move/destruction/reclaim/result consumption, servicectl resource status, existing
host service tests. Source is authoritative; no live VMs remain from Stage11x.
Problem: incoming remote service reports need a retained destination independent
of root Task, exact cumulative-delta merging, current/live observation ownership,
and explicit partial status after a lost stream. Nested report ownership must
keep the enclosing RemoteServiceAccounting alive and prevent premature finality.
Expected files: service_resource.hpp/.cpp, service_resource_model.hpp,
abi/service_resource.hpp flag addition, servicectl status, host/KTEST/docs.
Flow: a movable intrusive ServiceReportTarget binds a boot service slot or an
immutable enclosing accounting object before submission. Bind rejects finite
service CPU aggregates with ENOTSUP; unaffiliated processes return0. Reports
validate sequence, known flags, counter/peak monotonicity and final quiescence,
then merge deltas once. Current/live values remain owned by stream snapshots;
finished streams no longer contribute current/live/pending counts. Move repairs
list links under service lock without allocating. Nested target retains remote
object; cancellation releases it after unlink outside service lock. Unfinished
cancellation marks cumulative accounting incomplete unless explicit pre-start/
confirmed rejection cleanup proves no execution stream existed.
Invariants: configured limits/current local charges unchanged by report merge;
CPU counted once in deltas; invalid/duplicate reports cannot mutate counters;
root Task not retained by report destination; local restart history preserved.
Locks: compute callers may hold compute->service; snapshots service->resource;
no service->compute, callbacks, waits or allocation. Atomic remote ref released
outside service lock. Fixed report state and list moves are bounded.
ABI/wire: additive known service Snapshot.flags bit, layout/version unchanged.
Remote observation model remains internal until later wire codec integration.
Tests: host duplicate/monotonic/final/saturation/invalid rollback model; KTEST
multiple streams, moves, root-retirement, pending descendants, current/live
withdrawal and incomplete flags; finite admission refusal without mutation.
Build/test: Normal Build WOS, isolated diagnostics/KTEST, formatting/source guards,
host ASan/UBSan. Rollback this stage's target/model/flag/UI/test additions together.

Stage11y build correction: first builds found servicectl uses the mlibc mirror
sys/service_resource.h, not the kernel ABI header. Add the same new flag constant
to that mirror and an explicit ABI parity assertion; read mlibc instructions and
the exact mirror before editing. Layout/version and syscall behavior unchanged.
This extends the patch contract's expected files to the libc mirror and ABI test.
Host25 resource/report model tests already passed ASan/UBSan; builds need retry.

Stage11y implementation: ServiceReportState validates cumulative observations,
sequence/finality/known flags/monotonic counters and merges only new CPU/denials/
peaks; current/live values stay in each stream's last report. ServiceReportTarget
binds a local immutable service slot or retains an enclosing remote accounting
object, with allocation-free movable intrusive links under service_lock. Nested
pending/current/live observations participate in parent snapshots independently
of root Task lifetime. Finite soft/hard aggregate CPU bind returns ENOTSUP;
unaffiliated bind returns0; no WKI call-site activation yet. Completed targets
withdraw current/live/pending values. Lost unfinished targets mark destination
incomplete; cancel(false) can suppress that only before any report was applied.
ABI Snapshot.flags ACCOUNTING_INCOMPLETE bit mirrored in libc (same size/version)
and displayed by servicectl. Host25/25 ASan/UBSan resource/report tests passed,
ABI parity1/1 passed; logs/tmp/wos-goal22-service-report-{model,abi}-test.log.
Initial compile failed for missing libc flag mirror (fixed). Next normal build
failed in BusyBox/Bash on generated stdint.h include_next, while current installed
header already matched mlibc source; subsequent retry passed without changing
header/build code. Final normal and diagnostic builds exited0; format/tidy final
exited0 with warnings. Logs/tmp/wos-goal22-service-report-target-{normal-build-final,
ktest-build-final,check-final}.log. Init-control/WKI/exec source guards and diff
check passed. Full isolated KTEST packaging now; no Stage11y live result yet.

Next integration findings to preserve:
- Report/live counts must not count a local WKI proxy as an executing process in
  addition to its reported receiver. Mark service membership as remote-executing
  only at accepted handoff; keep physical proxy charges separate as appropriate.
- TASK_COMPLETE resource merge plus exclusion of report-counted denial deltas
  must be atomic against service snapshots. Updating State first and exclusion
  later can publish a transient double count that a monotonic report cannot
  retract. Preserve service->resource lock order (e.g. narrowly scoped service
  observation guard around resource merge/exclusion); never resource->service.
- Bind report destination in an already recovery-owned SubmittedTask row before
  sending. A separate read-only service admission query can reject finite policy
  before FD exports/frame allocation. Avoid a new untracked stack-owned target
  across fatal syscall exit. Track whether transmission was attempted so proven
  pre-send/reject cleanup can cancel(false), while uncertain lost streams remain
  explicitly incomplete. Move/reclaim/fence paths must retain report destination
  until reporting ends, independent of ordinary root Task/result ownership.
- Receiver can extend existing RunningRemoteTask lifetime after root completion:
  retain a RemoteServiceAccounting pointer and mark root completion already
  queued, release root Task/output normally, then keep only reporting state until
  a final report is durably delivered or session retirement marks it incomplete.
  This reuses checked receiver-row publication and avoids another allocation
  queue/registry. Root-completion-done rows must never fall back to PID lookup
  (PID reuse) or queue TASK_COMPLETE again. Existing accept/session eligibility
  applies; an unpublished report row must not emit observations before post
  succeeds, or a later reject could duplicate admission denials.
- Root attach after ELF construction needs explicit original imported denial
  baselines; current attach defaults to present counters. Validate/copy the
  original baseline under service->resource before linking, preserving admitted
  construction denials as new observations. Fork descendants start at zero.
These are source-derived integration requirements, not yet implemented behavior.

Stage11y final validation: full isolated KTEST8615 passed,0 failed, including
ServiceReportsRetainDestinationAndMergeOnce. VM powered off; launcher exited0;
serial saved/tmp/wos-goal22-service-report-target-ktest-serial.log. No owned VM
running. Final diff check passed. Both final builds, format/tidy (warnings),
source guards, host25/25 ASan/UBSan and ABI parity1/1 results recorded above.
Service report destination/model/status foundation is validated. WKI wire and
ownership integration, atomic completion exclusions/proxy observation, strict
negotiation/capability activation and capable/mixed-node evidence remain. Finite
aggregate admission is enforced by the new bind API only; placement call sites
are not connected yet. Goal22 remains active, with no indispensable blocker.

Stage11z patch contract (atomic task/service observation integration):
Previous turn progress: report target/model/status code and KTEST8615/0.
Source mapping read current submit row publication/wait setup, accepted proxy
handoff, winning completion merge, receiver publication and RunningRemoteTask
lifetime. Before adding wire reporting, prevent transient/durable double counts.
Expected files: service_resource.hpp/.cpp, resource_contract.hpp flag,
remote_compute.cpp, service/WKI KTEST, source guards and docs.
Problem: service reports and TASK_COMPLETE describe overlapping root denials/CPU;
merging root State then separately excluding report-counted observations permits
a service snapshot to publish a double count that cannot be retracted. Root
receiver attach after ELF construction must exclude original imported history,
not new admission denials. Proxy and reported receiver must not both count as
executing members once handoff commits.
Implementation: scoped service->Task.resource guard around completion State merge
and exclusion of actually-added (post-saturation) denial increments. Local and
remote membership observations both subtract exclusion baselines; CPU service
merge is skipped only for explicitly service-report-bearing submissions. Add a
reserved resource flag for that staged contract; no submitter emits it yet and
capability remains unadvertised. Accepted service-report proxy handoff marks
membership remote-executing for live/current service observation only, leaving
all physical charges untouched. Receiver attach accepts an explicit original
State history, validates denial baselines under lock before any membership/ref
mutation; descendants retain default fresh-history behavior.
Locks: compute->service->resource permitted; guard releases resource then service
before child-event locks, service CPU calls or network/cleanup. No allocation,
wait/callback/log while held. Fixed7 counters only. Subject lifetime held by
existing finalizer/constructor. Invalid resource completion leaves exclusions
unchanged; baseline exceeding current rejects attachment atomically.
ABI/wire: add known submission-only resource flag; record size/version unchanged.
Tests: actual WKI completion with reports arriving before/after it counts once;
proxy live/current exclusion; imported vs admission denials and invalid attach;
saturated denial increments preserve local observations. Normal/diagnostic builds,
format/source/host wire tests, isolated rootless KTEST. Rollback stage together.

Stage11z implementation: ServiceResourceMerge acquires service then Task resource
lock, captures7 denial counters and excludes only actual added increments after
a validated resource completion merge (including saturation); repeat exclusion
within one guard is idempotent. Local and remote member snapshots/retirement use
adjusted observations. Guard destruction releases resource then service before
child-event/service CPU paths. Baseline WKI_RESOURCE_SERVICE_REPORTS flag selects
independent service CPU reporting and atomic denial exclusion; accepted flagged
proxy handoff marks service member remote-executing. Service current/live views
exclude that shadow; physical Task charges are unchanged. The flag is known but
not emitted yet; resource size832/version2 and capability advertisement unchanged.
RemoteServiceAccounting::attach accepts original imported State history, rejects
any denial baseline exceeding current values before membership/ref mutation, and
copies baselines only. Fresh descendant inheritance uses present reset counters.
KTEST adds real WKI merge/report-first and completion-first exactly-once cases,
invalid completion no-exclusion, imported/admission baseline separation, invalid
attach, proxy observation/physical-charge preservation, saturated exclusion and
local service retirement without duplication. Host19/19 wire tests passed
ASan/UBSan (/tmp/wos-goal22-service-atomic-wire-test.log); final normal/diagnostic
builds exited0; format/tidy initial+final fixture exited0 with warnings; WKI/init/
wait source guards and diff check passed. A source guard initially used the
function parser for a destructor (unsupported); replaced with a scoped destructor
match and retained reverse lock-order assertion. Full isolated KTEST packaging.
Logs/tmp/wos-goal22-service-atomic-{normal-build-final,ktest-build-final,check,
final-fixture-check,ktest-launch}.log. Goal remains active; no runtime blocker.

Transport integration source notes (next required stage, not additional unused
scaffolding): TASK_SERVICE_REPORT can use the next free compute message0x57 on
RESOURCE channel. A bounded fixed report (<one Ethernet frame) can be handled
inline with existing exact channel-generation validation and ServiceReportTarget
apply (no allocation/blocking); inspect all dispatch/classification/diagnostic
switches before adding MsgType. Use existing reliable channel copy/retransmit
ownership for final report durability; verify whether an application ACK is
actually needed before inventing a second acknowledgment/tombstone protocol.
SubmittedTask currently has explicit move constructor/assignment and centralized
reclaim predicate. Add target ownership/pending report hold there; successful
root wait/result consumption must preserve the report hold. Sender's async-wait
publication block is the last lock-held point before send_task_submit_payload;
record transmission-attempt state there for fatal/timeout cleanup. Root executor
cleanup scans result_owner_task/local_task, clears both on consume and therefore
can leave a report-only row independent of Task lifetime. Known TASK_REJECT can
clear an unstarted report hold without incomplete status; uncertain send/timeout/
fence must retain or cancel with explicit incomplete state. Current peer cleanup
predicate only matches submitted.active; extend for pending report-only rows
without overwriting a previously completed root exit status or re-finalizing it.
Receiver RunningRemoteTask currently owns monitor Task/output until durable
TASK_COMPLETE queueing. Keep that existing behavior, then retain a report-only
row/accounting pointer and skip PID lookup/root completion once queued. Cancel/
session retirement, failed row publication and post_task failure must release
accounting exactly once. No new service report code is wired yet.

Stage11z final validation: full isolated KTEST8617 passed,0 failed, including
ServiceObservationMergePreservesLocalDenials and ServiceCompletionAndReportsCountOnce.
VM powered off and launcher exited0; full serial saved at
/tmp/wos-goal22-service-atomic-ktest-serial.log. No owned VM remains running.
Final diff check passed. Normal/diagnostic builds, host19/19 ASan/UBSan wire tests,
format/tidy (warnings) and source guards passed as recorded above. Atomic task/
service observation integration is validated. Next connect the service report
transport and row lifetimes, then strict negotiation/capability activation and
capable/mixed-node live evidence. Goal22 active; no indispensable blocker.

Stage11aa patch contract (service report transport and submission lifetimes):
Problem: validated service accounting has no production WKI delivery/lifetime.
Files read: remote_compute submit/receiver/completion/cleanup/session/reclaim,
remote_compute.hpp moves/rows, wki.cpp reliable dispatch/send retransmit ownership,
service_resource target/model/snapshot, resource wire codec and tests.
Expected files: those WKI/service files, new service_contract.hpp, host/KTEST,
docs and ledger. Flow: capability-gated SERVICE_REPORTS submission binds a durable
row target; receiver attaches a retained accounting object before publication;
periodic fixed reports follow the exact reliable channel generation. Root result
consumption is independent of final subtree reporting. Final reliable send has a
recoverable retransmit copy before WKI_OK; inline bounded report merge needs no
second application acknowledgment. Lost sessions cancel as incomplete.
Invariants: no root Task retained solely for report destination; root wait not
held for descendants; no repeated root completion/PID lookup in report-only rows;
known reject clears unstarted reports; unknown abandonment cannot claim complete.
No finite distributed CPU enforcement claim; reject before exports with ENOTSUP.
Locks: compute->service->resource, no reverse order; no send under compute lock.
RX merge allocation-free; receiver send copies fixed attempt and revalidates row
identity/session/sequence after unlock. Checked account allocation before publish;
every failed publication/post/discard releases its monitor ref exactly once.
ABI: append compute MsgType0x57 and fixed version1 report; existing resourcev2
size832, base messages and service ABI unchanged. Capability stays unadvertised.
Validation: wire malformed/unaligned/finality tests, actual handler session and
reclaim/peer cleanup KTEST, receiver root/report lifetime tests; source guards,
format, normal/diagnostic builds, isolated rootless KTEST. Rollback stage together.

Stage11aa implementation/validation: TASK_SERVICE_REPORT0x57/version1/384B is
recognized by reliable/lifecycle dispatch, chaos classification and Wireshark
name catalog. Exact node/channel/generation + fixed unaligned decode feed the
existing monotonic target merge. Source rows retain report-only ownership after
root result consumption; final merge frees that hold, known reject cancels clean,
unknown transmission/executor loss/peer retirement marks incomplete. Inactive
root peer cleanup preserves terminal status. Read-only finite service CPU
admission rejects before FD exports; reporting flag emitted only with negotiated
resource contract. Receiver creates/attaches accounting before publication,
retains monitor in checked RunningRemoteTask row, releases on failed publication/
post/discard, and releases root/output after ordinary completion queueing while
keeping descendant/nested observations. Periodic fixed attempts claim sequence,
unlock for reliable send, revalidate row/session, retry failure, release final
monitor only after WKI_OK owns retransmit copy. Report-only cancel avoids reused
root PID lookup. No additional process-tree kill semantics. Resource cap staysoff.
Review caught cancel_report inserted in ACCEPT instead of REJECT; corrected and
added direct accept+reject regression paths before final builds/live validation.
Existing source guards updated for extracted root-release helper, retaining
allocation-before-release and adding report/admission/identity/reclaim checks.
Host20/20 ASan/UBSan wire tests passed; normal and isolated diagnostic final builds
exited0; format/tidy initial+final exited0 with warnings; WKI/init/wait/Wireshark
source guards and diff check passed. KTEST8618 passed,0 failed, powered off;
launcher33549 exited0. Serial/tmp/wos-goal22-service-transport-ktest-serial.log.
Logs/tmp/wos-goal22-service-transport-{normal-build-final,ktest-build-final,
wire-test,source,check,check-final,ktest-launch}.log. No owned VM remains running.
Goal active; strict capability gates/advertisement and capable/mixed-node evidence
remain, then full completion-contract audit. No indispensable blocker.

Stage11ab patch contract (strict resource negotiation and advertisement):
Files read: WKI startup capability mask/token parser/negotiated helper; wire cap
and reject enum; both submit helpers, placement before export, receiver dispatch
and validation-before-fetch, direct-call search (production callers carryTask).
Problem: legacy/no-cap placement can currently omit resource policy entirely.
Expected files: wki.cpp, remote_compute.cpp, wire.hpp comments, tests/docs.
Flow: advertise completed resourcev2+service-report support by default; diagnostic
wki.resource_limits_off token removes only this capability. All new-kernel remote
submissions require exact negotiated capability and a process policy owner; strict
placement returns ENOTSUP for incompatible peers, allowed local fallback preserves
policy. Receiver refuses absent capability or absent resource identity before any
ELF fetch/admission. Capoff never re-enables legacy execution bypass locally.
Invariants: existing base wire/ABI unchanged; old peers receive explicit unsupported
rejection, unrelated capabilities untouched; no FD/VM/process effects beforegate.
Locks: negotiated helper already used before submit compute lock; fixed checks,
no new lock ordering. Receiver gate outside compute lock after session validation.
Unsafe contexts: no new allocation/blocking in gate; transmission usesexistingflow.
Validation: direct-call/capoff/receiver-version boundary tests, source ordering,
normal/diagnostic/KTEST, then capable and mixed rootless multi-node live workloads.
Rollback gates+advertisement together; never advertise a partially enforced path.

Stage11ab builds/isolated validation: normal and diagnostic builds exited0;
format/tidy exited0 with warnings; source checks passed. KTEST8618/0 passed
with new advertised capability, direct null-owner/incompatible-peer refusal,
and negotiated identity-size boundary tests. VM powered off/launcher4106exit0;
serial/tmp/wos-goal22-resource-negotiation-ktest-serial.log. Logs use
/tmp/wos-goal22-resource-negotiation-{normal-build,ktest-build,check,source,
ktest-launch}.log. Local enforcement remains enabled with capoff diagnostic.

Live cluster19 launched rootlessly using /tmp/wos-goal22-cluster19.json. One
node failed root mount with EAGAIN while WKI VFS mounts were being discovered;
init continued with initramfs and journald ENOENT. Initial tail suggested a stall,
but full log and GDB Task inspection showed this explicit mount failure. AllCPU
idle backtrace and init saved USER_RETURN/Sleep frame preserved in cluster19.
No resource-bearing remote execution had run on the failed node. Both ownedVMs
were stopped via QMP with PID/start-tick/overlay verification. No source fix or
claim of solved boot race. Existing host TAP read initially showedDOWN, yet the
launcher validated topology and acquired DHCP; launch environment evidence wins.

Cluster20 uses fresh private overlays, both nodes --debug-node halted, QMP net1
linkdown before cont, then linkup after both pivot_root and DHCP succeeded. This
avoids discovery overlapping the local root mount. Normal Build WOS source image,
8G/8CPU each, nodeswos-0(10.10.0.235)/wos-1(10.10.0.234). Strict on wos-1 hostname
returnedwos-1. Full remote matrix160/0 inclNPROC29/0; seven source finite policies
matched remoteprocfs: CPU3:4,FSIZE65536,STACK524288,CORE0,NPROC64,NOFILE8:256,
AS134217728. RemoteCPUwait10/0 andCPUobservations14/0 passed. Backgroundroot returned
with live5; eight descendant250000us runs remained afterroot and serviceCPU rose
15965419->18067552us, live4. Nested0->1->0 CPUobservation completedstatus0 when
nested inheritedIPCproxies were replaced by supported/dev/null FDs; re-exporting
proxy FDs correctly returnedENOTSUP. Receiver marker confirmsstatus0 and eight
background250000us outputs. Logs /tmp/wos-goal22-cluster20-{smoke,resource-matrix,
policy,cpu-observation,background,nested-marker,receiver-markers}.log.

Cluster20 also exposed spontaneous peer fences/reconnections at node1uptime125,
156,277s; source service flags becameACCOUNTING_INCOMPLETE, preserving knownlower
bounds as designed. Need distinguish protocol regression from preexisting peer
contention before claiming complete live telemetry. Source inspection: newreport
was unnecessarily classified with exclusive peer lifecycle; HELLO/ACK attempt a
nonblocking lifecycle acquisition and fence on failure. Frequentreports can add
contention there. Report merge itself only needs compute->service: peer retirement
cancels the same stable source row undercompute, so apply-before-retire or ignored
after-retire is already serialized and no channel/session creation occurs.

Stage11ac narrow patch contract: remove TASK_SERVICE_REPORT from exclusive
message_uses_compute_lifecycle classification; retain reliable authenticated RX,
exact node/channel/generation checks, stable report-holding rows and compute-lock
serialization againstpeer cleanup. No auth/HELLO changes and no newlock ordering.
Filesread wki.cpp lifecycle/discovery/reorder/dispatch, peer.cpp HELLO/ACK/fence,
remote_compute reporthandler+peer cleanup; expected wki.cpp/sourceguard/docsledger.
Allocation/blocking unchanged, no ABI/wire change. Validate sourceguard explicitly
excludingreport fromlifecycle, bothbuilds/KTEST then fresh capablelive >HELLOperiod
andmixedcapoff evidence. Rollback onlyclassifierchange if lifecycleinvariantfails.

Stage11ac validation: normal and diagnostic builds exited0; format/tidy exited0;
WKI/Wireshark source guards and diffcheck passed. Full KTEST8618/0 passed and
poweredoff; launcher82263exit0. Savedserial/tmp/wos-goal22-report-lifecycle-ktest-serial.log.
Build/check/launch logs/tmp/wos-goal22-report-lifecycle-*.log. Classifier correction
now documented and protected by sourceguard; complete service telemetry/reconnect
comparison still requires fresh live evidence. No auth/HELLO source changes.
Prepared disposable rootfsbacking/tmp/wos-goal22-remote-service-base.qcow2 with
newremote servicefixture, and bootbacking/tmp/wos-goal22-capoff-boot.qcow2 whose
only bootconfigdifference is wki.resource_limits_off. Cluster21capable and
cluster22mixedconfigs under/tmp useprivateoverlays. Defaultmanifest/normalbaseand
KTESTdisks notmodified byfixtureinjection. Both nodesdebughalt forlinkdownboot.

Cluster21 capable results (classifier correction): finite CPU2 service strict
onwos-1 rejectsENOTSUP (journal at89.902s; earlier pre-discovery launch correctly
failedEHOSTUNREACH); fallbackservice exited0, bothlive0/CPU2:2unchanged. RemoteCPU
signal matrix9/0 passed. Long-lived descendant root returned atserviceCPU65386us;
after8*250000us work, serviceCPU2168230us/live5 and netdiag submitted1/active0
proved report-only row retention. However node1 fencednode0 at149s whilechild
slept, then afterchildretirement sourceflag becameincomplete/submitted0. Therefore
classifierchange didnotresolve spontaneousfencing; don'tclaim complete telemetry.
LiveGDB onnode1 setonebreak atwki_peer_fence, capturedanatural recurrence anddetached.
Trace/tmp/wos-goal22-cluster21/fence-break.log shows caller wki_rx HELLO branch
wki.cpp3812, not heartbeat timeout. Lifecycle statewas0 atfenceentry (couldhave
cleared afterfailedtry); noauthsecretsprinted. This provesHELLOpath, notyetwhether
trylockcontention orauthinstall failure. Stresscommand/remotecpu returned0.

Stage11ad patch contract (busy discovery refresh must not fence):
Readwki_rxHELLO/ACK/CONFIRM, PeerLifecycleTryLease, peer.cppHELLO/ACKcontact early
returns andlifecycleCAS, auth.cppinstall/prepare active/retired-session checks,
peer/routing sourceguards. Problem: HELLO andACK currentlyconflate inabilityto
acquireexclusive lifecyclewithactual authfailure. A busy authenticatedpeer must
allowunreliable discoveryretry; itisnot evidenceforretiringallresourcecontracts.
Expectedwki.cpp andpeer sourceguard. Earlyreturnwhenlifecycletryacquirefails,
thenperformexactexistingauthinstall/prepare+success/fencebranches. Noauthvalidation,
key/session/nonce/replay changes; noleaseborrow, noalloc/block/log onbusyRX. Lease
stillreleasedbeforesend/fence; normalperiodicdiscovery suppliesretry. Epoch/session
validation unchanged; validframes cannotexecutewithoutauthentication. Validate
sourcebusy-return ordering+existingauth/routingtests, bothbuilds/KTEST, freshlive
reportstream acrossmultipleHELLOperiods. This addressesaconcretecontrolflowbug;
verifywhetheritexplainsobservedfencebeforeclaimingrootcause. Rollbackasunit.

Stage11ad validation: HELLO/ACK returnearlyonbusy lifecycle beforeauthinstall;
actualauthfailure fencingunchanged. Normal+diagnosticbuilds0;format/tidy0;peer,
routing,computesourceguards+diffcheckpassed. Authprotocol/crypto/policy14/14 passed
ASan/UBSan; initialmanualhostcompile missedkern_shim include, correctedcommand
uses tests/host/kern_shim beforekernelheaders. KTEST8618/0 passed,poweredoff,
launcher26747exit0;serial/tmp/wos-goal22-discovery-retry-ktest-serial.log.
Logs/tmp/wos-goal22-discovery-retry-{normal-build,ktest-build,check,ktest-launch}.log
and/tmp/wos-goal22-discovery-auth-tests.log. Alloldclusterlaunchersandprobes exited0.
Freshprivatebackings23/24 rebuilt againstnewnormalimage; priorfixtureoverlaysmust
notbe reused aftertheir normalbackingimagechanged. Cluster23capable,24capoff ready.
Goal remainsactive pendingfreshcomplete reportlong-lifetime/mixedpeer evidence
and final9-contract audit; no indispensableblocker.

Cluster23 stage11ad live: root returned, eight250000us descendants charged
~2.1s additional serviceCPU; after135s nopeerfence/noincomplete flag. FiniteCPU
strictENOTSUP/fallback0 also reproduced. However report-only rows remained
submitted1/active0 and running1/active1 after descendants retired. Read-only GDB
/tmp/wos-goal22-cluster23/members.log confirms seven retained members all
exit_notify_ready=true,cpu_accounting_finalized=true,resource_cpu_threads=0,
refs1,EXITING/DEAD; no IPC rows remained. Task metadata GC must not gate finality.

Stage11ae patch contract:
Problem: RemoteServiceAccounting.snapshot counts every linked Task metadata
record as an unfinished member, so final reports depend on unrelated GC progress.
Expected files: service_resource.cpp, service_resource_model.hpp comments,
remote_compute.cpp lifecycle KTEST, resource documentation and this ledger.
Files read: service model/member/destructor/attach/snapshot/report targets,
exit.cpp cleanup and readiness publication, scheduler final CPU/GC,
LifetimeOwner CPU contributors, remote proxy finalization/collector/report sender.
Invariants: immutable affiliation and retained domain ownership; existing
service->resource lock order; historical peaks/denials merged exactly once;
no final while member exit accounting or CPU contributors are unfinished;
nested report targets independently prevent finality. exit_notify_ready release
occurs after final local CPU or remote merge, FD cleanup and NPROC release.
Locks: snapshot retains existing service->resource; no additional locks.
Unsafe contexts: timer snapshot and existing KTEST; no change to IRQ/exit path.
Allocation/blocking: no new allocation, wait, logging, callbacks or destructor.
ABI/wire/syscall impact: none; members now describes unfinished execution,
not unreclaimed Task metadata; final still requires members0 and children0.
Implementation: count member until exit_notify_ready(acquire) and zero active
resource_cpu_threads; continue observing retained historical counters until
normal destructor folds/unlinks them. Keep metadata references immutable.
Build/test: KTEST keeps exited root/descendant objects alive and proves report
finality, thread gate and unchanged totals after destruction; both builds,
format/source guards, then fresh capable long-lifetime/nested/fence and capoff.
Rollback: revert only snapshot predicate and associated regression/docs.

Stage11ae review extension before final validation: coredump.cpp
finish_dump_observation commits CORE/FSIZE from the deferred daemon after process
exit readiness. Therefore exit readiness alone is insufficient for these resource
observations. Add Task.resource_deferred_observations, increment only alongside
successful retained-owner admission, decrement after successful observation or
cancellation before releasing owner; four fixed core slots bound contributors.
Gate both subtree finality and resource-bearing root completion on zero deferred
observations. No local wait delay, no allocation/blocking additions, unchanged
core reserve/output/durability. Counter release/acquire publishes final counters;
existing request Task reference keeps subject alive. Read full admit/finish and
all finish callsites; every failure/written path already finishes once. Extend
core admission KTEST for retained counter/repeated finish and transport tests
for pending observation gates. Task header/core source are added expected files.

Stage11ae validation: normal and diagnostic builds0, format/tidy0 (existing
warnings), ABI/resource+wire+init host ASan/UBSan71/71. Full isolated KTEST8618/0,
poweredoff, launcher91237exit0; savedserial
/tmp/wos-goal22-report-finality-ktest-serial.log. Logs finality-{normal-build,
ktest-build,check,ktest-launch}.log. Core pending counter gates both process
completion and subtree finality without retaining reporting until metadata GC.
Normal kernel SHA256 0de998bf4c5740ecb9fed2c7d0baffa408f877ef0e5393b0aaabf4b6eeed61f7;
diagnostic 7b7c05fbbe332ac2e9aaffc48425385f2363c073922bc2bcbe0a4ac283f24bf1.
Broader changed-source audit found stale VFS xattr advertisement assertion
requiring xattr as last semicolon-terminated bit; corrected for appended
RESOURCE_LIMITS, xattr semantics unchanged. Docs also corrected stale staged
report flag and pending enforcement language; source emits reports and enforces
allseven rows. Newprivate backings25/26 againstlatest normalimage; fixture
upload initialincorrect/etc/wos/services.conf failed, corrected and checksummed
at source-authoritative/etc/wos-services.conf. No normalfixturemanifest change.

Cluster25 capable latest-image live evidence:
- Full remote resource matrix160/0 (NPROC29/0), CPU signals9/0, CPUwait10/0,
  CPUobservation14/0. Core fixture33296bytes written on receiver; source service
  CORE peak33296 and three admissiondenials observed through subtree reporting.
- Root returned at67590us; aftereight250000us workers source2176301us/live5,
  submitted1/active0. At>135s root/descendant rowsreclaimed, no incompleteflag
  and no spontaneousfence. Other resource probes ran during laterinterval,
  so finalcombinedCPU is not attributed solely to backgroundtest.
- Nested0->1->0 with supported/dev/null FDs, bothrootreturns before15s descendant:
  nestedstatus0, source rows initiallysubmitted2active0/running1active1;
  after22s submitted0/running0/pending0, CPU increased19690294->20009081us,
  descendantmarker250000us, no incompleteflag. Receiverafterwardalsoallrows0;
  itsbackgroundmarkercontains8x250000us. Lifecyclefinalityfix verifiedlive.
- FiniteCPU2 strictservice returnsENOTSUP afterdiscovery; fallbackserviceexit0.
Logs/tmp/wos-goal22-cluster25-{resource-matrix,steady,nested,receiver-final,
finite-services}.log. All14changed Python sourcechecks passed afterxattrassertion
update, /tmp/wos-goal22-final-source-checks.log; superproject+libcdiffchecks pass.

Cluster25 deliberate receiverloss: backgroundrootreturned, one250000us child
reported; source20331379us/live5/submitted1active0. QMPquit verifiednode1 only.
First15s sample precedes mandatorysecondheartbeatprobe, so correctlystillpending.
Afteractualpeerretirement at305s, source20410352us/live4 with explicitincomplete
lowerboundtext, computeallrows0 and IPCexports/proxies/pumps/pending0. KnownCPU
neverreset. Logscluster25-loss-{before,after,retired}.log; node1serialwas stopped
onlyforintentional loss, source no spontaneousfence beforethat. Node0thenstopped;
launcher4439exit0. Cluster26mixedcapoff launch follows, no builds/baseimageedits.

## Completion record — 2026-09-08

Status: Done. The original goal command above is unchanged; the active goal
included the user's appended authorization to run/debug live with --no-setup.

Final mixed-capability evidence (cluster26): node1 boot token removes resource
capability only. Capable->off and off->capable strict `on` each returned
ENOTSUP (shell127); both forbiddenmarker files absent. `remotely` fell back to
the requesting node (hostnamewos-0/wos-1 respectively), retained NOFILE8:8 and
returned0. Disabledpeer local resource matrix160/0 includingNPROC29/0 and
corefixture33296bytes. Both final netdiag snapshots show computeallrows0 and
IPCexports/proxies/pumps/pending0; neither serviceaccounting becameincomplete.
Logs/tmp/wos-goal22-cluster26-{source,receiver,source-final,receiver-final}.log.
BothownednodesstoppedwithverifiedQMP; launcher21969exit0. NoownedtestVM remains.

Final completion-contract audit:

| Contract | Implementation and verification |
| --- | --- |
| 1. Kernel ABI and libc parity | PROCESS48-50 fixed v1 layouts; validated IDs/infinity/soft-hard/hard-raise privilege; libc no fallback. Host ABI parity and boundary/model tests pass. |
| 2. Inheritance and privilege | Process-owner snapshots across fork/clone/spawn, preserved exec state, checked real-UID population transfer and hard raises. Host/KTEST coherence and live exec/privilege cases pass. |
| 3. Seven enforced resources | FD/process reservations, VM/stack transactions, complete core admission, scheduler CPU and file endpoints enforce before irreversible effects; procfs advertises all seven. Live resource160/0 and CPU9/0; core binary verified earlier with WOSDBG and final remote peak observed. |
| 4. Atomicity and unwind | Hidden pair FD publication; checked process/VM admission; fatal-task reservation/IPC/frame cleanup; checked receiver publication/completion retry; immutable monitor/stream refs. Concurrent host tests and kernel allocation/cancellation/rollback cases pass; live final and fence rows reclaim. |
| 5. Safe signals and errno | CPU softSIGXCPU/hardSIGKILL at safe boundaries, stackSIGSEGV/ENOMEM/E2BIG, ASENOMEM, NPROCEAGAIN, fileEFBIG/SIGXFSZ. Existing IRQ/scheduler nonblocking rules and emergency reserves preserved; live signal/fault tests pass. |
| 6. Private process and public service observations | Procfs owner/root checks repeat on cached/inherited reads. Current/peak/denied/reason, lifetime CPU and service cumulative reports share authoritative state. Privacy/drop-UID, wait10/0 and CPUobservation14/0 pass; nested/core service observations and incomplete loss status verified. |
| 7. Declarative init policy | Seven validated manifest limits applied before exec; aggregate CPU spans descendants/threads/restarts. Manifest tests and earlier disposable local service13/0, NPROC0, AS0 and CPU2 persistent restart exhaustion pass. Finite aggregate remote policy explicitly ENOTSUP; fallback remains locally governed. |
| 8. WKI capability contract | Default advertised resource bit, resourcev2 submission/completion/rejection validation, reliable version1 subtree reports, strict receiver gate before fetch. Capable remote160/0, nested/background finality, deliberate loss and bidirectional mixedcapoff rejection/no-side-effect evidence pass. |
| 9. Boundary/rollback coverage | Host71/0 ASan/UBSan; isolated KTEST8618/0, resource matrix160/0, CPU9/0 and lifetime CPU10/0+14/0. Cases include zero/exact/lower/raise/infinity, privilege, concurrent consumers, allocation injection, rejection/timeout/cancel/fence and deferred ownership cleanup; prior stage logs retain the per-resource evidence. |

Changed areas: kernel resource/service ABI and sched state/models; descriptor,
process/thread, VM/stack, VFS file growth and core admission; libc sysdeps and
creation unwind; init manifest/service status and servicectl; procfs/strace;
WKI compute/IPC/wire/session admission and reliable service reporting; host,
KTEST and testprog fixtures; build/install mappings and docs. Per-stage patch
contracts above identify exact files, locks, ABI changes and rollback units.

Final limitations and stale-navigation corrections:
- Service CPU enforcement across nodes is deliberately refused for finite
  aggregate policy; explicit ENOTSUP is the advertised compatibility behavior,
  with locally enforced fallback where requested. NPROC remains node/realUID.
- WKI reports cannot recover work whose node is lost before reporting; known
  cumulative totals remain lower bounds and are explicitly marked incomplete.
- CPU limits retain scheduler-boundary overshoot, not new scheduling classes.
- Live two-node boot exposed a separate local-root/WKI-discovery EAGAIN race.
  Delaying WKI link-up until each root mounted allowed all required scenarios;
  no claim that this boot race or unrelated Task metadata GC was repaired.
- Earlier cache/navigation claims about 8954 application payload, raw RoCE
  availability and default IPC doorbells were stale versus local authenticated
  payload8914/rawRoCEdisabled/doorbell0. Staged unadvertised-report wording and
  portfolio's stale unfinished-goal enumeration are corrected in current docs.
- Formatting/tidy exited0 with existing warnings, not warning-free. All builds,
  test counts and runtime claims above were performed during this logical task.
- No commit/PR created; unrelated cluster/goal27 artifacts and normal service
  manifest preserved. All diagnostic and normal VM workflows stayed isolated.

No remaining resource-governance implementation or indispensable runtime
verification work is open under the nine completion contracts.

Final housekeeping: successful launchers remove live-debug descriptors, so the
initial descriptor-based shutdown audit encountered an absent file. Repeated
read-only /proc cmdline audit by exact owned image-path prefixes confirmed no
cluster25, cluster26 or isolated KTEST QEMU processes remain. Init documentation
sourcecheck and final superproject/libc whitespace checks passed. Goal tool
marked complete after the full contract audit; elapsed logical-task time ~12h24m.

### Repository handoff — 2026-09-08

The user requested committing the completed implementation and recording Done
in the goals folder. Both this file and goals/README.md retain the Done marker.
The mlibc implementation is committed as 3d8da30a (wos: use kernel resource
limits and unwind failed creation); the enclosing WOS commit records that exact
submodule revision and includes this completion record. Generated VM overlays,
routing evidence and other unrelated artifacts are excluded. This handoff adds
no implementation changes; whitespace checks pass, with build/runtime validation
retained from the completed implementation above. No push or PR was requested.
