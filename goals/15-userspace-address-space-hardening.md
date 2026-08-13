# Randomize and harden every WOS userspace address space

## Planning metadata

- **Target horizon:** one to two continuous weeks of agent time.
- **Execution model:** one primary integration agent and three bounded,
  non-overlapping subagent workstreams.
- **Critical surfaces:** ELF/exec ABI, page-table permissions, VM placement,
  thread/TLS layout, ptrace, coredumps, and debugger symbolization.
- **Completion rule:** the horizon is not a deadline or proof. Only the
  completion evidence below closes the goal.

## Goal command

```text
/goal Outcome: WOS creates collision-safe, per-exec randomized PIE, interpreter, stack, TLS, and anonymous-mapping layouts; supplies real AT_RANDOM entropy; installs guard pages; and enforces non-executable stacks, W^X load segments, and complete RELRO without breaking static or dynamic programs, ptrace, coredumps, WKI execution, or WOSDBG symbolization. Verification surface: prove the layout and protection contract with executable loader/VM tests, repeated-exec entropy measurements, deliberate write/execute fault tests, malformed-ELF cases, the normal Build WOS, focused host tests, isolated KTEST, and user-run VM/cluster evidence. Constraints: preserve the existing syscall and ELF ABI, lazy file mappings, COW, mlibc TLS layout, remote-exec placement, page-table ownership, and exact debug load biases; never weaken permissions as a fallback and never expose entropy through logs or dumps. Boundaries: change the kernel entropy/loader/exec/thread/VM/debug plumbing and directly required mlibc or WOSDBG consumers, but do not redesign the scheduler, general allocator, dynamic linker, or unrelated crypto APIs. Iteration policy: first record a deterministic layout/protection baseline, then land the smallest independently testable entropy, placement, guard, permission, and consumer slices; after each slice rerun its negative tests plus affected compatibility gates and select the next action from the first unexplained failure or highest-risk unproved invariant. Blocked stop condition: stop only after exhausting safe local investigation and alternative test paths; report attempted approaches, exact evidence, remaining failing invariant, and the smallest user-run log, platform decision, or external prerequisite needed. Elapsed time, token use, or a successful build alone never means complete.
```

## Why this goal exists

The local tree has much of a modern ELF loader, but important layout and
protection decisions remain deterministic or incomplete:

- `modules/kern/src/platform/loader/elf_loader.cpp::load_elf()` and
  `view_elf()` assign `0x400000` to `ET_DYN` images.
- `modules/kern/src/syscalls_impl/process/exec.cpp::wos_proc_execve()` loads
  `PT_INTERP` at the fixed `0x40000000` base. The alternate process-image path
  in `modules/kern/src/platform/sched/task.cpp` uses the same constant.
- `modules/kern/src/platform/sched/threading.cpp::create_thread()` derives TLS
  and stack addresses from the fixed `0x7fff00000000` top and writes the fixed
  value `0x3000000018` to the TCB stack-canary slot.
- `modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp` starts anonymous placement
  at the constant `MMAP_START` and resets the cursor there.
- Both aux-vector construction paths in
  `modules/kern/src/syscalls_impl/process/exec.cpp` omit `AT_RANDOM`, although
  WOS's mlibc ABI defines it and the runtime loader uses it to initialize
  `__stack_chk_guard` when present.
- `modules/kern/src/dev/random_device.cpp` exposes raw retried `RDRAND` as
  `/dev/urandom`, accepts writes without mixing them, and has no kernel random
  service that exec or VM placement can consume.
- `PT_GNU_STACK` is recorded as debug metadata in the ELF loader but does not
  itself drive a visible stack-permission policy. RELRO enforcement is skipped
  on the kernel's dynamic-load path on the assumption that `ld.so` completes
  it, and selected `.got.plt` pages remain writable.
- WOSDBG and kernel accounting contain fixed low-address assumptions, including
  `tools/wosdbg/debug_analysis_service.cpp::K_USER_SPACE_START` and hard-coded
  example ranges. Randomization therefore needs an end-to-end consumer audit,
  not only a changed load constant.

These are evidence of a cross-subsystem hardening gap, not proof that every
listed fixed address is independently exploitable.

## Completion contract

The goal is complete only when all of the following are evidenced:

1. A documented kernel entropy service has a defined seed/readiness/failure
   contract, does not expose raw hardware output as a cryptographic generator,
   and supports bounded early-exec draws without allocation or logging secrets.
2. Every successful exec places PIE, interpreter, initial stack/TLS/SafeStack,
   and the initial anonymous-map cursor at aligned randomized locations chosen
   from documented non-overlapping windows with minimum entropy targets.
3. Placement is overflow-safe and collision-aware. It behaves predictably for
   `ET_EXEC`, large TLS, many threads, shared address spaces, hints,
   `MAP_FIXED`, lazy file mappings, fork, and exec failure rollback.
4. Unmapped guard regions protect the initial stack, thread stacks, TLS and
   SafeStack boundaries. Normal bounded stack growth still works and cannot
   silently consume a guard.
5. Exec supplies at least 16 protected random bytes through `AT_RANDOM`, and
   mlibc initializes distinct process stack guards without a constant fallback
   on a healthy boot.
6. The loader rejects overflow, overlap, writable-plus-executable final pages,
   contradictory segment permissions, and executable-stack requests that
   violate the selected WOS policy. Dynamic and static RELRO becomes read-only
   after relocation with an explicit lazy-binding compatibility decision.
7. `/proc`, ptrace, coredumps, perf, memacc, and all three WOSDBG interfaces use
   actual load mappings/build IDs rather than guessed bases. Symbolized traces
   remain exact for PIE, the interpreter, shared objects, and WKI remote tasks.
8. A machine-readable final report records entropy samples, protection tests,
   compatibility cases, commands, artifact paths, and any deliberately retained
   limitation. There are no unexplained failures or silent protection fallbacks.

## Invariants and constraints

- Do not alter syscall numbers, aux-vector numeric values, mlibc TCB offsets, or
  ELF structure layouts.
- Preserve `ET_EXEC`, PIE, static, dynamic, TLS, SafeStack, COW, fork, exec,
  lazy-segment, and WKI remote-exec behavior.
- Address selection must hold the same shared-VM publication protection used by
  current mmap reservation; no check-then-map race between sibling threads.
- Do not allocate, sleep, or perform VFS I/O while page-table, runqueue, or
  loader debug-registry spinlocks are held.
- Never print seeds, `AT_RANDOM` contents, canaries, or generator state. Define
  coredump treatment for the random aux-vector bytes and retired task state.
- Final permissions may only become stricter during loader finalization. Failure
  to enforce W^X, guard mappings, or RELRO must fail the exec transaction cleanly.
- Keep KTEST state isolated in `build-ktest/` and `ktest-data/`.

## Boundaries

In scope are the kernel random service needed by exec, ELF load-range
calculation, interpreter and stack/TLS placement, mmap cursor seeding, guard
mappings, final ELF permissions, aux-vector construction, exec rollback, and
direct ptrace/proc/coredump/WOSDBG consumers. Small mlibc changes required to
consume the now-correct ABI are also in scope.

Out of scope are a general TLS ABI redesign, a scheduler or allocator rewrite,
transparent full-disk/process-memory encryption, unrelated kernel cryptography,
and broad dynamic-linker feature work. If a prerequisite in one of those areas
is unavoidable, document it and ask before broadening the goal.

## Parallel workstreams

- **Primary integration agent:** owns the impact map, critical-path patch
  contract, address-window and entropy policy, cross-workstream interfaces,
  staged merges, and final evidence ledger.
- **Subagent A — entropy and placement:** audits available hardware/platform
  entropy, implements the bounded kernel generator interface, randomized range
  selection, collision handling, and statistical tests.
- **Subagent B — loader and VM protections:** owns ELF validation, PIE/interpreter
  bias, stack/TLS guards, W^X, `PT_GNU_STACK`, RELRO, rollback, and negative
  fixtures without touching debugger consumers.
- **Subagent C — ABI consumers and validation:** owns auxv/mlibc integration,
  proc/ptrace/coredump/WOSDBG mapping awareness, target probes, and reports.

No two agents edit the same critical file concurrently. The primary agent
integrates each slice after re-reading the current diff and rerunning its gate.

## Verification plan

- Run the normal **Build WOS** task and formatting checks for every touched path.
- Extend host loader/VM/source tests with malformed and conflicting ELF layouts,
  range arithmetic, permission reduction, guard boundaries, and mapping metadata.
- Add isolated KTEST cases for deterministic seeded placement, collisions,
  rollback, page faults on NX/RELRO/guards, shared-VM reservations, and fork/exec.
- Cross-build target probes that exec repeatedly and record only addresses—not
  secrets—to validate variation, alignment, window coverage, and non-overlap;
  include static, PIE, dynamic, TLS-heavy, threaded, forked, and WKI-launched cases.
- Test deliberate writes to RELRO and deliberate execution from stack/data in
  disposable processes, proving the expected signal/exit without kernel failure.
- Have the user run the VM and multi-node WKI cases and provide serial logs,
  coredumps, and WOSDBG results when runtime evidence is required. Agents do not
  claim runtime success from static tests or build output.

## Iteration policy

Start by recording fixed-layout and permission behavior with executable probes.
Land one falsifiable slice at a time: entropy API, placement primitive, PIE and
interpreter, stacks/TLS/guards, mmap, auxv/canary, W^X/RELRO, then consumers.
After every slice, rerun the narrow negative tests and the affected compatibility
matrix. Choose the next action from the first regression, unproved invariant, or
highest-risk boundary—not from elapsed time. Keep an evidence ledger and revert
or bisect any slice whose safety cannot be explained mechanically.

## Blocked stop condition

Blocked means all safe source inspection, deterministic tests, alternative
instrumentation, and non-runtime work have been exhausted and progress now
requires a user-only VM/cluster run, an explicit security-policy choice, missing
hardware behavior, or an external dependency. Report the precise failing
invariant, attempted paths, smallest reproducer, current artifacts, and exact
input needed. Do not mark complete because a budget expired or most cases pass.

## Staging and rollback

Keep each stage independently revertible behind an internal compatibility switch
until its tests and consumers land. Retain the deterministic placement path only
as an explicitly test-only seeded mode, never as a production fallback. A
rollback must restore the previous coherent exec/VM behavior and remove partial
ABI consumers together; it must not leave mixed randomized bases, guessed debug
symbols, relaxed permissions, or exposed entropy.

## Completion record — 2026-08-13

- **Status:** Completed.
- **Live execution note:** I am able to run and debug WOS live without root
  access by appending `--no-setup` to the `wos-ktest` and `wos-cluster`
  scripts.
- **Implementation:** WOS now uses an IRQ-safe kernel entropy service and
  collision-aware per-exec placement for PIE, interpreter, stack, TLS,
  SafeStack, and anonymous mappings. Guard pages, protected `AT_RANDOM`, NX,
  W^X, complete RELRO, transactional VM publication, and exact runtime image
  catalogs cover kernel, mlibc, ptrace, procfs, coredumps, perf, memacc, WKI,
  debugserver, and WOSDBG consumers.
- **Verification:** The Normal `Build WOS` passed. Isolated
  `bin/wos-ktest --no-setup` completed with 7,410 passed and zero failed. A
  rootless four-node `wos-cluster --no-setup` run proved local and WKI remote
  execution, fault probes, exact image catalogs, coredump redaction, and PIE
  symbolization. Focused host tests, 49 WOSDBG semantic cases, malformed-ELF
  and malformed-DSO cases, formatting, and schema validation passed.
- **Evidence:** `tests/security/address_space_hardening_report.json` records 64
  repeated-exec layout samples, six protection tests, six malformed-ELF tests,
  twelve compatibility gates, artifact hashes, no retained limitations, and no
  recorded entropy bytes.
