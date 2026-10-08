# Goal 39: Isolated WOS Service Compartments

## Copy-pastable goal command

```text
/goal Implement WOS service compartments with independent mount/PID/IPC visibility, explicit capability inheritance, namespace-aware process lifecycle and a usable compartment launcher. Verify every acceptance criterion in goals/39-isolated-service-compartments.md through executable isolation and escape tests, ABI checks, Normal Build WOS and user-run simultaneous-service evidence. Preserve host init behavior, existing credentials/resource limits, stable VFS lifetime and WKI authority. Bound work to the specified compartment model without claiming Linux container compatibility; use checkpointed bounded subagents and follow the file's evidence and blocked rules.
```

## Outcome and scale

Run two services on the same WOS kernel with independently owned mount, PID and
named-IPC views, bounded resource budgets, and explicit inherited capabilities.
**Target: one to two continuous weeks with VFS, process/IPC, and userspace/test
subagents.** The deliverable is a usable isolation boundary and full lifecycle,
not a renamed chroot helper.

**Prerequisite:** reconcile the in-progress immutable-credential work in goal
19 with its owner before modifying shared security/Task surfaces. Reuse existing
resource governance. Virtualization goals are independent; compartments may
later contain VMM processes but that integration is not required here.

## Baseline and impact map

On 2026-09-08, `modules/kern/src/vfs/mount.cpp` has a file-local mount registry,
`mount_lock`, generation and pivot epoch. `platform/sched/task.hpp` has a
per-process root/cwd, but a root path does not provide a private mount table.
`platform/sched/scheduler.hpp` describes system-wide PID allocation.
New credential/authority code is present in the dirty working tree; it is not
safe to overwrite it or infer its completion from a plan.

Read `modules/kern/src/vfs/{vfs.hpp,core.cpp,mount.cpp,mount.hpp,file.hpp,lookup.hpp,fs/procfs.cpp}`,
`modules/kern/src/platform/sched/{task.hpp,task.cpp,scheduler.hpp,resource.hpp}`,
`modules/kern/src/platform/security/credentials.hpp`,
`modules/kern/src/syscalls_impl/process/{process.cpp,getpid.cpp,getppid.cpp,waitpid.cpp,exit.cpp,exec.cpp}`,
`modules/kern/src/syscalls_impl/{shm/shm.cpp,futex/futex.cpp}`,
`modules/kern/src/net/wki/{remote_compute.hpp,remote_ipc.hpp,remote_vfs.hpp}`,
and the relevant syscall dispatch/ABI/libc wrappers. For launch integration,
read `modules/init/CMakeLists.txt`, `modules/init/src/init.cpp`,
`modules/CMakeLists.txt`, and `configs/rootfs/aliases.tsv`.

Flow: launcher validates compartment spec → creates retained namespace objects →
constructs service task before publication → lookup/PID/IPC operations use its
namespace identity → child creation inherits that identity → final teardown
joins tasks and drains namespace-owned mounts/IPC. Existing explicitly inherited
FDs remain capabilities; path isolation must not magically revoke them.

## Checkpoints and parallel ownership

1. Define namespace object ownership, root/init namespace compatibility,
   authority ceiling, FD inheritance and support boundaries. Primary owns Task
   and ABI fields; independent subagents map mount, PID and IPC consumers.
2. Implement private mount views and process namespace translation with actual
   pathwalk/lookup tests, not just a detached model.
3. Integrate named IPC, procfs, signals/wait/ptrace policy, compartment init
   lifecycle, launcher and resource inheritance.
4. Run a hostile sibling/child test matrix and user-run concurrent services;
   independent review checks indirect references and WKI escape paths.

## Acceptance criteria

1. Two compartments mount different backing objects at the same path, and
   mount/unmount/pivot operations in one do not change the other's view.
   Root host behavior remains unchanged.
2. Compartment-local PIDs, parent IDs, process groups, procfs enumeration,
   signals, wait/reaping and ptrace all follow one documented translation and
   authorization policy. PID reuse cannot target a stale foreign task.
3. Namespace-init death has a bounded defined policy for descendants and zombies;
   teardown releases every retained task/namespace reference.
4. Named shared memory and named IPC lookup are compartment-scoped. Inherited
   descriptors and explicitly shared mappings remain intentional capabilities.
   Futex keying follows underlying shared-object identity without accidental
   cross-compartment aliasing.
5. Entry/creation cannot increase credentials, authority ceiling, CPU/memory/FD
   limits or visibility. Unprivileged callers cannot join a more privileged
   compartment through an integer handle or guessed PID.
6. Path escape tests cover `..`, absolute and relative symlinks, stale cwd,
   retained directory FDs, mount crossings, rename races, procfs links, root
   replacement and forbidden device nodes. Explicit inherited capabilities are
   documented in the launcher manifest and test oracle.
7. Remote execution/IPC/VFS export cannot discard compartment restrictions:
   either carry a negotiated enforceable identity/policy or reject before remote
   side effects. No silent host-namespace fallback.
8. Supply a WOS launcher and versioned spec with bounded parsing, help,
   diagnostics and reproducible service fixtures. Two independent service trees
   run for 2 hours while the host remains operational.
9. Across 1,000 create/exec/kill/destroy cycles including injected allocation
   failures, task, mount, IPC, FD and namespace counts return to baseline.
10. Existing init, filesystem, child-event, credential, resource and libc ABI
    behavior outside a compartment passes regression checks.

## Boundaries, locks, and rollback

No OCI/Docker compatibility, Linux namespace syscall compatibility promise,
user-ID remapping, checkpointing or independent network stack. Network access
is denied by default for the compartment fixture; explicitly granted host-network
or inherited socket access is reported as shared. Do not call this full network
isolation or a replacement for a VM boundary.

Mount/lookup, Task lifecycle, process-group, IPC, FD and credential locks need
one checked order; operations retain namespace objects across blocking work.
Use the subject task, not the executing worker's namespace. Rollback keeps the
root namespace path available and disables new compartment creation; drain
existing compartments before removing namespace-aware state.

## Execution, evidence, and stop rules

Read `AGENTS.md`, the subsystem index, applicable kernel/WKI/critical-path/build
instructions, and the source entrypoints below before changing code. Source
wins over this dated plan. Make an impact map and the full repository patch
contract, including locks, unsafe contexts, ownership, ABI, validation, and
rollback, before each materially different critical patch.

Use a primary integration agent and up to three active subagents with bounded,
non-overlapping assignments; rotate assignments between checkpoints. The primary
owns shared headers, integration, and acceptance. Keep
`goals/progress/39/ledger.md` and `goals/progress/39/evidence.md`:
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

## Current session checkpoint (2026-10-08)

Selected after user-requested Goal28 checkpoint commit70bc3fe7 and goal switch.
Goal28 remains incomplete as a repository checkpoint. The user cleared its
previous Goal Mode record on 2026-10-08 and Goal39 is now active with the full
contract, rootless runtime authorization and storage constraints.

Current source already implements immutable CredentialStore and retained root/
cwd views; the dated dirty-credential premise must not be copied. Full prior
Goal19 authorization completion is not inferred from its retired portfolio row.
Reuse existing checked APIs and map shared native consumers before modifying them.
Profile preparation, private native SysV IPC and native mount ownership are
implemented. Native mount reservations/publication/caches/covered edges,
retirement and pivot use exact retained namespace identity; Task lookup and
single/pair consumption carry the subject's view. Mount/unmount/pivot wrappers
now dispatch to explicit Task APIs: private tmpfs mounting rejects unsupported
sources/backends before effects, unmount retires an exact scoped mount ID, and
private pivot validates native directories and rebases from the current root.
Repeated pivot changes only exact-scope owners while explicit Files keep backing
busy. Host init spelling, bound thread mirrors and WKI export barriers remain.

Checkpoint05 Normal Build WOS, formatter and four existing VFS regression entries
pass. Isolated fast KTEST attempt07 passes 74,224 assertions, zero failures and
normal poweroff (1,271 cases), including all nineteen required IPC/mount/Task/
administration cases. Its four inactive disposable images (256,655,360 allocated
bytes) were pruned after exact ownership/live-reference audit; final disposable
usage is zero. Source/artifact/runner receipts remain pinned. Native fixtures
use actual unpublished Tasks and production backing/path/admin APIs; they do
not qualify scheduled hostile user processes or a usable compartment boundary.

Public compartment admission, remaining root/readdir/procfs consumers, PID and
init-death lifecycle policy, complete network/WKI gates, launcher and required
service campaigns remain pending. All ten full acceptance criteria remain
outstanding. Prior focused host ASan/UBSan cases are foundation evidence, not
full service qualification. See progress/39/{ledger,evidence,reconnaissance,
contract-01-profile,contract-02-ipc,contract-03-mount-registry,
contract-04-task-lookup,contract-05-mount-administration}.md and
docs/wos_service_compartments.md.

User explicitly authorizes owned software VM runtime/debug through
bin/wos-cluster --no-setup and isolated bin/wos-ktest --no-setup. This overrides
the older user-run-only software test guidance above. Preserve the shared launch
guard and build/data/sysroot isolation. Maintain one active topology,20GiB
disposable-image budget and40GiB free floor; use immutable required bases and
private COW/reflink images. Audit exact ownership/live process/QMP/FD/backing
references and prune inactive agent-created disposable disks/backings/per-run
appliances after PASS/FAIL/exceptions. Keep small evidence and user/unrelated
disks/source/current artifacts. All seven Goal39 KTEST attempts pruned their exact-owned
inactive disposable images; small logs/configs/manifests/receipts remain.
