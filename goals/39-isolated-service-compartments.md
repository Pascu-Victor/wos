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

## Current session checkpoint (2026-10-10)

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

Retained File directory reads now enumerate the File's exact mount view, recover
native directory identity after rename, preserve inherited backend capabilities,
and suppress synthetic names after namespace closure. Host WKI aliases use an
explicit captured host root; private `wki` names remain ordinary. File observer
fallbacks refuse replacement mount owners. Procfs mount reads and SEEK_END capture
the subject process's exact namespace/root and preserve caller output/cursor on
failed observations; private procfs PID/link support is still absent.

Checkpoint06 Normal Build WOS, isolated fast diagnostic build, formatter and
seven existing VFS/libc CTest entries pass. Actual isolated KTEST attempt09 passes
74,332 assertions, zero failures, 1,274 cases and normal poweroff, including all
22 required compartment cases. Four inactive images (256,786,432 allocated bytes)
were audited and pruned; final disposable usage is zero. Attempt08's typed-error
assertion failure and audited cleanup are retained as failed evidence. The broader
credential source inventory has stale expectations reproduced on the committed
baseline; its existing focused root-metadata observation and native credential
KTEST cases pass, without claiming the entire source inventory passes.

Native PID namespaces now retain exact Task ownership and reserve monotonic local
process/thread IDs. Canonical child/thread inheritance and requester-aware
scoped registry queries preserve host physical IDs. Private scoped/physical/
active registry publication serializes with namespace closure; failed or foreign
admission makes no registry writes. Named live and zombie queries are distinct,
and GC rechecks its final reference and withdraws all registry visibility under
one guard before dead-list detach. Heavy cleanup and existing epoch/current/
waitable-zombie checks remain. Six actual unpublished Task fixtures cover pool,
name/reuse, child/thread, host compatibility, native probe-chain repair and exact
zombie/active/host pin withdrawal. They do not make fake user Tasks runnable.

Checkpoint07 Normal Build WOS, isolated fast diagnostic build, formatter, nine
scheduler/resource and seven VFS/libc CTest entries pass. Final actual KTEST
attempt12 passes 78,598 assertions, zero failures, 1,280 cases and normal poweroff,
including all 28 required compartment cases. Four inactive disposable images
(257,114,112 allocated bytes) were audited and pruned; final disposable usage is
zero. Attempt10's host retirement-binding regression was corrected and its failed
evidence/prune receipt retained; attempt11 passed before the final GC amendment.
The 1000 native namespace reserve/close/drop cycles are owner-pool evidence, not
1000 service create/exec/kill/destroy cycles.

Durable local SID/PGID values now accompany physical job identities under the
same store lock and inherit together across fork/clone/spawn/thread construction.
GETSID/GETPGID select exact requester names and pin canonical thread owners.
SETSID/SETPGID/spawn groups resolve exact-scope members, with closed admission
serialized against lifecycle commit. PTYs retain value-only namespace stamps
and local foreground names while preserving physical terminal signal identities.
Names survive actual group/session leader Task destruction. Four native tests
cover production session/registry/child-publication and allocated PTY APIs;
public compartment entry remains unavailable.

Checkpoint08 final Normal Build WOS, isolated fast diagnostic build, formatter,
nine scheduler/resource and seven VFS/libc CTests pass. Actual isolated KTEST
attempt14 passes 78,761 assertions, zero failures, 1,284 cases and normal poweroff,
including all32 required compartment cases. Four inactive disposable images
(257,638,400 allocated bytes) were audited and pruned; final disposable usage is
zero. Attempt13's two assertion-width errors were corrected and its failed
source/serial/manual-stop/prune receipts retained. This is native adapter evidence,
not scheduled service isolation or the complete lifecycle campaigns.

Canonical local PID/TID/PPID observations now use retained process ownership and
lifecycle parent membership, including Host-supervised private init PPID0.
Creation returns and initial/replacement/pthread TCBs carry local names; private
fork refreshes its own COW TCB before publication. Private shared-VM clone
requires a separate TCB and refuses physical aliases of parent cache bytes.
Spawn inherits retained root/cwd before interpreter construction. Five native
tests use real Tasks/memberships, user PTE/COW/alias operations and actual initial
Thread creation. Normal Build WOS, isolated fast diagnostic build, formatter,
twelve selected scheduler/resource/libc checks and seven VFS/libc checks pass.
The source oracles were updated for local returns; their failed historical
receipts remain. Actual KTEST15 passes78,906 assertions/0 failures/1,289 cases,
all37 required compartment cases and normal poweroff. Four inactive disposable
images257,703,936 allocated bytes were audited/pruned, leaving zero disposable
usage. This is native creation consumer evidence, not scheduled service proof.

Public wait now selects exact local PID/TID and event-time group names. Queue
subjects retain their scope and names; foreign same-number memberships cannot
supply a public result. Waiter registrations carry the same public/physical view
through ordinary and no-child wakes; local-only group changes version queues.
Closed private claim/registration admission reports EIDRM while existing claims
can finish. Host supervision and trusted/WKI physical selection stay intact.
Private public wait bypasses WKI before distributed effects; returned local
values are copied separately from physical perf correlation. Embedded event/
selector/registration layouts and preceding Task offsets are unchanged.

Checkpoint 10 Normal Build WOS, isolated fast diagnostic build, format-only check,
six wait/usercopy, nine scheduler/resource/libc and seven VFS/libc CTests pass.
The first normal packaging attempt hit sandbox socket-bind restrictions; the
outside-sandbox retry passed. The combined formatter/tidy run hit a read-only
cache path; format-only apply/check passed. The stale TCB usercopy source oracle
was reproduced at the parent commit and updated for the moved helper; failed
receipts remain. Actual KTEST16 passes 79,109 assertions/0 failures/1,295 cases,
all 43 required compartment cases and normal poweroff. Six new fixtures exercise
real event producers/claims/reaping and public-view wakes without posting fake
runnable Tasks. Four inactive disposable images (258,031,616 allocated bytes) were
audited/pruned, leaving zero disposable usage. This qualifies native wait
adapters, not scheduled service syscalls or complete init closure/drain.

Checkpoint 11 adds private positive PID/TID and durable local group signal
selection before Host physical/proxy/WKI discovery. Exact scope admission,
current credential/session checks, stale-image refusal, coherent group capture
and existing local leases precede effects. Closed/unknown/remote requests have
no fallback or pending side effects. Guards release before pending/ptrace/wake
work; admitted effects can finish across closure/route publication using the
existing exec-drained lease. Host and trusted physical signal APIs keep their
behavior. Task/SignalAdmissionIdentity storage and syscall/wire ABI are unchanged.
Seven new native tests use real hidden unpublished Tasks/cohorts/pending state,
with ownership/lease drain and deterministic closure/route/group interleavings.
Public compartment creation remains disabled until the full boundary is ready.

Checkpoint 11 Normal Build WOS, isolated fast diagnostic build, format-only check
and 28 selected Host CTests pass. KTEST17 passes 79,602 assertions with zero
failures across 1,302 cases, all 50 required compartment cases and normal poweroff
with stable source/artifact pins. Four inactive disposable images (258,162,688
allocated bytes) were audited and pruned, leaving zero disposable usage.
Initial Host evidence records an unbuilt
test binary, a baseline-reproduced stale timer assertion and an empty VFS test
selection; corrected builds/oracle/selection pass and old receipts remain.
This qualifies native signal consumers, not scheduled private services or full
namespace-init closure/drain. Goal39 stays active with its full original scope.

Scoped ptrace checkpoint12 now preserves exact local selection, canonical
retained TRACEME parents, current credentials, Host physical inspection and
private local observations. Target/buffer/namespace/effect ownership drains
through ordinary and fatal process/thread cleanup. Eight native cases include
actual mapped public copies/registers, claims, closure/routes and reference
baselines. Normal Build WOS, isolated fast diagnostic build, format-only check
and 34 selected Host regressions pass. Actual KTEST19 passes 79,819 assertions with zero failures across 1,310 cases, including all 58 required compartment cases and normal poweroff.
Both the failed assertion-width VM and successful rerun pruned all disposable
images. Public creation and the full service boundary remain unavailable.

Public compartment admission, remaining root/procfs/positive PID consumers,
namespace-init lifecycle policy,
complete network/WKI gates, launcher and required service campaigns remain
pending. All ten full acceptance criteria remain outstanding. Prior focused host ASan/UBSan cases are foundation evidence, not
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
disks/source/current artifacts. All completed Goal39 KTEST attempts pruned their
exact-owned inactive disposable images; small logs/configs/manifests/receipts remain.


## Checkpoint21 live network qualification (2026-10-11)

Immutable native PID-owner network policy now denies new ambient socket/global
control effects by default, supports nonescalating exact-parent Shared grants,
and preserves explicitly inherited socket capabilities. No public creation or
full profile installer. Normal Build WOS/isolatedfast diagnostics/38hostchecks/
formatter+tidy and actual --no-setup KTEST36 pass82,083assertions/0failures/
1,345cases/all94mandatory/normalpoweroff; sources/artifacts pinned. Failed35
fixture width/raw-protocol evidence retained, corrected without production or
oracle relaxation, all final checks rerun. Both runs' eight disposable images
verified inactive/pruned; peak259,866,624B/final0. See progress39 checkpoint21
record and evidence audit. All ten full criteria remain INCOMPLETE; goal ACTIVE.
Continue exact-subject WKI preeffect admission and full native service creation.
Authorized bin/wos-cluster --no-setup and isolated bin/wos-ktest --no-setup;
one topology20GiB disposable/40GiB free, COW/reflink required seeds/current
artifacts; preserve user/unrelated disks and small evidence, prune inactive
agent disposable images after successful AND failed tests. No pause requested.

## Checkpoint22 WKI admission qualification (2026-10-11)

Actual rich/direct remote compute and Task FD export now refuse unknown, closed
or unsupported private canonical namespace owners before new remote effects.
Private automatic/explicit local placement and intentionally inherited File
capabilities remain available. Existing handoff cleanup, Host behavior and
Task/File/syscall/libc/wire compatibility are preserved.

Normal Build WOS, isolated fast diagnostics, 38 host checks, formatting and two
normal WKI clang-tidy units pass. Rootless KTEST38 passes 82,797 assertions,
zero failures, 1,347 cases and all 96 mandatory checks with normal poweroff and
70 stable runtime pins. Failed37 fixture ownership/KASan evidence is retained;
retained-file correction changed no production policy or expected denial.
Both runs' disposable images were verified inactive and removed; peak allocated
usage was 259,932,160 bytes, final usage zero. See progress/39 checkpoint22
record and audit. All ten full acceptance criteria remain incomplete and the
goal stays ACTIVE. Continue placement/routing/global controls and full native
service construction. Authorized bin/wos-cluster --no-setup and isolated
bin/wos-ktest --no-setup persist with one topology, 20GiB disposable budget,
40GiB free floor and cleanup after passing and failing tests.

## Checkpoint23 WKI target-control qualification (2026-10-11)

Private canonical views now reject unsupported named/remote/balanced target
configuration before usercopy or field effects; local/automatic placement,
modifiers and own configuration reads remain available. Unknown/closed owners
refuse. Host SET/GET, executor field ownership, fork/clone/exec/one-shot behavior
and syscall/libc/wire compatibility are preserved.

Normal Build WOS, isolated fast diagnostics, formatting/clang-tidy and 43 focused
Host checks pass. Actual --no-setup KTEST39 passes 83,210 assertions, zero
failures, 1,350 cases and all 99 mandatory checks with normal poweroff and 70
stable runtime pins. Four inactive disposable images were removed; peak
259,932,160 bytes, final zero. Expanded44Host failure and exact baseline
reproduction remain recorded: two exec identity checks corrected, broader
credential/VFS audit's 24 baseline oracle failures still require investigation.
No full Host regression or full service qualification claim. All ten full
acceptance criteria remain incomplete; goal ACTIVE. Continue remaining ambient
controls/routing/positive selectors, public native construction and required
service campaigns. Authorized bin/wos-cluster --no-setup and isolated
bin/wos-ktest --no-setup persist with the bounded SSD policy and cleanup after
successful and failed tests. No pause requested or performed.


### Checkpoint24 — Host hostname/power admission (2026-10-11)

Exact canonical Host PID/mount/IPC views and existing authority now precede
hostname publication and power prepare/reboot effects. PrivateRoot/Sharednetwork
refuses EPERM, unknownESRCH/closedEIDRM; invalidcommandEINVAL remains before
scheduler retrieval. Actual shared bodies, mapped inputs/canonical threads and
Host harmless/error validation preserve hostname/phase/resources/refs; Host/trusted
backends and Task/File/ABI/wire unchanged. Public creation remains disabled.

Final Normal Build WOS, isolatedfastdiagnostics, formatting/tidy,44focusedHost
checks and71mechanicalpreservation checks PASS. Actual rootlessKTEST41 PASS
83,682assertions/0failures/1,353cases/all102mandatory/88stablepins/normalpoweroff.
Nonfinal40passed, revieworderingfix repeatedallfinalvalidation; exact40source
reconstructedfromparent+patch/hash andtempremoved. Eightinactiveimagesacross40/41
verified/pruned520,257,536B,peak260,128,768B/final0, unrelateduserVMpreserved.
Broadercurrent129credential/VFSoraclecalls still24FAIL/unqualified; nofullHost
claim. See docs/wos_service_compartments.md and progress39/evidence audit24.
Authorized bin/wos-cluster --no-setup / bin/wos-ktest --no-setup persist; oneactive
20GiBdisposable/40GiBfree topology, immutablebases+COW/reflink, exactliveownership/
backing audit andpruningafterPASS/FAIL/exception; retain smallreceipts, notoldVMs.
Goal39 ACTIVE, alltenfullcriteriaINCOMPLETE. Continue routing/globaltargets,
publicconstruction/launcher, actualSHM/sharedfutex/scheduledlifecycle/hostilepaths,
two2hservice trees+1000fullfailurecycles/allrefs andcompleteHost/init/libcregression.

### Checkpoint25 — VFS routing and forwarding admission (2026-10-11)

All four ambient VFS rule controls now require exact open canonical Host
PID/mount/IPC views before copies, vector effects or boot-default visibility.
Private native paths ignore inherited/default rules and aliases; remote name
producers, discovery, physical/export expansion and compatibility fallback
refuse before remote effects. Existing inherited File operations, Host bodies,
trusted boot queries, Task/File layouts and syscall/wire ABI are preserved.
Public creation remains disabled.

Final Normal Build WOS, isolated fast diagnostics, formatting/tidy, 44 focused
Host tests and 65 complete-source checks PASS. Actual rootless KTEST44 PASS:
84,244 assertions, zero failures, 1,356 cases, all 105 mandatory checks and
95 stable source/artifact pins, normal poweroff. Failed42/43 fixtures were
corrected without disabling diagnostics or changing production close behavior;
exact sources reconstruct from retained parent/patches. Twelve inactive
disposable images across all three attempts were pruned, 515,293,184 allocated
bytes total, peak 260,194,304 bytes and final zero. User VM preserved.

Current broad credential/VFS census still has 24 failures in 129 calls; no full
Host/service qualification claim. Goal39 ACTIVE, all ten full criteria still
INCOMPLETE. Continue native public construction/root population, remaining
global controls and actual shared mappings/lifecycle/hostile service campaigns,
two independent service trees for two hours, 1000 full allocation-failure cycles
and complete Host/init/credential/resource/libc regression. Rootless
bin/wos-cluster --no-setup and bin/wos-ktest --no-setup remain authorized with
one active topology, 20GiB disposable/40GiB free floor, shared immutable bases
and COW/reflink copies; audit exact live ownership/backing refs and prune owned
inactive disks/appliances after PASS, FAIL and exceptions, retaining small
receipts and preserving user/unrelated/current/seed/source/sysroot artifacts.

### Checkpoint26 — native process construction (2026-10-11)

The actual shared file-backed builder can install an admitted private IPC/mount/
PID context and revalidated native root/cwd before ELF/TLS/TCB. Private PT_INTERP
uses the image subject's filesystem instead of a foreign executor's view. The
internal helper consumes Files/stacks and keeps existing owned-child recovery;
Host bodies, Task/File layouts and syscall/libc/wire ABI remain preserved.
Public creator admission, root population, launcher and publication remain work.

Normal Build WOS, isolated fast diagnostics, format/tidy,44 focused Host and41
complete-source checks PASS. Rootless KTEST48 PASS84,418 assertions/0 failures/
1,360 cases/all109 mandatory/110 stable pins/normal poweroff. Three invalid new
ELF fixture attempts retained exact reconstructible sources; corrected only
fixture layout while preserving production validation. Sixteen inactive images
across45..48 audited/pruned846,200,832B, peak260,456,448B/final0; user VM preserved.
Broad129 credential/VFS source calls still24 failures; full regression unproven.
Goal39 ACTIVE, all ten full criteria INCOMPLETE. Continue live creation/spec/
launcher, small/static ELF and recoverable metadata/OOM, remaining global/WKI/
device consumers, real shared mappings/futexes/scheduled lifecycle/hostile paths,
two independent service trees2h with Host operational and1000 full failure cycles
with all references at baseline. Authorized --no-setup launch/debug and20GiB/
40GiB storage bounds/pruning after every outcome persist; no old VM disk retention.

### Checkpoint27 — owned native construction (2026-10-11)

The admitted private builder now claims a fresh default child and transfers
File/stack ownership before name/pagemap allocation and main executable reads.
Task allocation/reclaim and stable File cloning defer fatal recovery around
temporary input owners. Child-owned parser scratch retires through the existing
unpublished recovery path. A separate may-fail pagemap API preserves Host
allocator/constructor semantics. Native small/static complete ELF sources are
bounded at8MiB; larger dynamic images retain bounded metadata/lazy construction.
No public creation, initial argument stack, root-population or publication API.

Normal Build WOS, isolated fast diagnostics,44 focused Host tests, formatting/
tidy and52 source/body/header/ownership checks PASS. Rootless --no-setup KTEST49
PASS84,669 assertions/0 failures/1,364 cases/all113 mandatory/117 stable pins
(115 distinct paths)/normal poweroff. New cases cover real small/static images,
executable leases, five allocation refusals and prepared-metadata recovery.
Four proven inactive images pruned260,521,984B/final0; unrelated user VM preserved.
Broad129 current credential/VFS source calls still24 failures.

Goal39 remains ACTIVE, all ten full criteria INCOMPLETE. Next construction work
includes tracked interpreter File/bytes and runnable argc/argv/envp/auxv;
live creator capture/revalidation, non-root root ownership/population, explicit
FD/shared mapping grants, local init/job/parent and atomic publication remain
required, alongside complete global policies and full service campaigns. Keep
the authorized --no-setup workflow,20GiB disposable/40GiB free floor and exact
owned inactive-image pruning after every outcome. No autonomous pause or push.
