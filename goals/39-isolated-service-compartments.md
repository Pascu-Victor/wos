# Goal 39: Isolated WOS Service Compartments

## Copy-pastable goal command

```text
/goal Implement WOS service compartments with independent mount/PID/IPC visibility, explicit capability inheritance, namespace-aware process lifecycle and a usable compartment launcher. Verify every acceptance criterion in goals/39-isolated-service-compartments.md through executable isolation and escape tests, ABI checks, Normal Build WOS and user-run simultaneous-service evidence. Preserve host init behavior, existing credentials/resource limits, stable VFS lifetime and WKI authority. Bound work to the specified compartment model without claiming Linux container compatibility; use checkpointed bounded subagents and follow the file's evidence and blocked rules.
```

## Outcome and scale

Current checkpoint 35 qualifies same-kernel initial nice inheritance and
private priority selection through both short public modes with 21 checks.
Goal39 is ACTIVE; all ten full criteria
remain incomplete. Launch exact-owned tests with `--no-setup`, maintain one
topology and prune disposable images after every passing or failing run.

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

## Checkpoint28: native interpreter and initial stack (2026-10-11)

The private builder now owns its actual interpreter File, compact metadata or
bounded bytes in child recovery, and maps it with the child's admitted policy.
Stable kernel arguments are copied before image reads: absolute path below512
bytes, at most256 entries per vector, combined2MiB strings/pointers. The real
mapped argc/argv/envp/auxv stack includes main ENTRY/PHDR, interpreter BASE,
credential IDs/AT_SECURE, EXECFN and entropy with16-byte aligned rsp. Registers
publish after complete stack writes. Executor guards protect interpreter loader
locals and blocking scratch retirement. Host bodies, Task layout and ABI remain
unchanged. Null arguments still prepare only an image; public creation is disabled.

Normal Build WOS, isolated fast diagnostics,44 Host tests, format/tidy and56
preservation checks PASS. Rootless --no-setup KTEST51 PASS85,923 assertions/0
failures/1,368 cases/all117 mandatory/122 stable pins (120 distinct paths), normal
poweroff. New cases exercise actual multi-page/empty stacks, small/static/sparse
interpreters, nonroot/secure identity, four partial allocation failures and
transferred launch recovery. KTEST50's grant fixture lacked its required ceiling;
the exact-owned failed VM was stopped, evidence retained, and only the fixture
corrected before51. Eight inactive disposable images pruned456,359,936allocatedB;
final0, unrelated user VM preserved. All129 current credential/VFS source calls
were run again and still have24 failures. Checkpoint audit passes; full regression
and all ten goal criteria remain INCOMPLETE.

Goal39 remains ACTIVE. No pause requested or performed. Continue actual live
creator capture/revalidation, owned root population/import/nonroot provisioning,
explicit FD/shared mappings, init/jobs/parent/atomic publication and a bounded
versioned launcher/spec; complete global consumer policy and the actual hostile,
two-hour service/1000 lifecycle/Host/init/libc campaigns. Read-only next-step
reconnaissance is recorded in goals/progress/39. Keep rootless --no-setup,
one active topology,20GiB disposable/40GiB free,COW/reflink and exact owned
inactive pruning after every outcome. No push.

## Checkpoint29: canonical creator and fresh-root preparation (2026-10-11)

Kernel-owned preparation now retains the canonical creator, jointly observes
credentials/generation and all 16 limits with service selection/network/umask,
intersects requested/service policy and revalidates after blocking work. It
reserves population, creates fresh mount/PID/IPC identities, provisions an
effective-UID/GID-owned TMPFS root, imports bounded stable native regular-File
assets and installs only explicit regular-File descriptor grants. Service
affiliation precedes actual private image/interpreter/stack mapping and preserves
the admitted limits. Owned abort/recovery retires child before root state;
successful preparation remains unpublished and does not transfer root ownership.

Exact private root pins and local typed views use retained mount identities and
existing DAC/membership/epoch/confinement policy. Foreign views are rejected,
closed scopes report EIDRM, and untyped physical/WKI export remains refused.
Task member layout, public ABI/libc/wire and ordinary Host bodies are preserved.

Normal Build WOS, isolated fast diagnostic build, 44 Host tests, format/tidy and 67
compatibility checks PASS. Rootless --no-setup KTEST56 PASS 86,874 assertions / 0
failures / 1,375 cases / all 124 mandatory / 127 stable pins (125 distinct), normal
poweroff. Seven new native tests cover real roots/imports/explicit FD rights,
canonical observation changes, actual service aggregate membership, eight
allocation/import/transferred-recovery failures and invalid inputs. Failed
attempts 52–55 are retained and reconciled; 20 inactive images pruned, final 0
disposable allocation and unrelated user VM preserved. Current 129 credential
source calls still have the same 24 failures; their original tests are unchanged.

Goal39 remains ACTIVE, all ten full criteria INCOMPLETE. Public launcher/spec,
atomic creator-policy publication/root transfer, explicit shared mappings and
nonregular FD policy, remaining global controls and actual hostile/2h-service/
1000-lifecycle/Host-init-libc campaigns remain required. Read-only global-control
reconnaissance records DEBUG, CPU affinity/domains, swap, init mailbox/status,
service reads, global sync and log-block/framebuffer gaps before public enablement.
Continue with --no-setup, one topology, 20GiB disposable / 40GiB free, COW/reflink and
exact-owned inactive-image/appliance pruning on PASS/FAIL. No pause or push.

## Checkpoint30: Host-global consuming admission (2026-10-11)

DEBUG interrupt control, physical CPU affinity/domain controls, swap, global init
mailbox/status, service configure/select/read, SYNC and log-block/VGA operations
now refuse private canonical PID/MNT/IPC scope before inputs/lookup/locks/effects.
Root credentials/shared networking do not grant Host scope. Closed/unbound owners
and copied thread fields use exact retained canonical ownership. Shared bounded
journal output/PERF_TRACE, FSYNC capabilities, trusted backends and ordinary Host
bodies remain; no Task/File layout, public ABI/libc/wire or scheduler primitive
change. Source audits retain all original assertions and follow consuming bodies.

Normal Build WOS, isolated fast diagnostics,44 Host tests, format/tidy and58
compatibility checks PASS. Rootless --no-setup KTEST58 PASS87,570assertions/0
failures/1,379cases/all128mandatory/136stablepins134distinct/normalpoweroff. Four
new native cases pass private combinations/nonroot/shared networking/canonical
threads/mapped sentinels/Host errors. Failed57 is retained and reconciled before
fixture corrections; existing authority helper now verifies unbound refusal and
original bound-Host non-init role. Eight owned inactive images pruned457,211,904B;
final0disposable, unrelated user VM and required seeds/current artifacts preserved.
All129 current credential calls still have the same24failures; broader regression
remains unqualified. Checkpoint audit PASS, all ten full criteria INCOMPLETE.

Goal39 ACTIVE. Continue atomic creator-policy publication/root ownership transfer
and versioned launcher/spec, complete FD/device/indirect consumer and shared
mapping manifests, actual hostile/init/futex/two-hour/1000lifecycle/Host-init-libc
campaigns. Reconnaissance31 records actual existing mutation guards and remaining
resource/service coordination plus prepared-mount lifetime requirements. Use
--no-setup, one topology,20GiBdisposable/40GiBfree,COW/reflinks and exact-owned
inactive-image/appliance pruning on PASS/FAIL. No pause or push.

## Checkpoint31: native local publication (2026-10-11)

Kernel-only publication revalidates the retained canonical creator under the
existing VM publication mutex, links child-event membership and uses explicit
local scheduler post. All sixteen limit setters and service selection coordinate
with this boundary; credential authorization is fresh after the guard. Private
root authority cannot raise an admitted hard ceiling. Privileged Host behavior
and ordinary soft changes within current hard limits remain.

Exact pending ownership pins the child through post/commit. Successful post
transfers namespace retirement to private init, returns the physical Host PID,
and finishes prepared mount state before releasing the protecting root pin.
Parent exit follows existing Host adoption semantics; a runnable child does not
return a retry result. Failure removes pending membership and leaves preparation
owned for explicit abort. No Task/File layout, public ABI/libc/wire or scheduler
primitive changes. Successful runnable publication remains unqualified.

Normal Build WOS, isolated fast diagnostics,44 focused Host tests, format/tidy
and38 preservation checks PASS. Rootless --no-setup KTEST59 PASS87,822assertions/
0failures/1,383cases/all132mandatory/137stablepins135distinct/normalpoweroff.
Four new native cases qualify actual mapped limit consumers, hard-raise refusal,
changed/closed/exiting/foreign owners and membership/real-post refusal recovery.
Four exact-owned inactive images pruned261,308,416allocatedB; final0disposable.
Required seeds/current artifacts and unrelated user VM preserved. Current129
credential source calls still have the same24failures. Checkpoint audit PASS;
full regression and all ten full criteria remain INCOMPLETE.

Goal39 ACTIVE. Continue bounded versioned public spec/launcher and scheduled
service publication/root transfer, explicit shared mappings and complete FD/
device/indirect consumer policy, actual hostile/fatal/init/futex/two-hour/1000
full-lifecycle/Host-init-libc campaigns. Use --no-setup, one topology,20GiB
disposable/40GiBfree,COW/reflinks and exact-owned inactive pruning on PASS/FAIL.
No autonomous pause or push.

## Checkpoint32: public mapped spec and launcher (2026-10-11)

Process selector54 and a standalone version1/408-byte header accept bounded
mapped input from the exact canonical Host creator. Persistent staging owns
partial allocations/File capabilities before usercopy, then reuses native
creator policy, root import, argument/ELF preparation and local publication.
All previous selectors/libc/wire/Task/File layouts remain. Descriptors backed
by WKI block proxies refuse before storage effects, including partition ancestry;
Host asset imports copy bytes without passing that transport capability.

The new wos-compartment launcher/header are built and registered in rootfs and
attestation dependencies. Explicit assets/FD grants, lower-only16limits, zero
authority/serviceprivileges, denied networking, empty environment and no ambient
FDs are defaults. Help, diagnostics and a manifest describe capabilities;
foreground raw64bit Host wait or explicit detach preserves supervision semantics.
Its README describes current invocation and experimental qualification limits.

Normal Build WOS, isolated fast diagnostics,46 selected Host tests, format/tidy
and43 preservation checks PASS. Rootless --no-setup KTEST60 PASS88,543assertions/
0failures/1,389cases/all138mandatory/149stablepins147distinct/normalpoweroff.
Five new actual mapped-input cases and one provider case qualify partial captures,
invalid specs/FDs/strings, two capture allocation refusals and exact owner recovery.
The synthetic images deliberately refuse before post; no runnable success claim.
All129 broader credential calls retain the same24failures. Four exact-owned
inactive images pruned261,373,952allocatedB; final0disposable, user VM preserved.

Goal39 ACTIVE/all ten full criteria INCOMPLETE. Continue actual runnable public
publication/root transfer and CPU-placement inheritance census, guest CLI and
reproducible fixtures, scheduled hostile/fatal/init/futex tests, full FD/indirect
policy, two independent2h service trees/1000full lifecycles/all baselines and
Host/init/libc regression. Use --no-setup, one topology,20GiBdisposable/40GiBfree,
COW/reflinks and exact-owned inactive pruning on PASS/FAIL. No pause or push.

## Checkpoint33: actual public runtime and native scheduler initialization (2026-10-11)

The native exclusively owned unpublished skeleton initializes heap_index=-1,
weight1024 and10ms slice before its first allocation, matching ordinary process
construction. Actual public attempt05 exposed the previous heap_index0 panic.
Scheduler corruption diagnostics/transitions, Task/File layouts, Host constructors
and all public/libc/wire interfaces remain. Existing actual native preparation
test now checks detached state before/after both publication-refusal paths.

Repository fixture/build helper/service fragment/README exercise process54 and
the actual CLI with UID/GID100,zeroauthority,NNP, explicit imports and one shared
regular result FD9. Two sequential private services each report PID1/parent0/
UID100/14checks/0failures, Host path refusals, marker data, denied networking,
named shared memory and localPID2 fork/wait. Host waits confirm exit0. These
are short actual service tests, not two simultaneous two-hour trees.

Normal Build WOS20.387s, isolated fast diagnostic build11.373s,46Host tests
34.875s, formatting7.779s and30preservation checks PASS. Public07 PASS39.343s,
169stablepins; public06 durable guest PASS but serial-only observer timeout is
retained separately. Corrected observer requires both exact binary reports and
durable Host wait assertions. KTEST61 rootless --no-setup PASS463.504s,
88,557assertions/0failures/1,389cases/all138mandatory/149stablepins147distinct/
normalpoweroff. Public07 four images91,242,496allocatedB and KTEST61 four
images261,439,488B pruned;0disposable, user13532 UID945 preserved.

All129 broader credential oracle calls retain the same24unresolved failures;
no weakening/masking or full-regression PASS. Goal39 ACTIVE/all ten full criteria
INCOMPLETE. CPU placement/domain inheritance remains separate from restored
EEVDF defaults; canonical/executor/setter/scheduler/descendant flow reconnaissance
recorded before future critical patch. Remaining hostile/shared/futex/init,
two2h trees,1000full lifecycles/all reference baselines and Host/init/libc evidence
remain required. Use --no-setup on launch, separate build/data/sysroots,one
topology,20GiB/40GiB budget,COW/reflinks and exact-owned pruning PASS/FAIL.

## Checkpoint34: private priority selectors and queue commits (2026-10-11)

GETPRIORITY/SETPRIORITY use the exact canonical private PID namespace, local
executing-thread zero selector and retained local targets. Same-UID Host and
sibling physical aliases cannot select targets. Fresh requester/target/canonical
liveness, stale-image, closure, authorization and remote refusal checks precede
reads/effects. Private SET holds VM -> actual owner runqueue -> namespace
publication through nice/weight/deadline and heap-accounting updates; at most
two bounded owner scans, no detached/unlocked private fallback. Queue refusal
preserves fields/sums; rescheduling occurs after queue/namespace unlock. Host
priority functions, physical selectors, encoded libc result, Task/File layout,
all ABI/syscall/wire surfaces and old native cases are preserved. Later exit
without VM publication is not claimed to be serialized by these fresh checks.

Four real native cases cover aliases/Host encoding, canonical threads/stale
images, closure/authorization/remote refusal and actual exclusively owned
current/handoff/heap/wait queues/accounting. Actual normal public-spec AND CLI
modes pass private PID1/parent0/UID100, 21 checks/zero failures, local priority
reads/updates/denied raises, raw 64-bit guessed Host-PID refusals and unchanged
Host nice values. Both Host waits return exit0; exact 56-byte reports and durable
Host assertions are required, not serial-only observer output.

Normal Build WOS PASS51.725s; isolated fast diagnostic build PASS56.438s;
48 selected Host tests PASS37.420s; affected final credential CTest PASS2.102s;
format/tidy PASS82.685s; guest fixture build PASS0.282s; 27preservation checks PASS.
Exactly24 previously failing credential source tests reconcile their real current
consumers/delegates, all104other bodies unchanged, all128names/calls unchanged.
Historical129calls included one display print, so there are128tests. Success print
now follows all tests; final affected CTest re-run passes. No skip/masking and no
claim of full Host/init/libc regression. Original failing format01 and source
snapshots01..04/patches retained; format-only fix preceded builds/runtimes.
Sourceaudit print move preceded VM freeze, required only affected CTest re-run.

Public34-01 PASS39.634s/169stablepins, both21checks. KTEST62 --fast --no-setup
PASS488.346s, 88,688assertions/0failures/1,393cases/all142mandatory/normalpoweroff,
149stablepins147distinct. Public and diagnostic four-image prunes remove
91,242,496B and261,570,560B allocated respectively; final0disposable.
20GiBdisposablebudget/40GiBfreefloor, one topology, COW/reflinks and exact
UID/PID/QMP/FD/inode/backing proofs retained. Inventories unreadable0; unrelated
user13532 UID945 preserved; immutable seeds/current artifacts preserved.

Audit checkpoint-34-audit.json SHA256
942f018c8ef059e942b29fbab627729f488af1373e2ba66500614cf36e63abda PASS;
all ten full criteria false. Goal39 remains ACTIVE, no pause/push. CPU/nice
inheritance is not implemented by priority selectors/defaultnice0. Remaining
canonical/executor CPU mask/domain/pin observations, coherent setters/descendant
inheritance/namespace ceilings and every scheduling consumer have read-only
sequence notes, not implementation claims. Hostile/shared/futex/init evidence,
two simultaneous2h trees,1000full lifecycles/all references and complete Host/
init/libc regression still required. Use --no-setup for launches, preserve
normal/KTEST build/data/sysroot separation and prune PASS/FAIL/exception images.

## Checkpoint35: initial priority inheritance (2026-10-11)

Native creation inherits the larger nice value of the exact canonical creator
and executing actor. Short VM publication guards capture both live sources,
derive the exclusively owned unpublished child's weight through the real
scheduler table, and revalidate both observations plus child nice/weight before
lifecycle registration or posting. Changed observations return EAGAIN before
effects. Ordinary local spawn snapshots the actor after claiming child recovery;
later parent changes do not retroactively change that child. Loader IO, resource
locks, WKI placement and heavy cleanup remain outside the new short VM scopes.

The construction-only scheduler helper validates ownership, exactly two child
references, active/unpublished/unlinked/detached state and signed nice range
before either write. It preserves runtime, deadlines and consumed slices. Task/
File layouts, public ABI/wire/libc interfaces, Host priority setters, boot and
remote-file defaults, in-place/worker exec and all 139 old native case bodies remain.
Fork/clone/pthread already inherited actor nice; this patch preserves that code
and does not claim coherent observations during concurrent priority changes.

Five actual native cases cover signed values/derived weights, canonical-vs-actor
priority, changed sources/child fields, invalid state/range/image/CPU refusals
and exact ownership/population/namespace drain. Both actual normal public-spec
and real CLI modes start at nice 5 from the Host, their local PID 2 forks retain 5,
they lower to 7 and receive EACCES for raise 6. Guessed Host physical IDs return
ESRCH; Host nice 5 remains unchanged, both waits return exit0, and both exact
56-byte reports pass all 21 checks. This qualifies same-kernel initial inheritance.

Normal Build WOS retry02 PASS 198.486s; isolated fast diagnostic build PASS 63.221s;
48 selected Host tests PASS 38.980s; format/tidy PASS 95.526s; fixture build PASS 0.297s;
31 preservation checks PASS. All 128 credential source tests remain present and
passing in the selected Host run. Public35-01 PASS 38.559s / 169 stable pins. KTEST63
--fast --no-setup PASS 485.420s, 89,409 assertions / 0 failures / 1,398 cases,
all 147 mandatory,
normal poweroff / 149 stable pins (147 distinct paths). Public/diagnostic four-image prunes
removed 91,242,496 / 261,570,560 allocated bytes; final 0 disposable, user VM
13532 UID945 preserved, inventories unreadable 0. One topology, 20GiB budget /
40GiB free-space floor,
COW/reflinks and exact UID/PID/QMP/FD/inode/backing proofs remain required.

Initial Normal build01 failed after 24.383s in bundled Bash/OpenSSL using a transient
generated stdint include_next shim. The completed installer restored mlibc's
complete header; unchanged-source retry02 passed. The graph permits overlapping
conformance reinstall and port builds; exact overlap is inferred, not traced.
The failed receipt and read-only diagnosis remain. Before any public35 launch,
review corrected the runner so serial PASS only triggers stopping; final PASS
requires durable Host rows and both exact private reports. All other receipts
and historical failed evidence are preserved.

Audit checkpoint-35-audit.json SHA256
fc86708187c24ddfff1cb6106220997153892cc07586aec9036bf79ab54f36c6 PASS.
Goal39 remains ACTIVE/all ten full criteria INCOMPLETE. CPU placement/immutable
ceilings, worker-exec actor priority, coherent ordinary fork/thread observations,
WKI nice transport, hostile/shared/futex/init evidence, two simultaneous 2h trees,
1,000 full lifecycles/all reference baselines and complete Host/init/libc regression
remain. Continue CPU-domain publication and placement work. Launch with
--no-setup; preserve normal/KTEST build/data/sysroot isolation and prune images
after PASS/FAIL/exception. No pause or push.
