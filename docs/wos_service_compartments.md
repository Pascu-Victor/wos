# WOS service compartment design

Goal39 is in progress. Profile preparation, private native SysV IPC namespace
ownership/lookup, private native mount-table ownership, Task native lookup and
subject mount administration are implemented. Checkpoint 32 added the experimental
versioned public creation syscall and `wos-compartment` launcher. Checkpoint 33
qualifies two short, sequential nonroot service trees through the public spec
and actual launcher, including local fork/wait and explicit result capabilities.
Checkpoint 34 confines priority selectors and scheduling updates to the exact
private PID view; both actual public modes pass 21 checks. Checkpoint 35
qualifies same-kernel initial nice inheritance through both modes and their
private fork. Checkpoint 36 qualifies immutable CPU-domain publication through
actual registry selftests and a concurrent Normal eight-CPU guest probe.
Checkpoint 37 qualifies fresh locked hard-domain admission. Checkpoint 38 qualifies
atomic access to the existing Task placement scalars with unchanged measured
layout. Placement transactions, CPU inheritance/ceilings, worker-exec and WKI
priority inheritance remain incomplete.
Checkpoint 43 prevents implicit Host console grants during private legacy
creation and native execve. Its two new native tests and the full diagnostic
suite pass. Checkpoint 44 qualifies the finite public process-launch and
regular-file I/O probe, including complete descriptor observations and an
actual init-reap witness.
Checkpoint 45 characterizes regular-file mappings through the public spec.
Attempt02 completes strict execution, but shared-map coherence, writeback and
private READ-to-WRITE promotion fail. Fresh remapping, ordinary private
isolation, materialized fork sharing and read-only grant restrictions pass.
Checkpoint 46 repairs private cached/fork-aliased WRITE promotion and READ/NONE
revocation, including anonymous swap permissions. Six new native regressions
and the complete diagnostic KTEST75 suite pass: 1,443 cases, 93,749 assertions,
zero failures. Task layout and public ABI remain unchanged. Fresh public
attempt03 also passes promotion and peer isolation (5/5 comparisons), with
strict execution and independent raw-data audit complete. Shared-map coherence
and writeback still fail (mask3); coherent mutable SHARED backing remains
unimplemented. All disposable images are pruned after both runs.
Checkpoint 47 removes nested timeout usercopy under the futex word pin and
ends MREMAP copy pins before error cleanup takes VM publication. Error priority
and insertion-time deadlines remain unchanged; timeout prefault/sampling moves
earlier. Normal/isolated diagnostic builds, formatting, seven existing Host
checks and 42 compiled caller regression vectors pass. The regression catches
both hazards in archived old bodies. These are Host ordering/lifetime checks;
broader root/reclaim drain safety and new guest execution remain unqualified.
Checkpoint 48 repairs XFS directory-index insertion around stale slots. The
fresh Normal finite replay passes cases 29/82/83 with the full 53,248-byte FD
report retrieved and strictly decoded; all eight disposable images are pruned.
Checkpoint 49 ends root-reader and Task usercopy leases before the lazy
file-fault worker enters a potentially blocking File close. Its retained Task,
issued work slot and existing shutdown hold span close; completion remains
after callback return. Twenty compiled Host regression cases pass and detect
the original ordering in all twenty negative controls. Normal Build WOS,
isolated fast KTEST build-only, formatting and eight focused Host tests pass.
These checks qualify caller ordering; native SMP/root-drain behavior, complete
mapped-file ownership and coherent mutable SHARED backing remain unqualified.
Checkpoint 50 fixes lazy file-range slicing: every retained fragment keeps its
original virtual-address-to-file-offset relationship, including cold faults
after partial unmap, replacement or protection changes. File offsets whose
aligned span exceeds signed positional-I/O range fail before publication with
the existing overflow error. The Host regression passes 253 cases and 52,777
assertions and detects all three original rewrite defects. Normal and isolated
fast diagnostic builds, formatting and six focused Host tests pass. The new
native real-PTE regression is compiled and enabled. Its first execution fails;
source review finds a fixture mismatch: it supplies a writable description to
an immutable-cache observer that requires read-only access. The original boolean
failure does not identify its first failing mode. Checkpoint 51 corrects the
description for the two read-protection modes and records the first failing
stage after cleanup, preserving every assertion and the writable overflow
vectors. Normal and isolated fast builds and formatting pass. Corrected KTEST77
passes all 1,444 enabled tests, including 193 mandatory cases, with 93,751
assertions and zero failures. Three existing disabled tests remain disabled.
Natural poweroff and launcher exit 0 are observed; all five prepared and stopped
payload readbacks match. All four disposable disks are pruned after both runs,
leaving zero disposable allocation; the unrelated VM is preserved. All 792
current launch inputs remain unchanged after the successful run. The mapping
fixture uses a synthetic read backend with real Files, PTEs and frame references;
it does not qualify mutable filesystem coherence or SMP ownership retirement.
Existing references, rollback, locking, Task/File layouts and syscall ABI are
preserved. Complete native mapping retirement and shared coherence remain open.
Checkpoint 53 makes private futex identity follow a nonreused page-root incarnation
and virtual word, so unrelated private physical aliases cannot wake each other
and a same-root frame replacement keeps its wake key. Explicit shared leaves keep
common-frame keys. Root creation, retirement and six fork rollback paths maintain
the same growable registry. Normal/isolated diagnostic builds, formatting and
11 focused Host checks pass. KTEST79 passes all 1,455 enabled cases and 101,101
assertions with zero failures, including nine new root/key fixtures, then powers
off naturally. All four disposable disks are pruned; the independent terminal
audit passes. Canonical shared-file coherence and complete owner drains remain
open, along with the two-hour services and full lifecycle/failure qualification.
Exact-owner ambient network admission is implemented;
complete PID, lifecycle and WKI qualification remains incomplete. Independent
service isolation and all ten goal criteria remain unqualified. Goal28 remains
incomplete at its committed wire-codec checkpoint while this work proceeds.

## Current policy preparation

`platform/security/compartment_policy.hpp` prepares a private `Profile` from
one admitted parent profile and an explicit request. Its credentials use the
existing immutable `Snapshot`; its limits use the existing resource `Limit`
ABI. These values are not a userspace layout, namespace identity, process
handle or authorization proof.

Preparation preserves the parent's real, effective and saved user/group IDs
and all supplementary groups. A service needing a different identity must use
the existing checked credential-change path before launch. Requested authority
must be a subset of the parent's ceiling. Sealed service grants must be a subset
of the parent's effective service authority; imported grant bits remain bounded
by their matching ceiling bits. Unknown bits fail. The resulting snapshot always
has no-new-privileges and sealed service privileges. Dumpability may be retained
or dropped, and cannot be added to a nondumpable parent's candidate.

Each requested soft and hard limit must fit its respective inherited bound,
with soft at most hard. Root has no preparation exception for raising a limit.
The NOFILE hard limit also must fit the caller-supplied native FD-table capacity.
Zero and infinity use direct comparisons without deadline or addition overflow.
The helper checks every existing ABI slot; this does not add enforcement for
resources unsupported by the current kernel.

Network policy defaults to `DENIED`. `SHARED_HOST` requires that the admitted
parent already carries that permission. It describes shared host networking;
it provides no independent network stack. A future installer must derive the
parent profile from retained kernel authority, never from user-supplied parent
fields. Native socket creation and global network controls now enforce the
installed PID owner's network policy even when an inherited credential has
network administration authority. Complete creator installation and other
device/global consumers remain integration work. Explicit inherited
socket descriptors remain capabilities and must be reported by the launcher.

The helper allocates nothing, takes no lock and calls no Task, File, VFS,
usercopy, clock, logger, scheduler or network operation. Failed preparation
leaves the output and parent unchanged, including when output aliases parent.
It neither installs credentials nor revokes descriptors or mappings. A live
installer must retain and revalidate the exact credential, resource and namespace
observations before publication. Existing resource consumption, process/service
membership and aggregate budgets cannot be reset by entering a compartment.

## Native integration requirements

The mount table now partitions native rows by exact retained namespace, while
table generations, pivot publication and storage remain globally serialized.
Retained native root/cwd views already enforce object identity and read-only/
nosuid restrictions. Task ownership, native component lookup and retained lookup
consumption and mount administration now carry the exact subject mount namespace.
Root/compatibility helpers and procfs PID/link consumers have native coverage;
scheduled public-service qualification remains required. Retained File listings and
subject mount observations now select their exact namespace. Shared backend
lifetime must remain distinct from visible mount attachment lifetime before
cloning existing mount attachments can be supported.

Kernel Task IDs remain internal. Public PID/TID, parent, session and process
group values need one requester-aware translation policy. Safe lookup retains
the exact Task before signal, ptrace or wait effects. Native child publication,
exit claims, tracer ownership, thread-owner references and namespace-init death
must drain without stale PID authority or lifetime cycles. Fork, clone, thread
creation, spawn and every exec preparation path must preserve the subject's
namespace before scheduler visibility. Worker context cannot select its view.

SysV shared-memory storage remains bounded and shared, while public key and ID
lookup now selects the subject process's retained IPC namespace before access
checks. Same-credential siblings cannot name, attach or control foreign objects.
POSIX `shm_open` uses `/dev/shm`, so it still depends on a private mount view. Existing inherited
mappings and descriptors keep their underlying object capabilities. Shared
futexes remain keyed by pinned backing identity; adding a compartment prefix
would incorrectly split explicitly shared memory synchronization. Named POSIX
semaphores currently return ENOSYS, and no WOS message-queue sysdep or AF_UNIX
implementation was found in the inspected sources. Unsupported interfaces must
retain explicit failure; this goal cannot qualify a silently global fallback.

WKI rich/direct compute and Task FD export now reject restricted native namespace
contexts before new remote effects. Remaining routing/VFS export and global
consumers require integration until a negotiated enforceable compartment
contract exists. Automatic placement stays local; explicit unsupported placement
fails. Captured credentials/root/resource grants cannot silently substitute the
worker's host namespace. Global host administration also needs explicit
compartment gates and a reviewed allowed-authority policy.

Final namespace/view release can close backend Files and block. Publication,
path, mount, scheduler, lifecycle, IPC and resource locks cannot span allocation,
usercopy faults, network waits, final destruction or backend close. Exact owners
must remain retained across blocking work. Preserve the root host/init path
through rollback and drain existing compartments before removing their state.

## Private native IPC ownership

`platform/sched/ipc_namespace.*` supplies a fixed pool of 64 private namespaces.
An empty `NamespaceRef` explicitly represents host IPC; an unbound thread has
no namespace observation and fails rather than falling back to host. References
are move-only with checked retention. Pool exhaustion preserves the output;
slot reuse requires the final reference to drop. Diagnostic generations are
never reused and cannot be converted into join handles. Retain/drop/closure
use atomics without allocation, blocking, locks or finalizers.

Task stores an appended private reference installed under exclusive unpublished
ownership and frozen thereafter. Native resource inheritance carries it before
fork, clone and legacy spawn publication; user threads and exec workers resolve
their exact retained process owner. Native exec preserves it. Remote factories
still lack compartment transport and must be gated before public creation can
be enabled; this checkpoint does not qualify WKI isolation.

Each SysV segment retains its namespace through destruction. Public GET,
ATTACH and CTL enforce exact namespace membership; internal attachment clone,
detach and process cleanup retain the exact global segment identity. Global IDs
stop at the signed 32-bit limit instead of wrapping into stale capabilities.
Explicit inherited mappings remain backing capabilities without granting name
or control access in another namespace. Shared futex keying is unchanged.

`IPC_STAT` reports historical creator and last-operator process names only to
readers in the creator's exact PID namespace generation. Host-created segments
retain canonical Host process IDs. Private creators use local process names,
including when a bound thread performs the operation. A foreign PID view sees
zero, even when its IPC capability allows access and its local numeric PID
matches the creator's name. Successful explicit attach/detach updates the last
name; a foreign-generation operator records zero. Fork inheritance, process
cleanup and IPC retirement preserve that history.

Segments store these names and the creator's generation as numeric history;
they retain no Task or PID namespace. Destroying the creator or reusing a PID
namespace slot cannot bind old metadata to a new process. Physical segment and
attachment ownership stays separate. The syscall selectors, errors, libc
interface and 120-byte `ShmidDs` layout are unchanged.

`shm_retire_namespace` closes private admission under the SysV registry lock
and marks only its exact segments for destruction. New GET/ATTACH/CTL return
EIDRM. Attached backing stays until the final attachment drains; retirement
does not unmap intentionally inherited memory. Host retirement is ignored.
The deferred compartment-init retirement worker calls this hook after
requesting exits from retained members. Complete scheduled reference and
retirement-drain qualification remains open.

Physical segment allocation/release retains the existing SysV lock/context
behavior. The new namespace's final release never performs heavy cleanup under
that lock. Complete namespace-init policy, aggregate IPC budgets and scheduled
user mapping/futex qualification remain integration work.

Seven native tests exercise actual owned Tasks, private user page tables,
guarded STAT copies, attach/detach, canonical threads, creator/scope destruction,
colliding names, real fork COW and retained mappings across IPC retirement.
KTEST73 passed 1,435 cases and 93,325 assertions with zero failures; all 184
mandatory cases passed and the three existing disabled BufferCache cases stayed
disabled. Normal Build WOS, the isolated fast diagnostic build, 64 host tests,
format/tidy and both compiler profiles' ABI checks passed. These native fixtures
do not qualify scheduled public PID observation, namespace-init death, the
unresolved SMP workload or the full Goal39 service/lifecycle requirements.

The separate Normal eight-CPU public StageA run verifies creator/operator
`1/1` in both private PID 1 roots through actual `IPC_STAT`. It preserves all
32 mutable SysV rounds, 32 read-only File futex rounds, two foreign-backing
negative rounds, guarded foreign IPC rejection and refused signal names.
Host supervision kills and reaps A's init with status 9, observes its child
disappear, and confirms B continues before its local PID 2 reap with status 0.
The independent init-exit snapshot records the Host probe's actual exit 0.
All 12 current uploaded objects match prepared and stopped read-only hashes;
all 886 source/artifact pins stay unchanged. The workload completes in 0.590s;
the runner stops its exact-owned VM at the existing 120s wall timeout because
service output uses the journal. Four disposable images are pruned. This does
not by itself qualify child-operator/history observation, successful foreign PID
projection, complete reference drains, two-hour operation or 1,000 lifecycles.

The subsequent Normal eight-CPU StageB run qualifies public child-operator and
retained-history observation. Each private root captures full guarded 120-byte
`ShmidDs` values around its child's extra attach/detach and its own extra
attach/detach. Creator/operator/attachment tuples are `1/1/2`, `1/2/3`,
`1/2/2`, `1/1/3`, `1/1/2`. B's actual PID 2 child creates a distinct object
and writes a run-specific marker. B observes `2/2/1` before attaching and
`2/1/2` afterward. Only after the original consuming wait returns local PID 2
with status 0 does it verify the retained marker and `2/1/1`, then detach and
verify `2/1/0`. Removal succeeds; the subsequent rejected STAT preserves all
120 sentinel bytes and both guards. The Host object separately reports physical
creator/operator PID 103. All status padding, permissions, sizes and reserved
fields are checked, including the failed observation.

All original sharing, refusal, credential and FD checks pass. A's child
disappears 1,101 microseconds after its init is killed, before its 30-second
sleep expires; B continues and exits successfully. The independent init
snapshot confirms the Host probe's actual exit 0. All 17 durable files total
57,222 bytes, within the 64,640-byte cap. All 12 uploaded objects match both
prepared and stopped read-only hashes; all 984 current runtime input paths
remain unchanged. The actual workload takes 0.649s. The runner reaches its
existing 120s wall timeout and stops only its owned VM through QMP, then checks
the saved reports; this is not natural poweroff. All four disposable images
are pruned, releasing 197,279,744 allocated bytes, while the unrelated VM is
preserved. See the [fixture protocol](../tests/fixtures/compartment_shared_runtime.md)
and its explicit-output builder for reproduction. Successful foreign-view
projection, writable File coherence/cache eviction, complete reference drains,
two-hour operation and 1,000 full lifecycles remain unqualified.

## Implicit stdio and explicit descriptor capabilities

Legacy creation and committed native execve share `ensure_exec_stdio_fallbacks`.
It resolves the canonical process owner before checking its exact namespace
scope. A live private PID, mount or IPC view preserves only existing descriptors
and succeeds without opening Host `/dev/console`. Closed views and unbound
owners refuse before credentials, FD-table or device effects. Host scope keeps
the existing fallback behavior and descriptor-limit handling. No caller order,
syscall layout, resource state or descriptor ownership rules change.

KTEST74 passes all 1,437 executed cases, including all 186 mandatory cases,
with 93,327 assertions and zero failures. The two new cases exercise actual
Host console fallback, all seven private-view combinations, root and UID100,
DENIED and SHARED_HOST networking, retained descriptors, canonical process
owners and closed-view refusal. Their synthetic close-only File controls cover
ownership; they do not establish regular-file I/O behavior. The diagnostic VM
shuts down naturally, and all four exact-owned disposable images are pruned.
The unrelated user VM remains intact. Normal Build WOS, the isolated fast
diagnostic build, 64 relevant Host tests, formatting and the two diagnostic
clang-tidy units pass. Compiler-measured Task and ShmidDs layouts are unchanged.

The [public stdio fixture](../tests/fixtures/implicit_stdio_runtime.md) is admitted
and compiles with libc-only linkage. Its Normal eight-CPU attempt03 passes actual
EXEC, null-options SPAWN, explicit-options SPAWN and FORK followed by EXECVE.
Two sequential UID100 private roots preserve the exact granted descriptor set;
the replacement after CLOEXEC removes only FD2. Three Host controls preserve
the original console fallback and explicit-options behavior. Console descriptors
are inspected with FCNTL/FSTAT only.

All 13 committed rows pass 3,816 row checks; the coordinator passes 59 checks.
Saved reports retain 3,328 signed descriptor results, 208 guarded resource-limit
observations, 52 complete 144-byte stat outputs and 78 I/O slots: 66 actual
PREAD/PWRITE returns and 12 explicit console skips. All 13 specific positive-child
consuming waits return their owned PID with status0. The 13 durable files total
62,400 bytes. The independent 4,160-byte init snapshot records generation1,
no restarts, EXITED/READY, PID/PGID-1, wait0 and an actual EXIT history entry.
The workload takes 0.559 seconds. The runner stops its exact-owned VM through
QMP at the 120-second bound; this does not establish natural VM poweroff or the
witness's own later exit.

Attempts01 and02 remain failed evidence. Attempt02 saved 11 reports and recorded
one coordinator failure without the init witness. Its original wire omitted the
first failing check, so the exact cause remains unproven. The narrow fixture
amendment verifies the harness-prepared DIR0700/UID/GID100 instead of attempting
parent-directory creation and records a first-failure ordinal. Fresh target
build and formatting checks pass; production kernel/libc/init sources remain
unchanged from checkpoint43.

All 12 uploaded objects match both prepared and stopped readbacks. Source and
artifact hashes remain stable; the unrelated user VM is preserved. All four
exact-owned disposable images are pruned, leaving zero disposable allocation.
Receipts are recorded in the Goal39 ledger. These finite native and public checks
do not complete a full Goal39 criterion. File-mapping coherence, complete
reference drains, two-hour simultaneous service operation and 1,000 full
lifecycles remain unqualified.

## Finite regular-file mapping characterization

The [File mapping fixture](../tests/fixtures/compartment_filemap_runtime.md)
creates two simultaneous UID100 private roots with DENIED networking and only
their own control FD plus explicit mutable/read-only grants of the same regular
backing File. One root forks after materializing its mappings. The fixture
separates execution completion from seven payload-based semantic groups.

Normal eight-CPU attempt02 completes the strict protocol. All 13 durable files
total 236,631 bytes, with 768 signed descriptor observations, 64 limit pairs,
nine partial grant-stat records, 83 positional calls and 83 guard records.
All 16 futex waits record exactly 5,000,000 microseconds and return 0; all 16
wakes return 1. Three exact consuming waits return status 0, and the 4,160-byte
init snapshot records generation 1, no restarts, sequence 52, EXITED6/READY2
and actual reap 0.

The independently recomputed semantic mask is 35: independent shared-map
coherence, writeback and private READ-to-WRITE promotion fail. Fresh remapping,
ordinary private isolation, materialized fork sharing and read-only grant
restrictions pass. The probe's exit0 denotes completed characterization;
it does not turn those semantic failures into successful isolation evidence.

Attempt01 remains failed evidence because the producer's two clock samples
yielded a timeout slightly over the strict 5-second bound. The corrected producer
uses one sample for its remaining timeout; the validator stays unchanged.
Fresh target build, format and complete external dependency checks pass for
the corrected executed artifact. No new Normal kernel or diagnostic build
was required for these fixture changes; production remains at checkpoint43.

All 12 prepared and 12 stopped artifact readbacks match. The rootless
`--no-setup` runner stops only its owned VM through QMP at the 120-second bound.
All four disposable images are pruned, releasing 200,163,328 allocated bytes and
leaving zero disposable allocation. The unrelated user VM is unchanged.
Independent terminal receipt SHA256 is
`fe7285b505362dce2a8d28e9449bf765e3fbab7b61953884ad9f7a0f87892b71`.
These are attempt02's historical observations. Checkpoint46 subsequently
repairs private promotion and permission revocation in local commit `c9af3ed6`.
Normal/isolated diagnostic builds, 64 Host tests, formatting, fresh Task layout
measurements and all 1,443 KTEST75 cases pass, including six new native tests.

Fresh Normal eight-CPU attempt03 repeats the same strict public protocol using
the repaired kernel. All 13 durable files total 236,632 bytes; all 12 prepared
and 12 stopped readbacks match. Independent raw decoding reconstructs 550
events and 222 semantic comparisons. Promotion passes 5/5; remapping, private
isolation, materialized fork sharing and rights/retention pass. Independent
shared-map coherence fails 64/80 comparisons; writeback fails 1/33, at the final
Host positional read after one private root exits. The source permits stale
unmap writeback, but this campaign captures no individual backend write trace
that establishes causation. The semantic failure mask is3.

Independent terminal receipt SHA256 is
`328278f0d1b763ddd1602f497334ed10b599a5ca1d14d85e7683b1c348a60ba0`.
It binds strict raw producer/init proofs, current sources/build/dependencies,
exact artifact readbacks and fresh Host cleanup. The owned VM is stopped by
QMP at the existing 120-second bound. Four inactive owned images are pruned
(200,163,328 allocated bytes), leaving zero disposable allocation and the
unrelated user VM unchanged. Shared coherence/writeback, complete reference
drains, two-hour operation and 1,000 lifecycles remain open; no full criterion
is qualified.

## Private native mount ownership

`vfs/mount_namespace.*` has a separate fixed pool of 64 move-only namespace
owners. Empty explicitly selects host. Exact refs pin immutable, nonreused
generations; retention/drop never allocates or invokes a backend finalizer.
There is no generation-to-reference or integer join interface. Each private
view admits at most 64 published mounts. Mount rows and initialization
reservations retain their namespace until rollback or exact backend teardown.

Trusted kernel mount APIs accept an explicit namespace; default callers select
host, independent of the executing task. Private creation accepts admitted
physical paths and tmpfs only. Private root tmpfs allocates its own tree rather
than borrowing the global boot root. XFS, FAT32, devfs, procfs and remote private
mounts reject before initialization. Existing local block writer leases exclude
remote writers but allow overlapping local writers: admitting private XFS needs
a separate physical-device/shared-backend ownership contract. This checkpoint
does not extend or qualify that contract.

Publication, path reservations, longest-prefix lookup, both mount caches,
covered-directory edges, descendant checks, enumeration and pivot compare the
exact namespace. Cache entries include its nonreused generation and revalidate
the global mount generation under mount_lock before dereferencing raw rows.
Private mounts are absent from host enumeration and automatic WKI discovery;
their mount/unmount operations do not publish global WKI storage events. Only
the exact pivot namespace can reopen its pending publication epoch. Global
serialization and sibling cache invalidation remain; sibling paths/backends do
not change. A teardown already in progress still blocks pivot to keep shared
row storage stable while heavy close runs unlocked.

`retire_mount_namespace` closes private lookup/publication admission under
mount_lock and drains exact rows in the existing root-last shutdown order.
Backend/File close runs unlocked. Bounded ref drain or live File/initialization
pins return EBUSY with backing retained for retry; intentionally held Files
remain usable. A namespace ref's final drop never implicitly destroys a mount.
The future namespace-init coordinator must close task capabilities and retry
drain; host namespace retirement through this hook returns EINVAL.

Native fixtures exercise actual separate tmpfs contents at identical paths,
both cache keys, foreign covered-directory refusal, private reservations and
closure, host-only enumeration, sibling descendant independence, pivot path/
cover ownership, mount-count exhaustion rollback and File capability drain.
They call trusted native mount/backend APIs. The Task path fixtures described
below add actual named lookup; hostile user execution, public creation and full
service lifecycles remain required follow-on evidence.

## Task mount ownership and native pathname lookup

Task appends an immutable retained mount namespace installed only on an active,
unpublished process. Threads and exec workers resolve their retained filesystem
owner; an unbound thread fails with ESRCH. Existing resource inheritance installs
the child's exact namespace before copying limits/service membership. Exec keeps
the same process owner. No userspace integer can be converted into a namespace
reference, and there is no public compartment entry or launcher yet.

Native component walking captures the subject's nonreused namespace generation
with its credential/root/cwd authorization and selects mounts in that exact view.
The selected MountRef owns the namespace throughout handle move and consumption.
Single and pair consumers reselect its exact namespace, check closure and match
the captured generation before backend effects; a different executing worker's
host view cannot select the pathname. Global mount/VFS/search epochs and backend
DAC, object identity, covered-parent proofs and read-only/nosuid checks remain.
Covered-edge validation also selects its parent in the child's retained view.

Only native TMPFS private mounts are admitted currently. Private lookup never
falls through to host text resolution, synthetic mount/WKI ancestors, procfs FD
link probes or host mount discovery. Unsupported physical export requests and
remote routing fail with ENOTSUP before those effects. A local `/wki` directory
is an ordinary private name. A missing native covered edge returns EXDEV rather
than bypassing containment. Public compartment entry, remaining root/procfs and WKI,
process and network consumers are not yet a qualified compartment boundary.

Foreign retained root/cwd/dirfd anchors cannot select names in another mount
view. An explicitly inherited File retains its own read/write/stat capability;
the launcher will report such grants. Closing a namespace denies new named
lookup and consuming its old lookup handles (EIDRM), while existing Files keep
backing alive until explicit close and retirement retry. Dropping a Task namespace
reference never performs implicit heavy mount teardown.

Native fixtures use real unpublished Tasks with production mkdirat, openat,
statat, symlinkat, renameat, child/thread ownership, moved lookup consumption and
read-only exec-input open. They check same-path sibling backing, dotdot/links,
stale rename handles, foreign dirfd refusal with retained fstat capability,
closure, covered crossings and missing-edge refusal. These are native kernel
integration tests; they are not user-process escape/service-lifecycle campaigns.

## Subject mount administration

Ambient mount, unmount and pivot wrappers delegate to explicit retained Task
APIs. A worker selects the subject's immutable namespace and credentials.
Private mounting accepts tmpfs with no source; devices, PARTUUID, remote URIs,
other backends and default filesystem detection fail before external effects.
Preparation and native covered-directory publication use the same exact scope.

Private unmount consumes the actual native target, records its admitted physical
path and nonreused mount ID, releases lookup/cover pins, and retires only that
exact scoped identity. It cannot remove a replacement at the same name. A live
inherited File keeps backing busy and usable. Closed namespaces refuse new
administration; explicit namespace retirement remains the drain path. Host
remote detach retains its local identity admission without requiring a fenced
peer's metadata response, and host export reconciliation stays separate.

Private pivot validates both native directories on the exact new-root mount,
with put_old strictly beneath it. Captured subject and namespace/mount/backend
epochs are revalidated after releasing lookup pins. Remapping uses the current
physical root, so a second pivot rebases relative suffixes rather than duplicating
the first root prefix. Only filesystem owners in the exact scope change root;
the retained unpublished subject is included. A plain cwd at the old root moves
with it. Explicit File/anchor references to remapped backing keep pivot busy;
these capabilities are not revoked. Host pivot preserves init spelling, bound
thread root mirrors and WKI export barriers, and does not rewrite private roots.

Three actual unpublished Task fixtures exercise independent mount backing and
unmount, stale replacement IDs, inherited File busy/drain, unsupported/closed/
unauthorized refusal, and two pivots including rebased service mounts. They do
not qualify public compartment entry, scheduled service execution, every root/
directory/procfs consumer or the required service and lifecycle campaigns.

## Retained directory listings and mount observations

Packed directory reads enumerate synthetic mount children in the retained
File's mount namespace. An inherited directory File keeps its own backend and
listing capability; it does not authorize named lookup through a foreign dirfd.
Closing that namespace disables synthetic augmentation while existing Files
continue to list their backing objects. Unlinked or stale directory text cannot
select replacement mount children. Native TMPFS/XFS directory identity supplies
the current path after rename; failed recovery suppresses augmentation and cache
hints. Native TMPFS entry reads and deduplication probes take its tree mutex.

File observer fallback queries also select the immutable File mount owner and
refuse a replacement or overmount at its historical name. Existing exact backing
and globally nonreused mount IDs remain cache identity. Only host Files observed
by host subjects receive host WKI aliases and WOSLINK tagging, using an explicitly
captured root. Private literal `wki` directories remain ordinary directories.
Packed DirEntry layout, cookies, partial reads and search-only FD refusal remain.

Root metadata captures a retained namespace together with the process owner's
root text/anchor and existing epochs. Procfs mount read and SEEK_END enumerate
that exact scope, hide private rows outside the captured physical root, and use
the canonical process owner for threads. Closed or stale observations fail
before copying caller output or changing the File cursor. Host mount-row spelling
and read-only/nosuid flags remain compatible. This does not admit private procfs
mounts or qualify its PID enumeration and link consumers.

Three native fixtures exercise packed sibling/inherited File listings, closed
scope capability behavior, renamed-directory replacement text, private WKI
names, and subject/thread procfs mount read/seek with failed-output preservation.
They use unpublished Tasks and trusted mount rows for augmentation; they do not
qualify scheduled service execution or hostile concurrent rename campaigns.

## Native PID ownership and registry foundation

Tasks now retain a private PID namespace and reserve an immutable local PID/TID
before native publication. The fixed pool holds 64 namespaces; local IDs are
positive signed32-compatible values, never reused while that namespace survives.
Drained pool slots get a globally nonreused generation. References remain exact
kernel-owned capabilities; no number can join a namespace. Children inherit the
canonical process owner's scope, and threads retain distinct IDs in that scope.
Unbound threads have no scope and cannot fall back to host lookup. Exec preserves
the installed owner. Host queries and internal physical IDs retain their existing
meaning; host-thread resource binding needs no local reservation.

The scheduler commits private scoped, physical and dense active visibility as
one allocation-free transaction. Capacity and collision failures write nothing.
An IRQ-safe per-namespace gate serializes closure with registration; lock order
is runqueue, registry, namespace. Scoped index storage has 131,072 slots and admits
at most 65,536 private members, including deferred zombies whose dense row has
already retired. Each row borrows the physical registry's lifetime; its Task
retains namespace identity through removal and destruction. Removal repairs
probe chains and rejects a foreign physical-PID collision.

Requester-aware retained queries select an exact local ID in that owner's scope.
Closed, missing, stale and foreign IDs fail without global fallback. Live queries
reject EXITING/DEAD; a separate metadata lifetime query admits zombies while
their namespace remains open. Host supervision can retain private Tasks by their
physical identity. This does not promise a zombie's reclaimed address space stays
intact; memory access still requires the existing usercopy/pagemap gates.

GC checks its final Task reference and withdraws all registry visibility under
the same registry guard used by lifetime queries. A concurrent retained pin keeps
the dead task queued; registry withdrawal succeeds only after pins drain. Existing
epoch/current-task/waitable-zombie guards and outside-lock heavyweight cleanup
remain. No new reference-count sentinel, teardown callback or user ABI is added.

Six native fixtures use actual unpublished Tasks and production registry/GC
withdrawal helpers. They cover pool exhaustion, failed output preservation,
monotonic names, closed retained owners, child/thread admission, host retirement
compatibility, same-number siblings, stale names, zombie pins, failed/colliding
registration and actual probe-chain repair. They do not publish fake runnable
user processes or qualify a public compartment boundary. The process/thread
creation adapters below extend this foundation; procfs, signal/wait/ptrace,
namespace-init death and launcher admission remain pending.

## Durable local session and process-group names

Task job snapshots now capture internal physical identity and durable local
SID/PGID names under the same job-store lock. The local store is appended to
Task; the embedded physical Identity layout and prior assembly offsets remain
unchanged. Fork, clone, spawn and pthread construction copy the pair together.
Members retain local names after the leader's Task is withdrawn and destroyed;
group resolution uses remaining exact-namespace members, never a dead leader
lookup. Unknown/imported private job identities expose no invented host alias.

GETSID/GETPGID resolve positive PID/TID names through the explicit requester,
retaining a thread's canonical process owner before releasing the thread.
Private queries return local job names, while Host observation returns physical
values. SETSID, SETPGID and spawn group options use local private inputs/outputs,
preserve physical credential/session/parent/exec admission and resolve groups
only among exact-scope members. Host public group mutation cannot rewrite a
private member's names. Closure serializes with the lifecycle/job-store commit;
failed queries and closed/foreign mutations preserve outputs and state.

PTY attachments retain physical SID/PGID and terminal allocation/attachment
generations for kernel signals. Additional pair-locked value metadata records
the globally nonreused PID-namespace generation and local foreground group.
TIOCGPGRP/TIOCSPGRP translate that exact attachment; the local name survives
group-leader exit. A private observer cannot turn a foreign terminal's numeric
group into a local name. Host observation remains physical. Revocation clears
the name/stamp with the physical attachment; no new namespace reference or
integer join capability is introduced. Existing explicitly inherited terminal
Files remain capabilities for their device I/O, without granting foreign group
selection or job-control authority.

Four native tests use real unpublished Tasks, production registry/session/
child-publication and allocated PTY APIs. They cover destroyed leaders,
canonical thread queries, identical sibling aliases, foreign/closed/imported
refusal, child exec admission, Host physical inspection and preserved failed
outputs. Public compartment entry stays disabled: the remaining signal/
ptrace/procfs, init teardown, WKI/network/resource and launcher paths must be
integrated before scheduled service boundary qualification.

## Local process and thread creation names

Private getpid observes the retained canonical process owner's local ID;
GetTid uses the executing thread's reserved local ID. PPID reads the process
owner's retained parent membership under the lifecycle lock. Namespace init
reports zero even with a Host supervisor, and missing or foreign parents expose
no numeric Host alias. Threads follow their owner's current parent instead of
an old thread-local parent mirror. Host keeps physical identities and its
parentless process/init-adoption convention. Retained identity observations
remain available after admission closes; they do not authorize new creation.

Fork, spawn, shared-VM clone and pthread creation return captured local names.
Initial/replacement ELF images initialize the canonical process TCB ID, and
pthread TCB publication uses the same local TID as its returned value. Private
fork updates only its own copy-on-write TCB before publication. Private shared-VM
clone requires an explicit separate TCB and refuses overlapping parent cache
bytes, including different virtual addresses mapping the same physical range.
Mapped writes remain nonfaulting under VM publication, with writable preflight,
explicit Task/frame pins and final mapping commit. Host clone keeps its existing
shared-TCB behavior, and Host fork keeps libc's existing cache refresh. The TCB
uses mlibc's aligned four-byte ID at offset0x18; struct and syscall widths stay
unchanged. Clone's caller-provided output slots may retain an early copied ID
after later construction failure, as before.

Creation preserves physical scheduler/debug/perf/WKI/lifecycle IDs. Short Task
pins cover immediate retirement during commit/trace or pthread exit propagation.
Spawn inherits exact retained root/cwd before interpreter pathwalks, while its
existing unpublished recovery owner covers blocking construction. Five native
fixtures use real Tasks/memberships, user PTE/COW/alias checks and actual initial
Thread construction. Public admission remains unavailable: signal/ptrace/
procfs/init teardown, complete WKI/network/resource/launcher integration and
scheduled hostile service/campaign evidence are still required.

## Local wait selection and event names

Public wait selection now uses the canonical owner's exact PID scope. Private
positive IDs select reserved local PID/TID values; zero and negative selectors
use local group names. Only actual owned child/tracer memberships and retained
queued event subjects confer wait authority. Same-number foreign subjects do
not become candidates, including a foreign reference in the owner's queue.
Host public waits retain physical names and can supervise private namespace
init. Trusted WKI claims, inspections and conditional queue-version claims
continue to use physical selectors and physical origin values.

Event publication takes one coherent internal/visible job snapshot and freezes
local PGID alongside the existing physical group. Five appended Task slots
correspond to its embedded exit, tracer exit, stop, continued and ptrace nodes;
the embedded event/selector/registration layouts and prior Task offsets remain
unchanged. Moving an event between owners does not rewrite the snapshot. An old
group remains selectable after live group changes or actual leader destruction.
Claims copy the public result under lifecycle ownership; usercopy and return
never dereference a copied borrowed event pointer. Perf retains the physical
subject separately from the returned local name.

Registered waiters retain their selection view in appended Task metadata.
Ordinary and no-child wakes use the same policy as initial claiming. Local-only
group changes invalidate queue/group versions and wake obsolete selectors.
Public claim/registration admission holds lifecycle then the exact namespace
publication guard; the guard spans both actions and drops before lifecycle
restores IRQs. Closed private admission reports EIDRM; an already admitted claim
can still commit using its existing verified cookie. Default trusted physical
claims remain available for controlled retirement. Private public wait bypasses
WKI before any distributed-wait operation; Host remote behavior is unchanged.

Six native fixtures exercise actual membership, producers, claims, event reuse,
nowait/copy retry, reaping and registration/wake paths. They initialize real
unpublished Tasks then mark them EXITING before producing events, without
posting fabricated user Tasks to the scheduler. Cleanup drains through trusted
physical claims and verifies Task/namespace references. Namespace-init closure
notification and bounded descendant/zombie teardown remain subsequent work.
Public compartment admission and scheduled service qualification stay pending.

Checkpoint 10 Normal Build WOS, isolated fast diagnostic build, format-only check
and 22 selected Host CTests pass. Actual KTEST16 passes 79,109 assertions with zero
failures across 1,295 cases, including all 43 required compartment cases and normal
poweroff. Four inactive private images (258,031,616 allocated bytes) were verified
and pruned; disposable image use returned to zero. Small failed sandbox/cache/
stale-source-oracle receipts are retained alongside the successful retries.

## Local signal selection and admission

Private public kill resolves positive PID/TID values only in the caller's exact
retained PID namespace. Zero and minus one preserve WOS's current-group convention;
other negative values select durable local group names. Group selection does not
depend on a live leader. Same-number siblings, physical IDs and Host tasks never
become fallback targets. Unknown or uninstalled identity fails with ESRCH and a
closed namespace with EIDRM. Host retains its physical and distributed routing.

A bounded registry snapshot owns group members before effects. Each target is
admitted again under VM publication and exact namespace admission, then through
the existing local signal lease. Visible PGID and physical session come from one
job snapshot. Existing credential, saved-ID, SIGNAL authority, SIGCONT session,
live-task and retired-thread-image checks remain in force. Accepted or proxy
remote routes fail locally with ENOTSUP; private delivery never redirects to WKI.
Group signals 18..22 retain the existing direct-thread exclusion and partial
delivery policy. Snapshot allocation fails before any member receives a signal.

Admission guards end before ptrace, pending generation or scheduler wakes. An
already admitted local lease can finish after closure or route publication;
exec drains it before transferring pending state. Delivery uses the existing
pending-before-visibility publication handshake, so an unpublished Task retains
its signal without being posted to the scheduler. Task storage, physical job
identity, syscall widths, wire formats and trusted lifecycle/terminal/WKI signal
APIs are unchanged. Namespace-init drain must integrate these admitted effects
before public compartment creation can be enabled.

Seven native cases exercise actual Tasks, scoped registry lookup/cohorts, pending
state and reference/lease cleanup. They cover local PID/TID and group selectors,
foreign and physical aliases, actual leader destruction, credentials/SIGCONT,
unknown/closed scopes, remote refusal, stale thread images, a 41-member cohort,
and deterministic closure/route/local-group changes around admission. Fixtures
remain unpublished to the scheduler. Full scheduled service qualification and
all ten goal criteria remain pending.

Checkpoint 11 Normal Build WOS, isolated fast diagnostic build, format-only check
and 28 selected Host CTests pass. Actual KTEST17 passes 79,602 assertions with zero
failures across 1,302 cases, including all 50 required compartment cases and
normal poweroff.
The first Host run found an unbuilt pending-signal binary and stale timer source
assertion. The assertion also failed at the committed baseline; the test now
checks the current atomic timer producer/claim path. Failed and zero-selection
receipts remain alongside successful reruns. Four proven inactive private images
(258,162,688 allocated bytes) were pruned, returning disposable usage to zero.

## Scoped ptrace ownership and observations

Private ptrace selects positive local PID/TID values through the exact requester
namespace. Zero borrows the requester. Same-number siblings, physical aliases
and Host tasks do not become fallback targets, even for root. Host keeps physical
inspection under existing credential and dumpability policy. Private TRACEME
retains the canonical process parent's actual lifecycle membership; namespace
init cannot select its Host supervisor through a numeric parent hint.
Internal tracer IDs, trusted child claims and WKI identifiers remain physical.
Only private public Event/StopInfo TIDs, thread lists and local RemoteInfo proxy
names translate. Private remote/proxy observation rejects before WKI discovery.

Each call owns a namespace operation reference/counter and its positive target
reference. Each private effect separately checks scope closure, live Task type,
current thread image and local route under VM publication and a short namespace
guard, preserving current credential revalidation. Source-image admission uses
the existing bounded local-effect lease. The namespace guard ends before faults,
allocation, usercopy and scheduler effects. An admitted effect can finish after
closure or route publication; subsequent effects fail. Initial memory denial
publishes no result; later invalidation preserves the existing partial-transfer
fault ABI.

SYSCALL_WAIT retains lifetime ownership across blocking, with separate leases
for resume and each claim capture. No source-image lease spans a park that may
resume the target's own exec. Private parks recheck closure within a 10ms timer
interval. A captured claim owns a copied local result and can finish copy/commit
after closure; existing competition, failed-copy retry and claim-cookie behavior
remains. Namespace-init descendant closure/drain remains future integration.

Current-executor ownership lists register target references, effect leases and
allocated observation buffers for ordinary and fatal cleanup. Both process and
worker-thread exit drain this ownership before image/WKI/stack reclamation.
Scratch buffers and nested operations release exactly once. The new Task pointer
follows the previous final data field; all preceding fields remain unchanged.
No public request, record layout, libc errno conversion or wire format changed.

Eight native tests exercise actual unpublished Tasks/registry and retained
parents, mapped bulk/word/partial copies, public register/thread/remote output,
closure/route interleavings, current credential denial, stop claims and nested
fatal/ordinary cleanup. The fixtures never become scheduled service processes.
The full service boundary and all ten goal criteria remain pending.

Checkpoint 12 Normal Build WOS, isolated fast diagnostic build, format-only check
and 34 selected Host regressions pass. Actual KTEST19 passes 79,819 assertions with zero failures across 1,310 cases, including all 58 required compartment cases and normal poweroff.
The first VM exposed 50 signed-width errno assertion mismatches; its source
stayed frozen, the exact-owned VM was stopped and all four images were pruned.
Corrected assertions passed the full rerun. Final four inactive private images
(258,555,904 allocated bytes) were verified and pruned; disposable usage
returned to zero. Small successful and failed receipts remain available.

## Retained native procfs views (checkpoint 13)

Procfs lookup and raw/resolved opens now carry the retained requesting Task.
Each procfs File owns its PID namespace reference, including roots and clones;
its target and backend PID remain physical retained identity. Private parsed names
use the exact native scoped index and canonical unpublished self. Integers never
join a view. Clones and inherited Files preserve the captured view and target;
Host observations continue using physical names, including Host supervision of
private Tasks. Credential, dumpability, FD-mode and generation checks remain.

Private root and thread listings filter before counting and emit dense local
names. Status/stat use local PID/Tgid/PPID and durable local group/session names.
Physical wait selectors are redacted in private status; public selectors retain
their local names. Private loadavg/cpu-stat task counts are scoped; the last-name
field is the greatest currently observed local TID, not an allocation counter.
CPU time/load averages, machine memory, uptime, version and timekeeping remain
explicit shared-machine metadata. Mounts retains its existing per-reader root
and mount-view semantics without a persistent shared listing cache.

Fresh private global perf, allocator, WKI topology/control and block diagnostics
return ENOTSUP and are absent from root enumeration. Private image observations
omit kernel images. Unsupported proxy/accepted/remote execution observations
reject before discovery or remote effects; private cwd/root observations disable
remote capture. Full WKI/network/export enforcement still requires integration.
Dynamic self links require an explicit reader in the captured PID view and emit
that reader's local process name. Host self links retain physical semantics.
Closed private views reject reads, seeks, enumeration, links, metadata, cloning
and fresh binding before output/cursor publication.

Private mount administration permits device-free native procfs and forces it
read-only. Entry from tmpfs proves the actual retained covered directory,
namespace, attachment and admitted parent component before procfs binding outside
publication. Dotdot exit proves the synthetic procfs root and selects the exact
cover's native parent. Rename/replacement text cannot renew an old mount edge.
FD reopening retains the actual underlying File capability, including its mode
and cursor; PID closure does not revoke an already inherited ordinary backing
File. Foreign mount-view dirfds still cannot confer fresh names. Owned native
DirectoryView roots and physical-export walks refused procfs in that checkpoint;
the owned-root extension below adds separately tested private path admission.

Nine kernel cases exercise actual unpublished Tasks/registry, native private
mounts and component traversal, target/view retirement, clones, reader self,
local/thread/parent/group/wait names, dense enumeration/census, closure staging,
global/placement denial, descriptor capabilities and renamed cover refusal.
The fixtures remain unscheduled. Public creation stays disabled and all ten full
goal criteria remain incomplete, including namespace-init drain, mapped shared
futex qualification, complete authority/WKI/network gates, launcher/spec,
two simultaneous two-hour service trees and 1,000 full service lifecycles.

Checkpoint 13 final Normal Build WOS, isolated fast diagnostic build, full formatter/clang-tidy
check and 35 selected Host checks pass. Actual KTEST22 passes 80,236 assertions,
zero failures and 1,319 cases, including all 67 required compartment cases and
normal poweroff. KTEST20 exposed the remaining tmpfs-only path gate plus an old
unsupported-procfs oracle. KTEST21 passed mounted traversal and rename refusal,
but found a new fixture's incorrect reused-output-handle expectation. Frozen
failed runs were stopped through exact-owned QMP and their images pruned before
correction. Final four inactive images (258,818,048 allocated bytes) were checked
and pruned; disposable usage returned to zero. Small evidence remains.

## Procfs beneath owned native roots (checkpoint 14)

Private procfs can now be traversed beneath a retained native DirectoryView root,
including through a retained TMPFS directory FD and through nested native mounts.
The component walker selects the exact native cover and its current attachment
before synthetic binding. Final consumption validates the retained ProcFile's
PID view and proves the same cover beneath the actual owned root. ProcFileData
is never treated as a native inode, and a path, PID or mount generation cannot
grant membership. Read-only/nosuid policy follows regular procfs opens; operation
credentials, target generations and output staging keep their existing checks.

Regular procfs Files need no native directory pin. Synthetic procfs directory
pins return ENOTSUP before FD publication; procfs cwd/dirfd ownership still needs
integration. That checkpoint refused captured root/cwd/directory-FD magic links
in an already-scoped walk, pending their additional root intersection and origin
lifetimes. Ordinary symlink translation and dotdot retain the native root
boundary. Host procfs and physical-export refusal retain their prior behavior.

Two additional native cases install production-admitted roots on unpublished
Tasks and exercise scoped regular opens/stat/self links, inherited policy,
native directory FDs, nested mount cover proofs, dotdot/root escape refusal,
foreign inherited dirfds, renamed/replaced covers, final consumer refusal and
closed-view output preservation. These remain unscheduled kernel fixtures.
Normal Build WOS, isolated fast diagnostic build and 35 selected host regressions
pass. Formatting covers all four current kernel paths; clang-tidy covers both
normal compilation units, and the fixture is compiled/executed separately.
Actual KTEST24 passes 80,442 assertions, zero failures and 1,321 cases, including
all 69 required compartment cases and normal poweroff. Its four inactive images
(258,949,120 allocated bytes) were verified and pruned; disposable usage returned
to zero. Source/artifact/runtime and cleanup receipts remain in the local Goal39
progress ledger. Public creation and all ten full acceptance criteria remain
incomplete.

## Scoped private procfs magic links (checkpoint 15)

Private root/cwd and native directory-FD links now intersect the retained caller
and target roots by actual native membership. Their policies combine, preserving
read-only and nosuid restrictions through repeated links. Absolute symlink
restarts use the resulting retained root. Host scoped-walk refusal and physical
procfs export refusal keep their existing behavior.

Local-only metadata recovery now accepts an admitted credential snapshot
independently of remote recovery permission. Remote root/cwd anchors are refused
before retention. Private self links return a relative local PID, so a non-default
or narrower procfs mount stays the origin; Host self spelling remains unchanged.

The lookup retains each followed procfs origin and its admitted credential
generation until final consumption. A bounded owner allocated outside
publication retains additional origins; move/reset transfers or drains it once.
All origins are revalidated before publication and each consumer. Outer retained
views prevent final File closes while intermediate roots are replaced under
publication. The existing eight-link limit is unchanged.

Two additional native tests exercise narrower directory FD roots, restrictive
policy through root/cwd/FD links, absolute-link escape refusal, repeated targets,
second-target credential/lifecycle cutovers, move/reset, closed PID views and
depth-limit reference drains. The existing remote-cwd admission test also checks
local-only refusal with supplied credentials and unchanged output/reference
counts. These remain unscheduled native fixtures. Public service creation,
synthetic procfs directory pins and the full acceptance campaigns remain pending.

Normal Build WOS, isolated fast diagnostic build and all 35 selected host checks
pass. Six kernel paths pass formatting; core, lookup and procfs normal units pass
clang-tidy. Production pins are unchanged after a fixture-only setup correction;
the final fixture is compiled and executed separately. Actual KTEST26 passes
80,605 assertions, zero failures, 1,323 cases and all 72 mandatory checks with
normal poweroff. Its four inactive images (259,080,192 allocated bytes) were
verified and pruned; disposable usage returned to zero. The earlier failed
fixture run was also pruned. Source/artifact/runtime and cleanup receipts remain
in the local progress ledger; all ten full acceptance criteria remain incomplete.

## Retained private procfs directory descriptors

Private procfs directory FDs without an owned native root now start component
walks from their retained PID view and target. A retained PID/thread directory
keeps that target after registry withdrawal; a fresh numeric root lookup still
requires scoped registry selection. Thread parents use exact process-owner
identity. Thread FD-directory names preserve the `/PID/task/TID/fd` form.

Directory names come from retained identities rather than cached PID text.
Traversal requires the caller's exact mount and PID views and current directory
lifecycle. Content access keeps its per-operation credential checks. Numeric FD
selection authorizes before probing the target table; borrowed description
release stays outside namespace publication. Final consumption revalidates the
exact native cover and attachment, including direct backend renames that do not
advance the VFS publication epoch. Failed output and FD reservations stay intact.
At that checkpoint, Host, owned-root synthetic-pin, serialized and physical-export refusals remained;
Task/File/anchor/view, callback, syscall, libc and wire layouts are unchanged.

Owned-root procfs directory pins and cwd publication were pending. At that
checkpoint, unpublished teardown required exactly two references before clearing
anchors and FDs; self-procfs objects added retained task references. The following
ownership checkpoint addresses that rollback prerequisite.

Three new native cases check registry withdrawal and exact FD capability reopen,
thread parents/canonical names, stale/closed/foreign views, bounded failed paths
and cover mutation before consumption. Normal Build WOS, isolated fast
diagnostics, formatting/clang-tidy and 35 selected host regressions pass. Actual
KTEST28 passes 80,721 assertions, zero failures, 1,326 cases and all 75 mandatory
checks with normal poweroff and stable source/artifact pins. The failed and
successful runs' inactive images were verified and pruned; disposable usage
returned to zero. These are unscheduled native fixtures. Public service creation
and all ten full acceptance criteria remain incomplete.

## Unpublished self-procfs rollback ownership

An unpublished child can open `/proc/self/status`, duplicate that descriptor,
and then fail a later spawn action. Its procfs metadata retains the child, so
the previous exact-two teardown check refused cleanup. Rollback now admits those
self references after proving that their complete ownership graph belongs to
the constructor. Duplicate descriptors count as File owners; each distinct
self-target procfs File contributes one Task reference.

The proof observes retained FD, image, lazy-file, root/cwd anchor and detached
view-pin owners using bounded scratch below 32KiB, without allocating during
rollback. It rechecks original source identities and lazy ownership, reservations
and relevant File/view/anchor reference counts. External aliases, unaccounted
Task owners, changed sources, unsupported graphs and overflow still refuse
before cleanup, preserving failed output and resources. Temporary owners retire
outside path, FD and lazy locks. Closed PID views do not prevent ownership drain.

Creator-slot and transferred-owner teardown retain their publication/lifecycle
guards and existing resource cleanup. Task/File/view/anchor, callback, namespace,
credential, syscall, libc and wire layouts are unchanged. Ordinary running-task
exit keeps its existing behavior. At that checkpoint, synthetic owned-root
pins/cwd and public compartment creation remained gated. Typed directory
membership and publication are implemented in the following checkpoint;
public compartment creation remains disabled.

Five native fixtures exercise distinct and duplicate self objects, image/lazy
aliases, closed views, external Task/File aliases, reserved descriptors,
transferred-slot cleanup and a lazy owner moved after capture. A test adapter
runs the actual spawn OPEN/DUP2 consumer followed by a failed OPEN and checks
descriptor, process-population and namespace drains. These are unscheduled
fixtures; they do not qualify user ELF execution, shared mappings/futexes, or
synthetic self view/anchor aliases.

Normal Build WOS, the isolated fast diagnostic build, all 35 selected host
checks, six-path formatting and clang-tidy for core/task/exec pass. Actual
rootless KTEST29 passes 80,800 assertions, zero failures, 1,331 cases and all
80 mandatory checks with normal poweroff and stable source/artifact pins.
Its four inactive images (259,342,336 allocated bytes) were verified and pruned;
disposable usage returned to zero. The initial diagnostic fixture compile error
and its reproducible source patch remain in the small evidence receipts.
All ten full acceptance criteria remain incomplete.

## Private procfs cwd under an owned native root

Private procfs directory descriptors now retain a detached search pin and an
immutable directory view beneath the process's native TMPFS or XFS root.
The internal pin factory clones the exact retained PID view and target; it
does not look up a PID string or read process content. Ordinary procfs opens
still reject access mode 3. A fresh directory File receives its prepared view
before publication; reopening an inherited descriptor preserves that File's
existing capability.

Relative cwd and directory-FD walks start from retained procfs metadata.
Canonical names use its exact target identity. Native membership validation
checks the private procfs mount's original cover, attachment and ancestry;
`..` can leave the procfs root through that cover, while the native process
root remains the boundary. Procfs directories cannot become process roots.
Renamed or replaced covers, closed views and foreign mount/PID scopes refuse
before cwd publication or descriptor reservation. Exhausted private lookup
retries return EAGAIN; Host lookup keeps its serialized fallback.

Cwd search uses typed procfs metadata and the admitted credential generation.
The anchor retains its mount and immutable view without treating procfs data
as a native inode. Replaced views, pins, anchors and membership owners retire
outside publication and path locks. Unpublished rollback counts a self-target
cwd pin once and refuses external File, view or anchor aliases. Task/File/view/
anchor layouts, callbacks, syscall/libc/wire ABI and Host behavior are unchanged.

Four new native fixtures cover cwd/dirfd/magic-link traversal, the native parent
boundary, same-mount foreign PID scopes, replaced covers, closed views and
self-cwd alias refusal/drain. These are unscheduled native fixtures; scheduled
service entry, namespace-init teardown, complete WKI/network/resource policy,
shared-futex user mappings, the launcher and full service campaigns remain
unqualified. All ten full acceptance criteria remain incomplete.

Normal Build WOS, isolated fast diagnostics, all 35 selected host checks,
five-path formatting and clang-tidy for core/procfs pass. Actual rootless KTEST31
passes 80,916 assertions, zero failures, 1,335 cases and all 84 mandatory checks
with normal poweroff and stable source/artifact pins. An earlier VM exposed a
missing directory-view attachment and a private lookup entering the Host
serialized fallback; both were corrected and requalified. Four images from each
failed and successful run were verified inactive and pruned; final disposable
usage is zero. Small source reproduction, log and cleanup receipts are retained.

## Private orphan ownership

Orphans from a private PID view now adopt through that view's retained local
PID 1. Private init does not select a reaper for itself. Missing, closed or
dying private reapers cause the existing parent-audience cleanup; there is no
fallback to host PID 1. Host orphan adoption keeps its physical-ID behavior.
Host supervision of a top-level private init remains an explicit membership.

Registry selection retains the exact reaper before the lifecycle lock. Private
transfer then holds the PID publication gate through membership and event-owner
mutation, checking the reaper's exact scope, local name, process type, ACTIVE
state and exit flags, plus the child's same-scope identity. Namespace closure
cannot split that commit. Canonical and proxy exit publish their exit flag
before the final child scan, so a transfer that wins earlier is included in
that scan. Scheduler wakes and parent-death routing occur after both guards.

Queued group names, status and accounting stay frozen when their owner moves.
Old parent claims are invalidated; independent tracer claims remain valid.
Task/namespace/event layouts and syscall/libc/wire ABI are unchanged. Native
fixtures exercise actual registry selection, ownership/claim transfers and
reference drains using per-invocation cutovers without a global hook or fake
runnable Tasks. The deferred retirement implementation below adds namespace
closure and exit requests; scheduled service teardown still needs qualification.
Public creation remains disabled and all ten full criteria remain incomplete.

Normal Build WOS, isolated fast diagnostics, all 35 selected host checks,
three-path formatting and clang-tidy for child events pass. Actual rootless
KTEST33 passes 81,243 assertions, zero failures, 1,338 cases and all 87 mandatory
checks, with normal poweroff and stable source/artifact pins. An earlier run
exposed fixture CPU accounting populated in cached Task fields; the fixture now
uses production resource accounting, preserving the expected event snapshots.
Four images from each failed and successful run were verified inactive and
pruned. Final disposable usage is zero; small failure, source and cleanup
receipts remain.

## Deferred private-init retirement

Canonical process exit and deferred local proxy exit now start retirement when
the subject is the exact private local PID 1. The PID publication gate closes
the view once; private mount and named-IPC views also close. Host views remain
open, including mixed-view internal fixtures. Late private child membership
checks closure and parent exit under the lifecycle lock and PID gate. Existing
construction can abort through its original ownership cleanup.

A kernel worker starts after Host init creation and before user dispatch.
Its static 64-record pool matches the PID namespace pool. Each occupied record
retains a distinct PID view and its mount/IPC owners, without retaining Tasks
or allocating on init death. One dispatch selects one record, retains at most
16 exact-scope victims, publishes cooperative fatal exit requests, and attempts
one mount teardown. Registry, lifecycle, PID and record locks are released
before delivery or backend cleanup. Ordinary executor-owned exit performs
File, mapping, ptrace, WKI and child cleanup; the worker never forces another
Task into DEAD. Registry-only construction remains part of the pending drain
but receives no scheduler wake before runnable publication.

Closed named IPC loses new name admission while inherited attachments retain
their backing. Private mounts retire leaf first and root last. A busy File,
mount reference or pivot leaves a retryable closed view; explicitly inherited
Files keep their existing capabilities until final close. Pending executor
cleanup and admitted PID operations keep the record alive. Ref drops and
backend work occur outside service locks. Retry budgets bound each dispatch;
they do not impose a deadline that frees live capabilities. Task/File/event,
syscall/libc and WKI wire layouts remain unchanged; the opaque PID registry
adds an internal retirement bit that resets on slot reuse.

Four native cases exercise closure, 16-victim batches, exact foreign/Host
exclusion, pending requests, admitted-operation drains, late construction,
all available PID slots, and retained-File mount/IPC retirement. They share
production dispatch/request stores with an explicit adapter for inert fixtures.
Normal Build WOS, isolated fast diagnostics, 36 selected host checks,
13-path formatting and seven normal clang-tidy units pass. Actual rootless
KTEST34 passes 81,899 assertions, zero failures, 1,342 cases and all 91 mandatory
checks, with normal poweroff and stable source/artifact pins.

A separate normal VM reaches Host PID 1, root pivot, journald and netd after
mandatory worker publication. Its original three-service probe fails because
the deliberately unmanaged interface supplies no usable address: netd's normal
readiness condition holds network-dependent services. The failed probe and
exact-owned stop receipt remain. This provides limited initial Host startup
evidence; worker-entry execution, scheduled private-init death, complete Host
services and full lifecycle campaigns remain unqualified. All eight disposable
images from both runs were verified inactive and pruned; final usage is zero.
Public creation remains disabled and all ten full criteria remain incomplete.

## Live ambient network policy

The opaque native PID namespace stores an immutable `Network` value before its
first reference is published. Default private creation uses `DENIED`; Host
ordinary networking remains shared. A kernel-only exact-reference factory can
request `SHARED_HOST` only from an open Host or already shared parent. Invalid,
aliased, denied, closed and exhausted requests preserve the output and owner
counts. Final publication rechecks the parent's closure under its existing PID
gate. Reused slots reset policy; generations provide no public join authority.
Canonical child/thread ownership and exec retain that same policy. This factory
does not install the full credential/resource profile or enable public creation.

Socket creation checks the canonical subject's exact PID owner before raw
credential authorization, FD reservation or socket allocation, following existing
domain/protocol validation. All eleven global interface/address/route/link/queue
controls check policy before usercopy, device/registry/WKI work or output changes.
Unknown canonical ownership returns ESRCH, closed private admission EIDRM, and
live denied admission EPERM. Explicit shared networking retains the existing raw
NETWORK_ADMIN check and Host argument/backend behavior. No independent network
stack, Task/File layout, syscall/libc ABI or WKI wire change is introduced.

Already inherited socket Files remain intentional shared capabilities, including
their protocol operations and sockets accepted from a granted listener. They
remain usable after namespace closure until final close. Such a descriptor does
not grant arbitrary global network control. A future launcher must list these
grants; this checkpoint does not promise revocation of existing capabilities or
already admitted shared operations. The production syscall and a per-invocation
native adapter share the same dispatcher and explicit subject helpers. Actual
executor parking and File operations retain existing semantics; inert native
fixtures use nonblocking calls without replacing current-task state.

Three native cases exercise real IPv4/IPv6 TCP/UDP/raw socket allocation,
full-authority default denial with NOFILE zero, all global-control ordinals with
bad pointers, canonical inheritance, closed/unknown owners, raw credential
refusal, retained inherited UDP Files and getsockopt into actual mapped user
memory. FD/reservation/resource/namespace baselines and failed outputs are checked.
Six production PID-registry host tests cover bounded delegation, closure,
exhaustion/reuse and concurrent closed-parent refusal. Normal Build WOS,
isolated fast diagnostics, 38 selected host checks, eight-path formatting and two
normal clang-tidy units pass. Actual rootless KTEST36 passes 82,083 assertions,
zero failures, 1,345 cases and all 94 mandatory checks, with normal poweroff and
64 stable runtime source/artifact pins.

Failed KTEST35 exposed native fixture integer-width comparison and unsupported
raw protocol zero. Its 39 observed assertions, exact failed sources and audited
stop remain; the corrected fixture uses checked int results and supported raw
protocols without changing production policy or expected denials. All eight
disposable images from failed and passing runs were verified inactive and pruned;
peak allocated usage was 259,866,624 bytes and final disposable usage is zero.
Small evidence is retained. WKI compute/export bypasses ordinary socket syscalls;
its separate namespace admission is described below. Scheduled private services,
full profile creation, remaining device/global policy and all ten complete goal
criteria remain unqualified.

## Private WKI compute and FD export admission

Current WKI contexts have no enforceable import contract for private native PID,
mount or named-IPC identities. An effect-free helper observes all three exact
canonical subject owners. Any unknown owner returns ESRCH, any closed owner
EIDRM, and any open private view ENOTSUP. An explicit shared networking grant
does not make these process/namespace restrictions portable. Host execution
keeps its original compute, IPC, wire, argument and lifetime behavior.

Rich placement checks scope before initialization fallback, placement flag writes,
peer/load reservations, exec callbacks, channel allocation and exports. Ordinary
automatic or explicit local execution keeps the same private subject local.
Named, remote-preferred and balanced requests fail ENOTSUP, including requests
that permit fallback. Named-self spelling is also unsupported; explicit local
placement is available. Unknown or closed ownership fails before local return.
Existing result error/deferred-ID outputs retain their defined initialization.

Both direct submission modes retain existing argument/count validation and
executor-linked guard adoption. Scope refusal then runs the original cleanup
before credential/filesystem grants, resources or submission effects. Already
supplied handoff pins and exports are released exactly once; local descriptor
slots remain owned. Fatal abandoned-stack recovery and unlocked potentially
blocking cleanup retain their existing protocol. Task FD export refuses before
snapshot pins, allocations, monotonic IDs and registry/backlog publication.
Its scalar failure outputs remain zero and map/handoff arrays stay untouched.
Intentionally inherited local or remote File operations remain capabilities.

Two native cases use all seven combinations of independently installed views,
explicit Shared network policy, canonical bound/unbound threads and closure.
They call actual rich/direct/export APIs, check untouched callbacks, fields,
maps, descriptors, resource and owner baselines, and exercise real pipe handoff
cancellation with retained File pins and preserved buffered payload. No current
task replacement, fabricated runnable state or peer mutation is used. These
fixtures remain unpublished Tasks. Full target/routing/VFS/device/global policy,
public service creation and all ten complete goal criteria remain unqualified.

Normal Build WOS, isolated fast diagnostic build, 38 selected host regressions,
current C++ formatting and the two unchanged normal WKI clang-tidy units pass.
Actual rootless KTEST38 passes 82,797 assertions with zero failures across 1,347
cases, including all 96 mandatory checks and normal poweroff. All 70 runtime
source/artifact pins remain stable. Mechanical comparison preserves the complete
Host compute and IPC implementation apart from the new admission gates, and
confirms unchanged Task/File ownership, syscall, libc and wire surfaces.

The first run exposed a fixture ownership error: a borrowed pipe File was passed
to an owning handoff guard. Its two failures and KASan panic remain recorded;
the fixture now uses the actual retained-file API. Production policy and expected
errors did not change, and final builds, host checks and runtime were repeated.
All eight disposable images from failed and passing runs were verified inactive
and pruned. Peak allocated usage was 259,932,160 bytes; final disposable usage
is zero. Small reproducible evidence remains in the local Goal39 ledger.

## Private WKI target configuration

The actual target SET/GET implementations borrow the calling executor and observe
its exact retained canonical PID, mount and IPC owners. Configuration fields keep
their existing executor ownership; thread namespace admission uses the retained
process owner. Unknown ownership returns ESRCH and closed ownership EIDRM.
Existing invalid SET flags retain EINVAL precedence.

Open private views permit automatic or explicit local placement, including
strict, no-inherit and one-shot modifiers. A null hostname or zero length keeps
the existing clear semantics. Valid named, remote and balanced requests return
ENOTSUP before hostname usercopy or field mutation, regardless of fallback.
Explicit shared networking does not authorize private remote placement. Private
GET may read its own configuration, with both outputs preflighted before copying;
closure refuses before observing fields or touching output. Host validation,
usercopy, output staging, fork/clone inheritance and exec one-shot behavior are
preserved. The execution and export gates above still independently apply.

Three native cases call those same implementations for every private view
combination, canonical and unbound threads, mapped local-policy outputs, closed
output sentinels and real Host hostname copies and errors. They remain
unpublished fixtures. Forwarding/default-global controls, public creation and
the complete service boundary still require integration and qualification.

Normal Build WOS, isolated fast diagnostics, formatting/clang-tidy and 43 focused
Host checks pass. Actual rootless KTEST39 passes 83,210 assertions with zero
failures across 1,350 cases, all 99 mandatory checks and normal poweroff; all 70
runtime pins remain stable. Its four inactive disposable images were audited
and removed, leaving zero disposable usage after a 259,932,160-byte peak.

The expanded 44-check Host selection initially failed two existing source
audits. Both failures were reproduced against the committed baseline. The exec
checks now follow namespace-visible child/TCB identities and retain publication
ownership and duplicate-spawn protection. The broader credential/VFS audit still
has 24 baseline oracle failures requiring investigation; it is not qualified by
the focused passing suite. Full Host regression and all ten goal criteria remain
incomplete. Failed and passing evidence is retained in the local progress ledger.

## Host hostname and power controls

Host-wide hostname publication and shutdown require the caller's exact canonical
Host PID, mount and IPC views as well as the existing credential authority.
Any private view returns EPERM, even with full root authority or explicit shared
networking. Unknown ownership returns ESRCH; closed ownership returns EIDRM.
Hostname refusal precedes usercopy and global publication. Shutdown preparation
and valid reboot commands refuse before phase changes, kernel-thread quiescence
or finalization. Invalid reboot commands retain EINVAL precedence and do not
observe current-task state. Host hostname reads, power-state reads, ordinary Host
validation and effects, and trusted boot/selftest backends remain unchanged.
No syscall selector, wire format or Task/File layout changes.

Three native cases call the shared production implementations through borrowed
subjects, covering all seven private view combinations, mapped hostname input,
retained canonical threads, unknown/closed ownership, dropped root authority and
nonroot credentials. Host error and harmless-command checks preserve the actual
hostname and shutdown phase. These remain unpublished fixtures; power subject
adapters exist only in diagnostic builds.

Final Normal Build WOS, isolated fast KTEST build, formatting/clang-tidy, 44
focused Host checks and 71 mechanical preservation checks pass. Actual rootless
KTEST41 passes 83,682 assertions with zero failures across 1,353 cases, all 102
mandatory checks, normal poweroff and 88 stable runtime pins. The earlier run40
also passed, but review found an invalid-command/current-task ordering change;
its exact source was preserved and reconstructed, the ordering restored, and all
validation repeated on final code. Both runs' eight inactive disposable images
were audited and pruned. Peak disposable usage was 260,128,768 bytes and final
usage is zero.

The broader credential/VFS source audit still has 24 failures from 129 current
oracle calls. It remains unqualified. Routing, other Host controls, public
construction/launcher and the complete service/lifecycle campaigns still require
work; all ten full goal criteria remain incomplete.

## Native VFS routing and forwarding admission

The four ambient WKI VFS rule controls now require open canonical Host PID,
mount and IPC views before copying user input/output, changing executor rules
or exposing boot defaults. Private views return ENOTSUP, unbound subjects ESRCH
and closed views EIDRM. Existing Host signatures delegate to shared explicit
subject implementations. The trusted boot-default getter and null-subject
effective-route query remain pure queries without current-task retrieval.

Private native path routing ignores inherited rules, global defaults and
submitter aliases. Native/public lookup refuses remote name expansion,
physical/export requests and declined compatibility fallback before discovery
or remote metadata effects. Remote directory FDs cannot authorize new child
name requests in an unsupported private scope. Direct inherited File operations
retain their grants across namespace closure. No syscall selector, wire format,
Task/File layout or ordinary Host descriptor implementation changes.

Three native cases exercise the actual shared controls with mapped Host/private
buffers, all seven private scope combinations, retained canonical threads,
closed/unbound owners, independent TMPFS backing, inherited routing fields,
literal private `/wki/host` directories and installed File capabilities. The
remote directory marker is disconnected; these tests do not qualify a live
peer or public service boundary. An older local-path case now verifies the
intended exact-mount success while preserving physical and closed refusals.

Final Normal Build WOS, isolated fast diagnostics, formatting/clang-tidy,
44 focused Host checks and 65 complete-source comparison checks pass. Actual
rootless KTEST44 passes 84,244 assertions with zero failures across 1,356 cases,
all 105 mandatory checks, normal poweroff and 95 stable runtime pins. Failed
attempt42 exposed an obsolete expectation and a raw fixture without file
operations; attempt43 exposed its disconnected marker's missing owned
reference. Fixtures were corrected, diagnostics stayed enabled and final
validation repeated. Both failed sources reconstruct exactly from the committed
parent and retained patches. All twelve disposable images from the three runs
were verified inactive and pruned: 515,293,184 allocated bytes total, peak
260,194,304 bytes and final zero. The unrelated user VM was preserved.

The broader current credential/VFS audit still reports 24 failures in 129
oracle calls and remains unqualified. Public construction/launcher, global
controls, actual shared-futex/service/lifecycle campaigns and complete Host
regression still require work. Goal39 remains active and all ten full criteria
remain incomplete; see the checkpoint25 evidence and construction map.

## Native unpublished image construction

The internal `create_file_backed_process_task_for_owner` constructs an admitted
private process image with an explicit recovery executor, three open private
namespace references and a `PreparedFilesystemContext`. Executables are native
TMPFS regular Files. This kernel C++ API supplies no public creator permission,
userspace Task pointer or integer join handle. The production caller must be
the sole executing recovery owner; unpublished fixtures are not runnable services.

The input guard covers Task allocation/reclaim and failure releases until the
child slot owns the File and kernel stack. A fresh default Task then initializes
its name and pagemap through a recoverable skeleton. The separate may-fail
pagemap API preserves the existing table pool and default allocator behavior.
Child-owned exec scratch holds main parser tables, copied launch arguments and
interpreter File/metadata/bytes before reads;
unpublished teardown retires this scratch before anchors, Files and user memory.
The native builder obtains a stable executable lease, checks fresh size/execute
permission, and attaches buffered bytes directly to the owned child before I/O.
Its short lease-cloning guard covers temporary backend owners as well.

Small dynamic and static images use a complete source bounded at 8MiB. Larger
dynamic images keep bounded metadata and lazy loading. A static fallback above
8MiB returns E2BIG; name/pagemap/scratch/buffer allocation refusal is recoverable.
The existing Host builder, PROCESS constructor, Host interpreter behavior and Task
member layout remain preserved. These are internal API changes, not syscall,
libc or WKI wire changes.

Resources, the complete immutable credential and UID population charge are
installed before namespaces. Both root/cwd anchors must belong to the exact
private native TMPFS mount view. Installation checks matching credentials,
current namespace/mount epochs, native search/membership and exclusive empty
unpublished ownership, then retains both anchors under the Task path lock.
Anchor and membership retirement stays outside publication/path locks. ELF,
TLS and initial TCB setup run afterward with the child's local process ID.

For every private PID/mount/IPC image subject, PT_INTERP uses the child's
filesystem and ignores the executor's paths and remote submitter identity.
Unknown/closed subjects fail before worker selection, VFS or cache access.
The existing all-Host temporary worker routing and complete read/execute,
freshness/content-match cache and debug/mapping behavior remain preserved.
Owned recovery holds the child through potentially blocking failure teardown.

KTEST48 ran four real native construction cases: two independently populated
TMPFS fixtures with different image/interpreter bytes and a foreign executor;
local TCB IDs; foreign-root/closed/missing-owner refusal; renewed search and
credential checks; interpreter execute denial and File/mount/thread/population/
namespace recovery. Full suite passed84,418 assertions, zero failures,1,360
cases/all109 mandatory/110 stable pins and normal poweroff. Normal Build WOS,
isolated fast diagnostics, focused44 Host checks, formatting/tidy and41 complete
source/body/header/ABI preservation checks pass. Broad129 credential/VFS source
calls still have24 failures, so full Host regression remains unqualified.

Checkpoint27 KTEST49 additionally qualified small dynamic/static mapped images,
local TCB identity and foreign-executor interpreter selection, real executable
write refusal until teardown, five one-shot allocation refusals, transferred
owner-slot recovery of prepared metadata, busy-owner preservation and oversized
static refusal. The complete run passed84,669 assertions/0 failures/1,364 cases,
all113 mandatory and117 stable pins (115 distinct paths), with normal poweroff.
Normal Build WOS, isolated fast diagnostics,44 focused Host checks, formatting/
tidy and52 complete-source preservation/ownership checks pass. Four proven
inactive disposable images were pruned; no completed-run VM disks are retained.

Checkpoint28 prepares native PT_INTERP directly through the child's native
TMPFS path and admitted credential. Its actual stable File and compact metadata
or complete bytes belong to child recovery; complete fallback is bounded at 8MiB.
The executor guard defers fatal abandonment over interpreter VFS/loader locals
and potentially blocking scratch retirement. Ordinary Host interpreter/cache
behavior is unchanged.

Optional `NativeProcessArguments` supplies stable kernel path/argv/envp views.
Capture precedes image reads and rejects relative paths, embedded NUL, paths of
512 bytes or more, vectors above 256 entries and combined strings/pointers above
2MiB. Child-owned arrays/arena survive partial preparation. The mapped initial
stack contains argc, argv/envp sentinels, main ENTRY/PHDR, optional interpreter
BASE, credential IDs/AT_SECURE, stable EXECFN and 16 entropy bytes. Writes use
the actual child pagemap and stack policy; rsp is 16-byte aligned and registers
publish only after completion. Null arguments remain an image-only internal
option; this is not a public launcher or scheduled service qualification.

KTEST51 passed 85,923 assertions/0 failures/1,368 cases/all 117 mandatory, 122
stable pins (120 distinct paths), and normal poweroff. Four new cases cover
multi-page copied arguments and empty vectors, static/small/sparse interpreter
stacks, nonroot IDs/groups and explicitly inherited sealed journal grants,
four partial allocation refusals, transferred interpreter/stack recovery and
invalid argument refusals with File/mount/thread/namespace/population baselines.
Normal Build WOS, isolated fast diagnostics, 44 Host tests, format/tidy and 56
preservation checks pass. KTEST50 was stopped after a fixture supplied the journal
grant without its required ceiling; only that fixture was corrected before 51.
Eight proven inactive disposable images from both runs were pruned; final usage 0.

Public creation was disabled at checkpoint 28. Checkpoints 29 and 31 add canonical
creator capture, owned root population and a kernel-only local publisher;
checkpoint 32 adds versioned public input and the launcher. Explicit shared
mapping qualification and scheduled publication remain required. Other image/
construction allocation paths and actual scheduled fatal races are not covered
by these nine targeted allocation refusals or transferred recovery hooks.
Checkpoint 30 supplies private-view admission for DEBUG, service-resource
controls and the selected Host-global operations described below. The remaining
explicit FD/device/perf/indirect WKI consumer census still needs qualification. The current129 credential source calls still have24 failures.
No scheduled service, shared-futex, two-hour or1,000 full lifecycle claim follows
from these unpublished native cases; all ten Goal39 criteria remain incomplete.

## Native creator and fresh-root preparation

`prepare_native_compartment_for_owner` accepts a bounded kernel spec and owns
the complete unpublished preparation in the executing recovery owner. It retains
the canonical creator, observes credentials and their generation together with
all sixteen limits, selected service and creation mask, and revalidates those
observations after blocking work. Creator/request/service limits intersect;
aggregate service affiliation is attached before image mapping without replacing
the admitted limits. Initial creation supports local Host creators. Nested/private
and remote creators are explicitly refused.

Preparation reserves process population before imports, creates fresh native
mount/PID/IPC objects, and provisions a TMPFS root owned by the admitted effective
UID/GID. A nonrunning mount-only populator uses admitted credentials and does not
consume the service's private PID1. The root is pinned through its exact retained
mount identity; typed local view lookup retains scope, search, membership, epoch
and confinement checks. Foreign scopes are refused, closed scopes return EIDRM,
and untyped physical/remote export remains unsupported.

The spec permits at most 64 imported readable local TMPFS/XFS regular Files and
256 explicit regular-File descriptor grants. Destination paths are copied,
canonical absolute, below 512 bytes, at most 16 components and fewer than 256 bytes per component;
mode is restricted to 0777. The executable name must also fit Task's 256-byte field.
Rounded import pages fit a page-aligned root budget from 4KiB through 32MiB and
each asset respects FSIZE. Stable native leases/copies and bounded 8KiB pread/
pwrite chunks preserve imported bytes against source mutation. Descriptor indices
are unique, below the actual table capacity and admitted NOFILE soft bound; no
ambient descriptors or stdio are copied. Existing descriptor rights are explicit
capabilities, not fresh access granted by path membership.

The existing private main/interpreter/argument-stack builder consumes persistent
inputs. Fresh job names are local 1, terminal association is empty and creation
mask comes from the observed creator. Root/import Files, filesystem context,
namespace references, population reservation and partial children drain through
owned abort/recovery paths. Successful preparation retains root state; ownership
is not declared transferred before real publication.

Public creation was disabled at checkpoint 29. Checkpoint 31 implements native
local publication and root ownership transfer, and checkpoint 32 supplies a
versioned userspace spec/launcher. Runnable success, scheduled races, explicit
shared mappings and complete FD capability qualification remain required. Checkpoint 30
closes the selected DEBUG, CPU-domain/affinity, swap, init mailbox/status, service
resource, global sync and log-block/framebuffer gaps. Remaining explicit device/
FD/indirect consumers still need qualification for the experimental public interface.
Scheduled fatal races, actual shared futex/init death, two
service trees on one kernel for 2 hours and 1,000 full lifecycles remain unqualified.
The current 129 credential source calls still have 24 unchanged failures. All ten
full Goal39 criteria remain incomplete and the goal is ACTIVE.

KTEST56 passed 86,874 assertions/0 failures/1,375 cases, including all 124
mandatory cases and seven new native creation/service cases, with 127 stable pins
(125 distinct paths) and normal poweroff. Normal Build WOS, isolated fast
diagnostics, 44 Host tests, format/tidy and 67 compatibility checks passed. Tests
cover independent nonroot-owned roots, actual imports/main/interpreter/initial
stacks, explicit retained descriptors, canonical thread creators and changed
credentials/limits/umask/service selection, aggregate service membership, eight
partial allocation/import/recovery faults and invalid capability/path/budget
refusals. They use unpublished real kernel Tasks; they do not qualify scheduled
services or scheduled fatal-signal races. Failed attempts 52–55 remain recorded.
Twenty proven inactive disposable images from all five attempts were pruned;
final disposable allocation is 0. Required seeds/current artifacts and unrelated
user VMs were preserved.

## Host-global syscall controls (checkpoint 30)

Private native Tasks cannot use DEBUG interrupt control, physical CPU affinity or
CPU-domain controls, swap activation/deactivation, the global init mailbox/status
API, global service configuration/selection/observations, filesystem-wide SYNC,
userspace log-block ownership, or VGA logging. Each consuming adapter checks the
exact canonical retained PID, mount and IPC views before usercopy, physical Task
lookup, global locks or effects. Any private view refuses with EPERM, a closed
view with EIDRM, and a missing canonical owner with ESRCH. Existing internal null
exceptions for swap and service management remain. Root credentials and an
explicit shared-host network grant do not confer Host scope. Thread shadow
fields neither grant nor revoke its retained process owner's scope.

Syscalls pass their actual current Task into kernel-only consuming adapters.
Host authority checks, the physical original-init role, validation errors and
existing implementation bodies remain; the service snapshot helper stays private
and grants friendship to its consuming reader. No selector, public ABI/libc,
wire layout, Task/File member, scheduler primitive or swap backend changes.
Trusted shutdown/swap backends and FSYNC of explicit File capabilities remain.

Ordinary bounded copied logging still writes the shared journal. PERF_TRACE
retains the existing per-caller startup tracing behavior. This is shared output,
not a private journal or a qualified producer-rate boundary. VGA and log-block
controls require Host scope. No broad ban on routine service logging is added.

The existing service-authority fixture now checks an unbound thread, then binds
a real retained Host owner and preserves its original non-init-thread refusal.
Four actual native KTEST cases cover all seven private-view combinations with
root/nonroot identities and shared networking; canonical bound, unbound, null
and closed owners; copied closed thread fields; mapped input/output sentinels;
and safe Host validation and original-init errors. They invoke the production
adapters, retain actual native scopes/Task mappings and check ownership/count
baselines. They never execute Host raw cli/sti, activate swap, create a CPU domain
or run a scheduled private service. Full publication/launcher/lifecycle/escape,
two two-hour trees on one kernel, 1000 complete cycles and broader regression
qualification remain required. KTEST58 passed 87,570 assertions/0 failures/1,379 cases, all 128 mandatory cases
with 136 stable pins (134 distinct) and normal poweroff. Normal Build WOS,
isolated fast diagnostics, all 44 Host tests, format/tidy and 58 compatibility
checks passed. All 129 current credential-audit calls still report the same 24
failures; both complete audit source files preserve their original assertions
and add checks at the new consuming bodies. KTEST57 failed only the service
sentinel/new fixture and old unbound-role expectation, then was stopped and
reconciled before correction. Eight exact-owned inactive images were pruned
(457,211,904 allocated bytes); final disposable usage is 0. Required seeds/current
artifacts and unrelated user VMs remain. All ten full criteria remain incomplete;
Goal39 is ACTIVE.

## Native local publication (checkpoint 31)

`publish_native_compartment_for_owner` consumes only the executor's exact pending
native preparation. Under the existing VM publication mutex it revalidates the
canonical creator policy, checks live matching scopes and local PID 1, links
child-event membership and uses the existing explicit local scheduler post.
It returns the physical PID for Host supervision. Policy changes, closed scopes,
shutdown and exiting or mismatched owners refuse before publication. Failed
membership or post leaves preparation owned for explicit abort and removes its
pending child-event references before destruction.

Successful post transfers namespace retirement to the private init lifecycle.
The child stays pinned through commit; ordinary Host-parent exit may adopt it
into Host init. A runnable child never produces a retry result merely because
its parent audience changed. Request cleanup finishes prepared mount state while
the root pin protects it, then releases its references outside the publication
mutex without closing transferred scopes. Task/File layouts, public syscall
and libc ABI, WKI formats and ordinary Host spawn behavior are unchanged.

All sixteen limit setters and service selection now coordinate with final
publication. Limit authorization reads fresh credentials after that guard;
usercopy remains outside it. Private processes can lower hard limits and change
soft limits within their current hard ceiling. Retained root authority cannot
raise that ceiling. Existing privileged Host hard-limit behavior is preserved.

KTEST59 passed 87,822 assertions, zero failures and 1,383 cases, including all
132 mandatory cases. Four new native cases exercise mapped limit setters,
private hard-raise refusal, policy/scope/owner refusal, actual child membership
rollback and the real local-post invalid-CPU refusal. These run before normal
scheduling; successful runnable publication is still unqualified. Normal Build
WOS, isolated fast diagnostics, all 44 focused Host tests, format/tidy and 38
preservation checks passed. The broader 129 credential source calls retain the
same 24 failures. All 137 source/artifact pins (135 distinct paths) remained
stable through normal selftest poweroff. Four exact-owned inactive images were
pruned (261,308,416 allocated bytes); final disposable usage is zero. Public
launcher/spec, scheduled hostile/fatal/init/futex tests, two two-hour service
trees and 1,000 complete lifecycles remain required. Goal39 is ACTIVE and all ten
full criteria remain incomplete.

## Versioned public input and launcher (checkpoint 32)

Process operation 54 accepts the executing process's version-1 `Spec` address.
The standalone header is installed at `/usr/include/wos/compartment.hpp`.
Its fixed 408-byte layout uses explicit 64-bit addresses/counts, sixteen resource
limits and zero-required flags/reserved fields. Existing operation numbers,
libc wrappers, Task/File layouts and WKI wire formats remain unchanged.

The kernel admits the exact canonical local Host creator before copying input.
It owns bounded staging in the existing persistent preparation request before
faulting usercopy, retains source FDs from that executor's canonical table, then
reuses native policy capture, root import, ELF/stack preparation and local
publication. Refusal aborts the exact owned child/root. A successful call returns
the physical Host PID for supervision; it exposes no namespace join handle.

Inputs allow 1–64 assets, at most 256 descriptor grants and 256 entries per
argument/environment vector, with a combined 2MiB string/pointer budget. Imported
data has a whole-page 4KiB–32MiB budget. Destination paths are canonical absolute
names below 512 bytes; the existing executable observation currently imposes a
stricter 255-byte executable-path bound. Empty argument strings are legal;
embedded NULs, unsupported versions, invalid records and partial copies refuse.

`/usr/bin/wos-compartment` defaults to denied networking, zero retained authority,
zero service privileges, disabled dumpability, an empty environment and no ambient
FD inheritance. It reads all sixteen current limits and accepts lower overrides.
`--asset SOURCE DEST MODE` copies readable regular Files into fresh private TMPFS;
`--fd SOURCE_FD DEST_FD` retains a local regular File's rights, offset and shared
mapping identity. Directory, device and socket grants are unsupported. An XFS
descriptor backed by a WKI block proxy is rejected before `fstat` or import I/O,
including partition ancestry and inactive/fenced proxy callbacks. Host asset
imports may read such storage because only copied bytes enter the service root.
The ancestry classifier performs no transport, registry, allocation or lock work.
Actual mounted remote-storage and indirect-consumer campaigns remain required.

The launcher prints assets, descriptor grants, network policy, authority,
dumpability, service privileges and all limits. It opens/owns only its asset FDs
and borrows explicit grant FDs. Foreground supervision uses the existing raw
64-bit WAITPID interface; `--detach` requests ordinary Host parent/adoption
behavior. Help and usage are documented in `modules/compartment/README.md`.

KTEST60 passed 88,543 assertions, zero failures and 1,389 cases, including all
138 mandatory cases and six new mapped-input/provider cases. Actual nonroot
mapped creation reaches a deliberate before-post refusal after native import,
arguments, empty/cross-page strings and explicit retained FD admission. Invalid
records, pointers, FDs, budgets, allocation failures, busy/foreign/closed owners
drain exact Task/File/thread/population and mount/PID/IPC owners. This deliberately
does not schedule the synthetic ELF. The actual launcher parser passes a Host
ASan/UBSan test with adapted raw syscall boundaries, including a PID above 32 bits;
that test is not guest execution evidence.

Normal Build WOS, isolated fast diagnostic build, all 46 selected Host tests,
format/tidy and 43 preservation checks passed. All 149 runtime pins (147 distinct
paths) stayed frozen through normal selftest poweroff. Four exact-owned inactive
images were pruned (261,373,952 allocated bytes); final disposable usage is zero.
The 129 broader credential source calls retain the same 24 failures. Successful
runnable publication, CPU placement inheritance review, guest CLI, scheduled
hostile/init/futex tests, two two-hour service trees and 1,000 complete lifecycles
remain required. Goal39 stays ACTIVE; all ten full criteria remain incomplete.

## Checkpoint33: actual public service execution

The native unpublished skeleton now initializes the detached runqueue index,
nice-0 weight and 10ms slice before its first allocation, matching ordinary
process construction. The previous zero heap index caused an actual normal
guest publication panic. Scheduler corruption checks, Task/File layouts,
ordinary Host constructors and ABI/wire surfaces remain unchanged. The existing
publication-refusal case now checks these fields before and after refusal.

The reproducible probe and service fragment in
`tests/fixtures/compartment_public_runtime.md` use UID/GID 100, zero authority,
NNP, explicit ELF/interpreter/libraries/data imports and one shared regular
result File. Each of two actual guest modes reports private PID1/parent0,
unchanged nonroot IDs, Host path refusal, marker contents, denied networking,
named shared memory and local child PID2/fork/wait. Both Host waits return
exit0 and both private reports contain14checks/0failures. The probe keeps
small fsynced receipts in an explicitly owned persistent directory; `/tmp` is
root-owned0755tmpfs, so it cannot serve that nonroot diagnostic purpose.

Normal Build WOS, isolated fast diagnostic build,46selected Host tests,
formatting and30preservation checks PASS. Rootless --no-setup KTEST61 PASS:
88,557assertions/0failures/1,389cases/all138mandatory,149stablepins147distinct,
normal selftest poweroff. Normal public attempt07 PASS39.343s/169stablepins;
attempt06's durable guest reports passed while its serial-only observer timed
out. Earlier attempts and the publication panic remain recorded. Both completed
topologies were pruned to0disposable allocation; unrelated user VM preserved.
All129 broader credential source calls still have the same24unresolved failures.

This qualifies short sequential execution, not two simultaneous two-hour trees,
1,000complete lifecycles or the full hostile/isolation/regression campaign.
CPU placement/domain inheritance remains unqualified; restored EEVDF defaults
do not implement that policy. Goal39 stays ACTIVE/all ten full criteria incomplete.

## Checkpoint34: private priority selection and commit

Private GETPRIORITY/SETPRIORITY resolve positive IDs and the executing thread's
zero selector in its canonical, retained PID namespace. Same-UID Host and
sibling physical IDs cannot select a target. Unbound, closed, terminal and
old-image worker observations refuse; unsupported remote targets fail before
effects. Existing credential authorization, nice clamping and libc's encoded
GETPRIORITY result remain. Host physical-ID and zero-selector behavior is
preserved; no Task/File layout, syscall number, libc or wire format changes.

Private updates hold VM publication, verify actual owner-runqueue membership,
then hold namespace publication through the weight/deadline and heap-accounting
commit. Queue refusal leaves those fields unchanged; a registry-only target
cannot use the Host helper's unlocked fallback. Fresh canonical-owner liveness
checks precede the read or commit. Rescheduling runs after queue/namespace locks
are released. These checks do not claim serialization against a later process
exit that does not take VM publication.

Four actual native cases cover local aliases and Host encoding, canonical
threads/stale images, closure/authorization/remote refusals, and exclusively
owned current/handoff/heap/wait queues with exact accounting. Both normal guest
modes pass all 21 checks, including real libc get/set priority, denied raises,
raw 64-bit Host-PID guessing and preserved Host nice values.

Normal Build WOS, isolated fast diagnostic build, 48 selected Host tests,
formatting and 27 preservation checks pass. All 128 credential source tests now
pass: exactly the previous 24 failing consumers follow their current production
delegates; the other 104 tests and all 128 test calls are retained. The historical
129-call census included a display print, not a 129th test. The success message
now follows the final test, and that affected CTest passed again afterward.
This source-audit result does not qualify the entire Host/init/libc regression.

Rootless `--no-setup` KTEST62 passes 88,688 assertions with zero failures across
1,393 cases, including all 142 mandatory cases, with normal selftest poweroff.
Both runtime source/artifact sets stay stable. Public and diagnostic runs prune
their four owned images each, leaving zero disposable allocation; the unrelated
user VM is preserved. Audit `checkpoint-34-audit.json` has SHA256
`942f018c8ef059e942b29fbab627729f488af1373e2ba66500614cf36e63abda`.

Goal39 remains ACTIVE and all ten full criteria remain incomplete. Native
nice-0 defaults do not preserve a restricted creator's priority. CPU mask,
domain and pin inheritance, hostile/shared-futex/init campaigns, simultaneous
two-hour trees, 1,000 full lifecycles with all reference baselines, and complete
Host/init/libc regression remain required.

## Verification checkpoint

The initial `compartment_policy_test` suite checks actual production preparation
and immutable credential APIs, including all ID/group slots, root/nonroot
inheritance, sealed grants, resource bounds, explicit network permission, failed
output preservation and set-ID exec suppression. Deterministic four-generation
profile chains exercise 2,048 independent inputs. Host ASan/UBSan checks and a
compile probe using the current Normal Build WOS freestanding flags are policy
evidence only. Subsequent native namespace, denied-network and owner-drain
qualification is described above. Full scheduled service behavior and lifecycle
campaigns still require integration and runtime qualification.

The production IPC registry has host tests for bounded exhaustion, unchanged
failed output, closure, exact retained ownership, move/alias handling, 1,000
create/close/drain cycles and concurrent retention/creation. These cycles are
registry qualification, not the goal's required 1,000 full service lifecycles.
Native KTEST fixtures call actual SysV creation/control/foreign attach checks,
canonical child/thread inheritance and metadata attachment retirement. Their
attachment fixture supplies no user PTE/shared-futex execution evidence.

The completion campaign must include actual path escape/mount/process/IPC/WKI
tests, two simultaneous independent service trees for two hours, and 1,000
create/exec/kill/destroy cycles with allocation failures and balanced references.
Existing init, filesystem, child events, credentials, resources and libc ABI
outside compartments must pass regression checks.

Owned software tests may use `bin/wos-cluster --no-setup` and isolated
`bin/wos-ktest --no-setup`. Keep one active topology, private COW/reflink images,
a 20GiB disposable budget and 40GiB free-space floor. Audit exact ownership and
live references before pruning disposable images after passing and failing
tests. Retain small evidence and preserve required current/seed artifacts and
all user or unrelated VM disks.

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

## Checkpoint36: immutable CPU-domain publication (2026-10-11)

The fixed 64-slot registry now serializes writers with the existing IRQ-safe
Spinlock and release-publishes the count after every slot is complete. Readers
acquire one immutable prefix and receive const pointers valid for the registry
lifetime. Root/group initialization publishes both slots once; exhaustion or
pre-initialization refusal preserves the count and prior slots. Names are copied
before the lock, and logging stays outside it. CPU indices64 and above return
root without shifting a 64-bit mask by an invalid count.

CpuDomain size56/name offset22, IDs0/1/2..63, parent IDs, name truncation, capacity
and existing CREATE0x106/QUERY0x108 42/264-byte records remain. Task/File/RQ layouts,
scheduler/placement/priority consumers, syscall/wire/libc and existing native
case bodies are unchanged. Five selftests use private instances of the actual
registry methods; they do not consume/reset live registry slots or post fake
runnable Tasks. KTEST parks APs and supplies no SMP execution evidence.

The separate Normal eight-CPU fixture uses actual UID/EUID/GID100 in its Host
supervisor and six workers. Four creators perform64 calls over16 waves:62 unique
IDs2..63 plus2 ENOMEM. Two readers make16,384 queries, checking complete masks,
immutable observations, ticket correspondence, output guards and excluded load
rows. Actual ABI probes include exact page-end42/264 records, one-byte-short
EFAULT and unchanged invalid-ID output. Four further capacity refusals and a
final64-slot sweep preserve all masks. Creators were observed on four CPUs
(bitmap0x72); readers on0x0c recorded69 queries during userspace creator activity.
These samples do not prove overlap inside the writer lock or all interleavings.
Both hard/soft flags are false; RQ stamping and Task placement remain unqualified.

Ten durable files total15,152 bytes. The Host records exact physical child104
wait104/status0 and six distinct TIDs105..110. The separate exit-success-dependent
witness records the existing4160-byte init snapshot: probe103, generation1,
no restarts, EXITED/READY, wait0/error0 and an actual EXIT transition. Its saved
sequence7/publication time is independently decoded and bound to init's exact
probe spawn PID and the pinned service spec. The runner reached its finite120s
runtime deadline; fixture stdout was absent from serial. PASS requires all
durable reports after exact-owned stop. Witness's own later exit and natural
Normal VM shutdown are not qualified by this evidence.

Normal Build WOS PASS44.823s; isolated fast diagnostic build PASS37.980s;
48 selected Host tests PASS37.774s; final format/tidy check PASS6.087s;
standalone fixture build02 PASS0.635s;42 preservation checks PASS.
KTEST65 --fast --no-setup PASS508.530s:89,414 assertions/0 failures/1,403 executed
cases, all152 mandatory, natural selftest poweroff/launcher0 and151 stable pins
(149 distinct paths). Normal SMP01 PASS130.508s with184 stable pins. Initial
KTEST64 could not boot because the sandbox denied QMP socket binding; its FAIL
receipt remains. Authorized nonroot sandbox escalation enabled the unchanged
retry. Disposable four-image prunes removed125,714,432 bytes for failed64,
261,767,168 for65 and91,242,496 for SMP01; final disposable allocation is zero.
Inventories show only unrelated user VM13532 UID945, with unreadable entries0.

Three pre-existing disabled BufferCache registrations were skipped, matching
the qualified prior63 serial; they are excluded from executed and mandatory
counts.

Audit checkpoint-36-audit.json SHA256
856857fdf39d2d5cda7e1d5aece344f3225f897c21c4e80bd484833471045abb PASS.
Independent checkpoint36-evidence-integrity03.json PASS, SHA256
3cc684fcdb4e40af7204ce19306a1d80ef7c823b1ef98605fea5919f223e7d38.
Initial integrity01/02 FAIL receipts remain intact: the checker assumed a wrong
serial prefix, rejected the known repository launcher symlinks, assumed a fixed
last storage event and rejected the three baseline disabled registrations.
The corrected audit independently verifies the actual execution census, exact
baseline skips, source/artifact pins, all ten durable SMP files and zero
disposable images; it does not alter runtime receipts.
Goal39 remains ACTIVE; all ten full criteria are incomplete. Next work is
race-free RQ domain stamps and authoritative hard-domain admission under the
victim lock, then coherent Task placement/inherited immutable CPU ceilings.
Worker-exec actor priority, general fork/thread coherence, WKI priority transport,
hostile/shared/futex/init evidence, two simultaneous2h trees,1,000 full lifecycles
with allocation failures/all reference baselines and complete Host/init/libc
regression remain. Launch only owned tests with --no-setup, preserve Normal/
KTEST isolation, one topology and20GiB/40GiB storage bounds; prune exact-owned
inactive images after PASS,FAIL and exceptions. No pause or push.

## Checkpoint37: atomic RQ stamps and locked hard-domain admission (2026-10-11)

RunQueue domain stamps now use lock-free acquire/release atomic_ref access while
retaining the existing uint32_t member, default, offset and queue layout. Both
advisory readers use one immutable registry snapshot. The actual victim try-lock
callback resolves its current stamp after acquiring that lock, then calls the
shared detach core. The stamp setter holds the same lock. An excluded hard mask
refuses before candidate diagnostics, CPU changes, stale-index repair, heap
mutation or accounting. Root, missing and soft stamps retain their existing
permissive behavior. No allocation, blocking, logging or new lock nesting was
added to these consumers.

The original candidate selection, scan limits, current/handoff exclusions,
CPU-before-remove order, rollback and destination publication are preserved.
The independent preservation audit passed152 checks, including116 protected
source files,28 original scheduler selftest bodies, the complete original native
registration prefix and unchanged header/layout tokens. Its initial FAIL01
remains: the checker assumed an if block where the existing advisory guard uses
an anonymous scope. Corrected receipt02 verifies the actual baseline boundary;
no production change was needed for that diagnosis.

Five appended native cases exercise the same production core with private actual
RunQueues, Tasks, Threads, owned context storage and heap/accounting primitives.
They verify atomic stamp/root/group/missing binding, CPU0/63/64 hard-mask bounds,
changed-snapshot refusal, exact successful heap order/indexes and accounting,
soft/null compatibility, existing Task guards and refusal before stale-index
repair. Wait/dead lists, timers, current/handoff, caches, fairness and Task fields
are checked; all borrowed fields and physical heap entries are cleared before
owned destruction after releasing the fixture lock. No fake Task is PID-registered,
posted or executed, and no live registry leaves are consumed.

Normal Build WOS PASS53.815s; isolated fast diagnostic build PASS56.594s;
48 selected Host tests PASS39.561s; final format/tidy check PASS91.114s.
KTEST66 --fast --no-setup PASS502.650s:89,419 assertions/0 failures,1,408 executed
cases, all157 mandatory, natural selftest poweroff/launcher0, no forced stop and
163 stable source/artifact pins (161 distinct paths). The same three pre-existing
disabled BufferCache registrations remain excluded from executed/mandatory counts.
The exact prior1,403-case set plus five new cases is preserved.

The four exact-owned private images were pruned, releasing261,832,704 allocated
bytes. Final disposable allocation is zero. Complete before/live/after inventories
preserve unrelated user VM13532 UID945; the test alone ran as UID1000. Normal and
KTEST build/data/sysroot isolation,20GiB disposable budget and40GiB free floor
remain. Required seeds/current artifacts and small logs/receipts are preserved.

Aggregate checkpoint-37-audit.json PASS SHA256
492c4e48349ed49aea6058ca21d4bcec623df0b375798106d22268981569a55e.
Private native snapshots qualify the locked core's effects; actual root/group/
missing lookup and source audit bind its fresh production delegation. They do
not qualify a live hard-stamp SMP race. KTEST APs remain parked. Task placement
transactions/inheritance and immutable CPU ceilings are still unqualified.

Goal39 remains ACTIVE; all ten full criteria remain incomplete. Next is consistent
atomic access to all five existing Task CPU/placement scalars, preserving layout
and behavior, then checked owner-locked policy/movement transactions. The current
SET_DOMAIN hard-first move failure and move-capacity/partial-fairness rollback
remain open. Worker-exec/ordinary ancestry, WKI, hostile/shared/futex/init evidence,
two simultaneous2h trees,1,000 full lifecycles with allocation failures/all
reference baselines and complete Host/init/libc regression remain. Continue owned
qualification with --no-setup, one topology and pruning after PASS/FAIL/exception.

Independent checkpoint37-evidence-integrity02.json PASS (all11 sections), SHA256
08bbbfc9a9ac0254849aa7e6ea44f659ee8484beca72d78c542a6a03d8120731.
Its initial FAIL01 and original checker remain intact: relative/absolute source
path spelling and the full157-row launcher pin list did not match the checker’s
initial schema. Corrected02 independently binds those actual rows, all execution
evidence, final boot artifacts, exact four-image pruning and zero disposable
allocation. No production/runtime evidence was changed or test rerun required.

## Checkpoint38: atomic Task placement scalar access (2026-10-11)

The existing Task cpu, domain_mask, domain_id, cpu_pinned and domain_hard members
now have ten lock-free acquire/release atomic_ref accessors. All 146 production
reads/writes in 22 CPP files use those accessors. Types, defaults, visibility,
member order, evaluation order, branches and existing scheduling behavior stay
unchanged. The helpers allocate nothing, acquire no lock, block nowhere and
retain no atomic_ref. An atomic scalar observation does not prove Task lifetime,
queue ownership or a coherent placement tuple.

Actual Clang 22 before/after measurements for both Normal and isolated fast
diagnostic compiler shapes match all 22 layout constants and retained command
arguments: Task size 16208/alignment 8; cpu 48, domain_mask 56, domain_id 8000,
cpu_pinned 11860, domain_hard 11861, deferred_task_switch 12015. Member widths and
alignment are unchanged. Task remains non-standard-layout; native member-address
measurements use integer addresses rather than subtracting unrelated pointers.

Three new TaskPlacementScalar native cases use one exclusively owned actual
heap Task each. They verify defaults/layout, mutable/const round trips, CPU 63/64/
maximum values, mask bit 63, maximum IDs, independent booleans and adjacent
lifecycle/queue/metadata canaries. No Task is registered, posted or executed; no
Thread or pagemap is borrowed. Existing 174 native CPP bodies and the header
selftest block remain byte-exact. Independent production preservation PASS 173
also checks 144 protected sources and all 146 typed access conversions.

Seven Host fixtures were maintained without dropping existing behavior oracles:
six follow actual atomic scalar spellings in extracted bodies or source checks;
the network diagnostic fixture adds its missing explicitly Host-only PID-view/
procfs admission seams. The latter failure was reproduced with the unchanged
fixture against five exact committed150eceb0 inputs before correction. It is a
Host extraction seam, not native namespace/lifetime qualification. Independent
Host preservation PASS 344 binds all seven original/final fixtures and 55 actual
unique passing CTest entries.

Normal Build WOS PASS 61.611s; isolated fast diagnostic build PASS 71.373s;
55 selected Host tests PASS 72.563s; full format/tidy PASS 1171.540s. Tidy retains
existing nonfatal warnings; this is not a strict warning audit. Its read-only
analysis overlapped runtime after all implementation/fixture sources were frozen,
and final qualification waited for actual completion. KTEST 67 --fast --no-setup
PASS 507.005s: 90,570 assertions/0 failures,1,411 unique executed cases, all 160
mandatory, natural selftest poweroff/launcher0/no forced stop. The exact prior
1,408-case set plus three new cases passed; the same three disabled BufferCache
registrations stay outside executed/mandatory counts. All 190 runtime source/
artifact pins (188 distinct paths) are stable.

Four exact UID 1000 disposable images were proven inactive and pruned, releasing
262,225,920 allocated bytes. Final disposable allocation is zero. Complete
before/live/after inventories preserve unrelated VM 13532 UID 945 and its exact
arguments. Required seeds/current artifacts, small logs/receipts, Normal/KTEST
isolation, one topology and 20GiB disposable/40GiB free-space bounds remain.

Retained failures include the original canonical include-order preservation
FAIL01, successful strict preservation02 followed by formatter03 refusal, and
amended preservation03 accepting only the formatter's exact existing MM include
permutation. No suppression was added. Initial layout-parser, stale Host fixture
and Host-review whitespace-parser failures also remain with exact diagnoses;
actual code, compiler flags and old assertions were not weakened to satisfy them.

Aggregate checkpoint-38-audit.json PASS SHA256
a4766f572fb5573f4b1680c3b37088859cfc2535000137ac3738c20688c9e714.
Before/after layout receipts are before-layout38-02.json/after-layout38-01.json;
source snapshot SHA256 4c0f426274191645762049ce5586ac81551d829dc22309c0b23cfdb9015c5b33.
The current Normal kernel is bfbd54385f609991dd947e9e971b3df614c5e7fa7fdf846b6a82e62816080125;
the diagnostic kernel is c37aef52f90cb3f0df12d55ede20536cdccacd0d3124d6841f947f0099397625.
Earlier checkpoint artifacts are historical receipts, not these rebuilt images.

Goal39 remains ACTIVE; all ten full criteria remain incomplete. KTEST APs are
parked; these private scalar/layout cases do not qualify simultaneous SMP memory
ordering, coherent setters/getters, placement inheritance or CPU ceilings.
Checkpoint38 deliberately preserves SET_DOMAIN's hard-first move failure, unlocked
policy tuples, target-capacity panic/partial-fairness rollback and wake-owner gaps.
Next is checked owner-locked admission/movement with real wake-election exclusion,
complete claim finishing and PROCESS image serialization. Ordinary wake capacity,
ancestry/worker-exec/WKI policy, hostile/shared/futex/init regression, two
simultaneous two-hour trees and 1,000 full lifecycle/allocation-failure/reference
baselines still require qualification. Continue exact-owned --no-setup tests and
pruning after PASS/FAIL/exception. No goal pause or push.

Independent checkpoint38-evidence-integrity02.json PASS (all 15 sections), SHA256
72d5d47768df6f9c569cb44c54b0c5272aed8d97f41aa83a33d63438e53f63fb.
Its original FAIL01 and initial checker remain intact: the generic command gate
expected seven Python fixtures in the tidy receipt, which correctly pins only
24 C++ sources. Revision02 binds those exact 24 rows; the separate Host execution,
source freeze and runtime pins cover all seven Python fixtures. No production or
execution evidence was changed for this correction.


## Checkpoint52: queued futex frame ownership (2026-10-14)

A queued futex waiter now owns one checked allocator-frame reference after its
scoped mapping pin ends. Final table/Task-slot retirement releases that reference,
including wake, timeout, cancellation and unpublished failure paths. The owner
keeps no Task, pagemap or usercopy lease across sleep. Existing physical-word keys,
timeout/error ordering and the two-owner election are preserved; checked extra-ref
admission can additionally return ENOMEM. Private/COW/shared-zero key policy and
canonical mutable file backing remain open.

The new registered ASan/UBSan Host regression compiles the complete real futex
translation unit and hash table, actual frame/stable-pin methods and checked ref
increment against explicit Host primitives. All 22 vectors, 4,176 assertions and
128 wake/cancellation races pass. The complete archived old futex unit genuinely
compiles, then fails the same pending-frame oracle with two premature-reuse
failures. Both runs retire their owned processes and remove private scratch.
The existing 42 caller-order vectors pass unchanged.

Final Normal Build WOS, isolated `bin/wos-ktest --fast --build-only` and formatting
pass. Ten related registered Host checks pass. The wider Host/KTEST manifest
checks fail on existing standalone KCOV and eight indirect Credentials wrappers;
the failing files/checkers are byte-identical to HEAD. Their failed receipts are
retained, and the wider manifest checks are not reported as passing. The raw VM
access test now enumerates the earlier private lazy-projection fixture exactly,
with guard, bounds, translation validity and cleanup checks preserved.

Both new native futex fixtures pass in KTEST78, using actual page roots/frames
and both waiter-owner retirement orders. They retain a private oracle reference
through teardown and clean up both fixture owners on failures. Final frame-count
observations happen while the oracle still owns the frame; the last decrement
itself checks zero. Native physical zero-free under the bucket IRQ lock remains
source-reviewed, with the analogous Host IRQ primitive exercised. Native77
remains evidence of its earlier exact source/artifact cohort.

The actual isolated `bin/wos-ktest --fast --no-build --no-package --no-setup`
run passes all 1,446 enabled cases, all 195 mandatory cases and 93,753 assertions
with zero failures; three disabled BufferCache cases remain excluded. It powers
off naturally, with launcher/worker/outer exit0 and no forced stop. VM runtime
is 466.239s; the outer command completes in 497.102s. All eight packaging/readback
commands succeed and all five prepared and five stopped payload readbacks match.
All 844 admitted inputs remain unchanged; this is a selected source domain,
not a complete compiler dependency claim or simultaneous SMP qualification.

The independent observer records 1,665 identity observations with a maximum
receive gap of 330.899ms. All 3,267 storage samples satisfy the 20GiB disposable
budget and 40GiB free floor; peak allocation is 394,547,200 bytes. All four owned
images are proven inactive and pruned (391,987,200 allocated bytes), leaving zero
disposable allocation. A separate physical postcheck confirms the four run PIDs,
sockets and images are absent, the original shared lock is released and the
unrelated UID945 VM is unchanged. Small logs/receipts and required readonly
seeds/current artifacts remain.

Runner source-name findings and the initial admission failure are retained:
the genuine build receipt omitted required command exit fields and used a
launcher spelling the preserved gate rejected. Corrected metadata derives from
actual saved results and a fresh watched relative-spelling build. The gate and
production sources were unchanged; the subsequent full gate and native run pass.

Independent production/native source review03 is CLEAR (10 checks/35 subjects),
SHA256 `97fada9029f5281b40952746f71462510b65320d2de165c19014d1b303cf0f2d`.
Independent saved Host audit01 is CLEAR (9 checks), SHA256
`6c3139ef4ba52dc44985b820bf0764b7c179d644cbb88d02205460ff504f0f90`.
Independent Native78 terminal review01 is CLEAR (12 checks), SHA256
`fe10c25d9fe7ae5e4089eec5619d249e7cbe4f5f2a130bc97ae5a253d6630c4c`.
Root rehashes all 889 combined current inputs and retained terminal subjects.
Earlier fixture failures/findings are kept.

Goal39 remains ACTIVE with all ten full acceptance criteria open. Next is
connected private futex address-space identity and sharing provenance, including
actual root creation, retirement and all six fork rollback paths. Its source-only
design is not implementation or runtime evidence. Canonical shared-file backing,
complete owner drains, two-hour services and 1,000 lifecycle/failure cycles remain
unqualified. Continue exact-owned `--no-setup` qualification with the shared launch
guard, isolated sysroot, one topology and all-outcome pruning. No pause or push.


## Checkpoint53: connected private futex identity (2026-10-14)

Private futex words now use `(root incarnation, virtual word)` keys. Different
roots or virtual aliases cannot communicate merely because they share a zero,
COW or cached physical frame. A same-root private frame replacement keeps the
key, so a wake can find a waiter that still retains its older frame. Leaves with
`PAGE_SHARED` use the retained physical word instead; intentional common-frame
sharing continues across roots and virtual aliases. Full equality compares the
key kind, owner and word; hashes select buckets only. The physical wake helper
can address only shared-frame keys.

Every public user-root factory admits an exact root association before returning.
A globally monotonic nonzero serial saturates instead of wrapping. Controls grow
in 96-slot physical pages with no fixed root-count cap. Existing mapping IRQ
locking protects associations, slots and counters; allocation, logging and page
freeing stay outside that lock. Final release preserves the existing exclusive
root gate and validates the sole allocator owner before detaching its association.
Empty metadata pages are freed. All six fork rollback frees use the matching public
release after mapping cleanup. Kernel and intermediate tables remain unregistered;
unknown futex provenance refuses capture rather than falling back to a physical
private key. The low-level release API still requires its real last owner; a
copied serial is not permission to free a recycled raw pointer.

Capture and revalidation use the original live page pin and actual leaf flags,
frame and word bounds. Queued waiters retain only their existing allocator-frame
owner and scalar key, with no Task, root-control or root-reader lease across sleep.
Task/File/pin layouts, syscall and wire ABI, count/error/timeout ordering and the
existing two-owner wake/cancellation election are preserved. `USER_ROOT_CONTROL`
appends one physical accounting owner without changing earlier owner values.

Normal Build WOS and isolated `bin/wos-ktest --fast --build-only` pass, as do
formatting and all 11 focused registered Host checks. The new ASan/UBSan actual-body
regression passes 27 vectors; the complete archived physical-key futex unit
compiles and fails the private-alias and replacement-wake oracles. Existing 42
caller-order and 22 frame-lifetime vectors remain intact and pass. The two stale
Host adapters found during validation are corrected without weakening count,
claim, lock, zeroing, pool or null-failure checks; initial failures are retained.

Actual `bin/wos-ktest --fast --no-build --no-package --no-setup` qualification
passes 1,455 unique RUN/PASS pairs, all 204 mandatory cases, three disabled skips
and 101,101 assertions with zero failures. The seven actual RootIdentity cases
qualify admission/retirement, recycled root addresses, 4,097 live roots, exact PTE
provenance and metadata/physical baselines. Two additional Futex cases use real
local hash tables, pins and frame owners to check private and shared key matching.
The VM powers off naturally; launcher, worker and outer command exit 0 with no
forced stop. All eight packaging/readback commands and all five prepared plus five
stopped payload readbacks match. The VM runs 466.179s; the outer command takes
496.956s. All 831 admitted inputs remain held through the terminal snapshot.

The observer records 1,664 identity samples with a maximum 365.117ms receive gap.
All 3,267 storage samples obey the 20GiB disposable budget and 40GiB free floor;
peak allocation is 394,743,808 bytes. Four exact-owned inactive images are pruned,
releasing 392,183,808 allocated bytes. Terminal disposable allocation is zero.
A separate physical check confirms all four run PIDs, sockets and images are gone,
the original shared lock is released and the unrelated UID945 VM is preserved.
Required readonly seeds/current artifacts and small logs/receipts remain.
Independent terminal review02 passes ten checks; its 876 retained subjects are
rehashed twice by the reviewer and again by Root before documentation changes.
The Native79 source/artifact cohort is preserved in its postrun closure; Native78
remains historical evidence of the earlier frame-lifetime patch.

The native fixtures use parked KTEST APs and local tables; they do not qualify
full public futex syscall concurrency or SMP wake/teardown. Their 1,000 empty-root
transactions are not the goal's 1,000 create/exec/kill/destroy/OOM cycles. Canonical
mutable shared-file object/offset identity and coherence, full native owner drains,
simultaneous two-hour services and every full acceptance criterion remain open.
Goal39 stays ACTIVE. Continue exact-owned `--no-setup` tests and pruning after
passing, failing and exceptional outcomes; no full VM disks are retained.

## Checkpoint54: shared-file registry growth (2026-10-14)

Splitting a heap-backed file-mapping row could append a right fragment, relocate
the registry storage, and then read/write the freed left row. The production
path now saves the left length before insertion and reacquires the left row by
index after insertion succeeds, under the existing registry mutex. Failed
allocation preserves the original row and File reference balance. File/root
identity, writeback policy, byte offsets, lock order and ABI remain unchanged.

The registered ASan/UBSan Host regression compiles complete actual registry
functions and the unchanged SmallVec implementation. It passes 576 cases and
18,108,589 assertions, including growth at 33/64/128 rows, all cut geometries,
seven injected allocation failures, retry, clone and reference/close ordering.
Independent byte coverage and offset oracles preserve the original mapping
projection. The complete archived predecessor compiles and fails with an ASan
heap-use-after-free in the split and SmallVec growth paths. Earlier compiler
selection and fixture name-resolution failures are preserved; the fixture-only
correction qualifies its `puts` counter. The Host File/Mutex/PageTable primitives
observe reference/final-close ordering and retained row policy, not real backend
destruction, protection authorization, PTEs or canonical shared-file data.

Normal Build WOS, isolated `bin/wos-ktest --fast --build-only`, formatting and all
eight selected registered Host checks pass with their watched sources unchanged.
The new native case uses actual registry storage, allocator growth, an owned
pagemap and retained File. It checks three real heap relocations, every surviving
byte projection, neighboring rows, access policy and final close outside both
locks; it preserves prior empty registry capacity and refuses a foreign nonempty
registry. It is compiled and enabled, with VM execution still pending. No VM was
launched and no disposable VM images were created for this checkpoint. Native79
remains historical evidence of its exact earlier source/artifact cohort.

Independent production/native review passes ten source checks; independent Host
review passes eleven checks over 53 held current and saved subjects. Source-only
TMPFS/XFS maps establish the next integration work: canonical frame ownership,
quota and inode retirement, actual changed-write prefixes, EOF mutation ordering,
all-root resize fencing and backend dirty/retry ownership. Those maps are not
runtime evidence or an installed backing implementation. Existing production
final close under the publication guard remains a broader owner-drain issue.

Goal39 remains ACTIVE. No full acceptance criterion closes: mutable shared-file
coherence, complete native reference drains, simultaneous two-hour services and
1,000 full create/exec/kill/destroy/OOM cycles remain required. Continue using
`--no-setup`, one topology, the 20GiB disposable budget/40GiB free floor and
exact-owned inactive-image pruning after every outcome.


## Checkpoint55: TMPFS page ownership (2026-10-14)

Resident descriptors now drop exactly their inode-owned physical reference;
remaining frame owners survive truncation and final inode retirement. Reclaim
refuses externally referenced pages before swap-slot allocation or I/O and drops
only the inode reference after successful pageout. Hole descriptors do not
uncharge retained pages. Both reclaim scans and swap migration skip hardlink
aliases instead of following their borrowed target during exclusive teardown.
Final inode cleanup unregisters/releases owned state under io_lock, ends its
guard, then deletes the inode. Ordinary canonical File I/O, quota/swap rollback,
lock order, object layouts and syscall/wire ABI remain unchanged. A single-node
selftest-only hook delegates exact owned-page eviction under the real inode lock.

The registered actual-body ASan/UBSan Host regression passes 20 independent
vectors and 53,661 assertions, covering quota/frame/slot accounting, swap failures,
retry, alias scans and mutex destruction. The complete archived predecessor
compiles, then fails the same oracles: 17 processes report the shared old guard
lifetime defect and three protected-alias cases report ASan faults. These are not
17 distinct production defects. All owned scratch/process sessions are retired.
Host physical/swap/mutex primitives do not qualify hardware PTEs or SMP behavior.

Normal Build WOS passes (initial30.629s, final1.179s); isolated fast diagnostics
pass (initial29.883s, final watched727 rebuild3.920s). Formatting and all11 selected
registered Host checks pass. Initial CTest10/11 exposed a stale swap-dispatch
source assertion; its helper-aware correction preserves syscall/ABI checks and
Host admission before usercopy. No production dispatch or errno change was made.

Actual KTEST80 fails two fixture assertions because KEXPECT_EQ compares raw-width
representations: correct ssize_t negative returns differed from int expected
operands. It is stopped promptly, with no full census, summary or natural poweroff
claim. Six packaging/readback commands and five prepared payloads succeed; no
stopped readbacks are claimed. Its four images (327,237,632 allocated bytes)
are pruned and physical postcheck passes. The exact old fixture and failed run
remain immutable. The successor changes only two expected operands to ssize_t;
every actual call, errno, assertion, failure injection and cleanup is preserved.

Corrected isolated `bin/wos-ktest --fast --no-build --no-package --no-setup`
KTEST81 passes all1,460 enabled cases, all209 mandatory cases and 101,314
assertions with zero failures; the same three BufferCache cases stay disabled.
The checkpoint54 registry-growth case and all four real TMPFS ownership cases
pass. The VM powers off naturally, with launcher/worker/outer exit0 and no forced
stop. VM time is 471.277s; outer command time is 502.790s. All eight commands
and five prepared plus five stopped payload readbacks pass. All893 admitted
inputs remain held; this is an explicit source domain, not a complete compiler
dependency or SMP qualification. Native frame, quota, swap and final File/inode
retirement are exercised with parked KTEST APs and owned private fixtures.

All four exact-owned inactive disks are pruned (392,445,952 allocated bytes),
leaving zero disposable allocation. The separate physical postcheck confirms
run PIDs/sockets/images absent, original shared lock39:197 released and protected
UID945 VM13532 unchanged. Small logs/receipts and required readonly seeds/current
artifacts remain. Keep20GiB disposable budget,40GiB free floor and all-outcome
pruning. No full VM disks are retained. The provisional review/preflight refusal
is preserved; fresh admission uses final frozen reviews. The auxiliary offline
producer's inherited80 execution label is documented, while operative81 role01
qualifiers/data are correct; later role02 outputs are retained history only.

The independent source, Host, build/artifact and terminal reviews pass their
stated checks. No full Goal39 criterion closes. There is no new native mapping
loan API, canonical shared-file layer, all-root EOF fence, dirty/retry owner or
complete family/File/Task shutdown drain. TMPFS resize failure atomicity and the
connected native-owner installation have source-only next-step maps. Those maps
do not qualify implementation. Two simultaneous two-hour services and1,000 full
create/exec/kill/destroy/OOM cycles remain required. Goal39 stays ACTIVE.

## Checkpoint56: native mapping ownership and TMPFS resize preparation (2026-10-15)

A typed mapping family owns one checked File reference; lazy and registry rows,
root bindings, pins and installers hold explicit owners. Pre-reserved callbacks
run on the real scheduler's dedicated retirement worker, outside MM, owner and
queue guards. Root retirement waits for actual PTE removal and TLB acknowledgment.
Task deletion waits for the destructive File callback to return, including the
ProcFS self-target case. Transferred ownership can recover through the real GC
worker after a pin initially refuses retirement. Shutdown closes admission and
preserves providers while native records or callbacks remain live.

This internal producer admits initially empty, exclusively owned unpublished
roots and at most eight 64-page bindings. It preallocates upper tables before
publication and uses an inline census for at most 512 leaves and 49 table frames.
The native retirement gate is held with preemption disabled until it is released;
the admitted walk must finish within 2,048 steps. This prevents the source-found
same-CPU dependency on an IRQ-off RAW caller waiting for a preempted gate owner.
It supplies no wall-clock latency bound for global scans or remote TLB progress.
Existing public RAW mappings remain compatible; native publication, fork/exec,
canonical shared bytes, dirty retry and all-root EOF fencing remain unfinished.

TMPFS resize prepares fallible replacement descriptors before materializing or
zeroing a retained tail. Preparation failure preserves size, bytes, descriptors,
quota and frame/swap ownership. Successful commit copies the current descriptors
after page-in. All 29 actual-body ASan/UBSan Host vectors pass, with 66,507
assertions; the genuine archived predecessor compiles and fails the resize oracle.
All 11 selected registered Host checks pass. Normal Build WOS and isolated fast
diagnostic builds pass with their watched sources held. The explicit capture
contains 686 kernel sources and inherited build/qualification inputs, 1,155 in
all; it is not a compiler dependency census.

Native83 completed six scheduled cases and 15 callback returns but remains FAIL:
the collector rejected an incomplete ordinary UART record after the finalizer.
The separately reviewed successor accepts only a bounded ordinary trailing
fragment after every required complete protocol record; the fragment supplies
no protocol credit. Fresh Native84 passes all six cases and 2,930 assertions on
32 online CPUs, with 15 callback returns, ordinary finalizer action2, natural
launcher exit0 and no forced stop. Its 129/129 Task census is context only.
This short scheduled test does not qualify the ordinary init/service workload.

Default Native82 stopped on the old Task-size expectation after the retirement
pointer was appended. The corrected fixture checks size16216 and the new pointer
at offset16208, width8 and initial null, preserving all previous scalar offsets,
alignment and cases. Fresh default Native85 passes all 1,463 enabled cases,
all 212 mandatory cases and 113,976 assertions with zero failures; the same three
BufferCache cases remain disabled. It powers off naturally after 476.218s, with
launcher, worker and outer exit0 and no forced stop. The three new real TMPFS
resize cases and the corrected layout case pass. This default suite runs with
parked APs; the separate scheduled84 run supplies the stated SMP evidence.

Native82,83,84 and85 each prune all four exact-owned inactive images. Their saved
prune receipts leave zero disposable allocation; separate real-host checks for82
and84/85 preserve the unrelated VM and original shared lock. The initial83 extra
blanket FD check failed on an inaccessible desktop process; later observers
retain the canonical scoped prune evidence and explicitly limit that FD claim.
All physical observations use the real host process view. Earlier sandbox-only
absence inference is superseded by the real-host protected-VM observation.
Small logs/receipts and required readonly seeds/current artifacts remain.

Builds and native results cover the combined local workspace. All 22 pre-existing
user edits are preserved; the scheduler's goal-only patch is separated by exact
forward/inverse reconstruction. Its clean-HEAD candidate is not independently
built or runtime-qualified. Goal39 remains ACTIVE with all ten full criteria open.
Continue `--no-setup`, one topology, a 20GiB disposable budget, a 40GiB free floor
and exact-owned pruning after every outcome. Next is connected canonical TMPFS
backing, followed by complete public ownership and the required service/lifecycle
qualification.

## Checkpoint57: canonical readonly TMPFS backing (2026-10-15)

The bounded unpublished producer now has an explicit readonly shared TMPFS mode.
Preparation retains the mounted inode, a fixed-EOF lease and canonical frame
anchors before taking root or installer guards. Lazy faults borrow an anchored
frame without backend I/O or allocation; successful installation transfers one
reference to a readonly, shared, non-executable PTE. The existing private-copy
producer is unchanged. File, lazy-range and Task layouts and syscall/wire formats
are unchanged.

Ordinary writes within the captured EOF remain visible through both roots and
hardlink aliases. A live backing refuses size changes, content replacement and
reverse executable admission before mutation. Its checked mount busy owner
survives unlink and final File close; ordinary unmount returns EBUSY. A scoped
mount fence prevents pivot or backend rebinding from invalidating its ancestry.
Real family retirement keeps the backing through the one terminal File put,
then releases frames, EOF/inode owners and finally the mount pin before reporting
callback return. Negative close is terminal and is never retried.

This producer retains the existing eight-bindings-by-64-pages limit and admits
only initially empty, exclusively owned unpublished roots. Construction checks
the dense TMPFS descriptor bound before prefaulting. Failed preparation may warm
cache or materialize a sparse prefix, but releases every newly acquired owner.
Generic VM operations refuse unsupported native operations before mutation.
Public mapping publication, writable sharing, fork inheritance, truncate
invalidation and complete exec/exit ownership remain separate work.

Normal Build WOS, the isolated fast diagnostic build, repository formatting and
all 11 selected registered Host checks pass. The actual-body TMPFS Host harness
passes 36 vectors and 67,001 assertions. Five genuine archived predecessor
variants compile and fail the corresponding regression checks, with scratch
owners cleaned up. Independent source reviews cover the backing, mounted owner,
consumer, installer and retirement ordering.

Fresh scheduled Native86 passes seven cases, 3,429 assertions and 20 callback
returns on 32 CPUs. The added case uses actual controller Tasks, two roots,
distinct File descriptions, a hardlink and an equal-byte distinct inode. It
checks canonical frames, cross-page VFS write visibility, readonly permissions,
fixed-EOF refusal, quota-failure rollback and mount lifetime after unlink and
final File close. Its direct frame/key observations do not qualify a blocking
userspace futex exchange. The exact paused family-callback interval is not
instrumented; the direct backing lifetime and callback ordering are separately
qualified. The independent terminal audit confirms the protocol, payloads,
natural exit and unchanged admitted inputs.

Native86 shuts down naturally and prunes all four disposable images, releasing
263,077,888 allocated bytes. Its real-host postcheck reports zero disposable
images and preserves the unrelated VM and original shared launch guard. Ordinary
init/XFS errors after the scheduled finalizer remain recorded context; this run
does not qualify a healthy ordinary rootfs boot or service workload.

Fresh default Native87 passes all 1,463 enabled cases, all 212 mandatory cases
and 113,976 assertions with zero failures, using the same freshly built
checkpoint57 kernel. The three BufferCache cases remain disabled. It powers off
naturally after 475.396 seconds, with launcher, worker and outer exit0 and no
forced stop. All four disposable images are pruned, and its real-host postcheck
reports zero disposable allocation with the unrelated VM and shared guard intact.
This default suite parks secondary CPUs; Native86 supplies the stated scheduled
32-CPU evidence. Neither run supplies the required two-hour service evidence.

These results cover the combined local workspace; all 22 pre-existing user edits
remain byte-identical. Goal39 remains ACTIVE with all ten full criteria open.
Continue exact-owned `--no-setup` qualification, one topology, the 20GiB disposable
budget, the 40GiB free floor and pruning after successful and failed runs. Public
canonical mapping ownership, writable backing, two-hour simultaneous services
and 1,000 full lifecycle/allocation-failure cycles remain required.

## Checkpoint58: public demand read-only TMPFS mappings (2026-10-15)

Public shared READ/NONE mappings of regular TMPFS Files now retain a canonical
inode, fixed EOF and mount owner, with demand page loans rather than a copied
file cache. Current admission requires `O_NO_CACHE` and the strict retained
TMPFS mount graph used by the earlier backing. Unsupported cached or attached
File state is refused before publication. This does not yet provide ordinary
cached-File or writable shared mapping support. The public backing has no
64-page construction cap; descriptor growth, quotas and memory remain fallible.

Preparation stages mirrored lazy rows outside publication and root guards.
Hardware and software faults retain root-independent snapshots through backend
I/O, then revalidate the exact binding, page-root incarnation and affine offset
before installing a read-only shared leaf. No backend I/O or allocation occurs
under the installer or root-reader guards. Whole READ/NONE protection and unmap
use bounded PTE/TLB batches, with actual acknowledgement before frame release.
Partial managed cuts, occupied fixed replacement and remap return unsupported
errors before effects; WRITE and EXEC cannot exceed the retained read ceiling.
Resize remains blocked by the fixed-EOF backing.

Fork prepares fresh child bindings and checked frame references. Shared-root
clones and threads prepare their lazy metadata before publication. Exec and
failed creation release typed owners outside publication. Syscall preparation
restores schedulable context for fallible work, while legacy task construction
keeps its prior exclusive-MM assumptions. File, Task, 112-byte lazy-row and
syscall/wire layouts remain unchanged; no Task backedge or new reaper is added.

Normal Build WOS and the isolated fast diagnostic build pass, as do formatting
and all 16 selected registered Host checks. Actual-body Host coverage includes
144 staging/fork/context cases, 17 demand-MM cases and 46 TMPFS ownership cases.
Fresh default Native89 passes all 1,463 enabled cases, all 212 mandatory cases
and 113,976 assertions with zero failures. It powers off naturally, with all
five prepared and five stopped payloads matching. The earlier Native88 failure
is retained: its test incorrectly required a 33-entry vector with spare
capacity to relocate. The test-only correction preserves the forced-growth
64/128-entry cases and every byte, reference, policy and cleanup check.

Native89's four disposable images are pruned. The real-host postcheck confirms
zero disposable allocation, absent owned processes/sockets/images, the original
shared launch guard and the unchanged unrelated VM. Its independent terminal
audit verifies all 1,258 admitted source/artifact inputs. These checks cover the
combined workspace; the 22 pre-existing user edits are preserved, including the
separate goal-only reconstruction for the mixed thread-control file.

The separate public retry02 passes on the normal eight-CPU kernel, using a
129-page sparse backing. Actual thread and fork inheritance, different-address
canonical mappings, cold software and hardware faults, in-EOF VFS write
visibility, immutable read permissions, whole READ/NONE protection and unmap,
unsupported-operation refusals and FD close/reuse are checked. A real read-only
futex WAIT0/WAKE1 exchange completes between roots. Forked children exec and
exit, consuming waits succeed, and the original init records the probe's exit.
Successful growth after final unmap witnesses eventual EOF-lease release; it
does not prove executor callback return or global reference balance.

The VM interval lasts 9.861 seconds; the outer command completes in 49.083
seconds. All nine bounded reports (316,480 bytes), 80 protocol events and all
13 prepared and 13 stopped artifact readbacks validate. The observer stops only
the owned VM through QMP after the completion witness; natural poweroff is not
claimed. Four disposable images totaling 116,146,176 allocated bytes are pruned.
The strict real-host postcheck passes with zero images/allocation, absent owned
processes/sockets and the protected VM and original guard unchanged. The
independent saved guest-terminal audit verifies all 80 raw events, all 1,931
admitted input hashes, ten successful commands and both artifact readback sets.

The earlier attempt remains failed evidence: it stops before VM launch when
libguestfs refreshes appliance-cache timestamps. All 1,916 input content and
alias hashes remain unchanged. Its one boot copy and an inactive guestfs socket
are cleaned up; the unchanged strict postcheck then passes. The retry permits
timestamp refreshes only for the three exact current appliance-cache files,
preserving their other identity/ownership metadata, all other source metadata
and complete content checks before and after execution.

Goal39 remains ACTIVE with all ten full criteria open. Ordinary cached-File and
writable shared mappings, full owner drains, two-hour simultaneous services and
1,000 lifecycle/allocation-failure cycles remain required. Continue `--no-setup`,
one topology, the 20GiB disposable budget, 40GiB free floor and exact-owned
pruning after every outcome.
