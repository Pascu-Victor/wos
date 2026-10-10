# WOS service compartment design

Goal39 is in progress. Profile preparation, private native SysV IPC namespace
ownership/lookup, private native mount-table ownership, Task native lookup and
subject mount administration are implemented. No public compartment creation
syscall or launcher is enabled. PID, lifecycle and complete network/WKI
integration remain incomplete. Independent
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
fields. It must enforce network denial in native consumers even when an
inherited credential has network administration authority. Explicit inherited
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
Remaining root/compatibility helpers and procfs PID/link consumers still require
integration before public creation can be enabled. Retained File listings and
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

WKI placement, IPC forwarding and VFS export must reject restricted namespace
contexts before remote effects until a negotiated enforceable compartment
contract exists. Pinning automatic placement local is insufficient for explicit
remote operations. Captured credentials/root/resource grants cannot silently
substitute the worker's host namespace. Global host administration also needs
explicit compartment gates and a reviewed allowed-authority policy.

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

`shm_retire_namespace` closes private admission under the SysV registry lock
and marks only its exact segments for destruction. New GET/ATTACH/CTL return
EIDRM. Attached backing stays until the final attachment drains; retirement
does not unmap intentionally inherited memory. Host retirement is ignored.
This hook still needs the future compartment-init teardown coordinator.

Physical segment allocation/release retains the existing SysV lock/context
behavior. The new namespace's final release never performs heavy cleanup under
that lock. Namespace-init policy, namespace-local stat PIDs, aggregate IPC
budgets and real user mapping/futex tests remain integration work.

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

## Verification checkpoint

The initial `compartment_policy_test` suite checks actual production preparation
and immutable credential APIs, including all ID/group slots, root/nonroot
inheritance, sealed grants, resource bounds, explicit network permission, failed
output preservation and set-ID exec suppression. Deterministic four-generation
profile chains exercise 2,048 independent inputs. Host ASan/UBSan checks and a
compile probe using the current Normal Build WOS freestanding flags are policy
evidence only. Native namespace isolation, actual denied network operations,
Task/File/mount/IPC reference drains and live service behavior require later
integration and runtime qualification.

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
