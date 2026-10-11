# WOS service compartment design

Goal39 is in progress. Profile preparation, private native SysV IPC namespace
ownership/lookup, private native mount-table ownership, Task native lookup and
subject mount administration are implemented. No public compartment creation
syscall or launcher is enabled. Exact-owner ambient network admission is now
implemented; complete PID, lifecycle and WKI integration remain incomplete. Independent
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
Child-owned exec scratch holds main parser tables and lazy staging before reads;
unpublished teardown retires this scratch before anchors, Files and user memory.
The native builder obtains a stable executable lease, checks fresh size/execute
permission, and attaches buffered bytes directly to the owned child before I/O.
Its short lease-cloning guard covers temporary backend owners as well.

Small dynamic and static images use a complete source bounded at 8MiB. Larger
dynamic images keep bounded metadata and lazy loading. A static fallback above
8MiB returns E2BIG; name/pagemap/scratch/buffer allocation refusal is recoverable.
The existing Host builder, PROCESS constructor, interpreter behavior and Task
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

Public creation remains disabled. Live creator capture/revalidation, bounded
root population/import/non-root ownership, explicit FD/shared mapping manifests,
init/job/parent and atomic publication, and a real argc/argv/envp/auxv initial
stack remain required. Main metadata recovery and recoverable native skeleton
allocation are implemented; the existing interpreter reader still has local
File/buffer ownership across reads that requires tracked preparation before
public use. Other existing construction/image allocation paths are not covered
by the five new fault points. SYS_DEBUG and global service-resource reads also
need explicit private-view policy alongside the remaining global/device/swap/
perf/WKI census. The current129 credential source calls still have24 failures.
No scheduled service, shared-futex, two-hour or1,000 full lifecycle claim follows
from these unpublished native cases; all ten Goal39 criteria remain incomplete.

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
