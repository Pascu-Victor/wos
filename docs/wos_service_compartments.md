# WOS service compartment design

Goal39 is in progress. Profile preparation and private native SysV IPC namespace
ownership/lookup are implemented. No public compartment creation syscall,
launcher, independent mount/PID view or network/WKI gate is enabled. Independent
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

The current mount table, mount/pivot generations and mount lookup caches are
global. Retained native root/cwd views already enforce object identity and
read-only/nosuid restrictions, but do not create independent mount registries.
Private views need explicit namespace ownership across publication, lookup,
mount crossings, caches, readdir, procfs, detach and pivot. Shared backend
lifetime must remain distinct from visible mount attachment lifetime.

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
