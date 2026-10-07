# Owner-authoritative remote VFS consistency

Goal26 design and qualification record. This work is in progress. Current source
does not yet advertise owner-version, atomic remote append or distributed
advisory-lock or committed-I/O capabilities. Host models, compiled KTEST cases
and native/live qualification will be reported separately as they actually
complete.

## Consistency contract

The filesystem owner is the authority for native object identity, committed
contents and metadata, directory membership, append offset and advisory locks.
Paths locate objects; retained native inodes identify open descriptions after
rename, unlink and hardlink aliasing. Owner boot, export incarnation, binding
nonce and ordinary open token qualify every remote operation. Stale numeric
FD/channel reuse cannot select a successor.

An owner snapshot carries a nonreused object identity and monotonic content,
metadata and directory publication versions. A cache hit requires an exact
authenticated owner validation covering the relevant domain and exact open
lifetime. Time and best-effort invalidations are hints, not freshness proof.
Initial rollout uses validation RPCs rather than timed leases. A read concurrent
with mutation may linearize before or after that mutation; one begun after a
confirmed owner commit must not return an older validated cache version.

Reads and stat on an open description refer to its retained object even when its
old pathname names something else. Metadata/path lookups remain subject to
captured caller credentials and retained namespace confinement. Directory
iteration is versioned and may restart or return a defined retry if membership
changes; it is not a cross-operation filesystem snapshot. Rename/unlink/xattr/
truncate and writes publish relevant versions after the backend commit point
while its serialization still protects the observed state.

All new client writes are owner-committed before successful return, including
fresh legacy message fallback; client write-behind is not authority to claim a
remote commit. Existing legacy dirty ranges are flushed before switching to
coherent operations and before read, append, fsync or close barriers. A failed
or fenced flush reports its retained error; an accepted/uncertain mutation is
never silently retried. Fsync preserves native durability semantics, without
implying power-loss qualification from software models.

Atomic append selects the current native EOF and writes under the same owner
inode sleeping lock used by local append. The reply includes exact selected
offset and committed byte count. Partial writes advance only by committed bytes.
A repeated operation identity returns its retained outcome rather than selecting
another EOF. Positioned writes remain positioned and do not inherit append
semantics accidentally.

## Advisory ownership and waits

All owner-local and remote requests for a canonical inode use one advisory
arbiter. POSIX and OFD record locks conflict within the record family; flock
uses its existing independent family. Remote process ownership includes the
authenticated peer/boot/process scope; OFD and flock follow the exact open
description and its dup/fork references. Local owners occupy a distinct domain
and cannot alias a remote numeric PID.

POSIX close semantics release that process's locks on the inode even if another
descriptor keeps the description alive. Final description close releases OFD and
flock ownership. Process exit, dead-peer/session cleanup, unmount and binding
retirement close admission and release all applicable locks/waiters once.

The owner network/hashed FIFO worker never sleeps waiting for a lock held by
another client. It retains a bounded waiter and returns a pending status. Only
the client syscall task sleeps or performs bounded status/cancel RPCs. Grant and
cancellation share one owner terminal transition: whichever commits first
determines the result, and repeated status/cancel cannot grant twice or release
a later unrelated operation. A signal that races after a committed grant may
observe success; one that wins cancellation observes interruption. Fence/unmount
wakes the client and prevents successor-generation grants.

## Compatibility and rollout

Keep WKI v3's 32-byte header, 172-byte HELLO and 40-byte authentication trailer,
the packed existing VFS operations, and the 32-byte kernel flock ABI.
Independent version/append/lock feature negotiation uses the existing
authenticated u64 VFS feature query. Every new request/reply validates version,
reserved bits, exact length and full owner/open lifetime before publication or
mutation.

Old or mixed peers use fresh authenticated ordinary RPC when that operation
supports it. Atomic append and distributed lock operations return explicit
`EOPNOTSUPP` if their semantics are unavailable; seek-write or client-local
arbitration cannot substitute. Unknown formats fail closed. Secure RDMA retains
its existing independently gated authority and no-effect-only fallback; this
goal does not enable its default mask.

## Current evidence

Reconnaissance and initial patch contract are recorded locally under
`goals/progress/26/`. Twenty-five production publication/cache/append-receipt
host cases and ten owner-version/append/positioned-I/O wire cases passed under
ASan/UBSan, alongside 43 existing wire cases. Four additional production
advisory-receipt cases passed, including 128 concurrent grant/cancel/retire
races. Normal Build WOS and fast diagnostic KTEST compilation passed. Native
tmpfs content/metadata/namespace and XFS committed-inode/data hooks now maintain
private owner revisions; exact-open authenticated version requests use deferred
owner work and strict reply framing. The version feature remains unadvertised.
Isolated KTEST runs passed 73,270, 73,280, 73,282 and 73,284 assertions
respectively, with zero failures. The second includes client cache-authority
policy and an actual owner append fixture sharing native EOF with a local open,
strict replay, stale-token rejection and retained-handle close behavior. The
third run includes XFS rollback publication guards, exact native positioned I/O,
partial/error outcomes and registry retirement during retained I/O. No multi-
client or packet-level qualification is claimed. Capability publication awaits
complete native coverage, cache/append/lock integration and the required
negative/recovery matrix.

Overlapping tmpfs operations suppress version snapshots until their guards
drain, without changing its existing data/tree sleeping-lock order. XFS inode
transactions publish after native commit; a cancelled transaction grants no new
committed revision. Data writes/truncates conservatively invalidate after
partial effects. Version reservation exhaustion disables that object's cache
authority before further native effects; fresh backend operations remain
available. Object identity changes after native inode cache re-instantiation, so
old caches cannot alias an evicted/reused in-memory inode.

The generic VFS stream cache excludes remote files, including `O_ALWAYS_CACHE`.
Ordinary open-time and shared bulk prefetch cannot provide version authority;
read operations discard those caches and use fresh bulk transfers. The bounded
8KiB message read-ahead cache requires exact-open validation on each hit and
matching owner snapshots before and after filling bytes. A racing mutation or
failed validation revokes that fill. Content validation grants no metadata or
directory authority. `SEEK_END` always obtains a fresh owner result. Directory
iteration uses one fresh fixed batch rather than a timed, growing global cache;
negotiated directory revision changes during scanning return `EAGAIN`. Older
peers retain fresh RPC operation without cache authority. Existing remote fstat,
path metadata and mapped readlink were already excluded from local stale caches.

The append RPC currently accepts one bounded transport-frame prefix, returning a
short write for larger requests. Its fixed per-open receipt holds one
pending/completed outcome with monotonic operation ID, requested length and
SHA256 bytes. Exact completed replay returns the original offset/count/errno;
mismatched bytes, old IDs and a pending duplicate cannot select EOF again. Every
admission retains the exact native File across backend work. Unknown completion
latches an error on the client open rather than issuing another append ID. The
append capability remains disabled, and unsupported remote append returns
`EOPNOTSUPP` rather than attempting separate seek/write calls.

Owned qualification may use `bin/wos-cluster --no-setup` and isolated `bin/wos-
ktest --no-setup`. Required immutable bases and private COW overlays are
retained; inactive disposable images/shared staging are pruned after successful
and failed tests. Campaigns enforce a 20GiB disposable storage budget and 40GiB
free-space floor and preserve small evidence and unrelated VMs.

Exact-open positioned message I/O is independently gated by committed-I/O
support. Read replies echo a nonreused query nonce and carry an exact bounded
initialized byte prefix. Writes use the fixed per-open append/write receipt,
with SHA256 over opcode, offset, length and bytes. Native positioned writes
preserve the requested offset even on an O_APPEND description. Delayed or
mismatched responses cannot authorize another read/write. New client writes
report only a confirmed native committed prefix; partial effects and native
errno remain visible. An unknown accepted completion latches uncertainty on that
open and prevents an automatic retry. Secure I/O retains its independent
existing no-effect-only fallback and default mask. These features are not yet
advertised; the normal/KTEST builds, host framing tests and actual single-node
native outcomes passed, while multi-client packet/failure qualification remains.

The shared native advisory API now distinguishes authenticated remote process
scopes from local PID ownership and uses the same inode table for owner-local
and remote POSIX/OFD/flock conflicts. It has 512 fixed process/open
associations, with at most 32 scopes per open, and one inline replay receipt per
open. Owner waits have a global limit of 64 pending limit; START/POLL/CANCEL
never sleep for a contested lock. Admission closure is permanent, and task-
context retirement releases the pending capacity and applicable locks before the
registry File reference drops. Saved native ranges preserve the first
admission's EOF/client position. Native normalization reads the retained backend
directly rather than generic stat cache authority. The normal build and
production receipt host race tests passed; isolated KTEST passed the native
arbitration, bounded waiter, cancel/grant, replay, close and retirement fixtures
(73,284 assertions, zero failures). Lock RPC, interruptible client parking,
independent process-close replay and packet-level cleanup qualification remain
required before advertising owner locks.
