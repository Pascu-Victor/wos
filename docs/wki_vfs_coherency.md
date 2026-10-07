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
races. Four production scope cases passed, including 128 concurrent close/
retirement races. Four lock wire and three further scope wire cases passed, plus
seven exact-observation framing/EOF/native-cookie cases (100 focused host cases
total). Normal Build WOS and fast diagnostic KTEST compilation passed. Native
tmpfs content/metadata/namespace and XFS committed-inode/data hooks now maintain
private owner revisions; exact-open authenticated version requests use deferred
owner work and strict reply framing. The version feature remains unadvertised.
Isolated KTEST runs passed 73,270, 73,280, 73,282, 73,284, 73,301, 73,302 and
73,304, 73,305, 73,306, 73,307 and 73,361 assertions
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
associations, with at most 32 scopes per open. Each open retains a monotonic
operation watermark and recent synchronous query/unlock replay receipt; up to 64 global
pending/terminal acquisition records remain until exact ACK or retirement. Every
acquisition reserves its receipt before a native effect, including an immediate
grant; higher shared-open query/unlock IDs cannot erase its result. Query and
unlock remain available when acquisition replay storage is full. A shared
open can accept unlock/query and independent waits while another process waits.
ACK releases only terminal receipt storage, preserving any granted lock.
START/POLL/CANCEL/ACK never sleep for a contested lock. Admission closure is
permanent, and task-
context retirement releases the pending capacity and applicable locks before the
registry File reference drops. Saved native ranges preserve the first
admission's EOF/client position. Native normalization reads the retained backend
directly rather than generic stat cache authority. The normal build and
production receipt host race tests passed; isolated KTEST passed the native
arbitration, bounded waiter, cancel/grant, replay, close and retirement fixtures.
The subsequent run passed real shared-open unlock/query during child waits,
multiple pending process scopes and ACK/capacity recovery. Lazy nonreused Task
process generations also passed PID reuse and retained thread-owner tests
(73,301 assertions, zero failures). The additive lock RPC owner adapter derives
peer/boot from the authenticated binding, retains the exact native File and
validates fixed operation-echoed replies. Its native foundation passed in the
following 73,302-assertion run. The unpublished PROCESS format now requires a
nonzero process/inode scope token; OFD/flock retain description ownership.

Native POSIX scopes have 512 fixed records and nonreused owner tokens. A retained
ordinary open witnesses a canonical inode/process scope. Closing it releases
only that scope's POSIX locks and cancels pending POSIX requests; repeated close
cannot release a later scope. ACK frees closed receipt storage. Registration
watermarks prevent a delayed register from resurrecting an acknowledged scope,
and old witness retirement acts only on its admitted token. Identity-only probes
remain available during version publication and grant no cache freshness.
The native scope lifecycle/capacity fixture passed in the 73,304-assertion run.
Independent scope CLOSE/ACK wire controls now use scope authority and an exact
authenticated binding, with no numeric FD lookup. Their native controller passed
in the 73,305-assertion run, including old close/ACK successor protection and
preservation of disjoint OFD and flock locks on process scope close. Client scope
witnesses, any-descriptor close outboxes,
interruptible parking and packet-level cleanup qualification remain required
before advertising owner locks.

Legacy read, directory batch, fstat, seek and fsync owner handlers now acquire
their own File reference before leaving the registry lock. Native operations and
final releases run outside that lock. The 73,306-assertion run passed retirement
from admitted read/directory/seek callbacks and generic stat/fsync after registry
retirement, including access, callback and stale-channel denials. The subsequent
73,307-assertion run passed immediate-grant replay across higher shared-open
queries and unlocks, all 64 terminal receipt slots, denial before a 65th effect,
unlock progress at capacity, ACK recovery and successor preservation.

Exact-owner fresh observations now have an additive unpublished protocol for
stat, directory batches and EOF selection. All requests retain the complete
ordinary-open token, binding, boot epoch and query nonce; replies echo the query
and operation with a bounded exact body. Stat uses the retained native inode and
bypasses generic metadata caches. TMPFS observes tree and inode data under the
existing tree-to-inode lock order; XFS uses inode-to-metadata order. EOF selects
fresh size plus a checked signed offset and preserves the native File position.
Metadata and EOF do not require readable access; directory reads do.

Directory observations hold native namespace protection while copying each
record and bracket the batch with directory versions. A membership race returns
`EAGAIN` without publishing entries. Batches follow opaque 64-bit `d_off`
continuations through sparse tmpfs slots and XFS cookies. The client validates
the complete batch, including native aligned record lengths and bounded names,
before returning an entry. Filtered recursive-WKI views scan with native cookies
and expose separate visible continuation ranks. This object-only reply grants no
cache authority; version validation remains independent. Clients select these
observations only with owner-version negotiation, keep fresh legacy fallback for
older peers, and never downgrade an exact observation denial. Host validation
passed. The 73,361-assertion run passed the actual TMPFS fixture with a proven
poisoned generic cache hit, fresh native bypass, sparse cookies, pending
publication denial, exact authority/access denials and retained unlinked
metadata. The same run passed real mounted-XFS identity and versions across
hardlinks, writes, append, truncate, chmod/chown, xattr, rename and unlink;
exact metadata/EOF/directory observations preserved cookies above 32 bits and
the retained inode's native device key. Its scoped POSIX wait/grant/cancel/ACK,
old CLOSE successor protection, independent OFD/flock and exact registry
retirement checks also passed, followed by clean native unmount. Client lock
syscalls/outboxes and packet-level multi-client/recovery qualification remain
required. New capabilities remain unadvertised.

Ordinary remote final close now tracks the generic detachment prefix and return
tail on the closing task's stack. The backend receives exclusive ownership only
after its own fatal-close tracker is linked, before flush or cancellation can
wait. Task cleanup first quiesces proxy/wait references, then finishes owned
prefixes or completed tails and leaves delegated effects to the backend.
Notification and stream attachments remain reachable through their detach mutex
acquisition. No new close allocation, File/Task layout, syscall or wire change is
required. A follow-up 73,361-assertion KTEST run passed the extended actual remote
File fixture for prefix/delegated/tail abandonment, unrelated and repeated PID
cleanup, exact cancellation slots, cache attachments, heap paths and mount pins.
These fixtures simulate abandoned ownership phases; they do not kill a process
parked on a contended detach mutex. Normal and isolated fast KTEST builds,
formatting and existing advisory source-policy checks passed. Client advisory
syscalls/outboxes and the packet-level matrix remain required for Goal 26.

The client prerequisites now include fixed-capacity lifecycle controllers for
process/inode scopes and acquisition receipts, with nonreused slot serials and
short explicit RPC gates. Lost registration retains its original identity;
close suppresses further grants while registration is uncertain. Confirmed close
permits a successor while old users and ACK storage remain pinned. Cancellation
preserves an already committed grant, and stale replies or gate releases cannot
affect reused storage. Nine production-header host cases passed, including
registration/close and cancel/grant races, alongside eight native scope/receipt
regressions.

Ordinary private-open preparation captures its reserved binding's validated
feature reply before publication. A peer offering owner locks is identified
while preparation still owns the File and cancellation record; unsupported
native lock semantics leave the object identity unavailable. The actual remote
File lifetime fixture passed in a further 73,361-assertion KTEST run and verified
that a changed proxy feature observation cannot substitute lock support or
trigger IDENTIFY on the captured non-lock binding. Normal and isolated fast
KTEST builds and formatting passed. Negotiated IDENTIFY, live client dispatch,
scope/operation outboxes and packet qualification remain required. The public
remote advisory path still returns `EOPNOTSUPP`; no new capability is advertised.

Retained TMPFS directory versions now cover their synthetic `..` entry when
rmdir or rename replacement detaches the directory. The directory and metadata
versions advance before reclamation; its identity and content version remain
unchanged. Source movement and both parents' membership publication retain their
existing coverage. Readonly, authorization, nonempty, root and stale-lookup
denials preserve committed versions and native errno behavior. Actual retained
directory and timestamp fixtures passed in a 73,497-assertion isolated KTEST run,
including TMPFS metadata-only timestamp publication and real mounted-XFS
timestamp commit, OMIT and denial behavior. Normal and isolated fast KTEST builds,
formatting and source-policy checks passed. This is a mutation-audit checkpoint;
full client integration and multi-client packet qualification remain required.
