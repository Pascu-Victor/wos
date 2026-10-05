# Goal19 traversal qualification (9MD)

The `vfs-traversal` authority domain is qualified as enforced. The review covers
one admitted credential snapshot, component search, retained root/cwd/object
identity, declared routing, mapped owner traversal, staged denial outputs and
the mount transitions that could otherwise substitute a different authority
boundary. This closes the remaining traversal reconciliation from9LU–9MC.
It does not close the separate open/create, mutation, metadata, mount-control,
WKI identity/resource or other pending domains.

## Requirement-to-evidence checklist

| Requirement | Current source contract | Authoritative evidence |
| --- | --- | --- |
| One identity per decision | Task adapters admit a Snapshot; lookup retries and explicit consumers retain it. ACCESS projects real UID/GID by default or keeps effective IDs for AT_EACCESS, preserving other credential fields. | Current `vfs/core.cpp` and `credentials.hpp`;9LV's40 native/mapped identity checks; current TMPFS/XFS ACCESS KTESTs. |
| Search original components | Common traversal checks visible directory search before processing raw components, including dot/dotdot and symlink expansion. Canonical text cannot erase a denied ancestor. | `acquire_local_component_lookup`, `local_walk_search`; current common-component, local/foreign-route and owner-boundary KTESTs; established1,568-case live traversal matrix. |
| Retain root/cwd and selected objects | Typed anchors, native directory views and mount membership retain exact objects. Namespace/mount/search epochs and backend identity are revalidated before consumer invocation; denied preparation does not replace the previous output. |9MB source reconciliation, native retained-context tests and their current9MC PASS records. |
| Honor declared routing | Raw/canonical route disagreement refuses; mapped remainder retains its components. Explicit LOCAL `/` selects receiver root. A retained directory FD keeps its selected object even under a later covering mount. | Current local/foreign-route KTESTs;9LW/9LX mixed-mount source and24-check ordinary fixture; repeated9LZ/9MA fixture evidence. |
| Explicit mapped authority | Authenticated policy and request bytes are copied before receive acknowledgement, with an exact retained binding. Snapshot mapping precedes explicit native owner consumers. Owner recursion into REMOTE refuses. |9MC owner flow and current mapped binding tests;9LV mapped identity and9MA directory-path fixtures. |
| Native backend policy | TMPFS/XFS use actual retained directory entries and current search authority. FAT32 reports its declared root ownership/fixed modes; readonly attributes narrow file access. DEVFS distinguishes traversable hybrid namespaces from permission to use the device. PROCFS uses explicit caller and retained subject metadata. | Current backend/core entrypoints; common-walk, DEVFS hybrid, PROCFS identity/output and readonly owner FAT metadata KTESTs. |
| Unsupported authority transfer refuses | Scoped native views require supported TMPFS/XFS membership proof. Missing covers and unsupported backends refuse instead of falling back to text. Confined owner PROCFS refuses because a worker PID cannot represent the caller. Owned-root/third-owner transfer limitations remain explicit. | `local_walk_bind`, `local_walk_cross_native_mount`, owned-root fallback guard;9MB review and native scoped-view/owner-boundary KTESTs. |
| Directory lifetime does not widen authority | Retained Files outlive registry removal. New child lookup checks admitted caller search. Grant adoption binds complete credentials, resource/source/target/submission identity and restrictions, then makes one final claim. | Current9MC source/destination retirement cases on TMPFS/XFS, existing cancellation/rename/name-reuse/purpose tests and readonly/nosuid assertions. |
| Automatic mounts remain within their admitted namespace | Bounded hostname/export construction rejects dot/dotdot components and malformed names. Pending work matches the resource generation; mount admission snapshots incarnation and validates resource/channel state around private auxiliary preparation. | `remotable.cpp` path builder/pending worker and `remote_vfs.cpp` mount entrypoints; current automatic-path KTEST and source guards. |
| Failure and retirement preserve unrelated mounts | Publication is one commit after fallible preparation; failed candidates retire only their group. Cleanup uses exact private-data ownership, honors descendants/open Files, and defers successors beneath an unreleased retired parent. Native export unmount drains admission and removes only the exact backing identity. | Current candidate-isolation, busy-owner/replacement, retired-ancestor, manual teardown and native export KTESTs;9LX/9LZ/9MA live mount/replacement evidence. |

## Automatic mount lifetime findings

The pending worker does not create remote directories while waiting for its own
withdrawal queue. It publishes validated mount-table children directly. An
unreleased retired ancestor defers successor attachment without consuming its
retry budget. Resource observation generation and owner incarnation prevent
stale queued work from selecting a replacement resource.

Private mount construction holds its reference across auxiliary preparation,
publication and cleanup. A successful publisher return commits the mount;
concurrent withdrawal can then retire it, but does not turn that commit into a
failed syscall. Group deactivation removes lane availability under `s_vfs_lock`.
Waiter completion, channel closure, VFS removal and buffer release run outside
that lock. Proxy erasure requires drained File/lifecycle references, released
resources and removal of its own mount row.

`unmount_filesystem_by_private_data` selects the exact owner under `mount_lock`
in a stable pivot epoch. `complete_retired_mount_teardown` waits outside the lock,
checks descendants/open Files again, and leaves an unretainable tombstone during
backend closeout. Busy/failing teardown reopens the same row. Remote owner
ownership is pinned until row/storage removal completes. It cannot remove a
replacement merely because the pathname is reused.

No new locks, allocation assumptions, ABI/wire layouts or runtime behavior are
introduced by this qualification.

## Evidence, failures and scope

`build/goal19-traversal-qualification/validation.json` indexes the current
source/kernel hashes, checks,57 relevant PASS records and prior evidence hashes.
Normal build ID is `735092b8027f69db3f19154ec37ab1af21450e70`; diagnostic build ID
is `0742beb2b60f4980f0cdf6bd86b7505e3a048545`. Both were built in9MC. That isolated
run passed59,759/0 and powered off; it was not rerun in9MD. No production code,
build, package or VM launch changed in9MD. Mount/remotable source checks and the
policy audit are rerun for this review.

The9LZ STAT_AT timeout remains a failed observation with an unknown cause.
The9MA successful matrix and2,240 additional directory checks do not erase it or
establish a transport fix. Neither the timeout nor the earlier auxiliary attach
ENOENT establishes an authorization bypass; the latter's partial-publication
behavior was separately reproduced and fixed in9LX. These availability limits
remain recorded for broader WKI qualification. No packet injection or
penetration testing was performed.

Enforced here means the listed traversal authority requirements have current
implementation and matching evidence. It does not claim every possible
concurrent schedule, universal filesystem feature support, transactional remote
mutation after an ambiguous transport failure, or completion of Goal19.
All21 manifest domains remain present;13 are enforced and8 pending. The next
pending domain is `vfs-open-create`.
