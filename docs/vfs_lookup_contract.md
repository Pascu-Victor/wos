# VFS pathname lookup and commit contract

This document is the checked inventory for pathname-bearing VFS entrypoints. It
describes the current retained-lookup coverage and the required end state. It is
not permission to add another resolve-to-string/act-later path.

## Core contract

`vfs_acquire_lookup()` produces a request-local, move-only `LookupHandle`. The
handle retains its `MountRef` and any dirfd `File`, records the mount and VFS
namespace generations and authorization context, and asks TMPFS or XFS to
retain the parent/final object identity. `vfs_consume_lookup()` takes
`g_vfs_namespace_publication_mutex`, rejects retiring or replaced mounts,
validates the backend generation/identity under the backend namespace lock, and
performs the operation before releasing the publication lock. A successful
namespace mutation advances `g_vfs_namespace_generation` while still
serialized. Callers release a stale handle before a bounded `-EAGAIN` retry.

The handle is local-only: it is never an ABI or WKI wire object and is never
stored in a cache. A remote-mount target handle may remain retained across the
bounded attach handshake, but no VFS, mount-table, or backend namespace lock is
held; the binding is revalidated only at publication. Open `File` references
and `MountRef` are the long-lived capabilities. Path caches are hints stamped
with cache/mount generations; a cache hit cannot replace handle validation at
a namespace-sensitive commit.

Current retained consumers include task-aware read-only operations,
single-parent mutations, dual-parent rename/link, pathname metadata, local
mount targets, and the checked remote-owner beneath adapters. Resolved/beneath
functions are trusted owner/backend adapters, not a reason for a syscall to
bypass acquisition.

## Checked inventory

The text between the markers is parsed by
`tests/host/unit/vfs_lookup_contract_source_test.py`. Keep every pathname API in
exactly one family row when declarations change.

<!-- VFS_PATH_INVENTORY_BEGIN -->
| Family and syscall operations | Public pathname APIs | Retained-handle migration or narrow exception | Linearization and remote owner fallback |
| --- | --- | --- | --- |
| `path.open` — `ops::OPEN`, `ops::OPENAT` | `vfs_open`, `vfs_openat`, `vfs_open_directory_view_as`, `vfs_open_file_as`, `vfs_open_file`, `vfs_open_file_resolved`, `vfs_open_file_resolved_beneath`, `vfs_open_read_file_as`, `vfs_open_read_file_resolved_beneath_as`, `vfs_open_file_resolved_beneath_as` | Retained for task-aware `vfs_openat` and explicit-snapshot read-only `vfs_open_file_as` (no descriptor allocation); the beneath form consumes a checked owner handle. The explicit directory-view form retains its native root/directory identity and inherited policy through File publication. Non-task boot/resolved loading is the narrow compatibility exception. Creation and truncation are mutations. | Validate parent/final identity and mount before fd publication; backend open/create is the commit. A remote handle is consumed by one owner request (`OP_VFS_OPEN_AS`, or regular-only `OP_VFS_OPEN_READ_AS`) carrying the admitted credential snapshot, never by a client lookup followed by a second request. |
| `path.stat` — `ops::STAT`, `ops::LSTAT`, `ops::STATAT`, `ops::STATVFS` | `vfs_stat`, `vfs_lstat`, `vfs_stat_resolved`, `vfs_stat_resolved_beneath`, `vfs_stat_resolved_beneath_as`, `vfs_statat`, `vfs_statvfs` | Retained for task-aware stat/statat and owner-beneath stat. `AT_EMPTY_PATH` is an fd/`File` exception. `statvfs` uses one admitted STAT lookup in process context, validates its selected object before querying the retained mount, and reports inherited readonly/nosuid policy. Path and File statistics publish output only on success. | Read the object and mount metadata validated by the handle. Remote stat is one admitted owner operation. |
| `path.access` — `ops::ACCESS`, `ops::FACCESSAT` | `vfs_access`, `vfs_faccessat` | Retained for task-aware access/faccessat; default checks select real UID/GID from one caller snapshot, and `AT_EACCESS` selects effective IDs. `AT_EMPTY_PATH` is an fd/`File` exception using the same selected identity. | Ancestor search and final permission checks use the same decision-only snapshot, preserving supplementary groups, ceiling and restrictions without publishing process credentials. Remote access may be answered by one owner stat/access request, never a client-side precheck used as commit authority. |
| `path.readlink` — `ops::READLINK`, `ops::READLINKAT`, `ops::REALPATH` | `vfs_readlink`, `vfs_readlinkat`, `vfs_readlink_resolved`, `vfs_readlink_resolved_beneath`, `vfs_readlink_resolved_beneath_as`, `vfs_realpath`, `vfs_realpath_resolved_beneath_as` | Retained for task-aware readlink and the owner-beneath adapter. `realpath` is a resolution-only result and confers no commit authority. | Read the retained final symlink while publication/backend locks validate it. The explicit owner adapter uses one caller snapshot and stages bytes until success. Remote `OP_VFS_PATH_AS` carries one admitted snapshot and uses the explicit owner adapter; legacy READLINK is refused. Native TMPFS/XFS/DEVFS/PROCFS REALPATH uses retained traversal. Remote REALPATH sends the complete raw remainder and one admitted snapshot to the confined owner adapter, then validates namespace and task context before publishing canonical text. Other legacy backend migration, remote PROCFS identity and forced-race qualification remain pending. |
| `path.mkdir` — `ops::MKDIR`, `ops::MKDIRAT` | `vfs_mkdir`, `vfs_mkdirat`, `vfs_mkdir_resolved_beneath`, `vfs_mkdir_resolved_beneath_as` | Retained for task-aware mkdir and the owner-beneath adapter; non-task boot use is narrow. | Validate retained parent/name, then create while serialized. Remote mkdir checks `OP_VFS_FEATURES` on the exact channel binding, then submits one mapped `OP_VFS_PATH_AS/MKDIR` with the admitted caller snapshot. The owner checks the confined parent and applies caller ownership, umask and parent setgid inheritance; legacy `OP_VFS_MKDIR` is refused. |
| `path.symlink` — `ops::SYMLINK`, `ops::SYMLINKAT` | `vfs_symlink`, `vfs_symlink_resolved`, `vfs_symlink_resolved_beneath`, `vfs_symlink_resolved_beneath_as`, `vfs_symlinkat` | Retained for task-aware symlink and owner-beneath symlink. The target is data; the link pathname is the retained destination binding. | Validate destination parent/name and commit once. Remote owner separately bounds literal target bytes and raw destination, maps one caller snapshot and consumes the confined destination parent. The exact-binding feature query precedes one mapped SYMLINK submission; legacy callerless symlink is refused. |
| `path.remove` — `ops::UNLINK`, `ops::RMDIR`, `ops::UNLINKAT` | `vfs_unlink`, `vfs_unlink_resolved`, `vfs_unlink_resolved_beneath`, `vfs_unlink_resolved_beneath_as`, `vfs_rmdir`, `vfs_unlinkat` | Retained for task-aware local operations and checked owner-beneath adapters. | Detach the validated parent/name under publication plus backend namespace lock, retaining open objects by backend lifetime rules. |
| `path.rename` — `ops::RENAME`, `ops::RENAMEAT` | `vfs_rename`, `vfs_rename_resolved`, `vfs_rename_resolved_beneath`, `vfs_rename_resolved_beneath_as`, `vfs_renameat` | Source and destination handles are both retained and validated before the namespace change. | Reject cross-mount rename and publish source removal plus destination replacement as one backend transaction/serialized compatibility commit. |
| `path.link` — `ops::LINK`, `ops::LINKAT` | `vfs_link`, `vfs_linkat` | Source object and destination parent/name are retained. | Validate both handles under deterministic lock order and commit atomically. Remote remains `-EOPNOTSUPP` because no two-path opcode exists. |
| `path.mode` — `ops::CHMOD`, `ops::FCHMODAT` | `vfs_chmod`, `vfs_chmod_resolved`, `vfs_chmod_resolved_beneath`, `vfs_chmod_resolved_beneath_as`, `vfs_fchmodat` | Retained for task-aware and checked owner-beneath operations; fd-only chmod uses a retained `File`. | Validate the final object and update its metadata under the backend lock. |
| `path.owner` — `ops::CHOWN`, `ops::FCHOWNAT` | `vfs_chown`, `vfs_fchownat`, `vfs_chown_resolved_beneath_as` | Retained for task-aware pathname chown; fd-only chown uses a retained `File`. Mapped remote ownership uses the admitted Snapshot; client readonly mounts reject before submission. Native backend ownership policies remain in force. | Validate final object and backend metadata generation at commit. |
| `path.time` — `ops::UTIMENSAT` | `vfs_utimens_resolved_beneath`, `vfs_utimens_resolved_beneath_as`, `vfs_utimensat` | Task-aware operations use retained handles; the owner-beneath form holds publication across confined resolve and commit. | Validate final object then update timestamps. |
| `path.xattr` — `ops::SETXATTR`, `ops::LSETXATTR`, `ops::GETXATTR`, `ops::LGETXATTR`, `ops::LISTXATTR`, `ops::LLISTXATTR`, `ops::REMOVEXATTR`, `ops::LREMOVEXATTR` | `vfs_xattr_resolved_beneath_as`, `vfs_setxattr`, `vfs_setxattr_beneath`, `vfs_getxattr`, `vfs_getxattr_beneath`, `vfs_listxattr`, `vfs_listxattr_beneath`, `vfs_removexattr`, `vfs_removexattr_beneath` | Task-aware XFS pathname xattrs capture one snapshot across retained lookup retries. Explicit owner APIs consume the supplied snapshot and confined native handles or retained Files; mapped WKI v2 stages retain and consume the admitted caller snapshot. Legacy ambient wrappers are not used by that transfer. | Consume one retained final object or one publication-serialized owner binding. |
| `path.chdir` — `ops::CHDIR` | `vfs_chdir`, `vfs_pin_directory_resolved_beneath_as`, `vfs_pin_directory_view_as` | Task-aware chdir consumes a retained directory handle; fd-only fchdir consumes a retained `File`. | Validate the directory handle and publish the logical task cwd while that binding is valid. Remote cwd text may name a remote route, but is not treated as an owner object capability. |
| `path.mount` — `ops::MOUNT`, `ops::UMOUNT`, `ops::PIVOT_ROOT` | `vfs_mount`, `vfs_umount`, `vfs_pivot_root`, `vfs_shutdown_unmount_all` | Mount targets consume retained lookup handles. Filesystem/device or remote-proxy initialization uses a reserved two-phase `PreparedMount`; only its table insertion occurs in the handle consumer. Unmount captures the exact retained mount/dev-id token, then uses the separate mount-topology retirement transaction; pivot/shutdown use that lifecycle transaction directly. | Serialize table publication; mark retirement before removal, exclude new refs, drain refs/open files without holding VFS/backend locks, then teardown. No namespace/backend lock crosses remote-device initialization, WKI attach/notification, or teardown I/O. |
| `path.batch` — `ops::METADATA_BATCH` | `vfs_metadata_batch` | The client preflights the complete bounded path set and retains one remote mount before sending. The owner preflights exact framing/confinement and each item consumes the corresponding checked beneath-export handle; no scalar fallback occurs after a mutation request is attempted. | The existing owner batch boundary and per-item status ABI are preserved. `-EOPNOTSUPP` is fallback-safe only before any request/effect; each owner mutation is independently linearized by its retained handle. |
| `path.resolve-helper` — no direct syscall | `vfs_resolve_dirfd`, `vfs_directory_path_beneath` | Text-only helpers. The directory-beneath query recovers a current name from an already-retained native directory and checks export confinement; it grants no child access. | No mutation commit. Directory-name recovery holds publication through native parent recovery and output. Never carry returned text across an unlocked lookup/act gap as authority. |
| `path.route-policy` — `ops::WKI_RULE_ADD` | `vfs_wki_rule_add`, `vfs_wki_rule_get`, `vfs_wki_default_rule_get`, `vfs_wki_effective_route_for_path` | Policy-prefix management/query exception; these strings select routing and are not filesystem object lookups. | Serialize in the WKI rule store. The selected route is input to later handle acquisition, never proof of an owner object. |
| `path.cache-observer` — no direct syscall | `vfs_cache_notify_invalidate_path`, `vfs_cache_notify_path_changed` | Post-commit observer exception. These APIs invalidate hints and must never perform or authorize a filesystem operation. | Call after/within the operation's publication protocol as required; cache epochs and mount generation make stale hints miss. There is no remote owner fallback—the owner operation sends invalidation only after commit. |
| `path.selftest` — no syscall; WOS_SELFTEST only | `vfs_selftest_remote_readonly_resolved` | Diagnostic fixture driver for direct remote pathname adapters using an explicit Snapshot and disposable disconnected mount. | Counts unexpected statuses; never used by production callers. Readonly mutations must stop before owner submission; both-OMIT preserves the existing owner validation path. |
<!-- VFS_PATH_INVENTORY_END -->

## Narrow boot and pseudo-filesystem exceptions

Boot-time TMPFS/initramfs population may use non-task resolved helpers only
before the namespace is published to concurrent users. Once tasks or mounts can
change concurrently, the normal retained contract applies. PROCFS and DEVFS may
materialize generated nodes or device registrations through their own retained
objects and subsystem locks; this exception is backend-internal and does not
allow a public pathname syscall to skip VFS mount retirement and authorization
checks. FD-only calls (`fstat`, I/O, `ftruncate`—the current `ops::TRUNCATE` ABI
case is fd-based—fsync, and fd xattrs/metadata) operate on retained `File`
references and are outside this pathname inventory.

## Remote owner admission and blocking rule

Legacy scalar owner handlers must reject malformed framing before confinement
or any VFS call. `relative_wire_path_has_safe_components()` rejects absolute
paths, `.`/`..` components, and embedded NULs;
`exact_relative_wire_path()` or the opcode-specific exact-suffix check rejects
trailing payload; `prepare_legacy_scalar_path()` checks both export-path and
visible-path concatenation bounds through `build_full_path()`. The checked
scalar set is open, stat, mkdir, readlink, symlink link-name, unlink, rmdir,
rename (both names), chmod, and utimens. Compound xattr and batch formats must
provide equivalent exact framing and beneath-export confinement.

The client may resolve routing and retain the remote mount, but the owner is the
only namespace authority. It must perform admission, beneath-export resolution,
authorization, lookup validation, and commit within one request. WKI sends or
waits must not occur while holding `g_vfs_namespace_publication_mutex`, a mount
table lock, TMPFS tree lock, XFS metadata/transaction lock, or a cached-path
lock. Unsupported operations fail before effects so callers can safely choose a
documented fallback.


Pathname handoff from a local alias to a remote filesystem selects the most
specific existing remote mount using the complete remaining path. Repeated
separators are collapsed, while names and dot components remain for owner
search and confinement checks. This matches direct remote pathname selection;
namespace, mount and admitted task context are validated before the lookup is
published. Descriptor-relative requests keep the exact retained directory
channel instead. Traversal through local overmounts beneath remote prefixes and
forced routing/mount races remain separate qualification work.

Final PROCFS READLINK retains the metadata parser's `ProcFile` and its pinned
target in the lookup handle. The metadata parser receives the request process
PID, so `self` is independent of the worker executing the lookup. Consumption
uses the admitted credential snapshot and PID, stages up to 512 bytes outside
namespace publication, then validates the lookup/task/mount context and the
retained target's credential generation before copying any bytes. CWD lookup
and borrowed descriptor release can enter VFS or wait for a remote owner and
therefore run outside publication. Cookie destruction also follows guard
release. Missing PROCFS final links return `ENOENT` before lookup publication.
Errors preserve the output buffer; successful reads preserve truncation
and the readlink convention of returning bytes without a terminating NUL.

The explicit PROCFS adapter still checks private target authority and lifetime;
a target policy change invalidates staged output. This is distinct from the
remaining public content migration, remote process identity mapping, and
broader path/FD mutation race qualification.

PROCFS OPEN also retains the selected `ProcFile`. `O_NOFOLLOW` checks its node
type without reading private link contents. Ordinary opens clone only metadata
and the pinned process reference; lazily generated content starts empty. The
request snapshot and PID govern diagnostic/private admission. The final
publication check validates lookup context and target credential generation
before installing the reserved descriptor, including its CLOEXEC bit. Candidate
File, target, mount and reservation cleanup follows release of the namespace
guard. Per-operation fops authorization remains dynamic after open.

A final PROCFS FD link follows its separate duplication contract: retain the
actual File from the pinned process's descriptor table, preserve its original
access grant, path, mount and shared offset, and install the retained reference.
The private target's generation is checked using the original ProcFile, rather
than interpreting the underlying local/remote File as PROCFS metadata. Failed
borrowed-FD release and successful no-cache remote notifications run outside
namespace publication. Missing final PROCFS opens return `ENOENT`; this backend
does not create a missing file.

PROCFS STAT and ACCESS retain the native/fallback ProcFile through final
consumption. Process nodes pin the selected Task when parsed; ownership metadata
uses that retained Task's single credential observation instead of another PID
lookup. Global nodes and a synthetic `self` link without a registered Task keep
their root-owned metadata. STAT stages the result outside namespace publication,
then validates the original request context and target generation before copying
it. ACCESS performs central DAC with that staged metadata and the original caller
snapshot. Missing paths and directory-required errors leave stat output unchanged.
Public metadata does not grant private-content authority or acquire a new debug
permission requirement. Retained metadata survives target exit; private reads,
links and opens retain their separate live-target policy. Public content
generators and remote process identity remain separate work.

Native local REALPATH consumes the retained PROCFS object directly. Its final
existence/directory check reads no contents and does not perform another PID,
credential or path lookup. The original component walk authorizes ancestor
search and private symlink traversal with the admitted caller snapshot/PID;
publication validates the original request and root before returning bounded,
canonical task-visible text. Public diagnostic filenames require no final read
permission merely to resolve their names. Confined remote-owner PROCFS remains
unsupported; enabling local REALPATH does not enable a remote process namespace.

### Mapped remote removal (Goal19 9DG)

`vfs_unlinkat` captures its credential snapshot before precommit lookup retries.
Remote paths retain raw ancestor components and exact directory descriptions;
the result of a submitted removal is returned directly without replay.
`vfs_unlink_resolved_beneath_as` uses the mapped snapshot for confined REMOVE /
NOFOLLOW_FINAL lookup and owner-side consumption under namespace publication.
TMPFS and XFS preserve parent search/write, sticky ownership, object type,
nonempty-directory and retained-open-object rules. Successful TMPFS hardlink
removal invalidates alias metadata; denied removals publish no cache change.
Scalar callerless unlink/rmdir are refused. Goal19 9DJ also maps metadata-batch
removal through the same explicit owner adapter.

### Rename pair credentials (Goal19 9DH)

Public `vfs_renameat` captures one credential snapshot before its retry loop and
uses it for source lookup, destination lookup and local commit. The shared
locked pair consumer rejects mismatched snapshots and revalidates both path
contexts, namespace/mount generations and native backend handles before invoking
the mutation. The legacy public pair API keeps its existing signature.

`vfs_rename_resolved_beneath_as` accepts an explicit path context, caller snapshot,
export confinement and optional retained source/destination directories. Both
lookups use that same identity and both confinement checks occur under namespace
publication before the native rename transaction. The serving task's credentials
are not substituted. Goal19 9DI connects this adapter to mapped remote rename;
the callerless scalar rename opcode is refused.


### Mapped remote rename (Goal19 9DI)

Remote rename retains raw source/destination components and optional directory
Files, including two descriptions carried by different WKI lanes. The client
copies one caller snapshot and both paths before binding queries, validates the
same export incarnation, and submits a single mutation on its captured channel.
Owner-issued directory references identify channel generations rather than
reconstructed path strings. The owner pins compatible live server bindings and
retains the exact directory Files before calling the confined pair adapter.
Renaming and reusing the original directory paths cannot redirect the operation.

Both parent/sticky checks and moved-directory write policy remain native backend
rules. Authorization precedes the backend trailing-source type check, preserving
existing errno ordering. Denials change neither source, displaced object nor
parent metadata. Missing mapped-rename support returns ENOTSUP without mutation;
there is no callerless fallback or replay after an uncertain response. Goal19
9DJ maps batch namespace operations as described below.


### Mapped metadata mutation batches (Goal19 9DJ)

`vfs_metadata_batch` captures one snapshot, applies the caller's umask once for
CREATE_CLOSE, and acquires OPEN/REMOVE/RENAME_SOURCE/RENAME_TARGET lookups to keep
raw remote components. It retains one remote mount through submission. Paths
requiring scalar-only handling are refused before mutation; the syscall ABI is
unchanged. The wire carries the same snapshot for every item, and the owner uses
explicit mapped open/unlink/rename adapters after whole-envelope validation.

Denied items preserve their objects and parent metadata while independently
allowed items may commit. Successful exclusive creation inherits caller UID,
parent-setgid or caller GID, and masked mode; an existing target returns EEXIST.
Mutation batches fit one packet and are submitted once on the binding used for
feature negotiation. The attempted flag forbids fallback after uncertain results.
Read-only STAT batches retain their existing version and chunking contract.


### Descriptor truncation grants (Goal19 9DK)

`vfs_ftruncate_file` consumes a retained File's admitted access mode through
central file-description policy before invoking its backend. Read-only or
invalid modes return EBADF without changing data or metadata. Directory,
negative-length, unsupported-callback and read-only-mount errors are explicit;
backend errors, including executable-content ETXTBSY, propagate unchanged.
Successful truncation leaves the descriptor position unchanged and publishes
cache invalidation once through the common helper.

A writable description retains its grant after credential changes, chmod,
rename, pathname replacement and unlink. Truncation does not reacquire pathname
permission or substitute the serving task's identity. Remote clients check the
grant before growth accounting or write-behind flush; the owner retains the
exact-channel File under its VFS lock and invokes the common helper after
unlocking. File release also occurs outside that lock.

Open-time O_TRUNC remains a separately authorized open effect, including an
O_RDONLY|O_TRUNC open when the caller has write permission. Actual remote
OPEN/OPEN_AT already applies that effect on the owner; VFS skips its redundant
second truncate. The returned read-only description cannot subsequently truncate.


### Explicit xattr owner authority (Goal19 9DL)

`vfs_xattr_resolved_beneath_as` accepts a caller snapshot and physical export
confinement, with an optional retained directory for relative paths. The native
component walk checks raw ancestors before normalization; publication validates
the same snapshot, path context, confinement and backend handle before calling
the existing XFS xattr consumer. Unsupported native backends return EOPNOTSUPP;
there is no ambient-path or recursive REMOTE fallback.

`vfs_xattr_file_as` consumes a caller-retained native File and explicit snapshot.
The current caller's xattr DAC applies even if the description was opened with
other credentials or is read-only. Rename, path replacement and unlink do not
redirect the retained inode. REMOTE Files return EXDEV at this owner API.
Existing namespace visibility, XFS permission/type rules, attribute limits and
backend errno remain. The shared path consumer checks a read-only mount before
mutation and validates any final directory requirement through the retained XFS
lookup before attribute access. It publishes cache invalidation only after success.

Local pathname syscalls capture one snapshot before retrying lookup and use
that same value at consumption. Goal19 9DM adds `LookupIntent::XATTR` so mapped
remote requests preserve the original component remainder and final directory
requirement. The WKI v2 owner consumes its admitted stage snapshot through these
APIs; legacy ambient wrappers are not used by the transfer.

A PATH transfer can carry a channel-bound directory FD with a nonempty relative
path. FD transfers pin the existing File and still apply current-caller metadata
DAC. This transport support does not yet give remote `fchdir` a retained cwd:
`change_directory_at` currently publishes pathname text for remote directories.
The runnable `service_wki_xattr.py --cwd-regression` exposes four rename/reuse
failures; remote cwd lifetime remains pending.


### Search-only native directory pins (Goal19 9DN)

`vfs_pin_directory_resolved_beneath_as` retains an exact TMPFS/XFS directory
under one supplied credential snapshot and physical export confinement. Raw
ancestor search, target search and directory type are checked through the native
retained lookup; publication validates the same binding before returning it.
Read permission is not required. Failure leaves the caller's output unchanged.

The returned File is pathless, holds its inode and mount until final release,
and uses the internal access mode `FILE_ACCESS_SEARCH_ONLY`. Public OPEN still
rejects mode 3. Data reads, writes, truncation and directory enumeration return
EBADF before effects, including VFS synthetic mount-entry enumeration. XFS close
excludes this mode from writable preallocation trimming. Read-only mounts can
supply pins. Later relative operations recheck the current supplied caller and
export boundary against the retained directory, including after rename/reuse.

This is the native prerequisite for retained remote cwd. It adds no WKI message,
remote directory retirement protocol, or cwd publication change. The four known
remote cwd regression failures above remained pending at this step; 9DQ below integrates them.


### Confined current directory names (Goal19 9DO)

`vfs_directory_path_beneath` recovers an already-retained native TMPFS/XFS
File's current name from its inode, while holding namespace publication. It
combines the current mount path with the native parent walk, checks a complete
component prefix against the canonical physical export root, and returns an
export-relative absolute name (`/` for the export root). Cached File path text
is never used. Removed directories return ENOENT; an object moved outside the
export returns EPERM. Short buffers and all other errors preserve both output
bytes and the optional output length. No File or mount reference is transferred.

This name query grants no access to directory contents. Like existing native
getcwd, it imposes no new search/read check on an already-held directory after
chmod. Pin acquisition and later relative access still check the supplied caller.
A future WKI caller must first authorize and retain the exact channel-bound File;
no worker credential or pathname fallback is supplied by this helper. REMOTE
Files return EXDEV, preventing recursive owner queries.

The helper is sleepable and must run outside an already-held namespace
publication lock. WKI pin/retirement/name-query transport and remote cwd
integration were pending at this step; see 9DP and 9DQ below.


### WKI directory reference transport (Goal19 9DP)

The new mapped PIN/PATH/CANCEL protocol uses the native search-only pin and
current-name primitives above. The client returns a pathless remote File with
an exact-binding retirement token; creation and name queries preserve caller
outputs on errors. Its owner File is selected by exact channel and token, so
rename/reuse cannot retarget name recovery or later directory-relative calls.
PIN captures/maps caller authority; name recovery reports already-held identity.
Data/listing callbacks reject search-only references before effects. See
wki_security.md for framing, feature negotiation and durable cancellation.

Task cwd still needs to retain this File, route relative operations through it,
and release it outside VFS publication and Task spinlocks. Public chdir/fchdir/
getcwd behavior is not changed in this transport slice. Existing four remote cwd
regressions were pending at this step; see the integration and live evidence in 9DQ below.


### Retained remote task cwd (Goal19 9DQ)

Remote chdir/fchdir now obtain a search-only directory PIN with one captured
caller Snapshot across lookup, RPC and publication retries. fchdir selects the
retained source FD; chdir preserves raw owner components. The resulting File
owns its mount busy reference and cancellation record. Publication installs an
atomic-refcounted DirectoryAnchor alongside cwd text after checking task context
and namespace/mount epochs. Local fork and exec retain the anchor, threads use their
filesystem owner, and ACTIVE exit/unpublished teardown release it. Candidate
and replaced anchors retire outside Task spinlocks and namespace publication,
including a change from remote cwd to a native directory.

Mapped AT_FDCWD operations retain the remote directory File, pass raw relative
components and the current operation's Snapshot, and never select the child by
cached cwd text. Current-name recovery supplies compatibility metadata only.
getcwd and staged procfs cwd readlink query the retained object outside namespace
publication and publish output only after validation. Name queries require no
new read/search grant; later relative access and fchdir recheck search. Removed
cwd names report ENOENT and short output reports ERANGE without partial output.

The owner currently confines the whole registered export. Remote directory
acquisition/use therefore requires the entire local export mount to lie beneath
the Task root; a narrower root returns EXDEV until owner-side narrower-root
confinement is implemented. Text-only/serialized helper paths cannot borrow a
remote cwd reference or issue network waits; unsupported operations return
ENOTSUP. At this step relative realpath/procfs symlink-follow remained
unsupported; 9DR below adds their integration. Remote compute
placement still transfers cwd text and needs a separate retained-identity contract. Public mapped
OPEN/STAT/access/READLINK/chmod/timestamps/xattrs/mkdir/symlink/remove/rename use
the retained-directory path. New cwd references require the negotiated directory
feature, with no old-owner read-open fallback.

The existing four rename/reuse cwd regressions now pass in the live two-node
cluster (294 total xattr checks). The additional service_wki_cwd fixture passes
25 checks for current names, execute-only chdir/fchdir, source close, fork/exec,
procfs, group search, revocation, removal and unchanged outputs/replacement data.
These are functional OS permission tests; broader fault/fence and concurrency
qualification remains pending. Full Goal19 is not complete.


### Relative realpath and procfs cwd following (Goal19 9DR)

A matching retained remote cwd now enters realpath's component lookup before
text preflight. Other callers keep the existing route-compatibility preflight.
The remote consumer chooses REALPATH_AT with its exact directory File and raw
relative components; it no longer discards that identity in favor of a pathname
recovered by a preceding query. The confined owner realpath helper accepts an
optional caller-retained directory, validates current traversal and export
membership under publication, and returns canonical export-relative text.

REALPATH_AT is PATH_AS selector13, negotiated through FEATURES bit128 on the
same captured binding used by the operation. It carries the canonical caller
record, existing8byte header, positive4byte directory FD and1..511 raw relative
path bytes. Only REQUIRE_DIRECTORY is allowed; final symlinks are followed.
The reply has0..511 canonical bytes without a wire NUL (empty means export root).
The client validates exact framing, selector and canonical text before adding a
NUL locally; errors/short buffers preserve output. READLINK_AT and STAT_AT keep
their formats and now also require the captured exact binding during their send.
No selector retries by pathname or moves a numeric descriptor to another lane.

The nonserialized local component walker also permits procfs cwd name queries
with its admitted caller Snapshot, outside namespace publication. Serialized
callers remain unable to wait for remote cwd. This restores ordinary following
of /proc/self/cwd into a remote directory without loosening procfs private-access
checks. It is not a cross-node Task cwd transfer mechanism.

Baseline9DQ returned ENOTSUP for relative realpath during text preflight and for
procfs cwd following. Source review additionally found the later pathname-only
RPC. The unchanged30-case live fixture now passes on XFS and TMPFS, including
retained realpath after ancestor search revocation, rename/reuse and unchanged
short output. The existing384-case scalar realpath matrix passes on each native
filesystem. Broader root/routing races, narrower Task roots and remote placement
identity transfer remain pending; Goal19 is still active.


### Retained native descriptor metadata (Goal19 9DS)

`vfs_fchmod_file_as`, `vfs_fchown_file_as` and `vfs_futimens_file_as`
accept a caller-retained `File` and an explicit immutable `Snapshot`. Syscall
wrappers capture the process snapshot once, retain the descriptor, call the
adapter, and release after the adapter's namespace/backend locks are gone.
The adapters neither consume that reference nor consult task credentials,
cached pathname text or path routing. Rename, pathname reuse and unlink do
not change the selected inode. Metadata authority is checked against the
current caller and retained inode; the description's original read/write
grant does not supply ownership authority or restrict an owner's metadata call.

TMPFS validates its operations table before casting private data, including
for chmod/chown, so pipe state cannot be interpreted as `TmpNode`. Mutable
native metadata rejects a retained mount's immutable read-only policy before
effects. Timestamp input validation precedes that check, and NOW/OMIT intent
survives preparation. TMPFS/DEVFS OMIT/OMIT stays a no-op; XFS retains its
existing backend read-only behavior. FAT retains fixed-projection/no-change
policy and errno. Cache notifications occur only after successful mutation.

These are blocking-context native adapters, called outside namespace and
FD-table locks. They preserve the existing syscall ABI. The native adapters
remain nonrecursive: a REMOTE File returns ENOSYS for chmod/timestamps and
EOPNOTSUPP for chown. Public syscall wrappers separately dispatch remote
descriptions through the mapped transport described below.


### File retirement with admitted references (Goal19 9DT)

`vfs_put_file_result` drops exactly one owned reference and returns the backend
close result only when that release destroys the file; a nonfinal or null release
returns zero. `vfs_put_file` remains the void form and discards the result. Both
use the existing atomic final-owner decision and destruction order. Final close
may block and must occur outside namespace, descriptor-table and WKI registry
locks. `vfs_close_file` still explicitly destroys an exclusively owned file.

Remote registry rows transfer one owned reference on publication. CLOSE, deferred
cleanup, executable cancellation and published OPEN reply-failure cleanup drop
that reference. A previously admitted xattr stage or operation keeps the exact
file, its native inode, mount pin and any executable writer exclusion alive.
Retirement stops new FD admission immediately; it never waits for a remaining
borrower, which may require a later request on the same channel to finish.
Synchronous CLOSE preserves final backend errno. If destruction is deferred,
its eventual close error follows the existing final-release logging behavior;
it cannot be reported retroactively by the already-completed retirement reply.

### Mapped remote descriptor metadata (Goal19 9DU)

Public fchmod, fchown and futimens capture one current caller Snapshot and retain
the File through the remote call. The transport uses the description's exact
proxy, owner FD and complete channel binding for a feature query followed by one
mutation. Missing feature256 returns ENOTSUP before mutation; no pathname,
alternate lane or reconnect fallback supplies authority. A lost mutation reply
remains an ambiguous outcome and is not replayed by this adapter.

The owner maps the canonical caller under the admitted peer identity policy
before looking up and retaining the exact registry File. Registry locks are
released before the native explicit-Snapshot adapter runs. Recursive remote
Files and private executable views are refused. Rename, pathname reuse, unlink
and registry retirement cannot substitute a different inode during the call.
Current owner/group/ceiling policy governs metadata independently of the open
description's data grant. Chown sentinels, set-ID clearing and raw NOW/OMIT
timestamp intent are preserved; the owner supplies the clock for NOW.

Successful changes invalidate both the exact remote context and the public
File metadata cache, including descriptions without a current pathname.
Denied calls and OMIT/OMIT do not issue cache-change notifications. The public
wrapper releases its reference after the RPC and cache notification; the owner
reference is released outside registry and namespace publication locks.


### Mapped pathname ownership (Goal19 9DV)

Public chown/fchownat use CHANGE_OWNER lookup intent, preserving raw remote
components, follow/nofollow and directory requirements. One request Snapshot
survives every native lookup retry and commit. AT_EMPTY_PATH retains the existing
descriptor dispatch. Both public native changes and the explicit confined
vfs_chown_resolved_beneath_as adapter use the same retained-handle consumer;
TMPFS/XFS/DEVFS enforce central owner/group/ceiling policy before commit and
reject mutable read-only mounts. FAT retains its fixed ownership policy.

Remote paths use a canonical caller record, owner/group uint32 values and either
an export-relative path or a raw path relative to the exact retained directory.
The read-only feature512 query and single CHOWN17 mutation share one complete
channel binding. Unsupported owners return ENOTSUP before mutation; no pathname
fallback substitutes for a directory reference. An ambiguous result is not
replayed. The owner maps credentials before retaining that directory under the
registry lock, then performs confined lookup and commit after unlocking.

The owner adapter uses its Task only as path context; caller authority comes
from the supplied Snapshot. Namespace publication protects export revalidation
and backend commit, with no RPC under that lock. UID/GID sentinels, supplementary
groups, set-ID clearing, nofollow symlinks and denied-no-partial-state behavior
are unchanged. Successful changes notify caches; failures do not. The internal
resolved-path fallback remains nonrecursive for REMOTE; public callers use the
mapped dispatch.


### Scalar metadata request continuity (Goal19 9DW)

Public pathname chmod now follows chown and timestamps: capture the request
Snapshot before the lookup retry loop, pass it to acquisition, and validate the
same supplied Snapshot at native commit under namespace publication. A later
credential publication cannot change an already-admitted operation's authority.
Namespace, mount, root/cwd and retained-object checks remain mandatory. A stale
lookup retries with the original caller; a mismatched caller or path context
cannot reach the consumer. Remote results return directly without replaying a
possibly completed mutation, including when the returned status is EAGAIN.

The native regression deterministically invalidates a real lookup with namespace
publication, changes only a private construction Task's credentials, retries and
executes actual TMPFS mode policy. It verifies owner success, nonowner denial,
unchanged denied metadata and rejection of a mismatched Snapshot or changed root.
No production test hook or mutable credential borrowing was added.

## Client read-only remote mount admission

Remote namespace and pathname metadata consumers check the immutable mount retained by their lookup before submitting a mutation. Direct resolved adapters use their retained MountRef. Local argument/path validation and cross-mount rename errors retain precedence; EROFS then precedes owner-only permission, existence, type, or transport errors. Readonly rename is rejected even for equal path text: the client cannot prove an atomic owner-side no-op. Writable-client owner errno/no-op behavior and all native backend contracts are unchanged. Both-OMIT timestamps are intrinsically non-mutating and continue through existing owner validation; invalid timestamps still fail locally. No speculative mutation, stat-then-mutate substitute or request replay is used.

Mutation batches validate the full path set and retain one mount before this gate. Every item in a rejected readonly mutation batch reports EROFS, with cleared stat output and no request attempted. Read-only STAT_FOLLOW batches remain eligible for remote submission. Open, xattr, truncate and descriptor metadata retain their corresponding mount-policy gates.


### Caller-authorized filesystem statistics (Goal19 9JH)

Process `vfs_statvfs` captures one credential Snapshot and uses the same retained STAT/FOLLOW_FINAL path as other metadata operations. Native and PROCFS targets must exist and satisfy trailing-directory semantics before filesystem statistics are queried. Shared consumption validates task, namespace, mount, backend and scoped directory/parent membership; cached path text cannot substitute for caller admission. `ST_RDONLY` and `ST_NOSUID` include the selected lookup's inherited restrictions. The no-Task early-boot resolver remains separate.

Remote pathname admission uses the existing authenticated stat or retained-directory stat-at operation outside namespace publication, then validates local request/mount state before publishing the existing synthetic readonly filesystem statistics. No remote totals protocol or transferable root grant is introduced. PROCFS metadata preparation likewise runs outside publication and its generation is revalidated at consumption. Lookup/reference retirement follows guard release.

Both pathname and File-based queries stage a complete `Statvfs` result. Failure leaves caller output unchanged; File queries keep their original mount and retained policy across rename or overmount. Statistics retain existing approximate accounting behavior. Runtime evidence and remaining limits are recorded in `build/goal19-statvfs-authorization/validation.json`.

`vfs_pin_directory_view_as` publishes a search-only native File with an independent retained root/cwd view. Lookup and final search admission use one explicit Snapshot; the File retains effective mount policy while its detached identities retain only native flags. Owner directory PIN derivation and PATH recover the boundary from that exact view. This does not transfer a grant or install task filesystem state.


### Remote mount publication (Goal19 9LX)

Remote lane admission and binding validation finish before the VFS publisher
commits a mount row. Precommit failure retires only the private candidate group;
it does not withdraw other mounts of the same resource generation. Publication
success is the commit: subsequent asynchronous retirement cannot change it into
a failed syscall. Constructor lifetime pins span publication and teardown;
registry locks do not span VFS publication or remote waits. Actual VFS removal
owns the mount-released flag. See [the audit](goal19_remote_mount_publication.md).
Declared LOCAL `/` routing and retained cwd authority are preserved.


## Native unmount export transition (Goal19 9LZ)

Public native unmount captures one authorization Snapshot and a bounded target
copy, then selects the exact backing dev ID under namespace publication. Before
attempting removal it releases the namespace guard and mount reference, closes
WKI VFS admission and drains admitted binding work. Successful exact-ID removal
retires only exports of that backing and publishes incarnation withdrawals before
reopening admission. Failed removal cancels the transition, retaining matching
bindings and issued Files; existing revision advancement invalidates previously
captured export snapshots. Custom unrelated aliases retain their names. Remote
proxy unmount remains on its exact-group teardown path. No caller-root export
rediscovery, declared VFS routing, wire format or existing pathname errno change.
See [review and qualification limits](goal19_native_unmount_exports.md).
