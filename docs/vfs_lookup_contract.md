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
| `path.open` — `ops::OPEN`, `ops::OPENAT` | `vfs_open`, `vfs_openat`, `vfs_open_file`, `vfs_open_file_resolved`, `vfs_open_file_resolved_beneath` | Retained for task-aware `vfs_openat`; the beneath form consumes a checked owner handle. Non-task boot/resolved loading is the narrow compatibility exception. Creation and truncation are mutations. | Validate parent/final identity and mount before fd publication; backend open/create is the commit. A remote handle is consumed by one owner request (`OP_VFS_OPEN`), never by a client lookup followed by a second request. |
| `path.stat` — `ops::STAT`, `ops::LSTAT`, `ops::STATAT`, `ops::STATVFS` | `vfs_stat`, `vfs_lstat`, `vfs_stat_resolved`, `vfs_stat_resolved_beneath`, `vfs_statat`, `vfs_statvfs` | Retained for task-aware stat/statat and owner-beneath stat. `AT_EMPTY_PATH` is an fd/`File` exception. `statvfs` retains its mount for the complete query. | Read the object and mount metadata validated by the handle. Remote stat is one admitted owner operation. |
| `path.access` — `ops::ACCESS`, `ops::FACCESSAT` | `vfs_access`, `vfs_faccessat` | Retained for task-aware access/faccessat; `AT_EMPTY_PATH` is an fd/`File` exception. | Permission check consumes the same validated object snapshot; it must not authorize one object and later act on another. Remote access may be answered by one owner stat/access request, never a client-side precheck used as commit authority. |
| `path.readlink` — `ops::READLINK`, `ops::READLINKAT`, `ops::REALPATH` | `vfs_readlink`, `vfs_readlinkat`, `vfs_readlink_resolved`, `vfs_readlink_resolved_beneath`, `vfs_realpath` | Retained for task-aware readlink and the owner-beneath adapter. `realpath` is a resolution-only result and confers no commit authority. | Read the retained final symlink while publication/backend locks validate it. Remote readlink is one admitted `OP_VFS_READLINK`. |
| `path.mkdir` — `ops::MKDIR`, `ops::MKDIRAT` | `vfs_mkdir`, `vfs_mkdirat`, `vfs_mkdir_resolved_beneath` | Retained for task-aware mkdir and the owner-beneath adapter; non-task boot use is narrow. | Validate retained parent/name, then create while serialized. Remote owner admits and commits one `OP_VFS_MKDIR`. |
| `path.symlink` — `ops::SYMLINK`, `ops::SYMLINKAT` | `vfs_symlink`, `vfs_symlink_resolved`, `vfs_symlink_resolved_beneath`, `vfs_symlinkat` | Retained for task-aware symlink and owner-beneath symlink. The target is data; the link pathname is the retained destination binding. | Validate destination parent/name and commit once. Remote owner separately bounds target bytes and admits the link name. |
| `path.remove` — `ops::UNLINK`, `ops::RMDIR`, `ops::UNLINKAT` | `vfs_unlink`, `vfs_unlink_resolved`, `vfs_unlink_resolved_beneath`, `vfs_rmdir`, `vfs_unlinkat` | Retained for task-aware local operations and checked owner-beneath adapters. | Detach the validated parent/name under publication plus backend namespace lock, retaining open objects by backend lifetime rules. |
| `path.rename` — `ops::RENAME`, `ops::RENAMEAT` | `vfs_rename`, `vfs_rename_resolved`, `vfs_rename_resolved_beneath`, `vfs_renameat` | Source and destination handles are both retained and validated before the namespace change. | Reject cross-mount rename and publish source removal plus destination replacement as one backend transaction/serialized compatibility commit. |
| `path.link` — `ops::LINK`, `ops::LINKAT` | `vfs_link`, `vfs_linkat` | Source object and destination parent/name are retained. | Validate both handles under deterministic lock order and commit atomically. Remote remains `-EOPNOTSUPP` because no two-path opcode exists. |
| `path.mode` — `ops::CHMOD`, `ops::FCHMODAT` | `vfs_chmod`, `vfs_chmod_resolved`, `vfs_chmod_resolved_beneath`, `vfs_fchmodat` | Retained for task-aware and checked owner-beneath operations; fd-only chmod uses a retained `File`. | Validate the final object and update its metadata under the backend lock. |
| `path.owner` — `ops::CHOWN`, `ops::FCHOWNAT` | `vfs_chown`, `vfs_fchownat` | Retained for task-aware pathname chown; fd-only chown uses a retained `File`. XFS/remote keep their existing no-op compatibility behavior. | Validate final object and backend metadata generation at commit. |
| `path.time` — `ops::UTIMENSAT` | `vfs_utimens_resolved_beneath`, `vfs_utimensat` | Task-aware operations use retained handles; the owner-beneath form holds publication across confined resolve and commit. | Validate final object then update timestamps. |
| `path.xattr` — `ops::SETXATTR`, `ops::LSETXATTR`, `ops::GETXATTR`, `ops::LGETXATTR`, `ops::LISTXATTR`, `ops::LLISTXATTR`, `ops::REMOVEXATTR`, `ops::LREMOVEXATTR` | `vfs_setxattr`, `vfs_setxattr_beneath`, `vfs_getxattr`, `vfs_getxattr_beneath`, `vfs_listxattr`, `vfs_listxattr_beneath`, `vfs_removexattr`, `vfs_removexattr_beneath` | Task-aware XFS pathname xattrs use retained handles; beneath variants hold publication across confined resolve and commit. fd/`File` variants use retained files. | Consume one retained final object or one publication-serialized owner binding. |
| `path.chdir` — `ops::CHDIR` | `vfs_chdir` | Task-aware chdir consumes a retained directory handle; fd-only fchdir consumes a retained `File`. | Validate the directory handle and publish the logical task cwd while that binding is valid. Remote cwd text may name a remote route, but is not treated as an owner object capability. |
| `path.mount` — `ops::MOUNT`, `ops::UMOUNT`, `ops::PIVOT_ROOT` | `vfs_mount`, `vfs_umount`, `vfs_pivot_root`, `vfs_shutdown_unmount_all` | Mount targets consume retained lookup handles. Filesystem/device or remote-proxy initialization uses a reserved two-phase `PreparedMount`; only its table insertion occurs in the handle consumer. Unmount captures the exact retained mount/dev-id token, then uses the separate mount-topology retirement transaction; pivot/shutdown use that lifecycle transaction directly. | Serialize table publication; mark retirement before removal, exclude new refs, drain refs/open files without holding VFS/backend locks, then teardown. No namespace/backend lock crosses remote-device initialization, WKI attach/notification, or teardown I/O. |
| `path.batch` — `ops::METADATA_BATCH` | `vfs_metadata_batch` | The client preflights the complete bounded path set and retains one remote mount before sending. The owner preflights exact framing/confinement and each item consumes the corresponding checked beneath-export handle; no scalar fallback occurs after a mutation request is attempted. | The existing owner batch boundary and per-item status ABI are preserved. `-EOPNOTSUPP` is fallback-safe only before any request/effect; each owner mutation is independently linearized by its retained handle. |
| `path.resolve-helper` — no direct syscall | `vfs_resolve_dirfd` | Resolution-only compatibility helper. It returns text, not authority; callers that later touch namespace state must acquire a handle or retained `File`. | No commit linearization. Never carry its returned string across an unlocked remote or local lookup/act gap. |
| `path.route-policy` — `ops::WKI_RULE_ADD` | `vfs_wki_rule_add`, `vfs_wki_rule_get`, `vfs_wki_default_rule_get`, `vfs_wki_effective_route_for_path` | Policy-prefix management/query exception; these strings select routing and are not filesystem object lookups. | Serialize in the WKI rule store. The selected route is input to later handle acquisition, never proof of an owner object. |
| `path.cache-observer` — no direct syscall | `vfs_cache_notify_invalidate_path`, `vfs_cache_notify_path_changed` | Post-commit observer exception. These APIs invalidate hints and must never perform or authorize a filesystem operation. | Call after/within the operation's publication protocol as required; cache epochs and mount generation make stale hints miss. There is no remote owner fallback—the owner operation sends invalidation only after commit. |
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
