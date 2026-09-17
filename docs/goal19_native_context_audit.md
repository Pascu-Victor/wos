# Goal19 native retained-context audit (9MB)

The reviewed TMPFS/XFS retained root/cwd paths preserve object identity,
request credentials and inherited restrictions through lookup consumption.
This closes the native-context source reconciliation within the traversal
checklist. It does not close mapped binding concurrency, automatic mount
lifecycle or the complete traversal domain. Declared VFS routing, including an
explicit LOCAL `/` selecting the receiver root, is preserved.

## Source and authority flow

| Surface | Entrypoints and observed contract |
| --- | --- |
| Retained ownership | `vfs/directory_view.hpp`, `directory_anchor.hpp`, `filesystem_context.hpp`, `lookup.hpp` and `lookup.cpp` retain typed root/cwd Files, mounts and membership references. Display paths do not replace object authority. |
| Task capture/publication | `Task::capture_filesystem_context` copies root/cwd and both anchors under the canonical filesystem owner's path lock, replacing the output after unlock. `change_directory_at` consumes the admitted lookup, checks target search, credentials and root/cwd identities, then swaps cwd ownership under that lock. Rejected/replaced ownership retires outside the guards. |
| Component traversal | `acquire_local_component_lookup`, `local_walk_bind` and `local_walk_cross_native_mount` in `vfs/core.cpp` check search before consuming raw components, retain actual covered directories at mount crossings and preserve root confinement. A hidden covered directory is an attachment proof, not a renewed search grant. |
| Membership | `NativeDirectoryMembershipReferences`, `validate_native_mount_attachment` and `directory_member_path_locked` retain exact child/parent mounts and covered Files. Frozen lookup membership cannot add missing edges while consuming a lookup. Changed epochs or attachments fail before consumption. |
| Context preparation | `prepare_directory_view`, `vfs_prepare_filesystem_context` and `vfs_prepare_global_cwd` stage owned candidates. Root/cwd membership comes from native objects; source and process restrictions intersect. Failure preserves the previous output. Prepared epochs alone are not installable authority. |
| Consumption | `consume_lookup_locked` revalidates request context, namespace/mount/search epochs, exact current mount, backend lookup and retained view before calling the consumer. Membership covers the selected directory or the parent of a regular/missing final component. |
| Native backends | `tmpfs_validate_lookup` checks parent/node incarnation, directory generation and actual entry identity. `xfs_validate_lookup` checks mount/parent sequence and authoritative directory entry under its metadata guard. |
| Covered mount lifetime | `publish_prepared_mount` and `capture_mount_covered_directory` in `vfs/mount.cpp` retain exact native covered objects under the mount lock; failed publication does not replace an existing mount. |

Namespace publication encloses validation/consumption. Short path locks protect
task metadata, mount locks protect table/attachment state, and backend locks
protect directory identity. Capturing membership may allocate; borrowing frozen
membership cannot. Owning references outlive validation guards so final closes
need not run under publication/path/registry locks.

Unsupported scoped backends refuse with the applicable EXDEV/ENOTSUP path.
Missing covered-directory proof is not silently reconstructed from text.
An owned root that cannot use the component walker cannot fall back to the
compatibility pathname walker. Confined owner traversal refuses PROCFS rather
than borrowing the serving worker's process identity. These are narrower
contracts than ordinary unscoped compatibility lookup, which remains a separate
surface.

The legacy `vfs_consume_lookup` adapter has current-task/bootstrap semantics;
explicit task/snapshot consumers use `lookup_request_context_matches` instead.
Its remaining compatibility callers are not qualified as transferable authority
by this audit. The mapped binding review must follow actual handler admission
and explicit `_as` calls, not infer safety from the adapter's name.

## Evidence and correction

`build/goal19-native-context-audit/validation.json` indexes this review.
The isolated diagnostic build completed successfully with no compilation work
after configuration. All eleven inspected source hashes and both kernel hashes
remain unchanged. No Normal Build WOS, packaging, VM launch or KTEST rerun was
performed in 9MB.

`prior-ktest-evidence.json` records source log line numbers and the log hash for
29 native-context PASS records from the final 9LZ run, including independent
root/cwd, covered-directory ownership, search revocation, retained view bounds,
mixed native mount membership, typed anchors, cwd recovery and scoped walks.
That diagnostic binary's full run was 59,759 passed / 0 failed. This is explicitly
prior runtime evidence, tied to the unchanged binary, not a new run.

Inspected assertions include unchanged output on rejected preparation, zero
consumer calls on stale membership/search authority, exact-object behavior after
rename and same-text replacement, restriction intersection, bounded-reference
reuse and close hooks outside publication/path/mount locks. They cover specific
forced transitions, not every possible concurrent schedule.

The mount lifetime source check initially failed because it still expected
`unmount_filesystem_if_dev_id(target, expected_dev_id)`. It now expects the
bounded copied `stable_target.data()` introduced in 9LZ. Exact mount matching,
dev-ID selection and ordering requirements remain intact. The initial failure
is archived. Lookup, mount lifetime, remote VFS and credential policy source
checks all pass after that correction. No production source changed in 9MB.

## Remaining traversal work

Follow mapped request admission through authenticated binding acquisition,
snapshot mapping, retained directory leases, cancellation/rotation and final
owner consumption. Reconcile internal automatic mount and mixed-attachment
lifecycle separately from public native mount/unmount. Keep the prior 9LZ
directory timeout and 9MA unsuccessful recording attempts visible; successful
later observations do not establish their cause or a fix.

Goal19 remains active, with 12 enforced and 9 pending manifest domains.
