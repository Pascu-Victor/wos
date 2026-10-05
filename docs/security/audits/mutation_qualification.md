# Goal19 9MM: namespace mutation authorization

The `vfs-mutation` domain is enforced for supported mkdir, symlink, unlink,
rmdir, hardlink, rename and remote metadata-batch namespace operations. The
review includes public syscall helpers, explicit confined owner APIs, native
commit functions and mapped WKI dispatch. It incorporates the 9ML `linkat`
Snapshot correction. This qualification changes evidence and status only.

## Admission and commit

| Surface | Current contract and evidence |
| --- | --- |
| Public requests | `vfs_mkdirat`, `vfs_symlinkat`, `vfs_unlinkat`, `vfs_renameat`, `vfs_linkat` and `vfs_metadata_batch` retain the admitted Snapshot. Both rename/link paths and bounded namespace retries use it. Current KTEST includes public TMPFS/XFS linkat credential replacement and retry, and rename owner/pair admission tests. Normal task wrappers delegate to these helpers; taskless resolved calls serve trusted kernel work. |
| Native lookup lifetime | Namespace publication revalidates retained identities, generations and directory authority before native callbacks. TMPFS uses `tmpfs_lock`; XFS uses its metadata guard; FAT uses its sleeping mount lock. Selected readonly policy reaches native mutation gates, including both sides of retained rename/link. Traversal/root/cwd view authority was separately qualified in 9MD. |
| TMPFS | Central create policy prepares UID, GID and mode before publication. Removal checks the actual canonical victim and parent/sticky policy. Protected hardlinks check source identity and access; rename checks both parents, displaced identity and moved-directory authority before detachment. Structural refusal and same-inode no-op behavior remain explicit. Current denial tests verify bindings, ownership, metadata and outputs; rename reserves the destination slot before detaching entries. |
| XFS | Retained lookup validation and central parent/object policy precede transaction allocation and namespace writes for mkdir, symlink, unlink, rmdir, link and rename. Rename performs type, ancestry, emptiness and link-count checks before its transaction. Current mapped namespace and linkat tests run on XFS; live matrices verify allowed and denied state. An authorized transaction's later device/cache error is not a promise of filesystem-wide rollback. |
| FAT and synthetic backends | FAT removal and rename use declared root-owned synthetic metadata and central policy before writes. Current FAT tests cover denial without directory/cluster writes, rename preflight, prepared-write rollback and retained open nodes. FAT mkdir/hardlink/symlink and synthetic filesystem namespace mutations retain their existing unsupported results. DEVFS/PROCFS mkdir probes run outside publication and revalidate before refusal; current tests assert no device callback or namespace effect. |
| Remote scalar mutations | A canonical credential record and required feature precede mutation on one exact binding. Receiver policy requires VFS_WRITE and explicit shared-identity trust, then maps one Snapshot into confined native calls. It retains exact directory Files outside the registry lock; cross-lane rename also retains matching sibling bindings. Legacy callerless mutations refuse. No mutating fallback or ambiguous replay is added. |
| Remote batch | Syscall input copying and complete bounded-path preflight precede submission. Every chunk uses the same Snapshot; receiver preflight and identity mapping precede per-item native calls. Results remain per-item: an allowed earlier item need not roll back because a later item is denied. Unsupported feature/input refusal occurs before submission; readonly batches refuse every item. Current batch fixtures verify state, ownership, denial and bounds. |
| Routing and readonly views | Declared VFS routing remains authoritative, including explicit LOCAL `/` selecting the receiver root. Confined absolute-symlink interpretation and retained directory identity have current TMPFS/XFS KTEST coverage. Client readonly gates precede mutation submission; imported directory policy also reaches owner native commit. Current readonly tests cover pathname, dirfd and batch requests and preserve writable controls. |

File-content writes, truncation, mapping writeback and xattrs have separate
description/metadata policy evidence; historical cross-cutting entries in this
row do not replace the still-pending `vfs-metadata` review. Mount administration
and general WKI control remain separate domains.

## Current verification

The current normal kernel is `1e1de786812557794e9661a99cea9355a44e53cf`
(SHA256 `fa467381e6455b638040767bd732d8a6cfb1cb5c7f78fbcc1208748aca21664f`).
The current diagnostic kernel is `063974c219e7af8108294d04ce4545faa626b467`.
Both were built successfully in 9ML in this task. Its full isolated KTEST passed
59,790 checks with zero failures and shut down. The qualification references
those exact unchanged artifacts and current PASS records, without claiming a
second build or KTEST run.

A new private two-node cluster booted the normal kernel from separately packaged
boot disks and private overlays. Existing functional fixtures passed on each of
TMPFS and XFS: mkdir 192, symlink 576, removal 512, rename 1,536 and batch 643,
for **6,918 checks**. The remote readonly fixture additionally passed **99 profile
checks**, writable controls and proxy retirement checks. All fixture cleanup
completed. Both guests synced, private boot backing chains were verified and the
exact owned QEMU processes acknowledged quit. PIDs 1794308 and 1794344 were
confirmed absent, with no remaining QEMU processes, at
`2026-09-16T07:34:29.253631+00:00`.

Current credential/lookup/WKI host models, lookup/mount and WKI source suites,
policy audit and 9ML formatting/tidy checks pass. Receipts, selected KTEST
records, fixture/source hashes and the manifest transition are retained in
`build/goal19-mutation-qualification/validation.json`. Historical mixed-version
or selector-disabled runs remain historical; no new such run or packet campaign
is claimed. Earlier row text saying traversal, cwd transfer or mapped scalar
mutation was unimplemented was stale and is superseded by this review and 9MD.

The manifest retains all 21 domains, now **15 enforced / 6 pending**. The complete
gate next stops at `vfs-metadata`. The original goal and `--no-setup` authorization
note remain unchanged. Goal19 remains active.
