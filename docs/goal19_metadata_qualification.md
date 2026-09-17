# Goal19 9MN: metadata authorization qualification

The `vfs-metadata` domain is enforced for supported pathname and retained-File
chmod, chown, timestamps and xattrs, with the separate established-description
rule for truncation and data writes. This is an evidence/status qualification of
the unchanged 9ML source and kernels. No production, ABI or wire change is made.

## Reviewed contracts

| Surface | Admission and preserved behavior |
| --- | --- |
| Pathname requests | Public chmod/chown/utimens/xattr paths capture one Snapshot before lookup retries and pass it to explicit native consumption. Confined owner adapters likewise retain the supplied mapped identity, root/cwd context and directory File. Namespace publication validates retained lookup identity. Remote mutation status returns directly without retrying an ambiguous operation. Current `MetadataRetryKeepsCallerAcrossCredentialPublication` and explicit-owner KTESTs cover this distinction. |
| Chmod/chown | Central prepare functions decide owner/group/FILE_OWNER authority, unchanged-ID sentinels, setgid restrictions and existing set-ID clearing before backend commit. TMPFS canonical inode locking, DEVFS selected-node metadata locking and XFS metadata transactions preserve denied identity, mode, ctime and output. Directory authority changes invalidate prior search admissions. |
| Timestamps | Copied request times distinguish NOW, explicit values and OMIT. Policy admits explicit changes for the owner/FILE_OWNER and NOW through the established writable-mode rule. Both-OMIT preserves timestamps and still follows descriptor/backend validation. Owner calls resolve NOW using the owner clock. Existing backend-specific readonly/no-op errno is preserved, including XFS's readonly refusal. |
| Retained Files | FD helpers capture the current caller once and retain the selected File through native/mapped dispatch, then release it outside namespace publication. Rename, unlink and FD reuse cannot redirect metadata to a replacement pathname. TMPFS fops validation rejects pipes before inode access. Metadata authority is checked for the current caller; an old writable data grant does not confer ownership authority. |
| Backend scope | Mutable TMPFS/XFS/DEVFS metadata uses central admission and retained readonly policy. DEVFS updates do not invoke device callbacks. FAT authorizes against its fixed root-owned projection and only reports ownership/mode success when representable; timestamp mutation remains unsupported. PROCFS/socket and unsupported metadata operations retain refusal behavior. Native xattrs are supported on XFS; other backends refuse. |
| Xattrs | XFS checks current-caller DAC for user.* and FILE_OWNER for trusted/security namespaces before transactions; listing filters privileged namespaces. Public symlink write restrictions and size/name/flag checks remain. Explicit owner APIs never substitute worker credentials. WKI permits its declared user.* surface, maps canonical BEGIN credentials before stage allocation, retains exact Files and mapped Snapshot, checks complete staged input before commit and retains terminal operation identity. No mutation replay follows an uncertain response. |
| Remote metadata | Path and File selectors require supported resource/features and one exact binding. RX-admitted VFS_WRITE plus explicit identity trust maps the supplied record before exact directory/File retention. Native callbacks and final File releases run outside the registry lock. Unsupported legacy operations refuse. Client immutable readonly policy rejects changes before submission; owner retained views also constrain commit. |
| Data grants and mappings | Truncate and direct/bounced/positioned writes check the retained File's admitted access and readonly view before effects. Truncate does not reauthorize a changed pathname. Mapping admission and mprotect use retained File grants; writeback preflights full offset intervals and all selected dirty ranges before backend writes. The existing 9IH/9II policy remains, with current KTEST and native mapping/truncate regressions. Authorized I/O failures do not promise rollback of completed data writes. |

Declared VFS routing remains unchanged: explicit LOCAL `/` still selects the
receiver root. Existing inherited directory views and ownership transfer were
qualified in 9MD; general mount administration and WKI control remain separate.
The old manifest text describing absent remote chown/descriptor metadata and
client readonly gates was stale; those implementations and their current tests
are now included rather than left as unresolved prerequisites.

## Current verification

This review uses the exact current 9ML normal kernel
`1e1de786812557794e9661a99cea9355a44e53cf` and diagnostic kernel
`063974c219e7af8108294d04ce4545faa626b467`. Normal Build WOS and the isolated
build passed in this task; the full diagnostic KTEST passed **59,790 / 0** and
shut down. Current central credential/lookup/WKI models, relevant source checks
and formatting/tidy receipts are referenced with unchanged source hashes; no
additional build or KTEST run is claimed for this status change.

Fresh private two-node VMs passed these existing functional fixtures:

| Checks | Count |
| --- | ---: |
| Mapped chmod on TMPFS/XFS | 96 |
| Mapped pathname chown on TMPFS/XFS | 92 |
| Mapped timestamps on TMPFS/XFS | 576 |
| Mapped retained-File metadata on TMPFS/XFS | 154 |
| Mapped XFS xattrs, including retained-cwd regression | 294 |
| Native XFS xattr permissions | 244 |
| Local and remote TMPFS/XFS truncate grants | 504 |
| Native readonly metadata | 96 |
| Native TMPFS/XFS mapping policy | 72 |
| **Total** | **2,128** |

All fixture cleanup completed. Both guests synced; their private boot backing
chains were verified and exact owned QEMU processes acknowledged quit.
PIDs 1796796 and 1796821 were confirmed absent, with no QEMU processes remaining,
at `2026-09-16T07:43:34.212664+00:00`. Current-task 9MM additionally supplies the
unchanged-kernel remote readonly profiles; those are identified as reused
receipts. Historical selector-disabled runs are not new evidence, and no packet
campaign was performed.

Commands, results, source/fixture hashes, selected current KTEST records and the
status transition are in `build/goal19-metadata-qualification/validation.json`.
All 21 domains remain: **16 enforced / 5 pending**, with `mount` first pending.
The original goal and its `--no-setup` authorization remain unchanged. Goal19
remains active.
