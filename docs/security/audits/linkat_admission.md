# Goal19 9ML: one admitted identity for linkat

`vfs_linkat` now captures its credential Snapshot once, before either lookup or
the bounded retry loop. Both path lookups and the namespace-publication commit
use that value. Previously each lookup/retry could capture a later identity and
the pair commit used current-task credential validation. This was a source-backed
consistency gap; no old-kernel runtime failure is claimed.

The existing namespace publication mutex, retained source/destination identities,
mount generations, directory-view authority, combined readonly policy, native
backend locks, protected-hardlink policy and errno remain in place. The change
does not add remote hardlinks, alter the syscall/wire ABI, or replace declared
VFS routing. An explicit LOCAL `/` rule still selects the receiver root.

The actual public `vfs_linkat` helper is tested on TMPFS and XFS. A selftest-only
hook changes a private Task's credentials after the source lookup and changes
the namespace to force revalidation/retry. An initially unprivileged request is
still refused after becoming root; an initially admitted root request still
succeeds after losing that identity. Denial preserves the source inode, size,
link count and ctime and leaves the destination absent. A fresh request observes
the replacement identity. The hook runs outside namespace publication and is
absent from the normal kernel.

Normal Build WOS and the isolated diagnostic build passed. Formatting and
clang-tidy, the policy and lookup source checks, and the credential, lookup and
WKI policy host models passed. The full current diagnostic KTEST passed 59,790
checks with zero failures at 481.329 seconds, powered off, and returned launcher
status 0. Both new Snapshot/retry cases passed. PID 1791869 was confirmed absent,
with no QEMU processes remaining, at `2026-09-16T07:29:22.523464+00:00`.

Normal kernel build ID: `1e1de786812557794e9661a99cea9355a44e53cf`.
Diagnostic kernel build ID: `063974c219e7af8108294d04ce4545faa626b467`.
Exact commands, hashes, logs, before copies, scoped diff and validation receipt
are in `build/goal19-linkat-admission/`. This stage has no multi-node runtime
claim. Mutation qualification remains separate: 14 enforced / 7 pending domains,
with `vfs-mutation` first pending. Goal19 remains active.
