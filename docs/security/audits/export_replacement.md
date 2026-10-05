# Goal 19 ordinary export replacement

9LY reproduced an enabled resource-lifetime gap on unchanged 9LX kernel
`44568576e16e80d8b04b103fd486eb5feb9a0471`. It does not close traversal,
mount or WKI qualification, and it does not establish a UID/DAC bypass.

## Observed behavior

Two task-owned nodes used private overlays and `wos-cluster --no-setup` with
existing topology and same-domain-root mapping. The owner created an owned
`/tmp/g19-replace-*` directory containing `UNDER`, mounted TMPFS on it and
created a separate `OLD` marker. Its automatic peer mount read `OLD`.

Ordinary owner `umount` succeeded. The exact automatic peer mount row remained,
and a new read through it returned `UNDER`. A second TMPFS at the same owner
path then held `NEW`; the same peer mount returned `NEW`. Serial evidence shows
one initial export (resource4104) and its original eight channel attaches,
with no replacement export for that path. All data Files were closed between
steps. No forced detach, packet manipulation or credential bypass was used.

A preceding probe of only unmount/underlying fallback independently observed
`OLD` then `UNDER`. The first observation attempt had timed out before reading
the baseline: SFTP returned empty `/proc/mounts` data. It ended and cleaned up;
the corrected driver uses WOS `cat` through the local route. Its failed log and
original driver are retained. The observation driver exits zero when it records
and cleans up successfully; this is a reproduced defect, not a passing
stale-binding regression.

Cleanup removed all scratch markers, directories and helpers on both nodes and
synced both nodes. Stale automatic peer mount rows remained until VM shutdown;
we do not describe those rows as having been withdrawn by cleanup. Initial and
final live build IDs/symbols matched, debugger sessions closed, and the owned
launcher exited zero. Host receipt at 2026-09-16T01:59:20.776753+00:00 found all
three owned processes absent and no WOS VM. No kernel code changed, no new WOS
build or KTEST run was needed; both existing kernel artifact hashes were checked.

## Source flow and impact

- `vfs_umount` admits one Snapshot, captures an exact mount dev ID under
  namespace publication, drops its references/guard and invokes exact-ID
  unmount. It currently does not prepare export reconciliation.
- `unmount_filesystem_impl` marks the matching row retiring and performs
  backend teardown outside the mount lock. Busy/failed teardown restores the
  old mount. Success emits a storage event and requests advertisement.
- Advertisement calls `wki_remote_vfs_refresh_exports`; discovery adds missing
  visible export names. It neither removes a withdrawn name nor replaces an
  existing name whose backing mount changed.
- `DevServerBinding` captures resource incarnation, backing dev ID and export
  path/name. Ordinary pathname worker dispatch passes captured path/name to
  confined lookup, which can select the now-different filesystem at that text.
- Pivot already closes VFS RX admission, drains retained binding users,
  reconciles exact export identity, retires unmatched bindings and publishes
  withdrawal records before reopening. Ordinary native unmount lacks this
  transition. Remote proxy unmount has separate exact-group teardown and
  must not enter a drain that waits for its own WKI worker.

## Bounded next implementation

Reuse the existing split-phase export transition around the public native
unmount commit: capture the stable target/dev ID/type; prepare outside all
namespace/mount locks; unmount only the captured mount; rebuild/withdraw on
success or cancel/reconcile the unchanged exports on refusal. Keep remote
proxy teardown separate. Check normal internal and shutdown callers before
placing the guard; do not presume every mount teardown is a public syscall.

The transition must preserve unrelated bindings and stored Files on EBUSY or
backend failure, retire the removed backing incarnation before reopening, and
permit a fresh mount/export to receive a fresh identity. Keep readonly/nosuid,
current mapped Snapshot, retained directories, declared LOCAL `/`, error
precedence and mount dev-ID matching. A path-stat check performed before an
operation is insufficient: it leaves a mount-change race. Permanently pinning
every attached mount would instead prevent ordinary unmount indefinitely.

Verification needs a deterministic native commit/cancel test, the unchanged
ordinary baseline with explicit old-binding retirement assertions, busy-unmount
preservation, fresh-export reattachment, and prior traversal/mount-cover tests.
A read through a genuinely fresh broader `/tmp` export is allowed after old-row
withdrawal; the assertion must distinguish that from using the old exact row.
Normal Build WOS, isolated KTEST and source/format checks must accompany the
production change. The underlying auxiliary ENOENT from 9LW is still a separate
unqualified cause.

Evidence: `build/goal19-export-replacement/validation.json`, `probe.log`,
`observations.json`, `underlying-only/`, `first-observation-failure/`, both serial
logs, live symbol receipts, `cleanup.json` and `down.json`.


9LZ resolves this demonstrated stale native-unmount binding through exact backing
retirement. The final ordinary fixture verifies20 assertions including busy-held
File preservation, exact old-row withdrawal and fresh replacement admission.
Broader traversal validation has one retained-directory timeout; see
[native unmount results and limitations](native_unmount_exports.md).
