# Goal 19 VFS traversal audit

The current traversal qualification is [9MD](traversal_qualification.md).
The traversal domain is enforced; Goal19 has13 enforced and8 pending domains.
The records below retain their historical status and failed observations. This document supersedes
old manifest statements that directory PIN/PATH transport and remote cwd
integration are still unimplemented. Those implementations and their tests
exist in the current tree. Broader operation qualification remains separate.

## Current call flow

| Surface | Current authority path | Remaining qualification |
| --- | --- | --- |
| Native pathname entrypoints | Task adapters capture a `Snapshot`; `vfs_acquire_lookup` and the component walker retain root/cwd, mount, backend objects and search generations. Original components, including ancestors before `..`, receive search checks. | 9MB reconciles retained native TMPFS/XFS root/cwd and scoped unsupported-backend refusal. 9MD reconciles internal mount producer lifecycle. |
| Native consumption | `consume_lookup_locked` validates namespace, root/cwd, mount, retained objects and search generations before the operation. Blocking remote RPC and final remote File release remain outside namespace publication. | General VFS mutation/open/metadata/mount rows have separate commit obligations. |
| Declared routing | Raw and canonical route destinations are compared; unsupported disagreement is refused. The raw mapped remainder is preserved. Explicit LOCAL `/` on a receiver selects that receiver's root. | 9MD reconciles automatic publication, mixed attachments and exact-owner cleanup. |
| Owner traversal | Authenticated operation policy maps the admitted snapshot before confined owner traversal. Relative operations retain the directory channel grant. Owner traversal cannot silently use the worker's credentials or follow another REMOTE mount. | Confined PROCFS is refused because the worker PID is not the remote caller. Assess actual refusal, not an imagined enabled path. |
| Remote cwd and names | Search-only directory PIN/PATH, cancellation, Task cwd lifetime and REALPATH_AT are implemented. Native views and mapped bindings retain object identity; current callers supply new child-search authority. Name output is staged and context is revalidated. | 9MB/9MC reconcile native root/cwd and mapped admission, exact ownership and final claim/cancellation/retirement. Unsupported transfers refuse;9MD reconciles internal mount lifecycle. |
| ACCESS/FACCESSAT | 9LV selects real UID/GID by default and effective IDs for `AT_EACCESS`, through ancestor and final checks, PROCFS, mapped stat/access and empty-FD checks. | This fixes the confirmed identity gap; it does not complete the entire traversal domain. |

Relevant entrypoints are in `modules/kern/src/vfs/{core,lookup,mount}.cpp`,
`vfs/{directory_view,filesystem_context}.hpp`, native filesystem adapters,
`net/wki/{remote_vfs,dev_server,auth_policy}.cpp`, and the VFS syscall adapter.
The main retained objects are `LookupHandle`, `DirectoryViewRef`, `MountRef`
and the remote directory `File`/channel binding. Namespace publication precedes
native backend validation; Task path locks protect context observations.
Remote waits and potentially blocking last-reference closes cannot cross that
publication lock. No new lock, allocation path or wire format is introduced
by 9LV.

## Confirmed ACCESS gap and correction

The unchanged 9LU kernel returned the wrong real/effective result in all eight
identity checks: real1002/effective0 callers incorrectly passed default access,
real0/effective1002 callers incorrectly failed it, and all `AT_EACCESS` calls
returned EINVAL. Two ordinary effective-ID open controls passed. Files and
ancestors were owned fixture objects; both opposite identity profiles used
WOS's supported set-ID exec/seteuid/setegid interfaces.

`Snapshot::for_real_id_access()` copies the single admitted caller snapshot and
selects its real UID/GID in the effective decision fields. Saved IDs,
supplementary groups, authority ceiling, service grants, NNP and restrictions
are preserved. The projection is never published as process credentials.
Native consumption receives that same explicit projection, with normal
root/cwd/search/mount validation. Effective-ID access uses the original
snapshot. Existing WOS access-mode masking and errno are preserved.

The real-ID rule includes ancestor traversal, as specified by
[the Open Group access/faccessat contract](https://pubs.opengroup.org/onlinepubs/9699919799/functions/access.html).
The implementation keeps WOS's authority ceiling and no-regain restrictions;
it does not grant discarded authority merely because a real ID is zero.

9LU first setup attempts used unsupported setres*/setre* calls and failed
with ENOSYS before checking permissions; they are not identity evidence.
9LV's first native-test build omitted a required open-mode argument and failed
compilation. Its first diagnostic candidate additionally rejected access-mode
bits that existing WOS tests require masking: that complete run had 59,733
passed and 24 failed assertions, all at the preexisting PROCFS mode check.
Both new identity test groups passed. The unrelated mode rejection was removed;
no identity assertion was relaxed. All failed attempts and original kernels
remain archived.

## Evidence and limits

- 9LU: 11 existing traversal fixture runs passed 1,568 checks on unchanged
  kernel `46f608cce52899aad75757b707ccc548d2d4ac24`. The separate ACCESS
  baseline failed eight identity checks and passed two open controls.
- 9LV: the new fixture passed all 40 checks over native/mapped TMPFS/XFS.
  The same 11 established fixtures passed all 1,568 checks. Both nodes were
  verified against normal kernel `39370c03f20ce569e587b8b0e5a7d300e02b476f`.
  Fixture inventories were restored, both nodes synced, debugger sessions
  closed and the exact owned cluster processes stopped.
- The host credential suite passed 63/63 with sanitizers enabled. Its first
  sandbox run reached 63/63 but exited on LeakSanitizer's tracer limitation;
  the unsandboxed rerun passed. Normal and isolated diagnostic builds passed,
  as did scoped formatting and clang-tidy (existing nonfatal diagnostics remain).
  Final isolated KTEST passed 59,757 assertions with zero failures at
  463.891 seconds and powered off at 463.892. The launcher exited zero;
  host verification at 2026-09-16T01:04:19.748365+00:00 found both owned
  processes absent and no WOS VM. Diagnostic kernel build ID is
  `06481fadbfc860b6d8910b3f2dd4eb28852e0ab5`.

9LU observed a remote-prefix/local-overmount selection question, but the public
mount path rejects REMOTE targets via retained consumption with EXDEV. No
user-reachable authority bypass is established by that source observation.
Internal mount producers remain the next concrete audit item. General
retained-root, automatic-mount and mapped concurrency reconciliation remains
open. These bounded fixtures do not close all VFS or WKI domains.

Artifacts: `build/goal19-vfs-traversal-audit/validation.json` and
`build/goal19-access-identity/`. Live runs used `--no-setup`, private overlays
and existing topology.


## 9LW/9LX mixed mounts and publication

The supported native-child then remote-parent layout routes ordinary paths to
the peer while retained native directory FDs keep their original objects and
permission checks. Attempting to mount over the occupied child returns EBUSY;
a distinct fresh target on the remote parent returns EXDEV. An initial fixture
confused those two targets; its failed run and errno probe are retained. The
corrected fixture tests both errors independently, with all 24 checks passing.

The baseline also exposed a real late-publication failure: auxiliary attachment
failed after the parent mount was visible, and broad rollback could retire
unrelated groups. 9LX prepares transport lanes before VFS publication and
retires only its candidate group on failure. The native regression verifies
same-resource isolation and native-child preservation. Normal/diagnostic builds,
full KTEST (59,758/0), and 1,568 established live traversal checks pass.
See [the publication audit](remote_mount_publication.md). General retained
root, automatic-mount and mapped concurrency reconciliation remains open;
this bounded correction does not promote the traversal domain.


## 9LY concrete remaining lifecycle gap

The [ordinary export replacement baseline](export_replacement.md)
replaces the next broad automatic-mount audit item with a reproduced behavior:
after a native owner mount is removed, its old exact peer row can read both the
underlying directory and a new filesystem mounted at the same path. Pivot has
export reconciliation; public native unmount currently lacks it. Fix and verify
that exact lifetime transition before promoting traversal. The probe preserves
normal identity policy and declared routing and is not evidence of a DAC bypass.


## 9LZ native unmount export retirement

Public native unmount now closes and drains existing WKI VFS admission before
exact backing-dev-ID removal. Success withdraws only exports of that backing;
failed/busy removal cancels the transition and preserves matching issued Files
and bindings. Unrelated custom aliases are not rediscovered through the caller
root. The target is copied before blocking, existing empty-target errno is
preserved, and declared LOCAL `/` routing remains unchanged. Cancellation retains
the existing revision advance, so pretransition snapshots require reacquisition.
No syscall/wire/credential layout changes or locks held across the drain.

Final Normal Build WOS and isolated diagnostic builds passed, as did formatting,
clang-tidy and focused remote-VFS/lookup source checks. On normal kernel
06b3301d14b88ab8b1936c6fe9492989607172d9, all20 new unmount assertions and
all24 existing mount assertions pass. The old exact peer row disappears and
replacement obtains resource4104 after old4103. All1568 traversal assertions
were executed:1567pass,1fails with ETIMEDOUT in retained-directory STAT_AT
(phase1,grouped1,path `.`,op2). The failure is preserved; only the six previously
unexecuted cwd cases were subsequently run. No green rerun replaces it.
WOSDBG reconstructs two client timeouts from both node logs but cannot prove
cross-node order or the missing reply's cause. Response send status is discarded
in the owner handler; that is an investigation lead, not a demonstrated cause.

All fixture files are removed, both nodes synced, initial/final live symbols
match, debugger sessions close and launcher82298 exits0. Host ownership audit
at2026-09-16T02:36:51.071635+00:00 confirms no WOS VM before isolated KTEST.
Final isolated diagnostic KTEST passes59759/0 at466.521s, including the native busy/cancel/commit/custom-alias regression; automatic poweroff and launcher exit0. Host audit at2026-09-16T02:57:27.660620+00:00 confirms all owned VMs down.
Earlier full-rebuild and pre-empty-errno candidates, their runs, a fixture
SFTPAttributes observation error, and the interrupted superseded KTEST remain
archived separately. No superseded run qualifies the final diagnostic kernel.

See [native unmount review](native_unmount_exports.md) and
`build/goal19-native-unmount/validation.json`. Goal remains active,12enforced/
9pending; vfs-traversal is first pending. This bounded lifetime fix does not
qualify broader traversal, transfer/rotation, mapped concurrency or timeout
behavior. All live runs use --no-setup and isolated artifacts.


## 9MA retained-directory timeout observation

The unchanged 9LZ normal kernel passed the same 20 unmount, 24 mount-cover and
1,568 traversal assertions on fresh private nodes. Ten additional sequential
retained-directory fixtures passed 2,240 assertions. The earlier timeout did not
recur and remains unresolved; no production timeout fix is claimed.

Initial perf recordings were saved, but structured conversion failed at the
legacy size limit, and their remote begin/end coverage was incomplete. Both
failures and raw data remain archived. A diagnostic-only userspace collector
then drained every 5 ms, retaining remote records after the drain. Its focused
traces pair 2,260 source and 5,100 owner remote VFS begin/end records. All 2,240
primary path requests plus ten opens match unique owner replies by channel,
cookie and status. This is bounded successful-run evidence, not proof of all
interleavings. WOSDBG loaded 71,790 derived remote records and both serial logs;
global cross-node clock order remains unproven.

Recording is disabled, scratch inventories restored and nodes synced. Initial
and final symbols match; debugger sessions close; launcher15174 exits0; the host
PID/start-time audit at2026-09-16T03:12:34.244795+00:00 confirms no WOS VM remains.
No production source or kernel change, Normal Build WOS or KTEST rerun was needed.
The diagnostic collector compiled successfully. All live work used --no-setup.

The current manifest decision now reflects implemented native-unmount retirement
from9LZ, replacing its stale statement that this transition still needed fixing.
Next reconcile supported backend/root/cwd membership and binding concurrency
against current source and native tests. Unsupported surfaces must be assessed
through their actual refusals. Goal remains active:12enforced/9pending, with
vfs-traversal first pending. See [the observation](directory_timeout_observation.md)
and `build/goal19-response-timeout/validation.json`.


## 9MB native retained-context reconciliation

Reviewed native TMPFS/XFS root/cwd ownership, covered-mount membership,
search/namespace epoch validation, staged publication and scoped unsupported
backend refusals. Declared LOCAL `/` routing is unchanged. This reconciles the
native-context portion of traversal; mapped binding concurrency and internal
automatic mount lifecycle remain open.

The isolated diagnostic build passes with no compilation work; eleven inspected
source hashes and both kernel hashes are unchanged. Twenty-nine native-context
PASS records are indexed from the final9LZ KTEST (59,759/0); KTEST was not rerun.
Lookup, mount lifetime, remote VFS and policy source checks pass now. The mount
check's stale expectation of the original unmount target was corrected to the
bounded copied target, preserving exact-mount/dev-ID requirements. Its initial
failure is archived. No production edit, Normal Build WOS or live run in9MB.

Goal remains active:12enforced/9pending, vfs-traversal first pending. Details:
[native context audit](native_context_audit.md); artifacts: `build/goal19-native-context-audit/validation.json`.


## 9MC mapped directory binding retirement

Traced authenticated policy capture, pre-ACK fixed request admission, exact
binding/File ownership and mapped native traversal. Added deterministic source
and destination retirement after native directory preparation, before final
adoption, on TMPFS and XFS. Each refusal preserves reply/name, unclaimed source
and activity, registry size and binding references; same-grant positive controls
pass. Production behavior, declared LOCAL `/` routing and ABI/wire layout are
unchanged.

Normal Build WOS and isolated diagnostic builds pass. Four focused source/policy
checks and scoped formatting/tidy pass; an initial read-only host cache failure
is archived, and the retry disables caching. New isolated KTEST passes59,759/0
at471.612s, including the expanded grant test, and powers off at471.613s.
Launcher50620 exits0. The host PID/start-time audit at
2026-09-16T03:40:53.851608+00:00 confirms all owned processes absent and no QEMU.
Normal kernel735092b8027f69db3f19154ec37ab1af21450e70; diagnostic kernel
0742beb2b60f4980f0cdf6bd86b7505e3a048545. No normal VM run in9MC.

This reconciles the reviewed mapped admission/ownership/claim boundaries;
internal automatic mount and mixed attachment lifecycle remain the next
traversal item. The earlier directory timeout remains unresolved. Goal stays
active:12enforced/9pending. See [binding retirement review](directory_binding_retirement.md) and
`build/goal19-directory-binding-retirement/validation.json`.


## 9MD traversal qualification

Qualified `vfs-traversal` after reconciling automatic mount admission/publication,
exact-owner cleanup, supported backend search policy and explicit scoped
refusals. The requirement-level checklist links current source to57 relevant
PASS records from9MC and prior native/mapped ACCESS, traversal, mixed-mount and
export replacement fixtures. One Snapshot, exact retained identity, staged
denials and declared LOCAL `/` routing are preserved. No production edit.

Current native/mapped source hashes and both9MC kernel hashes match. No new
build, packaging or VM run in9MD; the latest full isolated run remains59,759/0.
The earlier timeout and auxiliary attach ENOENT cause remain unexplained and
visible. Neither is silently relabeled a successful run or a fixed transport bug.
Separate VFS operation commit obligations and broader WKI domains remain open.

All21 authority domains remain: **13enforced/8pending**. Goal19 stays active;
`vfs-open-create` is now first pending. See [the qualification checklist](traversal_qualification.md) and
`build/goal19-traversal-qualification/validation.json`.
