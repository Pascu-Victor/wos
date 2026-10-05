# Goal19 mapped directory binding retirement (9MC)

This stage traces mapped VFS traversal from authenticated RX admission to the
owner's retained directory view, and adds a missing deterministic retirement
case. Production authorization, declared LOCAL `/` routing, credential layout,
syscall ABI and wire format are unchanged.

## Admission and ownership

`wki_rx` holds a peer lifecycle lease while authenticating and copying policy
and negotiated capabilities. Before advancing reliable receive/ACK state,
`try_queue_pre_admitted_vfs_op` copies the request into fixed storage and retains
the exact `DevServerBinding`. Pool/shard/server try-lock failure leaves the
request unacknowledged for retry. Queued work does not recover policy from a
replacement peer slot.

The worker uses the retained binding's immutable export names and channel
identity. The latter includes channel pointer, peer, numeric channel and local
generation. Binding references prevent erasure while the worker runs. Native
VFS work and final File release occur outside the queue/server/registry locks.
Export reconciliation closes admission and drains these references before
changing or retiring export bindings.

`auth_policy::map_vfs_read_identity` requires read and same-domain permission;
OPEN derives read/write requirements from its admitted flags. Nonroot trust
rejects every root UID/GID slot, root supplementary groups and effective service
authority. Successful mapping preserves the full Snapshot. The reviewed mapped
STAT/READLINK/REALPATH and directory PIN paths pass that mapped Snapshot into
explicit native `_as` consumers; the worker is not substituted as the caller.

`RemoteVfsFd` and `DirectoryGrantAdmission` retain independent Files before
registry unlock. A directory grant also binds source/target boots, resource
incarnation, submission, owner-issued ID, restrictions and the complete encoded
credential record. Source binding retention and registry validation are separate
steps, avoiding nested server/registry locks. Native preparation rechecks actual
view membership and target search. Final adoption checks source and destination
again and atomically publishes the destination plus the one-shot source claim
under `s_vfs_lock`.

An already admitted File can outlive registry removal and finish its operation.
A new grant adoption whose endpoint is observed retiring at the final check is
refused. Reporting a retained directory's name grants no new child-search
authority. Existing cancellation, purpose separation, rename/name reuse and
readonly/nosuid tests exercise these distinctions.

## Added regression

The existing
`WkiRemoteVfsDirectoryGrant/ExactCredentialsRecipientScopeClaimAndCancellation`
test now forces each endpoint's retirement after native preparation and before
final adoption. Both TMPFS and XFS run both cases. Each checks ENOTCONN, unchanged
reply/name, no destination row or leaked reservation, unchanged source activity
and claim, and balanced binding references. Restoring the private fixture flag
then allows adoption of the same grant as a positive control. This restoration
tests denial rollback; it does not simulate a production reconnect.

The only kernel-file edit is inside the existing `WOS_SELFTEST` section of
`modules/kern/src/net/wki/remote_vfs.cpp`, using its existing publication hook.
The hook and retirement flag are cleared before checking the result, including
failure paths.

## Validation and limits

Normal Build WOS and the isolated diagnostic build pass. Four focused
source/policy checks pass. The new isolated KTEST run passes **59,759 / 0** at
471.612 seconds, including the expanded grant test at325.543. It automatically
powers off at471.613; launcher50620 exits0. The exact host PID/start-time check
at2026-09-16T03:40:53.851608+00:00 confirms all owned processes absent and no
QEMU remains. The run used `--no-setup` and isolated KTEST artifacts. No normal
VM run was performed in9MC.

Normal build ID: `735092b8027f69db3f19154ec37ab1af21450e70`.
Diagnostic build ID: `0742beb2b60f4980f0cdf6bd86b7505e3a048545`.
Both built kernels and inspected source hashes are unchanged after validation.

The build, check and live-test receipts are recorded in
`build/goal19-directory-binding-retirement/validation.json`.
The initial formatter invocation passed clang-format but its clang-tidy cache
wrapper could not write the host cache. Running the same check with
`WOS_TIDY_CACHE=0` passed; the first failure remains archived.

This evidence targets owned-request admission and directory claim/retirement
boundaries. It does not establish every possible schedule or resolve the
earlier 9LZ directory-response timeout. No network packet injection or
penetration testing was performed. Internal automatic-mount and mixed-attachment
lifecycle reconciliation remains separate, as do open/create/mutation/metadata
commit obligations. Goal19 remains active.
