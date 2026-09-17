# Goal19 9NH: reverse child-authority admission

Previous turn: progress; 9NG implemented and qualified independent lifecycle claim
records. This stage implements the exact execution-record admission needed by the
distributed wait continuation engine. The protocol is not advertised or enabled
until its transactions, selectors, cleanup and notifications are integrated.

## Patch contract

Problem: reverse child authority must distinguish a committed exec from ordinary
remote spawn, bind the retained Task rather than PID, and defer a receiver request
that precedes source final-ACCEPT processing. Forward signal lookup names receiver
rows; it cannot substitute for source submitted-row authority.

Expected files: remote_compute.cpp/.hpp, its KTEST registration and goal evidence.
Files read: submitted/running row types and ownership, find/publish/reclaim helpers,
session capture/retirement and channel validation, exec READY/COMMIT/ACCEPT path,
RX worker/admission and existing signal/execution selftests, 9NF/9NG contracts.

Invariants: receiver lookup matches the exact retained Task; source lookup matches
task ID, receiver PID, peer, channel and generation on an exec-handoff row. A ready
source grant owns one explicit Task reference. Rejection/pending results own none,
consume no event and do not change credentials. No PID-registry fallback. Preserve
prepared-versus-accepted state, peer policy and receiver publication ordering.

Locks: helpers require s_compute_lock and a current authenticated session snapshot
where accepting source requests. They acquire only bounded channel/Task metadata
locks already used in this subsystem. No child lifecycle operation, allocation,
network send, scheduler wait or usercopy occurs under these helpers. Future
continuations must retain the returned Task and revalidate session state before
an event effect; a lookup reference alone is not a permanent authorization lease.

Unsafe contexts/allocation: bounded row lookup and reference acquisition only.
No new RX handler or syscall path is enabled by this preparatory layer. It remains
module-internal until the asynchronous transaction engine supplies its lifecycle.

ABI/wire/syscall impact: none. No message numbers, packed layouts or negotiated
resources change; credentials, public errno and declared VFS routing are preserved.

Build/test plan: actual submitted/running rows and session tables in deterministic
KTEST, including early ACCEPT, wrong IDs/pointers/channels/generations, stale
epochs/identity, non-exec spawn, retired/completing rows and exact reference release.
Normal Build WOS, isolated KTEST, WKI/wait source and policy checks. No new live
protocol claim is possible before the continuation engine is connected.

Rollback: exact before files under build/goal19-child-authority-admission/. Keep
all four pending domains pending; this admission layer is not distributed wait.

## Implemented admission

`capture_child_authority_route_locked` searches by the retained canonical Task
pointer. An ordinary remote spawn has no upstream family grant. Exec receivers
must match their current published/committed row and exact session/channel
generation; retired, completing or terminating rows are unavailable. A receiver
whose ACCEPT is still in flight can capture its route. The value copy includes
the local authenticated session identity; local epochs/pointers are not wire data.

`acquire_child_authority_owner_locked` requires the captured current identity,
COMPUTE plus IDENTITY_SAME_DOMAIN policy and credential capability. It matches the
active submitted exec row's task ID, receiver PID, peer, channel and generation.
Prepared/cutover requests still awaiting final ACCEPT return AWAITING_ACCEPT with
no reference. A ready grant must also match the source Task's accepted signal
route and acquire that exact live Task. It does not depend on proxy scheduler
readiness, mutable UID, or a global PID lookup.

The KTEST creates actual submitted/running rows and invokes the actual ACCEPT
handler. It checks the early-request window; equal-PID/different-Task lookup;
wrong task/receiver IDs; channel pointer/generation, epoch, boot, authentication
generation/session ID mismatches; individually absent policy/capability while
the identity still matches; non-exec rows; source exit; receiver completion; and
session retirement. Ready results acquire exactly one source reference, all other
results acquire none, and fixture references are restored after table cleanup.

These module-internal helpers are deliberately not called from a new wire or
syscall entrypoint yet. The continuation engine must own transaction storage,
duplicate responses, timeout/abandonment and session cleanup before it uses them.
A retained Task is a lifetime guarantee, not a permanent authorization grant.

## Additional integration requirement: child CPU accounting

Current `TaskResourceSnapshot` describes reaped-child CPU as completion-only
segment deltas. `wki_remote_compute_check_completions` copies the receiver's
`child_user_time_us`/`child_system_time_us`; `merge_proxy_resource_completion`
adds those deltas to the source proxy with `add_reaped_cpu_times`. Therefore a
future distributed commit must not charge a source-owned child both at its source
queue and again as newly reaped receiver CPU returned in TASK_COMPLETE. This is a
source-derived requirement for the new path, not a reproduced current accounting
failure. Qualify per-wait rusage, RUSAGE_CHILDREN observations, final parent rusage,
multi-hop completion and peer-failure cleanup with nonzero CPU values. Preserve
one accounting owner or distinguish imported observations from segment deltas.

## Verified results

Normal Build WOS and the isolated diagnostic build passed. Scoped formatting,
WKI/wait source guards and both WKI authentication-policy/wire host tests passed.
The freshly packaged live KTEST run passed 59,833 assertions with zero failures,
including `WkiRemoteCompute/ReverseChildAuthorityBindsAcceptedExecAndSession`.
The final serial receipt reports selftest poweroff at 483.499 seconds.

Normal kernel build ID: `0c2c3c487aaf0e3dfa2236d6702c641a1236b6cc`.
Diagnostic kernel build ID: `b61ca4282b8809026fede40c3c0d5514c14332ac`.
Source and artifact hashes match the code under test. Diagnostic VM PID2738206
was verified absent, with no matching WOS VMs, at `2026-09-16T17:55:16.604628+00:00`.

No normal-cluster, mixed-version, peer-failure or end-to-end distributed wait
qualification was performed in this stage. The admission helpers have no new
wire/syscall callers. Goal19 remains 17 enforced / 4 pending; distributed wait,
SIGCHLD delivery and the previously recorded selector/reply-loss/accounting
requirements remain open. Evidence receipts and scoped patch are under
`build/goal19-child-authority-admission/`.
