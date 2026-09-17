# Goal19 9NR: native remote-wait initiator

Previous turn: progress. Imported child observations passed60032/0 KTEST and12
real two-node exec checkpoints. All owned VMs are down. Goal remains17/4, signals.

## Reconnaissance and patch contract

Problem: source query/claim transports have no syscall initiator. Native waiter
exit must not abandon borrowed stacks, claims, pending commits or accounting.
Expected files: remote_compute client/worker/retirement API, child-events live
observation helper, waitpid and exec integration, KTEST/source guards and docs.
Read: current query/claim/session/retirement and RX workers, Task claim/lifetime,
child-events selectors/version/commit/exit, waitpid usercopy/park, resource exec
quiescence, process exit ordering, session group resolution and prior accounting.

Flow: canonical owner plus native waiter -> shared fixed query slot -> worker
INSPECT/PROBE/VALIDATE -> immutable ready depth -> local conditional claim or
upstream claim -> value-only syscall copy -> explicit COMMIT -> one observation
application -> CLOSE and deferred retirement. Shared query slots reserve outgoing
lanes globally for both incoming relays and native initiators.

Invariants: preserve declared VFS routing and syscall ABI; positive-PID collision
returns EOVERFLOW without claims; any-child remains eligible. Group aliases resolve
across the chain, with pid0 capturing the caller's exact group. Local waits retain
their original path. No event/Task pointers on wire or borrowed syscall stacks in
workers. Usercopy precedes commit. Exactly one worker owns a local claim/observation
step; retries/replayed receipts cannot charge twice. Detached native callers retain
pending commit until outcome or terminal failure; no new effects after CLOSE.
Native/canonical lifetime pins outlive cleanup; slot reuse waits for upstream CLOSE.

Locks: compute serializes records and flags; local lifecycle operations and owner
release run after dropping it. Outgoing transport uses one upstream peer lease;
workers never hold an incoming lease while waiting/sending upstream. Native caller
sees copied values only. Exit marks flags even while a worker owns busy. No new
raw-RX allocation/usercopy/blocking. Deferred workers use fixed storage and bounded
scans; syscall polling parks with finite deadlines instead of spinning.

Accounting: remote parent EXIT commit without WNOWAIT imports receipt values once;
local commit already charges locally. Stop/tracer/WNOWAIT never import CPU. Late
observations must not change an already-published local exit event. Remote exec
must drain detached commit observations after sibling quiescence before snapshot.

Compatibility: no new wire message/layout. Current query+claim capabilities and
exact accepted source session remain mandatory. No unsupported-source fallback to
local ECHILD. Errors and unknown commit outcomes never assert a rollback.
Build/test plan: native owner/waiter lifecycle, local/upstream selection, replay,
copy-failure close, fatal exit during commit, exact accounting and lane reuse tests;
normal/diagnostic builds, host/source/policy checks, live KTEST; then activate syscall
and rerun migrated-parent and lifetime fixtures on private normal VMs.
Rollback: pre-stage copies under build/goal19-child-wait-client/before.

## Implementation

The native initiator shares the64 query records with forwarding work. A record
pins both its canonical owner and native waiter, owns an independent local claim,
and exposes only copied values through a token bound to that waiter. The worker
performs lifecycle operations outside compute serialization and peer admission;
public poll/commit and retirement skip busy records. Query and claim replies keep
the existing exact session, lane, generation and immutable selector checks.

The worker collects and validates the whole authority chain before selecting an
event. Ambiguous positive PIDs return EOVERFLOW before either BEGIN. Exact groups
and globally resolved group aliases use the existing PROBE path. A local selection
uses conditional lifecycle claims; an upstream selection uses the claim protocol.
After syscall usercopy, COMMIT either consumes the local claim or waits for the
upstream receipt. Only parent EXIT without WNOWAIT adds imported CPU, once. Native
caller exit retains a requested commit until outcome; exec quiescence drains that
observation before another execution baseline. Already-received terminal commits
are applied even if the worker runs after its deadline. Unknown outcomes return
EIO without asserting rollback. CLOSE keeps its lane and pins until acknowledgment
or exact session termination.

The syscall retains the local path for local owners. Remote waits park with finite
deadlines and internally retry stale snapshots. Usercopy failure closes without
commit. Signals/exec quiescence interrupt before commit. While a requested commit
is outstanding, its worker retains outcome/accounting ownership.

The initial normal/diagnostic builds passed. A source guard needed the new native
worker condition; its failed receipt is preserved. The first diagnostic VM was
intentionally stopped before completion when review found the received-commit
versus deadline edge case; its partial output is preserved under
build/goal19-child-wait-client/before-deadline-fix. It is not counted as a passing
runtime run. The corrected build was tested afresh; results follow.


## Verified results and remaining work

Normal Build WOS and the isolated diagnostic build pass. Scoped formatting,
WKI/waitpid/exec source guards, the three host resource/wire/auth-policy tests and
the credential policy audit/unit checks pass. The complete enforcement gate still
fails at pending signals, as expected; the manifest remains17 enforced/4 pending.
Fresh KTEST passes60033/0, including the new NativeChildWaitPreservesCollisionClaimsAndDetachedCommit
case using the production native client and real local queues with encoded synthetic
upstream replies. Collision refusal occurs before claim, wait-any can still reap,
WNOWAIT and abandoned claims preserve the event, detached commits charge once,
and an already-received terminal receipt survives its local deadline. Reference
counts return to their initial values. The exact diagnostic VM3589864 is absent at
2026-09-16T22:50:16.160108+00:00.

Real private nodes wos-0/wos-3 pass all8 child-event cases at UID0/104: local
controls, remotely executed children, migrated parents, and round-trip parents.
Each pre-exit WNOHANG is0; each final wait returns the source child PID and status9472
with exactly one SIGCHLD. This resolves the four earlier migrated-parent ECHILD
failures. All34 recorded process probes are absent and both helper files are removed.
The child-CPU regression also passes12 checkpoints, including local and remote
exec round trips with exact visible child totals; all8 recorded identities disappear
and both helpers are removed. The exact normal VMs3591484/3591490 sync and exit.
No WOS VM remains at2026-09-16T22:52:58.379627+00:00.

Normal kernel build ID: d8a7d268685054c4a880bd28845a8f96138e09d9.
Diagnostic kernel build ID: e73af81f728f45ff1631b3dca6c82d23632cf107.
Source, image and fixture hashes plus command/cleanup receipts are indexed by
build/goal19-child-wait-client/validation.json. Its stage.patch compares against
the pre-stage copies without incorporating unrelated worktree changes.

The native syscall client is enabled. Collision, WNOWAIT, detached commit and
late receipt behavior have kernel coverage with synthetic upstream responses;
real two-node coverage now includes the query/claim transport for migrated waits.
Dedicated live group-selection, usercopy/fatal-waiter races and peer-loss outcomes
remain to qualify. Those functional OS cases and the remaining signal authority
audit precede any decision to mark signals enforced. The overall goal stays active;
network/hostname and both WKI domains also remain pending. Declared VFS routing,
credentials, syscall ABI and existing wire layouts are unchanged by this stage.
