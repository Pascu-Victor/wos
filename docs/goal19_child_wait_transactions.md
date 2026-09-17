# Goal19 9NJ: retained child-wait transactions

Previous turn: progress; 9NI qualified SIGCHLD forwarding while preserving four
migrated-parent wait failures. This stage supplies source-side persistent claims,
commit-result replay and cleanup needed by the asynchronous distributed wait
engine. It does not enable a wire message or replace the full goal with this layer.

## Patch contract

Problem: an RX worker cannot lend its own claim slot to a remote waiter. A commit
reply must remain repeatable after consuming an event; acknowledged or retired
requests must never acquire a different child when delayed duplicates arrive.

Expected files: remote_compute.cpp/.hpp, child_events lock-contract comment,
WKI KTEST/source guard and goal evidence. Files read: current child claim/release/
commit and cleanup paths, independent Claim storage, reverse admission helpers,
compute peer sessions, RX worker/peer lease, retirement/task cleanup, reliable
send ownership, local wait usercopy and the 9NF–9NI contracts.

Invariants: exact accepted exec and authenticated session authority; stable
independent claim storage; one owner reference per granted transaction; immutable
request arguments; no event consumption on rejection or release; WNOWAIT and
single child CPU accounting; duplicate commit returns the retained result. A
retired session/request cannot be reused to claim another event. No PID lookup.

Locks involved: metadata and slot ownership use s_compute_lock. Release it before
any child lifecycle operation. An already acquired peer lifecycle lease spans
only bounded local claim/commit work, giving that operation an admission point
against peer replacement. This refines the broad old no-WKI-lock comment: no
compute/channel/RX/queue spinlock is held and no operation here sends or waits on
the peer. Child producers release lifecycle before forwarding; claim/commit wakes
are local and request_gc is nonblocking. Deferred cancellation runs outside all
peer/compute locks. Busy slot ownership prevents concurrent cleanup/reuse.

Unsafe contexts: no operation from raw NAPI. The eventual dispatcher must acquire
the peer lease in daemon context; metadata-only retirement can mark slots while
effects run. Allocation/blocking: fixed transaction capacity and per-session
request-lane completion watermarks; reserve before claiming. Cleanup allocates
nothing, and no persistent record refers to a syscall stack or RX executor.

ABI/wire/syscall impact: none yet. Private command/result values are not packed
or serializable and contain local identity. Keep the wire disabled until client
ownership, selector aggregation, usercopy and multi-hop continuations are ready.
The eventual protocol must document EIO for an indeterminate commit after terminal
session loss; it must never describe that result as proof of nonconsumption.

Build/test plan: actual accepted rows, sessions, owners and child events in KTEST;
interleave duplicate/released/committed/acknowledged requests, lane reuse, stale
sessions, pending ACCEPT, busy retirement and task exit. Check nonzero rusage and
exact lifetime restoration. Normal Build WOS, isolated KTEST, source/host/policy
checks. This layer alone cannot change the four live distributed-wait failures.

Rollback plan: exact before files under build/goal19-child-wait-transactions/.
Leave the goal and four domains pending. No runtime blocker is present; live WOS
authorization with --no-setup remains in force.

## Implemented ownership and replay rules

The module has 128 stable transaction records and 64 request lanes per authenticated
compute session. Each lane carries a monotonically increasing, nonzero 64-bit
sequence. Completed-sequence watermarks survive record cleanup and reset only
when a new session epoch is established. A client must not wrap or reuse a sequence
within its session. The future client must retain lane ownership through ACK or
terminal session retirement; this stage does not publish a wire representation.

BEGIN reserves available storage before claiming an event and obtains the exact
source owner through the accepted-exec admission helper. Before final ACCEPT it
returns PENDING without retaining an owner or claiming anything; the future client
must retain and retry that immutable request. Once recorded, task ID, receiver PID,
selector and options cannot change. Concurrent reuse of a lane returns EAGAIN.
A claimed event remains retryable until COMMIT, or RELEASE cancels the claim.

COMMIT rechecks the current accepted owner before its effect. The retained reply
contains PID, group origin, kind/audience, status and CPU times as values, never
an event pointer. The borrowed internal event snapshot is cleared after commit or
release. Repeated BEGIN/COMMIT/RELEASE on a completed record returns its recorded
outcome: release after a successful commit cannot pretend the event was restored.
WNOWAIT commits an observation while preserving the event and child accounting.

ACK retires a completed record. RELEASE or ACK arriving before BEGIN records a
watermark without a claim, preventing later queued BEGIN work from resurrecting
that request. Task cleanup, proxy retirement and compute session retirement mark
matching records for cleanup; a busy record stays stable until its current local
effect ends. Compute workers cancel retired claims and release Task references
outside peer and compute locks before making the slot reusable.

The lock refinement applies only to bounded nonblocking child claim/commit effects
under an already acquired peer lifecycle admission. It does not permit waiting
for a remote response, usercopy, network send or process-child retirement under
that lease. Future multi-hop continuations must release it before another hop.
`request_gc` was checked in current source: it publishes a request and wakes the
GC daemon; it does not execute collection or wait inline.

The deterministic KTEST uses real submitted execution/session records and child
lifecycle objects. It covers pending ACCEPT, missing admission, invalid commands
and lanes, changed immutable arguments, same-channel changed authentication,
missing permissions/capability, competing claims, retained commit replay, exact
nonzero accounting, release/retry, WNOWAIT, pre-BEGIN cancellation, busy cleanup,
revoked execution, Task cleanup, session retirement and old-channel refusal.
These are deterministic state interleavings, not a live peer-loss campaign.

The optional PID-collision preference has not received an answer. The stated
implementation assumption is EOVERFLOW with no reap for ambiguous positive PIDs;
any-child waits remain usable. That behavior is not enabled by this patch. The
full protocol still requires selector aggregation, client fatal-exit cleanup,
application ACK/indeterminate-commit handling and single-owner distributed CPU
accounting before the four migrated-parent wait failures can be considered fixed.

## Verified results

Normal Build WOS and the isolated diagnostic build passed. Scoped formatting,
four source suites (WKI, waitpid, atomic signals and poll deadlines) and both
WKI authentication-policy/wire host suites passed. The freshly packaged live
KTEST run passed **59,836 assertions, zero failures**, including
`WkiRemoteCompute/ChildWaitTransactionsReplayCommitAndRetireClaims`.
Serial reports selftest poweroff at 483.081 seconds.

Diagnostic VM2877480 was verified absent, with no matching WOS VMs, at `2026-09-16T18:53:39.762323+00:00`.

Normal kernel build ID: `407347318f064a0a580212930ad6879af4a88112`; SHA-256: `98db3ac0feb3942e51966e054ae1652b5fbda248a629579310c4ff5e701d23cd`.
Diagnostic kernel build ID: `d6f36b34dc1449e72aa088ad99b1b1215e713ec2`; SHA-256: `8a1e03bd6006014d69aad6658289188ae15433810b8da2d78183088a80c8b034`.

Final source hashes and the diagnostic boot manifest match these artifacts.
The normal ELF contains 36,864 bytes of fixed transaction storage; the complete
compute peer-session array is155,648 bytes, including its new replay watermarks.
No heap allocation was introduced for these records.

No normal-cluster, live peer-failure or application reply-loss checks were run in
this stage. The command helper remains module-internal with no wire/syscall caller.
The four migrated-parent wait failures from9NI remain unresolved; the previous
568 live regression checks are historical evidence, not rerun qualification of
this kernel. Goal19 stays **17 enforced / 4 pending**, first signals. The next
implementation must connect aggregate discovery/selection, client ownership and
usercopy, acknowledgments, multi-hop continuation cleanup and distributed child
accounting, then make the full migrated-parent fixture pass.
