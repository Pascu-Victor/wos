# Goal19 9NP: query-bound claims and commit

Previous turn: progress. Query transport passed60030/0 KTEST and all owned VMs
are down. Goal remains17 enforced/4pending, first signals.

## Reconnaissance and patch contract

Problem: source claims are independent of query ownership; multi-hop wait claims
need immutable selection, correlated outcomes and close cleanup before activation.
Expected files: child_query.hpp, remote_compute.cpp/.hpp, wire.hpp, wki.cpp,
auth_policy.cpp, remotable.cpp, WKI tests/source guards and progress documents.
Files read: those entrypoints, peer/session admission, child event selectors,
conditional claims and retirement, Task event types, existing query wire workers,
source transaction execution, lifecycle cleanup and prior accounting findings.

Flow: authenticated claim packet -> exact retained VALIDATED query -> one fixed
depth/selector/options/version -> local conditional transaction or deferred upstream
claim -> correlated value receipt -> commit/release -> query CLOSE as final ACK.
Only the selected source queue may consume and charge child CPU. Forwarders cache
values without charging. PID ambiguity returns EOVERFLOW before a claim; no-ready
or stale observations do not become a new claim.

Invariants: one claim attempt per query; immutable depth and arguments; source
BEGIN/COMMIT replay never consumes twice; RELEASE after COMMIT retains the committed
receipt. CLOSE supersedes pending sends, advances query and claim watermarks, and
waits for local transaction retirement plus upstream close/session termination.
Late requests/replies cannot resurrect a closed or reused slot. Event pointers
remain private to stable source transaction storage, never a wire or RX stack.

Locks: incoming workers hold one authenticated peer lease, release compute lock
for bounded lifecycle claim/commit effects, and never wait for another peer.
Outgoing workers reserve value actions then acquire only the upstream lease.
Retirement/cancel and final owner release run outside peer/compute locks. Busy
records cannot be reclaimed while a local operation uses their immutable fields.
Allocation/blocking: fixed records; raw RX only validates/copies. Retry sleeps
belong to workers outside locks. No syscall usercopy or allocator changes.

ABI/wire impact: separate versioned claim request/response messages and advertised
capability, with exact request/session/action correlation. Existing exec preflights
and syscall ABI remain unchanged. waitpid activation awaits initiator/native-waiter
lifetime, wake handling and imported CPU accounting.

Build/test plan: real child queues and accepted exec/session rows for claim,
commit/replay, stale versions, cancellation, no-reap ambiguity, WNOWAIT and cleanup;
synthetic encoded forwarding/correlation; normal/isolated builds, host wire/policy,
scoped checks and fresh live KTEST. Multi-node syscall qualification remains open.
Rollback: pre-stage copies under build/goal19-child-query-claims/before.


## Implementation and qualification

Messages 0x67/0x68 use canonical packed 32-byte requests and 88-byte replies,
version 1, with separate process-child-claim-v1 advertisement 0x53494708.
The request supplies only a query key, selected depth and bounded action generation;
selectors, options and queue versions come from the retained validated query.
COMPUTE plus IDENTITY_SAME_DOMAIN, credential capability and the exact accepted
execution/session remain mandatory. No user credential or event pointer is sent.

BEGIN reserves a local conditional transaction or an upstream continuation. A
committed or canceled receipt is immutable and replayable. A forwarding record
accepts only the exact issued request, current session, legal reply phase and
unchanged event values. Pending receipts do not complete an action. Intermediate
records do not charge child CPU. Retry reservations rotate across fixed records
with 10ms deadlines, and the worker drives sends outside incoming peer leases.
Query and claim responses share an explicit cached-outgoing flag so received
responses cannot accidentally be echoed. Payload classification checks its output
pointer before either query or claim decoding.

CLOSE retires matching local transactions and their sequence watermarks, supersedes
pending claim sends and retains the query until local transaction storage is gone
and upstream CLOSE is acknowledged or the exact upstream session ends. Losing a
session during COMMIT yields an indeterminate EIO; it never asserts rollback.

KTEST coverage added: malformed request/response envelopes and inactive fields;
a real source queue with WNOWAIT commit/replay, cancellation, stale versions,
wrong depth, retained receipts, pending CLOSE and late BEGIN; synthetic encoded
upstream claim/commit/release, timed retries, malformed correlation, changed event
values, command/phase mismatch, duplicate replies, no intermediate CPU charges,
and positive-PID collision without local reaping.

The normal ELF reserves 52,224 bytes for the 64 query records and 38,912 bytes
for the unchanged 128 source transactions. No per-message allocation is added.

Fresh live KTEST passed 60,031 assertions with zero failures, including the new
claim-wire and expanded ownership cases. Normal Build WOS, isolated diagnostic
build, scoped formatting, WKI/waitpid source checks, policy audit/unit tests and
host wire/auth-policy tests passed. The completion gate remains intentionally
pending at signals (17 enforced / 4 pending). The initiating waitpid/native-thread lifecycle,
blocking wake handling, imported CPU observations and real multi-node syscall
qualification remain open. Existing declared VFS routing is unchanged. No
penetration testing is performed.


## Next integration constraints confirmed from current source

`waitpid.cpp` acquires a canonical process owner but puts local claim/waiter
state on the native calling Task. A future remote client must retain and cancel
its own continuation on native-thread exit as well as canonical owner exit;
a blocked syscall stack cannot own the upstream cleanup chain. Output writability
and copying must still precede consumption. The current local claim API and
`preemptible_syscall_park` wake contract must remain valid for ordinary local waits.

`resource_contract_model.hpp::restore` and `valid_completion` require zero
submission reaped counters. `check_completions` exports receiver child counters as
segment deltas, and `merge_proxy_resource_completion` adds them at the source.
`child_events.cpp::commit_claim` charges the actual source owner exactly once.
Thus receiver observations of remote reaps cannot be added to exportable counters.
A separate imported observation total, preserved across exec and excluded from
completion deltas, needs a compatible transfer contract and tests for chained exec,
local/remote mixtures, WNOWAIT, saturation and exit-event aggregation. Merely
relaxing the current zero-baseline validation would be incorrect.


Diagnostic VM 3366962 powered off; no matching WOS VMs remained at
2026-09-16T21:45:58.298968+00:00. Built-source and packaged kernel hashes match. No failed
build or runtime attempt occurred in this stage. Live packet delivery between
multiple nodes was not exercised; KTEST transport delivery is synthetic. The four
migrated-parent wait failures from the earlier cluster baseline remain unresolved.
Evidence: `build/goal19-child-query-claims/validation.json`.
