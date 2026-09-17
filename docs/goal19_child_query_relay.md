# Goal19 9NM: asynchronous child-query relay state

## Patch contract

Problem: multi-hop child discovery cannot recursively block an RX worker while
holding peer lifecycle admission. Round-trip exec may revisit the same peer and
worker shard. A relay must preserve local discovery while awaiting upstream work,
then validate the full chain before acting on aggregate candidates.

Expected files: new child_query.hpp state machine, child_query_ktest.cpp and goal
evidence. Files read: current remote_compute RX classification/worker/reply retry,
reverse exec owner admission, session retirement, synchronous pending wait cleanup,
child claims/versions/candidates, WKI wire/auth dispatch and prior contracts.

Impact map: the new transport-independent relay consumes owned-queue value
snapshots and emits explicit upstream work; callers retain Tasks and exact
sessions using the existing admission helpers. It will be used by the deferred
wire/client adapter. No raw Task, event, channel or stack-wait pointer enters it.
It does not itself grant authority or provide transport/lifetime admission.

Invariants: all owned queues contribute before PID/group resolution; positive
PID ambiguity remains EOVERFLOW/no reap. Numeric group aliases need one global
resolution followed by a probe of that exact origin with unchanged versions.
Collect/revalidate intervals must overlap. Duplicate/stale operation replies
cannot advance another phase. Failed discovery/validation never becomes proof of
unique ownership or ECHILD. Cancellation supersedes in-flight operations and
waits for upstream cleanup acknowledgment or terminal session loss.

Locks: the runtime adapter must serialize a record, reserve actions before
unlocking, and release peer/compute locks before further hops or network waits.
The state machine takes no locks, allocates nothing, blocks nowhere and retains
no executor or syscall-stack storage. Unsafe contexts: adapter entry from raw
NAPI remains forbidden; work belongs to deferred workers.

ABI/wire/syscall impact: none yet. This is the relay algorithm, not an advertised
protocol or completed waitpid path. Task/session pinning, bounded global record
storage, immutable request matching, wire decoding and application ACK/lane
retirement still belong to the adapter before activation.

Build/test plan: actual child lifecycle snapshots, synthetic multi-hop action/
response interleavings, PID node/boot collisions, group origin resolution,
version invalidation, replay/retry, cancellation and session loss in KTEST;
Normal Build WOS, isolated build/live KTEST and scoped checks.
Rollback: new files and exact prior goal under build/goal19-child-query-relay/before.
Goal remains17 enforced/4pending; no runtime blocker or requested user action.

## Relay behavior and adapter obligations

INSPECT merges every upstream PID/group candidate before resolution. Readiness
uses a bounded hop depth and local-first selection without inventing a global
event timestamp. Positive-PID summaries count live children, even when only one
queue has an event. Numeric group discovery deliberately suppresses readiness
until the initiating node chooses one physical origin and PROBE visits that exact
origin in every queue with unchanged versions. A subtree cannot substitute its
own preferred alias for the initiating node's choice.

VALIDATE checks the local retained version before dispatching upstream validation.
The caller must supply an actual lifecycle validation result; the relay does not
manufacture a queue proof. Earlier group probe/discovery outcomes remain cached,
but a failed query cannot be treated as ECHILD or uniqueness. Claiming still needs
the selected queue's conditional claim at its inspected version.

Each upstream action includes record identity, operation and a bounded generation.
Reservation precedes sending. A transient send failure retries the same action;
a response can win before send completion. Late or duplicate completions cannot
advance a new phase. CLOSE supersedes pending work, rejects prior replies, and
keeps cleanup pending after an unsuccessful close response. Only acknowledgment
or exact upstream session termination makes that side safe to retire.

The adapter must own the retained Task/session and bounded record storage, bind
immutable selector/options/caller origins to every supplied snapshot, correlate
actions with exact authenticated channel/generation/request keys, and preserve
that binding through retries. The relay's record identity is not wire authority.
No real Task/event/channel/stack pointer is stored by Relay. It must not be enabled
on RX or exposed to syscall callers until the adapter implements those obligations,
final claim/commit/ACK, fatal-exit cleanup and distributed child CPU accounting.

Five KTEST cases use actual child lifecycle queues and conditional claims while
simulating relay action/response delivery across three logical namespaces. They
cover node/boot PID collisions without claims, any-child selection at depth2,
global group precedence and exact-origin probing, stale validation after a real
claim/release, transient retries, duplicates, cancellation, malformed depth,
version changes and session loss. This is in-kernel relay-algorithm qualification,
not live WKI packet delivery or an end-to-end distributed wait syscall.

The adapter must also preserve request-lane retirement watermarks when CLOSE
arrives before INSPECT or before a delayed claim. A CLOSED relay cannot replace
those session-bound tombstones after its storage is reused. Incoming and outgoing
request keys are distinct bindings; a relay action's local identity must be mapped
to the reserved upstream lane/sequence, never forwarded as implicit authority.
CLOSE cleanup must include an existing source claim's RELEASE/ACK (or its retained
committed result), while query records beyond the selected queue also retire.

## Blocking wait integration finding

Current child_event_signal_local adds SIGCHLD and wakes the exact process owner.
It does not itself wake a native thread parked in a remote child query. Current
release_claim and commit_claim select local registered waiters without generating
another SIGCHLD. Thus the frontend cannot assume forwarded SIGCHLD alone covers
all readiness changes, especially a competing claim's release or native waiters.
It needs explicit notification/registration or a deliberately bounded retry design
with correct park/wake and fatal-exit cleanup. Do not synthesize extra user-visible
SIGCHLD signals merely to notify a released claim. This is a source integration
finding, not a newly reproduced live wait failure; the four existing migrated
parent failures remain the end-to-end baseline. Relevant source hashes are in
build/goal19-child-query-relay/adapter-source-hashes.json.

## Qualification

Normal Build WOS, isolated diagnostic build, scoped formatting, WKI/waitpid
source suites and policy checks passed. Freshly packaged live KTEST completed
60,028 assertions with zero failures, including all five new relay cases and the
retained child-wait transaction test. Diagnostic VM3096616 powered off; the exact
process was absent and no matching WOS VMs remained at 2026-09-16T20:12:33.721984+00:00.
Normal kernel behavior and its image hash are unchanged from9NL. No normal-cluster,
live peer-failure or end-to-end distributed wait syscall checks ran in this stage.
Production Task/session storage, transport, final claim/commit/ACK, blocking-wait
wakeups, fatal-exit cleanup and distributed CPU accounting remain to be integrated.
The four migrated-parent wait failures remain unresolved. Goal remains17 enforced/
4pending, first signals; EOVERFLOW/no-reap is qualified in the relay tests but is
not yet enabled for distributed waitpid. Evidence:
build/goal19-child-query-relay/validation.json.
