# Goal19 9NG: independent child-event claims

Previous turn: progress. 9NF established that distributed waits require persistent
claim ownership independent of the RX worker and asynchronous continuation state.
This stage implements the lifecycle claim foundation; the complete distributed
wait/SIGCHLD contract remains required.

## Patch contract

Problem: event claims currently bind directly to one Task's single event pointer.
A shared executor cannot hold independent remote transactions. Introduce a stable,
noncopyable claim record and use it for existing local waits as well as explicit
nonblocking claims. Do not impersonate remote waiters with synthetic Tasks.

Expected files: Task child-event layout, child_events.cpp/.hpp, child-event KTEST
registration, wait source guards and goal/audit evidence.

Files read: all claim/link/queue/retirement users in child_events.cpp; Task event
and waiter layout; waitpid claim/usercopy/commit; process/thread exit cancellation;
ptrace acknowledgement; lifecycle KTESTs/source guards; 9NF WKI session, deferred
worker and fatal-stack ownership reconnaissance.

Invariants: event removal clears the current claim backlink before freeing event
storage. Identity plus cookie validates release/commit. Local claims retain their
Task, preserve wake handoff, cancellation, EFAULT retry, WNOWAIT, rusage and one
winner. Busy claim records never overwrite an existing event. External records
have stable caller-owned storage through explicit cancellation/commit; they carry
no scheduler identity and cannot register a sleeping Task. Future session slots
must cancel before destruction/reuse and must not depend on fatal-stack RAII.

Locks: existing IRQ-save lifecycle lock protects record/event links. Caller holds
the owner Task lifetime and must not hold scheduler, WKI or resource locks. Local
Task references pin embedded records; external storage is retained by its owning
transaction. Wakes and GC requests remain outside lifecycle lock.

Unsafe contexts: no new allocation, network operation, blocking or registry lookup
in claim transitions. Cancellation preserves existing out-of-lock wake behavior.

ABI/wire/syscall impact: internal Task layout changes only. Local public syscall
signatures/errors, credential snapshots, wire formats and declared VFS routing are
unchanged. No new remote protocol is enabled in this stage.

Build/test plan: deterministic real lifecycle tests for independent claims from
one executor, busy-record refusal, wrong-record refusal, cancellation/reissue,
owner exit/tracer detach with actual subject reclamation, accounting and local
waiter competition. Normal Build WOS, isolated full KTEST, source/host suites,
formatting and existing owned-process live wait/signal/terminal/ptrace checks.

Rollback: exact before files and scoped diff in
`build/goal19-independent-child-claims/`; no unrelated edits or domain promotion.

## Implementation

`ChildEventClaim` contains the lifecycle-protected event backlink and an immutable
optional local waiter pointer. It cannot be copied or moved. Each local Task
embeds a record initialized to that Task; independent transaction storage uses
a record without a waiter. `ChildEvent::claimed_by` names the record. Event
retirement clears the record before releasing the optional local Task reference.

`claim_locked` serves both existing `claim_or_register` and the nonblocking
`try_claim` API. A busy record returns WOULD_BLOCK before scanning or modifying
another event. Independent claims never register a sleeper or look up an executor.
Release and commit still validate the current backlink, borrowed event identity,
claimant identity and cookie under the lifecycle lock. `cancel_claim` is explicit
and idempotent; local `cancel_wait` continues to cancel both its registration and
embedded claim. Existing syscall and ptrace callers retain their public API.

The new independent-claim test interleaves two records on the same executor,
refuses a busy record, rejects wrong-owner/wrong-record commits, and competes with
a registered local waiter. Cancellation lets that local waiter claim; WNOWAIT permits
reclaiming the same event with a new cookie. Stale tokens and duplicate commits
cannot consume it again. Two exits contribute exactly 40 microseconds once, and
all fixture references return to one. The second test cancels via parent exit and
tracer detach, frees the embedded subject/event storage, then exercises stale
release/commit and repeated cancellation. The existing local retirement and
reissued-cookie tests run through the same implementation.

The independent API does not implement authentication, remote duplicate replies,
session cleanup, remote sleeping waiters or distributed notification. Those are
requirements of the next protocol stage, not properties inferred from these local
lifecycle tests. Future transaction storage must remain stable until cancellation
or commit and must be discoverable during fatal caller/session teardown.

## Verified results

Normal Build WOS used the current task's `scripts/dev/build_wos.sh` and passed.
The isolated diagnostic build passed, and freshly packaged KTEST passed
**59,832 assertions, zero failures**, powering off at guest time 486.554.
Both new lifecycle tests passed, along with the existing local claim tests.
Five host suites, five source suites and scoped formatting passed. Kernel and
KTEST sources stayed unchanged after the builds; receipts check their hashes.

Normal build ID: `30c2ff9bb47787943b682e1115306262e0383b07`.
Diagnostic build ID: `7932d8faf8b6ac88e2ea39af3104d6d343f80e11`.
Full hashes and matching private boot manifests are in the stage directory.

Existing native group-origin waits passed at UID0 and UID104, including correct
zero/negative selectors, exit41/exit37, child/direct-parent reaping and helper
removal. The unchanged helper binaries were hash-checked. Existing signal (20),
terminal (112), terminal-transition (28), ptrace (384) and extended ptrace/signal
(24) checks passed: **568 checks**, besides the two group-wait cases. These are
owned-process functional checks; no remote claim protocol was exercised.

Diagnostic PID2669530 was confirmed absent before the cluster launched. Private
guests2671384/2671379 synced, their boot backing chains matched the private image,
and both acknowledged QMP quit. Their absence and no remaining WOS VMs were
verified at **2026-09-16T17:25:53.013873+00:00**.

The goal and manifest retain **17 enforced / 4 pending**. The policy audit/unit
checks pass; the completion gate must still fail on pending `signals`. Next is
the negotiated, session-bound reverse exec authority and continuation protocol,
including selector aggregation, reply/cancel cleanup and SIGCHLD admission.
Positive-PID collisions and committed-reply session loss remain explicit design
requirements from 9NF. No pending domain is promoted by this foundation.

Evidence: `build/goal19-independent-child-claims/validation.json`, command receipts,
source/kernel/fixture hashes, full diagnostic serial log, live fixture rows and
exact VM shutdown receipts. The original fenced goal text is unchanged.
