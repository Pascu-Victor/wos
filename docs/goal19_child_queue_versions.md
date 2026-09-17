# Goal19 9NL: validate child queue selection before claiming

## Patch contract

Problem: advisory inspection alone cannot prove a selected event and absence of
PID collisions belonged to one cross-queue snapshot. A stale BEGIN must not claim
a newly arrived event. User policy remains EOVERFLOW/no reap on ambiguous positive
PIDs, with any-child waits usable.

Expected files: Task child metadata, child_events API/implementation/KTEST,
remote_compute private transaction backend/tests, goal evidence.
Files read: Task lifecycle/identity fields; child publication/abort, group updates,
tracer lists, enqueue/move/remove, claim/release/commit, reparent/exit; waitpid;
session mutation and spawn initialization; reverse exec admission and complete
child transaction backend/tests. WKI/critical/kernel/build instructions apply.

Impact map: lifecycle lock protects per-owner selector-visible state. Child and
tracer links, immutable events, claim readiness and live group changes feed the
inspection. Versioned inspection and conditional claim will share that lock.
Source transactions bind the inspected version as an immutable request argument.

Invariants: no version reuse; a stale conditional claim has no event/lifecycle-reference/accounting
side effect; local wait keeps existing behavior. One canonical owner reference,
exact accepted exec/session checks, retained commit replay, WNOWAIT and CPU charge
remain unchanged. One queue version belongs to one retained Task, not a PID.

Locks: lifecycle then existing job/resource locks; no scheduler/compute/channel/RX
spinlock may be held. An already held peer lifecycle admission may span only
bounded local inspection, version validation and claim/commit, never network work
or usercopy. This extends the existing bounded claim/commit exception to read-only
inspection. No new blocking, allocation, logging or network work in these paths.
Unsafe contexts: no raw NAPI call; source backend remains a daemon-context helper.

Version mutation audit: list insert/remove covers publication, abort, reparent and
reap; tracer insert/remove covers attach/detach/exit; event insert/unlink and claim
publish/clear cover readiness; update_job_identity invalidates parent/tracer group
observations. Snapshot versions include a separate lifecycle-protected global
group generation, because traced native threads derive groups from a retained
process owner that need not itself be traced. Unrelated group changes may thus
conservatively retry a distributed discovery; no global PID scan is introduced.
Both counters use the same saturating increment and exhaustion policy. LIVE/ZOMBIE changes alone do not change potential subject matching;
publication/removal events already invalidate. Terminal-only identity writes do
not alter selector groups. Construction initializes identities before publication.

ABI/wire impact: Task internal layout and private command values change, but no
syscall/wire ABI or advertised protocol is enabled. A saturated version refuses
versioned validation/claim with EOVERFLOW; ordinary local waits keep working.

Build/test plan: actual lifecycle KTEST for publication/abort, group changes,
tracer changes, event/claim ABA, WNOWAIT, transfer/retirement, version saturation
and exact reference/accounting cleanup; source backend tests stale BEGIN replay
and a fresh sequence after rediscovery. Normal Build WOS, isolated diagnostic
build and live KTEST, formatting/source/policy checks. No user runtime blocker.
Rollback: exact pre-edit files under build/goal19-child-queue-versions/before.

A future complete distributed discovery pass must retain the full exec chain,
collect and revalidate every queue version before acting on the aggregate
result (including EOVERFLOW/ECHILD), and require the selected version at BEGIN. The unchanged-version intervals must overlap. This patch supplies local
validation/conditional claims, not a completed asynchronous cross-node engine.
Goal stays 17 enforced / 4 pending until end-to-end behavior is qualified.

The native-thread mutation audit found that ptrace attach accepts native Tasks,
whose job_identity reads their immutable resource_owner. That owner reference
survives thread CPU retirement until Task destruction. A local queue version
alone therefore misses inherited group changes; the additional group generation
covers them without traversing or locking the scheduler registry. A dedicated
actual-owner/native-tracee KTEST checks this case and commit of an already claimed
immutable stop after a later owner group change. This was found during source
review, before live qualification.

## Implemented request and replay behavior

QueueInspection now captures readiness, potential subjects, normalized candidates
and a two-part version under one lifecycle lock. The queue component changes on
owned topology/event/readiness mutations. The group component changes on effective
PGID/origin changes, including changes inherited by native tracees. Counters
saturate at UINT64_MAX; an exhausted component refuses validation/conditional
claim rather than wrapping. The ordinary local claim API remains usable.

validate_queue_version compares both components on the same retained owner.
try_claim_if_current performs that check and the existing claim operation under
one lock. STALE/EXHAUSTED returns an empty output snapshot without disturbing an
existing claim, queue, waiter or accounting. A busy current record retains the
existing WOULD_BLOCK behavior. Later changes do not revoke an already-held
immutable claim; commit still checks its actual link, owner and cookie.

The private WKI ChildWaitRequest now requires nonzero queue/group versions and
binds them into immutable request equality. A stale BEGIN is retained as DONE
with EAGAIN, while version exhaustion is DONE/EOVERFLOW. DONE is distinct from a
transient PENDING response: a client must ACK, perform fresh discovery and use a
new sequence. The cached refusal retains its transaction owner reference until
ACK or normal task/session cleanup; it holds no child claim. Replayed BEGIN/COMMIT cannot opportunistically claim an event after
a prior stale rejection. Successful COMMIT replay/accounting and session/task
retirement retain the existing behavior. No wire decoder or syscall caller is
enabled by this stage.

The source transaction KTEST exercises a real independent claim/release between
discovery and BEGIN, checks the cached stale refusal and unchanged child/CPU
state, then succeeds with a fresh snapshot/sequence. Three lifecycle KTESTs cover
normal mutations, tracer/parent retirement and saturation, and inherited native
tracee groups. The existing parent-event transfer test now checks invalidation of
both old and new owners. These tests do not establish a distributed snapshot or
qualify a complete cross-node wait syscall.

The normal ELF contains 38,912 bytes of fixed child-wait transaction storage
(2,048 bytes more than9NJ/9NK), and the global group generation occupies8 bytes.
Task gains one8-byte queue counter. No heap allocation or registry scan was added
to inspection, validation, version advancement or conditional claims.

## Verified results

Normal Build WOS and the isolated diagnostic build passed. Scoped formatting,
WKI/waitpid source suites and credential policy/unit checks passed. The expected
completion gate still rejects the signals domain; the manifest remains17 enforced
and4 pending.

Freshly packaged live KTEST passed **59,841 assertions, zero failures**. The three
new lifecycle tests, strengthened parent-event transfer test and extended WKI
transaction replay test all passed. This includes native tracee group inheritance,
conditional claim rejection, preserved held claims, retirement, saturation and
retry with a fresh request after retained stale refusal. No build or runtime test
failed during this stage. The native-thread gap was found in source review before
live qualification and is covered by the final tested kernel.

The guest reported selftest poweroff at480.120 seconds. Exact diagnostic VM3034935
was verified absent, with no matching WOS VMs, at `2026-09-16T19:42:11.404678+00:00`.
Normal build ID: `632a55211dc444e2b04b7c61de809ba72a41bd70`; SHA-256: `107a45ee4eddb9b2d291ae0d3eb8062b52be09743012367eff7f8cb70bfc7d6d`.
Diagnostic build ID: `bcaa1ea3bca68aafd0b23c297355838d38fa7fe8`; SHA-256: `d3fc248934e58f433b20a7884eb141a2398cf3857c6706fcf1724f704de49f6e`.
Final source hashes and the diagnostic boot manifest match the tested builds.

No normal-cluster or live peer-failure test was run. No cross-node collect/validate
continuation or waitpid syscall integration is enabled yet; the four previously
observed migrated-parent wait failures remain unresolved. The next step must
connect retained asynchronous query/validation/claim/commit/ACK, client fatal-exit
cleanup and single-owner child CPU accounting, then qualify the user's EOVERFLOW
policy and any-child behavior with actual multi-node families. Goal19 remains
active. Evidence: build/goal19-child-queue-versions/validation.json.
