# Goal19 9NA: parent-death delivery across remote exec

Implemented and verified. The unchanged eight-case formerly failing
baseline is retained in `build/goal19-parent-death-baseline/` (9MZ).

## Patch contract

Problem: Remote exec omits parent-death registration and parent exit only writes
the parked source Task. Copying the old signal alone misses receiver registration
changes made after exec.

Expected files to change: Task registration; process prctl/fork and shared signal
admission; child_events parent exit; WKI wire/dispatch/resource advertisement and
remote compute snapshot, forwarding and receiver; peer compute policy; native
worker remote-exec signal capture; focused KTEST/host/live tests.

Files already read: 9MZ impact map, Task signal admission/generation and ownership,
PendingSignalTarget, exec cutover drain, READY/COMMIT/ACCEPT and final installation,
compute session/accepted-row forwarding, deferred RX, resource advertisements,
child reparenting and process exit, syscall registration, existing wire tests.

Invariants to preserve: The registration is child-owned and independent of UID;
one exact retained Task and current accepted session/channel generation; current
receiver registration controls delivery; disabled registration has no effect;
no PID lookup or local-proxy fallback after remote acceptance; ordinary user kill
and trusted cancellation remain separate; credential/syscall ABI, saved IDs,
thread retirement and declared VFS routing remain unchanged.

Locks involved: Existing lifecycle, metadata/admission, VM publication and compute
session locks. Registration uses one atomic read/store; local event delivery owns
admission through generation. Forwarding owns no local lease, and lifecycle lock
is dropped before delivery. No new spinlock or allocation for local delivery.

Unsafe contexts: Reparent/exit, exec preparation and final commit, deferred RX.
No blocking send or cutover wait in RX/NAPI or under a lifecycle/metadata lock.

Allocation/blocking assumptions: Transfer the retained child reference into
PendingSignalTarget cleanup. Remote event delivery may wait for final ACCEPT,
which follows final signal installation and receiver publication; ordinary and
fatal cleanup release references/admission. Hop bound remains finite.

ABI/wire/syscall compatibility impact: Exec signal record v2 appends registration
and reserved padding; existing v1 fields retain their meanings. A new advertised
compute resource gates both v2 transfer and a dedicated parent-death event. Old
peers refuse before handoff; invalid versions/lengths/registration values have no
effects. Existing prctl IDs/errno/usercopy and kill/terminal/cancel messages remain.

Build/test plan: Real helper admission/acceptance interleaving, disabled/changed
registration and exact-row/session refusal tests; versioned wire host tests;
Normal Build WOS and full isolated KTEST; unchanged failing live fixture plus
receiver changes and further-hop functional cases; existing signal/terminal and
ptrace regressions. Verify owned-child/helper cleanup and exact VM shutdown.

Rollback plan: Exact before files and separate artifacts under
`build/goal19-parent-death-delivery/`; retain the original baseline and failures.
No domain promotion follows from this one producer. SIGCHLD remains separate.

Additional source review: local native-worker exec retains the canonical owner's
parent-death registration while adopting worker mask/pending state. Remote exec
now takes that same owner registration in both preparation and final capture;
the existing actor-drain KTEST uses differing owner/worker values to verify this.
The new event requires both COMPUTE and IDENTITY_SAME_DOMAIN in the shared peer
policy dispatcher, like the accepted-task terminal event.

## Implementation

- `Task`, process prctl and `child_events`: atomic child-owned registration and
  transfer of the exact retained child into shared local admission / forwarding.
  No lifecycle lock crosses network waiting; no local admission crosses remote
  waiting. The local lease covers signal generation and any scheduler wake.
- `wire.hpp`, peer policy, dispatch and resource advertisements: a bounded
  eight-byte parent-death event, compute/identity permission checks, and an
  advertised `process-parent-death-v1` resource. Exec signal record v2 is 40 bytes;
  commit is 56 bytes. Previous reserved fields retain their meanings.
- Remote compute and exec: transfer and final installation of registration;
  accepted-task/channel/session routing; wait for final ACCEPT before forwarding;
  current receiver registration determines delivery even when the source value
  was zero. Native worker replacement uses the retained owner registration.
- Kernel/host tests and native fixtures cover current registration, old-image
  admission draining, refusal without local fallback, exact receiver selection,
  invalid event/record shapes and retained references. The extended actor-drain
  test distinguishes owner and worker registration values.

## Observed validation

Both the normal `Build WOS` task command and isolated diagnostic build passed.
The initial diagnostic compilation failed only in the new test helper because
`Task::acquire` does not exist; `try_acquire` corrected that and its failed receipt
is retained as `build-ktest-initial*`. Final kernel identities:

| Build | Build ID | SHA-256 |
|---|---|---|
| Normal | `0dc73554313cada14a7e1488f6f5b8710bd20478` | `b4509e648eeb9c5de8634925096a072f64fad759be3f6e02a8bbb45812facb97` |
| Diagnostic | `3d9d20025e797c3b219715f620aa85a07e1f3c69` | `9d2c00d4a6e64314230d815a7dd3693108f49cbdead7ef43b52570f3e57d7bd3` |

Full isolated KTEST: **59,825 assertions, zero failures**, ending at kernel time
482.868; automatic poweroff at 482.869. New local parent-death admission case
passed at 248.476, exact-session receiver case at 349.514. Diagnostic PID 2400124
was verified absent at 2026-09-16T11:55:01.143579+00:00. The boot manifest matched
the diagnostic kernel hash; no stale package is used as evidence.

Five host suites passed: credential model, job control, signal pending, WKI wire,
and WKI auth policy. Four source suites passed: signal, atomic signal, remote
compute, exec. Scoped formatting and the authorization manifest/unit audit passed.

### Live functional cases

The unchanged 9MZ binary now passes **8/8** cases. In both previously failing remote
enabled cases (UID0 and UID104), the replacement reports registration10 and observes
one SIGUSR1 after parent death. All local and disabled controls pass.

The extended fixture passes **30/30** cases: UID0/104 × local/remote/roundtrip ×
unchanged disabled, unchanged enabled, receiver disables, receiver enables, receiver
changes SIGUSR1 to SIGUSR2. Each final receiver reports readiness after making any
registration change, before the observer releases its parent. Roundtrip is
wos-0 → wos-3 → wos-0, exercising forwarding through two accepted execution rows.

All38 direct parents were reaped with status0 and all38 final target reports
received. All48 physical receiver/intermediate procfs absence probes passed.
Both copies of each native helper were removed. Orphan target cleanup evidence is
the final report and procfs disappearance, not a wait receipt held by the observer.

Existing regressions passed: signal identity/group/query/wide20 checks, terminal
identity/lifetime/session112 checks, terminal transitions28 cases; ptrace
observation180, registers128, memory48, images28; extended ptrace/signal24 cases.
That is **568 regression checks/cases**, plus38 parent-death cases. Their scripts
verified owned target/controller and helper cleanup.

Both private cluster guests synced before exact QMP shutdown. PIDs2401535 and
2401574 were verified absent at **2026-09-16T11:57:52.392845+00:00**. The immediate
post-QMP absence check ran before process exit completed; the subsequent check
confirmed both gone. Normal VM boot overlays backed this stage's private boot disk.
No user-owned VM was stopped.

## Limits and remaining work

The goal remains **17 enforced / 4 pending**; `signals` is still the first pending
area. The completion gate correctly remains unsuccessful. This patch preserves
declared VFS routing and does not promote a whole domain based on one producer.
SIGCHLD remains a separate direct-source producer to audit. Broader native-worker
and distributed qualification remains open. Old wire versions are rejected and
missing capability gates are present in source; live mixed-version rollout and
parent-death delivery during peer restart were not qualified. Existing bounded
forwarding failure behavior is retained; there is no delivery guarantee after a
peer/session is lost. No packet campaigns or penetration tests were performed.

Artifacts, exact commands, failed and successful receipts, serial logs, helper
hashes and stage patch: `build/goal19-parent-death-delivery/`.
