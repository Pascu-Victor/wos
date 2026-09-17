# Goal19 9NE: canceled child claim lifetime

Previous turn: progress. 9ND fixed and verified group-origin wait selection.
The distributed child authority and SIGCHLD contract remains incomplete.

## Source flow and patch contract

Problem: `ClaimedEvent::node` is borrowed event identity, not a lifetime reference.
Queue insertion retains the subject Task containing that embedded event. Event
removal clears the claimant's `child_claimed_event` under the lifecycle lock and
releases queue/subject references. Parent exit, tracer detach and waiter cancellation
can invalidate a claim while its value snapshot is outside the lock for usercopy.
`release_claim` and `commit_claim` currently dereference the borrowed node first.
If final reclamation intervenes, their queued/cookie checks read retired storage.
This is a source-backed interleaving, not a reported live crash.

Expected files: child_events.cpp/.hpp, deterministic child-event KTEST registration,
wait source guard and goal ledger. No Task layout, syscall or wire change.

Files read: full child event ownership/claim/removal/reparenting code; waitpid
claim/usercopy/commit loop; ptrace capture/publish_wait_event and its retry tests;
exit's prepare_process_exit ordering; Task child storage and 9NC proxy retirement.

Invariants: obtain the current event through `waiter.child_claimed_event` under the
existing lifecycle lock. Only this queue-owned link may be dereferenced. Match
both borrowed identity and cookie before any effect. A cleared, moved or replaced
claim is rejected; stale release cannot clear a new claim even at the same event
address. Preserve EFAULT retry, WNOWAIT, single-winner commit, accounting, wake
handoff, exact group origin and parent/tracer audience separation.

Locks/context: existing IRQ-save lifecycle lock; bounded validation only. No new
allocation, reference, registry scan, scheduler lock, network work or blocking.
The claim snapshot remains value-only, so abandoned syscall stacks gain no new
cleanup responsibility. Existing queue references pin the currently linked event
until the lock is released or its validated commit removes it.

ABI/wire/syscall impact: none. Public errors and payloads, credentials, peer policy
and declared VFS routing remain unchanged.

Build/test plan: actual lifecycle KTEST for parent-exit and tracer-detach removal,
releasing final subject storage before stale release/commit; canceled/reissued
claims in the same embedded event; stale-token refusal preserving a new claim,
NOWAIT and final reference release. Normal Build WOS, full isolated diagnostic
KTEST, source/host suites and owned-cluster wait/ptrace regressions.

Rollback: exact before files and receipts under
build/goal19-wait-claim-retirement/. No pending domain is promoted by this fix.

## Implementation and evidence

Both release and commit now read `waiter.child_claimed_event` while holding
`g_lifecycle_lock`, reject a missing or different identity, then validate the
queue, claimant and cookie. The header documents the copied node as borrowed
identity only. Queue removal already clears this backlink before releasing the
embedded event's subject reference. The fix therefore needs no extra ownership
on syscall stacks and cannot release a replacement claim.

Two actual-lifecycle KTESTs cover parent exit and tracer detach followed by final
subject deallocation, stale release/commit, replacement claims, same-event cookie
reissue, WNOWAIT and final reference cleanup. A separate host regression extracts
the old and current release bodies: inaccessible retired storage faults in each
old case, while both current cases return safely and preserve the replacement.
Its lock/wake shims make this pointer-validation evidence, not kernel concurrency
or a reproduced live kernel crash. The initial host compiler lookup lacked C++
headers; using `/usr/bin/g++` resolved that fixture-only issue, with the initial
receipt retained.

Normal Build WOS (`scripts/dev/build_wos.sh`, selected by the current task) and
the isolated diagnostic build passed. The freshly packaged diagnostic kernel
passed **59,830 assertions, zero failures**, including both new tests, and powered
off. Five host suites, five source suites and scoped formatting passed. No
production source changed after these kernel builds.

On the current normal kernel, the existing group-origin fixture passed at UID 0
and 104: zero-selector waits preserved the imported group, negative aliases
selected the receiver-local child, and the final imported-child wait returned
exit 37. All children and direct parents were reaped and helpers removed. The
existing signal (20), terminal (112), terminal-transition (28), ptrace (384) and
extended ptrace/signal (24) checks also passed: **568 checks** besides the two
group-wait cases. These were owned-process functional tests.

Kernel receipts in `build/goal19-wait-claim-retirement/kernels.json`:

- Normal build ID: `5748d05d86c68cbdaa12246cfb9e31da58f6b7f2`.
- Diagnostic build ID: `e1205741bb04a53d2cdacd4821e304757a386bf6`.

The private boot manifests match those kernel hashes. Diagnostic PID 2585347
was confirmed absent before the cluster launch. Both private guests synced;
PIDs 2591900 and 2591860 then acknowledged QMP quit and were confirmed absent
at **2026-09-16T16:57:02.151150+00:00**. No WOS VMs remained in that snapshot.

Goal19 remains **17 enforced / 4 pending**, first pending `signals`. This stage
does not implement distributed child-event authority, remote claim/commit or
SIGCHLD forwarding. Those remain required, including session-bound ownership,
merged group-origin selection, abandoned-claim cleanup and peer-failure behavior.
Credentials, syscall/wire ABI, errno, accounting and declared VFS routing are
unchanged. Full command receipts, source hashes and the scoped diff are retained
under `build/goal19-wait-claim-retirement/`; `validation.json` records the final
evidence checks without promoting a pending domain.
