# Goal19 9MV: signal producer and terminal cutover review

Historical baseline: the source window below is fixed and verified by
[9MW source terminal admission](terminal_source_admission.md). The original
source hashes and observations remain preserved; broader signals qualification
is still pending.

The source-side terminal producers still have an unleased local-delivery window.
This is a source-backed finding, not a reproduced live failure. The 9MU receiver
membership fix and its passing receipts remain valid; they do not cover this
window. Signals remains pending, with Goal19 at 17 enforced / 4 pending domains.

## Current call flow and finding

`pty.cpp::master_write` captures terminal allocation, attachment generation,
foreground group and origin under the pair lock. It drops that lock before
`pty_signal_pgrp` calls `scheduler.cpp::signal_terminal_process_group`.
The scheduler retains each selected Task, checks full terminal membership, calls
`wki_proxy_task_terminal_signal`, and directly generates local pending bits if
that helper returns no route. `revoke_terminal_attachment` uses the same pattern
for HUP/CONT after clearing the selected binding.

The route helper reads `Task::wki_execution_identity` and returns `nullopt` when
both accepted signal and proxy route IDs are zero. That is an observation, not
ownership of local delivery. Neither terminal producer acquires a local signal
admission before generating the bits. A retained Task reference prevents object
reclamation but does not prevent exec cutover.

`exec.cpp::capture_remote_exec_cutover_signals` publishes the accepted route,
waits for local signal admissions to drain, and captures pending state. The
following interleaving is therefore not excluded by these paths:

1. A terminal producer selects a retained, live source Task and observes no route.
2. Exec publishes the accepted route; the local admission count is zero.
3. Exec captures the old image's pending state for transfer.
4. The terminal producer sets a pending bit on the source after that capture.

The source has neither forwarded this event nor included it in the transfer.
This review does not claim which later source cleanup action will consume that
bit, or that the sequence caused any historical timeout. A deterministic test of
the actual producer is required with the fix. Ordinary kill and receiver terminal
delivery already own `PendingSignalTarget` admissions; copying only their route
check into a source producer does not provide that ownership.

## Producer inventory

| Producer | Authority and ownership observed | Remaining review |
| --- | --- | --- |
| Positive PID / signal zero | `wos_proc_kill` captures caller credentials once; `signal_as` retains the target and routes remotely or uses `signal_local_as` admission/current target credentials. Accepted-route race retries without local fallback. | Keep exact target and errno coverage when changing shared admission. |
| Group operations | WKI discovery identifies full logical homes; receiver admission checks current group, current credentials and exact placement. The canonical lease lasts through worker effects. | Distributed movement, retirement and compatibility receipts must be reconciled before domain promotion. |
| Terminal receiver | Versioned 9MU operation retains the accepted Task, checks current full SID/PGID under admission, and preserves the terminal grant across UID changes. | Live multi-hop transitions are not qualified by the two-node 9MU matrix. |
| Terminal source | Full allocation/generation/session/group selection and retained Task; route observation followed by unleased local generation. | Concrete cutover window above. Also preserve native worker image checks and terminal mutation ordering in the fix. |
| Ptrace KILL / trace stops | `require_traced` rechecks shared `may_trace` policy under VM publication for KILL; stop generation follows authorized signal admission. Detach intentionally permits release after authority loss. | A trace attachment and a job-control stop are separate states; do not treat existing ptrace access tests as complete interaction coverage. |
| Parent-death and child events | Lifecycle-linked, retained child/parent relationships select the recipient; parent-death registration is self-selected. SIGCHLD uses child-event ownership. | Remote-exec treatment of these producers is not proved by terminal tests. No new UID bypass is asserted here. |
| Alarm, CPU faults, SIGPIPE, SIGXFSZ | Timer, interrupted execution, retained I/O actor and file-growth guard select the originating task. | Keep attribution and old-image lifetime separate from ordinary kill authorization. |
| Exit/exec retirement and WKI cancellation | Explicit lifecycle ownership rather than an ordinary user's kill request. | Keep the 9MQ distinction between thread retirement and process termination; do not reuse cancellation for terminal events. |
| System shutdown | Power authorization precedes the internal visible-process broadcast. | Internal broadcast is not evidence that a user signal request may skip receiver policy. |

This inventory describes current source entrypoints. It does not promote the
unreviewed rows merely because their signal writes use atomic helpers. Existing
orphaned-group wording also does not establish an orphaned-group implementation:
the reviewed job-control/session/signal sources do not contain an orphan-group
transition controller. Any required behavior must be stated and verified, rather
than inferred from the group fixture's unrelated `orphan` discovery mode.

## Impact and lock map for the next patch

- Source selection: `dev/pty.cpp`, `platform/sched/scheduler.{cpp,hpp}` and
  `platform/sched/session.cpp`.
- Admission and handoff: `platform/sched/task.{cpp,hpp}`,
  `syscalls_impl/process/process.cpp`, `syscalls_impl/process/exec.cpp` and
  `net/wki/remote_compute.cpp`.
- Tests: actual scheduler/Task admission KTEST, signal/PTY/WKI source checks,
  and the existing live terminal transition fixtures.

Task route observation and local admission both use `path_metadata_lock`;
job snapshots additionally use the canonical owner's job-identity spinlock.
Job-control signal generation can acquire the owner's resource lock and wake
members. Registry lookups retain Task references and release the registry lock
before delivery. Do not hold a path/job spinlock across generation, scheduler
wakes, WKI sends or waits.

Session mutation orders VM publication before its mutation mutex. In particular,
`detach_terminal` owns both while revoking a leader's terminal. `process_exit`
and last-master hangup hold the mutation mutex without acquiring publication.
Consequently, directly calling `signal_terminal_as`, which acquires the
nonrecursive publication lock, from all these callbacks is not safe. The current
remote forwarding helper may wait for compute cutover; a local admission must
never be retained across such a wait, or it could prevent that cutover itself.

The `revoke_terminal` comment also cites CLOEXEC teardown as an example of a
caller holding publication. Current `finish_native_exec_image` closes CLOEXEC
descriptors after the scoped commit helper has returned; that comment alone is
not proof of a publication lock at that close loop. The explicit
`detach_terminal` call path is sufficient to establish the reentrancy constraint.

## Next patch contract

```text
Problem: Source terminal delivery can write after remote exec's final pending
snapshot because route observation does not acquire local admission.
Expected files to change: scheduler/Task or shared process admission helpers,
focused KTEST and source checks; update this list after choosing the lock-safe API.
Files already read: the impact-map files above, signal/job_control, child_events,
ptrace, authorization, VM guard implementation, relevant fixtures and tests.
Invariants to preserve: exact Task retention, full session/group provenance,
terminal allocation/generation, UID-independent terminal grants, job-control
generation, fatal cleanup, and no parked-frame fallback after route acceptance.
Locks involved: path metadata, job identity, resource/job-control, session
mutation and nonrecursive VM publication; compute/channel locks only after any
local admission has been released or refused.
Unsafe contexts: terminal callbacks can already own publication/mutation;
do not introduce recursive publication or remote waits under a local lease.
Allocation/blocking assumptions: local admission/generation should be bounded;
if a cohort or deferred event is needed, establish failure and cleanup semantics
before effects. Do not silently discard an event merely to avoid this design.
ABI/wire/syscall compatibility impact: preserve 9MU message/capability and
current ioctl/kill ABI, returned errno, credentials and declared VFS routing.
Build/test plan: deterministic actual-producer interleaving at route acceptance,
pre/post-cutover and refusal controls, worker and owner cases; Normal Build WOS,
isolated full KTEST, scoped formatting/source checks, fresh two-node terminal
and user-signal regressions. Multi-hop qualification remains a separate receipt.
Rollback plan: retain exact before files and stage patch; revert only this change,
keeping the 9MU receiver protocol and historical receipts.
```

The minimal strategy is to make local terminal generation participate in the
same admission/drain ownership as exec transfer, and route after admission
reports acceptance. The exact API must handle both terminal callback lock
contexts and canonical/worker ownership. Rechecking a route immediately before
the pending-bit write still leaves the window; holding a Task reference alone
also does not close it.

## Evidence and limits

`build/goal19-signal-producer-audit/inspection.json` records the exact source
hashes, function locations and checked source preconditions for the finding.
The stage's command receipts record checks actually rerun during this review.
No production implementation, new kernel build, new KTEST run or live race
reproduction is claimed. The next action is implementation and deterministic
verification of the source-side admission boundary; no user input or external
blocker is needed.

The source inspection and all four rerun source suites pass, as do the policy
audit and its unit suite. The completion gate still fails at `signals`, as
expected. The first inspection-script run incorrectly expected a `while` token
for the forwarding wait, which actually uses `for (;;)`. That assertion was
corrected against the read source; its original failure is preserved. No
production code changed to satisfy the inspection.
