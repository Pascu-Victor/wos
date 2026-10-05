# Goal19 9MZ: parent-death signals across exec

Reconnaissance and the live failing baseline are complete. The previous turn made verified
progress: 9MY fixed fatal ptrace parking, with full KTEST and live regressions.
This stage leaves production code unchanged while checking the next signal path.

## Impact map and observed source flow

- Registration: `syscalls_impl/process/process.cpp::wos_proc_prctl` stores/loads
  the calling Task's `parent_death_signal`; fork initializes the child's value
  to zero. The current field is a plain uint32_t.
- Parent lifecycle: `child_events.cpp::prepare_process_exit` retains each child,
  reparents it under the lifecycle lock, captures its registration, unlocks,
  then directly generates/wakes the selected signal on that retained Task.
- Remote exec: `exec.cpp::capture_remote_exec_cutover_signals` publishes accepted
  routes, drains local admissions, and takes the final signal snapshot.
- Wire/receiver: `TaskExecSignals` v1 carries blocked, ignored and pending bits.
  `remote_compute.cpp::snapshot_exec_signals` and `install_exec_signals` do not
  transfer the parent-death registration.
- Scheduler: the existing WKI ownership guard prevents a signal wake from
  resuming a parked source proxy. Exact Task retention alone does not deliver an
  event to its accepted receiver or serialize it with the cutover snapshot.
- Child notifications: `child_events.cpp::deliver_wake` similarly writes SIGCHLD
  on a retained signal owner. This separate producer is not qualified by the
  parent-death baseline and requires its own lifecycle/cutover review.

The lifecycle lock owns child membership, event queues and retained references.
Wakes run after it is released. Task path-metadata locking owns local signal
admission and accepted-route publication. Remote forwarding may wait for cutover
and must not retain a local admission or lifecycle spinlock while waiting.
Syscall errno/IDs, immutable credential policy, peer/session/channel provenance,
thread retirement and declared VFS routing must remain intact.

## Baseline plan

A root observer starts its own parent/child pair under UID0 or104. The child
registers SIGUSR1 or disables the registration, then execs the helper locally or
on the peer. The replacement image installs a handler, reports its registration
and identity, and only then does the observer permit its parent to exit. It
reports the bounded signal observation and exits normally. The observer reaps
its direct parent and checks that the source child disappears from procfs; the
runner separately checks receiver disappearance. An absent procfs entry is
recorded as such, without claiming a userspace wait receipt from the reaper.

Only owned descendants and private guest helpers are involved. Preserve the
original failure results independently from cleanup. Compile/format the fixture,
run the two-node comparison on the already validated 9MY kernel, verify guest
cleanup/sync/exact shutdown, and record actual results before designing a fix.
No new kernel build or KTEST result is claimed for this source/fixture-only stage.

## Implementation requirements to resolve next

A correct implementation must define ownership of registration after remote exec
and the logical parent relationship across placements. Merely copying one field
does not route parent exit; merely forwarding a signal does not preserve GET/SET
registration semantics. Capture the lifecycle grant under its owning lock, use
local admission through generation or the exact accepted route, and validate the
receiver's process/registration identity before effects. An ordinary caller's
kill authorization and trusted task cancellation are different contracts and
must not stand in for this lifecycle grant. Preserve refusal without a local
proxy fallback, event/reference lifetimes, and compatibility with older peers.

## Observed results

On unchanged 9MY normal kernel `b52361d6ebff001545f03b2c340a89f7037016d8`, both
enabled remote cases fail: UID0 and104 replacements report registration **0**
and observe **0** SIGUSR1 deliveries in the bounded 1.5-second interval after
parent exit. The matching local cases report registration **10** and exactly
**1** delivery. All four disabled-registration controls pass. Total: **six
controls pass, two remote enabled cases fail**. This is not a production fix.

Every replacement reports readiness before the observer releases its parent.
All eight direct parents are reaped with status0, all eight targets report their
final observation, and source plus actual receiver procfs entries disappear.
Both helpers are removed. These disappearance checks do not claim the observer
received the orphaned child's wait status from init.

`build/goal19-parent-death-baseline/validation.json` records the exact outcomes,
frozen source/kernel/fixture hashes, backing chains and shutdown receipts. No new
kernel build or KTEST is claimed; this stage uses the already validated payload.

## Next patch contract

```text
Problem: Remote exec loses the self-selected parent-death registration; parent
exit generates only on the source Task and does not serialize with exec cutover.
Expected files to change: Task registration accessors; process prctl/fork;
child_events parent-death producer; shared signal admission; exec signal snapshot;
WKI wire/capability/dispatch and remote-compute receiver; focused KTEST/live tests.
Files already read: Task and signal admission, process prctl/fork, parent exit
and SIGCHLD selection, remote exec prepare/cutover, exec signal wire/snapshot/
installation, accepted-row terminal forwarding/receiver, scheduler proxy guard.
Invariants to preserve: Registration belongs to the child, not caller UID0;
one exact retained lifecycle subject; current receiver registration controls
delivery; no signal on a recycled PID or old proxy frame; disabled registration
has no effect; denied/incompatible operations cannot publish partial state;
saved-ID/set-ID behavior, thread retirement and declared VFS routing unchanged.
Locks involved: Existing lifecycle, Task metadata/admission, VM publication and
compute/session locks. Registration currently has unsynchronized plain-field
access; give its reads/writes a defined synchronization/linearization point.
Unsafe contexts: Parent exit and reparenting; remote-exec acceptance/commit;
RX dispatch must defer any operation that waits for cutover or channel progress.
Allocation/blocking assumptions: Retain the exact child while dropping the
lifecycle lock. No local admission or spinlock across a remote send/wait.
ABI/wire/syscall compatibility impact: Preserve prctl IDs, errors and pointer
copy behavior. Negotiate registration/notification support explicitly; do not
repurpose reserved v1 fields or silently accept unsupported older peers.
Build/test plan: Keep this unchanged eight-case failing executable. Add receiver
disable/change/enable after exec, local/remote and further-hop transitions,
actual producer admission/cutover tests, wrong/stale accepted-row refusal and
reference accounting, then Normal Build WOS and isolated full KTEST. No packet
campaigns: ordinary owned-process functional tests only.
Rollback plan: Separate implementation artifacts and exact before files; retain
the current failed baseline, payload, all controls and cleanup receipts.
```

A candidate design is to forward the parent-death **event** along the accepted
execution chain, with the receiver selecting its current registration. Sending
only the source's captured signal number would miss a receiver that enabled or
changed its registration after exec. This is a design direction requiring the
commit/publication and compatibility review above, not an implemented contract.
SIGCHLD needs a separate assessment; its registration, audience and event queue
semantics must not be inferred from this parent-death fix.

Both guests synced and acknowledged QMP quit. Exact owned VMs 2335525/2335529 are absent at 2026-09-16T11:25:38.571120+00:00. No WOS VMs remain on the validation topology. Goal19 stays active at17 enforced/4pending.
