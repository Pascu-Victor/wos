# Goal19 9NB: child events after parent remote exec

Verified failing baseline and retained-reference diagnosis. No production change.

## Impact map and flow

- Child lifecycle: `syscalls_impl/process/child_events.{hpp,cpp}` owns retained
  parent/child links, immutable ordered child events, claims, waiter registration,
  stop/continue/ptrace notifications and reparenting.
- Wait syscall: `syscalls_impl/process/waitpid.cpp` acquires the local canonical
  owner, chooses a PID/group selector, claims a local event, copies outputs and
  commits consumption. No distributed child-event query appears in this path.
- Remote exec: `syscalls_impl/process/exec.cpp` captures signal state, drains local
  admission and publishes an accepted execution route. Source task and child links
  remain on the originating node; receiver is a different local Task.
- Remote completion: `net/wki/remote_compute.cpp` retains accepted execution rows,
  retires source proxies, and publishes their exit events. `finalize_proxy_task`
  does not currently call `child_events::prepare_process_exit`.
- Scheduler Task: `platform/sched/task.hpp` retains child membership, queues,
  signal state and references. No public PID lookup can substitute for ownership.

Current local flow: child exit → lifecycle-locked queue insertion and reference
selection → unlocked `deliver_wake` → direct SIGCHLD pending bit / event wake.
Local `waitpid` claims that queue with stable event identity across usercopy.
Remote child completion enters the same source proxy exit publication path.
Parent remote exec appears to leave that queue on the source representative.

## Invariants and contexts

Lifecycle lock protects relationships, event sequence/claim cookies and waiters.
Selected Tasks and queued events own explicit lifetime references. Wakes occur
only after lifecycle/resource locks are dropped. Job-control event production
also runs from scheduler-related contexts, so direct reuse of a forwarding helper
that can wait for remote ACCEPT would need a separate context audit.

Any fix must preserve one canonical parent/child authority, wait selectors,
stop/continue/ptrace audiences, WNOWAIT, EFAULT retry, single-consumer commit,
rusage, orphan reparenting, and exact accepted session/channel identity. Merely
forwarding SIGCHLD would not restore wait authority or retire source child links.
Declared VFS routing remains unchanged.

## Baseline plan

Use owned families at UID0 and UID104. Controls retain the parent locally with a
local or remotely executed child. Additional cases exec the parent remotely or
roundtrip after creating a local child. Child exit is gated until the replacement
parent installs a SIGCHLD handler and reports its pre-exit wait result. Capture
signal observation, exact wait PID/status, parent wait receipt and physical-node
procfs lifetime evidence. Any retained zombie is reported, not counted as cleaned;
all private guest state is discarded by exact VM shutdown after inspection.

Build the native fixture with the existing WOS sysroot, check formatting and
relevant source/policy suites, and run against the unchanged qualified 9NA kernel
on fresh private overlays using `--no-setup`. No new kernel build or KTEST result
is claimed for this baseline. Runtime is authorized directly by the user.

## Minimal implementation direction, pending runtime evidence

Treat signal notification, distributed child-event authority and source proxy
retirement as one lifecycle contract. First establish which fail in ordinary
execution; then design bounded event/query/claim/commit forwarding or retained
home-owner routing, with exact-session negotiation before remote handoff. Do not
replace missing semantics with ECHILD or reject all parents that have children.

## Observed cases

Mode0: local parent/local child. Mode1: local parent/remote child. Mode2: parent
execs to wos-3 after creating a local child. Mode3: parent execs wos-0 → wos-3 →
wos-0 after creating a local child. All children wait for the observer's release
until the replacement parent has installed its handler and completed WNOHANG.

| UID | Mode | Source parent/child | Receiver parent | Pre-exit wait | Final wait / status | SIGCHLD | Result |
|---|---|---|---|---|---|---|---|
| 0 | 0 | 108/109 | wos-0:108 | 0 | 109 / 9472 | 1 | PASS |
| 0 | 1 | 110/111 | wos-0:110 | 0 | 111 / 9472 | 1 | PASS |
| 0 | 2 | 112/113 | wos-3:107 | ECHILD | ECHILD / -1 | 0 | FAIL |
| 0 | 3 | 114/115 | wos-0:116 | ECHILD | ECHILD / -1 | 0 | FAIL |
| 104 | 0 | 117/118 | wos-0:117 | 0 | 118 / 9472 | 1 | PASS |
| 104 | 1 | 119/120 | wos-0:119 | 0 | 120 / 9472 | 1 | PASS |
| 104 | 2 | 121/122 | wos-3:110 | ECHILD | ECHILD / -1 | 0 | FAIL |
| 104 | 3 | 123/124 | wos-0:125 | ECHILD | ECHILD / -1 | 0 | FAIL |

The expected encoded exit status is37<<8 =9472. The fixture observes for2 seconds
and allows a further200ms for handler delivery. ECHILD before the child is released
already proves missing ownership; this is not just a late signal. Returning to the
source node does not recover the original parent object or its authority.

All8 direct parents were reaped status0, all children reported readiness and the
exit path, and both copies of the helper were removed. The fixture does not claim
failed receivers reaped their children. All recorded `/proc/PID/stat` opens returned
ENOENT, which initially looked consistent with cleanup but did not prove it.

## Retained task diagnosis

`/proc` uses `find_task_by_pid_safe`, whose `try_acquire` rejects EXITING/DEAD Tasks.
A dead-list inspection is therefore necessary here. WOSDBG's live catalog and
configuration were inspected; the launch descriptor has no debug endpoint, so
its live discovery cannot attach. An exact-PID/start-time check bound a private
UNIX GDB listener to node0's QMP socket. Both owned nodes were QMP-paused, a bounded
read of scheduler dead-list links and only the recorded fixture Task fields was
performed, and both nodes were resumed. The GDB batch used the unchanged matching
kernel ELF and no target writes or breakpoints. The first listener attempt used
client syntax and failed; its finally block resumed both nodes. Corrected server
syntax succeeded. Both receipts are retained.

The scan traversed exactly8 dead-list entries across4 CPUs. It found precisely
these source parent/child pairs:112/113,114/115,121/122,123/124. In every pair:

- Both Tasks are DEAD, `has_exited=1`, and retain refcount3.
- The parent is unlinked from its own parent but still owns the child list and
  child event. `exit_in_progress=0`, and `sig_pending=65536` is SIGCHLD.
- The child points back to that parent, is in ZOMBIE membership3, and retains
  exit status9472. Its ordinary exit path set `exit_in_progress=1`.
- The control family PIDs are absent from all four dead lists.

This confirms both notification stuck at the source and an actual reference
cycle, beyond the wait-ownership failure. `/proc` absence alone must not be used
as proof that all kernel Task references were reclaimed in other qualifications.
The exact typed fields, addresses and QMP pause/resume receipts are in
`task-lifetimes.json`, `gdb.log` and `task-inspection-session.json`.

## Next implementation requirements

1. Retain canonical child-event authority across parent exec, including further
   hops and newly created receiver children. Resolve PID/group selector aliases
   without granting unrelated same-number Tasks authority.
2. Preserve immutable event identity, source rusage, claim/usercopy/commit retry,
   WNOWAIT and one winner. Cross-node ordering must not invent a global clock.
3. Route SIGCHLD to the active accepted execution identity while preserving local
   admission during cutover. Event producers include scheduler-related job-control
   paths, so bounded deferred work must own references before any network wait.
4. Perform child/event/waiter/tracer retirement exactly once when a source proxy
   finally exits, before publishing its own final lifecycle. Audit all callers:
   immediate spawn completion, scheduler proxy-block completion, peer failure and
   deferred TASK_COMPLETE. The scheduler callback currently can finalize inline;
   adding a blocking parent-death forwarder there without a context design is unsafe.
5. Negotiate any new protocol before handoff and validate exact peer/session/channel
   generation. Denials, broken sessions and abandoned claims must release ownership
   without partial user-visible consumption. Existing VFS routing stays declared.

Notification-only forwarding or refusing every exec with existing children would
not satisfy the intended lifecycle semantics. The complete child-event contract
remains required; this baseline does not promote the signals domain.

## Validation and shutdown

Native fixture compilation, scoped formatting/Python syntax, waitpid/exec/signal/
remote-compute source suites, and authorization policy/unit audit passed. The
completion gate still reports pending `signals`:17 enforced,4 pending. Production
sources and normal/diagnostic kernel hashes stayed unchanged. No new Build WOS or
KTEST run is claimed; runtime used the already qualified9NA normal kernel:

- Build ID: `0dc73554313cada14a7e1488f6f5b8710bd20478`
- SHA256: `b4509e648eeb9c5de8634925096a072f64fad759be3f6e02a8bbb45812facb97`

Both guests synced, private boot backing chains were verified, and exact QMP
shutdown removed PIDs2404078/2404091. Absence was verified at
**2026-09-16T12:12:12.984058+00:00**. The eight retained Tasks were not reclaimed by
the guest; their private VM state was discarded by shutdown. No user VM was stopped.

Artifacts: `build/goal19-child-event-baseline/validation.json`, native binary,
command receipts, source/kernel hashes, serial logs, live rows and debugger reads.
