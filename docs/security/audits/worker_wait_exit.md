# Goal19 9NU: worker wait retirement during process exit

Previous turn: progress. 9NT fixed fatal remote-wait unwinding and qualified
main-process SIGKILL. This stage tests a worker executing waitpid while its main
thread requests ordinary process exit. No production change is proposed.

Problem: existing live evidence does not exercise a native worker's pending
COMMIT during process-wide exit or verify preservation of the main exit status.
Expected files: shared termination fixture/driver, this document, goal and
signal evidence index. Files read: waitpid loop and interruption gate;
threadControl THREAD_CREATE/THREAD_EXIT and TCB publication; Task thread naming;
exit request publication, sibling selection, safe-return dispatch and cleanup;
ChildQueryRecord owner/native waiter and existing fixture/driver.
Flow: owned source child exits -> parent remote exec -> receiver worker waits ->
hardware breakpoint identifies worker before commit -> pause source -> verify
COMMIT -> same-UID USR1 to main -> handler sets a flag -> main calls _exit(23) ->
worker receives process-exit request and unwinds -> resume source -> reap status23.
Invariants: distinct worker/owner identity, existing authorization and exact
query/VM identity, no writes to kernel memory or protocol, original SIGKILL mode
retained, every error path resumes guests, owned fixture cleanup only.
Locks/contexts: no production locks or contexts change; GDB reads pinned query
state during short VM pauses. Allocation/blocking stays in host/userspace code.
ABI/wire compatibility: unchanged. Build/test: fixture cross-compile/format and
Python syntax, existing source guards; live UID0/104 on unchanged verified9NT
kernel; process status/procfs and final Task/queue inspection, then owned shutdown.
No repeat kernel build/KTEST is needed if production hashes remain unchanged.
Rollback: stage before copies under build/goal19-worker-wait-exit/before.
Limits: whole-process exit does not prove a detached worker committing for a
surviving canonical owner, hard CPU expiry, usercopy races or peer restarts.

## Results

Both cases passed on their first attempt. UID0 worker107 belonged to owner106;
UID104 worker110 belonged to owner109. Matching query records were observed in
COMMIT before ordinary same-UID USR1 to the main thread. The signal handler only
sets a flag; main then calls _exit(23). While the source remained paused, both
workers entered exit and disappeared from procfs, observed after288.34ms and
287.72ms including host debugger/SSH overhead. Both source controllers reaped
status5888 (normal exit23). All8 recorded process probes were absent; both
helpers were removed. Final read-only inspection found empty dead lists on all
four CPUs of each node, empty child-signal/proxy-exit queues and zero active
child-query/claim records.

Fixture cross-compilation, formatting, Python syntax, waitpid/signal source
checks and policy audit/unit checks pass. Normal/diagnostic kernel hashes and
relevant production source hashes remain identical to9NT. No new kernel build
or KTEST is claimed; prior diagnostic qualification was60034/0.

The shutdown helper initially rejected its stale stage-local boot-image
expectation before issuing any quit. That attempt is preserved under
initial-stop. The corrected helper verifies the exact backing image declared
in cluster.json. Both guests synced and acknowledged shutdown; the host audit
found no WOS VM at2026-09-16T23:43:16.825852+00:00. Evidence and stage patch:
build/goal19-worker-wait-exit/validation.json.

Source review identifies local exec as a separate path to a surviving canonical
owner: exec.cpp calls retire_exec_siblings after local image publication;
remote prepare alone calls wki_child_wait_quiesce. A live local-exec case must
therefore establish the worker's retirement and retained commit/accounting
completion across the replacement image. This stage does not claim that result.
Hard CPU expiry, concurrent usercopy and peer restart/mixed versions also remain
unqualified. Goal19 stays17 enforced/4 pending, first signals.
