# Goal19 9MX: ordinary SIGKILL against a trace-stopped child

The unchanged 9MW kernel reproduces a signal-delivery gap: ordinary `kill` accepts
SIGKILL for an owned trace-stopped child, but the child does not terminate within
the fixture's two-second observation. All eight such cases fail across two nodes,
UID 0/104, and pure userspace/syscall loops. Cleanup detaches and sends SIGKILL
again; every target is then reaped with signal status 9. Recovery is not counted
as a successful primary result.

The sixteen controls pass. Explicit ptrace KILL terminates the stopped child.
SIGCONT leaves the trace stop in place, and a queued SIGUSR1 handler runs after
detach. Every case first observes a running child, attaches, consumes a stopped
wait event and verifies that progress has stopped. All 24 targets and controllers
are reaped, both helpers are removed and the private SHM attachments are released.
This is an ordinary OS functional test using fixture-owned descendants only.

## Current flow and implementation boundary

`process.cpp::wos_proc_kill` captures caller credentials and retains the exact
target. `signal_as` / `signal_local_as` apply current target authorization under
admission, generate pending bits and call `scheduler.cpp::wake_task_for_signal`.
Job-control resume handles STOP/CONT/KILL state, but it does not clear ptrace stop
state. Three scheduler selection/parking branches unconditionally park a Task
whose `ptrace_stopped` flag remains set. That is distinct from job-control stop.

`ptrace.cpp::sys_ptrace(KILL)` explicitly clears the trace-stop flag and tracing
step/syscall state before rescheduling. That control succeeds in this fixture.
The ordinary signal path does not perform an equivalent fatal release. The
source flow explains a concrete obstruction consistent with the eight failures;
the fixture does not directly read the target's kernel pending-bit storage.

The source also contains two `while (task->ptrace_stopped)` loops in
`report_syscall_stop` and `report_fatal_syscall_stop`. A scheduler-only change
cannot qualify these paths. This baseline attaches to running tasks; a loop
which happens to issue syscalls is **not** proof of a PTRACE_SYSCALL entry/exit
stop. Those stops need explicit additional test cases with the implementation.

## Impact map

| Area | Entrypoints and constraints |
| --- | --- |
| Signal authorization | `syscalls_impl/process/process.cpp`: keep stable caller/current target snapshots and denied operations effect-free. |
| Pending delivery | `platform/sys/signal.cpp`: SIGKILL must reach a validated current-stack return; preserve internal thread-retirement versus process-exit ownership. |
| Scheduler | `platform/sched/scheduler.cpp`: idle selection, running-task parking and candidate selection all observe the trace-stop state; preserve runqueue locking and remote proxy parking. |
| Ptrace | `platform/debug/ptrace.{cpp,hpp}`, `platform/sched/task.hpp`: attach, signal/exception/syscall stop publication, resume flags and saved return snapshots. |
| Child lifetime | `syscalls_impl/process/child_events.cpp`: lifecycle lock owns stop/exit events, claims and retained tracer/tracee links. Ordinary fatal delivery must not require the tracer to acknowledge a stop. |
| ABI and callers | `platform/sys/syscall.cpp`, `abi/ptrace.hpp`, WOS `ker::process::ptrace` wrapper and native fixture. No request-number or signal errno change is needed. |

Ptrace control uses VM publication for authorization/mutation; stop producers and
scheduler readers also run in syscall, interrupt and scheduler contexts. Do not
call a blocking debugger operation, allocate or acquire publication from a
runqueue-locked branch. `publish_ptrace_stop`, acknowledgment and `publish_exit`
use the lifecycle lock and distinct event slots. Preserve queued/claimed event
ownership and references when fatal delivery wins against another stop producer.

## Next patch contract

```text
Problem: Authorized ordinary SIGKILL does not terminate a trace-stopped target;
scheduler parking and ptrace syscall park loops can retain that stop.
Expected files to change: shared ptrace/Task stop admission, scheduler gate and
ptrace stop loops as required, focused KTEST/source tests, expanded live fixture.
Files already read: all impact-map entrypoints, Task pending helpers, current
ptrace KILL/CONT/detach/attach and signal/exception/syscall stop producers,
job-control resume, current-stack SIGKILL consumer and child stop/exit publication.
Invariants to preserve: one stable authorization decision; denied SIGKILL leaves
stop/pending state unchanged; SIGCONT and catchable signals do not release a
trace stop; SIGKILL remains fatal; tracer relations/events and exact Task lifetime;
safe return frames, remote proxy park guards and thread-only exec retirement.
Locks involved: existing VM publication, runqueue, lifecycle and Task state;
no new blocking or allocation from scheduler/interrupt paths.
Unsafe contexts: interrupt/exception stop publication, scheduler selection,
syscall park loops, asynchronous signal generation and fatal cleanup.
Allocation/blocking assumptions: release/admission must be bounded; preserve
existing deferred event cleanup, never depend on an external tracer resuming.
ABI/wire/syscall compatibility impact: none intended; keep ordinary kill errno,
ptrace request numbers, WKI caller mapping and declared VFS routing.
Build/test plan: actual production gate/stop tests for KILL before/during stop,
catchable/CONT controls, denied foreign caller, queued/claimed stop events,
and explicit syscall stops; Normal Build WOS, full isolated KTEST, existing
signal/ptrace suites and unchanged failing matrix on fresh private guests.
Rollback plan: retain 9MW kernel and this failing fixture/receipts; archive exact
before files for the implementation and keep the original failures immutable.
```

The fix needs coherent fatal-stop admission, not merely a successful ordinary
kill return or a test-side detach. Verify the interval in which the signal
consumer removes the pending KILL bit before exit begins: a transient bit test
must not let another stop strand termination in that interval. Also test stop
creation after fatal delivery is already pending, instead of fixing only the
already-stopped case.

## Evidence

`build/goal19-ptrace-signal-baseline/validation.json` indexes source/kernel/fixture
hashes, commands, all 24 primary outcomes, cleanup and shutdown. The standalone
fixture compiles for WOS; formatting/tidy and Python syntax checks pass. No new
kernel build or KTEST run is claimed: this baseline uses the verified 9MW normal
kernel `58f889e02b2602caa3ff1b80f1a4e17dc6e9e46e`. Its boot manifest and exact
private backing chains identify the payload; no live debugger build-ID read is
claimed.

Both guests sync and acknowledge QMP quit. Processes 2217679/2217701 are absent
at `2026-09-16T10:33:06.698035+00:00`, with no WOS VMs left running. Goal19 remains
active at 17 enforced / 4 pending domains, first pending `signals`. This observed
gap is not fixed by this baseline stage and is the next implementation task.
