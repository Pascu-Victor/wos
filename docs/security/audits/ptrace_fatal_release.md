# Goal19 9MY: fatal delivery through ptrace stops

Implemented and verified. The unchanged failing baseline is
retained in `build/goal19-ptrace-signal-baseline/`; this stage uses separate artifacts
under `build/goal19-ptrace-fatal-release/`.

## Patch contract

Problem: Accepted SIGKILL remains parked behind ptrace scheduler gates and syscall
stop loops, including a stop producer racing with the signal wake.

Expected files to change: Task stop gate, scheduler, ptrace implementation/header,
SIGKILL return consumer, ptrace KTEST, live fixture and goal evidence.

Files already read: Task pending/stop state, scheduler selection/debug-stop/exception
park and syscall-park cancellation, ptrace stop producers and resume requests,
signal return/handoff, process signal admission, exit/thread retirement,
child-event publication/claim/detach, syscall dispatch, ABI and fixture wrappers.

Invariants to preserve: Current signal authorization and denied-operation state;
SIGCONT/catchable signals do not release debugger stops; stop-event claims retain
their ownership; termination uses the target's own safe kernel stack; internal
exec retirement remains thread-only; WKI proxy guards and declared VFS routing.

Locks involved: Existing runqueue, VM publication and lifecycle locks. The shared
gate performs atomic reads only. Stop publication wakes after lifecycle unlock;
no new nested lock, allocation or network operation in scheduler selection.

Unsafe contexts: Scheduler/interrupt stop publication and syscall stop loops.
Fatal exit is allowed only at syscall entry/exit outside operation-owned scopes
or the existing validated user-return path. Exception stops retain their existing
saved-frame handoff; the post-park publisher rechecks fatal wake state.

Allocation/blocking assumptions: No new allocations. Ptrace park loops exit when
termination is pending. SIGKILL remains pending until exit cleanup, where
exit_in_progress is already published, eliminating a consume-before-exit gap.

ABI/wire/syscall compatibility impact: No layout/request/errno or wire change.
Task stop flag becomes atomic; private Task layout is not userspace ABI.

Build/test plan: Normal Build WOS; isolated full KTEST; unchanged 24-case baseline;
explicit syscall entry/exit and queued-stop cases, denied foreign UID control;
ordinary ptrace/signal regressions. Validate all child/helper/VM cleanup.

Rollback plan: Exact pre-edit files in this stage's before directory; original
baseline payload and failed receipts retained. Do not overwrite historical evidence.

## Implemented behavior

`Task::ptrace_stop_blocks_execution()` separates execution parking from ownership
of a debugger stop event. Pending SIGKILL, a process exit request, or exit cleanup
allows execution to reach termination. SIGCONT and catchable signals retain the
stop. The stop flag is atomic, and the predicate allocates and locks nothing.

All three scheduler trace-stop gates use that predicate. The asynchronous debug
stop path rechecks termination under the runqueue lock, and a fatal ptrace wait
cannot create a new voluntary block. Stop publication checks for a fatal wake
after releasing the lifecycle lock. For exception stops, publication occurs
after parking, closing the wake-before-park ordering with the existing reserved
task wake token and handoff machinery.

Both syscall stop loops use the same predicate. Their entry/exit boundaries may
terminate on the current syscall stack after restoring the stop snapshot; no
operation-owned RAII scope spans those boundaries. Exit disposition selection
still handles internal thread retirement. The existing user-return SIGKILL
consumer now leaves its pending bit visible until exit cleanup, which has already
set `exit_in_progress` before clearing pending state.

The change neither acknowledges nor discards a stop claim to permit fatal
execution. Existing child-event code retains and publishes the independent exit
event. The focused kernel test holds a stop claim across exit publication, then
consumes both events and verifies reference counts. Other tests exercise fatal
admission before/after stop creation and the transition into exit cleanup.

The live extension checks explicit entry/exit stop reasons, a fatal syscall stop,
a breakpoint stop, an unconsumed attach event, and SIGKILL denial from a separate
UID using only fixture-owned descendants. It records primary termination and
cleanup separately. The unchanged original executable remains the baseline.

No lifecycle/WKI protocol or routing changes are included. Broader signal
qualification remains pending, including nonterminal producers during remote
exec and complete distributed movement/retirement coverage. These tests do not
exhaustively exercise every concurrent stop-producer timing.

## Verification

Final Normal Build WOS and isolated diagnostic builds pass. Normal BuildID
`b52361d6ebff001545f03b2c340a89f7037016d8`; diagnostic BuildID
`07518bf7b8c30fcd2d70118a9c89fc8712720c99`. Boot manifests bind the packaged
payloads to frozen ELF hashes; no live debugger build-ID read is claimed.

Full final KTEST reports **59,823 passed, zero failed** at 486.221 seconds and
powers off at 486.222. Both new PtraceSignals tests pass at 248.633/248.636.
Exact diagnostic VM 2330108 is gone at 11:07:11.942979 UTC. The preliminary
run was intentionally stopped after the focused tests passed to apply a selftest
formatting correction. A later `--no-package` launch was detected using the older
payload and stopped; packaging was refreshed for the final full run. Neither
partial run is counted as final-suite evidence. Their logs, payload metadata and
shutdown receipts remain available.

The original, unchanged failing executable now passes **24/24 cases**, including
all eight ordinary SIGKILL cases, with `recovered=0`. The extension passes
**24/24 cases** across root/UID104 and both guests: explicit stop reasons 11/12
(syscall entry/exit), 5 (fatal syscall), 2 (breakpoint), and 10 (attach). Every
queued attach stop is consumed before the signal-9 exit is reaped. Each denied
UID200 sender is reaped and leaves the trace stop intact; subsequent authorized
termination succeeds. All target/controller exits and SHM/helper cleanup pass.

The first extension run failed during UID104 child setup: it attempted to
reattach root-owned mode-0600 SHM after dropping credentials. Moving the drop
after reattachment fixes only fixture setup. All twelve targets/controllers from
that partial run were reaped, SHM detached, and the helper removed before retry.
The failed source, binary and receipts are preserved. Initial fixture compilation
(namespace typo), formatting and source-assertion corrections are also retained.

Existing ptrace observation/register/memory/image suites pass **384 checks**.
Signal identity/group/query/wide cases pass **20**, terminal/session cases
**112**, and local/remote terminal transitions **28**. Scoped formatting/tidy,
Python syntax, four source suites, four host models and policy audit/unit pass.
The completion gate still correctly rejects the pending signals domain.

Both final guests sync and acknowledge QMP quit. VMs 2331712/2331730 are gone at
2026-09-16T11:13:36.453235+00:00, with no WOS VMs found. Goal19 remains active at
**17 enforced / 4 pending**. `build/goal19-ptrace-fatal-release/validation.json`
indexes commands, frozen source/kernel hashes, outcomes and exact shutdown proof.
