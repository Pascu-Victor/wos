# Goal19 9NC: source proxy child retirement

Implemented and verified. The eight retained Tasks from 9NB are reclaimed;
9NB's distributed child-event contract remains open.

## Patch contract

Problem: A completed source proxy skips child/event/waiter/tracer retirement,
leaving the confirmed parent/child reference cycles. Its finalizer can run from
the scheduler proxy-block callback, where newly blocking lifecycle forwarding
must not be introduced.

Expected files to change: Task intrusive deferred-exit link; WKI compute finalizer,
local deferred work and RX worker loop; focused kernel tests and source guard;
ordinary native orphan regression and goal ledger.

Files already read: 9NB source map and live field inspection; all four proxy
finalization call sites; submitted-row reference transfer/reclaim; scheduler
proxy-block callout; child_events::prepare_process_exit and claims; parent-death
forwarding; deferred compute RX worker, lifecycle leases and startup.

Invariants to preserve: The existing ACTIVE→EXITING transition admits exactly one
completion. Caller/row references remain intact; a no-allocation intrusive queue
owns an additional lifetime reference until final publication. Child/event/waiter/
tracer retirement finishes before has_exited, exit_notify_ready, DEAD and parent
exit event publication. The worker runs with no compute, peer lifecycle, runqueue
or local signal-admission lease. Existing payload replay and FD cleanup ownership
remain unchanged. UID/GID, syscall/wire ABI, wait status/rusage and declared VFS
routing remain unchanged.

Locks involved: A separate IRQ-save queue spinlock for pointer insertion/removal;
existing child lifecycle and scheduler locks only during their bounded helpers.
No queue lock crosses retirement, signal forwarding or scheduler wake calls.

Unsafe contexts: Finalization may originate from scheduler parking, submit return,
peer cleanup or deferred completion RX. Only the newly added lifecycle retirement
and final publication run in the compute worker outside any peer lifecycle lease.
Existing output/FD cleanup precedes enqueue and is not broadened by this patch.

Allocation/blocking assumptions: Queue nodes are embedded in Task and cannot fail
allocation after completion has been accepted. Queue wake/pop do not block. Worker
retirement may block in parent-death forwarding; other RX and inline ACCEPT/ACK
processing remain available. Process charge stays owned until retirement ends.

ABI/wire/syscall compatibility impact: None. Task layout is kernel-private. No
new message or permission grant; this repairs terminal ownership cleanup.

Build/test plan: Test the same enqueue/dequeue and retirement/publication helpers
with private Tasks, including duplicate completion, FIFO ownership, queued child
exit and live-child unlink without touching live init. Normal Build WOS and full
isolated KTEST; unchanged 9NB fixture retains the known SIGCHLD/wait failures but
must lose all eight dead-list cycles; further owned-child parent-death controls
and prior signal/ptrace regressions. Verify exact VM identities and cleanup.

Rollback plan: Exact before files and all attempts under
build/goal19-proxy-child-retirement/. No domain promotion follows from fixing
retirement; distributed claim/commit and notification remain required.

## Implementation and invariants

The existing ACTIVE→EXITING winner sets `exit_in_progress`, stabilizes completion
status/rusage, replays output and closes its resources. It then hands one retained
Task reference to an intrusive FIFO. Compute RX workers pop one retirement per
iteration outside any peer lifecycle lease, call `prepare_process_exit`, release
the process charge and publish the immutable exit event before dead-list insertion.
Queue insertion/removal use a separate IRQ-save spinlock. Neither retirement nor
network forwarding runs with that queue lock held. Existing wake tokens preserve
event-before-park delivery; startup also wakes workers for queued work.

The queue node is embedded in Task, so accepted completion needs no queue-node
allocation. Duplicate finalization loses the existing state transition and cannot
enqueue twice. The new KTEST exercises the production preparation, FIFO and
retirement helpers with private Tasks, verifies one completion and retained queue
ownership, and checks that live/queued-exit child links are released before the
parent event is published. It does not modify the real KTEST init process.

## Observed runtime results

- The unchanged eight-case child-event baseline retains four passing local-parent
  controls and four known migrated-parent failures (ECHILD and no SIGCHLD). This
  patch does not claim to restore distributed wait authority.
- All 16 new live-child retirement cases pass: UID0/104, remote/round-trip parent,
  local/remote child, parent-death registration disabled/enabled. Each direct
  parent is reaped before the observer releases the child. Disabled children see
  no signal; enabled children see exactly one SIGUSR1.
- All 30 extended parent-death regression cases and 568 existing signal, terminal,
  transition and ptrace checks pass. Fixture helpers are removed from both nodes.
- After these tests, both owned nodes were briefly QMP-paused and inspected through
  private UNIX GDB sockets using the matching normal kernel ELF. All four per-CPU
  dead lists on each node were empty, as were both deferred proxy queues. None of
  the recorded baseline/orphan Tasks remained. Both nodes were resumed. This
  replaces 9NB's actual eight-Task leak with observed reclamation; procfs absence
  alone was not used as its proof.

## Build and validation

Normal Build WOS and isolated diagnostic builds pass. Full freshly packaged KTEST
passes **59,826 assertions, zero failures**, including the new proxy-retirement
case; it powers off normally at 484.970 seconds. Five focused host suites, four
source suites, scoped formatting and fixture compilation pass. The initial WKI
and wait source guards assumed all finalization lived in one function; they were
updated to check cleanup, deferred retirement and publication ordering across the
new helpers. Their failed receipts are retained. No production change followed
the final kernel builds or runtime qualification.

Normal kernel Build ID: `d879cfee9b2e937a7fdf415085b1859f9d28df90`.
Diagnostic Build ID: `e1cbc56dedbfddb6d23c977bfb80fd87629cbbc7`.
Kernel hashes, exact packaged manifests, command results, live rows, bounded
debugger reads, source hashes and the stage diff are recorded under
`build/goal19-proxy-child-retirement/`; `validation.json` checks these receipts.

Both private guests synced before exact QMP shutdown. Their absence and absence
of other WOS VMs were verified at **2026-09-16T12:42:36.104525+00:00**.
The isolated diagnostic VM had already exited after selftest completion.

## Remaining work

Goal19 remains active with 17 enforced and 4 pending domains; signals is first.
Migrated-parent wait selectors, distributed claims/commit and SIGCHLD routing
still require the complete 9NB contract. Live peer failure during deferred
retirement is not qualified here. Existing pre-enqueue output/FD cleanup keeps
its prior contexts; this patch defers lifecycle retirement and final publication.
Credentials, UID/GID/syscall and wire ABI, wait status/rusage, authenticated peer
policy and the user's chosen declared VFS routing are preserved.
