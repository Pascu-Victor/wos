# Goal19 9NI: child-event signal delivery

Previous turn: progress; 9NH qualified reverse child-authority admission.
Distributed waits still need asynchronous transactions and selector/accounting
integration. This stage fixes the existing direct SIGCHLD producer, which is
required independently of that wait protocol.

## Patch contract

Problem: child event delivery writes pending SIGCHLD on the source Task even
after remote exec. Lifecycle producers cannot wait for ACCEPT or a network reply.

Expected files: child_events/process helpers, Task queue links, remote_compute,
wire/auth-policy/core dispatch/resource advertisement, KTEST/host/source tests,
and goal evidence. Files read: those entrypoints, task signal admission and
execution identity, peer lifecycle admission, compute RX/submission/ACCEPT and
retirement, reliable generation-bound send, scheduler wake API, and 9NF–9NH
contracts. Existing source guards still require the old direct bit write and
must be updated to check admitted generation plus wake.

Invariants: exact retained event owner; signal admission drained before exec
snapshot; only SIGCHLD, no sender-selected signal or UID grant; exact accepted
Task/session/channel generation on receivers; no source fallback after handoff;
standard-signal coalescing; event claims, status and child accounting unchanged.
Declared VFS routing and credential/syscall semantics remain unchanged.

Locks: producers call after the child lifecycle lock. Local generation holds a
bounded signal admission through the wake. Deferred queue uses a separate IRQ
spinlock, released before compute/peer/send/scheduler calls. Workers retain one
Task reference outside all queue locks and acquire peer lifecycle admission only
in daemon context, never recursively from a compute RX handler. Retry releases
peer/compute locks before requeue or sleep. Task cleanup unlinks queued work.

Unsafe contexts: producers may run during process/scheduler transitions. They
perform only local admission/generation/wake or allocation-free enqueue. No
allocation, sleep, ACK wait or network send occurs at the producer. RX validates
bounded payloads and exact execution ownership; further hops only enqueue.

Allocation/blocking assumptions: one intrusive queued record per Task with one
reference; in-flight workers own separate references. Enqueue coalesces pending
standard signals. Daemon send may allocate transport storage but never waits for
an application response. Pending ACCEPT/transient enqueue failure requeues;
retired execution/channel and exiting Task terminate delivery without retargeting.

ABI/wire/syscall impact: new versioned child-signal message and advertised compute
resource, required before submission/handoff. Existing payload layouts, syscall
numbers and wait behavior are unchanged. No claim of distributed wait completion.

Build/test plan: Normal Build WOS, isolated freshly packaged KTEST, policy/wire
host tests and relevant source guards. KTEST covers cutover admission, queue
coalescing/retry/cleanup, malformed requests and stale execution/session refusal.
Run owned UID0/104 local/remote/round-trip child-event fixture and existing signal
regressions. Record remaining ECHILD failures separately from SIGCHLD delivery.

Rollback plan: exact before files under build/goal19-child-signal-delivery/;
preserve unrelated work and all four pending domains. User authorized live WOS
with --no-setup; no user-run runtime information is currently required.

## Implemented path

`child_events::deliver_wake` sends its retained owner to `signal_child_event`
after dropping the lifecycle lock. `try_signal_child_event_local` obtains a
local signal admission, adds the standard SIGCHLD bit, wakes the task and releases
the admission. A concurrent exec must drain that admission before its final
pending-state snapshot. A remote admission never writes the old frame.

Remote work coalesces in `ChildSignalQueue`, with one intrusive queued link and
Task lifetime reference. Pop transfers that reference to a worker, so a concurrent
producer can enqueue another notification without reusing the worker's storage.
Retry merges with any newly queued notification. Exit cleanup unlinks a queued
record and drops exactly its pin; an already popped worker independently checks
exit state before an effect or requeue.

Compute workers service this queue before acquiring the RX work item's peer
lease. Sender lookup names the exact retained Task and submitted execution, waits
asynchronously for final ACCEPT, and checks channel generation. After acquiring
the destination peer lifecycle lease it rechecks the route and current policy,
credential capability and advertised resource. Generation-bound send either
transfers ownership to the reliable stream or leaves transient failures queued.
Retirement never retargets an old notification by PID or channel number.

`TASK_CHILD_SIGNAL` (0x64) is an eight-byte versioned task ID/hop-count message;
resource 0x53494706 advertises support. All four submission preflights require it.
RX validates size/version/task ID/hop bounds before accepting fixed queue storage.
The worker binds authenticated session identity and generation to the exact
published running row, refuses completing/terminating rows, and grants only
SIGCHLD. A further exec hop enqueues work instead of recursively waiting under
that RX peer lease. Existing credential, wait-event, accounting and VFS behavior
is unchanged.

The new deterministic tests cover local admission through cutover; UID-independent
lifecycle authority; masked standard-signal coalescing; malformed payloads;
boot/authentication/session/channel mismatches; missing policy/capability;
completed execution rejection; source ACCEPT deferral; equal-PID/different-Task
refusal; queue coalescing while a worker owns popped storage; and exact reference
release. Runtime evidence below must distinguish notification delivery from the
still-missing distributed wait operation.

## Verified results

Normal Build WOS and the isolated diagnostic build passed. Scoped formatting,
five source suites (waitpid, signal, atomic signals, poll deadlines and WKI),
and the authentication-policy/wire host suites passed. Freshly packaged live
KTEST passed **59,835 assertions, zero failures**, including both new cases.
The diagnostic VM powered off at serial time 483.761 seconds; its exact PID
2806495 was verified absent before the private cluster launch.

The unchanged eight-case child-event fixture now records `sigchld=1` in every
case at UID0/104. Four local-parent controls still pass completely. All four
migrated-parent cases (remote and round-trip) now receive the missing signal,
but retain their pre/post-exit `ECHILD` wait failures. Their FAIL rows and the
fixture's exit code 1 are retained; this is not a passing distributed wait test.
All eight direct parents were reaped with status0, children reported their exit
paths, recorded procfs entries were absent and both helper copies were removed.
Matching-ELF GDB inspection briefly paused and resumed both exact owned guests:
all per-CPU dead lists, proxy-exit queues and child-signal queues were empty.
This bounded inspection supports fixture cleanup; procfs absence alone would
not establish Task reclamation.

Existing signal/terminal checks passed160, ptrace suites384, and extended
ptrace/signal checks24, totaling568. Their recorded child reaping and helper
cleanup checks passed. Both private guests2809121/2809098 were synced and stopped.
No matching WOS VMs remained at `2026-09-16T18:22:39.174622+00:00`.

Normal kernel build ID: `d0ab3c968bf63efebeac8372ff48cee1302f9500`; SHA-256: `e3251788f8ddc026716ae84be0b642ee036e02ec2e03a1a1fd926802d04c23d8`.
Diagnostic kernel build ID: `50dc858c833f7b3d66f9dc48f12a9e8d808f3129`; SHA-256: `35f66f7a2e1c4df4b2ca7c637e0db87d72d52cb4129b2e902b5f0561eb3629d5`.

Built source hashes, packaged kernel manifests and reused unchanged fixture
binaries are recorded in the stage evidence. The first KTEST build command
incorrectly combined --build-only and --no-setup; argparse rejected it before
a build. The corrected isolated build passed. The first WKI source guard still
required the old worker parking condition; it now checks both retirement and
notification progress before parking. These failed receipts are retained.

No mixed-version or live peer-failure qualification is claimed. Native-worker
notification behavior needs broader coverage. Distributed wait transactions,
selector merging, claim/commit reply-loss semantics and single-owner child CPU
accounting remain open. Goal19 remains **17 enforced / 4 pending**, first signals.
The stage validator checks the full observed scope and preserves the original
goal text; it cannot substitute for completing the remaining goal requirements.
