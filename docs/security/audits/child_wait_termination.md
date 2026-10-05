# Goal19 9NT: termination during remote wait commit

Previous turn: progress.9NS qualified real cross-node PID collisions, WNOWAIT,
source group selection and direct kernel EFAULT at UID0/104. This source review
checks the next remaining signal-domain gap; the goal stays17 enforced/4 pending.

## Reconnaissance and patch contract

Problem: wait_remote suppresses every interrupt after COMMIT. Scheduler fatal
handoff deliberately rejects parked kernel frames so scoped syscall owners can
unwind. Consequently SIGKILL, hard CPU termination and requested thread/process
exit can be held behind the15-second remote operation deadline; repeated parks
also immediately cancel for an interrupt that this loop refuses to handle.
This is a source-established path, not a claimed live timing reproduction.

Expected files: waitpid.cpp/.hpp, child-event KTEST registration, wait source guard,
signal manifest evidence/index and goal/docs.
Files read: full local/remote waitpid loop; Task interrupt, mask, handler, CPU and
process-exit fields; scheduler wait cancellation/park and handoff commit; signal
validated-return admission and KILL/CPU-limit handling; process exit ordering;
remote native client close/retirement/commit/accounting and prior lifecycle tests.
Flow: syscall polls/copies/requests commit -> checks irrevocable termination ->
marks continuation detached and returns EINTR -> releases syscall owner pin ->
existing safe user-return teardown. Worker retains commit/receipt ownership for a
still-live canonical owner when a native thread retires. A dying owner follows
existing source queue/reparenting and record retirement rules.

Invariants: catchable signals and exec-quiescence alone must not turn an already
requested commit into a restartable wait. Only pending unmaskable KILL, hard CPU
KILL or an irreversible process/thread exit request can detach after commit.
Do not consume those markers or call noreturn teardown on a syscall stack.
Usercopy still precedes commit, known receipts/accounting remain worker-owned,
source outcome is never described as rolled back, and exact session/claim cleanup
is unchanged. Preserve credentials, syscall/wire ABI, errno conventions and
all declared VFS routing.
Locks: no added locks or allocation in production; read existing atomic fields.
Unsafe contexts: helper runs only on current syscall task with retained owner,
no scheduler/compute/lifecycle locks; KTEST uses private Task storage.
Allocation/blocking: existing finite syscall park only; no new peer wait.
ABI/wire/syscall impact: no layout change; fatal/retirement requests now unwind
promptly despite outstanding commit. Catchable-signal behavior is preserved.
Build/test plan: production gate KTEST covering ordinary/blocked/handler signals,
KILL, hard/soft CPU events, process and native-thread retirement, exec quiescence,
marker preservation and credentials; existing detached commit/accounting KTEST;
normal/isolated builds and live fullKTEST; source/policy/format checks; rerun real
wait semantics and child-event regressions. Deterministic live in-flight termination
and peer outage timing remain distinct evidence requirements, not inferred from
this gate test or successful baseline waits.
Rollback: exact pre-stage copies under build/goal19-child-wait-termination/before.

## Live-debug fixture addendum

The bounded test creates only its own parent/child, migrates that parent, and
uses a hardware GDB breakpoint at wki_child_wait_commit on the exact private
receiver. It verifies the Task name, pauses the exact owned source VM through
QMP after the claim, removes the breakpoint and resumes the receiver. A second
read checks that this exact token/native Task is in COMMIT before ordinary
same-UID SIGKILL. A third read may inspect exit_in_progress only while the query
record still pins that Task. Receiver procfs absence or entered exit, within2s,
proves fatal admission without a source reply; final parent status9 and recorded
process cleanup are checked after both VMs resume. All error paths resume both
owned guests and remove temporary helpers. The live case covers main-process
SIGKILL, not native-thread retirement or hard CPU expiry.

WOSDBG remains preferred for read-only inspection, but its current interface does
not support breakpoints. This fixture needs one exact existing call boundary, so
it uses QEMU's private UNIX GDB endpoint with the matching normal ELF. Hardware
breakpoints do not rewrite kernel text. No guest memory/protocol packet is edited,
no external target is used and no peer-restart or mixed-version result is inferred
from this short ordinary debugger pause.
The breakpoint address is obtained from the matching ELF's exact mangled symbol;
entry arguments use the established x86-64 register ABI before its prologue.
This avoids interpreting optimized-out source locals as actual task identity.

## Results

Normal Build WOS and the isolated diagnostic build passed. Formatting, waitpid,
signal and WKI source guards, three resource/auth-policy/wire host tests, and
policy audit/unit tests passed. Fresh KTEST passed 60034 tests with zero failures,
including the new interruption gate and existing detached-commit/accounting test.
The normal kernel build ID is e408b2b703d17eda699f9a94e23fea3152fa4807;
the diagnostic ID is b8d356bf5c65d3c0d222402ffe227224884c0367.

Both live termination cases passed on the first run. At UID0 and UID104 the
matching query was observed in COMMIT before ordinary same-UID SIGKILL. The
receiver entered exit and disappeared from procfs while the source remained
paused, observed after 291.25 ms and 287.71 ms respectively. These are observation
intervals including debugger/SSH overhead, not precise kernel latency measurements.
After resuming the source, both controllers reaped status9. All six recorded
process probes were absent and both temporary helpers were removed.

On the same candidate, all26 live collision/WNOWAIT/group/EFAULT/CPU checks and
all8 child-event cases passed at UID0/104. Their42 recorded process probes were
absent and their helpers removed. Read-only final inspection found empty dead
lists on all four CPUs of each node, empty child-signal/proxy-exit queues, and
zero active child-query/claim records. The initial inspection failed to create
a second GDB listener on node3; both guests resumed, and reusing the existing
listener completed inspection successfully. The failed tooling attempt remains
under initial-lifetime-inspection; it was not a kernel failure.

The diagnostic VM powered down after KTEST. Both private normal VMs were synced
and stopped; the host audit found no WOS VM at 2026-09-16T23:31:58.001910+00:00.
Build, fixture, image, cleanup and source evidence is recorded under
build/goal19-child-wait-termination/validation.json with a stage-only patch.

This qualifies main-process SIGKILL during an outstanding remote commit. Native
worker retirement, hard CPU expiry, concurrent usercopy and peer restart/mixed
versions are not live-qualified here. The production gate and existing KTEST
provide narrower deterministic coverage, not substitutes for those live cases.
Catchable signals, stable claim/accounting ownership, PID-collision EOVERFLOW,
credentials, syscall/wire ABI and declared VFS routing are preserved. The policy
completion gate still rejects pending signals; Goal19 remains17 enforced/4 pending.
