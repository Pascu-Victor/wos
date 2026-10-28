# Actual public Task placement runtime fixture

This fixture exercises the current raw WOS threadControl ABI in a fresh Normal
eight-CPU VM. It creates real pthreads, uses actual physical TIDs/current-CPU
observations and real pipe blocking/wakeups. It does not consume or fabricate
kernel Tasks, private queues, model results or a kernel critical-section witness.
It qualifies this bounded workload, not inherited CPU ceilings, unrestricted
remote execution, arbitrary image writers or the whole Goal39.

The service `task-placement-probe` runs `/usr/libexec/task-placement-runtime`
without arguments as UID/EUID/GID100 in the existing Host namespace views. The
binary explicitly selects WKI LOCAL placement in the main thread and every
worker/controller. Its dedicated fresh directory `/srv/task-placement-results`
must be UID100-owned and mode0700. Install the ELF0755 and retain its existing
WOS interpreter/libc. A second UID100 service `task-placement-exit-witness` runs
the same binary with `--init-exit-witness` after the probe's exit-success
dependency. The runner owns service configuration, image injection and topology;
this source never launches or modifies host VMs.

Compile to a unique evidence path with
`python3 tests/fixtures/build_task_placement_runtime.py --output PATH`.
The helper uses the current WOS target/compiler/sysroot and the same flags as
the independent domain fixture; it does not rebuild WOS or add rootfs aliases.
The generic libc affinity sysdeps remain outside qualification. The fixture calls
raw selectors0x104/0x105/0x106/0x107/0x108 and checks their signed errors on an
eight-CPU mask. It makes no libc signed-bit63 success claim.

## Workload and refusal boundaries

Sixteen workers first pin themselves to their actual observed CPU. During a
finite busy-yield setup, the parent assigns target0-relative destinations
`(target0_first_cpu + index) % 8` through checked single-bit affinity operations,
retrying only EBUSY. Each worker must execute on that actual destination and
successfully pin itself there again before publishing confirmation. This gives
exactly two workers on each CPU and a live peer on target0's unchanged original
CPU; setup does not assume round-robin creation survives work stealing.

Workers then nanosleep in phase0. Target0 announces a two-second nanosleep;
after10ms and before its announced midpoint the parent requires excluding that
owner to return EBUSY, with GET unchanged. The userspace announcement is not an
internal WAITING-state observation; the actual checked refusal and unchanged
policy are the measured results. A delayed/failed probe fails instead of adopting
a success. Selector checks preserve missing-TID-before-invalid-affinity and
invalid-domain-before-missing-TID ordering, and exercise domain-u32 truncation.

The parent creates exactly fresh domain2, a hard singleton on a different CPU.
Phase1 workers yield under actual contention. SET_DOMAIN includes high32bits in
the domain argument and retries only EBUSY until success. Afterward target0 must
observe the destination CPU exactly64 times in phase2 before the parent unpins
it; wrong residency aborts. GET must equal that domain mask. After the64th
observation the worker stops enforcing that earlier hard policy, because the
parent is now permitted to replace it. Full-online affinity must succeed and
GET must return0xff before phase3.

In phase3 target0 reads a real blocking pipe. The parent sends256 one-byte events,
each with an exact one-byte ACK before the next send. Parent data writes and ACK
reads are nonblocking with checked finite deadline polling. Two independent
setter pthreads each make1024 calls, alternating0xff and the two-bit subset
containing the original and destination CPUs. One getter makes1024 calls and
accepts only those two masks or EBUSY. Every controller must have a successful
call and a nonzero activity sample. Controllers wait for the first completed ACK
before making calls. They also require at least one before/after call sample in
which completed-event progress increases from a value1..255. The first64 calls
include a bounded1ms post-call sampling pause. Atomic userspace counters and
progress samples around these invocation/sampling intervals encourage/record
controller activity during the pipe-wake interval; they do not establish
simultaneous execution inside the kernel's locked critical sections.

The live target pthread is retained through all setters/getters. The parent joins
all controllers, stops workers cooperatively with an explicit pipe byte, and
joins all16 workers before reading mutable reports or unwinding their State.
Any failure while threads are live attempts one fresh, bounded durable failure
diagnostic, then performs raw whole-process EXIT55 without C++ stack unwind.
Cooperative loops check the same absolute60s monotonic deadline on their first
live poll and every64 calls thereafter, with a trivial private TLS counter per
thread. Every original workload operation, yield, pause and success assertion
remains. This limits incidental CLOCK_GETTIME writable-usercopy/TLB traffic; it
does not change the deadline or establish freedom from kernel stalls. The runner
also requires a120s VM wall deadline for blocked/broken kernel paths.
The witness independently polls for at most30s. The runner must not infer a
probe exit from launcher/QMP status or the workload's PASS line.

## Additional bounded diagnostics

MAIN writes exactly eleven fixed stage files in the same dedicated directory:
`phase-setup`, `phase-park`, `phase-domain-return`, `phase-query-return`,
`phase-phase1ack`, `phase-hard-admission`, `phase-hard-cpu64`,
`phase-full-affinity`, `phase-controller-ready`, `phase-pipe-calls-complete`
and `phase-joins`. Each contains `task-placement-runtime stage=NAME\n`, where
NAME is the suffix after `phase-`. The calls are outside loops, after the named
conditions have been checked. `pipe-calls-complete` follows all256 ACKs and all
three controller joins; `joins` follows all19 successful child-thread joins.
No mutable worker/controller report is read to write a stage diagnostic.

Each diagnostic uses fresh O_EXCL0600, a complete write, fsync and close, with
at most256 bytes. A failed MAIN stage write aborts via raw whole-process EXIT55.
`failure` contains the existing bounded `FAIL task-placement-runtime live
code=... id=... actual=...\n` line before EXIT55. Racing aborts compete for the
same O_EXCL file; a failed or losing diagnostic attempt still exits without
unwinding borrowed State. There are at most eleven stage files and one failure
file, with a3072-byte combined bound and no additional clock calls or sleeps.
These files are diagnostic only. They cannot replace any of the original23
required durable success files, their exact schemas, validation or init-reap
witness. An optional bounded offline download must preserve that distinction.

The twelve diagnostics above retain their3072-byte combined bound. One additional
optional file, `failure-context`, is attempted only when a sampled `alive()` check
detects CLOCK1 or DEADLINE2, immediately before the same existing abort and EXIT55.
It uses the same fresh O_EXCL0600, complete-write, fsync and close helper. Failed
formatting, persistence or a racing O_EXCL loss still proceeds to the original
failure-line attempt and raw whole-process exit without unwinding State.

Its exact single-line ASCII schema is:

```text
FAIL task-placement-runtime context code=C tid=T phase=P events=E hard-seen=H start-us=S deadline-us=D now-us=N
```

C is1 or2; T is the current caller's raw thread ID; P/E/H are individual acquire
loads of atomic phase/event_progress/hard_seen. S/D are immutable start/deadline
and N is the already sampled clock value, including UINT64_MAX on clock failure.
The maximum line length is225 bytes within the256-byte buffer. No additional
operation is added to successful `alive()` checks, and no mutable report is read.
These values are not a coherent tuple or a kernel progress/simultaneity proof;
racing failures may give the context and primary failure files different writers.

All13 optional diagnostic names are individually bounded to256 bytes and together
to3328 bytes. The presence of `failure` or `failure-context` forbids runner PASS.
The original23 mandatory success files and their58816-byte total bound, exact
validators, failure line, absolute60s deadline and external120s limit remain.
Diagnostic persistence can still block in a broken kernel, so the external VM
bound applies. Optional context never substitutes for any success evidence.

## Durable binary reports

Every report is a distinct O_EXCL0600 file, fully written, fsynced and closed.
An existing file is a failure, never evidence to adopt. All binary fields below
are little-endian64-bit words unless a signed word is explicitly identified.
Magic is `0x39335441534b504c`, version1. Every report's byte-size word must match
the actual exact file length. `Checks` is six words: checks, failures, first_code,
first_id, first_actual, first_expected. Require positive checks, failures0 and
all first-failure fields0. The nonce, process PID and relevant TIDs must match
across reports; require all19 child-thread TIDs distinct and distinct from the
main thread's physical TID. The20 workload binaries total53,632bytes;
including the4160-byte snapshot and both <=512-byte text receipts the bounded
payload is58,816bytes or less.

`worker-0.bin` through `worker-15.bin` are216bytes/27words:

| Words | Meaning |
|---|---|
|0..7|magic, version, bytes, nonce, PID, physical TID, role0, index|
|8..13|Checks|
|14..21|final setup CPU, first observed CPU, final setup pin mask, self-pin calls, self-pin EBUSY count, all observed CPU mask, hard observations, hard wrong-CPU count|
|22..26|pipe reads, pipe ACKs, yields, phase bitset, LOCAL selected1|

All first/final CPUs are below8. Final CPUs follow the target0-relative assignment
and cover each CPU exactly twice. Pin mask equals1<<final CPU; self-pin calls
equal busy+2. Observed masks are nonzero subsets of0xff containing first/final
CPU bits. Target0 requires exactly64 hard observations, wrong0, reads256, ACKs256
and all four phase bits0/1/2/3; other workers require hard/read/ACK counts0 and positive
yields. Setup can yield before phase0, so phase0 observations are checked only
where actually required (target0's park receipt).

`control-0.bin`/`control-1.bin` (role1 setters) and `control-2.bin` (role2 getter)
are16560bytes/2070words each:

| Words | Meaning |
|---|---|
|0..7|magic, version, bytes, nonce, PID, physical TID, role, index|
|8..13|Checks|
|14..21|calls, successes, EBUSY count, overlap-call count, observed CPU mask, summed activity samples, completed1, changing pipe-progress samples|
|22..2069|1024 ordered two-word outcomes: input mask, signed return value|

Setter input at callj is0xff when `(j + index) % 2 == 0`, otherwise the host
subset. Its result must be0 or -EBUSY. Getter input is0 and result must be0xff,
the subset, or -EBUSY. Recompute successes/busy directly from every outcome and
require calls1024=successes+busy, successes>0, overlap>0<=calls,
active_samples>=overlap, completed1 and wake_changes1..255. CPU masks are nonzero
subsets of0xff. Progress samples record userspace call intervals, including the
explicit post-call sampling pause; they are not kernel instrumentation.

`host-0.bin` is496bytes/62words:

| Words | Meaning |
|---|---|
|0..5|magic, version, bytes, nonce, service PID, main physical TID|
|6..11|Checks|
|12..16|UID100, EUID100, GID100, native CPU count8, signed LOCAL result0|
|17..20|workers16, controllers3, setup setter calls, setup EBUSY count|
|21..27|final setup CPU mask0xff, target0 TID, target0 original/final setup CPU, different destination CPU, domain ID2, singleton domain mask, two-bit subset|
|28..32|target park announcement us, parent probe us, signed excluded-affinity result-EBUSY, signed GET before, signed GET after|
|33..36|hard setter calls, hard EBUSY count, signed hard setter result0, signed hard GET singleton|
|37..40|full-online setter calls, full EBUSY count, signed full setter result0, signed full GET0xff|
|41..49|pipe roundtrips256, controller successes sum, controller EBUSY sum, controller overlap sum, changing progress sum, distinct worker TIDs16, start us, end us, closed pipe FDs4|
|50..61|12 signed selector outcomes in the order below|

Require setup calls=busy+16, hard calls=busy+1, full calls=busy+1; positive nonce,
PID/TID and bounded chronological timestamps; park probe-announcement is at least
10,000us and less than1,000,000us; park GET before/after both equal1<<original CPU.
End-start must be less than60,000,000us. Recompute controller sums from their
rows. The12 selector expected results are
`[-ESRCH,-ESRCH,-EINVAL,-EINVAL,-EINVAL,-ESRCH,-EINVAL,-ESRCH,-EINVAL,-EINVAL,0,0]`:
GET missingTID; SET missingTID/mask0; SET liveTID/mask0; SET liveTID/bit63;
SET_DOMAIN missingTID/id63; missingTID/id0; missingTID/(2^32|63);
missingTID/2^32; QUERY_DOMAIN id63/output address1; CREATE_DOMAIN zero mask;
QUERY ROOT0; QUERY GROUP1. CREATE's42-byte input is unchanged; QUERY's264-byte
output is guarded and all outside-mask loads must be0. Those synchronous query
assertions are recorded in positive host Checks, not a fabricated load model.

Successful `status` is at most512bytes and has exactly this schema:

```text
PASS task-placement-runtime nonce=N pid=P tid=T workers=16 controllers=3 cpus=0xff domain=2 destination=D hard-calls=H hard-busy=B hard-observations=64 roundtrips=256 calls=3072 successes=S busy=E overlap=K wake-changes=W failures=0 first=0
```

All values must match independently parsed binary reports, S+E=3072, K>0 and W>=3.
Every binary must pass its own checks; the PASS line alone is insufficient.
The main returns0 only after all reports and status are durably saved. Normal
receipt failure returns56; live failure exits55; invalid arguments return58.

## Actual init reap witness

`--init-exit-witness` reads the existing guarded read-only init_status ABI only.
It publishes no control request, domain or process. It saves an independent
`init-exit-snapshot.bin` exactly4160bytes and `init-exit-status` <=512bytes. Only
EAGAIN is retried. Require ABI1/size4160, positive sequence/publication time,
service_count1..16, every service name NUL-terminated within64bytes and every
history_count<=4. Require exactly one `task-placement-probe`: generation1,
restart_count0, EXITED6, READY2, PID/PGID both-1, raw wait0, last_error0 and a
positive-time EXIT5→EXITED6 history entry no later than publication time. UID,
EUID and GID must each be100. Guards surround the raw snapshot.

Successful witness text:

```text
PASS task-placement-init-exit service=task-placement-probe generation=1 restarts=0 sequence=N published-ns=T state=6 readiness=2 wait=0 error=0 history-exit=1 snapshot-bytes=4160 abi=1 uid=100 read-error=0 failures=0
```

Sequence/time must exactly match raw snapshot. Snapshot header: bytes0 sizeu32,
4 versionu32,16 sequenceu64,24 publicationnsu64,32 countu32; services start64,
stride256. Service name0[64], state64u32, readiness68u32, PID80i64, PGID88i64,
generation96u64, restarts104u64, wait112i32, error116i32, history_count136u32;
history160, stride24 (time0u64, from8u32, to12u32, reason16u32).

The terminal snapshot clears PID/PGID. Bind host-0's service PID to this owned
run's exact init spawn serial line for `task-placement-probe`, generation1;
never invent a PID binding from the cleared terminal fields. Serial witness PASS
may trigger the runner's exact-owned stop, but qualification still requires all
20 workload binaries, both statuses and raw snapshot read offline, stable source/
artifact pins, no panic, launcher0 and proven cleanup. The witness returns0 only
after its own receipts are saved, else57. This snapshot proves the probe's actual
raw init reap; it does not prove the witness's later exit or natural poweroff.

No completed disposable VM disks belong in evidence retention. The root runner
uses exact-owned rootless --no-setup/COW storage and safely prunes inactive disks
after both PASS and FAIL, preserving only immutable/current seeds and small
receipts. This fixture has not been built or run by its implementation author.

## Weighted fairness mode

`--weighted-fairness` runs three trials with four measured workers on CPUs 2
and 3. Each trial has a one-second warmup and a five-second measurement window.
Equal and unequal nice weights must satisfy the existing five-percentage-point
band, including the full uncertainty of the thread-CPU-time measurements.
Setup and all trials share one 60-second monotonic deadline.

Before creating measured workers, the controller pins itself to its current CPU.
If that CPU is not 0, an unmeasured thread inherits the same singleton affinity
and moves the runnable controller to CPU 0. The controller yields until the move
completes, verifies its CPU and affinity, then joins that thread. Joining before
the move would put the controller in a waiting state that correctly refuses an
affinity mask excluding its owner. Measured workers inherit CPU 0 before the
controller places them on CPUs 2 and 3. Yielding alone does not guarantee that
a sole runnable task becomes eligible for a remote placement change.

This mode writes `weighted-fairness.bin` (6,064 bytes) and
`weighted-fairness-status`, alongside the two exit-witness files described above.
The runner must validate all four files and the probe's consumed init exit status.
The setup thread is excluded from the twelve measured worker rows; joining it
establishes user-thread completion, without asserting kernel task reclamation.
