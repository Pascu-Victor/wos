# Actual SMP CPU-domain publication probe

This standalone WOS userspace fixture qualifies the public CPU-domain registry
on one fresh **Normal 8-CPU kernel**. The Host supervisor forks one workload
before starting threads. That workload starts four actual creator pthreads and
two actual reader pthreads, using implemented WOS thread/futex/private-barrier
paths. It never creates or posts synthetic kernel Tasks.
The supervisor and every worker require actual UID/EUID/GID100, matching the
disposable service's selected `httpd` Host account.

Sixteen barrier waves issue 64 CREATE calls. Fresh root ID0 and group0 ID1 leave
62 slots: successful IDs must uniquely cover2..63; the final wave must produce
two `-ENOMEM` results. Each ticket0..63 has a unique normalized mask `0x80|ticket`;
alternating requests additionally contain offline bit63 to verify clipping.
Both flags remain false. Readers make exactly8 sweeps of64 IDs in each wave:
8192 queries each,16384 total. They check complete valid masks, immutable repeated
observations, exact admitted ticket/ID correspondence when available, zero
excluded/offline load rows, and output guards. Dynamic included CPU loads may
vary or be zero. The supervisor reconciles all reports, queries all64 final
slots, and requires four additional valid CREATE calls to refuse without
changing any existing mask. Allocation success alone does not pass.

The probe additionally checks exact42-byte CREATE and264-byte QUERY records,
page-end successful requests/outputs, one-byte-short/inaccessible EFAULT cases,
zero/offline-only mask EINVAL, invalid IDs64/UINT32_MAX, and untouched output for
invalid-ID refusals. A partial EFAULT copyout may change its mapped prefix.
It requires creators observed on at least two CPUs and reader queries while
the userspace creator activity counter surrounds actual CREATE syscalls.
Those samples encourage and record real SMP contention; they do not prove
overlap inside the short registry writer critical section or exhaust all
hardware interleavings. A run without the observations fails rather than being
called concurrent evidence.

This fixture explicitly selects LOCAL execution. It qualifies one kernel's
registry/public ABI; it does not qualify WKI placement, RunQueue stamp order or
data races, hard-domain stealing, Task placement, immutable native CPU ceilings,
or the full goal39 campaigns. QUERY exposes only mask/loads, so actual registry
tests and the release/acquire review must separately validate names, parent,
level and flags. Domain slots are permanent: never run this capacity test inside
user VMs or reuse a live VM's registry. KTEST parks APs before the scheduler and
cannot supply this actual SMP evidence, even when configured with8 vCPUs.

## Build and disposable installation

Use the current completed Normal Build WOS sysroot:

```sh
python3 tests/fixtures/build_cpu_domain_runtime.py \
  --output build/goal39-evidence/checkpoint36-cpu-domain-runtime
readelf -l -d build/goal39-evidence/checkpoint36-cpu-domain-runtime
```

The helper mirrors the existing WOS fixture's target/compiler/sysroot flags.
Its ELF needs `/lib/ld.so` and `libc.so`; it has no libjournal or libc++ dynamic
dependency. Compile output should use a new evidence label for each source
revision. A fixture compile is separate from Normal/diagnostic kernel builds.

Install only in an inactive agent-owned disposable COW/reflink rootfs:

- `/usr/libexec/cpu-domain-runtime`,0755.
- `/srv/cpu-domain-results`,0700,owned by its selected Host service account.
  Begin with an empty directory on local persistent FAT/XFS. Do not use tmpfs
  or pre-existing receipts; all result creation uses O_EXCL.
- Existing current `/lib/ld.so` and `/usr/lib/libc.so`; set
  `LD_LIBRARY_PATH=/usr/lib` using the same service convention as other fixtures.

Invoke the SMP probe with **no arguments**. A disposable init service fragment can mirror the
existing public compartment fixture, using the `httpd` Host account100 and
`exec=/usr/libexec/cpu-domain-runtime`, one matching `arg=` executable,
oneshot/normal/restart-never/readiness-exit-success/journal stdout, and at least
150000ms readiness timeout. No additional delegated service privileges are
needed by the current Host-only domain ABI. Do not modify the tracked service
manifest or existing compartment fixture; the VM runner owns its private overlay.

Launch a fresh one-node configuration with exactly8 CPUs, the Normal kernel,
no `--selftest`, no `sched.single_process_cpu` or migration diagnostic override:

```sh
bin/wos-cluster --launch --no-setup --config /absolute/path/private-cluster.json
```

`--no-setup` is explicitly authorized for rootless launching; isolated KTEST
launches may also use `bin/wos-ktest --no-setup`. The supervisor leaves its PID,
UID/EUID/GID, nice, TID and affinity unchanged. It modifies only its own WKI
target selection before the fork and deliberately consumes the disposable VM's
domain slots. It does not change affinity or init/parent process policy.

## Deadlines and mandatory terminal evidence

One overall60s monotonic deadline covers preflight, thread startup and workload.
Partial pthread creation publishes an abort gate before any worker enters the
uninitialized barrier; only created threads are joined. Ordinary assertions do
not return early from a barrier wave. A nonzero join result invokes actual
whole-child raw EXIT50 immediately, without reading an unjoined worker report,
destroying its barrier or unwinding shared State/Argument storage. The kernel
closes thread admission, requests sibling retirement and drains their CPU
contributions; the supervisor's nonzero exact-child exit fails qualification.
The supervisor uses raw64-bit waitpid with
WNOHANG for its exact positive fork result. On timeout it sends SIGKILL only to
that exact child and polls for at most5s more. A timed-out run remains failed
even if reaped. If it cannot reap, it avoids racing final registry probes with
the live child. Kernel hangs still require the VM runner's finite wall timeout,
suggested120s, exact-owned VM stop and cleanup.

Require init/launcher wait for **this fixture exit0**, seven correctly sized
matching binary reports, and the complete fsynced status line. A PASS string
alone is insufficient. Workload returns50 for startup/join/worker/report errors;
supervisor returns51 for any failed validation or receipt write; invalid args
return52. The separate witness mode below returns54 for any failure.
Status-write failure emits a separate FAIL stdout line.

Successful `/srv/cpu-domain-results/status` has this shape:

```text
PASS cpu-domain-runtime nonce=N child=P waited=P status=0 workers=6 waves=16 attempts=64 successes=62 refused=2 queries=16384 overlap=K creator-cpus=0xM host-preserved=1 failures=0 first=0
```

`N`, `P`, `K` are positive decimal integers; `P` must match the independently
parsed physical child ID and raw waited result; `M` must contain at least two set
CPU bits within0xff. A useful initial regex, followed by numeric/file checks:

```regex
^PASS cpu-domain-runtime nonce=([1-9][0-9]*) child=([1-9][0-9]*) waited=\2 status=0 workers=6 waves=16 attempts=64 successes=62 refused=2 queries=16384 overlap=([1-9][0-9]*) creator-cpus=0x([0-9a-f]+) host-preserved=1 failures=0 first=0$
```

## Fixed report format

All fields are native x86-64 little-endian64-bit words, no pointers. Magic is
`0x3633444f4d41494e`,version1. Signed fields use two's-complement int64; pending
result is INT64_MIN. Static size assertions in the source pin this layout.
Nonce must agree across all files, and each worker PID must equal the Host's
actual fork result. All six worker TIDs must be distinct and nonzero.

Each `creator-0.bin`..`creator-3.bin` or `reader-0.bin`..`reader-1.bin` is
**1560 bytes,195 words**:

| Word offset | Fields |
|---|---|
|0..7|magic,version,bytes,nonce,PID,TID,role,index;role0 creator/1 reader|
|8..13|checks,failures,first-code,first-ID,first-actual,first-expected|
|14..18|CPU-observation bitmap,query count,overlap count,CREATE calls,completed waves|
|19..82|64 last successful masks by domain ID;0 means not yet observed|
|83..194|16 outcomes,7 words each:ticket,input-mask,signed result,CPU-before,CPU-after,start-us,end-us|

Creators require16 calls,zero queries,16 waves,zero failures,correct ticket
`wave*4+index`,input mask and monotonic timestamps. A successful result is2..63;
only tickets60..63 may contain ENOMEM and precisely two do. Reader reports
require8192 queries,zero CREATE calls,16 waves and zero failures. Reader masks
that were observed must match the final Host mapping. Unused creator masks are
zero; unused reader outcomes retain pending/default values. Each worker opens,
writes,fsyncs and closes its own file once; no per-query disk writes or shared
inherited offsets. Header mismatch, short file, syscall or fsync failure fails.

`host-0.bin` is **1208 bytes,151 words**:

| Word offset | Fields |
|---|---|
|0..5|magic,version,bytes,nonce,supervisor PID,physical workload child PID|
|6..11|checks,failures,first-code,first-ID,first-actual,first-expected|
|12..18|signed raw waited result,wait status,timed-out,valid worker files,queries,successful creates,ENOMEM results|
|19..22|creator CPU bitmap,reader CPU bitmap,overlap count,Host state preserved|
|23..86|64 signed creation results by ticket|
|87..150|64 normalized immutable masks by ID;root/group are0xff|

Require zero Host failures/timeouts,waited==child,status0,workers6,queries16384,
successes62,refused2,Host preservation1 and exact64-ID mapping. The six binary
files plus Host file total10568 bytes; status is bounded512 bytes, far below
64KiB. Successful file fsyncs use WOS's actual retained-File FAT/XFS flush path.
Host/status persistence failure is also reflected by nonzero fixture exit.
Keep small partial reports for diagnosis rather than accepting reduced evidence.

## Independent init-reaped probe exit witness

An independent nonroot Host oneshot service named `cpu-domain-exit-witness`
must depend on the exact `cpu-domain-probe` service's exit-success readiness.
It executes the same ELF with the one optional argument
`--init-exit-witness`. Do not pass that argument to the primary SMP probe.
Use the same httpdUID100 account, zero delegated privileges, empty environment
with LD_LIBRARY_PATH, journal stdout, restart-never and readiness-exit-success.
Its argument records are the executable followed by `--init-exit-witness`.
The original no-argument probe, seven binary layouts and report paths are
unchanged. Only the witness uses the existing read-only init_status_read ABI;
it never submits controls or publishes fabricated init status.

Init's actual reap_children records the raw waitpid result; its completed
oneshot model publishes EXITED plus READY and last_wait_status through the
atomic kernel snapshot. The witness polls for at most30s using a checked
monotonic start/end clock, retries only -EAGAIN, and validates exact4160-byte
ABI1,a positive kernel-assigned sequence and positive init-published time,
1..16 services, bounded
NUL-terminated names and history counts before indexing. It requires one exact
`cpu-domain-probe` entry, generation1,restart count0,EXITED6,READY2,wait0,error0
and a positive-timestamp history entry with reasonEXIT5 and destinationEXITED6.
Malformed/unsupported snapshots, root execution, other syscall errors, deadline
or receipt write errors fail with exit54. On failure it may save a last-read
snapshot or zero initial buffer for diagnosis; those never qualify as PASS.

The witness persists `/srv/cpu-domain-results/init-exit-snapshot.bin` as the
entire4160-byte raw existing StatusSnapshot, and fsyncs/closes it. It then writes,
fsyncs and closes `/srv/cpu-domain-results/init-exit-status` (<=512 bytes). Both
are O_EXCL and must not replace historical evidence. Only after both succeed
does it emit this stdout/status line:

```text
PASS cpu-domain-init-exit service=cpu-domain-probe generation=1 restarts=0 sequence=N published-ns=T state=6 readiness=2 wait=0 error=0 history-exit=1 snapshot-bytes=4160 abi=1 uid=100 read-error=0 failures=0
```

Require positive `N` and `T`, the selected nonroot UID, and independently parse
the saved raw snapshot rather than trusting that line. A suggested regex:

```regex
^PASS cpu-domain-init-exit service=cpu-domain-probe generation=1 restarts=0 sequence=([1-9][0-9]*) published-ns=([1-9][0-9]*) state=6 readiness=2 wait=0 error=0 history-exit=1 snapshot-bytes=4160 abi=1 uid=100 read-error=0 failures=0$
```

Raw snapshot fields follow the existing init_control ABI, not a new fixture
record. Offsets and enum values for independent Host decoding:

| Byte offset | Field |
|---|---|
|0,4,8,12|u32 size,version,supervisor-state,flags|
|16,24|u64 kernel sequence,published monotonic ns|
|32,36,40|u32 service-count,reserved;three reserved u64|
|64|16 ServiceStatus records,256 bytes each;use only service-count entries|

Within each ServiceStatus:

| Byte offset | Field |
|---|---|
|0|64-byte NUL-terminated service name|
|64,68,72,76|u32 state,readiness,flags,reserved|
|80,88|i64 PID,PGID|
|96,104|u64 generation,restart-count|
|112,116|i32 raw last-wait-status,last-error|
|120,128|u64 state-since-ns,deadline-ns|
|136,140,144|u32 history-count,reserved;two reserved u64|
|160|four24-byte transitions;use only bounded history-count entries|

Each transition contains u64 timestamp at0 and u32 from-state,to-state,reason,
reserved at8,12,16,20. Require history-count<=4 and an entry with to-state6,
reason5 and timestamp>0,no later than the snapshot's positive published time.
Preserve exact64-byte name matching up to its bounded NUL; no substring match.

Actual runtime PASS now requires **all original seven binary reports/status and
the independent saved init snapshot/witness**, with matching snapshot line
sequence/time and exact service completion. The cluster launcher's exit0 after
QMP stop observes the launcher, not the fixture's own exit. Only witness PASS
stdout may trigger early VM stop; the primary probe PASS must not trigger it.
Finite VM timeout still applies if dependencies fail and the witness never runs.
This witness proves the primary probe's init-reaped exit. It cannot qualify its
own later exit, natural VM shutdown, the whole init regression, or full goal39.
Total binary evidence is14728 bytes; two text lines are each bounded512 bytes,
still comfortably below64KiB.

## Storage and preserved state

Before each batch measure allocated disposable storage/free space: one active
test topology,20GiB disposable ceiling,40GiB free floor. Use required read-only
current bases and private COW/reflink disks. Preserve unrelated/user VM13532
UID945 and all source/current build/sysroot/required seeds. On PASS,FAIL,timeout
or exception, stop only the exact-owned VM; prove PID/QMP/FD/inode/backing
references are gone; extract/hash the small reports,then prune its disposable
boot/rootfs/overlays and obsolete guestfs appliances. Do not retain completed
VM disks or reset the registry inside a live VM.
