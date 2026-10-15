# Public readonly TMPFS mapping regression

This is a separate userspace fixture for the public readonly shared TMPFS path.
It does not change `compartment_filemap_runtime.cpp`, its frozen protocol, or the
kernel selftest catalogue. No guest execution is qualified by adding or building
these files.

The Host service runs as UID/EUID/GID/EGID100, NNP1, authority0 and locked service
privileges256. Root prepares a fresh `/srv/tmpfs-readshared-results` directory
with mode0700 and owner100:100. The Host validates that directory, closes ambient
descriptors, creates one exclusive control File and invokes actual compartment
CREATE with its current408-byte Spec, the Host's sixteen actual resource limits,
network DENIED, default32MiB private root and no authority. Only control FD9 is
granted. The copied assets are the probe, `/lib/ld.so` and `/usr/lib/libc.so`.

The private controller creates `/backing.bin` itself. Its real root is a
registered TMPFS mount constructed by the ordinary compartment API. It opens
same-inode `O_RDWR|O_NO_CACHE` and `O_RDONLY|O_NO_CACHE` descriptions, normalized
to FD10 and FD11. This deliberately exercises the supported pure TMPFS ownership
graph. The old fixture's `/srv` backing is normally XFS. Host `/tmp` may retain
unsupported XFS covered-directory ancestry. This regression neither broadens
the mount policy nor qualifies arbitrary TMPFS mount graphs.

The backing EOF is129 pages (528384 bytes). Truncate creates the logical extent;
only page0 receives an initial16-byte marker. Other pages remain sparse until
actual faults. No full-file zeroing, prefault loop, fake frame, private kernel
entrypoint or selftest hook participates.

The finite experiment performs these actual operations:

1. Map the entire129-page interval READ/SHARED through FD11. Create a pthread
   with an explicit64KiB stack. While that sibling is live, publish a second
   whole129-page NONE/SHARED binding and protect it READ. The worker reads both
   aliases, changes the second whole binding NONE then READ, and exits. Join
   records both actual protection results and both observed byte pairs. This
   covers inherited and subsequently published shared-root metadata.
   Whole WRITE and EXEC protections must return EACCES; partial unmap, partial
   NONE protection and remap must return EOPNOTSUPP. Recorded before/after bytes
   remain intact. A whole NONE/READ roundtrip additionally submits a cold page64
   to kernel usercopy while NONE: require EFAULT and unchanged control guards.
2. Require growth to return `-EBUSY` while both families hold fixed EOF. Submit
   an untouched page65 as the source of a raw PWRITE into guarded control bytes;
   this makes kernel usercopy fault the readonly source. Load page128 directly
   to exercise a different cold hardware fault. Both sparse payloads must be
   zero. A same-inode FD10 PWRITE changes page0; both already materialized maps
   and a positional FD11 read must immediately see its exact marker/complement,
   without msync or remap.
3. Close original source FD11, dup2 control FD9 into that numeric slot, and
   record the changed File identity/access. The existing mapping continues to
   read the original inode. Unmap the second complete binding.
4. The joined pthread has consumed local ID2 (monotonic namespace IDs). Fork an actual local child3. It reads its inherited map and a cold inherited
   page127, removes that whole inherited binding, opens a fresh readonly
   description of the same inode, and creates an independent129-page mapping
   at a validated different VA. It closes that source FD before proceeding.
5. The child commits READY then calls real readonly FUTEX_WAIT on a zero word
   at page1+64. Parent PWRITE changes adjacent page0 data and calls readonly
   FUTEX_WAKE on the corresponding word in its own mapping. Every zero retry
   is retained; require one actual WAKE1, child WAIT0 and exact ACK. READY alone
   is never described as an observed parked waiter. Positive WAIT0/WAKE1/ACK
   prove the completed wake path across different roots, VAs and families.
6. Child3 closes FD10 and reused FD11, commits its old-image row and invokes
   actual execve of the copied probe in `--after-exec` mode. It intentionally
   leaves its independent mapping for exec teardown. The new image has local
   PID3/PPID1, sees FD10/11 closed, records its own row and exits0. Parent consumes
   child3/status0 while its original map remains coherent and still blocks EOF
   growth. A second actual fork child4 reads inherited data and a cold page126,
   commits its own row and exits with that mapping intact, exercising normal
   process-exit retirement. Parent consumes child4/status0 and remains usable.
7. Parent removes its last whole binding, then records at most64 growth calls,
   spaced50ms after `-EBUSY`, until actual ftruncate succeeds. It closes FD10 and
   reused FD11, commits its row and exits0. This is a scoped EOF-lease release
   witness: family Fileput has returned before its lease is dropped. It does
   **not** prove the executor's RETURNED state, all node/mount tail cleanup,
   global reference balance or full Goal39 acceptance.

Raw negative syscall results retain their complete signed64-bit representation.
Unexpected errors or semantic mismatches produce an execution failure; the
decoder does not accept a reported pass alone. Payloads contain a64-bit marker
and its complement (zero is0/0). VFS operations serialize writers; no128-bit
atomic snapshot is claimed. The readonly futex word is never stored through a
mapping. WRITE/EXEC and partial/remap calls are explicit refusal probes only.

## Version1 serialized protocol

All words are little-endian64-bit integers. MAGIC is `0x3538524f544d5046`, version1,
guard `0x6d41a8c297e53b0f`. The fresh nonce is odd with its high bit set. Control
File size is147456 bytes. Each row is32768 bytes (4096 words), with a64-word
header and at most256 twelve-word events. Unused header words, event tail and
control gaps are zero. Last row word is the committed nonce. Publication writes
the complete row with commit0, fsyncs, then writes the nonce and fsyncs. The Host
archives two identical complete committed reads after consuming the controller.

| Role | Meaning | Control offset | Archive |
|---|---|---:|---|
|0|Host supervisor|separate|`host.bin`|
|1|private controller, local1/PPID0|8192|`role-1.bin`|
|2|fork child3, old image|40960|`role-2.bin`|
|3|same child3, replacement image|73728|`role-3.bin`|
|4|second fork child4|106496|`role-4.bin`|

Header words:0MAGIC,1version,2role,3nonce,4global-deadline-us,5actual-local-PID,
6actual-local-PPID,7row-start-us,8row-end-us,9event-count,10failures,11first-error,
12backing-dev,13backing-ino,14initial-EOF,15original-readonly-open-flags,
16inherited/first-VA,17second/independent-VA,18first-child-or-CREATE-return,
19second-child-return,20..23UID/EUID/GID/EGID,24NNP,25authority,26locked-privileges,
27control-dev,28control-ino,29row-bytes,30capacity,31reserved0,
32Host-observed physical CREATE PID (private rows),33..61reserved0,
62guard-XOR-nonce-XOR-role,63complement-of62. The new image has no backing/VA
header claim. Host PID and physical CREATE PID are explicitly separate from
private namespace PIDs.

Event words are `[opcode, sequence, observation-us, a, b, signed-result-bits,
x, y, z, w, v, guard-XOR-nonce-XOR-role-XOR-sequence]`. Sequence starts1, is
contiguous, and observation times are nondecreasing within the row. Worker
results are serialized after join; their observation times are record times,
not a claim that each worker syscall happened after join. The decoder's exact
ordered parser specifies all event fields, including required zero fields:

|Opcode|Name|Fields after observation time|
|---:|---|---|
|1|GRANT|fd, flags, fstat-result, dev, ino, size, mode, F_GETFD-result|
|2|TRUNCATE|10, requested-size, raw-result, actual-fstat-size, fstat-result,0,0,0|
|3|MAP|129pages, prot, returned-VA, source-fd, offset0, MAP_SHARED1, logical-id, hint|
|4|READ|base-VA, byte-offset,0, marker, complement, expected-marker, stage,0|
|5|PWRITE|10, offset0, raw-result, marker, complement, expected-marker,0,0|
|6|COPY|cold-source-VA,16, raw-result, actual-marker, actual-complement, guard, complement-guard,0|
|7|PROTECT|VA,129pages, raw-result, requested-prot,0,0,0,0|
|8|THREAD_CREATE|65536,0, pthread-result,0,0,0,0,0|
|9|THREAD_JOIN|actual-return-pointer-matches1,0, pthread-result,0,0,0,0,0|
|10|THREAD_READ|VA, logical-id,0, actual-marker, actual-complement, expected-marker,0,0|
|11|THREAD_PROTECT|VA, requested-prot, raw-result,0,0,0,0,0|
|12|CLOSE|fd,0, raw-result,0,0,0,0,0|
|13|DUP|source-fd, destination-fd, raw-result,0,0,0,0,0|
|14|FORK|ordinal1/2,0, actual-local-child-return,0,0,0,0,0|
|15|READY|child-readonly-futex-VA,0,0,0,0,0,0,0|
|16|WAIT|child-readonly-futex-VA, actual-timeout-us, raw-result,0,0,0,0,0|
|17|WAKE|parent-readonly-futex-VA,1, raw-result, attempt-index,0,0,0,0|
|18|ACK|independent-child-VA,0,0, observed-marker,0,0,0,0|
|19|EXEC_INTENT|local-child3,0,0,0,0,0,0,0|
|20|NEW_IMAGE|actual-local-PID, actual-PPID,0, actual-F_GETFD10, actual-F_GETFD11,0,0,0|
|21|EXIT_INTENT|retained-VA/0, retained-length/0,0,0,0,0,0,0|
|22|WAITPID|exact-local-child, actual-status, actual-return,0,0,0,0,0|
|23|UNMAP|exact-VA,129pages, raw-result,0,0,0,0,0|
|24|GROW_RELEASE|10, new-EOF, raw-result, actual-size, actual-fstat-result,0,0,0|
|25|CREATE|descriptor-count1,0, actual-physical-child-return, all-assets-closed1,0,0,0,0|
|26|REAP|exact-physical-child, actual-status, actual-return,0,0,0,0,0|
|27|PREAD|11, offset0, raw-result, actual-marker, actual-complement, expected-marker, guard, complement-guard|
|28|REFUSE|probe1..5, VA, raw-result, length, prot/new-length, remap-flags/0,0,0|
|29|COPY_NONE|cold-source-VA,16, raw-result-EFAULT, actual-marker, actual-complement, guard, complement-guard,0|

REFUSE probes are1whole-WRITE/EACCES,2whole-EXEC/EACCES,3partial-unmap/
EOPNOTSUPP,4partial-NONE/EOPNOTSUPP,5remap-grow-MAYMOVE/EOPNOTSUPP. Their negative
raw returns and subsequent byte observations must all match; a successful
permission change cannot be reported as an acceptable semantic variant.

Marker for tagT is `(nonce XOR (T * 0xd6e8feb86659fd93 modulo2^64)) OR1`.
Initial tag1, first hot VFS update tag2, wake-associated update tag3. READ stage
values are frozen by the decoder's ordered parser. Original FD10/11 must have
same backing identity/EOF; reused FD11 must have control identity. Temporary
open descriptor numbers are actual observations and may vary, but cannot equal
the live9/10/11 slots when a normalization DUP is recorded.

Control records at0/512/1024 are REQUEST/READY/ACK. Each has64 words:
0MAGIC,1version,2nonce,3deadline,4sequence,5state,6actor,7value,8..60zero,
61guard-XOR-nonce-XOR-sequence,62complement,63committed-nonce. REQUEST is
sequence0/state1/Host-PID/physical-CREATE-PID; READY1/state1/child-local3/
independent-VA; ACK2/state2/child-local3/tag3-marker. Receivers double-read and
check exact committed identity. The guarded32-byte COPY destination is at4096;
the cold source supplies only its middle16 bytes. Remaining control gaps stay
zero. No user-provided pass bit substitutes for these observations.

Nine files total316480 bytes: control147456, five rows32768 each, two512-byte
statuses and the4160-byte init ABI snapshot. Host status is a zero-padded line:

```text
PUBLIC_COMPLETE tmpfs-readshared nonce=N failures=0
```

The same binary `--init-exit-witness` is a separate Host service
`tmpfs-readshared-witness`, depending on `tmpfs-readshared-probe` exit-success.
It uses actual init status ABI1 and records the exact one target service's
generation1/restarts0/EXITED6/READY2/pid=pgid=-1/raw-wait0/error0 and EXIT
transition. The validator checks every service/header/history reserved bound,
the actual snapshot, matching padded status and all fixture rows. Root's runner
must additionally bind the original init serial spawn to the actual Host PID;
the decoder alone does not prove artifact, VM or Host-process identity.

## Bounds, cleanup and qualification

Global work deadline60seconds; each polling/I/O phase is at most5seconds.
Readonly wake attempts are at most128, with10ms pauses after zeros; EOF growth
attempts are at most64, with50ms pauses after EBUSY. The controller emits at most
245 events including temporary FD normalization and all retries. Capacity256 includes
every attempt. Overflow fails rather than dropping observations. Blocking
syscalls and pthread_join cannot be forcibly interrupted by these userspace
checks; Root's independent VM wall bound is required. The init witness has a
separate30second bound.

Positive child IDs and thread creation are owned immediately. Failure kills
only the exact unconsumed local fork child and attempts bounded consuming reap.
It never revokes shared memory while a worker might access it. The controller
calls process EXIT while its worker storage remains alive; actual kernel process
and namespace-init retirement release remaining threads, maps and descriptors.
Host failure kills/reaps its exact positive CREATE child. Root's proven owned-VM
stop/prune remains the final backstop on every outcome. No unrelated process is
signalled. Failed rows/control data remain small diagnostic evidence.

Reuse the unchanged explicit builder with fresh output paths:

```sh
python3 tests/fixtures/build_compartment_filemap_runtime.py \
  --root "$PWD" --source tests/fixtures/compartment_tmpfs_readshared_runtime.cpp \
  --output build/compartment-tmpfs-readshared-runtime
```

The builder refuses existing binary/dependency/receipt outputs, uses the actual
WOS sysroot/target/libc-only C++23 flags and gates ELF64 PIE, `/lib/ld.so`, sole
NEEDED `libc.so`, no RWX LOAD and no executable stack. Complete source/header/tool
freshness still needs Root's watched build wrapper and current Normal cohort.
Use a separate Normal8CPU runner derived from the existing finite runner with
fresh source/artifact/admission selectors; preserve original canonical storage
guards and exact protected-host identity observations. Launch rootless with
`--no-setup`, enforce20GiB allocated budget/free40GiB floor, private overlays or
reflinks, and prune all proven inactive owned disposable images on PASS/FAIL.

```sh
python3 tests/fixtures/validate_compartment_tmpfs_readshared_runtime.py RESULTS_DIRECTORY
python3 tests/fixtures/validate_compartment_tmpfs_readshared_runtime.py --selftest
```

The selftest uses synthetic parser vectors only and reports no guest credit.
Saved-directory PASS means the exact protocol and byte/syscall oracles passed;
it explicitly leaves source/artifact/VM identity, executor RETURNED, global
reference balance and whole Goal39 qualification false. Runtime results must be
reported only after genuine Root-owned execution and independent artifact/data
review.
