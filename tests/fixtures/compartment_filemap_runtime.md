# Finite regular-File mapping characterization

The fixture and its explicit-output builder are admitted. Normal eight-CPU
attempt02 completed the strict execution protocol but failed shared-map
coherence, writeback and private READ-to-WRITE promotion (semantic mask35).
The seven groups are reported separately below. Kernel, libc and init sources
were unchanged for this characterization; their repairs remain separate work.

The fixture follows the frozen criterion4 contract01, public protocol01 and exact
wire-protocol02. It characterizes a same-granted regular File through seven
separate semantic groups. Completing the campaign with process exit0 is
`CHARACTERIZED_COMPLETE`; it is never a shorthand for coherent writable maps,
cache eviction safety, balanced global references or full Goal39 acceptance.

## Invocation and admitted capabilities

The Host service is UID/EUID/GID/EGID100, NNP1, authority0, locked service bits256.
It runs `/usr/libexec/compartment-filemap-runtime` without arguments. Root prepares
the result directory `/srv/compartment-filemap-results` with exact DIR0700 and
UID/GID100, with fresh backing/control paths. The Host verifies that directory's
type, mode and ownership using STAT before creating files; it does not attempt
parent-directory creation. The Host closes its ambient descriptors before
opening the exclusive files. Each successful CREATE return is stored immediately,
before asset descriptor cleanup or later clock observations.

The two actual private roots receive only these capabilities:

| Capability | Contents |
|---|---|
| Copied executable | `/app/file-probe` |
| Copied interpreter | `/lib/ld.so` |
| Copied dependency | `/usr/lib/libc.so` |
| FD9 | Its own Host-created control File, O_RDWR, no CLOEXEC |
| FD10 | The same Host O_RDWR open backing File retained by both roots |
| FD11 | The same Host O_RDONLY open backing File retained by both roots |

CREATE uses the current408-byte Spec, default32MiB private root, DENIED networking,
authority and requested privileges0, the Host's sixteen actual limits, and no
ambient descriptors or directory grant. Both roots attest local PID1/PPID0,
identities100, NNP1/authority0/locked256, all256 F_GETFD results, each grant's
access/dev/ino/size and all16 raw limit results. Private open of the Host result
path must return-ENOENT. The Host closes both source backing descriptors after
both attestations. Later raw mapping and positional I/O establish retained grant
usability; the RETAINED acknowledgement alone does not establish it.

A performs one real local FORK5 after mapping/touching the tested pages. Its child
attests local2/PPID1 and inherits those actual VAs. Host proc discovery runs
**before** child HELLO, so the child can record the independently observed physical
child ID immediately. It requires exactly one live physical child of A's exact
CREATE-return PID, Name `file-probe`, all four proc UID fields100, and fresh kill0.
This is the precise chosen order for protocol02's adjacent discovery/HELLO steps.
Physical IDs supplied to private reports are labeled Host observations.

## Ordered experiment and raw oracles

The backing File is16384 initially zero bytes. Payloads are exactly16 bytes:
marker and complement; initial zero is0/0. A store writes the complement first,
then marker with release; a reader acquires marker and reads complement. Commands
serialize writers. This does not claim a128-bit atomic snapshot.

| Group/bit | Actual calls and semantic oracle |
|---|---|
| INDEPENDENT_MAP/1 | A/B touch their independent page0 RW/SHARED maps before the first store. In16 rounds A stores M; B's existing-map reads must see M before and after SYNC. After A PWRITE P, A/B's existing maps must see P. Every raw read's16 bytes remain in its event. |
| WRITEBACK/2 | B records a pre-SYNC diagnostic PREAD without judging its value. After A SYNC0, B PREAD16 must see M; after PWRITE16, B PREAD16 must see P. After A's exit/unmaps, a reopened Host observer with exact original dev/ino/size must PREAD P. This last observation precedes B's cleanup and specifically tests A's unmap effects. It does not claim the backend stays unchanged after B later unmaps. |
| FRESH_REMAP/4 | B records old-map bytes first, then FREE0/remap/touch page0 after each PWRITE. Its fresh map must see P. A's old map remains live, so this does not replace the independent-map oracle. |
| PRIVATE_ISOLATION/8 | A/B map page2 RW/PRIVATE through the O_RDONLY grant, which current policy permits. A's16 stores and SYNC0 must preserve B's map/backing0. After materialized fork, the child stores a distinct private marker; A must retain its pre-fork final marker. |
| MATERIALIZED_FORK_SHARED/16 | Page1 RW/SHARED is touched before fork. A separate zero futex word is at+64. For each of16 rounds: exact child READY, fresh actual Host WAITING/futex_wait observation, A marker store and raw WAKE calls, then child raw WAIT0 and marker/ACK. Every zero WAKE retry is retained, with at most128 wake calls total and exactly16 successes1. Actual local consuming wait must return2/status0. |
| PROMOTION_PRIVATE_ISOLATION/32 | B first touches page3 READ/SHARED through FD11. A touches READ/PRIVATE through FD10, calls PROTECT RW0, then stores. B's existing map and backend PREAD must remain0; A must see its own marker. This tests the read-only cached-frame promotion edge separately. |
| RIGHTS_AND_RETENTION/64 | FD11 PWRITE must return-EBADF with its payload/guards unchanged; RW/SHARED MAP must return-EACCES; PROTECT of its READ/SHARED map to WRITE must return-EACCES and preserve its current bytes. These denials are distinct from permitted private writes. The complete actual pre-call payload is now saved in one existing READ event immediately before the permission calls, followed by the original post-call READ; neither complement is synthesized. Retained FD10 calls after Host source close must remain usable. |

Valid semantic mismatches record a failed bit and continue through remaining
phases and exact cleanup. Unexpected errors, invalid identity/publication, guard
damage, capacity exhaustion, missing park/wake/ACK, failed wait or deadline are
execution failures. Successful permission operations where denial was expected
are recorded as semantic failures and any unexpected mapping is explicitly freed.

The exact command schedule ends with A root seq103, B root seq134, child seq19:
HELLO/RETAINED both;16 independent rounds;16 private rounds; A FORK; Host physical
child discovery; child HELLO;16 child READY/park/A WAKE/child DONE rounds; child
private store and A private read; child EXIT_CHILD and A WAIT_CHILD; B baseline
promotion, A promotion, B read and rights; A STOP_ROOT/reap/archive; Host backend
PREAD; B STOP_ROOT/reap/archive. No consuming wait is performed early.

Root controls/progress use512-byte nonce/sequence records, full write with
commit0/fsync then final commit/fsync. Receivers double-read and require guards,
zero reserved fields, current role/nonce/global deadline and exact next sequence.
Each read/write polling phase uses a checked5-second limit within the60-second
global limit. These are userspace checks around actual calls; they do not promise
to interrupt an arbitrarily blocked kernel syscall. The root-owned VM wall bound
is the final backstop. READY is not proof of a parked waiter; Host PARK is an
independent fresh point, and WAIT0/WAKE1/ACK provide the positive completed path.

## Exact serialized evidence

All words are LE64; signed returns retain their full uint64 bit pattern. Rows
contain128 header words and12-word event slots. Root rows have320 slots; child
and Host have64. No C++ padding or full native stat is serialized. A complete
transferred16-byte positional payload and both guards are retained in paired
PREAD/PWRITE plus GUARDS events. Full144-byte FSTAT serialization is outside this
schema; actual grant access/dev/ino/size/raw FSTAT are stored in the header.

The full header/ordinary-event/special FD_VECTOR/control/request fields are
frozen in `checkpoint39-criterion4-filemap-wire-protocol02.md`. FD_VECTOR stores
eight actual signed F_GETFD results in a special12-word event, with timestamp
at word3; ordinary events have timestamp at word4. Exactly32 vectors cover all256
private descriptors. Event timestamp checks distinguish these shapes. The
generated run nonce sets its high bit and bit0, so it cannot equal the bounded
command sequences or make a nonce-XOR-sequence commit accidentally0.

Command/progress slots are0/512/1024/1536. Source request1024B is at4096, root row
at8192, child row at40960, all inside the65536-byte control File. Root alone writes
its row; the child alone writes its row. Full row commit is0 until its fsynced
final nonce write. Host archives two identical complete committed reads only
after consuming the exact root exit. Child archival also follows A's consuming
local child wait and Host A reap. Final control slots show the final command,
not every historical command; the offline validator must reconstruct the frozen
schedule using each event's command sequence, phase, round, arguments and nonce.

| File | Maximum bytes |
|---|---:|
| `root-a.bin`, `root-b.bin` |32768 each|
| `child-a.bin`, `host.bin` |8192 each|
| `control-a.bin`, `control-b.bin` |65536 each|
| `backing.bin` |16384|
| `proc-child-discovered.txt`, `proc-child-first-wait.txt`, `proc-child-last-wait.txt` |4096 each|
| `init-exit-snapshot.bin` |4160|
| `status`, `init-exit-status` |512 each|

Thirteen files total at most246848 bytes. Status files are fixed512-byte zero
padded buffers with one finite line. Unused row slots/tails remain zero; final
word is the run nonce. Overall source event caps are A<=292, B<=294, child55 and
Host22. B's ordinary path is<=293 including the new pre-call READ; an unexpected successful RO-grant RW/SHARED
MAP adds one cleanup unmap event, so its supported semantic-failure path is<=294,
still under320. The frozen wire02 B<=292 statement is retained as historical
ordinary-path arithmetic; this amendment makes the extra event explicit. All128 actual
wake attempts are included in A's worst-case budget; no zero retry is dropped.
Overflow is an execution error, not truncation or dynamic growth.

Status line:

```text
CHARACTERIZED_COMPLETE compartment-filemap nonce=N semantic-fail-mask=M events=C failures=0
```

`EXECUTION_FAIL` uses the same keys and a positive failures field. The Host emits
its actual event count, not a substitute for checking private rows. An offline
validator must reconstruct all seven groups from every raw event; semantic mask0
and a successful process exit are insufficient by themselves.

## Failure ownership and init witness

Normal paths keep all futex maps live through ACK, explicitly unmap and close
private backing descriptors, publish rows, consume child local2/status0 and each
positive Host root/status0, then close controls. No private operation depends on
a new console capability. The private raw noreturn EXIT does not unwind local
RAII objects; operational failures make bounded best-effort error publications
and exit80. Host sends SIGKILL only to its exact positive unconsumed CREATE IDs,
then tries exact consuming reaps for at most10 seconds. Namespace-init retirement
is the child backstop. Host report or guarded-PREAD failure returns through this
owned-root cleanup path rather than raw-exiting the supervisor.

Partial controls/backing/proc files and the Host execution-failure status are
kept for offline diagnosis. A whole private failure row may be inside its control
File even when no standalone archive qualifies. Root VM stop/prune is the final
bounded cleanup; this does not measure global Task/File/PID/scope reference debt.

The same binary's `--init-exit-witness` mode is a separate UID100 Host service
`compartment-filemap-witness`, depending on `compartment-filemap-probe` exit-success.
It reads the existing init status ABI for at most30 seconds, with guard words.
The exact target must occur once with generation1/restarts0/EXITED6/READY2/
pid=pgid=-1/raw wait0/error0, and a positive EXIT-reason5 transition to EXITED6
not later than the raw snapshot publication time. The4160-byte ABI1 snapshot and
512-byte status are fsynced/closed; status includes sequence and published-ns
from the same snapshot. The witness then writes only the unpadded line to its
supported inherited Host stdout for the root runner's saved observation. The
runner stops its owned VM at the120-second bound. This console
is not granted to private roots.

The independent validator binds raw snapshot and status to the
original init's exact serial spawn PID for the Host probe and `host.bin` physical
PID, and validate all headers/services/history/reserved bounds. Init's recorded
raw wait0 is the probe-exit evidence. It does not prove the witness's own exit,
natural poweroff, correct shared-map semantics or whole-Goal completion.

## Build and qualification

The builder requires explicit `--root`, `--source` and `--output`, refuses
existing binary/dependency/receipt outputs including symlinks, and uses the actual
WOS target/sysroot/C++23/libc-only hosted flags. It fixes the PT_INTERP path to
`/lib/ld.so`, emits compile and link dependency files, checks tool/source identity
before/after and gates the resulting little-endian x86_64 PIE, executable entry,
non-RWX LOAD segments, non-executable stack and exact sole NEEDED `libc.so`.
Its `PASS_ARTIFACT_ONLY` explicitly leaves complete header freshness and runtime
false. Root must hold actual complete dependency/tool/source streams in an
independent compile wrapper and review the actual ELF before any guest use.
For a fresh artifact, run from the repository root:

```sh
python3 tests/fixtures/build_compartment_filemap_runtime.py \
  --root "$PWD" --source tests/fixtures/compartment_filemap_runtime.cpp \
  --output build/compartment-filemap-runtime
```

The output, dependency files and build receipt must all be absent beforehand.
This standalone artifact receipt does not qualify execution or complete
dependency freshness by itself.

The root-owned runner/validator remain qualification helpers in ignored evidence.
They must
install/read-back current Normal kernel/initramfs/init/loader/libs/fixture,
exact console/journal dependencies, manifest and limine config before launch and
after exact stopped overlays on all outcomes. The probe and witness use supported
Host `stdio=inherit`, valid names shorter than32 bytes and one fresh topology.
Keep guest60s/phase5s/failure cleanup10s/host120s, rootless `--no-setup`, allocated
20GiB/free40GiB gates, private overlays/reflinks, exact UID/PID/start-ticks/argv
ownership, and final prune on PASS/FAIL/exception. Unrelated user VM13532 must
remain byte-identical in before/live/after ownership observations.

No cache eviction, pressure seam, remote peer, unmaterialized-fork shared fault,
concurrent dirty-writer/sync race, identical-VA negative backing, complete reference
drain or two-hour/1000-cycle qualification is claimed by this first campaign.

## Actual checkpoint45 observations (2026-10-12)

Attempt01 remains failed evidence: the strict validator rejected an actual
futex timeout slightly above5seconds. The producer had sampled its starting
clock before a second clock sample calculated the bound. The admitted correction
uses the same sample for both, checks overflow and a positive remaining bound,
and leaves the strict5-second validator unchanged.

Fresh target build, repository format and ELF/dependency checks passed before
attempt02. The executed artifact was202704bytes, SHA256
`1bea0f2d38910e65ad1db5974260a18a8fd5c4f25173474a7211b9014a534754`.
It is a libc-only x86_64 PIE with `/lib/ld.so`, no RWX LOAD and no executable
stack. Complete5312 lexical build inputs were stable;364 emitted headers and
four link inputs represent367 unique external dependencies. The subsequently
updated source header comment and builder description change documentation only;
the artifact and runtime receipts retain their exact executed-source hashes.

The unchanged strict decoder and an independent raw decoder both accept all13
files, totaling236631bytes. Actual event counts are A180, B293, child55 and
Host22. They include768 signed FD observations,64 resource-limit pairs, nine
partial grant-stat records,83 positional I/O calls with83 guard records,
16 WAIT calls with exactly5000000microseconds/raw0,16 WAKE calls/raw1, and
three exact consuming waits/status0. The workload lasted4691088microseconds.
The4160-byte init snapshot has sequence52, generation1, no restarts,
EXITED6/READY2 and actual reap0.

| Semantic group | Actual attempt02 result |
|---|---|
| INDEPENDENT_MAP/1 | FAIL |
| WRITEBACK/2 | FAIL |
| FRESH_REMAP/4 | PASS |
| PRIVATE_ISOLATION/8 | PASS |
| MATERIALIZED_FORK_SHARED/16 | PASS |
| PROMOTION_PRIVATE_ISOLATION/32 | FAIL |
| RIGHTS_AND_RETENTION/64 | PASS |

All12 prepared and12 stopped artifact readbacks match. The runner used
`--no-setup` and stopped only its owned VM through QMP at the120-second bound;
launcher0 does not establish natural poweroff. All four disposable images were
pruned, releasing200163328 allocated bytes and leaving zero disposable allocation.
The unrelated user VM retained its exact identity. Independent terminal receipt
`checkpoint39-criterion4-filemap-independent-terminal02.json` has SHA256
`fe7285b505362dce2a8d28e9449bf765e3fbab7b61953884ad9f7a0f87892b71`.
Execution characterization is complete; criterion4 and Goal39 remain incomplete.
