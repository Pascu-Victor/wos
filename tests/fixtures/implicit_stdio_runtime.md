# Finite public implicit stdio probe

This fixture exercises real public EXEC/SPAWN creation and FORK followed by native
EXECVE. Its target is preservation of the exact explicitly admitted descriptor
set in private compartments. Actual target builds, formatting and runtime receipts
are recorded separately in the goal ledger. Compilation alone does not establish
public runtime qualification. The complete preparation/read-pin protocol is
`checkpoint39-implicit-stdio-public-protocol03.json` in ignored evidence.

Install the libc-only fixture as `/usr/libexec/implicit-stdio-runtime`. Run its
Host service as UID/EUID/GID/EGID 100 with all sixteen inherited limit pairs
recorded explicitly: fifteen are UINT64_MAX/UINT64_MAX, and NOFILE (resource7) is
256/256. The runner prepares `/srv/implicit-stdio-results` with mode0700 and
UID/GID100, fresh output names, exact immutable artifact/config bindings and
separate rootless disposable images before launch. The coordinator verifies that
prepared directory type, mode and ownership before creating output. The fixture refuses existing
output files rather than reusing prior evidence. No directory or device is
granted into a private compartment.

The Host coordinator makes two sequential native private roots, each with a
fresh PID/mount/IPC namespace, DENIED networking, local root PID1/PPID0, zero
authority, locked zero service privileges and NNP1. It copies only its executable,
`/lib/ld.so`, `/usr/lib/libc.so` and a nonce-bearing 64-byte marker into the fresh
root. Dumpability may change during execution and is outside the oracle.

Each private root executes four cases, consuming each returned positive visible
PID exactly once with a specific raw WAITPID and requiring raw status0:

| Case | Actual public operation |
|---|---|
| 1 | PROCESS3 EXEC1 creates a child. |
| 2 | PROCESS3 SPAWN43 with null options creates a child. |
| 3 | PROCESS3 SPAWN43 with an actual nonnull 72-byte SpawnOptions, size72/version1 and every other field zero. |
| 4 | PROCESS3 FORK5, then native EXECVE24 in the fork child. Successful replacement cannot return into the old image. |

The first private root grants only regular read/write FD9. Every child must retain
FD9 and have FD0–2 absent. The second root additionally grants three regular
Files as stdin/read-only, stdout/write-only and stderr/write-only. All ordinary
creation cases retain those exact dev/inode/size/access identities. The fork
child for case4 marks only FD2 CLOEXEC before actual EXECVE; the replacement must
retain FD0/1/9 and have FD2 absent. Child local PIDs are greater than1, tied to the
actual positive creation return and consumed wait, without assuming PID2–5.

After both roots are consumed and all temporary source descriptors closed, the
Host coordinator closes every descriptor except its newly installed FD9. Three
owned Host controls then execute cases1,3,4. Actual legacy creation and native
EXECVE must fill FD0/1/2 with character-console Files and access modes0/1/1;
the nonnull options case must keep FD0–2 absent. The two console controls must
observe the same actual dev/inode across all three console slots. No fixture
path opens, reads or writes console. Only raw FCNTL and guarded FSTAT inspect it.

Every root and replacement entry reads its immutable FD9 request twice, checks
its fresh nonce/wave/deadline, checks its pathname marker, and records raw
UID/EUID/GID/EGID, NNP, authority ceiling and locked privilege mask. All sixteen
actual guarded GETRLIMIT results must match the explicit admitted request. It
records raw F_GETFD for all256 slots, with exactly the expected set present;
loader and marker descriptors must be closed before this census. Four guarded
FSTAT observations retain every byte of the actual 144-byte outputs for FD0/1/2/9.
On EBADF the entire prefilled A5 output must remain untouched. Successful stat
identity/type/size fields are checked; timestamps are actual observations and
are not assumed constant across IO.

Regular grant usability is tested by actual raw VFS5 PREAD26/PWRITE27 at offset0.
Before either call, the preceding raw descriptor/guarded stat evidence must prove
an exact expected regular grant or an absent descriptor. An unexpected private
console skips IO and fails. Host console always uses the INT64_MIN skip sentinel.
Private regular stdin returns its exact64-byte nonce token; writing stdin returns
EBADF. Reading stdout/stderr returns EBADF and writing each returns64 with a
case-specific token without increasing the file size. The case4 CLOEXEC stderr
slot must return EBADF for both calls. After consuming the second private root,
Host reads the exact final stdout case4 and stderr case3 tokens using owned
regular paths. No trusted report flag substitutes for the saved data oracle.

General report/marker IO uses bounded positional pread/pwrite with EINTR retries
and monotonic checks before and after calls. Each of the six dedicated stdio
probes makes one raw call, preserves its actual signed return (including EINTR),
and fails a late/regressed-clock result separately. A single60-second global
deadline caps the coordinator. Private waves and individual waits/IO phases have
at most5 seconds; receipt/owned cleanup recovery is bounded separately by5 seconds
and cannot turn a timed-out run into PASS. Positive child ownership is recorded
before subsequent IO, clocks or closes. Recovery targets only the exact owned
positive PID with SIGKILL9 and a bounded consuming wait, never wait(-1).

## Saved wire format

All words are little-endian uint64; signed raw syscall returns are preserved as
two's-complement bit patterns. Magic is `0x39535444494F5052`, version1; the guard
is `0xA596C387E1D24B6F xor nonce xor (wave<<48) xor (case<<32) xor selector`.
Root wave/case is1/0 or2/0; private leaves use case1–4; Host leaves wave3 use
case1,3,4. Kind1 denotes a root,2 a private leaf,3 a Host leaf. Actual replacement
entry sentinel is `0x45584543454E5452`. A returned EXECVE is recorded as a failure,
never interpreted as a successful new-image entry.

Each ledger begins with one immutable512-byte request, then fixed4096-byte rows.
The two private ledgers each contain root row0 and child rows1–4. Host contains
three rows at indices0/1/2 for cases1/3/4. Row offset is512+index*4096.

| Request words | Contents |
|---|---|
| 0–5 | magic, version, nonce, wave, Host physical PID, deadline-us |
| 6–12 | UID, EUID, GID, EGID, NNP, ceiling, locked privileges |
| 13–16 | row count, parent stdio mask, marker bytes64, marker hash |
| 17–20 | report dev, inode, exact ledger size, access2 |
| 21–29 | three grant triples: dev, inode, size64 |
| 30–31 | request guard(selector `0x52514553`, case0), its complement |
| 32–63 | sixteen exact soft/hard limit pairs in resource order0–15 |

Host request stdio mask0 describes its initially closed parent. Expected Host
leaf mask derives independently from its case:7 for1/4,0 for3.

| Row words | Contents |
|---|---|
| 0–7 | magic, version, kind, nonce, wave, case, actual PID, PPID |
| 8–14 | UID, EUID, GID, EGID, NNP, ceiling, locked privileges |
| 15–23 | checks, failures, first-failure code, start/end/deadline-us, expected stdio mask, actual present count, unexpected count |
| 24–31 | marker open/read/close raw results, marker bytes/hash, actual CLOEXEC action or unused0, replacement entry, row guard(selector0) |
| 32–127 | sixteen6-word records: raw GETRLIMIT, before guard(selector `0x100+resource`), soft, hard, after guard(complement), resource |
| 128–383 | all256 actual signed raw F_GETFD results |
| 384–479 | four24-word slots for FD0/1/2/9: fd, raw F_GETFD, raw F_GETFL, raw FSTAT, before guard(selector `0x200+fd`), all18 actual stat words, after guard(complement) |
| 480–503 | root-only four6-word waits: case mode, raw create, raw consuming wait, raw status, start/end-us; leaf words zero |
| 504–509 | actual signed pread0/pwrite0/pread1/pwrite1/pread2/pwrite2 results, or the defined skip sentinel |
| 510 | zero |
| 511 | complement of row header guard |

Each writer writes the whole row with commit0, fsyncs, writes the terminal commit
word and fsyncs again. The consuming parent reads each completed row twice and
requires exact equality/fresh guards. Successful check counts derived from source
are: private absent leaves291/root303; private granted ordinary leaves293/root305;
private granted case4 leaf292; all three Host leaves291. The independent
auditor derives every actual oracle from saved fields, without trusting those counts
or `failures==0` alone.

The2048-byte Host summary has words0–12: magic/version/nonce/four IDs/PID/start/
end/globaldeadline/checks/failures. Word13 is the first failing Host check ordinal
(one-based), or zero on success. The successful wire and check count are unchanged.
Words14–22 record initial
ambient closes, roots created/consumed, Host children created/consumed, root rows
read, total rows read13, final ambient closes and two final regular tokens checked.
Words23/24 are actual final stdout/stderr read returns;25/26 their64-byte hashes.
Words27–31 are zero. Words32–43 retain the two root waits (mode54);44–61 retain
three Host waits. Words62–253 are zero,254 guard(selector `0x484F5354`, wave/case0),
255 its complement. This summary is backed by actual rows/requests/tokens/waits.

## Durable caps and init witness

| Exact unique filename | Bytes |
|---|---:|
| absent-ledger.bin | 20992 |
| granted-ledger.bin | 20992 |
| host-ledger.bin | 12800 |
| host.bin | 2048 |
| init-exit-snapshot.bin | 4160 |
| status | 512 |
| init-exit-status | 512 |
| marker-absent | 64 |
| marker-granted | 64 |
| marker-host | 64 |
| stdin-regular | 64 |
| stdout-regular | 64 |
| stderr-regular | 64 |

The exact aggregate is62400 bytes, below65536. No config/proc/serial endpoint is
silently part of this guest durable budget; runner host logs/configs are
separate small immutable evidence. Every qualifying guest file has the exact
length above. Missing, partial, stale, duplicate or uncommitted outputs fail.

The same ELF with `--init-exit-witness` observes actual init status for
`implicit-stdio-probe`, bounded by30 seconds, and persists the complete4160-byte
ABI1 snapshot plus a512-byte status. Qualification requires the actual service
generation1/restarts0, EXITED6/READY2, PID/PGID-1, raw wait0/last error0, with a real
EXIT transition timestamp not later than publication. The witness performs no
console IO. Its own later exit, VM natural shutdown, complete Task GC, wider WKI
and full Goal39 acceptance remain separate questions.

The builder uses the current WOS clang++ target/sysroot, C++23/O2/debug info,
Wall/Wextra/Werror, no exceptions/RTTI and libc-only linkage. It rejects an
existing output and supports an explicit source for a separate build. Each fixture
revision requires fresh target build and formatting receipts before runtime
qualification. Existing production and StageB evidence is recorded separately.
