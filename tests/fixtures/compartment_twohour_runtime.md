# Two-hour public compartment fixture

This fixture exercises one original Host controller, two original private roots,
and one persistent worker in each private root. It uses the current public
compartment ABI, a direct CREATE for A, and the real `wos-compartment` CLI for B.
It changes no kernel, syscall, init timeout, FD, File, Task or wire layout.

Build the guest and the current CLI together with the standalone helper below.
Use the resulting ELFs with their recorded source, toolchain and dependency
identities. Source paths, debug paths or compiler-command changes can alter
artifact bytes, so bind hashes from actual fresh outputs. The saved-data decoder
provides offline report analysis; an artifact build or saved-data decode does
not qualify VM execution or a two-hour interval.

## Build the guest and the current CLI

From the repository root, after an ordinary target sysroot and toolchain exist:

```sh
python3 tests/fixtures/build_compartment_twohour_runtime.py \
  --root "$PWD" --output-dir /tmp/wos-twohour-fresh-build
```

The output directory must not exist. The builder compiles
`compartment_twohour_runtime.cpp` and the current canonical
`modules/compartment/src/main.cpp`; it does not duplicate or freeze the CLI in
the fixture. Explicit `--source-dir`, `--sysroot`, `--llvm-bin`, and
`--resource-dir` overrides support a private source mirror and target toolchain.
The default resource directory is the repository's current Clang22 layout.

This standalone artifact helper is separate from Normal `Build WOS`. It uses the
current hosted WOS target and C++23/O2/debug/PIE/no-exceptions/no-RTTI command
shape, canonical ABI headers, target libc and `/lib/ld.so`. Compilation and
linking are separate commands so the temporary object is a recorded, owned
intermediate. Default compiler configuration and implicit system header search
are disabled, and resource/sysroot include directories are explicit. These
command and filename differences do not promise byte-identical old artifacts.

The standard-library-only helper imports no Host test, historical builder or
saved receipt. It preholds candidate input trees and then validates every actual
emitted external dependency against that pre-command set. Selected source,
compiler, linker, header, library and alias pins are compared after compilation.
Each completed artifact must be x86_64 ET_DYN with one `/lib/ld.so` interpreter,
one NX stack, file-backed executable entry, no writable executable LOAD, and
exactly one `NEEDED` dependency, `libc.so`.

One 60-second deadline includes commands, checks and cleanup, with five seconds
reserved before starting another command. Output is capped at16MiB, private
scratch at64MiB, each compiler file at8MiB and each log at1MiB, with at least1GiB
free before starting. Compiler sessions are sequential, exact-owned and reaped;
scratch is removed only after matching its saved identity and proving owned
descendants absent. Existing outputs, artifacts, sysroots, VM images and shared
VM locks are untouched. This helper never launches either target ELF.

Outputs are `compartment-twohour-runtime`, a freshly built `wos-compartment`,
four compile/link depfiles, four command logs, the pre-command candidate census,
and `result.json`. Only a real successful invocation can produce artifact credit.
No output receipt from this helper qualifies a VM or two-hour interval.

## Build the ordinary-shutdown wrapper

Build `compartment_twohour_shutdown.cpp` with the existing generic target helper:

```sh
python3 tests/fixtures/build_compartment_filemap_runtime.py \
  --root "$PWD" \
  --source "$PWD/tests/fixtures/compartment_twohour_shutdown.cpp" \
  --output /tmp/wos-twohour-fresh-build/compartment-twohour-clean-witness
```

Use fresh output names; the ELF, `.d`, `.link.d` and `.build.json` must not
already exist. This helper uses the repository toolchain and target sysroot,
records its actual compiler/linker command, and checks the libc-only x86_64 PIE,
`/lib/ld.so`, NX stack and executable entry. It qualifies an artifact only;
complete emitted-header/library freshness and runtime admission require separate
checks of the actual dependency files and current inputs. Rebuild and bind the
actual output hashes after installing or changing any fixture source.

## Guest setup and service lifetime

Install the actual freshly qualified guest as
`/usr/libexec/goal39-twohour-runtime` and the freshly built current CLI as
`/usr/bin/wos-compartment`, and install the wrapper as
`/usr/libexec/goal39-twohour-clean-witness`. Resolve the actual `PT_INTERP` and
`DT_NEEDED` closure recursively for every injected program and library, including
init, journald, the guest, CLI and wrapper. Bind each literal library alias and
resolved target; include real transitive dependencies rather than assuming a
fixed payload count. The guest separately imports the actual `/lib/ld.so` and
`/usr/lib/libc.so` into the two private roots. The Host result directory is
`/srv/twohour`, owned by UID/GID100, mode0700. Its newly created regular reports
are mode0600, UID/GID100, nlink1. Use an explicitly prepared private test rootfs;
the fixture does not construct a VM or assign host image ownership.

The exact init topology remains `journald`, `twohour-controller`, and
`twohour-exit-witness`. Both test services depend only on journald and run
concurrently with no supplementary groups, umask0077, NNP, locked service
privileges none, empty environment plus `LD_LIBRARY_PATH=/usr/lib`, inherited
stdio, normal class/priority0, `type=daemon`, `readiness=immediate`, and
`restart=never`. They must not be ONESHOT services or depend on one another;
the operation does not raise init's existing ten-minute readiness limit.
Journald retains its real normal service definition and priority10. Save and
validate the exact runtime manifest and inherited Host namespaces.

Start the private manifest with `version=1` and the unchanged journald definition
from `configs/wos-services.conf`, including its priority10. Append these exact
two test-service blocks; keep this manifest separate from the normal system
configuration. Verify that the `httpd` account has UID/GID100.

```ini
[service twohour-controller]
user=httpd
group=httpd
supplementary-groups=none
umask=0077
authority-ceiling=0
no-new-privileges=yes
service-privileges=none
exec=/usr/libexec/goal39-twohour-runtime
arg=/usr/libexec/goal39-twohour-runtime
arg=--host
depends=journald
type=daemon
class=normal
priority=0
environment=empty
env=LD_LIBRARY_PATH=/usr/lib
readiness=immediate
readiness-timeout-ms=150000
restart=never
backoff-initial-ms=250
backoff-max-ms=30000
restart-budget=1
restart-window-ms=60000
stable-run-ms=60000
stop-term-ms=2000
stop-kill-ms=1000
drain-timeout-ms=2000
stdio=inherit
enable=always

[service twohour-exit-witness]
user=root
group=root
supplementary-groups=none
umask=0077
authority-ceiling=19
no-new-privileges=yes
service-privileges=none
exec=/usr/libexec/goal39-twohour-clean-witness
arg=/usr/libexec/goal39-twohour-clean-witness
depends=journald
type=daemon
class=normal
priority=0
environment=empty
env=LD_LIBRARY_PATH=/usr/lib
readiness=immediate
readiness-timeout-ms=150000
restart=never
backoff-initial-ms=250
backoff-max-ms=30000
restart-budget=1
restart-window-ms=60000
stable-run-ms=60000
stop-term-ms=2000
stop-kill-ms=1000
drain-timeout-ms=2000
stdio=inherit
enable=always
```

`twohour-controller` runs the guest with `--host` as UID/GID100 and ceiling0.
`twohour-exit-witness` runs the wrapper without arguments as root with
ceiling19: SET_UID, SET_GID and SIGNAL. It must be launched by the original
Host init; a numeric parent PID1 alone does not attest that namespace. After
fork the wrapper retains only SIGNAL (ceiling16). Its child clears supplementary
groups, permanently sets real/effective/saved UID and GID to100, drops its ceiling
to0, and verifies NNP and locked service privileges none before executing the
unchanged guest `--init-exit-witness` mode. The wrapper is a shutdown controller,
not one of the five measured unprivileged roles.

Only a consuming wait for that exact original child with status0 allows the
wrapper to send SIGUSR2 to original init. A consuming wait or ECHILD ends its
numeric child-PID authority. The wrapper bounds witness polling and failure
cleanup; it does not signal init on child failure. Init handles SIGUSR2 through
its ordinary poweroff flow. Signal acceptance and a successful witness are not
proof of natural VM exit, successful sync/unmount or clean filesystem state.

The full padded PASS line is stored in `init-exit-status`. Serial carries the
separate `WOS39_2H_WITNESS_COMMITTED` event, whose nonce and sequence must match
that status and the raw init snapshot. Use the strict saved-report decoder to
check this relationship.

`--tree A|B NONCE HOST_PID DEADLINE_US` is an internal private-image mode. The
Host imports the guest as `/app/probe` and one tree-specific marker as
`/same/name`. Private mappings, WKI, writable shared backing, repeated CREATE
stress and whole-kernel reclamation are outside this workload.

## Timing and actual work

There are1,443 samples, rounds0 through1442, in each of five roles. After all
round0 samples commit, S is the maximum of their actual end times. Later round r
targets `S+r*5,000,000us`, with actual starts at/after the target and completed
samples before target+2,000,000us. The final tail is scheduled at S+7210s;
E is the minimum of the five final-round starts, and E-S must be at least7200s.
Private roles sleep through the public absolute monotonic sleep ABI.

Each private sample checks the same original local PID/PPID, all four IDs100,
NNP1, ceiling0 and sealed service privileges256. It performs a real open,
single positional read64, close and nonce-token check, plus256 unsigned CPU
iterations. Each private root checks guarded WNOHANG0 for its original worker;
workers record an explicit skip. Each Host sample performs actual retained-file
pwrite64/pread64/fsync, a fresh path open/read/close, four original physical PID
signal0 checks and exact nohang checks for its own A root and CLI wrapper.

Private worker status0 is consumed by its original root; Host consumes A and
the original foreground CLI wrapper. The CLI consumes B. No serialized Host
wait for B is invented. Initial/final full FD and resource-limit observations,
guarded complete stat bytes, actual CLI manifest, and fixed proc snapshots bind
the original five measured roles. No replacement/restart can satisfy the wire.

The guest deadline is startup+7440s. A runtime controller needs one absolute
7500-second envelope including boot, extraction and cleanup, with cleanup time
reserved before launch. Guest time alone is insufficient. The Host writes
exact complete `WOS39_2H_START` and `WOS39_2H_END` lines, and an independent live
Host receiver must save first-read `time.monotonic_ns()` timestamps separated by
at least7200s, with poll gaps at most1s and scan state at most64KiB. A parser of
a saved timestamp ledger cannot establish that the ledger was actually captured
live; admission and independent terminal review must establish that provenance.

## Exact durable wire

All words are LE64. Raw signed syscall returns retain their bit patterns.
The magic is `0x5733393248435031`, version1, with per-nonce/tree/role/selector
guards and terminal commit words. Whole records are written with commit0,
fsynced, committed and fsynced again. Transport retries do not turn a retried
work operation into a single successful work call. Decoder source is the exact
field-level authority for every guard, zero reserve, hash, count and chronology.

| Filename | Exact bytes |
| --- | ---: |
| control-a.bin | 786432 |
| control-b.bin | 786432 |
| host-ledger.bin | 763392 |
| host-operational | 64 |
| marker-a | 64 |
| marker-b | 64 |
| cli-stdout.bin | 8192 |
| cli-stderr.bin | 8192 |
| proc-initial.bin | 16384 |
| proc-final.bin | 16384 |
| init-exit-snapshot.bin | 4160 |
| init-exit-status | 512 |
| campaign-status | 512 |

The total is2,390,784 bytes. Each control has a512-byte request, root/worker
command and progress slots, four4096-byte initial/final attestations, two512-byte
lifecycle records, and two1,443-row256-byte sample ledgers starting at24576 and
393984. The Host ledger has a512-byte request and anchor, initial/final4096-byte
attestations, a4096-byte summary, and1,443512-byte sample rows at24576. Unused
areas must be zero. The complete wire contains7,215 sample rows, ten complete
attestations,2,560 signed FD results,160 guarded limit pairs and40 full stat
slots, plus the full console stat anchor.

The init witness saves the real raw4160-byte snapshot and status line showing
the original controller daemon EXITED, NOT_READY1, generation1, no restarts,
raw consuming wait0 and an actual EXIT transition. A successful witness does
not imply the witness itself has exited or the VM has naturally powered off.

## Decode saved data

```sh
python3 tests/fixtures/validate_compartment_twohour_runtime.py saved-reports \
  --serial saved-serial.log --host-receive saved-live-receiver.json
```

`--host-receive` is optional for saved-protocol analysis. All thirteen report
files are mandatory and are bounded, identity-checked and double-read. Serial
is bounded64MiB, regular/nlink1, double-read, strict UTF8 and NUL-free. The
optional Host receive ledger is bounded1MiB and must bind exactly the four
complete original nonce-bound serial markers, their offsets/hashes and saved
receive timestamps. Every original decoder function is preserved.

Success is `PASS_SAVED_TWOHOUR_PROTOCOL_ONLY`. Runtime, criterion8 and full-goal
flags remain false even when a saved receiver ledger passes. Separate actual
artifact/source admission, exact process identity, live Host interval,
ordinary init poweroff, natural launcher exit0 and exact-owned process reap
remain necessary for this shutdown profile. After the VM is proven offline,
require a successful read-only Linux mount of the stopped rootfs and strict
readback of all thirteen durable reports and every injected payload, including
its actual hashes and applicable ownership/modes. Neither serial completion nor
natural poweroff replaces those filesystem checks. QMP quit or a forced fallback
is failure cleanup and leaves the attempt diagnostically FAIL. Preserve canonical
all-outcome owned-image cleanup evidence. Do not reuse
full VM disks for evidence: retain small reports/logs/hashes and prune proven
inactive agent-owned copies after successful and failed runs.
