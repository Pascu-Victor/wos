# Lifecycle construction/fault/census fixture

This fixture uses actual public Spec408/CREATE54, real static or tiny-interpreter
image entry, File and SysV work, positive-PID SIGKILL and consuming WAIT/raw9.
The fixed plan is eight uncounted warmups, 24 kernel-selected construction faults
in enum order, then 1,000 successful cycles. User records never select a fault or
qualify deletion, executor RETURNED, owner census, init reap or the VM.

Build the four target images with the standalone helper below, then package
only ELFs and the manifest from the same invocation. Record hashes from the
actual outputs; source or toolchain changes require fresh artifacts. The Host
codec checks saved wire structure. Compilation and codec results do not qualify
kernel fault consumption, lifecycle execution, owner baselines or VM cleanup.

The builder imports only the Python standard library. It does not read old raw
pilot sources, their builders, historical evidence, or another Host test driver.
Use explicit read-only target inputs and a fresh direct evidence child:

```sh
python3 -B tests/fixtures/build_compartment_lifecycle_runtime.py \
  --root "$PWD" --source-dir "$PWD/tests/fixtures" \
  --output-dir "$PWD/build/goal39-evidence/lifecycle-artifacts-01" \
  --sysroot "$PWD/toolchain/sysroot" \
  --llvm-bin "$PWD/toolchain/host/bin" \
  --wire-header "$PWD/modules/kern/src/test/lifecycle_wire60.hpp"
```

The evidence parent must already exist. An optional `--resource-dir` overrides
the default LLVM `lib/clang/22`. Existing outputs are refused. The 120-second
whole build bounds owned sessions, logs, scratch and output storage. Raw startup
modes1/2, freestanding flags, loader metadata, stack usage and entry disassembly
are checked. Every emitted target dependency must match a preheld input or an
actual owned intermediate. Scratch is removed only after owned processes are
reaped and absent. This is an artifact build, never a target-image launch.

Only after all four actual ELFs and source gates pass does the builder generate
`wos-lifecycle60-manifest.bin`, exactly1024 bytes: nonzero random campaign nonce,
actual driver/static/tiny/interpreter byte sizes, full SHA256, ELF type/entry/PHDR
counts and zero reserved fields. No expected image byte arrays or placeholder
hashes are compiled into the kernel or guest. Host modes are0555/0444; the future
packager must actually install UID0/GID0 and verify readbacks.

Configure exactly journald plus the `lifecycle-raw-pilot` Host service, LOCAL,
UID/GID100, no supplementary groups, authority0, NNP and locked service policy,
daemon, restart-never, dependent on journald, readiness immediate. Invoke
`/usr/libexec/lifecycle60/lifecycle-driver` with no arguments. Install its three
source assets as `/usr/libexec/lifecycle60/lifecycle-raw-static`,
`lifecycle-raw-tiny`, `lifecycle-tiny-ld.so`. The hosted driver also requires its
actual builder-pinned `/lib/ld.so` and `libc.so`. Kernel diagnostic admission must
verify the real executable/source Files/manifest and exclude external FD-table
and File writers. The manifest does not grant user-selected kernel authority.

Keep the ordinary `readiness-timeout-ms=600000` startup limit. Immediate
readiness clears that deadline after exec; the diagnostic controller and driver
retain their separate 3,600-second campaign bound. Readiness proves exec only.
Completion requires the original init's consuming wait for driver exit0, full
service cleanup and the final `EXITED/NOT_READY` boundary, together with every
cycle, fault, record and ownership check below.

Create fresh `/srv/lifecycle60`, UID/GID100 mode0700. The driver closes inherited
FD0/1/2 once before any open, reads/closes the manifest, then holds exactly nine
private descriptions: three RO source assets and these six RW Files, all CLOEXEC
and NO_CACHE. It writes reports only to Files and never opens/closes/rebinds this
ledger between attempts. Parent limits must permit the fixed evidence extent;
preallocation fully zero-writes each final file before the first measured cycle.

| File | Exact bytes | Meaning |
| --- | ---: | --- |
| control.bin |4096| sealed512-byte header/current record; remainder zero |
| child.raw |8192| input0..511, mutable HB1024..1279, READY4096..6143 |
| attempts.bin |528384|1032 sequential512-byte records |
| evidence.bin |4227072|1032 slots of input/READY/accepted HB A/B and zero tail |
| fd-ledger.bin |8192| complete256 GETFD/GETFL entries plus sealed header |
| driver-status |512| terminal user summary, requiring independent actual exit0 |

The saved decoder additionally takes the exact1024-byte manifest, for seven
buffers totaling4,777,472 bytes. All fields/offsets live in
`test/lifecycle_wire60.hpp`. IDs are index+1, independent of the kernel's
nonreused actor/attempt cookies. FNV detects torn bytes; actual immutable image
SHA, File capability identity and kernel hooks supply authority.

State1 REQUEST is durable before CREATE and remains visible at AFTER_FAILURE,
before userspace sees errno. State2 BEFORE_WAIT is durable before WAIT; the real
AFTER_WAIT hook runs before userspace sees raw9. State3 COMPLETED is published
only after the hook returns. The next request validates the completed prefix;
the last WAIT provides the final cycle census without an extra credited CREATE.
Two accepted real heartbeats must advance File/SysV work before a successful
kill. Final mutable HB equality is deliberately uncredited: kill can interrupt
a later heartbeat write. Immutable input/READY and unused raw regions must hold.

The kernel's exact execution-owned parked ACK/read lease is required for FD and
owner snapshots; a retained Task alone is insufficient. Backend report I/O has
no ACK/readers and must close temporary owners before scalar census. Completion
requires actual24 consumed fault tokens, actual1,000 full cycles, typed Task
storage freed, real callback RETURNED, versioned owner baselines and original
init consuming driver exit0 after supervisor cleanup. All remain runtime work.

`validate_compartment_lifecycle_runtime.py DIRECTORY [--prefix]` reads exact
regular saved files; pure `decode(saved_bytes)` and `scan_prefix(saved_bytes)`
also exist. Full result is `PASS_SAVED_USER_LIFECYCLE60_PROTOCOL_ONLY`; prefix is
diagnostic only. Every kernel/runtime/criterion qualification field stays false.
The registered Host codec test constructs explicit synthetic wire data and runs
37 positive/negative assertions, with no guest, fault or census credit. It can
optionally consume an actual `--manifest FILE` for metadata shape tests; its
in-memory default synthetic manifest is never an artifact or packaged output.
