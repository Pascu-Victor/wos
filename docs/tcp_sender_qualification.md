# Goal 29 TCP qualification

Qualification uses the sender described in [tcp_sender.md](tcp_sender.md).
The fixed correctness, impairment, persist, fairness, and comparative gates pass.
The CMake policy default is ON, and the final Normal Build WOS passes.
All measurements were made on 2026-10-05. Failed receipts remain available;
they are not counted as passing evidence.

## Artifacts and fixture

| Artifact | SHA256 |
| --- | --- |
| Corrected optimized TCP-enabled candidate | `480be1c70772576291452579cfe07c5e4157743b6e52f9ec779a98ad2444258d` |
| Preserved pre-goal kernel | `3c98f7d6984cb4344cd196548e283bb947cf01c9a5cfb20f883c6f249ff83023` |
| Shared benchmark binary | `2cfe99af2faf9da7e2df825a9ea6206d4b9fa6786818ed61bd101b434ec2d86a` |
| Corrected isolated diagnostic kernel | `66d540bfe2bb347693f7a728af6f2dcc8d1d99cd3e85c326cfd857a5c0c97395` |
| Final normal TCP-enabled kernel | `99f485fabc0981e8105ab8e7b0e1720c2904dff3fc7883efe52d9802451b4569` |
| Capped campaign bridge | `029cb78e2a6116b1717c6759772f8e41edc634b2c22e8c9dee983461d37b203d` |
| Capped campaign runner | `d0a0d5da40a295736ab26535e83de76d7a9fe35c3ae72e57370052ae4f5cf1db` |
| Capped campaign validator | `8aa020c68ea9686a55b00ad74f7bf605fdf98ee45c534fafb877f3470fb598a0` |

The final correctness, impairment, persist, and fairness receipts use the
corrected `480be1c70772...` candidate. Earlier `edb64d414481...` measurements
are retained as superseded checkpoints in the calibration history below.

The original kernel is an existing pre-goal artifact, not an established exact
source revision. Its receipt must not be described as an exact HEAD baseline.
A current TCP-off build is a diagnostic control with the safe oldest-head
timer, so it does not reproduce the original whole-queue timeout behavior.

The optimized candidate is RelWithDebInfo with libc++ fast hardening and
expensive kernel sanitizers disabled. Two private WOS nodes each use two CPUs,
2 GiB RAM, one virtio NIC queue, and an explicitly configured guest MTU of 1500.
The rootless Ethernet bridge connects distinct multicast NIC endpoints and
accepts only each node's source MAC. It bounds its queue to 1024 packets/8 MiB.
Private boot/rootfs copies, overlays, logs, and packaged-kernel verification
avoid modifying normal VM images. Launches use `bin/wos-cluster --no-setup`.

Each receipt records the kernel and benchmark hashes, base images, guest
scripts, bridge profile, results, wire counters, diagnostics, and packet pools.
The final comparator requires matching benchmark, guest scripts, bridge code,
base-image identity, server deadline, and clean-link policy.

## Fixed gates

Thresholds were selected before measurements: five 64 MiB clean transfers per
IP family must have median throughput at least 95% of baseline; twenty 64-byte
streams per family must have p95 stream time at most 110% of baseline. Two and
four competing 16 MiB streams per family must have Jain fairness at least 0.90
and overlap at least 0.80. Exact server-validated payloads, clean diagnostics,
and packet-pool balance are required independently of performance.

The final predeclared five-pair capped campaign passes using every sample:

| Family | Metric | Baseline | Candidate | Ratio | Gate |
| --- | --- | ---: | ---: | ---: | ---: |
| IPv4 | Median bulk MiB/s, 25 samples each | 11.466 | 11.465 | 0.999913 | >= 0.95 |
| IPv6 | Median bulk MiB/s, 25 samples each | 11.306 | 11.305 | 0.999912 | >= 0.95 |
| IPv4 | Tiny p95 microseconds, 100 samples each | 1048 | 1105 | 1.054389 | <= 1.10 |
| IPv6 | Tiny p95 microseconds, 100 samples each | 913 | 906 | 0.992333 | <= 1.10 |

Canonical result: `ktest-data/goal29-runtime/fixed-gates-windowcap-final.json`;
execution log: `build/goal29-runtime-windowcap-campaign.log`. All ten required
runs pass their 50 expected cases (500 total), with no exclusions or retries,
exact payload validation, clean diagnostics, and pool use 256->256 on both
nodes. Each run tracks 50 flows within the 128-flow cap table; effective peer
credit is at most 131072 bytes in both families. The maximum bridge queue is
107/1024 packets, with no overflow or bridge errors.
Gate receipt SHA256:
`bee1c29673e0499983d7e2ad115a8f3769bc1fc8a02bbcad415267ba435bc89a`.
Strict count, cap, pool, and provenance summary:
`ktest-data/goal29-runtime/windowcap-campaign-summary.json`.

The predeclared campaign evaluates pooled median/p95. Per-pair statistics are
retained too: IPv4 tiny ratios for pairs 3 and 4 individually exceed 1.10
(1.377702 and 1.334934); all their samples remain included. This result qualifies
the aggregate campaign and does not claim every individual pair passes its
latency ratio. Every per-pair bulk gate and IPv6 tiny gate passes.

Fairness passed on a shared 100 Mbps link with 5 ms added delay:

| Family | Flows | Jain index | Overlap fraction |
| --- | ---: | ---: | ---: |
| IPv4 | 2 | 0.9999998 | 0.999047 |
| IPv4 | 4 | 0.9999365 | 0.980898 |
| IPv6 | 2 | 0.9999798 | 0.991082 |
| IPv6 | 4 | 0.9999849 | 0.990344 |

Receipt: `ktest-data/goal29-runtime/corrected-fairness-mtu1500/results.json`.
All 12 streams passed exact delivery and both packet pools returned to their
baseline of 256 occupied NIC/RX buffers.

## Impairment evidence

The same deterministic payload-packet impairment policy runs for both IP
families. ACK, handshake, ARP, and NDP packets retain rate/delay service.
Each transfer delivers exactly 8 MiB and the server verifies every payload.

| Scenario | Receipt directory under `ktest-data/goal29-runtime/` | Wire evidence |
| --- | --- | --- |
| Loss | `corrected-loss-mtu1500` | 797 dropped payload packets |
| Reorder | `corrected-reorder-mtu1500` | 1125 held/released packets |
| Duplicate | `corrected-duplicate-mtu1500` | 952 duplication attempts |
| Mixed | `corrected-mixed-mtu1500` | 354 drops, 893 duplication attempts, 1159 reorder holds |

All eight family/scenario streams completed with exact bytes and packet-pool
use 256 to 256 on both nodes. These receipts establish delivery and pool
balance. The corrected loss run sampled live connections 78 times for IPv4
and 80 for IPv6: respectively 389/399 retry packets, 7/6 recovery requests,
389/399 RTOs, and 565/532 accepted SACK blocks, with no scoreboard overflow.
The mixed run sampled each family 44 times and recorded 178/179 retry packets,
6/4 recovery requests, 174/176 RTOs, and 750/755 SACK blocks. These deterministic
loss fixtures recover mostly through RTO; this establishes recovery and delivery,
not strong loss throughput. Fast reorder/duplicate runs can finish between
whole-second observer samples, so zero recorded samples establish no counter
claim for those scenarios.

## Host and diagnostic checks

`build/goal29-host/tcp-ctest-final.log` records 12/12 focused TCP CTest entries
passing, including 24 sender-model cases, existing receive/FIN/endpoint/timer
source tests, impairment/window-cap tooling, fuzz smoke, and an ASan/UBSan harness using
actual current TCP function bodies. The harness substitutes clock, pool,
output, and wake seams; it does not exercise a physical NIC. The stateful
sender fuzzer completed 100,000 inputs without failure, recorded in
`build/goal29-host/tcp-sender-fuzz-final.log`.

The preceding isolated `bin/wos-ktest --fast --no-setup` executed with TCP enabled,
KCFI, report-mode KUBSan, KASan, KCOV, selftests, and allocation provenance.
Fresh packaging used only `build-ktest/` and `ktest-data/`. All 66,791 assertions
passed with zero failures, including all seven `TcpSender` cases. Receipt:
`build/goal29-ktest-final-serial.log`, SHA256
`9a6b8248616814718c35111fba9e3ef88be24df289ff0352993184638d5e670d`.
This executed the preceding `afd509a30ea0...` diagnostic artifact. The corrected
diagnostic build and fresh execution also pass: 66,791 assertions, zero failures,
all seven sender cases. Final corrected receipt:
`build/goal29-ktest-corrected-serial.log`, SHA256
`0b675087d728c7a89fb9916d74c678c96be2377dc3f3e3c3b9b92f9c8c6a1003`.

Final Normal `Build WOS` used `scripts/dev/build_wos.sh` with
`WOS_CMAKE_ARGS=-DWOS_TCP_CONGESTION_CONTROL=ON`, 16 jobs, and workspace
`TMPDIR`. Both the initial enabled full build and the final artifact relink/
packaging build exited 0. Logs: `build/goal29-normal-final-build.log` and
`build/goal29-normal-final-restored-build.log`. The cache confirms
RelWithDebInfo, libc++ fast hardening, TCP ON, and KASan/KUBSan/KCFI/KCOV/trap
diagnostics OFF. The final normal ELF hash differs
from the isolated optimized candidate; read-only ELF section comparison proves
its `.text`, `.rodata`, and `.data` are byte-identical. Receipt:
`build/goal29-normal-artifact-comparison.json`, with `.text` SHA256
`3dc47f35a5121881e211ccf7cf1c3a265ae462256941cef3f0c6d52caf531ed0`.

An initial section-extraction command unexpectedly rewrote the ELF containers.
The candidate was restored from its private packaged boot image and checked
against the original `480be1c70772...` hash; the normal ELF was relinked and
packaged again. The final comparison parses sections without modifying either
artifact. No runtime receipt used the temporarily rewritten container.

Scoped C++ formatting checks and final `git diff --check` pass. The runtime
fixture's old rootfs perf binary was not replaced, so runtime qualification
uses the new procfs rows; the new `perf tcp-report` command is built by Normal
Build WOS, without a claim that it executed inside those fixtures.


## Changed surfaces and preserved invariants

- `modules/kern/src/net/proto/tcp_sender.hpp`, `tcp.hpp`, `tcp.cpp`,
  `tcp_input.cpp`, `tcp_output.cpp`, and `tcp_timer.cpp`: bounded sender,
  cumulative ACK/SACK accounting, retry/persist timers, and deferred ownership.
- `modules/kern/src/vfs/fs/procfs.cpp` and `modules/perf/src/{commands.cpp,
  perf.hpp,support.cpp}`: bounded connection snapshots and `perf tcp-report`.
- `modules/testprog/src/{main.cpp,netbench.cpp}` and `scripts/bench/`: exact
  payload benchmarks, private rootless fixtures, deterministic Ethernet
  impairment, provenance, and fixed-gate evaluation.
- `modules/kern/src/test/tcp_sender_ktest.cpp`, `tests/host/unit/tcp_*`,
  `tests/host/fuzz/tcp_sender_fuzz.cpp`, and their CMake/runner/shim integration:
  model, actual-source sanitizer, fuzz, and fixture checks.
- Kernel CMake policy, `docs/kernel_debug_flags.md`, `docs/tcp_sender.md`, and
  the goal ledger/index document activation, limits, receipts, and rollback.

Socket ABI and TCP wire layouts are preserved. Sequence comparisons remain
wrap-safe within bounded flight; only cumulative ACK retires stored packets.
Retransmission and SACK do not charge or retire new flight. Metadata, scoreboard,
timer batches, packet preparation, and diagnostics have fixed bounds. Protocol
state changes require the TCB lock; prepared output and wakes run after unlocking,
with retained references and generation checks across ACK/close races.

## Preserved failures and calibration

Unrestricted original and TCP-off MTU-1500 controls stalled. They are not
valid throughput baselines. Both clean controls were therefore calibrated to
100 Mbps without induced delay, loss, reordering, or duplication. The server's
idle accept deadline increased from 60 to 180 seconds because its IPv6 tiny
listener could expire while the preceding 640 MiB of bulk tests ran. Payloads,
sample counts, and numeric gates stayed fixed for both artifacts.

The first valid matched clean pair met bulk and IPv4 tiny gates but failed
IPv6 tiny p95: 1667/917 microseconds, ratio 1.8179. That failure is retained in
`ktest-data/goal29-runtime/fixed-gates.json`. The bridge's unconditional
positive 1 ms selector wait rounded a tiny packet's 5.92 microsecond service
into roughly 1 ms. A 100-deadline idle-selector calibration measured legacy
median lateness 1.052 ms/p95 1.058 ms, versus precise median 1.39 microseconds/
p95 1.60 microseconds. Receipt: `bridge-deadline-calibration.json` in the same
runtime root. The precise profile uses coarse waiting and a final bounded
100 microsecond nonblocking service interval, and records forwarding lateness.
Fresh matched kernel runs, not this calibration, determine the latency gate.

The first precise pair passed throughput and IPv6 tiny latency, but IPv4 tiny
p95 was 859/777 microseconds, ratio 1.105534, narrowly above 1.10. It remains a
failure in `fixed-gates-precise.json`; rounding is not used to make it pass.
Before further measurements, `paired-qualification-protocol.json` declares
five entirely fresh matched pairs on the corrected frozen candidate. Each run
retains five bulk and twenty tiny samples per family; kernel order alternates
between pairs. The campaign pools all 25 bulk and 100 tiny samples per family
using the same median/p95 gates. Every receipt and per-run statistic remains
visible, an incomplete/unmatched run rejects the campaign, and no sample or
failed pair is excluded or retried to obtain a favorable result.

That uncapped campaign is invalid: original-kernel pair 3 aborted its third
IPv4 bulk session following a client TX drop, completing 47/50 cases. There
was no bridge error/overflow (peak 939/1024 queued frames) and pools balanced.
All scheduled receipts remain included. The fresh clean
profile caps advertised peer windows equally at 128 KiB, bounding the old
sender's source burst while retaining the same benchmark and kernel binaries,
100 Mbps rate, payloads, sample counts, and numeric gates. The other runtime
scenarios retain uncapped normal peer windows. Before its measurements,
`paired-windowcap-protocol.json` froze all five fresh matched pairs, alternating
kernel order, fixed counts/gates, and all-run/no-exclusion rules. Its SHA256 is
`5613904d444d7a9f975606dee8e79691d502d4f96a4d332326d7dd49ccb346e7`.
The cap has a fixed 128-flow table, requires both SYN offers before ordinary
ACKs, preserves zero windows, and modifies only window/checksum words. Host
tests independently recompute IPv4/IPv6 TCP checksums, including scale 0/14,
absent/one-sided negotiation, malformed/truncated/duplicate options, unknown
flows, and table exhaustion. Runtime receipts require rewrites and an observed
maximum effective credit no larger than 131072 bytes in both families.

The first persist run delivered exact bytes with four wire zero-window/reopen
episodes per family and balanced packet pools, but failed its observer-based
assertion. The rootfs rejects `sleep 0.2`; the observer exited after its first
tick. Whole-second sampling and a final per-flow snapshot repair the fixture
without changing the benchmark binary or server pauses. Fresh corrected persist
and loss runs supply the missing live-counter evidence above and below.

With the observer repaired, the preceding candidate's loss run exposed actual
recovery: IPv4 recorded 388 retry packets, eight recovery requests, 386 RTOs,
and 581 accepted SACK blocks; IPv6 recorded 405, eleven, 399, and 517 respectively,
with zero scoreboard overflow. Most holes in this deterministic drop-every-17
fixture recovered through RTO, taking about 80 seconds per family. This is
conservative NewReno recovery evidence, not a claim of strong loss throughput.

The repaired persist run exposed a shared ACK/output race: a zero-window ACK
could retire the final entry before arming persist; a poll-based writer then
never entered the send-side arming path. IPv6 reached zero window/no flight
without a deadline, despite exact delivery after reopening. The corrected RX
arms on fresh zero-window updates and zero-window handshake completion, retaining
the existing empty probe without charging flight. Half-closed receive states
also reply to predecessor probes. Actual-source sanitizer tests cover retirement,
rejected admission, both families, handshake, stale/invalid ACK exclusion,
empty-probe accounting, reopen cancellation, half-close replies, and timer fallback.
At that checkpoint the fresh diagnostic and corrected impairment qualifications
passed, while the fixed performance campaign remained required. Its final result
is recorded above.

The corrected candidate's `corrected-persist-mtu1500/results.json` now passes:
both families have 13 live samples, four successfully admitted probes, zero
RTOs, four wire zero-window/reopen episodes, exact 8 MiB delivery, and packet-pool
use 256 to 256 on both nodes. Reopened live samples have a zero persist deadline.
Native IPv6 diagnostics display the bracketed endpoint and scope. The fixed
performance campaign now passes as recorded above.

Earlier normal/isolated builds encountered `/tmp` quota and transient target
headers during parallel sysroot installation. Retrying with a workspace
`TMPDIR` and stable installed headers succeeded. These failed builds are not
reported as successful validation. The full host manifest also has an existing
`kcov_runtime_test.cpp` registration issue; the focused TCP manifest passes.

These receipts qualify the documented VM fixture. They do not establish
physical-NIC performance, Internet-path behavior, or exact baseline source
provenance.
