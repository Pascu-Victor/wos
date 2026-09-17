# Goal 19 retained-directory timeout observation

9MA investigates the single retained-directory STAT_AT timeout from 9LZ without
changing the production kernel or retrying an operation inside the kernel.
The original failure remains in `build/goal19-native-unmount/matrix-timeout/`.
This observation does not establish its cause or claim a timeout fix.

The same normal kernel, build ID
`06b3301d14b88ab8b1936c6fe9492989607172d9`, ran on two fresh private nodes using
`--no-setup`. The ordinary sequence passed 20 native-unmount, 24 mount-cover and
1,568 traversal assertions. Neither serial log recorded a VFS timeout.

The first 90-second `perf record --filter=wki --structured` capture saved both
raw recordings, but structured conversion returned failure because each legacy
snapshot exceeded the utility's size limit. The current kernel's `wki` mask
includes local event scopes; local pipe activity dominated these recordings.
Their remote VFS begin/end records do not all pair, so they cannot establish
complete request history. Raw recordings and conversion failures are preserved.

A diagnostic userspace collector, built only under the observation artifact
directory, drained `/proc/kperf` every 5 ms and retained remote/transport text
records after draining. It made no kernel or installed utility changes. Ten
sequential ordinary retained-directory fixtures passed all 2,240 assertions.
Both collectors completed their bounded 90-second runs, disabled recording,
reported no incomplete text snapshots and removed their private files.

The focused source trace contains 2,260 paired remote VFS begin/end records;
the owner trace contains 5,100. All 2,240 primary path requests plus ten directory
opens match 2,250 unique owner replies by peer direction, channel, request cookie
and status. Async CLOSE does not require a reply and is excluded from that
reply comparison. No focused remote VFS event reports timeout. Generic transport
begin/end counts are not a loss metric: their instrumentation has different
event obligations. These checks qualify this recorded stream, not every possible
kernel event or interleaving.

The host converter preserves source file and line provenance and uses the local
`perf_event_json.cpp`/telemetry envelope contract. WOSDBG loaded all 71,790 derived
remote events and both serial logs, reconstructed WKI records and produced a
bounded distributed timeline. Clocks remain local; the timeline explicitly has
no proven global cross-node order. Matching reply identities does not require
inventing that order.

Source review still identifies a possible reply-loss path: successful directory
handling calls `send_buffered_resp`, which discards a reliable-send refusal.
Reliable send can refuse before sequence publication for allocation, credit,
route or channel state. Once accepted, retransmission owns delivery even if the
initial transport attempt fails. No refusal was tied to the original timeout.
A future response-send change needs its own context/lock/retirement contract;
adding client replay or increasing timeouts would not establish correctness.

Both nodes' fixture inventories were restored and synced, recording was verified
disabled, initial/final symbols matched, debugger sessions closed and launcher
15174 exited successfully. The host PID/start-time audit at
`2026-09-16T03:12:34.244795+00:00` found no remaining WOS VM.

Evidence: `build/goal19-response-timeout/validation.json`,
`focused/rpc-correlation.json`, `analysis-results.json`, and retained raw traces
under that artifact directory. No new Normal Build WOS or KTEST was needed:
production code and both kernel hashes remain unchanged from 9LZ.

The next traversal audit must reconcile supported backend root/cwd membership,
declared route transitions, automatic mount producers and mapped binding
lifetimes against their actual current entrypoints and native regressions.
9LZ has implemented native-unmount retirement; treating it as still absent is
stale. Unsupported transfers and confined PROCFS must be assessed through their
actual refusals. Goal 19 remains active with 12 enforced and 9 pending domains.
