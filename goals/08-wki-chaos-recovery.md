# Goal 08: Deterministic WKI Chaos, Recovery, and Invariant Qualification

## Planning metadata

- **Expected effort:** 1–2 continuous weeks of agent work. The estimate supports staffing; only evidence completes the goal.
- **Team shape:** one primary integration agent owns the fault model and cross-service acceptance, with exactly three bounded subagent workstreams for transport/peer reliability, service lifecycle scenarios, and orchestration/observability.
- **Critical surfaces:** WKI RX/NAPI, peer and channel epochs, retransmit/reorder/credit state, deferred workers, task/VFS/IPC/net/block ownership, fencing, and distributed log correlation.
- **Completion rule:** the defined deterministic campaign must pass and all reproducible in-contract invariant violations must be fixed or explicitly excluded with evidence. Merely creating a harness, running a soak, or spending the estimated time is incomplete.

## Copy-pastable goal command

```text
/goal Outcome: Build a deterministic seed-replay WKI chaos framework that injects bounded loss, duplication, reordering, delay, corruption, partition, and peer restart at transport/session boundaries, then use it to eliminate every reproducible lifecycle, reliability, ownership, and recovery violation in the agreed WKI scenario matrix. Verification surface: prove deterministic replay and clean convergence through focused host/model tests, the Normal Build WOS task, isolated bin/wos-ktest, fixed-seed and randomized campaigns, wosdbg-compatible evidence, and user-run multi-node Ethernet/RoCE/ivshmem runtime logs. Constraints: preserve normal-path behavior and wire ABI when injection is disabled, keep RX/NAPI bounded and nonblocking, preserve exact PacketBuffer/retransmit/object ownership and epoch/generation/cookie fencing, avoid timing-dependent assertions, and never hide a failure by widening timeouts. Boundaries: include reusable WKI transport fault hooks, invariant snapshots, peer/channel and compute/VFS/IPC/net/block scenarios, minimized reproduction, and fixes directly exposed by the campaign; exclude unrelated feature work, arbitrary kernel-wide fuzzing, production packet corruption controls, and broad protocol redesign. Iteration policy: add one deterministic scenario class at a time, run it, minimize every failure to a seed and event trace, repair the smallest violated invariant, rerun narrow and full matrices, and continue until the completion contract is met rather than until time expires. Blocked stop condition: mark blocked only after the same runtime or external dependency blocks progress for at least three consecutive goal turns, all safe local/model/KTEST alternatives are exhausted, and the exact user-run command, logs, or hardware capability required is recorded.

I am able to run and debug WOS live by appending --no-setup to the wos-ktest and wos-cluster scripts to not need root access.
```

## Local source evidence

- `modules/kern/src/net/wki/wki.cpp`, `wki_rx()`: one RX path performs bounds/checksum checks, forwarding, lifecycle admission, ACK/credit processing, reliable ordering, and service dispatch. Helper tests alone cannot qualify that ordering.
- `modules/kern/src/net/wki/wki.hpp`, `WkiChannel`: channel state includes sequence windows, retransmit and reorder ownership, advertised credits, generation, timers, and a per-channel lock.
- `modules/kern/src/net/wki/peer.cpp`, `handle_hello()`, `handle_hello_ack()`, heartbeat/fence/reconcile paths: boot and channel epochs distinguish sessions while cleanup and reconnection cross deferred work.
- `remote_compute.cpp`, `remote_vfs.cpp`, `remote_ipc.cpp`, `remote_net.cpp`, `dev_proxy.cpp`, and `dev_server.cpp` contain bounded workers, exact cookies/generations, attach/detach, and stale-completion defenses that interact during partition or reboot.
- Existing KTEST files—`wki_channel_ktest.cpp`, `wki_peer_liveness_ktest.cpp`, `wki_wait_ktest.cpp`, `wki_wire_ktest.cpp`, `wki_remote_compute_ktest.cpp`, `wki_remote_net_ktest.cpp`, `wki_dev_proxy_ktest.cpp`, and `wki_dev_server_ktest.cpp`—provide valuable focused checks but not a full packet-schedule campaign.
- `scripts/test/` contains `run_wki_anywhere_stress.sh`, `run_wki_one_shot_affinity_stress.sh`, and `run_wki_pipe_reader_close_stress.sh`; no general deterministic loss/reorder/duplicate/partition/reboot driver was found.
- `tools/wosdbg` already supports distributed timelines, WKI reconstruction, and coredump/log correlation, providing a natural output and triage surface.

## Measurable completion contract

1. A disabled injector adds no semantic change and negligible measured overhead. Enabled rules are bounded by peer, channel, message type, sequence/event count, and direction.
2. Every run emits a stable seed plus an ordered local injection trace sufficient to replay the same logical fault schedule. A failing campaign can be reduced without editing kernel source.
3. Supported actions include drop, duplicate, delay, reorder within a bounded window, checksum/payload corruption before validation, one-way/two-way partition, transport failure, and peer reboot/session replacement. Ownership for every injected frame is explicit.
4. Invariant snapshots report peer lifecycle/epochs, live channel generations, retransmit/reorder counts, credits, waiters, resource bindings, task ownership, proxy/export objects, block writer leases, deferred work, and packet-buffer deltas without unsafe unbounded allocation.
5. Scenario coverage includes channel loss/reorder/ACK starvation; reconnect during stale frames; route flap; remote task submit/cancel/exit; VFS mutation/close/detach; IPC pipe/epoll teardown; remote-net operation; block operation, flush, suspend/resume, and detach.
6. After each heal/restart, peers and services converge within documented bounds or surface a terminal error. No old-session completion mutates a successor, no resource has two terminal owners, queues remain bounded, and teardown reaches zero retained references.
7. Every reproducible violation in the committed scenario matrix has a regression test and a minimal correctness fix. Remaining runtime-only unknowns are itemized rather than silently waived.
8. Campaign failures produce artifacts directly consumable by WOSDBG or a documented conversion path, including completeness of cross-node timestamps rather than invented global ordering.

## Invariants, locks, and unsafe contexts

WKI transport RX and NAPI paths cannot block, sleep, wait for ACKs, or perform unbounded allocation/logging. Delay/reorder storage must be bounded and preferably preallocated; overflow must follow a deterministic documented policy. Preserve `PacketBuffer`, retransmit-entry, and deferred-work ownership exactly once. Respect `g_wki.peer_lock`, `WkiPeer` lifecycle admission, `WkiChannel::lock`, service registry locks, per-proxy/file I/O locks, and exclusive teardown gates. A fault event may not bypass the same epoch, generation, incarnation, detach-cookie, or exact-binding checks that production input uses. The harness must not alter packed wire structs or on-wire behavior unless a separately reviewed additive diagnostic capability is unavoidable; host-side or transport-local hooks are preferred.

## Scope boundaries

In scope are deterministic hooks, schedule/replay format, bounded observability, the stated WKI scenarios, orchestration, minimized regression cases, and fixes caused by those cases. Out of scope are generic network emulation for non-WKI traffic, new distributed services, authentication/encryption, throughput redesign, unlimited fuzzing, and changing correctness thresholds solely to make a campaign pass.

## Parallel workstreams

- **Primary integration agent:** define the event/fault and invariant contracts, approve hook placement, integrate cross-service fixes, and own the acceptance matrix.
- **Subagent 1 — transport/peer:** channel model, ACK/credit/retransmit schedules, peer epoch/fence/reconcile scenarios, and focused KTESTs.
- **Subagent 2 — services:** remote compute, VFS, IPC, net, block/device lifecycle scenarios and exact ownership regressions.
- **Subagent 3 — orchestration/observability:** seed runner, partition/reboot control, artifact schema, WOSDBG integration, campaign reduction, and documentation.

## Verification plan

Run focused host/model schedule tests, the **Normal Build WOS** task, formatting checks, and isolated `bin/wos-ktest`. Run fixed seeds before randomized campaigns and retain each failing trace. Agents may run and debug the configured VM topologies rootlessly with `--no-setup`; retain multi-node Ethernet, RoCE, and ivshmem logs, serial output, coredumps, and WOSDBG timelines. Physical-hardware checks remain user-run. Do not claim runtime success without the corresponding evidence.

## Iteration, blocked state, and rollback

Stage transport hooks disabled by default, then peer/channel scenarios, then one service family at a time, and finally orchestration. Each exposed defect receives a narrow regression before its fix. Use the command’s three-turn blocked rule; unavailable cluster runtime does not prevent further model/KTEST work. Roll back by disabling or reverting the latest hook/scenario stage while retaining independent regressions and correctness fixes. Never “rollback” by dropping the failing seed, increasing timeouts without proof, or weakening lifecycle assertions.

## Completion record (2026-08-31)

- **Status: Done.** The bounded injector/model, transport and semantic block hooks, exact replay/reduction, invariant snapshots, strict `wkictl` adapters, all ten service scenarios, rootless topologies, evidence schema, and WOSDBG batch generation are implemented. Injection remains boot-opt-in and disabled by default; wire/provider ABIs are unchanged, and the disabled hot paths retain only their relaxed-atomic gates.
- The disabled-path benchmark completed 20,000,000 calls across seven rounds and measured a median 0.630 ns/call delta (1.275 ns direct versus 1.905 ns through the relaxed-atomic gate), satisfying the negligible-overhead contract without weakening the gate.
- The authoritative final evidence is under `test-results/wki-chaos-goal08-final-20260831-v45/`. Its matrix report has `outcome=passed` and `fullMatrixPassed=true` for C1, C2, V1, V2, I1, I2, N1, B1, B2, and X1. The catalog digest is `sha256:021b00a9695bf969c5206eb05fb200368963f3ade4fa2449e4268676f4beaf0f`; the matrix digest is `sha256:873264fd62846bc9efeaea132d0b52b22be288b6a434b8e4553fcc69da869c4b`.
- Every required host-model and KTEST-model lane was freshly qualified. The isolated `bin/wos-ktest --no-build --no-setup` run completed with **8062 passed, 0 failed**; each qualifier retains the same bounded serial evidence with SHA-256 `d0210394f1d00b73fcbb423a785ea706faac210abeb0fc2e5a461258abdbf479`.
- Required live lanes passed on multi-node Ethernet, routed Ethernet, ivshmem, and software RoCE. B2 independently passed ivshmem and RoCE. The final fixed X1 run passed as `sha256:7f39cb1730043bc90cf610f3bdb71f0b1223eb14660f6da9f678f9e3aebe3a62`.
- Supplemental live seeds `0x5eed000000000001` through `0x5eed00000000000a` passed across the ten scenarios. X1's minimized alternate trace replayed twice consecutively on the same warm topology with the identical run ID `sha256:0eab8049a7635b5de46c2ac1896e81bd5b55f2e43fb8e7d373fbb7616864ef97`, complete evidence, and exact packet-pool convergence (`used=769`, `free=2303` before and after).
- Campaign-exposed defects received narrow regressions and minimal repairs, including RoCE fragment ordering in the shared backlog, I1 readiness handling, pending remote-VFS attach fencing, and stopped IPC-pump cleanup starvation. The final live serial evidence reduces peer fence-to-hostname retirement from the reproduced 10–53 second stalls to approximately 0.9–1.5 seconds without widening a timeout.
- The Normal **Build WOS**, focused source/model tests, formatter checks, and the isolated diagnostic suite pass. X1 and B2 WOSDBG batches load every telemetry lane and produce bounded clock-partitioned distributed timelines; `globalOrderAvailable=false` correctly records that the node clocks are not globally comparable. Final routed serial/QEMU logs are retained under `test-results/wki-chaos-goal08-final-20260831-v45/logs/final-routed/`, and the panic/KASan/UBSan scan is clean.
- Failed and pre-fix evidence is retained under the versioned `failures/` trees rather than overwritten. No required emulated-runtime lane or reproducible in-contract violation remains open. Physical-hardware behavior remains outside this matrix and was not claimed.
