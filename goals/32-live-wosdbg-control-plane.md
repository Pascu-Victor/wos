# Goal 32: Live Multi-Node WOSDBG Control Plane

## Planning metadata

- **Target horizon:** one to two continuous weeks with session/security, debugger adapters, distributed capture, and frontend parity workstreams.
- **Primary surfaces:** WOSDBG shared analysis backend, CLI/MCP/GUI, QMP/QEMU gdbstub, WOS debugserver, symbols/coredumps, cluster tooling, and incident bundles.
- **Completion rule:** live sessions must be bounded, auditable, cleanup-safe, and equivalent through all three frontends; a GUI-only prototype is insufficient.

## Copy-pastable goal command

```text
/goal Outcome: Extend WOSDBG's shared backend with bounded live kernel/userspace and multi-node debugging: topology discovery, lease-safe pause/resume, registers, memory, backtraces, coordinated snapshots, and deterministic handoff into incident bundles and replayable transcripts. | Verification surface: mock protocol/state tests, parser and hostile-peer fuzzing, GUI/CLI/MCP contract parity, timeout/disconnect cleanup tests, Normal Build WOS, WOSDBG builds/tests, and user-run rootless single-node plus multi-node live capture evidence. | Constraints: Keep read-only operations default, require explicit auditable authority for mutation, enforce target allowlists/build-ID checks/deadlines/byte limits/cancellation, never strand nodes paused, never expose arbitrary host/guest shell, and never invent cross-node order. | Boundaries: Add shared-backend live sessions, QMP/gdb/debugserver adapters, snapshot/incident integration, transcripts, frontend bindings, and tests; do not build a general remote administration plane or frontend-specific dispatchers. | Iteration policy: Prove session/lease cleanup with mocks, add one read-only adapter and shared tools, then pause/resume, symbols/backtraces, coordinated snapshots, incident capture, mutations, and frontend parity, fuzzing every parser before live use. | Blocked stop condition: Stop only after the same indispensable VM/runtime blocker repeats for three goal turns, all safe mock/fuzz/offline alternatives are exhausted, and the exact required topology, log, or user action is stated.
```

## Local source evidence

WOSDBG's `DebugAnalysisService::tool_catalog()` already unifies offline log, coredump, incident, memory, source, and distributed-timeline capabilities across GUI, JSON CLI, and MCP. It does not provide a live target/session lifecycle. `modules/debugserver/src/main.cpp` separately implements a single-client GDB remote stub with register/memory/breakpoint operations. Cluster code exposes QMP and optional localhost QEMU gdbstubs, and `scripts/cluster/qmp.py` has a bounded helper that is not integrated into WOSDBG.

Goals 12 and 13 delivered reproducible incident bundles and structured telemetry; this goal connects live acquisition to those established offline contracts.

## Measurable completion contract

1. A versioned live-session schema identifies topology, node/target, transport, build ID, capabilities, lease owner, deadlines, limits, and transcript.
2. Adapters for QMP/QEMU gdbstub and guest debugserver share cancellation/error semantics and are testable against mocks.
3. Pause ownership is lease/RAII based: timeout, client disconnect, adapter error, process exit, and partial multi-node failure resume every node the session paused unless explicitly transferred.
4. Read-only register, bounded memory, backtrace, address/PTE/source, and symbol operations reuse existing WOSDBG analysis code with build-ID verification.
5. Mutating operations are excluded by default and require an explicit capability plus auditable confirmation and bounded target set.
6. Coordinated capture records per-node timestamps and completeness without asserting unavailable global order.
7. A live snapshot can atomically produce an incident bundle containing topology, transcripts, registers/memory bounds, logs/coredumps, build IDs, and integrity metadata.
8. CLI, MCP, and GUI enumerate and invoke the same backend catalog and return schema-equivalent results.
9. Parsers and state machines survive malformed/hostile peers, oversized packets, hangs, disconnects, and duplicate responses.
10. Rootless single-node and multi-node runs demonstrate pause cleanup, exact symbolization, deterministic transcript replay, and successful offline incident reopening.

## Invariants, security, and boundaries

No arbitrary command execution, unrestricted filesystem read, or open network bind is introduced. Endpoints default to localhost/tunnel and explicit allowlists. Memory reads and transcripts are byte-bounded and may contain secrets, so incident redaction/storage policy is explicit. Multi-node failure cannot strand a healthy node paused.

## Parallel workstreams

The primary agent owns schemas, security, shared catalog, and integration. Subagent A builds session/lease/mocks/fuzzers. Subagent B implements QMP/gdb adapters. Subagent C implements guest debugserver and symbol reuse. Subagent D may add coordinated incident capture and UI/CLI/MCP parity on disjoint files.

## Verification, iteration, and rollback

Run WOSDBG unit/semantic/fuzz tests, adapter mocks, Normal `Build WOS`, interface source tests, and user-run rootless VM/cluster scenarios. Add live tools disabled unless configured, and land read-only capability before mutation. Rollback must close sessions and resume leased targets. Use the `/goal` blocked condition exactly.

## Completion record — 2026-09-04

- **Status:** Completed.
- **Live execution note:** I am able to run and debug WOS live without root
  access by appending `--no-setup` to the `wos-ktest` and `wos-cluster`
  scripts.
- **Backend result:** WOSDBG now has one bounded live-session catalog shared by
  CLI, MCP, and GUI, with allowlisted QMP/GDB-RSP and debugserver transports,
  owner-bound leases, audited pause authority, exact build-ID gating, bounded
  register/memory/backtrace/PTE/source reads, and replayable transcripts.
- **Cleanup result:** RAII leases resume only targets paused by the session;
  timeout, disconnect, shutdown, adapter failure, and partial multi-node
  acquisition paths are covered by semantic tests. No raw protocol, host shell,
  guest shell, or general remote-administration operation is exposed.
- **Capture result:** Coordinated snapshots preserve per-node host timestamps
  and explicit clock quality, then atomically produce bounded live incident
  bundles containing topology, build identities, transcripts, register
  snapshots, and allowlisted logs/binaries for immediate offline reopening.
- **Build/test evidence:** Normal `Build WOS`, the integrated WOSDBG build, all
  three WOSDBG semantic CTests, 18 incident tests, 27 cluster-launcher tests,
  the debugserver source check, and repository formatting completed
  successfully. The isolated KTEST selftest run passed 8,111 tests with zero
  failures.
- **Runtime evidence:** Rootless single-node KTEST and three-node cluster runs
  verified exact build IDs and source lookup. The cluster run acquired all
  three nodes under one lease, read 66 negotiated registers per node, walked a
  live kernel PTE, resolved `_start` to `modules/kern/src/main.cpp`, captured
  and reopened a valid 13-member incident, closed with zero sessions, and left
  all three VMs running after the lease was released.
