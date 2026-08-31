# Deterministic WKI chaos campaigns

`bin/wos-wki-chaos` is the bounded host-side planner, runner, exact-replay
driver, reducer, and matrix reporter for Goal 08. It does not enable a
production packet-corruption interface and does not set up cluster networking.
It attaches to WOS nodes that are already running and executes only validated
guest argument arrays and allowlisted typed QMP operations.

## Safety and determinism contract

Scenario, plan, replay, reduction, run-manifest, matrix, and matrix-report
objects are versioned. Inputs reject duplicate JSON members, unsupported
members, shells, oversized commands, unbounded timeouts, unsupported QMP
commands, and unresolved node/NIC names. Guest commands are arrays rather than
shell strings. The SSH boundary quotes each token before passing it through
OpenSSH's remote command protocol; a scenario cannot request `sh -c` or another
known shell. A live run uses one bounded OpenSSH control connection per node
and explicitly closes every control master at the end. Concurrent checkpointed
workloads use separate channels on that same connection. This prevents the
observer itself from accumulating short-lived TCP teardown buffers during an
exact packet-pool convergence check.

Plans use the repository-defined SplitMix64 version 1 generator. The seed is an
unsigned 64-bit decimal string in generated JSON, and potentially large IDs or
timestamps are also strings. Stable input order plus the same scenario and seed
produce byte-identical plan JSON and `planDigest`. An observed replay retains
the chosen event order, group membership, host-monotonic observation bounds,
exit code, timeout, bounded stdout/stderr, output truncation, QMP reply/events,
and normalized failure signature.

Committed live scenarios place the narrowly scoped token `seed=$PLAN_SEED` only
in the exact direct command `/usr/bin/wkictl chaos enable`. Plan construction
replaces it with the chosen canonical decimal seed before hashing or execution;
replay retains that resolved value. Any use of `$PLAN_SEED` in another command
is rejected. Consequently identical seeds are byte-stable, while a different
campaign seed changes both the plan digest and the injector's enable argument.

Each output directory is new and private; an existing path is rejected rather
than overwritten. It contains:

- `manifest.json`, with completeness, outcome, SHA-256 artifact inventory, and
  configured bounds;
- `replay.json`, sufficient to execute the same logical schedule;
- `telemetry/host.jsonl` and per-node JSONL lanes in canonical
  `wos.telemetry` version 1 envelopes;
- `wosdbg-batch.json`, which loads every emitted lane and calls
  `build_distributed_timeline` with the campaign correlation keys;
- raw and parsed diagnostic snapshots when the scenario requests them.

Each JSONL record is capped at WOSDBG's 1 MiB parser limit. If a bounded poll
accumulates enough retained attempts to cross that limit, the telemetry lane
keeps the event identity, status, operation, fault, scalar result summary,
collection counts, and a SHA-256 digest of the full record. The complete
attempt evidence remains in `replay.json` and the referenced raw snapshot
artifacts; compaction therefore affects only the timeline projection.

Convergence may begin with tombstones from an earlier successful peer
replacement. Such a disconnected row is accepted only when the same numeric
identity was already disconnected at baseline, its lifecycle remains
`FENCED`, its nonzero boot epoch and replacement identity are unchanged, and
every cleanup gate is zero in both snapshots. Disappearance is harmless;
reactivation, ambiguity, or any changed fence fails the invariant.

A stable pair that still violates an invariant is not terminal by itself.
Convergence continues through the scenario's existing bounded sample count and
deadline until two consecutive projections are both stable and passing. A
persistent violation exhausts that unchanged bound and reports the exact final
issues; this avoids turning transient observer traffic into a failure without
widening a timeout or weakening PacketBuffer accounting.

Host observation timestamps have the `host_monotonic` domain and
`local_observer` quality. Guest telemetry keeps its own node/boot clock
metadata. WOSDBG, not the runner, determines whether clocks are comparable. It
reports clock partitions and `globalOrderAvailable`; the campaign never
constructs an unsupported cross-node total order.

## Commands

Generate a stable plan without contacting a VM:

```sh
bin/wos-wki-chaos plan \
  --scenario test-results/scenario.json \
  --seed 0xc100000000000001 \
  --output test-results/c1-plan.json
```

Execute against an already-running topology (`run` and `attach` are equivalent
explicit names for this surface):

```sh
bin/wos-wki-chaos attach \
  --scenario test-results/scenario.json \
  --seed 0xc100000000000001 \
  --output test-results/c1-run
```

Use `--dry-run` to validate the exact plan and artifact path without external
commands. Exact replay consumes a prior observation:

```sh
bin/wos-wki-chaos replay \
  --input test-results/c1-run/replay.json \
  --output test-results/c1-replay
```

Reduction removes complete reducible groups, preserving event order and
partition/heal or stop/continue pairs. It matches the normalized tuple of
scenario, invariant code, node, and object identity. It never changes a
deadline, treats an incomplete replay as a failure, or widens a timeout:

```sh
bin/wos-wki-chaos minimize \
  --input test-results/c1-run/replay.json \
  --max-attempts 32 \
  --output test-results/c1-minimized
```

Run the generated WOSDBG batch from the artifact directory so its relative
telemetry paths resolve:

```sh
cd test-results/c1-run
../../tools/build/bin/wosdbg --batch wosdbg-batch.json
```

## Live and rootless prerequisites

Agents are able to run and debug WOS live without acquiring root access by
appending `--no-setup` to the `wos-ktest` and `wos-cluster` scripts. This is a
reuse mode, not a topology creator: the configured bridges, TAP devices, and
shared-memory/RDMA topology must already exist and be usable, and VM overlays
and logs must be owned by the launching user.

After changing the kernel, libc, or userspace, rebuild the isolated KTEST
artifacts and let the launch command package fresh disks before reusing the
existing topology:

```sh
bin/wos-ktest --config configs/node_ktest_rootless.json \
  --kernel-cmdline "--selftest" \
  --build-only --reset-sysroot
bin/wos-ktest --config configs/node_ktest_rootless.json \
  --kernel-cmdline "--selftest" \
  --no-build --no-setup --debug-node
```

Use `--no-package` only when the existing KTEST disks were already packaged
from the exact artifacts under test. `--no-setup` is valid only for an actual
launch. It validates that every bridge and TAP is present, UP, correctly
enslaved, and multi-queue where configured; a validation failure requires the
user to perform the one-time privileged topology setup.

Chaos clusters deliberately use isolated base images. Seed the isolated rootfs
once and package a boot image with the test-only kernel command-line opt-in;
never point these overrides at the normal `disk.qcow2` or `mountfs.qcow2`:

```sh
mkdir -p wki-chaos-data
cp --reflink=auto --sparse=always --no-clobber \
  mountfs.qcow2 wki-chaos-data/mountfs.qcow2
WOS_BOOT_DISK=wki-chaos-data/disk.qcow2 \
WOS_ROOTFS_DISK=wki-chaos-data/mountfs.qcow2 \
WOS_BOOT_IMAGE_MANIFEST=wki-chaos-data/boot_image.manifest \
WOS_KERNEL_CMDLINE='wki.chaos wki.chaos.workload wki.ivshmem' \
  scripts/build/make_image.sh
```

`wki.ivshmem` explicitly reserves a single shared-memory PCI function from the
generic ivshmem network driver so the phase-6 WKI transport can claim it for
RDMA. Without that token the historical generic-driver ownership policy is
unchanged. The ivshmem live lane must prove `wki-ivshmem` selection in its
captured diagnostics; a RoCE fallback does not qualify it.

The Ethernet topology is then launchable and debuggable without root setup:

```sh
bin/wos-cluster --config configs/cluster_wki_chaos.json \
  --launch --no-setup --debug-node 0
```

The SSH resolver includes bounded tails from root cluster logs, isolated KTEST
logs, and `wki-chaos-data/*serial-vm*.log`. When stale lane logs contain the
same hostname or VM ordinal, the newest complete hostname/IP observation wins;
an incomplete newer boot cannot mask the most recent complete address.

Use `configs/cluster_wki_chaos_ivshmem.json` for the ivshmem lane and
`configs/cluster_wki_chaos_roce.json` for the software-RoCE lane. N1/X1 may use
`configs/cluster_wki_chaos_routed.json`, whose WKI links overlap as
`wki-a={0,1}` and `wki-b={1,2}` while the LAN still contains all three nodes.
The two routed WKI zones use distinct explicit QEMU socket-multicast groups,
so they need no host bridge or TAP. Each group sets optional `localaddr` to
`127.0.0.1`, keeping same-host multicast delivery independent of the host's
default route and multicast-loop policy. The schema requires a multicast IPv4
address and a port in `1..65535`, accepts an optional IPv4 `localaddr`, requires
one queue and `vhost=false`, and rejects an endpoint reused by another zone.
Node 1 joins both groups. The LAN remains
TAP-backed for host SSH access, so `--no-setup` validates only its existing
bridge/TAPs and skips host-link probes for the two socket-backed zones.
The launcher also supplies the normalized per-node NIC policy through
`opt/wos/netdevs` fw_cfg, using semicolons as line separators. Early kernel WKI
initialization consumes that policy before falling back to the shared
initramfs `/etc/netdevs`; the later rootfs file is generated from the same
assignments. This ensures every WKI NIC on a multi-homed node is non-remotable
before the first peer resource snapshot.
Non-GLOBAL cluster zones may declare these stable global memberships with
`node_ids`; legacy `nodes: N` still means IDs `0..N-1`, and if both fields are
present then `nodes` must equal the `node_ids` length. TAP, hostname, MAC, QMP,
and overlay naming continue to use the global node ID, so `--no-setup` checks
only TAP-backed links for actual zone members. All four configs refer only to
`wki-chaos-data/disk.qcow2` and
`wki-chaos-data/mountfs.qcow2`. `--no-setup` does not create either image,
bridge, TAP, or ivshmem file. The RoCE overlay is initialized by WOS over the
raw-L2 WKI Ethernet NIC when ivshmem is absent; its live lane must still prove
`wki-roce` selection in captured diagnostics rather than inferring it from the
configuration filename.

For host-controlled link failure or reboot, add an opt-in socket to each node's
VM configuration:

```json
{"vm":{"qmp_socket":"cluster-data/qmp-vm0.sock"}}
```

The launcher then exposes rootless QMP. Scenarios may use only `stop`, `cont`,
`set_link`, and `system_reset`. Logical NIC names map explicitly to QEMU netdev
IDs such as `net1`. Use kernel fault rules for one-way partitions and precise
TX/RX occurrence matching; QMP link down represents a physical two-way failure.

## Guest commands and injector adapter

Every guest operation supplies a node, role, bounded timeout, expected exit
codes, and a direct `argv` array. `wkictl chaos` is the strict guest adapter for
the test-only `/proc/wki/chaos` control file. Mutating commands accept `clear`,
`enable seed=<u64>`, `disable`, `heal id=<u32>`, `release id=<u32>
[count=<u32>]`, or `rule ...`; the adapter validates a bounded token-only
command and performs one exact write. For example, a scenario can use:

```json
{"argv":["wkictl","chaos","enable","seed=13907115649320091649"]}
```

Non-clear/disable commands require the kernel test boot opt-in `wki.chaos`.
`clear` and `disable` intentionally remain authorized without that opt-in so
cleanup can always remove stale test state and restore the identity fast path;
they cannot add or release a fault.
Keep the adapter invocation as guest `argv`; do not embed procfs command syntax
or a shell in the host runner.

Rules may add `op=<u16>` (decimal or `0x` hexadecimal) to select an exact
operation in a structurally valid `DEV_OP_REQ` or `DEV_OP_RESP`. `op=*` removes
the selector. The matcher first validates the WKI version and payload bound,
the DEV_OP message type, the appropriate request/response prefix, and exact
agreement between its `data_len` and the WKI payload length. A truncated frame,
a non-DEV_OP frame with coincidental leading bytes, or a valid DEV_OP carrying
another operation does not match. For example, I1 uses `op=0x0702` for
`OP_PIPE_DATA` on the exported-pipe leg and `op=0x0701` for that leg's
`OP_PIPE_CLOSE_WRITE`. Its reverse remote-writer leg is flow controlled, so the
reader-close recovery rule selects `op=0x0704` (`OP_PIPE_DATA_FLOW`); a rule
targeting that leg's close would instead select `op=0x0706`.

The adapter also accepts `neighbor_host=<hostname>`, `src_host=<hostname>`,
and `dst_host=<hostname>` on `rule` commands. It reads one fixed 4096-byte
`/proc/wki/peers` snapshot, requires the exact seven-column schema, exactly one
local row (which may itself be selected), unique hostname and node identities,
and a connected matching row. A malformed, duplicate, missing, disconnected,
or producer-ceiling/truncated snapshot fails closed before `/proc/wki/chaos`
is opened for writing. Each alias is rewritten to the canonical decimal
`neighbor=`, `src=`, or `dst=` token. The existing numeric and `*` syntax is
unchanged, but one rule may not contain both a numeric selector and its
matching hostname alias (or repeat either selector).

Block-ring rules use two explicit local semantic surfaces:

```text
rule id=51 surface=blk_doorbell dir=tx action=drop \
  origin=proxy lane=roce neighbor_host=wos-1 zone=* resource=* \
  ring_generation=* ring_index=* cookie=* blk_op=* occurrence=0 limit=1

rule id=52 surface=blk_sqe dir=tx action=corrupt \
  origin=proxy lane=ivshmem neighbor_host=wos-1 resource=* \
  sqe_offset=4 sqe_xor=0x80 occurrence=0 limit=1
```

`blk_doorbell` accepts only TX `pass`, `drop`, `duplicate`, or `delay`.
`blk_sqe` accepts only TX `corrupt`, with a byte offset in the current
24-byte `BlkSqEntry` and a nonzero XOR byte. Block selectors are
`origin=<proxy|server|*>`, `lane=<ivshmem|roce|*>`, `zone=<u32|*>`,
`resource=<u32|*>`, `ring_generation=<u32|*>`, `ring_index=<u32|*>`,
`cookie=<u32|*>`, and `blk_op=<u8|*>`; the common transport, neighbor,
occurrence, after, every, limit, and chance selectors also apply. Frame-only
selectors and payload corruption parameters are rejected on block surfaces,
and block-only selectors are rejected on `surface=frame` (the default).
Every rule key is singular; repeating any required field, selector, schedule
parameter, or corruption parameter is invalid even when the values agree.
A rule ID cannot be replaced while a held, ready, or in-flight frame or block
event still references it. The replacement fails closed until `release`,
`heal`, transport discard, or `clear` retires that row, preserving unambiguous
release ownership and `applied` evidence.

`action=corrupt` retains invalid-checksum corruption when no mode is supplied;
`corrupt=checksum` selects that behavior explicitly. The bounded payload mode
is:

```text
rule id=41 dir=tx action=corrupt op=0x0100 \
  corrupt=payload payload_offset=12 payload_xor=0x01
```

`payload_offset` is zero-based from the first WKI payload byte, not from the
operation body. The mask must be a nonzero byte. The injector copies into its
existing fixed queue, XORs exactly the selected byte, and writes a valid,
nonzero WKI checksum without changing the caller's frame or retransmit copy.
For message-fallback `OP_BLOCK_READ`/`OP_BLOCK_WRITE`, the four-byte
`DevOpReqPayload` prefix makes the first LBA byte offset 4 and the first block
count byte offset 12. An offset outside the declared WKI payload does not match.
Rule and trace rows report the operation validity/ID, corruption mode, offset,
mask, before/after byte, and before/after checksum.

Reliable byte-identical retransmissions of one held `delay`/`reorder` frame,
or of one `partition` frame retained until `heal`, reuse the
existing fixed queue record. They do not consume another rule occurrence,
increment `applied`, allocate another slot, or append one trace row per retry.
The aggregate `coalesced` counter records every suppressed retry, and the one
release/heal trace row reports its final `coalesced` count. Coalescing requires
the exact frame bytes, transport object/ID, ingress identity, direction, and
neighbor. It is restricted to message types handled by WKI's reliable channel
dispatcher; identical raw `HELLO`, `HEARTBEAT`, and other unreliable control
events remain independent schedule occurrences. `duplicate` is also never
coalesced and retains its exact second-delivery semantics.

`wkictl chaos wait id=<u32> applied=<u32> timeout_ms=<u32>` is the bounded,
read-only injection barrier. It repeatedly opens and parses a fresh complete
`/proc/wki/chaos` snapshot against an absolute monotonic deadline. It succeeds
only for one active exact rule with the requested applied count, enabled runtime
control, zero invalid/overflow counters, the unique final completion sentinel,
and—before explicit release—a corresponding held frame for delay/reorder. An
overshot count or malformed, truncated, contradictory snapshot fails closed.
`matched=<u32>` remains accepted as a compatibility spelling; new manifests
use `applied` to match the proc rule field directly.

I1's direct workload is:

```sh
testprog wki-chaos-workload I1 --run-id ID --target HOST \
  --checkpoint-dir /tmp/wki-chaos-workload --timeout-ms 30000
```

Its single-use checkpoints are `pipe-open`, `before-data`, `data-issued`,
`before-close`, `close-issued`, `before-discard`, and `reader-discarded`; `wait` observes the
exact ready state and `advance` creates one `O_EXCL` advance token. Remote
application acknowledgements bracket the data stages, EOF, and recovery, so
the host can prove exact drop/duplicate counts before the next stage, hold and
reverse two remaining DATA frames, and hold CLOSE before EOF. After the first
pipe is complete, `before-discard` isolates the second pipe so a rule can drop
its first flow-controlled DATA frame before the local reader closes. Success
requires the exact 16 KiB payload, EOF only after all bytes, the exact discard
prefix, and `EPIPE` after the reader closes. The paired cleanup command is bounded, cancellation-aware,
idempotent for a missing run, and removes only its validated 0700 run directory
and known 0600 state files.

The other direct checkpoint workloads use the same run/wait/advance/cleanup
protocol and one reducer-atomic group:

- C1 enables `compute-publish hold=1` on node 1, proves exactly one blocked
  receiver submit worker, advances cancellation, waits for exact TX
  `TASK_CANCEL` (`type=0x54`, channel 3) application, and only then releases
  publication. Its checkpoints are `submit-issued`, `cancel-issued`, and
  `cancel-complete`.
- C2 delays the old `TASK_COMPLETE` (`type=0x53`, channel 3) on surviving node
  0 RX, resets node 1 only after `applied=1`, proves SSH and new connected boot
  epoch, then publishes a successor. It releases the old completion while the
  successor exists; the fenced old child must fail and the successor must exit
  zero. Its checkpoints are `task-running`, `exit-issued`,
  `successor-running`, and `successor-complete`.
- V1 drives exact peer/type/op VFS selectors for negotiated RDMA WRITE
  `0x0411`, CLOSE `0x0403`, RENAME `0x040B`, and UTIMENS `0x0415`. The channel is deliberately
  wildcarded because VFS traffic uses negotiated dynamic lane channels rather
  than channel 3. Every delayed/reordered response has an `applied` barrier
  before release, the WRITE request is duplicated, and CLOSE is counted before
  final verification. UTIMENS uses the still-current remote path before rename,
  since descriptor-only `futimens` is not a supported remote-file operation.
  Its nine checkpoints span `file-open` through `close-done`.
- V2 opens eight remote files and uses the same dynamic-channel selector
  policy, partitions the first exact WRITE until its applied/heal barrier,
  completes every write/fsync, then holds and snapshots the first exact CLOSE
  before release. Its checkpoints are `lanes-open`, `operations-done`,
  `before-close`, and `files-closed`.
- I2 delays the old socket GETPEERNAME request (`DEV_OP_REQ`, `type=0x43`,
  `op=0x0753`, channel 3) on surviving node 0 RX, resets node 1 after the exact
  barrier, proves recovery and a changed boot epoch, and releases old control
  work only after successor publication. The fenced old helper must fail and
  the successor must exit zero. Its checkpoints are `control-issued`,
  `old-session-fenced`, `successor-issued`, and `successor-complete`. This uses
  a connected socket because WOS intentionally pins tasks that already own an
  epoll instance: their interest lists contain local `File*` identities and
  cannot be migrated. The socket operation therefore exercises the real remote
  IPC cookie/epoch path instead of producing a local false pass.
  Its workload deadline is 160 seconds, leaving a bounded margin for the two
  independent SSH and peer-epoch recovery probes inside the committed
  180-second scenario budget. The paired
  `WkiIpcLifecycle/StoppedPumpPublishesDiscardFence` KTEST proves that both
  pre-read and post-read pump retirement publish the same ordered teardown
  marker, so a close-only delivery cannot survive after its proxy is gone.
- N1 keeps one exact managed NET attachment from node 0 to node 2 through
  `wki-a={0,1}` and `wki-b={1,2}`. Its node-0 TX rule selects only
  `DEV_OP_REQ type=0x43 op=0x0303` (`OP_NET_GET_STATS`) through node 1,
  drops every second matching occurrence with `limit=2`, and requires
  `applied=2` before QMP lowers only node 1's `wki-b` link. After link-up, a
  bounded query must prove the same nonzero peer boot epoch, local observation
  generation, cookie, and channel generation before exact detach reports zero
  active matches. NET has no wire resource-incarnation suffix, so these rows
  explicitly require `resource_incarnation=0` instead of inventing a token.
  A forwarded HELLO that arrives before reciprocal SPF is available remains
  unresolved and is retried by the bounded routed probe; it cannot be promoted
  to a direct neighbor using the intermediate hop's Ethernet path.
- B1 corrupts byte 12 of exactly one message-mode WRITE request
  (`type=0x43`, `op=0x0101`) so the block-count low byte becomes invalid. The
  workload exits zero only when its result row proves a negative WRITE status,
  unchanged scratch contents, successful restoration, a non-RDMA exact
  binding, and completed detach; `applied=1` is still mandatory.
- B2 uses `lane=*` so the same materialized plan runs against either required
  RDMA topology. It corrupts the block-count byte at offset 16 of one exact
  proxy WRITE SQE, proves safe rejection, clears, then duplicates one exact
  proxy WRITE doorbell and proves normal completion. Both operations report
  the selected `ivshmem` or `roce` lane, nonzero incarnation/cookie identity,
  valid ring geometry and indices, zero tag/slot bitmaps, and a quiescent ring.
  Old-CQE fencing remains the separate
  `WkiDevProxyRdmaCompletion/OldEpochTagCannotMatchOrFreeSuccessor` KTEST proof;
  the live row does not invent tag observations it cannot expose.
- X1 composes the existing adapters across three routed nodes. It holds only
  the checkpointed compute `TASK_COMPLETE` and IPC socket-control old-session
  tails. It admits the C2 helper through `task-running` and `exit-issued`
  before starting I2, avoiding an unobservable race between two simultaneous
  SSH launches while retaining both held remote-session tails. It resets node
  1 after both rules reach `applied=1`, proves SSH readiness
  plus changed connected boot epochs on both surviving consumers, publishes
  both successors, and only
  then releases the stale tails. Separately, it proves old NET-tuple rejection
  and exact successor attach/query/detach, one post-reset message-block
  verify/restore/detach from node 2, and an exact post-reset VFS `/tmp`
  unmount. It does not claim that an old block or VFS session crosses reset.
  A restarted peer may advertise the same hostname while its predecessor is
  still awaiting a liveness fence. Successor auto-mount attempts remain
  bounded; after the deferred worker releases an actual VFS withdrawal batch,
  it rearms the then-live exact observations so old same-path mount rows cannot
  leave the successor permanently discovered but unmounted. This is tied to
  teardown progress and does not widen a retry timeout.
  Remote helpers may open their executable through a software-RoCE VFS lane.
  Open prefetch is optional: its server sends the tagged data burst before the
  same-NIC control response. Receipt of that response is therefore the causal
  completion boundary; an incomplete burst falls back immediately to ordinary
  reliable VFS reads instead of waiting the generic VFS operation timeout.

All frame rules select WKI identities with `neighbor_host`, `src_host`, and
`dst_host`; VM ordinals are never treated as WKI node IDs. Each scenario keeps
every involved node's fault evidence before explicit clear and requires stable
post-clear convergence. These are executable contracts, not claims that a live
campaign was run in this revision.

Long-lived workloads use a paired `guest_start` and `guest_wait` operation in
one reducer group. The start records a bounded background process, the wait
collects its exit and bounded output, and every early failure or timeout forces
process-group cleanup plus the optional direct cleanup `argv`. Validation
or pair split across reducer groups.

Reset scenarios use `guest_poll`, never a timing sleep. It retries one direct
argv under a monotonic deadline with independently bounded attempt timeout,
interval, and attempt count, and retains every attempt's exit, timeout,
truncation, stdout/stderr, and host-monotonic bounds. C2, I2, and X1 first poll
node 1 SSH readiness, then collect `/proc/wki/peers` and the complete
`/proc/wki/netdiag` in one direct bounded command. The poll resolves the stable
hostname to exactly one connected remote row, requires that row's exact
lifecycle to be `CONNECTED`, and requires a nonzero remote boot epoch different
from its pre-reset snapshot. Disconnected predecessor rows may coexist while
diagnostic retirement is pending, but cannot qualify; multiple connected rows
are ambiguous and fail closed. This deliberately follows a rebooted peer whose
random node ID changed instead of mistaking the fenced predecessor for the
successor. X1 also requires the same proof from node 2 before its successor and
post-reset service work. Successor publication cannot precede the required
readiness and peer-epoch proofs.

Every real run has an unconditional `finally` cleanup on every declared node:
the runner retries the exact direct commands `wkictl chaos-workload clear` and
then `wkictl chaos clear` within fixed bounds. Workload clear must exit zero
and emit exactly one bounded row with `op=none status=0 detached=1`; an empty,
extra, malformed, or mismatching row fails cleanup even when the command exits
zero. Cleanup observations have their own telemetry and replay evidence. A
cleanup failure makes the outcome failed or incomplete without replacing the
first scenario failure. Dry runs record the cleanup contract but perform no
external command. Explicit success-path clear events retain the same row proof
so fault snapshots precede clearing and convergence.

Service adapter events may declare a `stdout_row` contract. It requires
exactly one tokenized result row with an exact prefix and bounded field set;
fields can be exact strings, members of a finite set, nonzero integers, or
negative integers. This is how B1's intentional rejection remains an exit-zero
success without reducing the proof to exit status alone.

Guest commands whose stdout is declared `capture_telemetry` must emit only
bounded `wos.telemetry` JSONL. Invalid or truncated telemetry makes the event
and run incomplete.

## Snapshots and convergence

Snapshot and convergence operations each supply five configurable guest argv
arrays, one for `/proc/wki/netdiag`, `/proc/wki/peers`, `/proc/wki/chaos`,
`/proc/wki/pipes`, and `/proc/kipcstat`. The runner saves raw output and parses
the current key/value rows. It treats a missing file, command error, timeout,
output truncation, malformed row, explicit `truncated` counter, incomplete
detail flag, unstable live RDMA ring snapshot, or missing chaos overflow
counters as incomplete evidence—never as a pass.

`/proc/wki/netdiag` and `/proc/wki/chaos` are complete only when their unique
final parsed rows are respectively `wki_netdiag_end complete=1` and
`wki_chaos_end complete=1`. This closes the case where a bounded proc buffer
ends cleanly on an earlier newline but silently omits later diagnostic groups.

A convergence event references a prior baseline snapshot and takes at most its
declared sample count before its fixed deadline. Two consecutive complete
snapshots must have the same invariant projection. The built-in evaluator then
requires connected peers, zero unallowlisted pending work/waiters/retransmit/
reorder/ref queues, zero chaos overflow and held queues, no duplicate visible
owner identities, no stale-generation and successor rows for one logical
owner, and packet-pool `used` and `free` values within the declared baseline
delta. Exact and logical ownership identities cover VFS, net, block, server,
resource, compute-lifecycle, and IPC export/proxy/pending rows. Published block
or NET proxy storage that is fully quiesced after unregister remains visible as
a raw-pointer lifetime tombstone, but is not an owner; missing detail or any
live operation, attach, detach, cleanup, resume, channel-generation, RDMA, or
binding gate fails closed and keeps the row in ownership checks. Per-scenario
`allow.nonzero`, `allow.max`, `packet_delta`, and `peer_disconnected` entries
make legitimate persistent service state explicit. `peer_replaced` names a
logical hostname whose old disconnected ID may remain as a fenced tombstone,
but only when the snapshot also proves one different connected successor, a
different nonzero boot epoch, the old lifecycle row's exact replacement link,
and zero retirement/cleanup gates. It cannot waive an arbitrary disconnected
peer. C2 and I2 use this proof on node 0; X1 uses it on both surviving
consumers, nodes 0 and 2, for the reset of `wos-1`.

By default, those bounded convergence samples are evenly distributed across
the event's existing `deadline_ms` instead of being captured back-to-back.
The runner reads packet-pool-bearing `netdiag` first and reuses its per-node SSH
control connection across commands, so collection does not manufacture a
stream of FIN retransmit buffers. An explicit `interval_ms` overrides the
cadence; `interval_ms: 0` is intended for deterministic fixtures that do not
use a live guest transport. The deadline and `max_samples` remain hard bounds,
and no timeout or packet delta is widened while waiting.

These diagnostics are separate subsystem snapshots, not a globally atomic
kernel transaction. Consecutive stable post-heal samples establish the bounded
quiescent observation point; they do not invent cross-node simultaneity.

## Committed matrix and completion

[`configs/wki_chaos_matrix.json`](../configs/wki_chaos_matrix.json) commits C1,
C2, V1, V2, I1, I2, N1, B1, B2, and X1 with fixed seeds, deadlines, exact
setup/workload/fault/heal/snapshot/convergence phases, transport prerequisites,
invariant signatures, and current execution readiness. `model-only` means the
contract is committed while a concrete scenario adapter/workload is still
required; it does not waive any required result lane.

[`configs/wki_chaos_scenarios.json`](../configs/wki_chaos_scenarios.json) is
the machine-readable readiness catalog. It maps all ten IDs to the exact host
checks and KTEST PASS markers that can be run now, and to either a validated
live scenario or a precise list of missing workload/topology adapters. Inspect
it with:

```sh
bin/wos-wki-chaos readiness
bin/wos-wki-chaos materialize \
  --scenario C2 --output test-results/wki-chaos-scenarios/C2.json
```

`materialize` fails closed and prints the exact blockers while an entry is
`model-only`; it emits a validated scenario only after the catalog contains a
complete live scenario and both catalog and matrix say `live-ready`. At this
revision all ten entries are `live-ready`. C1, C2, V1, V2, I1, and I2 drive
their checkpointed direct workloads; N1, B1, B2, and X1 drive the strict
kernel-backed service adapters described above. Every plan proves exact
injector application, retains all involved nodes' pre-clear evidence, runs
workload cleanup before injector cleanup, and requires stable post-clear
convergence. Live-ready describes an executable evidence contract; it is not a
claim that its required live lane passed in this revision.

The 2026-08-31 Goal 08 qualification record is retained under
`test-results/wki-chaos-goal08-final-20260831-v45/`. Its report has
`fullMatrixPassed=true`: every required host-model, KTEST-model, Ethernet,
ivshmem, and RoCE lane is executed, complete, and passed. The isolated suite
finished with 8062 passed and 0 failed. This dated local evidence record, not
the `live-ready` catalog label by itself, supports the completion claim.

Lane results are not hand-written pass booleans. The host and KTEST qualifiers
execute or ingest real evidence, retain bounded stdout/stderr or the source
serial log, inventory every evidence artifact by size and SHA-256, and emit
`SCENARIO/LANE/result.json`. Matrix evaluation re-hashes that inventory and
rejects missing, moved, or edited evidence, contradictory check fields, and
results without the qualifier identity. Generate the available model lanes,
after building the host tests, with:

```sh
bin/wos-wki-chaos qualify-host \
  --scenario C1 \
  --host-test-build "${WOS_TEST_BUILD_DIR:-/tmp/wos-tests}" \
  --output test-results/wki-chaos-matrix/C1/host-model

bin/wos-ktest --config configs/node_ktest_rootless.json \
  --kernel-cmdline "--selftest" \
  --build-only --reset-sysroot
bin/wos-ktest --config configs/node_ktest_rootless.json \
  --kernel-cmdline "--selftest" \
  --no-build --no-setup

bin/wos-wki-chaos qualify-ktest \
  --scenario C1 \
  --serial ktest-data/serial-vm0.log \
  --output test-results/wki-chaos-matrix/C1/ktest-model
```

Repeat the qualifier commands for each matrix ID. KTEST qualification uses
only the most recent suite block, requires its unique zero-failure terminal
summary, and requires each scenario-specific PASS marker in that same block.
An older successful block cannot qualify a newer partial boot.

Use I1's committed fixed seed for its required Ethernet transport lane:

```sh
bin/wos-wki-chaos materialize \
  --scenario I1 --output test-results/wki-chaos-scenarios/I1.json
bin/wos-wki-chaos attach \
  --scenario test-results/wki-chaos-scenarios/I1.json \
  --seed 0x1101000000000001 \
  --output test-results/wki-chaos-matrix/I1/ethernet
```

Supplement the fixed campaign with separately retained randomized seeds; do
not overwrite or substitute them for the committed fixed lane:

```sh
bin/wos-wki-chaos attach \
  --scenario test-results/wki-chaos-scenarios/I1.json \
  --seed 0x5eed000000000001 \
  --output test-results/wki-chaos-random/I1/5eed000000000001
```

For B2, independently run the same materialized B2 scenario against the
ivshmem and RoCE topologies and place the fixed-seed manifests under
`B2/ivshmem` and `B2/roce`. The absence of either real manifest is incomplete,
never a synthetic pass.

Evaluate it with:

```sh
bin/wos-wki-chaos matrix \
  --matrix configs/wki_chaos_matrix.json \
  --catalog configs/wki_chaos_scenarios.json \
  --results test-results/wki-chaos-matrix \
  --output test-results/wki-chaos-matrix-report.json
```

The full matrix is `passed` only when every declared required lane is present,
executed, complete, and passed. Missing, model-only-but-unrun, skipped,
malformed, or evidence-incomplete lanes make the report incomplete; any real
violation makes it failed. B2 independently requires ivshmem and RoCE RDMA live
lanes. Ethernet service lanes require at least two nodes, while route flap and
X1 require at least three. Reboot cases require QMP, and every live case
requires the test-only WKI chaos boot opt-in.

## Implemented block-ring and doorbell fault hooks

B2 uses semantic block-ring hooks rather than inferring direct-RDMA behavior
from `DEV_OP` frames. The provider function-pointer ABI and the RoCE/ivshmem
wire formats are unchanged; arbitrary byte-level `rdma_read`/`rdma_write`
faults, torn writes, CQ corruption, and region revocation remain outside this
surface.

The proxy constructs every SQE in a stack-local `BlkSqEntry`, applies an
optional one-byte `blk_sqe` XOR to that copy, and assigns the complete copy to
the already validated, currently owned SQ slot. Existing data-before-SQE,
compiler-barrier, and SQ-index publication order is unchanged. The hook never
retains the descriptor address. Its disabled path is one relaxed atomic load;
the enabled path uses the bounded chaos lock and does not allocate, sleep,
yield, or log. Trace rows contain the selected byte offset/mask and its exact
original and mutated values. `sizeof(BlkSqEntry)` remains 24 bytes and no field
was added to the shared ring.

`blk_doorbell` surrounds the complete proxy-to-server SQ notification and the
server-to-proxy CQ notification. `drop` suppresses the semantic notification;
`duplicate` invokes that same idempotent notification path twice. `delay`
copies only immutable scalar identity into an eight-entry fixed pool—never a
ring, binding, transport, or descriptor pointer—and requires explicit
`release`. A ninth held event sets sticky overflow/invalid state and is
suppressed fail-closed. The deferred drain is bounded by the combined fixed
frame and block-event capacities.

The copied identity is `{surface, direction, origin, lane, transport,
neighbor, zone, resource, ring_generation, ring_index, operation_cookie,
blk_op, attach_cookie, channel_generation, owner_boot_epoch,
resource_incarnation}`. `ring_generation` is a local nonzero digest of the
existing channel generation, attach cookie, boot epoch, and resource
incarnation; it is not a new wire field. A proxy release re-resolves permanent
published state under the proxy registry lock, rejects inactive, fenced, or
cleaning state, and acquires an atomic callback retain before unlocking. It
then rechecks the complete identity, valid ring geometry/indices, and the
still-owned tag before signaling. Teardown fences new retains, waits for the
submitting I/O and callback retain to drain in task context, and only then
clears RDMA identity or zone state. It deliberately does not acquire
`io_lock`: the submitting operation holds that lock while awaiting the delayed
notification. The server side similarly retains the exact non-retiring
binding under its registry lock, rechecks the complete identity and ring, and
releases that reference after notification; successor reuse cannot cross the
retained retirement boundary.

Every copied doorbell carries a finite local delivery deadline. Proxy events
reuse the operation's existing 100 ms deadline; server completion events use
a conservative 400 ms ceiling. Expired release is terminally traced as failed
and never delivered. Therefore live B2 does not depend on an SSH round trip to
release a proxy delay within 100 ms: the independent ivshmem and RoCE live
lanes use one-shot `duplicate` and `blk_sqe` corruption. Drop/delay schedules
remain model/KTEST coverage unless a dedicated in-flight live coordinator
supplies their bounded completion semantics.

`clear` discards held events but refuses to erase an in-flight callback.
Transport unregister discards each held/ready block event for that transport
ID exactly once and records `transport_removed`; an already in-flight event is
finished by its exact retained callback. Release transitions one held event to
ready once, and repeated finish/release calls cannot double-complete it.
These rules preserve the existing block tag epoch, cookie/generation/
incarnation fencing, descriptor ownership, writer lease, binding refs, and
retirement behavior.

`/proc/wki/chaos` keeps schema 1 and reports aggregate `queued`, plus
`queued_frames` and `queued_block_events`. Rule rows add `surface`, block
selector/value pairs, and SQE corruption parameters. Block trace rows add the
complete identity, delivery deadline, and SQE before/after bytes; release
rows report success, expiry/stale failure, or transport removal explicitly.
Summary, active-rule, and trace rows are copied under one chaos-lock
acquisition, so a completed proc snapshot is one internally consistent
generation. Frame release/heal rows include their bounded retransmission
coalescing count. These are additive local diagnostics, not wire ABI.
