# WKI routing convergence

The routing worker owns SPF, advertisement transmission and routed discovery.
RX validates and coalesces advertisements into a fixed LSDB; it does not run SPF
or send advertisements. Source authority is `net/wki/routing.cpp`,
`routing_model.hpp`, `routing_lease.hpp`, and the `ResolvedRoute` consumer in
`wki.cpp`.

## Publication and lifetime

The worker captures a revision, copies one LSDB entry per routing-lock hold,
computes outside the lock, and publishes only if the revision is unchanged.
Service and discovery tables switch together. A reader copies one complete
route under the lock; it never retains a pointer into reusable table storage.

The route records next-hop boot/session generation and transport identity.
TX validates those identities while retaining the next-hop lifecycle and
transport registry membership. Service TX also retains the destination's
lifecycle, and final submission rechecks the expiry and revocation barrier.
A synchronous send already owning the exact
current task's completed exclusive lifecycle can borrow that ownership; it
cannot borrow a writer still draining readers. Peer/contact mutations obey the
same lifecycle. No routing lock crosses TX or peer/channel locking.

## Negotiated leases

`wki.routing_lease` is an explicit boot-command-line opt-in. It advertises
`WKI_CAP_ROUTING_LEASE`; the existing LSA prefix and neighbor layout are unchanged.
Only negotiated peers exchange the appended versioned identity/lease suffix or
the `LSA_ACK` challenge payload. A full authenticated boot nonce distinguishes
even colliding 32-bit boot epochs. Epochs are opaque identities, not counters.

A discovery advertisement does not grant service reachability. The receiver
must authenticate the origin and challenge it end-to-end. Only a matching
response establishes a lease, whose deadline starts at the receiver's request
time, not at arrival. Thus reliable retransmissions cannot extend the lifetime
of indefinitely queued bytes. Repeated sequence values need an exact unchanged
payload and a fresh challenge; serial half-range ambiguity is rejected.

Origin-authenticated `PEER_GOODBYE` is an immediate terminal withdrawal; queued
advertisements and an idempotent old-session HELLO cannot reverse it. A new
completed session is required to admit that origin again. Third-party
`FENCE_NOTIFY` is advisory until the receiver independently confirms the local
fence. The suffix decoder also accepts authenticated challenged withdrawals
with zero neighbors; local graceful shutdown uses `PEER_GOODBYE`, not a new
withdrawn-LSA emission API.

Service lookup checks expiry synchronously, including the minimum lease of all
origins along the selected path. Discovery routes are available only for
HELLO/ACK/CONFIRM and LSA/challenge messages, never as a service fallback.
Legacy compatibility cannot provide the same end-to-end age guarantee: old
relayed advertisements contain no authenticated origin epoch or lease. Keep
that limitation distinct from the negotiated guarantee in mixed-version tests.

## Bounds and what they prove

| Surface | Configured bound |
| --- | --- |
| LSDB and route capacity | 256 nodes, 32 neighbors per advertisement |
| Worker scheduling interval | 20 ms, plus actual work and scheduler delay |
| Own topology advertisement rate | At most one new generation per second |
| Origin lease / refresh / reply timeout | 30 s / 10 s / 5 s |
| Discovery advertisement lifetime | 60 s |
| Challenge / response attempts | Separate budgets of 8 per worker iteration |
| Flood attempts | 256 per worker iteration |
| Routed HELLO attempts | 8 per second |
| Packet forwarding TTL | 16 initially; each forwarding hop decrements it |

The production pure SPF has fixed O(V) working storage and
O(V² + V·E) work, with E bounded by 32V. The worker's LSDB and table scratch are
static, not large kernel-stack allocations. RX performs at most 32-neighbor
validation and fixed-capacity admission; overload coalesces newer state.
Routing static storage is approximately 614 KiB with the current layouts;
three 182272-byte LSDB copies dominate that cost.
The flood budget can service a worst-case 256-by-256 target sweep in 256
iterations (5.12 s of configured sleeps), below the 10 s refresh interval
before adding computation, transport and scheduler time. This is a throughput
budget, not a guarantee under loss, continuous topology churn or CPU starvation.

For a common topology, positive edge costs make destination distance strictly
decrease at each next hop: deterministic equal-cost selection cannot form a
loop. Independently published topologies can transiently microloop during
change. The host test explicitly demonstrates this counterexample; this is
not a distributed ordered-update protocol. Packet TTL bounds each such attempt,
and negotiated lease expiry bounds stale service state even without worker
progress. Scheduling and packet delivery assumptions must accompany any
wall-clock convergence claim.
The 256-node capacity does not imply 255-hop transport reachability: usable
service paths must fit the existing 16-hop wire TTL. Unknown or fenced peers
can be probed through bounded discovery paths even when lease support is off;
legacy duplicate observations can refresh discovery, but never restore fenced
service reachability without a new completed session.

`/proc/wki/netdiag` adds `wki_routing version=1` counters and `wki_route` service
snapshots. `now_us` and `lease_expires_us` are local guest monotonic times, not
cross-node timestamps. Max RX/SPF timings are observations, not hard real-time
guarantees. The existing packet-pool, peer, channel and terminal-completeness
rows remain available for chaos accounting.
The normal-build three-node qualification campaign checks observed maxima
against a 250-us RX budget and a 20-ms SPF budget, in addition to exact route
sets, coherent generations, unexpired visible leases and unchanged packet
accounting. These test budgets are not scheduler guarantees or substitutes
for the separate maximal-capacity host benchmark.

## Isolated live fixture

`configs/cluster_wki_routing.json` uses three nodes with WKI chain 0–1–2,
socket-multicast WKI links and an existing TAP-backed management LAN. WKI node
identities are 1–2–3. It has separate routing disks, overlays, QMP sockets and
logs. Do not reuse its artifacts while its VMs are running.

Seed and package the normal-build kernel into the isolated fixture:

```sh
cp --reflink=auto --sparse=always --no-clobber \
  mountfs.qcow2 wki-chaos-data/routing-mountfs.qcow2
WOS_BOOT_DISK=wki-chaos-data/routing-disk.qcow2 \
WOS_ROOTFS_DISK=wki-chaos-data/routing-mountfs.qcow2 \
WOS_BOOT_IMAGE_MANIFEST=wki-chaos-data/routing-boot.manifest \
WOS_KERNEL_CMDLINE='wki.routing_lease wki.chaos wki.chaos.workload' \
  scripts/build/make_image.sh
bin/wos-cluster --config configs/cluster_wki_routing.json --launch --no-setup
```

`--no-setup` permits live execution/debugging without root setup when the
configured management bridge/TAPs already exist and are usable. Node 2's WKI
interface is QMP `net1`; its management interface is `net0`. The middle node's
second WKI leg is `net2`. Link changes must target those explicit interfaces,
leaving management reachable for diagnostics.

After all three nodes have management access and both expected service routes,
run the reproducible qualification scenarios (the supplied management addresses
are 10.10.0.235, .234 and .233; adjust them if DHCP differs):

```sh
bin/wos-wki-chaos attach --scenario configs/wki_routing_partition.json \
  --seed 27 --output wki-chaos-data/routing-partition-run
python3 scripts/test/wki_routing_check.py wki-chaos-data/routing-partition-run
bin/wos-wki-chaos attach --scenario configs/wki_routing_flaps.json \
  --seed 27 --output wki-chaos-data/routing-flaps-run
python3 scripts/test/wki_routing_check.py --flaps wki-chaos-data/routing-flaps-run
```

Use a new output directory per run. The first scenario cuts only node 2's WKI
interface for 35 seconds, checks withdrawn routes, then allows 75 seconds for
recovery. The second applies three two-second down/up cycles before checking
recovery. Runner completion alone is not a routing assertion: run the checker.
If an ancillary diagnostic fails after recovery, retain that campaign as
incomplete. A separately saved recovery capture from the same uninterrupted
VM run can be checked with `--recovery-directory PATH`; the checker reports
that provenance explicitly and checks monotonic guest times and unchanged
next-hop boot identities. This does not turn the original runner result into
a pass or establish its original recovery-capture deadline.
Repeat against a separately packaged cap-off image and corresponding QMP paths
to qualify legacy mode. If initial rootfs mount fails with `-11`, preserve the
serial log before resetting that VM; this remains an unresolved boot issue,
not evidence of routing convergence or successful management startup.

For a mixed rollout, package a second boot image without `wki.routing_lease`
and override only the selected node's `vm.disk0` in a separate config. Preserve
both exact images and configs with the resulting evidence. Run the ordinary
Build WOS, isolated diagnostic KTEST, host model/source/fuzz checks, partition,
heal, reboot, flap and packet-accounting checks before treating a version as
qualified. Goal 27's execution ledger records which checks actually ran.
