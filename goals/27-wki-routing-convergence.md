# Goal 27: Loop-Free, Bounded WKI Routing Convergence

## Planning metadata

- **Target horizon:** one to two continuous weeks with protocol/model, task-context SPF, publication, and chaos workstreams.
- **Critical surfaces:** WKI RX/NAPI, LSA wire format, LSDB lifetime, peer sessions/fencing, route publication/pinning, packet forwarding, and observability.
- **Completion rule:** stale routes must disappear, published routes must never reference fenced generations, and loops/latency must respect explicit deterministic bounds.

## Copy-pastable goal command

```text
/goal Outcome: Replace WKI RX-context synchronous route recomputation with an epoch-aware, aging, withdrawal-capable, task-context routing pipeline that atomically publishes loop-free bounded route snapshots through reboot, partition, heal, and flap. | Verification surface: exhaustive small-topology and randomized bounded models, malformed/stale/wrapped/reboot/withdrawal/equal-cost tests, RX and recomputation latency budgets, packet accounting, Normal Build WOS, isolated KTEST, WKI chaos, and user-run partition/heal cluster traces. | Constraints: Preserve authenticated WKI wire compatibility through capability gating, bounded allocation-free RX validation, TTL as defense in depth, peer/session fencing, packet ownership, and no transmission or O(V^2) work under routing spinlocks. | Boundaries: Evolve LSA identity/freshness/withdrawal, deferred SPF, immutable route snapshots, diagnostics, and tests; do not replace WKI transport, peer authentication, or service protocols. | Iteration policy: Specify convergence invariants in a pure topology model, add generation/aging wire negotiation, defer computation, atomically publish snapshots, then enable withdrawals and chaos scenarios while measuring every bound. | Blocked stop condition: Stop only after the same indispensable cluster/runtime blocker repeats for three goal turns, all safe topology/model/KTEST/chaos alternatives are exhausted, and required topology/log evidence is exact.
```

## Execution record (2026-09-07)

- Status: Done. The original `/goal` objective was loaded verbatim; completion
  is against the bounded convergence contract below, not instantaneous
  distributed loop freedom across different topology generations.
- Appended execution authorization: I am able to run and debug WOS live by
  appending --no-setup to the wos-ktest and wos-cluster scripts to not need root
  access. This overrides older user-run-only debugging instructions.
- Reconnaissance: routing.cpp owns LSDB admission, SPF, publication, and LSA
  flooding; peer.cpp calls it from HELLO, fence/goodbye RX, and the timer;
  wki.cpp consumes it for raw/reliable TX, retransmits, ACKs, and forwarding.
  The topology model and send-path integration are delegated bounded workstreams.
- Initial reconnaissance confirmed synchronous RX SPF, disabled aging, and separately
  published peer routing fields. Existing authentication uses random boot epochs
  (opaque identities, not ordered counters) and session-bound HELLO capabilities.

### First patch contract

Problem: RX computes routes and floods LSAs synchronously; stale peer route
fallbacks survive table withdrawal.
Expected files to change: routing.cpp/.hpp, routing_model.hpp, peer.cpp, wki.cpp,
host routing/model/source tests, and this execution ledger.
Files already read: root and subsystem instructions; routing.cpp/.hpp,
wki.hpp, wire.hpp control-plane layouts, wki.cpp RX/send/init/worker entrypoints,
peer.cpp HELLO/fence/timer paths, auth.cpp/.hpp, netpoll.hpp, netdevice.hpp,
kernel CMake configuration, existing routing tests, and Build WOS task/script.
Invariants to preserve: fixed bounded storage, authenticated admission, packed
wire layouts, packet ownership, kernel task stack bounds, and KTEST isolation.
Locks involved: routing lock; global/per-peer locks and auth replay lock;
transport registry lock. Never nest peer locks below the routing lock.
Unsafe contexts: RX/NAPI and HELLO/fence handlers only request/coalesce work.
Allocation/blocking assumptions: routing RX and database copies allocate nothing;
worker owns preallocated large scratch storage; SPF and TX occur without the
routing lock. Dedicated worker avoids blocking behind VFS attach operations.
ABI/wire/syscall compatibility impact: none in the first stage; negotiated epoch
extension remains a later required stage, not assumed delivered here.
Build/test plan: exhaustive/random production-model tests, source boundaries,
Normal Build WOS, then isolated KTEST and rootless cluster chaos evidence.
Rollback plan: revert this focused pipeline patch together with its consumers;
never publish partially computed routes.

### Iteration ledger

1. Hypothesis: fixed coalesced LSDB updates plus revision-checked worker SPF
   removes quadratic RX lock time while keeping route generations coherent.
   Patch and evidence pending. Remaining completion work includes negotiated
   epochs/withdrawals, final lifecycle pinning, aging compatibility, overload and
   latency evidence, KTEST, and live partition/reboot/heal traces.
2. Implemented shared pure SPF, bounded deferred routing worker, revision-checked
   double-buffer publication, next-hop generation validation, routed-hop lifecycle
   and transport retention, and no stale peer-route fallback. Review fixed lost
   own-LSA notification, peer-snapshot revision, and transport-change refresh races.
3. Evidence (first-stage binaries, not final-goal evidence): Normal Build WOS
   passed twice; 27 ASan/UBSan host routing/model/lease tests passed, including all
   4096 four-node directed topologies and 2000 randomized weighted topologies;
   routing fuzz completed 10000 seeded cases. Rootless isolated diagnostic KTEST
   passed 8274 assertions with zero failures, including five new routing KTESTs.
   KTEST packaging initially hit sandbox-denied libguestfs socket binding; an
   approved outside-sandbox rootless retry rebuilt the isolated images and passed.
   The generated KTEST rootfs image was replaced by packaging and is rebuildable.
4. Rootless three-node chain booted and authenticated both routed endpoints by
   approximately 3 seconds. QMP link-down on node 2 fenced node 3 at middle-node
   guest time 126.526s. Live GDB showed an invalid LSDB tombstone, max RX 1us,
   max SPF 3us, no LSDB-full or flood-retry events. After link restoration the
   new resolver blocked HELLO_ACK to a fenced direct peer: this is a discovered
   regression under repair, not passing heal evidence. Logs saved under
   wki-chaos-data/goal27-phase1/; first-stage ELF is /tmp/wos-goal27-stage1.elf.
   All three test VMs were stopped through their own QMP sockets afterward.
5. Protocol review: age suffixes alone cannot bound stale resurrection because
   reliable queues retain exact old authenticated bytes indefinitely. The next
   stage uses end-to-end authenticated origin challenges with local request-start
   lease deadlines, full boot nonce identity, sticky downgrade rejection, and
   separate discovery-only routes. Its pure lease model is verified; kernel and
   wire integration is in progress and the capability stays opt-in pending mixed
   version evidence. Procfs routing counters/route snapshots are being added.
6. Negotiated lease integration now has separate discovery/service graphs,
   request-start deadlines propagated through every path origin, authenticated
   boot identity tracking, and synchronous lookup expiry. Review corrected
   relayed-hint capability poisoning and legacy reboot sequence retention.
   Direct/routed TX and advisory contact mutations now share peer lifecycle
   admission. A new isolated three-node fixture and protocol/bounds guide live
   in configs/cluster_wki_routing.json and docs/wki_routing.md. Final submission
   expiry and origin-session invalidation checks are under integration.
   The third normal build has started; this entry is not a passing-build claim.
7. Third Normal Build WOS passed with the lease integration. The all-new
   rootless chain booted and renewed endpoint leases. The middle node initially
   failed its rootfs mount with -11; a planned QMP reboot mounted successfully
   and both direct sessions eventually recovered (17.844s and 47.370s after
   reboot), followed by all routed service leases. A 35-second endpoint WKI
   partition removed all cross-cut service routes; healing recovered direct and
   routed sessions in approximately two seconds. Chaos snapshots before/during/
   after preserve exact pool usage 769/1025/769 with no growth or TX refusal.
   Campaign evidence is in wki-chaos-data/goal27-partition; captured source-build
   ELF /tmp/wos-goal27-phase2.elf and logs wki-chaos-data/goal27-phase2 are
   intermediate evidence, not validation of subsequent race fixes. Observed
   maxima across these three nodes: RX 15us, SPF 81us.
8. Host production-model/wire/lease suites now pass 30 tests under ASan/UBSan.
   The maximal 256-node, 8192-edge standalone SPF benchmark recorded median
   1839us / maximum 1929us across 31 host samples, with 3328-byte workspace.
   These are instrumented host timings, not end-to-end kernel latency bounds.
   Final review additionally requires completed-session-only origin authority,
   destination lifecycle retention and final publication-revocation admission;
   those fixes are in progress before the final normal/KTEST/live rebuild.
9. Completed-session authority, service-origin lifecycle retention, final
   revocation admission, and terminal GOODBYE barriers are implemented. Review
   corrected third-party FENCE_NOTIFY to wait for independent local confirmation.
   Both subsequent diagnostic KTEST runs passed 8310 assertions / zero failures.
   All 18 WKI source-regression suites and the KTEST manifest check passed.
10. Mixed-build bootstrap used two lease-enabled kernels and the frozen
    pre-lease first-stage kernel on node 3. Authenticated routed sessions formed
    both ways and renewed steadily for over 100 seconds; new-node diagnostics
    had no malformed or rejected lease messages. Evidence: goal27-mixed-baseline
    and goal27-mixed under wki-chaos-data. This is cross-build bootstrap evidence,
    not a claim that every historical kernel passes recovery. A recurring
    middle-node initial rootfs mount -11 remains outside the routing proof;
    resetting that VM previously recovered its management services.
11. Final compatibility review fixed cap-off discovery: terminal service
    tombstones must not prevent bounded routed HELLO recovery. Legacy duplicate
    observations now refresh discovery only, never revive services. Host suite
    now passes 31 tests. Flood service budget increased from 8 to 256 attempts
    per iteration so a 65536-target sweep is not inherently starved by 10-second
    refreshes (5.12s nominal sleeps, plus actual work/scheduling/transport).
    Latest Normal Build WOS passed. Matching candidate ELF is
    /tmp/wos-goal27-candidate.elf.
12. The exact candidate passed diagnostic KTEST: 8310 assertions, zero failures
    (71.906s guest time; log and normal candidate ELF in
    wki-chaos-data/goal27-candidate). Its enabled-mode partition/heal campaign
    passed all nine semantic snapshots, and three short down/up flaps passed
    all six snapshots. Both preserved exact packet-pool accounting, coherent
    generations and unexpired visible routes; maximum RX was 2us and SPF 91us.
    Evidence: goal27-candidate-partition-retry and goal27-candidate-flaps.
    The earlier goal27-candidate-partition attempt was incomplete because a
    guest had no management service after initial rootfs mount -11; it is not
    counted as a pass. QMP reset recovered that guest, and authenticated direct
    and routed sessions returned at guest 18.362s and 20.113s respectively.
13. Persisted reusable partition/flap scenarios and the fail-closed snapshot
    checker in configs/wki_routing_{partition,flaps}.json and
    scripts/test/wki_routing_check.py. JSON parsing and Python compilation pass.
    Latest reruns passed 31 sanitizer routing/model tests and 31 chaos tests
    (zero skipped), plus routing/peer source guards. A sandbox-only LeakSanitizer
    ptrace incompatibility was resolved by rerunning the unchanged binary
    outside that wrapper, with exit status zero. Formatting checks and tidy
    passed with nonfatal style/include and pre-existing branch-clone warnings;
    the results are not warning-free. WOSDBG loaded all three candidate logs
    and reconstructed WKI/distributed traces successfully; global timestamp
    order is unavailable, so cross-node monotonic times are not ordered.
14. Default cap-off routing qualification passed nine semantic snapshots using
    baseline/partition from goal27-capoff-partition-retry and the separately
    complete goal27-capoff-recovery capture from the same uninterrupted VM run.
    Exact route sets, unchanged packet accounting, coherent generations,
    monotonic guest times and unchanged next-hop boot identities all passed.
    Maximum RX was 1us and SPF 59us; all lease request/response counters were
    zero. Authenticated routed recovery appeared 1.1s after link restoration
    in the guest log. The two original cap-off campaigns remain INCOMPLETE:
    the first could not load /lib/ld.so for a pipes read; the retry timed out
    after 5s reading /proc/kipcstat. Subsequent direct reads and the complete
    recovery capture passed. These ancillary failures are retained as risks,
    not relabeled as successful full campaigns or a 75s capture-bound proof.
15. Final 10000-input seed-27 fuzz smoke also passed with -max_len=1024 and
    -len_control=0, reaching full-size lease suffix inputs immediately.
    Rechecked both enabled campaigns with the final checker, Python syntax,
    the 649-enabled/3-disabled KTEST manifest and git diff --check. Generated
    VM authentication provisioning is ignored by wki-chaos-data/.gitignore.
    All task-owned cluster VMs were stopped; logs, exact candidate ELF, KTEST
    evidence and cap-off fixture config remain in goal27-candidate.

### Final scope and remaining limits

- Changed WKI routing/model/lease/wire definitions, peer lifecycle and send-path
  admission; added procfs routing diagnostics, host/fuzz/KTEST coverage, isolated
  chain fixture, reproducible chaos scenarios, checker and design documentation.
- Preserved authenticated legacy framing through opt-in capability gating,
  fixed allocation-free routing RX, packet ownership and peer/session fencing;
  SPF and transmission execute outside routing spinlocks.
- Common-topology snapshots are loop-free; independent changing generations
  may microloop transiently, bounded per packet by the existing TTL of 16.
  Strong end-to-end aging requires negotiated leases, which remain opt-in.
  Legacy peers cannot supply an origin-age guarantee absent from their wire
  format. Mixed evidence covers cross-build bootstrap, not every historical
  release or historical mixed-version partition-recovery combination.
- Latency observations are qualification budgets, not hard scheduling bounds;
  maximal graph coverage is host/model evidence, not a 256-VM deployment test.
  Initial rootfs mount -11 and intermittent ancillary diagnostic failures remain
  unresolved. No cache or public-branch claims supersede the local source.

### Second patch contract

Problem: asynchronous publication alone does not establish origin freshness,
expire arbitrarily delayed reliable advertisements, or close the direct TX fence
race; first live partition exposed fenced handshake admission regression.
Expected files: routing.cpp/.hpp, routing_lease.hpp, wire.hpp, wki.cpp/.hpp,
peer.cpp, procfs.cpp, routing and peer host/source/KTEST tests.
Read surfaces: first-contract files plus auth_protocol/auth_policy, the shared
peer lifecycle entrypoints, procfs network diagnostics and chaos snapshot parser,
and rootless cluster/QMP entrypoints.
Invariants: legacy packed prefixes and secure-v3 framing stay intact; capability
is opt-in until mixed-version evidence; only authenticated origin challenges
renew leases; expired service routes cannot fall back to discovery paths;
HELLO/ACK/CONFIRM remain available for fenced direct-peer recovery.
Locks: RX admission takes bounded routing locks after peer identity capture;
worker copies under locks then computes/transmits outside; route TX retains a
shared lifecycle lease or explicitly borrows its own completed writer lease.
Contexts: RX/NAPI never runs SPF, waits for challenges, or allocates routing
records; periodic task owns bounded challenge/reply/flood work.
Allocation: fixed LSDB, lease/request slots, and double-buffer graph snapshots;
procfs may allocate bounded diagnostic output scratch in syscall context.
ABI/wire: additive negotiated LSA suffix and LSA_ACK challenge only; legacy
payload and service messages unchanged; procfs adds versioned rows.
Validation: malformed/old/wrapped/session/lease/withdrawal tests, live legacy
recovery regression, enabled and mixed cluster validation, normal and diagnostic
builds. Rollback: disable the capability, retain coherent old-protocol worker;
do not restore stale per-peer routing fallbacks.

## Original source evidence (before implementation)

`routing.cpp::handle_lsa`, reached from `wki_rx`, stored an advertisement and performed synchronous O(V²)-style recomputation under the global routing lock. LSDB aging was disabled in the routing timer. LSA identity used origin/sequence without a full boot/session freshness or explicit withdrawal contract. Forwarding remained in RX/NAPI context and TTL was the final loop bound, while peer routing fields and route-table state were published separately.

Goal 08 supplied route-flap chaos machinery but deliberately avoided a routing protocol redesign.

## Measurable completion contract

1. A route model defines origin boot/session epoch, sequence arithmetic, age, withdrawal, tie-break, cost saturation, and unreachable state.
2. RX performs only bounded length/auth/freshness validation and enqueue/coalescing; it never runs SPF or emits advertisements under a spinlock.
3. A task-context worker snapshots the LSDB, computes routes with explicit complexity/memory bounds, and discards obsolete results.
4. Complete immutable route generations publish atomically; forwarding pins one generation and never mixes next-hop/peer state.
5. Peer fence, reboot, session rotation, LSA expiry, and withdrawal remove routes within documented time and cannot resurrect stale state.
6. Equal-cost selection is deterministic or explicitly load-balanced without persistent loops; next hops are live for the pinned peer generation.
7. Queue overload coalesces safely and provides bounded recovery, reason-coded counters, and no packet/route-reference leak.
8. Compatibility with old peers is capability-gated and cannot let unauthenticated or stale fields override newer state.
9. Model and cluster tests demonstrate convergence and loop bounds for configured topology limits.

## Invariants, locks, and unsafe contexts

Routing RX/NAPI is allocation-free or uses fixed bounded pools. Never hold the routing lock while transmitting, allocating, logging expansively, or acquiring peer/channel locks in an unproved order. Snapshot reclamation waits for all forwarding readers. Epoch/sequence comparisons must be wrap-safe and authenticated.

## Boundaries and overlap

In scope are WKI link-state convergence and route publication. Generic IPv4/IPv6 routing, TCP congestion, peer authentication, and service semantics are out of scope. Reuse Goal 08 chaos and Goal 13 telemetry. New LSA fields must remain covered by Goal 09's authentication envelope.

## Parallel workstreams

The primary agent owns invariants, protocol compatibility, and lock/publication design. Subagent A builds exhaustive/random topology models. Subagent B implements deferred LSDB/SPF. Subagent C implements immutable publication/forwarding. Subagent D may add chaos/observability on disjoint files.

## Verification, iteration, and rollback

Run model/property/fuzz tests, focused host WKI suites, Normal `Build WOS`, isolated KTEST, route-flap chaos, and user-run partition/reboot/heal scenarios. Keep the new protocol capability disabled until mixed-version convergence passes. Roll back to the prior coherent route generation, never a partially published table. Use the `/goal` blocked condition exactly.
