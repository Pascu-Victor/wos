# Secure WKI RDMA

Goal 25 implementation and qualification record. Authenticated secure records
and all four consumers are implemented behind independent opt-in negotiation.
The security evidence below covers production-source models, isolated KTEST
and the stated software-QEMU topologies. The default service mask remains zero;
multicore availability observations and physical hardware qualification still
limit production rollout.

Private VM qualification uses standalone read-only seed images with private
qcow2 overlays (`wki_secure_rdma_fixture.py --image-mode qcow2-overlay`).
The helper records distinct overlay and backing hashes and rejects writable
or indirect bases. Sequential Goal25 campaigns check a 20 GiB disposable-image
budget and a 40 GiB free-space floor before image preparation and launch.
After successful and failed runs, only proven inactive owned images are pruned;
serial logs, scripts, configurations and receipts remain. Current builds,
sysroots, required seeds and unrelated VMs are preserved. These storage checks
do not qualify secure RDMA behavior.

## Threat model

WKI v3's provisioned pairwise credentials, authenticated handshake and current
service policies remain the trust roots. Secure RDMA adds authority for each
data-plane operation; authenticating an rkey exchange alone is insufficient.

Adversaries include unauthenticated network senders, authenticated malicious
peers, stale or rebooted endpoints, and peers able to read and modify a complete
ivshmem BAR. They may replay, reorder, drop, duplicate or change records; forge
doorbells, completions, indices and region hints; race revocation and snapshot
copying; request unauthorized ranges/services; and retain old credentials,
capabilities, operation tickets or ciphertext. An authenticated malicious peer
knows its own session traffic keys, so successful AEAD verification is only the
first admission check. Private registration and consumer state must still
authorize the exact operation.

Registered backing, service indices, grants, keys, replay windows, waiters and
completion state stay in private kernel memory. Secure RDMA staging in the BAR
contains untrusted metadata and encrypted records. Receivers copy a bounded
record once, verify and decrypt that same private snapshot, then apply checked
effects. Shared doorbells are work hints and cannot authorize an effect.

Existing ordinary WKI message rings remain compatible. Their authenticated
payloads, including service data sent through message fallback, are plaintext
in shared memory. Secure RDMA does not add confidentiality to those messages;
it keeps registered backing private and encrypts its negotiated direct records.

Disclosure protection covers unregistered memory, out-of-grant ranges, other
services/peers, stale backing reuse, and private data carried through staging.
A peer legitimately permitted to read data can retain that data after revoke;
revocation cannot undo prior disclosure. Traffic sizes/timing and fixed public
record metadata remain visible. This work does not promise forward secrecy,
protection from a compromised local kernel, or isolation from a malicious host.
Host provisioning and ivshmem mappings remain within their existing trust
boundary. Availability against a peer that destroys shared ring indices or
withholds valid responses is bounded rejection/failure and message fallback,
not guaranteed progress.

## Compatibility and negotiation

Preserve WKI v3's packed 32-byte header, 172-byte HELLO and 40-byte auth trailer.
All sixteen HELLO capability bits are assigned. A new versioned authenticated
negotiation advertises secure transports and zone/VFS/IPC/block support
independently. The exact ordered offer transcript, record version and suite
are key-derivation inputs. Unknown versions, suites, reserved bits, changed
offers within an active incarnation and unsupported services fail closed.
Unnegotiated peers retain authenticated ordinary-message service paths. Old
raw RoCE records and direct BAR-backed service pointers are never enabled as
a downgrade. Raw IRQ mailboxes cannot authorize callbacks.

Secure IPC negotiation establishes the fixed receiver data channel before
publishing its offer. This lets an export grant arrive before TASK_SUBMIT imports
the pipe descriptors. Grant reception only looks up existing channels and keeps
their exact allocation generations; it cannot create a channel from a grant.

## Record and cryptographic boundary

One packed, explicitly versioned record covers source/destination, transport,
service, operation/direction, exact session, region capability/generation,
binding identity, armed receive ticket, range, full operation identity,
fragment position/total, monotonic record counter and ciphertext/tag. Parse
exact lengths and zero reserved fields before any memory effect. Registration
returns a 128-bit unpredictable capability and nonwrapping local generation;
the legacy u32 rkey is solely an authenticated lookup hint.

Use scalar [RFC 8439 ChaCha20-Poly1305](https://www.rfc-editor.org/rfc/rfc8439)
and existing [RFC 5869 HKDF-SHA256](https://www.rfc-editor.org/rfc/rfc5869).
Derive independent directional keys for each session/negotiation/transport/
service domain. A fixed domain prefix and reserved u64 record counter form an
injective 96-bit nonce. Counter reservation never rolls back on send failure,
and exhaustion closes admission instead of wrapping. Local capability
derivation uses private boot entropy, not a key known to the peer. Secret
state and private plaintext staging are erased on retirement/error.

Reordering is explicitly bounded. Current limits are eight admitted operations,
four MiB per operation, 8192-byte fragments and an 8192-record replay window.
Accept reorder within that window; reject duplicates, old counters, zero,
stale sessions and pressure without unbounded state. Fragment coverage is a
fixed bitmap with exact offsets and lengths; duplicate/overlapping fragments
cannot complete holes. Completion requires every expected byte and the exact
operation/receive ticket, rather than registry disappearance or a reused
16-bit service cookie. These limits are fixed by the versioned offer and its
exact validation; peers cannot expand them during negotiation.
The eight-operation limit is global, with at most 32 MiB of private transfer
scratch. Slot turnover or failed sends can move counters beyond the window;
the guarantee is counter-age reordering within 8192 records, with bounded
timeout/failure outside it, not guaranteed delivery of every live operation.

## Ownership, locks and consumer contracts

Short shared peer lifecycle admission protects immutable session material
during authentication/replay/checked effect. It never spans a network wait,
allocation or consumer callback. Exact region-operation holds protect backing
through deferred progress but do not authorize effects after retirement.
Release paths must not need peer admission, so fencing can close and drain
without waiting on a reader that needs network progress. Lock order is peer
admission, short session/replay guard, short region admission guard; release
before TX, waits or consumer locks. Real IRQ work only latches hints. Bounded
RX/drainer admission does not allocate, delete, block or invoke native I/O.
Private registrations retain the exact channel allocation generation in
addition to the wire channel number and opaque binding generation. Channel
retirement closes those grants independently of session retirement.

Revoke closes new admission and queued access first, cancels/terminalizes
waiters, then drains retained operations before erasure/free/reuse. A failed
drain retains storage rather than manufacturing quiescence. Session/transport
rotation invalidates all associated grants, replay state and receive tickets.
The worker remains active until the operation table is empty, including
terminal slots whose job handles have already retired. Each service pass keeps
the bounded one-slot scratch erase; operation-guard pressure prevents a
successful drain snapshot.
Consumer operation claims and retirement share a guarded linearization point:
queued/unclaimed work cancels; already claimed work retains its private backing
through an exact or uncertain terminal result, and retirement drains it. A
live check followed by an unguarded native effect is insufficient.

Message fallback is safe before secure admission or after a proven no-effect
rejection. An accepted write/append/block request that loses completion retains
its partial or uncertain outcome; it is never silently replayed through the
message path. Duplicate offers are idempotent; changed active offers require
retirement and drain before new keys/tickets can be installed.

- Zones use private backing and explicit synchronization on both transports;
  retained local memory is not a remotely writable shared pointer.
- VFS preserves credential checks, exact channel/lane ownership and serialized
  file operations. Receive tickets/full operation IDs replace reused low16
  cookies as data-plane authority; unregister precedes staging free.
- IPC admits append-only payload publication against private head/tail/credit
  state. Peers cannot overwrite unread/in-flight bytes, advance tail, forge
  geometry or publish a head without verified payload. Preserve EPIPE/EIO,
  poll credit, close-after-delivery and cancellation semantics.
- Block uses exact private native intents and local operation reservations;
  legacy shared SQ/CQ, bulk flags and raw rkeys remain disabled. It validates
  attach cookie, resource incarnation, binding generation, nonwrapping sequence,
  access policy and checked LBA/byte ranges before native I/O. Authenticated
  full native RESULT determines WRITE completion. Native errors fence uncertain
  writes without replay; FLUSH preserves existing native completion ordering.

## Qualification requirements

Primitive vectors/differential corpus, production record/parser/replay/property
tests and sanitizer/fuzz runs precede adapters. Each consumer requires separate
RoCE/ivshmem/fallback matrices covering tamper, wrong authority, replay/reorder,
stale incarnation, revoke/fence/reuse, forged completion/doorbell, pool pressure,
exact bytes/status, recovery and teardown counts. Run Normal `Build WOS`,
isolated KTEST and rootless live throughput/recovery fixtures. Diagnostics
report reason-coded drops and current lifetime counts without keys, tags,
capabilities or payloads. No capability becomes enabled until its matrix passes.

Client statistics separate final exact READ and WRITE completions and bytes
by service and transport. An IPC metadata QUERY or an ordinary-message payload
cannot qualify a secure payload WRITE. The relaxed counters are diagnostic
samples, not a transactional accounting interface. In the opt-in `wki.chaos`
profile, successful secure shutdown reports actual lifetime counts; an
incomplete snapshot reports `complete=0` and proves no zero-count claim.

Already owned records retain their FIFO position through temporary local guard
pressure. Successful authentication commits replay once; a retained decoded
record rechecks its exact binding before operation admission. The `PRESSURE`
counter includes deferred processing attempts, so it is not a count of unique
discarded records. Persistent contention at the FIFO head can delay other
records; control, maintenance and operation progress still run each worker pass.
The bounded ciphertext receive pool uses a separate queue gate, so ordinary
engine-table contention does not discard data before ownership. Enqueue captures
the published secure incarnation; it grants no effect authority. The worker
checks the fully confirmed context and exact session, provider and channel again
before authentication and admission. Queue capacity or queue-gate contention
can still discard an unowned direct datagram; no retransmission guarantee is added.
With both `wki.chaos` and `wki.secure_rdma`, the task worker also reports at most
32 expired jobs per boot, once per job. These metadata reports identify the
phase, ACK/effect/result state and errno without secrets or payloads.

An active ivshmem transport caps the existing task receive-poll interval at
1 ms in both plain and doorbell modes. The IRQ remains bounded and only masks
its vector; task polling drains records and re-arms it. Absent, closed, or failed
ivshmem preserves the original timer cadence. Qualification must measure the
additional timer CPU work as well as actual vector-scoped IRQ notifications.

The user authorizes owned `bin/wos-cluster --no-setup` and isolated
`bin/wos-ktest --no-setup` launches/debugging in this session. Preserve unrelated
VMs, shared launch exclusion and KTEST artifact isolation. Hardware claims need
hardware evidence; custom software L2 RoCE does not claim hardware RDMA.

## Evidence boundaries

The host fixtures compile the current crypto, protocol, session, registration,
operation and complete secure engine implementations under ASan and UBSan.
The complete-engine READ_END tests process independently encrypted END before
DATA, verify that four worker passes leave the operation incomplete and caller
output unchanged, and then require exact content after DATA. Withheld DATA must
time out without publishing partial output. A private semantic control that
completes on END without full coverage fails both transport cases while its
unchanged WRITE control passes.

Native ZONE, VFS, IPC and BLOCK fixtures complement live observations with
checked authority, at-most-once effects, no-effect fallback, uncertain-write
handling and retirement races. Their controlled endpoints and native I/O are
explicit test boundaries. They cannot manufacture an omitted guest diagnostic,
qualify a failed live case, or resolve an intermittent runtime hang. Actual
KTEST, live fault/recovery, IRQ, ordinary/mixed fallback and performance receipts
remain separate requirements.

Independent live consumer campaigns enable one secure service on both nodes
and use the existing three other `wki.secure_rdma_*_off` command-line tokens.
Their assessor requires the exact packaged commands, one directional key pair
and replay window, zero secure counters for disabled services, and the unchanged
native content/provider/fault/recovery/teardown checks. All-service campaigns
remain necessary to exercise combined operation; isolation alone does not
qualify concurrent consumers.

Performance qualification scopes counters to each native scenario. ZONE/VFS/
BLOCK use the initiating node's before/after snapshots; IPC uses separate
owner-local snapshots around the pipe workload. Global cumulative counters,
including executable loading or fixture setup, do not prove measured payload
used the secure path. Ordinary and partially falling-back results retain their
actual path status beside timings.

VFS supplies per-write/fsync nanosecond latency samples. Additional native
scenario wall stamps use `/proc/uptime` with 10 ms resolution, explicit timing
bounds and declared payload volume. ZONE timing includes create/destroy; BLOCK
includes attach, an optional warm READ, durability and restoration; IPC includes
remote process/pipe setup, producer delay and EOF. Such single-pair QEMU results
do not establish isolated encryption cost, hardware throughput or a statistical
speedup. Security qualification combines the independently checked models,
observable fault/recovery matrix and authenticated fallback evidence; it does
not turn a partial secure-throughput result into a passing measurement.

### Current measured campaign

The nine current-kernel measured pairs completed with actual nine-zero secure
retirement and WKI shutdown on both nodes, without forced stops. All six
ordinary and mixed-peer cases passed native ZONE, VFS, IPC and BLOCK content,
status and scoped zero-secure-data checks. Every doorbell pair also passed its
separate ordered connection/vector-scoped IRQ checks. The plain-ivshmem secure
pair passed all four native consumers with exact per-consumer directional
counters. The separate fault coverage and earlier hang limits are recorded below.

The Ethernet secure pair had partial VFS fallback. The doorbell secure pair
had partial BLOCK and VFS fallback. In both, concurrent owner shell xtrace
interleaved with IPC snapshot boundaries, so the scoped IPC assessment remains
failed even though native pipe content/status passed. A later quiet-owner fixture
completed both transports with valid IPC snapshots: each observed one 48-byte
secure QUERY and one exact 4096-byte secure WRITE. ZONE also passed. Those
follow-ups preserved BLOCK setup timeouts and partial VFS fallback (Ethernet
14 READs/16 WRITEs; doorbell 15 READs/16 WRITEs), so their overall results remain
FAIL. No incomplete snapshot is treated as zero.

Actual scenario elapsed seconds are shown below. BLOCK declares 16384 payload
bytes, ZONE 8192, VFS 65536 written plus readback, and IPC 4096. All intervals
have 0.01s clock resolution and conservative endpoint bounds of ±0.02s; IPC
includes the deliberate three-second producer delay. Candidate intervals with
partial fallback or invalid scoped snapshots do not qualify secure throughput.

| Transport/profile | BLOCK | ZONE | VFS | IPC | Scoped qualification |
| --- | ---: | ---: | ---: | ---: | --- |
| Ethernet ordinary | 0.08 | 0.05 | 0.07 | 3.56 | All four PASS |
| Ethernet candidate | 0.07 | 0.06 | 0.13 | 3.06 | BLOCK/ZONE PASS; VFS/IPC FAIL |
| Plain ivshmem ordinary | 0.07 | 0.06 | 0.15 | 3.07 | All four PASS |
| Plain ivshmem candidate | 0.14 | 0.06 | 0.25 | 3.13 | All four PASS |
| Doorbell ivshmem ordinary | 0.08 | 0.07 | 0.24 | 3.10 | All four PASS |
| Doorbell ivshmem candidate | 0.13 | 0.08 | 0.32 | 3.13 | ZONE PASS; BLOCK/VFS/IPC FAIL |

Whole-kernel connected-idle busy time was 1.90%/2.62% for Ethernet ordinary/
candidate, 5.24%/7.13% for plain ivshmem and 6.16%/7.84% for doorbell ivshmem.
These single-pair samples include polling, heartbeats and diagnostics and do
not isolate encryption cost. The source-bound matrix receipt is
`build/goal25-evidence/measured-consumer-matrix-39-01.json`; it verifies the
campaign kernel, all 50 frozen production source files, input hashes, native
checks, partial-path status, IRQ evidence and real retirement independently.

### Live fault coverage and recovery scope

The ordered live coverage receipt
`build/goal25-evidence/negative-coverage-ordered-draft-20.json` selects 60 of
60 observable fault cases: ZONE, VFS, IPC and BLOCK on software Ethernet/RoCE,
plain ivshmem and doorbell ivshmem, each with tag corruption, ciphertext
corruption, duplicate, END-before-DATA and lost RESULT. Every selected case
has no failed observable check, actual graceful nine-zero secure retirement
and WKI shutdown on both nodes. Each selected doorbell fault has separate
connection-order and vector-scoped IRQ evidence. The nine required BLOCK
fresh-owner companions have different nonzero boot IDs and later same-host
runtime receipts under serialized launch exclusion. That ordering is not a
cross-guest clock or cryptographic timestamp claim.

Three selected BLOCK faults used one CPU per node: Ethernet duplicate, plain
ivshmem lost RESULT, and doorbell END-before-DATA. The remaining selected faults
used two CPUs. Single-CPU success establishes that topology's observable scope;
it does not fix or qualify the failed multicore cases. Earlier failures remain
in the coverage receipt instead of being relabeled.

Live diagnostics intentionally omit tampered payload bytes, raw uncertain-write
outcomes, partial BLOCK native results and the internal processed-END barrier.
Those fields remain unsupported live observations. Separate production-engine,
native-consumer, chaos and KTEST fixtures establish the corresponding invariants:
the engine has 115 cases/7664 checks, native consumer fixtures have 313 cases,
and the isolated KTEST run has 73194 passing assertions, zero failed assertions
and 330 passing WKI cases. None converts an omitted field into a live receipt.

An earlier two-CPU plain-ivshmem VFS lost-RESULT run exceeded its host deadline
and required owned QMP shutdown; it remains unqualified. Later reproductions,
including the archived-v7 workload without the added watchdog, shut down
gracefully. The latter's assessor still failed because the launcher removed
its temporary debugger descriptor before input-pin verification. These runs
do not identify or prove a fix for the intermittent stall. Normal NTP retry
exhaustion explains the later absence of NTP messages; silence alone cannot
establish that the kernel clock stopped.

The corrected descriptor-retaining reproduction completed in 50.02 seconds
with no failed observable checks, valid immutable input pins, and actual
nine-zero/WKI shutdown on both nodes. It also did not stall, so no bounded
WOSDBG pause/backtrace capture was triggered. It establishes its actual fault,
recovery and closure observations, rather than a diagnosis of the earlier hang.

### Formatting and historical artifact continuity

Normal Build WOS18 passed after the five remaining formatting fixes. The final
host run rebuilt the affected primitives and passed all 29 selected tests.
The existing WKI v3 wire and authentication targets were also rebuilt, and all
four compatibility tests passed. The full 58-file formatting/tidy check passed.
Historical runtime and KTEST receipts retain their actual source/kernel hashes.
The explicit formatting bridge archives the old five files and kernel and
compares the rebuilt ELF entrypoint, every load segment's geometry and every
loadable byte. Executable code is identical. The only loadable differences are
the GNU build-ID descriptor and six checked `PerfCallsiteInfo.line` fields,
each pointing to the same source statement at its newly formatted line.

`build/goal25-evidence/format-bridge-41/continuity-result.json` records those
exact differences. This proves reviewed formatting continuity; it is neither
a new KTEST execution nor permission to repin or qualify old failed results.

The current strict assessors re-analyzed all 60 selected fault logs and nine
fresh-owner recoveries. Their native/security predicate ASTs are unchanged;
only exact historical-kernel resolution through the verified archive was added.
All observable checks and immutable input checks passed. This resolves obsolete
assessor-path pins by performing a new host review, without reconstructing old
assessor bytes or claiming new guest execution. The review receipt is
`build/goal25-evidence/historical-live-review-44-01-result.json`. Final source
continuity verifies 45 unchanged production files, five explicitly archived
formatted files, and 297 model inputs (five through exact source archives).

### Combined-consumer follow-up and rollout limits

Fresh current-kernel single-CPU pairs passed all four scoped consumers on plain
and doorbell ivshmem. Each VFS interval observed 16 exact READs and 16 WRITEs;
IPC observed one 48-byte QUERY and one 4096-byte WRITE. BLOCK observed two READs
and two WRITEs, exact contents, durability, restoration and detach. ZONE observed
one READ and one WRITE. Both nodes actually retired all secure references, and
doorbell connection/vector-scoped IRQ qualification passed separately.

The Ethernet single-CPU pair passed exact ZONE/VFS/IPC scopes but its BLOCK
restoration returned `EBUSY`; that FAIL is retained. One bounded fresh follow-up
passed BLOCK, ZONE and IPC, but its VFS READs used authenticated ordinary fallback,
so it also remains a strict combined FAIL. Individual secure consumer paths
are observed with all services opted in; these separate scopes do not establish
that every Ethernet transfer uses the secure path in a single combined run.

Transient guard pressure may permit a proven no-effect fallback. An ordinary
BLOCK error on a secure-compatible binding is conservatively uncertain and
prevents further writes on that binding. Earlier strict fallback/availability
failures and the intermittent unclassified hang remain recorded. The current
security proof does not promise progress against every timing or hostile peer,
enable default rollout, fix those availability observations, qualify physical
RDMA, prove power-loss durability or establish a performance advantage.

| Goal25 acceptance criterion | Evidence and scope |
| --- | --- |
| Threat model | Malicious/stale peers, BAR tamper, replay/reorder, doorbells, reuse, rotation, disclosure and downgrade are described above. |
| Versioned authenticated identity | Packed v1 record/offer tests, production engine and 60 live fault scopes; WKI v3 header/HELLO/trailer layouts preserved. |
| Key domains and erasure | Direction/session/transcript/transport/service derivation, nonwrapping reservations, private capability entropy and production-model/KTEST zeroization checks. |
| Bounded replay/reorder | Fixed 8192-counter window, exact fragment coverage, worker END-before-DATA barrier and deliberately failing semantic control. |
| Generation registration/revoke/fence | Unguessable capabilities, exact local channel/provider/incarnation bindings, guarded claims and drain-before-reuse model/native tests. |
| Equal transport security semantics | Common authenticated engine, typed software Ethernet/IV providers, 20 selected faults per mode, plus independent doorbell order/IRQ evidence. |
| Independent consumers and fallback | One-service live profiles and native models; all six ordinary/mixed four-consumer pairs PASS. Safe fallback and strict partial secure-path measurements remain distinct. |
| Zero lifetime references on teardown | All selected faults and required recoveries have actual both-node nine-zero/WKI closure; engine/native tests enforce retained loans, cancellation, erasure and exact release. |
| Redacted diagnostics | Reason-coded drops and exact lifetime/directional counts, chaos redaction models and KTEST; omitted raw fields retain their separate model complements. |
