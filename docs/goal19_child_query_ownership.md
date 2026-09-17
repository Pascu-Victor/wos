# Goal19 9NN: retained child-query authority

Previous turn: progress; 9NM qualified the relay with 60028/0 live diagnostic
assertions. Goal remains 17 enforced / 4 pending, first signals.

## Reconnaissance and patch contract

Problem: the relay has no retained process/session storage. A transport must not
borrow RX/syscall stacks or reuse a canceled query while an upstream record lives.
Expected files: remote_compute.cpp/.hpp, child_query.hpp, WKI KTEST and worker
source guard, this document and goal progress. Files read: those sources plus
wki.hpp, wire.hpp, wki.cpp, child_events.hpp/.cpp, waitpid.cpp, session.hpp and
kernel CMake; workflow, kernel, WKI/cache, critical, build/logging/test instructions.

Flow: authenticated deferred request -> exact accepted source-owner acquisition
-> fixed record and immutable arguments -> local lifecycle snapshot -> existing
relay -> reserved upstream value action -> exact session/action completion.
Incoming CLOSE, owner exit and session retirement -> retained close continuation
-> upstream acknowledgment or terminal session -> deferred owner release.

Invariants: only the accepted exec's canonical Task owns the queried queues.
One owner pin per live record. Selector/options/origins and incoming/outgoing keys
cannot change on replay. Physical group origins localize only at the owning node.
No claim, reaping, credential change or CPU accounting occurs here. Slots and
outgoing identities remain stable through cleanup; tombstones outlive records.
Sequence allocation saturates rather than wrapping. All delayed work is value-only
and checks slot identity, full session and operation generation before use.

Locks: s_compute_lock protects records/session bindings; busy reserves a record
while lifecycle inspection runs outside that lock. Existing single peer lifecycle
admission may cover bounded local inspection, never a second peer admission or a
network wait. Task releases and retirement run outside all peer/compute locks.
Unsafe contexts: no raw NAPI/channel-lock caller, allocation, blocking, logging or
send added. Fixed storage and bounded scans only.

ABI/wire/syscall impact: none yet. No advertised resource, packet dispatch or
waitpid caller is enabled. Claim/commit/ACK, initiator/native-waiter ownership,
notification and imported CPU accounting still need integration before activation.

Build/test plan: Normal Build WOS, isolated diagnostic build/live KTEST, scoped
format/source/policy checks. Actual private Tasks, submitted/running rows, sessions
and child queues exercise ACCEPT deferral, immutable replay, validation, cleanup,
late action/session replies and slot reuse; transport delivery stays synthetic.
No user runtime input is needed; --no-setup live runs are explicitly authorized.
Rollback: exact pre-stage files under build/goal19-child-query-ownership/before.

## Implemented ownership contract

64 fixed records each reserve their own upstream lane. A global increasing token
never wraps; per-session incoming lane watermarks survive record reclamation and
reset only with a new authenticated compute session. Full immutable selector,
options, caller/group origins, task identity and depth bind every incoming replay.
The exact source owner is acquired after ACCEPT and rechecked before observations.
Private value-only work handles bind slot/token, upstream task/receiver, session
identity and relay action generation. No Task or event pointer escapes in work.

Lifecycle snapshots run with the record busy and compute lock dropped. Exit and
session retirement set flags while busy; the executor applies them before publishing
its result. Canceled records retain their slot and owner until upstream CLOSE is
acknowledged or that exact session is terminal. A deferred worker releases the
record's pin outside peer/compute locks. Short admission-reference drops are only
atomic Task::release operations (task.hpp); they do not run a destructor or block.

The new KTEST uses real private Tasks, accepted submitted/running rows, peer leases,
channel/session records and child queues. It checks ACCEPT deferral, immutable
replay, stale-session refusal, actual claim/release version invalidation, physical
and alias group localization, cancellation while busy, sequence exhaustion, reserved
send retry, CLOSE overtaking INSPECT, cleanup retry, slot reuse and both directions
of session retirement. All network action delivery is simulated; no wire resource
or syscall behavior is enabled. Existing claim/accounting state stays untouched.

## Accounting integration finding

Current merge_proxy_resource_completion adds completion reaped counters to the
source owner. check_completions exports the receiver's whole child counters as
segment deltas, relying on a zero starting value. resource_contract_model.hpp
explicitly rejects nonzero reaped counters during receiver install and in submission
baselines. Therefore imported wait observations cannot simply be added to existing
receiver child counters or copied into the current submission resource snapshot:
that would either violate admission or count them again at source completion.
A separate observation/delta representation and explicitly compatible exec transfer
are still required before enabling distributed wait. CPU observations and immutable
exit-event snapshots also read the existing counters and must be assessed together.
No accounting behavior changes in9NN. Relevant current source hashes are recorded
in build/goal19-child-query-ownership/integration-source-hashes.json.

Measured normal-kernel record storage is 37,888 bytes; query retirement watermarks
add 131,072 bytes across 256 peer-session entries. No dynamic query allocation.

## Remaining adapter activation work

The transport driver must fairly schedule outgoing slots with bounded retry times;
the current private take-work helper scans from zero and must not be used in an
unbounded immediate retry loop. PENDING responses must retry the same operation,
not be converted into a successful empty child summary. A versioned wire decoder
must bind each reply to its reserved full session and immutable work identity.

Before exposure to waitpid, connect final conditional BEGIN/COMMIT/RELEASE/ACK to
query ownership, preserve lane retirement across both query and claim cleanup,
and retain the initiating process plus actual native waiter through fatal exit.
The blocking frontend needs readiness notifications or bounded retry independent
of SIGCHLD, and the separate imported CPU accounting described above. Qualification
must rerun the unchanged four failing migrated-parent cases on real nodes, alongside
local controls, collisions, native waiters, cleanup and peer loss. This stage does
not satisfy those end-to-end requirements.

## First qualification failure and correction

The first full run reported 60028 passed / 1 failed, solely in the new ownership
fixture. Matching-ELF live GDB showed my_node_id=0, local_boot_epoch=1 and
initialized=false. InitExecutor orders scheduler setup (where KTEST runs) before
WKI initialization; the new adapter correctly rejects a nonphysical local identity.
The fixture now scopes an explicit node/boot identity and restores the old values
on return. Production admission stays unchanged. Failed-run logs, image/source
hashes and GDB evidence remain under build/goal19-child-query-ownership/attempt1.
VM3166500 was explicitly stopped after the completed failed suite; no matching WOS
VM remained at2026-09-16T20:35:33.729285+00:00. A fresh run is required.

## Final qualification

Normal Build WOS, isolated diagnostic build, scoped formatting, WKI/waitpid
source suites and policy checks pass. Corrected freshly packaged live KTEST
passes60029/0, including the new retained-authority test and existing relay/claim
transaction tests. Source and packaged image hashes match. Diagnostic VM3227666
powered off; no matching WOS VMs remained at2026-09-16T20:48:19.320440+00:00. The first
run's sole failure was a pre-WKI-init fixture identity; matching-ELF live debugging
confirmed it, the scoped fixture correction leaves the normal kernel hash unchanged,
and failed evidence is preserved under build/goal19-child-query-ownership/attempt1.
No normal-cluster, real packet delivery or peer-failure protocol tests ran in this
stage. Four migrated-parent wait failures remain unresolved. Production activation
still requires fair/timed sends, versioned decoding, query/claim cleanup integration,
initiator/native-waiter lifetime, blocking wake handling and imported CPU accounting.
Goal remains17 enforced/4pending, first signals. Evidence:
build/goal19-child-query-ownership/validation.json.
