# Durable remote job design

Goal28 is incomplete and checkpointed while work switches to Goal39 at the
user's request. Private value layers and effect-free wire codecs define bounded
identity, admission, lifecycle and output-credit rules. No job service is
advertised and no job syscall, dispatcher or scheduler path is enabled. The
existing one-shot remote-compute service keeps its current semantics. Native
integration, stream transport, public controls and runtime qualification remain
required before Goal28 can be completed.

A job belongs to the provisioned authenticated submitter node and the effective
UID from its authorized mapped credential snapshot. Status, attach, cancel and
signal must authenticate the current request and map its credentials again.
Knowing an ID or inheriting a numeric PID grants no authority. A channel/auth
session identifies one request, not the retained job. A new authenticated session
may control only that same stable node/principal scope.

The full key contains that owner scope, a nonzero submitter incarnation, a
monotonic nonzero job serial, a nonzero receiver incarnation and an immutable
nonzero attempt. Receiver-incarnation mismatch reports `LOST`; it cannot admit
or automatically resubmit the old job. A new execution requires an explicit new
identity. A repeated serial cannot change its attempt or command digest.
Incarnation generation and authenticated native service admission are subsequent
integration work. The private codec translates wire keys field by field; neither
the values nor successfully decoded bytes authenticate input by themselves.

The current receiver provisioning table is immutable during normal operation:
one provisioned credential identifies each node until shutdown/restart. Session
rotation derives new ephemeral keys from that provisioned credential. The
future job adapter must check that binding as well as current compute/identity
permissions. Any future live credential replacement needs an explicit retained
job-authority policy; numeric node reuse alone must not transfer ownership.

The receiver reserves a fixed record and advances its scope watermark before
acknowledging admission. Exact repeats replay the current record; changed bytes
or attempt fail. Once heavy result storage expires, a serial at or below the
retained watermark is explicitly expired and cannot execute. A previously unseen
lower serial can also be refused after a higher serial is admitted; bounded
out-of-order refusal grants no execution and must be reported explicitly.
Compact scope watermarks remain for the receiver's lifetime. Exhausting their
bounded table denies new scopes instead of evicting replay protection. Quota
refusal before reservation consumes neither serial nor scope storage.

| State | Legal progress and linearization |
|---|---|
| `PROPOSED` | Record/digest/dedup reservation; accept, cancel or expire; no native task |
| `ACCEPTED` | Acceptance is observable and retained; commit, cancel or expire; no execution |
| `COMMITTED` | One commit schedules work; one worker may claim `STARTING`; cancellation can win first |
| `STARTING` | One worker owns construction; publication can be claimed once; cancellation requests abort/termination |
| `RUNNING` | Checked native publication succeeded; completion competes with cancellation |
| `TERMINAL` | Native completion or launch failure is retained; duplicate controls have no new effect |
| `CANCELED` | Pre-start cancellation or actual completion after cancellation won; never executable again |
| `EXPIRED` | Proposal/acceptance/result storage is retired; retained watermark rejects replay |
| `LOST` | Authenticated receiver incarnation changed; observer outcome, not a rerun instruction |
| `UNKNOWN` | No retained/current knowledge; never evidence that an uncertain accepted execution did not occur |

Publication has a separate single claim. Before that claim cancellation prevents
publication. After it, a native post may win the race; the adapter must retain
the exact task and deliver cancellation after publication if necessary. It must
report checked publication failure and retain a terminal launch outcome instead
of trying to execute again. A fast task's exit notification must remain owned
until publication bookkeeping makes completion consumable. Completion that wins
the registry lock first remains terminal; cancellation that wins first is
reported canceled only after actual native cleanup/completion.

Recording cancellation and claiming its native effect are separate transitions.
A full deferred queue must leave the request pending before the effect claim.
The adapter may claim only after retaining the exact published Task and work
ownership. That claim is once-only across disconnect/reattach. Construction
before publication needs no native signal; completion can win before a pending
effect is claimed. The value layer does not perform native signal delivery.

Disconnect and reattach change attachment state without changing lifecycle.
They cannot turn a committed or terminal job back into a proposal. Registry
handles include nonreused generations, so late callbacks cannot mutate a reused
slot. Reclamation requires terminal/pre-execution expiry eligibility and zero
metadata holders. The future adapter must retire its Task/File/channel ownership
before dropping those holders; value-only tests cannot prove those native
lifetimes. Active work cannot be discarded to meet a result quota.

The stream values in `net/wki/job_stream.hpp` use independent fixed stdout/stderr
rings and nonwrapping absolute byte offsets. Append returns the accepted prefix
and backpressure when storage fills. The initial policy permits no producer
truncation. Disconnect may stop a lossless writer until an authorized consumer
reattaches or cancellation closes it. The native File adapter still needs to
implement partial writes, nonblocking errors, interruptible parking outside
locks and wakeup on credits/cancel/completion.

Credits specify a consumed offset, absolute window and monotonic revision under
one attachment token. Duplicate grants cannot add credits twice; old revisions
cannot reopen a spent window. Only application-consumed offsets reclaim sender
storage. On the receiver, reading creates space without implicitly advertising
more credits. A smaller grant preserves the earlier grant's high-water mark for
already authorized packets in flight, while the physical ring remains bounded.
Frames must contain the next offset or an entirely accepted duplicate; gaps,
overlaps, conflicting retained bytes and capacity violations change nothing.

Preparation copies one packet into private caller-owned storage and claims a
nonreused frame generation before sending. The caller must already own that
buffer and a job holder. TX refusal leaves both the copy and reservation owned
for retry; finishing is allowed only after reliable TX takes responsibility.
Application ACK can then recycle the ring without changing the private packet.
Detach retires the old reservation. Reattach uses a new token and replays from
the consumed offset, fencing old callbacks and credits. A requested offset below
the retained prefix produces an explicit `GAP`; an offset beyond prepared data
is rejected. EOF names the final offset and needs no payload credits.

Both stream attachments require preflight under the job lock before either is
changed. Current authenticated peer/credential and exact request-channel checks
remain mandatory. These values cannot authenticate an attachment themselves.
The native completion adapter must close both producers and publish their final
accepted offsets together with the root task's terminal result. Inherited File
writers after that boundary must fail explicitly; they cannot report discarded
bytes as successful writes or keep expired heavy result storage alive. The
Task/File ownership and wait/wakeup implementation for that boundary is still
outstanding. Existing combined 4096-byte capture and generation-scoped IPC do
not satisfy these durable stream semantics.

The registry requires explicit positive proposal/result TTLs and a byte limit.
Admission charges the caller's bounded maximum payload/stream cost before native
allocation, in addition to fixed record/scope count limits. Replays add no charge.
The conservative charge remains until eligible zero-holder reclamation; active
committed/starting/running work cannot be expired to admit another job. Manual
retirement requires explicit result/proposal policy, while timer retirement uses
the original creation time or first stamped terminal time. Reattach cannot renew
either. Missing terminal stamps and backward clock observations retain safely.
Elapsed-time comparisons avoid deadline overflow. Reclamation frees the charge
while leaving compact dedup watermarks intact. The native adapter still must
choose realistic limits/costs, stamp transitions and drain exact ownership.

`net/wki/job_control.hpp` supplies count-bounded signal receipts per exact job.
Each immutable serial/signum has one claim and one checked result; retries replay
proposed, claimed or completed evidence without another effect. Claimed receipts
cannot be reclaimed. Retirement of completed receipts keeps a compact watermark,
so an expired signal serial cannot deliver again. Callbacks carry a typed signal
handle and the enclosing job handle; both generations must match. The adapter
must retain the exact native Task before claiming and deliver outside locks.
Whole-job cancellation is independent of optional signal-receipt capacity.

`net/wki/job_protocol.hpp/cpp` supplies an unadvertised version-one schema and
bounded, allocation-free codecs. Request, response, output and credit headers
have exact lengths, explicit integer fields, canonical 80-byte credentials,
zero reserved fields and separate request/control/attachment identities. Decodes
and failed encodes leave outputs unchanged; payload spans borrow their input
owner. Encoders support overlapping payload buffers. EOF and stream credits use
checked absolute offsets, and replies must match the complete job key, service,
operation, correlation and signal serial. Receiver-restart replies carry a new
incarnation with `LOST` and no invented task/output evidence.

The owner mapper uses the production compute identity policy and checks the
current authenticated node and mapped effective UID. Root and active service
privileges require separately provisioned root identity trust. The mapper does
not verify MACs, channel generations, live peer admission or negotiated service
grants. Those checks must precede native effects. SUBMIT framing exposes a bounded
opaque plan; its process, filesystem, argv, resource and authority semantics must
be validated before acceptance. Framing success grants no execution.

All existing HELLO capability bits are allocated. Future job discovery uses the
named, versioned COMPUTE resource `process-durable-jobs-v1` and explicit INFO
negotiation bound to the authenticated peer and exact RESOURCE channel. No new
MsgType or resource advertisement is installed by these codecs. Native service
negotiation/admission, retention/controls, stream transport and the public CLI
remain to be implemented.

The lifecycle implementation is `net/wki/job_state.hpp`. Its host test target,
`wki_job_state_test`, exercises actual production values under ASan/UBSan,
including lost acceptance/commit replies, disconnect/completion/reattach,
duplicate identity/digest/attempt rejection, quota refusal, replay after expiry,
late generation callbacks and publication/cancellation races. The state-space
walk covers all reachable combinations of lifecycle and attachment flags; it
does not simulate native task construction or authenticate a network frame.
Normal Build WOS and a separate instantiation using its current freestanding
compiler flags passed. No new VM run is required for these unconnected value
layers; native protocol and lifetime qualification remain outstanding.

The stream host target, `wki_job_stream_test`, has 12 cases. Together with the
lifecycle cases it passes under ASan/UBSan. Sixteen deterministic two-stream
traces compare 323,200 unique delivered bytes against independent input vectors,
including send refusal/retry, lost/duplicate/reordered credits, rejected stream
gaps and attachment replay. Additional cases cover full buffers, EOF, stale
callbacks, early ACK ownership and numeric exhaustion. Freestanding stream
instantiation, repo format check and Normal Build WOS pass. These are value-model
checks; actual WKI packets, native blocking and task/FD cleanup are unqualified.

The current foundation suite has 41 ASan/UBSan cases: 22 lifecycle/retention,
12 streaming and seven signal-receipt tests. Retention cases exercise byte quota
refusal, original TTLs, missing/time-zero terminal stamps, active/pinned expiry
refusal, clock/unsigned boundaries, stale timer handles and 2,500 reclaimed
accepted completions. Signal cases exercise immutable control identities, lost
claim/result replies, generation/capacity refusal and concurrent once-only claims
with balanced outer job holders. The effect counters are model witnesses, not
native signal-delivery evidence. Freestanding instantiation, formatting and
Normal Build WOS pass for this stage; native adapters remain outstanding.

The codec target adds 19 production-source ASan/UBSan cases: golden byte layouts,
all truncated lengths, operation/credential/reserved/range errors, incarnation
and correlation binding, root/service trust, unchanged output on rejection,
overlapping encoder payloads and 5,000 deterministic mutated/random inputs.
All 100 focused cases (60 job foundations/codecs and 40 existing authentication
policy cases), the repository format check and Normal Build WOS passed for the
wire checkpoint. The normal kernel compiles and links the codec. This is host
framing/mapping evidence, not native process execution or network qualification;
no Goal28 VM images were created. LeakSanitizer was disabled by the host harness.

Owned tests may use `bin/wos-cluster --no-setup` and isolated
`bin/wos-ktest --no-setup`. Keep one active topology, private COW/reflink images,
a 20GiB disposable budget and 40GiB free-space floor. Audit exact ownership and
live references and prune disposable images after successful and failed tests;
retain small evidence, current artifacts and user/unrelated disks.
