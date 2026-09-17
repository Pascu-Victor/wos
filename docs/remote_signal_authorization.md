# Remote signal authorization

## 9FN witnessed group visibility gap after COMMIT

The owned-child handoff fixture now accepts `--group`. Each child establishes its own process group before exec, and the parent tests signal zero and SIGUSR2 against that group. The receiver reports its current PGID, host and real/effective/saved UIDs. The parent stays outside the child group, and retirement checks include a peer-local `/proc/$$` positive control.

The existing diagnostic publication hold now accepts `compute-publish hold=1 stage=commit`. It pauses the owning receiver submit daemon after a validated COMMIT and before scheduler publication. The hook holds no compute lock or peer lifecycle lease; cancellation, session and deadline validation still follow its release. A single atomic stage publishes hold and phase together, configuration rejects a stage change during an active hold, and the existing release generation/event wake bounds waiting. The default `compute-publish hold=1` retains its preconstruction meaning; `hold=0` releases either stage. The procfs snapshot adds `compute_stage` (0 inactive, 1 prepare, 2 commit). Both existing boot gates are required; no live packet-fault rules were enabled.

Actual qualification confirms the previously source-derived gap. All four post-COMMIT cases (root/UID 100, main/worker exec) return ESRCH for both group discovery and SIGUSR2 delivery. After hold release, the replacement runs with the expected original group, host and credentials but pending=0/delivered=0. These are false absence results, not successfully admitted signals later lost. Each hold was released 33–35 ms after its witness, well inside the operation deadline. All sixteen group controls before exec, before preparation and after acceptance pass. All sixteen positive-PID controls also pass.

Normal and isolated builds, fixture compilation, formatting, source checks and the authorization audit pass. The isolated kernel passes 52,190 tests with zero failures, including the expanded publication-control parser. An initial KTEST command had an invalid extra argument and never launched; the first corrected launch failed in supermin during disk packaging. Packaging succeeded with workspace-backed temporary storage. All 36 fixture children were reaped, all 28 remote child receiver PIDs checked absent, both controllers exited and all helper/control files removed. Both guests were synced and stopped; host process checks confirm both cluster VMs, launcher and isolated KTEST VM absent. Evidence: `build/goal19-group-exec-handoff/validation.txt`.

The group protocol remains unfinished. Publishing a second enumerable representation alone cannot prevent a traversal from visiting the destination before movement and the source after it. Simply forwarding through source aliases can duplicate delivery or use stale group membership/credentials. The current fanout allocates a different request ID per peer and has no migration-wide receipt. Receiver workers also hold a peer lifecycle lease while applying group operations, which a forwarding design must account for before adding remote waits. The next implementation must jointly handle discovery, current receiver authorization, migration ownership and duplicate suppression. See `build/goal19-group-exec-handoff/group-protocol-reconnaissance.txt`. Full Goal 19 remains active at eight enforced and thirteen pending domains, including the remaining handoff timing and other authority requirements.

## 9FM prepared receiver and source signal cutover

Process-owned remote exec now uses a versioned READY/COMMIT exchange. The receiver validates and constructs its image, arguments, credentials, descriptors and completion ownership before reporting READY, but remains unpublished. The source closes local signal admission, drains already admitted effects, captures the final actor/owner signal state and retires old contributors before sending COMMIT. The receiver validates the exact request, PID, channel generation and session, installs the committed signal values and only then publishes the task. Duplicate COMMIT cannot overwrite the first snapshot. Premature ACCEPT/COMPLETE cannot bypass preparation, and ACCEPT must match the prepared PID.

Local signal admission now remains owned through its effect. Its registration participates in ordinary and fatal syscall cleanup so an abandoned caller cannot strand the drain count. Signals routed after cutover wait outside locks for receiver admission and retain their original caller credential snapshot. Source cutover is irreversible: a subsequent failure terminates the logical process rather than returning to the original image. Before cutover, rejection and optional local fallback retain their prior rollback behavior. The receiver releases its peer lifecycle lease while waiting; peer cleanup marks an unpublished row discarded and leaves destruction to its owning submit worker.

The new named COMPUTE advertisement gates the submit flag and new wire records. Existing syscall, HELLO, submit, response and user-signal layouts remain unchanged. Strict process exec rejects unsupported peers; optional placement can remain local. READY and COMMIT retry transient send failures for a bounded interval. If a validated response wins a send-failure race, its completed waiter result remains authoritative.

Final Normal and isolated builds pass. Final KTEST passes 52,190 tests with zero failures, including actual admission/fatal-cleanup, READY/COMMIT identity, duplicate handling and shared bounded-retry tests. Four rebuilt host targets and focused source, policy and formatting checks pass. The previously failing handoff matrix now passes all sixteen cases, including all four witnessed pre-publication signal arrivals. All 72 exec cases pass across main/worker and retired-owner replacement, fallback, rejection, missing-interpreter rejection and pipes. Ordinary/handler signal state passes eight, group membership six, foreign discovery one and signal permissions twelve. Another 32 short remote execs preserve exit status 23; their receiver PIDs are checked absent. Final logs show two COMMIT sends retrying WKI_ERR_NO_ROUTE and succeeding on attempt two.

The first live run passed all handoff and exec behavior cases but a pipe retirement query returned 255; its receiver later retired unpublished. The original send error was not captured, so its precise cause is not claimed. That run exposed the one-shot COMMIT weakness. Its kernels, logs and overlays are preserved. All remaining receiver checks and helper cleanup were completed, and both the first and final guests were synced and stopped. Evidence: `build/goal19-exec-signal-cutover/validation.txt`.

The full goal remains active at eight enforced and thirteen pending domains. This fixes the witnessed positive-PID post-capture loss; it does not qualify global group migration. Source alias exclusion still creates an interval before prepared-receiver publication that group discovery/delivery must handle, with operation deduplication across movement. Further STOP/CONT, peer/fatal timing and mixed-version runtime qualification remain. The diagnostic fault model does not yet classify the two new message types; no packet-fault rules were enabled or qualified here. Other authority domains retain their full completion requirements. All live tests were ordinary functional OS tests.

## 9FL request signal version lifetime and witnessed handoff loss

`SubmittedTask` now transfers `signal_version` in both move operations. Actual slot reclamation assigns a fresh value before reusing the same storage, so the next request starts unnegotiated instead of retaining the prior occupant's version. Matching ACCEPT already writes its negotiated version; this change fixes the stored request lifetime and does not claim that an ordinary matching ACCEPT previously granted incorrect authority. No wire/syscall layout, lock, allocation or task-reference ownership changes.

An actual kernel selftest covers move construction/assignment with zero, supported and unknown versions, then publication, reclamation and reuse of the exact same slot. Its initial version incorrectly called a current-task API during boot-time KTEST; that call necessarily returned ESRCH before the version gate. The corrected test checks the actual stored state, while live functional tests exercise caller-owned signal RPCs. The failed diagnostic run and kernel are preserved separately. Final Normal and isolated builds pass; the final KTEST run passes 52,189 tests with zero failures. Four host targets, focused source/policy checks, fixture compilation and formatting pass.

The new `service_remote_exec_signal_handoff` fixture gives a separate, deterministic failing regression. A parent signals only its own child as root or UID 100, with main-thread or worker exec. All eight pre-exec controls and four post-acceptance controls preserve pending/delivered SIGUSR2. All four cases held at the receiver's witnessed publication gate return successful signal admission but lose the signal: replacement identity and exec succeed, pending=0, delivered=0. Hold release follows the witness within 12–23 ms, well inside the existing VFS_REF deadlines. Every source child is reaped; all twelve remote receiver PIDs are individually checked absent; the controller releases its hold and exits; helper/control files are removed.

Existing ordinary/handler signal state passes eight cases, signal permissions twelve and canonical remote-exec ownership/status eight. Both guests were synced and stopped; both host VM processes are absent. Evidence: `build/goal19-signal-admission-handoff/validation.txt`. The signal handoff remains unresolved: receiver preparation, draining admitted source effects, irreversible ownership cutover, ordered pending transfer, peer/fatal failure semantics and group migration deduplication require a complete protocol. A final pending-bit copy after acceptance would still allow lost or duplicated delivery. Full Goal 19 remains active at eight enforced and thirteen pending domains. These are functional OS tests, with no penetration testing.

## 9FK canonical remote exec ownership

Remote exec now runs through a durable request owned by the canonical process. The request copies argument bytes and ELF metadata, retains the calling actor, and owns rollback of temporary source metadata. Main-thread exec uses that owner directly; worker exec transfers an independent request through the existing owner handoff queue, including when the main thread has retired.

The owner revalidates credentials and quiesces old contributors before descriptor export. The request carries the actor's signal state and the owner's pending bits. Acceptance acknowledges the worker before sibling retirement, then the canonical process owns proxy identity, completion and the parent-visible exit status. Cleanup finishes before publishing scheduler proxy state. Successful worker handoff abandons the old owner continuation through a terminal assembly park. Missing-target fallback resets the handoff waiter before native exec; rejection releases admission and resumes the original image and threads.

Receiver-side missing-interpreter rejection exposed a nested-return FPU provenance failure. The validated user-return gate now retains its exact task authorization on the continuation stack and rearms it after blocking work. It clears obsolete same-CPU suppression without relabeling a parked kernel frame or weakening restore validation. Noreturn exit and exec retain their existing abandonment cleanup. The failed kernel, VM overlays, WOSDBG register evidence and per-CPU backtraces are preserved.

Normal and isolated builds pass. The final isolated run passes 52,188 tests with zero failures. Five host targets and focused source/policy/format checks pass. All 72 live exec cases pass, including accepted replacement, missing-target fallback/rejection, receiver-side missing-interpreter rejection, retired-main workers and inherited pipes. Ordinary/handler signal state passes eight cases, groups six, foreign discovery one, signal permissions twelve, session policy forty and the credential suite all 66 top-level checks. Final helpers are removed, source children reaped, accepted receivers individually checked absent and the eight rejected receiver processes checked absent. Both final guests were synced and stopped. The initial halted VM could not sync; its exact helpers remain only in preserved failed overlays, and its host processes are gone. Evidence: `build/goal19-canonical-remote-exec/validation.txt`. This supersedes the outstanding old-thread and parent-status defects in the historical 9FI/9FJ sections below. The full goal remains pending. Pre-ACCEPT ownership, signal arrivals after capture, group migration deduplication, fatal/peer timing, mixed versions and other authority domains remain in scope. No syscall or wire layout change is introduced by 9FK.

## 9FJ remote worker exec loses the replacement exit status

The thread-ownership fixture now accepts `--exit-status` and records the value
received by the replacement. With status 23, all four local cases report the
correct parent wait status 5888. Both remote main-thread cases also preserve
23, although their old actors still run. Both remote worker cases return zero
to the original parent despite the replacement receiving 23 and acknowledging
completion. All four remote cases still reproduce old-actor survival.

This rules out attaching the committed proxy/result to the calling worker.
The canonical process must own the remote completion and its child event.
The native owner handoff currently consumes only prepared native images and
never returns on success; its service also runs inside main-thread retirement.
A remote success must therefore park the owner terminally after cleanup, rather
than resume an old retirement continuation. Durable input and rollback ownership
must survive worker acknowledgement and cancellation without borrowed pointers.

The eight children were reaped, receiver PIDs 90–93 individually verified gone,
and all control files/helpers removed. Both test guests were synced and stopped.
Fixture compilation, formatting, Python parsing and the policy audit pass.
Production code and kernel hashes remain unchanged from 9FH; no new Normal
Build WOS or KTEST run is claimed. Evidence and the implementation contract are
in `build/goal19-remote-exec-parent-status/`. These are confirmed outstanding
defects, not a completed remote exec implementation.

## 9FI remote exec still leaves old threads running

The native thread-ownership fixture proves a prerequisite for the remaining
signal handoff: direct remote exec does not retire the old process's other
threads. All four local controls pass. For both main and worker exec, as root
and UID 100, the receiver reports the replacement running on `wos-3`, then an
old actor executes on `wos-0` after the parent explicitly releases it. The old
actor retains its source credentials. All eight source children were reaped,
the four receiver processes were verified gone through peer-local proc checks,
and control directories and the helper were removed.

The final fixture uses file markers independent of inherited descriptors. Its
first pipe-based version could not prove old-thread retirement because exec
transfers those pipe endpoints too. It also exposed missing worker descriptor
inheritance: user-thread Tasks deliberately keep an empty private FD table,
whereas the current remote exporter reads the supplied Task's table directly.
Those first observations are retained but do not qualify thread retirement.

The direct remote attempt precedes native canonical-owner handoff and sibling
retirement. A complete fix must bring remote exec under owned preparation,
canonical descriptors and process identity, worker quiescence and rejection
rollback. Moving the scheduler proxy flag early or correcting FD lookup alone
would leave the process transition incomplete. The independent accepted route
from 9FH remains valid for its Task, but does not retire or transfer the other
old actors' authority.

No production code or kernel artifacts changed in 9FI. Fixture compilation,
formatting, Python parsing and the policy audit pass. Normal Build WOS and KTEST
were not rerun; both kernel hashes and the prior scoped production sources match
verified 9FH. Both test guests were synced and stopped. Evidence and the next
implementation requirements: `build/goal19-remote-exec-thread-owner/`.

## 9FH accepted signal routing before scheduler readiness

A Task now keeps an accepted-exec signal route separately from its scheduler
proxy identity. A matching ACCEPT that claims the outstanding waiter publishes
the route and remote PID before waking the executor. Early completion that
implicitly accepts exec publishes the route too. Wrong-session responses,
rejection and responses that lose the waiter claim do not publish it.

Local signal admission captures the route together with current credentials and
job identity under the Task path-metadata lock, inside existing VM publication.
An accepted source representative cannot authorize a local signal or be selected
as a resident group member. Positive signals use the retained submitted-task row
and existing mapped receiver RPC. If acceptance appears between the initial route
lookup and local admission, the caller retries receiver routing once. Unsupported
or retired routes return their error without an old-frame or lifecycle-cancel fallback.

The route stays set until Task destruction. Row reclamation and scheduler-proxy
clearing therefore cannot revive the old local image's signal authority. Forked
resident children start with no accepted route. Scheduler proxy readiness and
cleanup ordering are unchanged; ACCEPT processing takes no VM mutex, and no new
network wait or allocation is introduced. The syscall and wire layouts stay unchanged.

The kernel test invokes actual ACCEPT, early-completion, rejection and timeout
handlers with retained submitted rows. It checks route publication, stale response
rejection, unchanged scheduler readiness, current admission values and refusal to
fall back after row retirement. Process tests also exercise accepted-source group
exclusion and the local-to-remote retry result. Runtime evidence belongs in
`build/goal19-accepted-signal-route/validation.txt`. Normal and isolated builds
pass. The full isolated run passes 52,185 tests with zero failures, including the
new actual-handler test. Native ordinary and handler exec inheritance pass all
eight cases, group membership all six, foreign group discovery one, signal
permissions twelve, session policy forty and native credentials all sixty-six
test groups. Helpers are removed, both guests synced and all task VMs stopped.

This closes routing after source acceptance. It does not qualify the earlier
pre-ACCEPT interval, arrivals after exec-state capture, multithreaded remote exec
or duplicate group operations across migration. A deterministic live hold inside
accepted CLOEXEC cleanup has not been exercised; ordinary runtime regressions
must not be described as that timing-specific proof. The full goal remains pending.

## 9FG pre-exec signal context

Full WKI compute identity now appends a 32-byte `TaskExecSignals` record after its
existing credential and job-origin records. Version 1 carries blocked, ignored
and pending bitmasks, with reserved fields required to be zero. SIGKILL and
SIGSTOP cannot be blocked or ignored. Unknown versions and invalid masks reject
before executable loading; earlier prefix/resource/credential/origin offsets and
the separate signal request layouts are unchanged. Exact full-context length
changes mean older kernels may refuse remote exec; mixed-version operation is
not qualified.

The sender observes the actor's mask and delivery-pending state and a complete
process handler-table snapshot. Only SIG_IGN survives in the handler record;
caught handler addresses, flags, restorer and handler masks are omitted. Capture
does not consume source pending bits, so preparation and rejection preserve them.
After canonical credential mapping and before scheduler publication, the fresh
receiver installs the ignored table, blocked mask and pending storage. Import
does not wake an unpublished task or replay signal generation/fanout. Its fresh
alternate stack remains disabled.

Kernel checks exercise actual encoding, validation and installation for inline,
VFS-ref and resource-ref envelopes, including signal 64, invalid reserved fields,
unsupported versions, unblockable masks and truncated suffixes. They also verify
source state remains intact and old handler addresses do not reach the receiver.
Runtime results are recorded in `build/goal19-exec-signal-context/validation.txt`.

The full isolated suite passed 52,184 checks with zero failures. Native ordinary
exec and exec from an alternate-stack SIGTERM handler passed all eight local and
remote root/UID 100 cases. The latter verifies that SIGTERM remains blocked in the
new image while both old caught handlers reset and the alternate stack is disabled.
The six group membership cases, foreign-group query, twelve signal cases, forty
session cases and sixty-six credential groups also passed. All helpers were removed.

This preserves state already present at capture. It does not establish ownership
of signals arriving afterward, make source/receiver handoff atomic, or solve
multithreaded remote exec. Those remain required work; moving scheduler proxy
readiness earlier or silently omitting in-flight recipients is insufficient.

## 9FF direct remote exec inheritance baseline

`service_remote_exec_signal_state` compares direct local and remote exec using
only its own root and permanently UID/GID 100 children. Before exec, each child
ignores SIGUSR1, installs a SIGUSR2 handler and alternate stack, blocks SIGUSR2,
queues it to itself and verifies it is pending. The new image inspects inherited
state before installing its own handler and unblocking. It never sends a new
signal to replace a lost one. Hostname and real/effective/saved IDs prove placement
and identity, and the driver reaps each child.

Both local cases pass. Both remote cases retain the correct IDs, reset caught
handlers and disable the old alternate stack, but lose the ignored disposition,
blocked mask and pending signal. Their new handlers therefore never run. Remote
source children 104/105 map to receiver 90/91 and finish with exit status 1; all
four children are reaped and the helper is removed. The original output is in
`build/goal19-remote-exec-signal-state/native-baseline/`. This is a failure baseline,
not a successful remote-exec qualification.

At the 9FF baseline, `WkiTaskIdentityContext` and its resource/credential/job-origin
suffix carried no signal state. The receiver constructs a fresh Task and posts it runnable
before sending ACCEPT. The source publishes its scheduler proxy identity after
accepted IPC handoff and potentially blocking CLOEXEC cleanup. Moving that flag
earlier is insufficient: scheduler blocking can then invoke proxy completion on
a task whose stack is still performing cleanup.

The next production contract must cover both preserved pre-exec signal state and
signals arriving during handoff. A one-time pending-mask copy alone leaves the
interval after capture unresolved. Signal routing authority must be separate
from scheduler proxy readiness; accepted delivery must retain one owner, rejection
must preserve original state, and fatal/session cleanup must retire any queued
requests. Group delivery must avoid duplicate source/receiver effects while still
discovering current receiver-created members. The full signal domain stays pending.

## 9FE receiver group operations

Group kill now queries and delivers on connected execution nodes. Each receiver
selects its current resident members, including children created there after the
submitted root exits. Parked source proxies are excluded. The retained recipient
cohort survives registry changes and fatal cleanup; membership, session and current
credentials are checked together under VM publication before signal effects.
The existing per-task ordinary signal and process job-control behavior is preserved.

Explicit numeric group aliases resolve to a namespace-local group first, then the
caller's inherited group, then a unique foreign origin. Ambiguous foreign aliases
fail. Queries have no signal effects, and an unavailable query cannot establish
foreign uniqueness. Signal zero uses the same current authorization decision.
Delivery succeeds if any recipient accepts, preserving the existing error policy.

`GROUP_SIGNAL_REQ` (0x5A, 136 bytes) and `GROUP_SIGNAL_RESP` (0x5B, 24 bytes)
use version 1. Peers advertise the COMPUTE resource `process-group-signals-v1`
with ID 0x53494701. A sender requires that advertisement before issuing requests;
unsupported peers return ENOTSUP without falling back to lifecycle cancellation.
Existing HELLO and positive-PID signal layouts remain unchanged. The authenticated
peer maps the canonical caller credential record. Replies bind request kind, node,
channel, generation and captured boot origin. Deferred reply retries reuse a cached
result and never repeat delivery. Outbound waits release the peer lifecycle lease;
receiver delivery does not recursively forward to another node.

The native group fixture now includes local and remote leave, join and orphan
descendant cases. Each target verifies permanent UID 100 before the caller sends
signals. In the orphan case, the submitted root is reaped before the operation;
an out-of-group supervisor explicitly reaps the member, and a separate owned
probe checks that supervisor exit is observable. This does not claim the source
can reap a supervisor adopted by receiver init. Runtime results belong in the
Goal 19 ledger and `build/goal19-group-signal-owner/validation.txt`.

All six membership cases passed on the two-node cluster. The separate `--query`
case also passed: receiver caller PID/session/group 165 resolved source group 497
as a foreign group, and its permanent UID 100 authorized signal zero and SIGUSR1
across distinct sessions. The source member reported handler execution and was
reaped. The forty-member cohort, twelve signal cases, forty session cases and
sixty-six credential groups passed. The full isolated suite passed 52,184 checks
with zero failures. These are functional tests of owned processes.

The signal domain remains pending. This operation does not make membership across
nodes globally atomic. Concurrent remote exec before source proxy publication,
movement during delivery, live peer/caller retirement, mixed versions, terminal
foreground selection and child-event/wait origins still need qualification.

## 9EY historical baseline

The native fixture `tests/fixtures/service_signal_identity.cpp` checks permissions
after a target changes its credentials. A separate caller permanently becomes
UID/GID 100. Each target verifies its real/effective/saved IDs, blocks SIGUSR1,
reports readiness, and waits for a pipe handshake. The caller tries signal zero
and SIGUSR1. If sending succeeds, the target waits for the pending signal before
unblocking it and reporting handler execution. Every target is explicitly released
and reaped. The fixture uses its own local and remote processes only.

The launch identity has real UID 0 and effective/saved UID either 0 or 100. The
target regains effective UID 0 through its real UID, then permanently changes all
three IDs to the requested final UID using supported WOS calls. This verifies the
permission after the final change, not authority from the temporary launch state.

| Placement | Launch effective/saved UID | Final target UID | Expected caller UID 100 | Observed |
|---|---:|---:|---|---|
| Local | 0 | 0 | Deny | EPERM, no handler |
| Local | 0 | 100 | Allow | Success, handler ran |
| Local | 100 | 200 | Deny | EPERM, no handler |
| Local | 100 | 100 | Allow | Success, handler ran |
| Remote | 0 | 0 | Deny | EPERM, no handler |
| Remote | 0 | 100 | Allow | **EPERM, no handler** |
| Remote | 100 | 200 | Deny | **Success, handler ran** |
| Remote | 100 | 100 | Allow | Success, handler ran |

Both signal zero and SIGUSR1 produced the shown permission result. All eight
cases verified final identity, completed their handshake, and reaped the target
with exit status 0. Six permission cases passed; two failed. The helper was
removed. Logs are in `build/goal19-signal-identity/qualified-baseline`.

## 9EZ implementation

`wos_proc_kill` captures caller credentials and job identity, retains each selected
Task, and calls `signal_as`. For a remote representative, the new synchronous
request carries a canonical caller credential record and session origin to the
receiver. No user signal uses the stale representative credentials or falls back
to credential-free `TASK_CANCEL`. Signal zero follows the same receiver decision.

The receiver maps the caller through configured authenticated-peer policy and
selects the exact running task by submission session, source and channel
generation. It retains that Task, captures its current credentials and session
under VM publication, and applies the central signal policy before queuing or
waking anything. The helper consumes the lookup reference on every return path.
Trusted kernel lifecycle and terminal cancellation retain their separate path.

A bounded table of 64 outgoing operations owns each response waiter and consumer.
Replies must match node, channel, generation, request ID and task ID. Request IDs
never wrap. Normal return, send failure, timeout, peer retirement and fatal caller
cleanup detach and quiesce waiters before stack reclamation; target references
outlive that cleanup. No application operation is resent after a timeout. A group
signal succeeds if any recipient accepts; otherwise it preserves a concrete
remote error, with the existing EPERM preference.

The receiver uses existing deferred compute slots. Once authorization/delivery
finishes, the slot stores the result before attempting reliable reply enqueue.
Backpressure requeues that same result until enqueue succeeds or the session
retires. Reply retries therefore do not repeat signal delivery. No network wait
runs under VM-publication, scheduler or compute locks. Response processing can
progress independently of a receiver worker holding its peer lifecycle lease.

Session identity now includes origin node and boot epoch. Local zero/zero is
relative to the authenticated sender; imported origins survive fork, clone, exec
and further remote submission, and normalize when returning to their origin.
`setsid` creates a new local origin. Equal numeric SIDs with different origins do
not grant the SIGCONT exception. Group-existence checks also compare origins,
and an imported session cannot claim a receiver-local terminal.

## Compatibility

The UID/GID syscall ABI and saved-ID semantics are unchanged. WKI adds
`TASK_SIGNAL_REQ` (0x58, 112 bytes), `TASK_SIGNAL_RESP` (0x59, 16 bytes) and an
8-byte session-origin record. The credential record remains 80 bytes. ACCEPT
advertises signal version 1 in reserved byte 0; its other reserved bytes stay zero.
REJECT resource suffix encoding is unchanged. Unknown signal support returns
ENOTSUP without a legacy user-signal fallback.

Full compute identity appends a 16-byte pair of session and group origins after
its existing credential record, followed by the 9FG 32-byte exec signal record.
Existing prefix/resource/credential/origin offsets remain unchanged. The origin
pair replaced the earlier 9EZ 8-byte session-only suffix.
Incompatible full-context lengths reject before execution. Mixed kernel versions may refuse remote compute; live mixed-version
compatibility has not been qualified.

## Functional test follow-up

The expanded native fixture checks 12 local/remote UID-transition and SIGCONT
session cases, using only its own processes. Both signals are blocked while the
caller records results; the target checks accepted pending signals, unblocks,
waits for their handlers across userspace-return boundaries, then reports and
exits. Every target must be reaped. Separate-session targets must reject a
foreign UID's SIGCONT.

The initial expanded run returned correct permission results but sampled the
second handler too early. WOS delivers one handler per return boundary; the
fixture now waits for both. It also exposed a separate real pipe EOF race: the
remote gate read returned 0 / errno 0 after the parent successfully wrote one byte
and immediately closed the pipe. The speculative read could observe an empty
ring before DATA and then see CLOSE after DATA. `proxy_pipe_read` now uses its
existing locked ring/pending-queue recheck before returning EOF. The fixture
keeps immediate gate closure to exercise this ordering. Original failing logs
are retained under `build/goal19-signal-owner/native-signals*`.

Final build and runtime results are recorded in the Goal19 ledger and
`build/goal19-signal-owner/validation.txt`. The complete signal domain remains
pending: wider group/session/terminal interactions, concurrent producers, and
live peer/caller retirement for the new RPC still need qualification. Deterministic
helper tests are not evidence of live packet loss or timing-race coverage.


## 9FA historical leadership collision

The new `service_session_origin` fixture aligns source and receiver numeric IDs.
A real source session/group leader with ID 102 correctly gets EPERM from another
`setsid`. After remote execution, eleven sequentially reaped padding children
produce receiver PID 102, which inherits source SID/PGID 102. That receiver child
is a different process from the foreign group creator, yet `setsid` wrongly
returns EPERM because `session::create` compares only numeric PGID and PID.
All fixture processes were reaped and the helper removed. The full baseline
is retained in `build/goal19-session-origin-collision/`.

The 9FA session-origin record fixed SIGCONT comparison, but group leadership
still needed a coherent identity: a receiver may create a new process group while
remaining in an imported session. Session origin alone cannot distinguish that
group from an inherited foreign group. Group creation/join, recipient selection,
child-event selectors, and import/export must preserve their corresponding
identity. Merely restricting group signals to the caller's session would also
change valid cross-session behavior. The 9FB foundation below addresses leadership.
The aligned test has not yet qualified different-origin SIGCONT: the failed
`setsid` prevents creating the distinct receiver session.


## 9FB independent group origin

Job identity now stores separate session and group origins. Leadership requires
a matching local-origin object and PID, so an inherited foreign PGID no longer
makes an equal-numbered receiver process its leader. `setsid` creates a local
session and group; `setpgid` and spawn group creation reset only group origin.
Joining a group copies the resolved origin within the existing session. Local
numeric groups take precedence over imported aliases, while the inherited own
group remains resolvable. Unrelated ambiguous imported groups fail without
mutating the target.

The WKI full credential context carries both origins in a 16-byte suffix and
validates them before executable work. Both normalize independently against
authenticated node/boot identity. Zero numeric IDs get local defaults with local
origins. Fork, clone and exec copy the complete job identity. Genuine local
group/session leader restrictions, child/exec permission checks, and UID/GID
syscall ABI are preserved. Mixed-version full-context lengths may reject.

The expanded fixture retains the exact numeric-collision case and adds creation
of a local group in an imported session, correct leader denial, fork/exec
inheritance, group join and subsequent session creation. Kernel tests exercise
actual session/spawn admission and both canonical origin records. Final results
are recorded in the Goal 19 ledger and `build/goal19-group-origin/validation.txt`.

Group signals, terminal foreground selection, and child-event/wait selectors
still need origin-aware recipient/selection ownership. Remote group changes can
also leave source proxy membership stale, requiring receiver-current membership
checks and coherent group discovery. This implementation does not qualify those
paths or the complete signal domain.


The final 9FB cluster passed the aligned collision and group-transition controls,
40 session/terminal cases, 12 signal cases, 66 credential groups, 10 output cases
and fallback capture. The corrected full isolated suite passed 52,181 checks
with zero failures. Initial test-only errno-width failures are retained in the
validation log; they did not require a production behavior change. All fixture
processes were reaped, helpers removed, and both guest VMs synced and stopped.


## 9FC receiver membership baseline

`service_group_signal.cpp` and its Python collector create a private session and
group for each case, keep one member in that group, and move a second target out
of or into it. Targets permanently become UID100, install/block SIGUSR1, and
report their final identity before a separate UID100 caller signals the group.
The caller finishes before targets are released to observe their handlers.

| Placement | Change | Retained member | Changed member | Result |
|---|---|---|---|---|
| Local | Leave | Delivered | Not delivered | Pass |
| Local | Join | Delivered | Delivered | Pass |
| Remote | Leave | Delivered | Delivered | Fail |
| Remote | Join | Delivered | Not delivered | Fail |

Both calls (`kill(-pgid, 0)` and SIGUSR1) return success in every case because
the group retains an authorized member. Receiver group110 becomes91 in the
leave case, while proxy112 remains selected by group110. Receiver group116
becomes114 in the join case, while proxy116 is omitted by selection114.
All targets reported the expected final identity, exited0 and were reaped;
the uploaded helper was removed. Both guests were synced and stopped.

This confirms an uncorrected recipient-selection gap, independent of the
receiver credential fix. A group selector on a per-task request prevents the
leave error but cannot discover the joined member. Complete discovery must also
cover receiver-created descendants and further handoffs without duplicate
delivery through parked proxies. Current group identity and mapped caller
authority need one coherent admission snapshot. Ordinary authorized signals
across sessions must remain possible. The source map and design constraints
are in `build/goal19-group-signal-membership/reconnaissance.txt`; actual checks
are in `validation.txt`. No production sources or kernel artifacts changed
in this checkpoint; the signal domain stays pending.


## 9FD retained recipient cohort

Group kill now snapshots retained registry recipients before starting delivery.
The prior separately locked index loop could skip or revisit a Task when
registry removal moved another entry during a remote wait. The captured cohort
keeps each unvisited Task alive and transfers each visited reference into the
existing nested signal owner immediately. Fatal cleanup retires WKI waiters
before releasing both the in-flight target and all remaining recipients.

The owner has 32 inline slots, grows outside the registry lock and makes at most
four capture attempts. Allocation/capacity failure returns ENOMEM/EAGAIN before
signal effects. Cleanup frees heap storage and tolerates repeated destruction.
Positive PID behavior, numeric group matching, existing per-thread/job-control
selection, credential admission, error aggregation and wire/ABI remain intact.

Corrected full KTEST passed 52,182/0. The initial new test assumed a current
scheduler Task existed during boot; it now supplies an explicit test-owned Task
to the same registration/cleanup code. The original failed log/kernel remain.
A native 40-member group passed delivery and normal reaping for every child,
exercising heap storage through the actual syscall. Existing 12 signal cases,
40 session cases and 66 credential groups passed. All helpers were removed and
both guests synced and stopped. See
`build/goal19-signal-recipient-snapshot/validation.txt`.

The two remote membership failures still reproduce. Snapshot ownership does
not correct source-cached membership or discover receiver-created children.
The next receiver operation must resolve group origin and select current
resident members using mapped caller authority, with bounded peer/request
ownership and no duplicate forwarding through parked proxies. Preserve current
ordinary per-task and job-control delivery semantics during that change;
a broader process-directed pending-state redesign is a separate concern.


## 9FO home identity and group delivery during exec

Each logical process now has one home enumeration anchor, identified by its
origin node, boot epoch and a saturating per-boot serial independent of its
recyclable PID. Imported receivers preserve that identity without becoming new
anchors; local fork descendants start new anchors. Enumeration uses sorted
32-entry pages and a fixed serial ceiling. Every lookup also validates the
home incarnation before admission, so a reused PID cannot receive its signal.

The original caller follows explicit redirects or no-effect retry responses.
Each actual receiver maps the original credential record and checks its current
group and credential snapshot. Receiver workers return routing information
without waiting for another node. Transport failure is not an application
resend. A completed receiver decision stays in its owned work slot while its
reply is retried. The source defers its own logical process until other
recipients have been visited.

Final local admission holds the canonical owner's signal lease through all
existing per-thread effects. Source exec drains that lease before capturing
committed pending state. The receiver installs committed signals before
becoming available for group admission, including during the diagnostic
post-COMMIT publication hold. Pending generation precedes observation of
scheduler visibility; publication marks visibility and checks pending bits,
so an admitted signal cannot miss both wake paths.

The protocol adds GROUP_ROUTE_REQ/RESP, the named process-group-routing-v1
COMPUTE feature, and an explicitly flagged home identity suffix. Existing
HELLO, submit header and legacy signal layouts remain unchanged. New senders
require the feature before submitting routed process identities; unsupported
peers cannot silently receive these semantics. Syscall and saved-ID ABI,
authenticated peer policy, canonical exec retirement and existing signal
permission decisions remain intact.

Normal and isolated builds pass, five host suites pass, and KTEST passes
52,192 checks with zero failures. A repeated live matrix passes all 20 group
handoff cases, including the four prior post-COMMIT ESRCH cases, plus 16
positive-PID controls. Group membership 6, foreign discovery 1, a 40-member
group, exec signal state 8, exec behavior 72 and signal permissions 12 pass.
All 40 wide-group children receive their handler once and are reaped.

The first group run passed 19 cases but its final after-acceptance case timed
out after successful query/delivery. Its parent subsequently hit SIGALRM and
the child was killed; no payload result marker was produced. Source and
receiver processes were verified absent, diagnostics preserved, and remaining
fixture files removed before the separate repeat. The exact completion failure
is unresolved; the repeat does not establish that it is fixed. An additional
remotable source checker also fails against the saved pre-change source.

Both guests were synced and stopped. Evidence, exact process retirement,
build/source hashes and the scoped patch are in
`build/goal19-group-signal-routing/validation.txt`. Further-hop and return-home
execution, differing mapping policies, fatal/peer timing and mixed-version
qualification remain before the signal domain can be declared complete.


## 9FP return-home execution witness

The new `service_group_return_home` fixture follows one owned child through
B, home A, and B again, while the original parent signals its original process
group. At each stop it records the current PID, host, group, real/effective/saved
UIDs and observed SIGUSR2 count. Final completion requires normal exit23 through
the original parent. Standard-signal coalescing limits what an observed count
of one alone can prove about duplicate requests.

The first root/main case passed the first two stops (B receiver90, then A
receiver103; original group102). The third stop reached B receiver91 and both
authorization calls succeeded, but no observation marker arrived and the
original parent did not reap normally. No other identity/executor cases ran.
Both nodes reported heartbeat probe transmission failures. The original child
was later reaped after SIGKILL, and subsequent SSH inspection was refused.
The precise completion/transport cause remains unresolved.

The unchanged kernel, first fixture binary/sources, marker files, serial logs
and bounded debugger evidence are preserved in
`build/goal19-group-return-home/`. Endpoint ownership and kernel hashes were
verified. The failed cluster is stopped and all host VM processes are absent;
guest sync and complete guest helper/intermediate-process cleanup are not
claimed. Final fixture logging now streams output and records admission times
so a future failing stage can be inspected while its child is still live. This
logging refinement was compiled and checked but has not been live-rerun.

See `build/goal19-group-return-home/validation.txt`. The third hop is a failed
completion observation, not verified multi-hop delivery. The signal domain
and complete authorization goal remain pending.


## 9FQ return-home qualification

The final streaming fixture now passes two full matrices on the unchanged
9FO kernel: root and UID 100, each with main-thread and worker-thread exec.
All 24 B→A→B stage observations preserve the expected host, process group and
real/effective/saved UIDs and observe one SIGUSR2. All eight original children
are normally reaped with exit23. Each of the 24 stage receiver retirement
checks passes with a native positive control, and both runs remove their
owned helper and marker files.

Both nodes were debug-enabled. Bounded read-only GDB captures before and after
the fixtures match the saved ELF build-ID on each node. The final source
inventory includes init, journald, netd, httpd and dropbear. No third-stage
wait triggered a stalled-receiver capture; the earlier intermittent completion
failure remains unexplained. These passing runs do not establish a fix.

Both guests were synced and stopped, with exact host process absence verified.
No production code or ABI changed, and no Normal Build WOS or isolated KTEST
rebuild was needed for this evidence-only increment. See
`build/goal19-group-hop-debug/validation.txt`. Differing-policy original-caller
mapping and fatal, peer and mixed-version qualification remain pending.


## 9FR asymmetric policy and local receiver discovery

`service_group_return_home --mapping` keeps a root parent on A while its
UID100 child executes B→A→B. B trusts nonroot numeric IDs; A also trusts root.
The parent queries the original group and sends SIGUSR1. A separately reaped
UID100 sender queries that same group and sends SIGUSR2. Distinct handler
counts expose effects from each caller. The fixture retains the ordinary
identity, completion and retirement checks.

Two live runs confirm root is denied EACCES at B with no observed SIGUSR1,
while UID100 succeeds. They also expose an incorrect return-home restriction:
root can query the current local receiver by PID, but querying or signaling
the original group still returns EACCES. UID100 succeeds at that same stop.
The home anchor routes through the older B proxy, whose identity gate rejects
root before returning the redirect to A. The expected local authorization
remains a failing regression. Worker mapping and third-stop mapping have not
passed; no production fix is claimed.

Current local receivers need discovery and logical-home deduplication without
requiring obsolete remote hops. That must include receivers with foreign home
anchors and preserve canonical admission, receiver policy, bounded enumeration,
late self-delivery and fatal cleanup. Removing the receiver identity gate alone
is not the proposed fix.

Ordinary UID100 main/worker controls pass all six stops and normal exit23. Both
failed source children are eventually reaped with exit6; all actual receivers
in those runs are individually verified absent before fixture file cleanup.
The cluster was synced and stopped. Builds of both fixture revisions and final
format/syntax checks pass; kernels remain unchanged. Evidence and bounded live
build-ID/public policy snapshots: `build/goal19-group-caller-mapping/validation.txt`.


## 9FS current local receiver discovery

Group signaling now captures a fixed cohort of current local canonical receivers
before following home anchors. Imported receivers participate even when their
immutable home is another node. Each selected full home incarnation is omitted
from subsequent anchor traversal, including after a denial or uncertain transport
result. The captured PID is a routing starting point; existing exact home checks
and canonical admission revalidate current placement, group and credentials.
Own-process delivery remains deferred. Remote receiver identity mapping and
numeric group namespace resolution are unchanged.

The cohort reuses `PendingSignalTarget` ownership for both normal return and
fatal syscall cleanup. Filtering keeps every remaining pointer in the registered
dense range while waiting for publication; excluded entries are removed by
swapping the last retained entry before release. No registry, publication or Task
lock crosses the callback's remote waits. The ownership selftest checks pointer
density during filtering, allocated-cohort reference counts and fatal cleanup.

Final asymmetric-policy tests now pass both main and worker B→A→B cases:
root is denied with no observed SIGUSR1 at B and succeeds at returned A, while
UID100 signal calls succeed at every stop. Two additional cases fork a process
on B, execute it on A, create a local group, and successfully signal that group
from root on A despite B's nonroot-only identity policy. Both parent chains
complete with exit23. UID100 controls and a 40-member group also pass.

Normal and isolated builds pass; final KTEST passes 52,192 checks with zero
failures. Full-trust return-home12 stages, group publication handoff20, positive
PID handoff16, membership6, foreign-query1 and signal-state8 checks pass. Owned
files are removed, receiver retirement is verified and both final clusters are
synced and stopped. Exact final hashes, live build-ID checks and evidence are in
`build/goal19-local-group-discovery/validation.txt`.

This fixes the demonstrated current-local denial. Current remote receivers behind
a differently trusted historical hop still need qualification and discovery work,
along with the existing foreign-group ambiguity, lifecycle and mixed-version
boundaries. The earlier intermittent completion failure remains unexplained.


## 9FT current remote receiver discovery regression

The return-home fixture now accepts `--third` to execute its middle stage on a
third node C. Root on A owns a UID100 child; B admits A's nonroot identities and
C also admits A's root identity. Main and worker B→C→B cases complete normally,
but root group query and SIGUSR1 incorrectly return EACCES at C. Separately
reaped root controls execute on the actual C hostname with UID0/0/0 and query
the exact local receiver successfully. UID100 query/SIGUSR2 succeeds at every
stop. Failed policy assertions remain failures even when the fixture continues
healthy owned-child execution for later observations and verified cleanup.

Current-local discovery cannot select a receiver on C. Existing C enumeration
lists only home anchors; the imported receiver is absent. A's home route still
passes through B, whose caller-mapping gate rejects root before redirecting.
The next implementation needs current remote receiver enumeration carrying full
immutable home identity and exact receiver PID, stable candidate selection and
deduplication, and storage registered for fatal cleanup across remote waits.
Preserve existing receiver admission and publication-handoff coverage; removing
the B identity gate does not resolve this design requirement.

Both final mapping cases retain the C policy failure while completing source
exit23 and all receiver/probe retirement checks. Ordinary UID100 controls pass
six stages and normal completion. A later SSH refusal prevents the separate
return-home control and guest sync. Published init status records dropbear wait
status9 and EXIT followed by cleanup TIMEOUT/FAILED, while its group TERM/KILL
returns EACCES. The daemon's original termination cause remains unknown. All
peer directions are still connected with active authentication. Exact owned
host processes are stopped; unsynced overlays/logs are preserved. Fixture build,
format, syntax and policy checks pass; production kernels are unchanged. See
`build/goal19-third-node-group-mapping/validation.txt`.


## 9FU fixed group target selection

Group operations now collect one typed target set before resolving the numeric
group or delivering a signal. Both passes use that same set. The first route
for each complete immutable home identity wins; a later observation never
replaces its endpoint. Current local receivers still precede home anchors.
Checked, nonthrowing storage growth occurs before any process-control effect.
The retained local cohort's existing fatal-cleanup registration also owns the
route storage, so a blocked operation cannot leak it on syscall termination.
Cleanup clears its callback before invocation and tolerates later destruction.

Actual kernel tests cover storage growth beyond32 targets, failed growth,
node/boot/serial deduplication, stable first endpoints and normal/fatal cleanup.
Normal and isolated builds pass; KTEST reports52,193 passes and zero failures.
A same-guest asymmetric retry passes mapping, foreign-home, UID100 and40-member
cases. Its initial remote readiness-file close failure remains separately
recorded. Correctly identified full-trust guests pass return-home, publication
handoff, membership and signal-state matrices. All owned children/files are
retired, final guests synced and exact host processes stopped. One mistakenly
selected older-image run is preserved as unqualified, with its incorrect-ELF
typed captures explicitly invalidated and cleanup verified.

Wire layouts and receiver credential gates are unchanged. This is the storage
and selection prerequisite for current remote receiver discovery; it does not
fix the9FT C-node denial. Per-node collection is not a globally atomic group
membership freeze. Existing admission, handoff and redirect rules still apply.
Evidence: `build/goal19-group-target-snapshot/validation.txt`.


## 9FV current remote receiver discovery

A separately advertised `process-group-receivers-v1` COMPUTE capability adds
current remote receivers before legacy home-anchor discovery. Each receiver
maps the original caller before enumeration. Group delivery still checks the
current canonical credentials, group membership and exact immutable home/PID.
The fixed target set retains the first endpoint for each full home identity;
discovery does not grant signal permission or replace an earlier denied route.

The new enumeration-only request reuses the existing 176-byte route request
layout under message type 0x60; reply 0x61 is 1056 bytes with at most 32 rows.
Each row carries a full immutable home, PID and receiver-local incarnation
cursor. The cursor does not wrap or depend on reusable PIDs. A frozen ceiling
bounds continuation. Registry and committed imported rows are merged, including
published imports to close the transition gap between the two snapshots.
Precommit, thread, forwarded and completed rows remain invisible. Replies bind
to the exact pending operation, request, peer boot, channel and generation.
Existing messages and userspace syscall layouts remain unchanged. Unsupported
peers retain legacy discovery, including its existing reachability limits.

Normal and isolated builds pass; final KTEST reports 52,195 passes and zero
failures. Five host suites and source/policy/format checks pass. The all-current
three-node regression now admits the root parent at C while denying it at B,
for both main and worker exec. UID100, return-home, foreign-home, wide-group,
publication handoff, membership and signal-state regressions also pass, with
owned process/file cleanup, guest sync and exact host-process retirement.

An intentional older-B/current-A-and-C run passes the root mapping matrix.
Its separate UID100 worker case stalls after successful C admission, with
pending signals on a parked C receiver, then fails completion. The source is
later reaped with SIGKILL; source and receiver absence and file cleanup are
verified. One repeat on the same guests passes all six UID100 stages. This
repeat does not resolve the first failure or explain the remote netd restarts.
The mixed guests are synced and stopped; all evidence is preserved. Per-node
collection is not a globally atomic membership snapshot, and broader lifecycle
and authority-domain qualification remains pending.
Evidence: `build/goal19-current-remote-discovery/validation.txt`.
