# Goal19 9NY: signal authorization enforcement audit

The `signals` authority domain is enforced for Goal19's immutable-credential
contract. This is a reconciliation of current source and the recorded functional
evidence, not a new kernel change or a claim that every signal scheduling and
transport interleaving has been tested. Network/hostname, WKI identity and WKI
resources remain pending; the goal remains active at **18 enforced / 3 pending**.

## Scope and decision criteria

The original goal requires one stable credential snapshot per authorization
decision, central policy, preserved syscall semantics and task lifetimes, and
denials without unauthorized effects. For signals this means every ordinary
recipient is checked, including signal zero and a recipient reached after remote
exec. Internal producers must have an identified, narrower source of authority;
they cannot serve as alternate user-kill paths.

Group delivery checks recipients independently. Success for an authorized member
does not grant delivery to a denied member. A transport timeout after an allowed
signal was delivered is an uncertain reply outcome, not an authorization denial
or a promise of rollback. The 9NX live test explicitly observed both admitted and
denied receiver decisions before losing their replies.

## Current entrypoints, ownership and evidence

| Surface | Current decision and lifetime contract | Evidence |
| --- | --- | --- |
| Positive PID and signal zero | `wos_proc_kill` captures caller credentials/job identity once. `signal_as` consumes a retained target; `signal_local_as` obtains a local admission and current target credentials/job identity under VM publication. `security::may_signal` uses SIGNAL authority, caller real/effective IDs against target real/saved IDs, or the full same-session CONT exception. Denial and zero return before pending bits, ptrace stop or wake effects. | Credential model exhaustive ID combinations and irreversible ceiling tests; `Credentials/SignalDenialAndSavedIdentityPolicy`; 9EZ and fresh 9NX 12-case current-identity controls. |
| Native and remote exec | Local admission refuses replaced native worker address spaces. Accepted remote routes prevent old-image local authorization; a raced ACCEPT retries receiver routing without local fallback. Exec drains admitted producers before final pending-state capture. | 9FK canonical owner/result fix; 9FM/9FO pending/group handoff; 9HQ/9HR native-worker admission; current accepted-route, local admission and signal-state KTESTs. |
| Remote ordinary signals | `authorize_user_signal` decodes a complete snapshot, maps it through captured authenticated compute identity policy, retains the exact published execution in its submission session and calls the shared decision path. Replies bind request/task/peer/channel generation. Version refusal and bounded slot retirement do not fall back to trusted cancellation. | `UserSignalReplyRetirementAndCurrentTargetAuthority`, `SignalVersionFollowsRequestAcrossSlotReuse`; 9HT correlated policy-denial replies; 9NX four live receiver resets, bounded caller completion and empty final signal slots. |
| Process groups | Discovery carries home/node/boot provenance. Final delivery checks current membership and credentials together; canonical admission lasts through worker effects. Local aliases, inherited groups and unique foreign origins have explicit resolution rules. Per-target discovery is not a delivery grant. | 9FB session/group origins, 9FE/9FV routing and receiver discovery; current group membership, home visibility, first-home-route, wire and reply-ownership KTESTs. |
| Terminal input/hangup | Scheduler selection uses terminal allocation/generation plus full session/group identity. `signal_terminal_source_as` retains local admission or forwards the accepted execution. Receiver terminal delivery checks its current membership. The terminal grant is independent of UID and cannot become arbitrary user kill. | [9MW](goal19_terminal_source_admission.md), [9MU](goal19_remote_terminal_admission.md), actual source-admission and exact-version/session/current-membership KTESTs. |
| Ptrace | KILL uses `require_traced` under VM publication before stop acknowledgment and signal effects. Trace authorization remains `may_trace`; ordinary authorized SIGKILL can release a trace stop. | [9MY](goal19_ptrace_fatal_release.md), 24 live ptrace interactions plus stop/churn controls; current ptrace and signal source guards. |
| Parent death | The child owns its registration. Local delivery holds admission; the dedicated remote event names the accepted execution/session and consults the receiver's current registration. It carries no arbitrary caller privilege grant. | [9NA](goal19_parent_death_delivery.md), local/remote/further-hop functional cases and actual registration/admission/session KTESTs. |
| SIGCHLD and wait ownership | Retained child relationships authorize a fixed SIGCHLD event. Producers release the lifecycle lock before local delivery or bounded deferred enqueue. Remote waits bind accepted execution and child ownership, copy values before COMMIT, retain detached commits independently, and account once. | [9NI](goal19_child_signal_delivery.md), [9NR](goal19_child_wait_client.md), [9NT](goal19_child_wait_termination.md), [9NV](goal19_worker_wait_exec.md), [9NW](goal19_child_wait_restart.md). |
| Ambiguous child PID | A positive selector naming two owned children on different nodes returns EOVERFLOW before claiming either. Wait-any remains usable. WNOWAIT and failed kernel usercopy do not reap or charge child CPU. | User-selected contract; [9NS](goal19_child_wait_semantics.md), repeated 26 checks on the current kernel in 9NT and 9NW; `NativeChildWaitPreservesCollisionClaimsAndDetachedCommit`. |

The producer inventory also includes self-directed faults and bad signal frames
in `platform/sys/signal.cpp`, timer-owned SIGALRM in `scheduler.cpp`, SIGPIPE for
the writing/waiting pipe actor in `vfs/core.cpp`, and SIGXFSZ for the caller of
`resource::FileGrowth`. These are effects of the selected task's execution, timer
or I/O operation, not requests to signal an arbitrary PID. CPU limits use the
canonical resource policy and safe user-return teardown. Internal exit/exec
retirement belongs to the process lifecycle. Shutdown broadcast is reached only
after the POWER authorization in `platform/power/power.cpp`.

WKI cancellation remains a separate submission-lifecycle grant scoped to the
authenticated submission session. Ordinary user signals and terminal events do
not call `wki_task_cancel`. Its public declaration currently has no external
kernel call sites; internal submission failure/retirement sends cancellation on
the captured submission channel. `SubmitCancelIsSessionScoped` remains covered
by the recorded full KTEST. This distinction must survive future call-site changes.

## Locks, contexts and compatibility

Credential policy copies bounded values and does not allocate. Ordinary local
admission orders VM publication before task metadata/job state; generation and
wakes occur with the admission retained after the publication scope. Registry
lookups retain task references before leaving registry ownership. Canonical
job-control effects use the existing resource lock. These are distinct from the
session-mutation/terminal callbacks, which must not recursively take publication.

Compute locks protect exact execution/query/claim records; they do not span user
copy, remote waits or scheduler cleanup. Deferred lifecycle queues own references
and release their queue locks before peer/compute work. Fatal syscall unwinding
does not erase its termination marker; validated user-return teardown and a
worker-owned COMMIT outcome remain separate. Receiver reset retires the old
incarnation, never establishing cleanup merely by absence of a reused numeric PID.

No code, lock, ABI, wire version, credential rule or declared VFS routing changes
in 9NY. Existing WOS `kill(-1)` process-group behavior is preserved; it is distinct
from `waitpid(-1)` selecting any owned child.

## Reconciliation of historical pending notes

The former manifest decision accumulated historical failures and prospective
tests. Its stale claims of missing current-target authorization, canonical exec
ownership, terminal admission, parent-death forwarding, SIGCHLD forwarding and
native distributed wait are superseded by the implementations and evidence above.
The old decision is retained with the stage's before files; chronological evidence
in the goal ledger and manifest is preserved.

Hard CPU expiry in a live COMMIT, a concurrent unmap during wait usercopy, the
faster EHOSTDOWN completion race, and every group/source-restart/mixed-version
combination have not all been exercised live. Current production KTESTs cover
irrevocable termination markers, pre-COMMIT failure/claims, exact sessions,
version refusal and reply retirement; the recorded live cases cover actual
authorization, handoff and cleanup through transport. These unrun combinations
are additional runtime coverage, not evidence of a missing authorization seam.
No stronger live claim is made for them.

Historical 9FV/9FZ progress stalls, 9LG's interpreter-fetch timeout, 9LZ's directory
timeout and 9MP's worker status137 remain unexplained observations. Later passing
runs do not diagnose them. They are retained for broader distributed progress
review in the pending WKI domains; this audit found no demonstrated surviving
signal-authorization violation in those observations. An orphaned-process-group
transition controller is not implemented in the reviewed subsystem; this goal
does not add general POSIX job-control feature completeness as an acceptance rule.

## Validation

Fresh host compilation and four model suites pass: credentials, resource
contracts, WKI wire and WKI authentication policy. Signal, signal-atomic, waitpid
and remote-compute source guards pass. The policy audit and its unit suite pass;
`--require-complete` still fails, now at `network-hostname`.

No new kernel build, KTEST boot or VM run is claimed. The normal kernel SHA256
is `f4d71142f4946b3d0a32beb8012f5c1e88cb8e17785c019ba3c0d91ff6846596` and diagnostic
SHA256 is `48834eb31c11bf6ddf4ddf8062c4b14be534d385c6e207203d121e6efe188858`, matching
9NT/9NX. The prior Normal Build WOS and isolated build passed; 9NT KTEST recorded
**60,034 / 0**. Selected actual PASS lines, current source hashes, linked receipt
hashes, fresh command results and the single-domain manifest transition are
recorded in `build/goal19-signal-authorization-audit/validation.json`.

Next: audit the pending network/hostname operation inventory against its current
authorization and transaction boundaries, including hostname, raw sockets,
NETCTL/link/address controls, sealed netd grants and mapped owner control. The
same original goal completion rule applies; no domain is deleted or bypassed.
