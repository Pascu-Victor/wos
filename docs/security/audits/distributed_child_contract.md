# Goal19 9NF: distributed child authority contract

Reconnaissance against the current local tree after 9NE. Previous turn: progress.
No production change or new kernel/runtime qualification in this stage. Goal19
remains 17 enforced / 4 pending, with `signals` first. The user's authorization
to run WOS with `--no-setup` remains in force; no runtime blocker was encountered.

## What the current source establishes

| Subsystem | Entrypoints and authoritative behavior | Consequence |
| --- | --- | --- |
| Local waits | `waitpid.cpp::wos_proc_waitpid` calls `acquire_process_owner`, local `claim_or_register`, usercopy and `commit_claim` | Receiver-local ownership alone cannot reach a pre-exec source child queue. |
| Event ownership | `child_events.cpp::begin_publication`, `enqueue_event_locked`, `clear_event_claim_locked`, `prepare_process_exit` | Queued events retain exact owners/subjects; cancel clears claimant backlinks before reclaim. A remote grant must retain this ownership rather than reconstruct it by PID. |
| Claim storage | `Task::child_claimed_event`, `ChildEvent::claimed_by`, `claim_or_register` | One Task has one current claim. A shared RX worker cannot represent concurrent remote waiters or repeated requests without independent transaction ownership. |
| Source execution | `SubmittedTask`, `handle_task_accept`, `wait_for_exec_submit` | The source keeps an exact `local_task`, target, submit channel and generation. Exec cutover and final ACCEPT are separate states. |
| Receiver execution | `RunningRemoteTask`, `find_running_task` | Reverse traversal must start from the exact retained receiver Task and its published execution row, then validate the source's submitted row. The forward `find_running_task` helper is not a source-owner lookup. |
| Deferred RX | `compute_rx_worker_loop`, `PeerLifecycleLease`, `compute_rx_worker_index` | Up to four workers are sharded by source node. Processing holds an exclusive peer lifecycle lease. A recursive wait must not hold this worker/lease while waiting for the next hop. |
| Session retirement | `wki_remote_compute_retire_submit_session`, `fail_submitted_tasks_for_peer`, `terminate_running_remote_tasks_for_peer` | Retired generations and full peer loss have distinct cleanup scopes. New claims must participate in both without canceling newer sessions. |
| Fatal caller exit | `retire_user_signals_for_task`, `wki_remote_compute_cleanup_for_task`, `wki_wait_cleanup_for_task`, `child_events::cancel_wait` | Stack-backed waiter cleanup and distributed claim cleanup are different responsibilities. A C++ stack destructor is insufficient if fatal exit abandons that stack. |
| SIGCHLD | `child_events.cpp::deliver_wake` | It still writes a source Task's pending bit directly. Migration requires local admission or deferred forwarding to the accepted execution, alongside wait authority. |

`build/goal19-distributed-child-contract/inspection.json` records source hashes,
function excerpts and the evidence supporting these conclusions. Earlier 9NB
runtime failures remain historical evidence; they were not rerun here.

## Decisions changed by this review

**Use continuations, not recursive synchronous RX RPC.** The existing compute
worker's `PeerLifecycleLease` is exclusive. Peer RX admission refuses that writer
state. A new synchronous reverse wait that returns through the same peer can
therefore obstruct the response it needs. Source-node sharding also means four
workers do not provide four independent lanes for one peer. This is a source-based
constraint on the proposed protocol, not a reproduced deadlock in current waits,
which do not yet perform a remote request. Persist bounded continuation state,
release all peer/compute/lifecycle locks, and resume only after revalidation.

**Bind reverse authority to exec handoff.** A receiver running an ordinary remote
spawn is not entitled to its submitter's children. Only a committed exec-handoff
row can extend the same logical parent's child authority upstream. Match the
request to the retained submitted Task and exact accepted generation; neither
remote PID, same UID nor a generic COMPUTE permission replaces this relationship.
Use the accepted relationship even if subsequent set-ID exec changes credentials.

**Allow receiver start to precede source ACCEPT processing.** A published receiver
may issue its first wait while final ACCEPT is in flight. The source must defer
the reverse request until the accepted cutover resolves, or terminate it with the
handoff's actual failure. Returning ECHILD merely because ACCEPT has not yet been
processed would preserve the original bug as a timing-dependent failure.

**Separate local event claims from syscall Tasks.** A persistent claim record needs
an event backlink and generation cookie independent of the RX executor. Event
removal must invalidate that backlink under the existing lifecycle lock before
dropping its subject. Local waiters must retain their current cancellation/wake
behavior. Remote claimant slots need a bounded session-bound owner, explicit
cancel/commit transitions, and deferred cleanup outside WKI locks. Do not allocate
synthetic registered Tasks to impersonate remote waiters.

**Merge group candidates before choosing an origin.** Each queue reports owned
live children, tracees and immutable queued groups in the initiating waiter's
namespace. Normalize a hop-local zero origin to that hop's node/boot before the
merge. `session::GroupCandidates::merge` can preserve local/own/other ambiguity;
calling `selector_for_process_group` separately on each hop loses candidates and
can consume an event before discovering a preferred or ambiguous origin elsewhere.

**Do not count send success as wait commit.** `wki_send_on_channel_generation`
accepts reliable transport work; it is not an application-level commit receipt.
The protocol must preserve the response to a committed transaction until explicit
acknowledgment or terminal session cleanup. Duplicate commit returns the same
result, not a new claim. A timeout after commit cannot be reported as proof that
the event remained unconsumed. Response-loss behavior needs explicit tests and a
defined terminal policy before the feature is qualified.

## Native-owner hypothesis checked

The earlier suspicion that ordinary `pthread_exit` makes the leader unavailable
to `acquire_process_owner` is not established. `threadControl.cpp` routes a main
thread exit to `retire_main_thread`; that function retains the process store and
cleanup stack, handles pending exec/signals, and parks until sibling CPU admission
closes. It does not immediately publish the owner as DEAD. Worker resource
ownership is retained by `resource::LifetimeOwner`. This review makes no native
owner fix and does not label this as a reproduced bug. Distributed receiver-local
ownership remains a separate proven gap.

## Implementation contract

Problem: preserve a parent's owned child/event queues across committed remote
exec, with local and receiver-created children, further hops and notifications.

Expected files: `platform/sched/task.hpp`, `syscalls_impl/process/child_events.*`,
`waitpid.cpp`, signal admission code, `net/wki/remote_compute.*`, `wire.hpp`,
`auth_policy.cpp`, `wki.cpp`, resource advertisement, corresponding kernel/host
tests and owned-process userspace fixtures. Read the relevant subsystem contracts
again when implementing each changed entrypoint; this document is not a substitute
for code review.

Files already read: those in the source map, `session.hpp`, `resource.cpp`,
`threadControl.cpp`, `exit.cpp`, exec submission/commit and peer lifecycle helpers,
plus current wait/WKI source guards and the existing child-event fixture.

Invariants: one stable credential snapshot where credentials are relevant; exact
accepted relationship for child ownership; source event sequence and rusage;
parent/tracer separation; EFAULT release, WNOWAIT and one consuming winner;
9NE's safe backlink validation; no authority from unrelated same-number Tasks;
process exit reparenting and reference release; declared VFS routing unchanged.

Locks: retain a Task under `s_compute_lock`, then release it before entering the
child lifecycle lock. Never hold a peer lifecycle lease or compute/channel/RX
queue lock over a remote wait, event usercopy, scheduler park or child retirement.
Each continuation revalidates the exact session immediately before its effect.

Unsafe contexts: scheduler-related event producers and RX/NAPI. Producers enqueue
bounded reference-owning notification work; they cannot invoke a blocking remote
wait. RX performs length/policy/session admission and bounded transaction updates.

Allocation/blocking: reserve all transaction and reply capacity before claiming
an event. Exhaustion returns a defined retry/error without consuming an event.
Waiting syscall state must be registered in task cleanup before any interruptible
park. Cancellation and session cleanup must have a no-allocation path.

ABI/wire/syscall compatibility: preserve the wait syscall number, PID argument,
status/rusage layout and local errno behavior. Introduce a separately advertised,
versioned child-authority protocol with packed size assertions and policy mapping.
Gate handoff before transferring authority to a peer lacking the contract; do not
silently fall back to receiver-local ECHILD or deny all parents with children.

Rollback: retain exact before files for each implementation stage and preserve
the current local claim API until the new path has equivalent lifecycle coverage.
No production rollback is necessary for this documentation-only stage.

## Required sequence and acceptance evidence

1. Introduce independent lifecycle claim ownership and cancellation, used by the
   local path as well as remote transactions. Kernel tests must interleave two
   claims serviced by one executor, cancellation, event transfer, subject reclaim,
   WNOWAIT and duplicate commit. No wire path should borrow an RX Task's claim slot.
2. Implement versioned reverse exec authority and continuation records. Validate
   prepared versus committed exec, exact source relationship, generation changes,
   ordinary remote spawn, and cancellation during every transaction phase.
3. Aggregate owned queues before resolving selectors; claim at one selected queue,
   copy on the initiating waiter, then commit/release at that queue. Extend the
   current fixture to children created at each hop and concurrent native waiters.
4. Route SIGCHLD through local admission or deferred accepted-route forwarding.
   Cover event production before/during/after cutover, retained local events,
   blocked waits, ignored/masked notifications and process exit.
5. Qualify full behavior with Normal Build WOS, isolated KTEST and owned local,
   remote and round-trip fixtures at UID0/104. Require the four historical 9NB
   failures to pass as well as new receiver-created-child and contention cases.
   Verify cleanup by explicit reaping/reference evidence, not procfs absence alone.

Two design details must be settled during implementation before enabling the
wire path: positive-PID collisions between children created in different execution
namespaces, and the terminal result of a committed wait whose reply is lost with
the session. Neither is resolved by the existing group-origin fix. Keep them
explicit in tests; do not invent cross-node event order from timestamps or return
ECHILD for a valid owned child to make a narrow test pass.

Current source guards cover local claim/usercopy/commit and proxy exit publication.
Their passing status cannot prove reverse child authority or the new transaction
states. This stage runs the relevant guards to record that distinction and the
policy audit to keep completion status honest. It does not rerun builds, KTEST or
VMs for unchanged production code, and requests no user-run debugging.
