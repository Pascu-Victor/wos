# Goal19 9ND: wait group identity

Previous turn: progress. 9NC eliminated eight observed retained Tasks and qualified
deferred proxy retirement. Distributed wait and notification remain incomplete.

## Reconnaissance and patch contract

Problem: waitpid group selectors, potential-subject scans, queued event snapshots
and registered waiters store only numeric PGIDs. Existing job identities already
carry node/boot group origins. Equal numbers from distinct origins consequently
match as one group, including waitpid(0). Forwarding that selector across WKI would
preserve the wrong ownership boundary.

Expected files: Task child event/registration storage; child_events.hpp/.cpp;
waitpid.cpp; deterministic child-event KTEST registration; source guards and ledger.

Files read: Task child storage and job identity; child_events producers, claims,
reparenting and waiter selection; waitpid usercopy/commit; session Identity and
GroupCandidates/resolve_group; signal group resolution; remote proxy retirement;
9NB distributed contract. Current local sources are authoritative.

Flow: wait entry captures a selector -> lifecycle-locked potential/event scan ->
claim -> usercopy -> commit. Event snapshots and blocked registrations must retain
the same complete group identity through every step. Group updates already use the
lifecycle lock and wake invalidated selectors.

Invariants: positive PID and any-child selection retain existing semantics.
waitpid(0) names the caller's exact current group, including origin. A negative
PGID resolves only among this owner's child/tracer subjects and immutable events:
local alias first, own inherited alias next, otherwise one unique foreign origin.
Ambiguous/missing subjects return ECHILD without consuming events; malformed numeric
selectors retain EINVAL. Resolution is fixed for one wait invocation. No global
PID scan grants additional parent/tracer authority. Claims, retry, WNOWAIT, rusage,
and single-winner publication remain unchanged.

Locks: existing lifecycle lock and bounded job snapshot lock in the established
order. No extra registry, WKI, resource or runqueue locks. No allocation, network
wait, logging, or blocking in selection. Retained Task/event lifetimes unchanged.

ABI/wire impact: kernel-private event/selector storage only. Encoded procfs wait
diagnostics and userspace waitpid syscall ABI remain unchanged. No new WKI message
or remote wait authority is claimed. Declared VFS routing is preserved.

Build/test plan: actual lifecycle helper KTEST for equal-PGID/different-node and
boot identities, negative alias precedence and ambiguity, immutable event origin,
waiter invalidation after origin-only movement, retry/NOWAIT/single winner and
reference release. Normal Build WOS, isolated full KTEST, existing wait/exec/signal/
remote compute source checks and focused host models. Runtime orchestration uses
the user's authorized --no-setup workflow. No missing user-run prerequisite.

Rollback: exact before files and command receipts in
build/goal19-wait-group-origin/. Full distributed child authority and SIGCHLD
forwarding remain next work; this prerequisite does not promote signals.

## Implemented behavior

`ChildWaitSelector` retains the encoded selector and group origin. Child events
capture PGID and origin from one job-identity snapshot. Matching pending subjects,
queued events and registered waiters uses both values. `ClaimedEvent` preserves
the publication-time origin through usercopy/release/commit, including WNOWAIT.
An origin-only job change uses the existing lifecycle wake path to notify waiters
that have lost their final matching child. Diagnostic selector text remains the
existing encoded integer.

Negative-PGID resolution examines only the owner's child list, tracee list and
immutable event queue under the lifecycle lock. It reuses `GroupCandidates` for
local/own/unique precedence and refuses ambiguous aliases as no matching child.
The caller's zero-PID selector always uses its exact group, even when a different
local group shares the same number. Positive PID and any-child behavior is unchanged.

## Deterministic and live evidence

Both new actual-lifecycle KTEST cases pass. They distinguish equal PGIDs across
different nodes and different boot generations; exercise local alias precedence,
inherited and unique foreign selection, and ambiguity; verify origin-only waiter
invalidation and immutable events after a child moves; and preserve usercopy retry,
WNOWAIT, single-winner claims and final reference counts. Different boot generations
are deterministic test inputs, not a live peer-restart claim.

The same native fixture binary ran on private two-node boots of the previous 9NC
kernel and this kernel. For each UID, the parent first creates a source-local group
then execs on wos-3. One receiver child inherits that foreign-origin group. A second
receiver child creates a local group with exactly the same number using `setsid`.
Bounded, immediately reaped padding forks align the PID counters. The local child
exits first, and WNOWAIT proves its event exists before group selection is tested.

| UID | Group | Local / foreign child | Previous waitpid(0) | Fixed waitpid(0) | Fixed negative wait | Final own-group wait |
|---|---|---|---|---|---|---|
| 0 | 123 | 123 / 108 | 123 (wrong group) | 0 | 123, status10496 | 108, status9472 |
| 104 | 141 | 141 / 126 | 141 (wrong group) | 0 | 141, status10496 | 126, status9472 |

Both previous-kernel rows fail exactly the selection assertions: the wrongly
consumed event leaves the negative wait returning0. Both fixed rows pass, with
receiver UID verified and identical aligned PIDs. Status10496 is exit41; status9472
is exit37. Each run confirms no remaining waitable child, reaps both migrated
parents with status0, observes all recorded source/receiver procfs entries absent,
and removes both helper files. Reap receipts are the child-cleanup evidence;
procfs absence alone is not claimed to prove kernel reference reclamation.

## Validation and cleanup

Normal Build WOS and isolated diagnostic builds pass. Full freshly packaged KTEST
passes **59,828 assertions, zero failures** at483.280 seconds and powers off at
483.281. Five focused host suites, wait/exec/signal/atomic-signal/remote-compute
source suites, scoped formatting and native fixture compilation pass. Existing
signal/terminal/transition/ptrace fixtures pass another568 checks on the new kernel.

The first wait source check required the old numeric-only snapshot expression.
The atomic-signal guard still expected 9NC's former monolithic proxy finalizer;
its producer assertion now follows the deferred worker. Failed intermediate
receipts are retained. Initial fixture compilation exposed PID printf-width
warnings; corrected before either live execution. No production edit followed
the final normal/diagnostic builds.

- Normal Build ID: `f494e7851017ca39728a2dfd8e8fa22e4f3181ae`.
- Diagnostic Build ID: `ddb45f3091a3dd24ec16a6b41c9ef456721b337b`.
- Baseline Build ID: `d879cfee9b2e937a7fdf415085b1859f9d28df90`.

Packaged manifests, unchanged backing-image/fixture hashes, command receipts,
serial logs, live results and the stage diff are recorded in
`build/goal19-wait-group-origin/validation.json` and adjacent artifacts.
Baseline guests2527146/2527184 and fixed guests2527795/2527800 synced before exact
QMP shutdown; diagnostic2524985 powered off after selftests. No WOS VMs remain at
**2026-09-16T13:06:58.907093+00:00**. No user VM was stopped.

Goal19 remains active at17 enforced/4pending domains, first pending signals.
The complete 9NB distributed child-event authority/claim/commit and SIGCHLD route
contract remains required. In particular, a future remote group query must merge
candidate origins across the owned execution chain before resolving one selector;
independently resolving numeric aliases at every hop would reintroduce ambiguity.
