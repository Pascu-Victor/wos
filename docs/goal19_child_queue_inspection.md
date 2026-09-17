# Goal19 9NK: child queue inspection

## Patch contract

Problem: distributed wait needs to inspect all authorized child queues before
choosing a numeric PID/group. The user explicitly requires EOVERFLOW without
reaping either child on a positive-PID collision; waitpid(-1) remains usable.

Expected files to change: child_events.cpp/.hpp, child_events_ktest.cpp and goal
progress evidence. Files already read: child lifecycle publication, selector,
claim, retirement and tests; waitpid.cpp; Task child links and job identities;
session::GroupCandidates; previous reverse-exec admission and transaction contract.

Impact map: child_events owns membership, immutable events, tracer lists and
claims; session::GroupCandidates merges numeric group aliases. Local waitpid
resolves aliases then claims or registers. The new inspection helpers return
values from these same lists; a future WKI continuation will merge them before
claiming. No global PID registry grants child authority.

Invariants to preserve: current local selector precedence; immutable queued
groups; claimed events remain potential subjects; no claim, reap, wake, refcount
or accounting effect during inspection. Positive PID ambiguity includes live
children even when only one child has a ready event. Repeated reports of the same
node/boot namespace do not create a collision. Distinct boots do.

Locks involved: child lifecycle, then existing job-identity lock. Callers retain
the owner and hold no scheduler, WKI or resource locks. Unsafe contexts: no raw
NAPI call or usercopy. Allocation/blocking assumptions: no allocation, sleep,
network operation or escaping task/event pointer; scans bounded by owned lists.

ABI/wire/syscall compatibility impact: no wire or syscall change in this stage.
Inspection is advisory, not a reservation or atomic cross-node snapshot. Future
claim admission must handle mutations between inspection and claim; this layer
alone cannot enforce EOVERFLOW at the syscall boundary.

Build/test plan: KTEST actual child/event/claim/tracer queues, origin merge order,
PID collision without mutation, any-child claims after collision, local selector
regressions, Normal Build WOS and isolated diagnostic build/live KTEST. No runtime
information is required from the user; --no-setup authorization remains in force.

Rollback plan: exact pre-edit files under build/goal19-child-queue-inspection/before.
Goal remains 17 enforced / 4 pending until distributed wait and the remaining
authority domains are fully qualified.

## Implementation and integration requirements

`inspect_queue` reads potential-child membership and event readiness under one
lifecycle lock. It shares the predicates used by local waits, including tracer
ownership, option filtering and serialization of all events for one subject.
`inspect_pid_candidates` counts selector-visible subjects even while alive or
claimed; its merge record preserves node/boot ambiguity through retries.
`inspect_group_candidates` scans immutable events and current child/tracer groups,
normalizes this queue's local origins to its physical namespace, and classifies
candidates relative to the initiating caller. Local group selection now uses that
same locked scanner with its existing local/inherited/unique-foreign precedence.

Callers must consistently provide the physical namespace for each queue from
retained local/authenticated execution state, not an unchecked peer assertion.
PID origins are independent of group origins. The result of GroupCandidates'
LOCAL resolution is zero/zero in the initiating caller's namespace; a relay must
make that origin physical before localizing it for a different queue. It must
not interpret the result as its own local group.

These snapshots grant no authority and reserve nothing. The next WKI layer must
retain the exact reverse-exec owner, release compute/peer locks before inspection,
revalidate current session/execution state before exposing a response, and handle
queue changes between inspection and claim. Cross-node queries alone are not an
atomic snapshot. Candidate collection must not consume or claim events before
ambiguous positive-PID selection is rejected. Incomplete traversal, pending
ACCEPT or an unavailable hop cannot be treated as proof of unique PID ownership.

Source reconnaissance confirms that `capture_child_authority_route_locked`
returns only the next exact exec hop, while `acquire_child_authority_owner_locked`
returns a retained source owner. `PendingUserSignal` and `WkiWaitEntry` retain
response/consumer stack pointers and require explicit retirement; they cannot be
copied into a recursive RX handler and allowed to outlive that handler. Further
hops need persistent continuations with bounded hop/lane ownership and cleanup.
Existing `resolve_session_origin` and `concrete_signal_origin` show the current
physical/local node and boot normalization convention. These are integration
requirements, not implemented or live-tested distributed wait behavior.

A possible next implementation is a per-owner lifecycle version, collected for
all queues and then revalidated across the same retained exec chain before a
claim. The selected queue's BEGIN must also require its inspected version;
otherwise it could claim a newly arrived event after a new PID collision. Two
complete version collections can establish a shared snapshot interval, but only
if all selector-visible mutations advance the version and the selected queue
still matches at claim. This is a proposal requiring implementation and tests,
not a property of the current inspection API.

The mutation entrypoints read for that proposal are child list insertion/removal,
tracer list insertion/removal, queue insertion/unlink, publish/clear subject
claims, and update_job_identity for both parent and tracer observers. Publication,
reparenting, commit, cancellation and exit retirement use these helpers. A full
mutation audit, version-exhaustion policy, retained query ownership and cleanup
are required before relying on version validation. No version field has been
added speculatively in this stage.

## Verified results

Normal Build WOS and the isolated diagnostic build passed. Scoped formatting,
WKI and waitpid source suites, the credential policy audit and its unit suite
passed. The expected completion gate still rejects the pending signals domain;
the manifest remains 17 enforced / 4 pending.

Freshly packaged live KTEST passed **59,838 assertions, zero failures**, including
both new ChildEvents queue-inspection/candidate-merging tests and existing local
group/claim tests. The new tests exercise actual child publication, tracer
ownership, pending and claimed events, group changes, WNOWAIT, exact CPU accounting
and lifetime restoration. Synthetic namespace collisions leave any-child claims
usable; these are in-kernel lifecycle tests, not a cross-node wait syscall test.

The diagnostic guest reported selftest poweroff at 485.749 seconds. Exact owned
VM2953094 was verified absent, with no matching WOS VMs, at `2026-09-16T19:17:10.576741+00:00`.
Normal build ID: `f4d5009ce4ef733702ed4648867ae270a69617aa`; SHA-256: `e10af9c2d606753abc0e4bf0361f62dc281ec27d0e7c60f644f7cfa9054b6aca`.
Diagnostic build ID: `e2cc5c7e9ba95959d0be8b6236c0bef4235023b4`; SHA-256: `3d038255b527c013981dd14e3ce5e4d137176a777f30c49b5170d5d91d166094`.
The source hashes and diagnostic boot manifest match the tested build.

Initial formatting checks caught line wrapping and include ordering; both were
corrected before final builds and live qualification. No build or runtime test
failed. No normal-cluster or peer-failure checks were run in this stage. No wire
message, distributed client or EOVERFLOW syscall behavior is enabled yet. The
four previously observed migrated-parent wait failures remain unresolved, and
Goal19 stays active. Evidence: build/goal19-child-queue-inspection/validation.json.
