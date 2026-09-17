# Goal19 9NO: child-query transport

Previous turn: progress. 9NN qualified retained query records with60029/0 KTEST;
all owned VMs stopped. Goal remains17 enforced/4pending, first signals.

## Reconnaissance and patch contract

Problem: retained child queries have no wire dispatch or fair retry driver.
Expected files: wire.hpp, remote_compute.cpp/.hpp, child_query.hpp, wki.cpp,
auth_policy.cpp, remotable.cpp, WKI KTEST/source guard and progress documents.
Files read: those entrypoints plus peer timer, Task/session definitions, child
events, existing deferred RX admission/workers, resource advertisements and
query/claim ownership. Applicable WKI, kernel, critical-path, build and testing
instructions govern this patch; explicit user authorization permits live debugging.

Flow: authenticated reliable resource-channel RX -> fixed deferred slot -> strict
versioned decoder -> retained exact-exec query adapter -> cached value reply.
Upstream actions reserve stable identities and retry on a rotating bounded scan,
outside every incoming peer lease. Incoming replies bind exact session, request,
operation and generation before advancing a record. Pending replies never become
empty successful discovery. CLOSE retries retain storage through ACK/session loss.

Invariants: no child claim/reap/CPU charge, credentials or syscall changes. No
pointer/channel/session epoch is serialized. Immutable arguments, accepted owner
pins, queue versions, physical group origins and no-reuse tokens remain intact.
Old peers are refused before an upstream action can be issued; existing exec
placement compatibility remains unchanged. Query advertisement is separate from
the still-unimplemented complete wait service.

Locks: raw RX copies under channel and bounded queue locks only. Workers hold one
peer lifecycle admission for local effects or one generation-bound send; no second
peer lease, recursive send/wait or usercopy crosses it. Compute serialization is
dropped for lifecycle snapshots. Final record retirement stays deferred.
Unsafe contexts: no allocator, blocking wait or logger is added to raw NAPI/RX.
Workers can sleep between retry deadlines outside locks and peer leases.

ABI/wire impact: two new versioned query messages and one compute resource advert;
strict packed sizes, reserved fields, flags, selectors, origins, reply correlation
and version checks. No syscall ABI change or waitpid activation.
Build/test plan: decoder/dispatch/policy/correlation/retry KTEST on real private
Tasks and session rows with synthetic packet delivery; normal and isolated builds,
scoped format/source/policy checks and fresh live KTEST. Real multi-node query and
wait behavior remains to qualify once an initiating client exists.
Rollback: exact pre-stage files under build/goal19-child-query-transport/before.

## Implementation

TASK_CHILD_QUERY_REQ0x65 and RESP0x66 use fixed76/120-byte packed records. Responses
echo the full request, including generation, flags and physical origins. Pending,
error and closed replies carry no successful discovery data. Canonical re-encoding
rejects inactive-field garbage; semantic checks bound origins, flags, hop depth,
PID/group candidates and queue versions. Both messages require COMPUTE plus
IDENTITY_SAME_DOMAIN; retained admission also checks credential capability.

Raw reliable RX only validates and copies into existing fixed storage. An explicit
private reply-pending bit distinguishes an outgoing cached response from an incoming
response, preventing response echo loops. Workers capture authenticated sessions,
execute bounded local query work and retain response bytes across send retries.
The outgoing driver runs before incoming peer admission, rotates among64 records,
reserves one action per10ms deadline and sends on the exact channel generation.
Application retries preserve the logical request; reliable enqueue is not query
completion. PENDING replies leave the existing retry deadline in place. New phases,
including cancellation, bypass the old phase's delay with a new action generation.
Workers sleep1ms while queries await progress and otherwise use ordinary parking.

The compute resource process-child-query-v1 (0x53494707) advertises discovery only.
No submission preflight or syscall caller changes. Before its first relay action,
a source checks the upstream advert and caches ENOTSUP if absent, so cancellation
can reclaim that record without inventing an upstream acknowledgment. Private
KTEST fixtures explicitly inject upstream support to test session/action handling
without a resource-advert registry; the normal packet entry always checks support.
Numeric group aliases must complete exact-origin PROBE before VALIDATE.

Initial source validation failed because clang-format split the existing worker
parking condition across lines. The check now normalizes whitespace while retaining
the full condition, and additionally checks that query sending precedes incoming
peer lifecycle admission. The failed receipt remains as wki-source-initial.*.

A second source guard assumed the first send in the whole worker was a signal
response. Query replies now precede that branch. The unchanged signal decision/
send invariant is checked within the signal branch; wki-source-second.* preserves
the failed receipt. These are source-pattern corrections, not ignored failures.

## Validation scope

The wire KTEST uses production encoders/decoders and RX classification to check
length/version/reserved/flag/origin bounds, canonical pending/error/close forms,
summary depth/version consistency and both required policy scopes. The retained
ownership fixture now calls the actual packet entry for a leaf and an unsupported
upstream, then drives two simultaneous supported records through encoded pending,
tampered, valid and duplicate replies. It checks deadline-based reissue, rotating
slot selection, full-chain validation and close receipts without claims or CPU
charges. Existing cancellation/session-loss cases remain. Upstream resource support
and packet delivery are injected; no real network roundtrip is claimed.

Normal-kernel query-record storage is40,448 bytes, an increase of2,560 bytes for
retry/action state; the per-session watermarks are unchanged. No dynamic query
allocation, new syscall number or client-side wait ownership is introduced.

## Final qualification

Normal Build WOS, isolated diagnostic build, scoped formatting, WKI/waitpid
source suites, host wire/auth-policy tests and policy audit/unit checks pass.
Freshly packaged live KTEST passes60030/0, including all existing relay cases,
retained-ownership/claim tests and the new wire case. Source/image hashes match.
Diagnostic VM3296977 powered off; no matching WOS VMs remained at2026-09-16T21:14:00.738663+00:00.
Two stale source-pattern assertions were corrected with failed receipts retained;
no kernel build or runtime failure occurred in this stage. No normal-cluster or
real multi-node query delivery was exercised. Query transport is enabled and
advertised, but no initiating waitpid client or distributed claim/commit transport
is enabled. The four migrated-parent wait failures remain unresolved. Native
waiter/fatal-exit cleanup, notification and imported CPU accounting remain open.
Goal remains17 enforced/4pending, first signals. Evidence:
build/goal19-child-query-transport/validation.json.
