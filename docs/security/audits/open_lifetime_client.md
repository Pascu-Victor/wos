# Goal 19 ordinary OPEN client lifetime — 9MJ

Ordinary scalar OPEN, OPEN_READ and retained-directory OPEN_AT now use the
owner lifetime protocol introduced in 9MI. A client first requires the new
`WKI_VFS_FEATURE_OPEN_LIFETIME` feature, queries the exact owner binding with a
challenge, and validates its boot epoch. Unsupported receivers refuse before
create or truncate. The owner advertises the feature on incarnation-capable
bindings. Existing credential mapping, retained directory authority, declared
VFS routing and syscall layouts are unchanged.

A separate bounded 1,024-slot cancellation outbox reserves an independent proxy
reference before mutation. `PinPreparation` owns that reservation until failure,
task exit, or successful transfer to the published File. The operation token is
assigned after acquiring the exact proxy's operation slot, so concurrent callers
cannot send tokens in reverse order. Exhaustion never wraps or reuses a token.
Cancellation identifies the owner binding, boot epoch and token; it does not
trust a returned numeric descriptor. Ambiguous mutating requests are not replayed.

The response handler validates the expected inner operation before copying:
complete scalar responses with positive descriptors, exact OPEN_AT header and
result, challenged binding queries, and empty cancellation/error replies. Stale
sequence/generation responses remain ignored. Invalid matched responses preserve
the destination and report `EPROTO`.

Final ordinary close is tracked through flush and cancellation waits. Writable
close waits for cancellation and preserves a prior flush error. Readonly close
hands off to the durable queue. Fatal-task cleanup queues retirement and finishes
the abandoned File with `ECANCELED`; it never replays an interrupted flush or
claims that buffered bytes were committed. Successful cancellation, a request
that never received a token, or proven exact binding retirement releases the
outbox reference. Other outcomes retain it for the bounded deferred worker.

Shared RDMA staging-slot guards also have intrusive task ownership. Cleanup
quiesces proxy waiters, releases these slots, then drains prepared or closing
Files. An abandoned guard becomes inert and cannot release a successor's slot.
File, cache, heap path, mount and proxy releases happen outside the registry
lock. This does not establish general cleanup of every syscall-stack resource.

## Verification

The actual client KTEST exercises full outbox refusal, pre-send release, token
exhaustion, task-specific preparation cleanup, retained cancellation after a
transport error, proven retirement, shared-slot reuse, abandoned final File
cleanup, normal readonly/writable close, flush-error precedence and cancellation
slot reuse before the old close stack unwinds. The actual response-handler test
checks valid and invalid wrapped forms, query challenge, destination preservation,
capacity, trailing bytes, stale sequence and stale generation. These tests use
private kernel state and the production cleanup/admission helpers.

The live fixture `tests/fixtures/service_wki_open_lifetime.cpp` checks child exit
with an open writable descriptor, visibility after wait, and more readonly
opens than the outbox capacity. It uses ordinary filesystem operations in an
owned test directory. It does not interrupt transport or establish the outcome
of a lost mutation reply.

Normal Build WOS, the isolated diagnostic build, host authorization/source/policy
checks and scoped formatting/tidy passed. Full KTEST passed **59,788 checks with
zero failures** at 475.150 seconds, followed by automatic poweroff and launcher
exit 0. The new client case passed at 312.054 seconds; wrapped reply admission
passed at 312.078 seconds. Two private nodes passed 426 existing scalar/OPEN_AT
checks and the new 48 exit-flush / 2,200 readonly cycles on TMPFS and XFS, with
zero backpressure retries. Each exit iteration uses distinct data. Both guests
synced and acknowledged exact QMP shutdown; the launcher exited 0. Host audit
at `2026-09-16T07:02:58.347168+00:00` found all three stage VMs absent and no QEMU.

Normal build ID: `308c35f4db6dd0d98d300e73e1a96c7cd3d1e1af`.
Diagnostic build ID: `588e42faa3515a325932e0af168575ddcb225fb2`.
Both boot manifests match their recorded kernel hashes; cluster overlay chains
resolve to the private current boot image. Exact identities and receipts are in
`build/goal19-open-lifetime-client/validation.json`. Initial source checks had
stale unwrapped-envelope/helper expectations. The successful constant-data
fixture draft is archived separately; final visibility evidence uses distinct
per-child payloads. Goal19
remains active; open/create and the other outstanding domains retain their
separate authorization/commit qualification obligations. Historical timeouts
remain unexplained.
