# Goal 19 OPEN reply admission — 9MG

The current scalar remote OPEN sender emits three complete reply forms: the
descriptor/directory/stat-presence prefix, that prefix plus Stat, or Stat plus
a positive bounded prefetch length. The client previously used generic bounded
copying without requiring one of those forms. A reply containing only the FD
could therefore be accepted while the missing metadata retained defaults.
This finding comes from source review; no live network-input experiment was run.

OPEN_AS and OPEN_READ_AS now require exact outer framing and one complete
current reply form before copying into the pending caller's output. Negative
errno replies must be empty, positive statuses are refused, descriptor and
boolean fields are checked, and stat/prefetch presence must match the form.
Prefetch describes a non-directory and stays within the existing bulk limit.
Invalid matched content completes with EPROTO and zero output bytes. Outer
framing mismatch remains ignored, matching the other strict WKI operations.
Neither refusal sends CLOSE for an untrusted descriptor nor repeats OPEN.

The shared response handler also had a duplicated status-publication condition:
directory-reference and directory-grant validation could set EPROTO and then
overwrite it with the reply's status. All existing and new content-validation
failures now feed one rejection condition used both before copying and before
status publication. Exact channel generation, operation and cookie matching,
waiter ownership, valid errno, credential snapshots, declared VFS routing and
wire layouts are unchanged. The validator allocates nothing and introduces no
blocking or lock acquisition in RX.

## Verification

Normal Build WOS and isolated diagnostic builds passed. Scoped formatting/tidy
and selected source/credential-policy checks passed. Initial formatting errors
and tests expecting the old condition verbatim are retained in the stage logs;
their replacements check the complete shared rejection condition and ordering.

The new diagnostic test calls the actual reply handler in memory before WKI
initialization, using a private proxy and no transport sends. It covers both
scalar opcodes, all three success forms, bounded incomplete/extra lengths,
capacity refusal, invalid descriptor/boolean/prefetch fields, unchanged output
on refusal, stale cookie/generation matching, valid negative errno and retained
directory-reference/grant protocol errors. It passed at 314.068 seconds. The
full isolated suite passed **59,785 assertions, zero failures** at 477.440 seconds,
powered off at 477.441 seconds and the launcher exited 0. Diagnostic kernel:
`d8e7398b323c0aad35d4b25547cfe79153ee76b6`.

Both private normal nodes booted the verified image containing kernel
`08ab318942127404bbdebcf8149e5dff2c66e2ee`. All **66 scalar OPEN checks** passed.
The first TMPFS OPEN_AT invocation timed out after setup/mutation; the guest
logged FSTAT operation 1050 waiting 60 seconds. Fixture cleanup succeeded,
and subsequent process inventories proved the fixture absent on both nodes.
The XFS invocation then passed all 180 checks, followed by one TMPFS repeat
passing all 180. These are **426 successful checks plus one unresolved initial
timeout**, not a claim that every invocation passed or that the timeout was
caused/fixed by this patch. The same historical availability uncertainty remains. The 9MH receipt review
corrected the earlier PATH_AS label: local `wire.hpp` defines operation 1050
(0x041a) as FSTAT; the original serial log records only the numeric opcode.

All successful fixture cleanup and final guest sync completed. Boot backing
chains were checked against the final image before exact-process QMP shutdown.
The cluster launcher exited 0. Host audit at
`2026-09-16T05:19:59.791319+00:00` found every owned process absent and no QEMU.
Current source/kernel hashes and complete receipts are in
`build/goal19-open-reply-admission/validation.json`.

## Remaining lifetime requirement

The review also confirmed a separate ordinary-open cleanup gap. Task-exit
cleanup clears the pending operation's reply identity and drains private client
storage. An owner may already have published its File; a later unmatched reply
is discarded. Owner stale-FD collection deliberately skips connected peers.
Thus releasing client allocations does not establish eventual owner descriptor
retirement. OPEN_AT has the same unknown-result problem; ordinary close also
has transport-failure lifetime obligations.

The next change needs a client-known non-reused operation identity and cleanup
ownership reserved before any mutating request, coupled to exact owner binding
and task-exit handoff. Existing executable/directory cancellation outboxes are
the relevant local precedent. The design must cover unsupported-peer admission,
ambiguous replies, cancellation ordering, successor isolation and final close
without replaying create/truncate or closing an untrusted reply FD. The next
source map is `build/goal19-open-reply-admission/ordinary-open-lifetime.md`.

Goal 19 remains active with all 21 domains present: 13 enforced and 8 pending.
This stage does not qualify the whole open/create domain or claim historical
transport timeouts fixed.
