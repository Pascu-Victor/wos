# Goal 19 ordinary open owner lifetime — 9MI

Historical receiver stage. The subsequent [9MJ client cutover](open_lifetime_client.md) advertises and uses this protocol.

A receiver can publish a File before its OPEN reply is discarded. The client
then lacks the returned descriptor needed by legacy CLOSE. This stage implements
the receiver half of a client-known token contract; it does not yet switch or
advertise production client support. Unknown-result cleanup remains pending.

The new versioned OPEN_LIFETIME wrapper carries the existing scalar/read or
retained-directory OPEN request, an operation token, an owner-issued binding ID
and owner boot epoch. A binding query echoes a client challenge and returns the
binding identity. IDs never repeat within an owner boot and exhaustion refuses.
Local channel generations are not carried in legacy requests. Current channel
allocation also prevents dynamic-channel reuse within an authenticated channel
space; the new wrapper adds its own explicit lifetime identity. The successor
tests do not demonstrate acceptance of an old frame by the live transport.

The existing exact-binding pre-ACK admission and peer/channel FIFO worker own
the wrapper. The owner retains a highest consumed token per binding: OPEN must
advance it, while CANCEL retires only its exact token and binding, or succeeds
idempotently if absent. CANCEL of an unseen token prevents a later OPEN from
resurrecting it. No unbounded canceled-token table is needed. Clients must assign
tokens in actual per-proxy send order, after acquiring the operation slot; a
counter assigned earlier would be incorrect under concurrent callers.

Existing credential decoding, operation permissions, retained-directory authority,
backend admission and declared VFS routing remain in the existing inner handlers.
Both ordinary owner publication paths store token and binding ID in their
preallocated descriptor row. Cancellation extracts that row under s_vfs_lock
and releases its File after unlocking. Already admitted users retain independent
references; a close error does not restore the row or permit a second close.
Executable and directory pin identities remain separate. Legacy wire forms and
syscall ABI are unchanged.

## Verification and limits

Host tests exercise wrapper framing, query challenge matching and policy parity.
The actual owner KTEST covers unknown-before-open cancellation, duplicate/old
OPEN rejection, independent successor bindings, wrong wire binding/boot identity,
retained reader lifetime, close-error idempotence, retirement and maximum tokens.
It uses private in-memory state, not live packet injection. Source checks cover
the two publication paths and ordered admission wiring.

Final Normal Build WOS and isolated diagnostic builds passed. The final host
authorization suite, scoped formatting/tidy and selected source/policy gates
passed. The final KTEST passed **59787 assertions, zero failures** at
467.814 seconds; the new owner case passed at 307.797 seconds. Automatic
poweroff followed at 467.815 seconds and the launcher exited 0.
Normal kernel: `5bf07be2099fe9d7f3f878f6d66a65b1d4c92d02`.
Diagnostic kernel: `649a30be9e3e9e1da10bad1322099eb5425a33de`.
The packaged diagnostic kernel hash matches the recorded binary. Exact host
process audit at `2026-09-16T06:23:46.250354+00:00` found all owned VMs down and no QEMU.
No live multi-node wrapped-RPC test is claimed: clients have not switched yet.
Receipts are in `build/goal19-open-lifetime-owner/validation.json`. The first test
run used a superseded 16-byte draft without the owner-issued wire binding; it
cannot qualify the final 32-byte contract. Initial source assertions requiring
the old publication call text were updated to require the token/binding arguments.

FEATURES does not advertise this protocol, and existing production clients still
use their legacy operations. Client binding query, send-order token assignment,
pre-reserved durable cancellation, strict reply admission and final-close/task-exit
ownership are required before cutover. The concrete integration map is
`build/goal19-open-lifetime-owner/client-cutover-next.md`. This stage is not a
claim that lost OPEN replies or writable close ambiguity are resolved.

Goal19 remains active: all21 authority domains remain13enforced/8pending.
Historical timeouts remain unexplained.
