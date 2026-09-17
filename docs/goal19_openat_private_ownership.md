# Goal 19 OPEN_AT private ownership — 9MH

The retained-directory OPEN_AT caller allocates an unpublished File, reserves
its descriptor, prepares its compatibility path and attaches its mount pin
before entering remote VFS. Previously the File's unique owner, newly allocated
RemoteFileContext and proxy open-reference guard all depended on stack
destructors across the RPC. Fatal task exit can abandon that stack. Scalar OPEN's
tracked preparation did not cover this separate entrypoint.

PinPreparation now supports that caller-prepared File. Proxy admission still
precedes context allocation, preserving ENOTCONN/ENOMEM ordering. The File and
proxy reference enter the intrusive task-owned list before allocation and RPC.
Normal failure releases the context/proxy reference and leaves the File with
the caller. Success attaches the context to that exact File and transfers its
proxy reference. Actual task-exit cleanup takes ownership of the abandoned
unpublished File, then calls the real VFS final release outside the registry
lock so its path and mount pin are released as well. Repeated cleanup is inert.
Existing scalar/directory/executable preparations retain their prior behavior.

OPEN_AT also stops holding the retained directory's I/O mutex across this wait.
It reads only the directory's immutable proxy/FD identity; the caller's retained
File already protects that lifetime. It does not access directory cache or
read/write state. Credentials still come from one supplied Snapshot; original
directory identity, declared routing, backend admission and wire formats stay
unchanged. The public internal header now states the prepared File's unique
heap ownership and the distinction between ordinary return and stack abandonment.

## Verification

Normal Build WOS, isolated diagnostic build, scoped formatting/tidy and selected
source/credential gates passed. Initial formatting and an old cleanup-call
source assertion failed; those logs are retained. The source check now requires
explicit abandonment and release after unlocking. The final diagnostic build
follows the formatting correction; the later host-test adjustment changes no
kernel source or binary.

The new KTEST exercises proxy-before-allocation error ordering, allocation
failure without destroying the caller's File, ordinary return, successful
transfer, another task's surviving preparation and repeated actual task-exit
cleanup. It gives private Files heap paths and modeled admitted mount pins,
then checks the actual VFS mount-pin release. It performs no owner RPC and
does not claim to test complete syscall-stack unwinding. The new case passed at 312.529 seconds. The full isolated suite passed
**59,786 assertions, zero failures** at 474.745 seconds, powered off at
474.746 seconds, and its launcher exited 0. Diagnostic kernel:
`6c349d84275c3668c709f8eb0c74d75b8fee34b3`.

Both private normal nodes used the verified image containing kernel
`b0aa115c528e978cdb9f3550d5b1aa7309d452a2`. All **426 live checks** passed:
66 scalar OPEN cases and 180 OPEN_AT cases on each of TMPFS and XFS. All
fixture cleanup and guest sync succeeded. Both boot backing chains were
verified against the final image; both exact owned QEMU processes acknowledged
QMP quit and the cluster launcher exited 0. The host audit at
`2026-09-16T05:49:12.085860+00:00` found all owned processes absent and no QEMU.
Receipts and unchanged source/kernel hashes are recorded in
`build/goal19-openat-private-ownership/validation.json`.

The receipt review also corrected the preceding 9MG timeout annotation:
operation 1050 (0x041a) is FSTAT, as defined in local `wire.hpp`; the serial
log supplied only the number. This does not change the unresolved timeout
or establish its cause.

## Remaining owner lifetime work

This is a prerequisite for durable operation cleanup. The receiver may still
have published an ordinary FD when task exit or timeout discards its reply.
That FD needs a client-known, non-reused operation token and cleanup ownership
reserved before mutation. Scalar OPEN, OPEN_AT and final close must use the
same complete lifetime contract. Existing pin cancellation provides a bounded,
exact-binding retry precedent, but sharing its storage requires distinguishing
ordinary open purpose and preserving existing pin diagnostics and semantics.

Owner request workers are FIFO-sharded by peer/channel; cancellation must use
the same exact binding. Reliable sequence cookies are only 16-bit in replies
and cannot serve as durable tokens. Unsupported peers must be refused before
mutation. The detailed next-stage map and unresolved design checks are in
`build/goal19-openat-private-ownership/owner-cleanup-next.md`.

Goal 19 stays active with all 21 domains present: 13 enforced and 8 pending.
The historical OPEN_AT timeout, including 9MG's initial TMPFS failure, remains
unexplained and is not claimed fixed by this ownership change.
