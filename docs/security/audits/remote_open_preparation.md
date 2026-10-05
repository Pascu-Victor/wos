# Goal 19 scalar remote-open preparation — 9MF

Scalar remote OPEN previously performed two kinds of required allocation after
the owner could create or truncate a file: owner descriptor-map insertion, and
client File/context allocation. An allocation failure could therefore follow
the filesystem effect. The owner map insertion also allocated while holding
the VFS registry lock. This ordering was established from source; no live
memory-exhaustion experiment was performed.

The owner now uses the same `RemoteFdReservation` as OPEN_AT. It prepares map
storage outside the registry lock and reserves an invisible row before calling
the backend. File metadata and optional prefetch still precede publication.
Publication checks the exact channel identity and refuses a retired row; a
failed publication closes the private File outside the lock. Failed reply
cleanup retains its previous exact-identity behavior. No File is dereferenced
after publication makes it available to peer cleanup.

The client now uses the existing task-linked private preparation mechanism for
ordinary opens as well as retained pins. Required File/context storage and its
proxy open reference exist before the request. A long request's heap buffer
belongs to the same preparation. Task exit first quiesces proxy waiters, then
detaches private ownership under the registry lock and frees it outside the
lock. Successful publication transfers File/context/proxy ownership together;
the preparation still frees its temporary request buffer. The short request
remains stack-local. Optional prefetch-copy allocation can still fail without
turning a successful open into a failure.

Mapped credential Snapshots, access-mode checks, declared VFS routing and wire
layouts are unchanged. Existing default client `EIO` on private allocation or
proxy admission failure is preserved. Owner descriptor exhaustion remains
`EMFILE`; a retired reservation refuses publication with `ENOTCONN`, as OPEN_AT
already does. A transport failure after an authorized mutation can still have
an ambiguous outcome. This change does not make such requests safe to replay.

## Verification

Normal `Build WOS` and isolated diagnostic builds passed. Scoped formatting and
clang-tidy checks passed with existing nonfatal warnings. The remote-VFS source
checks now enforce reservation and private allocation before the backend/RPC,
while retaining checks for exact identity, prepublication metadata, failed-reply
cleanup and no File dereference after publication. Remote device-server,
retained lookup, credential policy and policy-unit checks also passed.

Both private normal VMs used kernel
`bd27f414ff020af03ade6b408c124e0de3968652`; their boot overlay chains were checked
against the final image. All **66 scalar OPEN checks** passed across TMPFS and
XFS, covering creator IDs/mode, contents, truncation, group and foreign access,
denied create/truncate, symlink/search policy, and long requests that use the
heap buffer. Existing retained-directory OPEN_AT fixtures passed another
**360 checks**, including descriptor-limit refusal before mutation and unchanged
contents after denied truncation. All fixture objects were cleaned up; both
guests completed sync before verified private-VM shutdown.

The new diagnostic test injects failure at each required client allocation,
checks released partial ownership, exercises actual task-exit cleanup and its
idempotence, preserves another task's preparation, and checks successful
ownership transfer. Injection is confined to WOS_SELFTEST and a synthetic task
ID. Existing owner reservation tests cover invisible rows, exact-generation
publication and retirement refusal. Both named tests passed. The full isolated
suite passed **59,784 assertions, zero failures** at 473.930 seconds and powered
off at 473.931 seconds; the launcher exited 0. Diagnostic kernel build ID:
`469478c3f9f87b7ce7da43ab535de9d174af5637`. The final host PID/start-time audit
at `2026-09-16T04:55:29.555134+00:00` found every owned process gone and no QEMU
VM running. Full receipts and unchanged source/kernel hashes are recorded in
`build/goal19-remote-open-preparation/validation.json`.

Initial formatting and source checks failed on changed layout and references to
the removed allocation path. Their logs are retained; checks were updated to
assert the new ordering without dropping the lifetime requirements. A host
shutdown scan initially encountered a protected unrelated `/proc` entry and
stopped before any QMP command; the retry skipped inaccessible entries and
verified each private VM's actual disk arguments, PID/start time and QMP path.

This is one open/create requirement, not qualification of the entire domain.
Goal 19 remains active with 13 enforced and 8 pending domains. The next review
must reconcile backend commit/error semantics and the remaining request and
publication lifetimes against the full operation contract. Earlier transport
timeouts remain recorded and are not claimed fixed here.
