# Exec file admission and byte stability

Goal 19 remains incomplete. TMPFS/XFS executables retain private content views,
and remote inputs use versioned owner-held views. Cancellation ownership now
survives failures, and live request/response interruption recovery is verified.
Owner-restart lifetime and mount recovery are verified. Consumer restart and
same-boot disconnect have passing TMPFS/XFS lifetime cases, but an intermittent
remote executable-fetch timeout remains unexplained. Exec authorization is
qualified by the final9LT checklist below; general VFS/WKI work remains pending. Raw direct-memory RDMA stays disabled under authenticated WKI v3;
it is not a supported execution path awaiting qualification.

## Current authority audit (9LT)

The current requirement checklist is in
[the exec authority audit](goal19_exec_authority_audit.md). Private candidate
metadata, saved-ID/set-ID transitions, secure auxv, interpreter admission,
one-shot commit, signal handoff and finite stream output/read work have later
qualification records. See [remote output completion](remote_exec_output.md)
for the current output evidence. The former 9ED statement that no output fix
existed was stale; it is superseded by those records.

Exec authority is enforced after9LR receiver final admission/private cleanup
and9LS committed-private source retirement with a retained remote File. The
9LT requirement reconciliation records the scope and limits of that decision. The historical 9FZ completion observation and 9LG
fresh interpreter-fetch timeout remain unexplained; the later passing runs do
not establish their cause or a fix. This audit does not require implementation
of every unsupported transport feature before assessing Goal 19 authority.

## Consumer retirement and confirmation retry (9EC)

Consumer-restart qualification exposed a WKI confirmation retry defect. Bounded
live WOSDBG reads showed the owner CONNECTED with an active session and TX
counter 696, while the consumer remained UNKNOWN with the matching pending
session. Both lifecycle and VFS-rebind gates were clear. HELLO traffic refreshed
owner liveness timestamps; this did not prove reciprocal authenticated heartbeat
progress. No key material was read.

Pending HELLO_CONFIRM verification required counter 1, but the sender reserves a
new session counter for every retry. Losing the first opportunity, or another TX
reserving it first, therefore stranded the responder. The captures do not prove
which event consumed the first opportunity. Verification now accepts a nonzero
counter for the exact pending session after its normal directional-key tag check.
Promotion under exclusive peer lifecycle rechecks the verified trailer identity
and seeds the replay window with the actual confirmation counter. Zero counters,
stale pending identities, and duplicate promotion fail without partial mutation.
Existing key erasure, permission policy, credential mapping, syscall ABI and wire
layout are preserved; the sender is unchanged.

Normal Build WOS and the isolated diagnostic build pass. Ten focused host suites,
the authority audit (zero documented legacy root checks), formatting and the
applicable clang-tidy checks pass. Existing compiler/tidy diagnostics remain.
The private-peer KTEST covers first/delayed counters 1 and 81, unchanged state on
rejection, key transfer/erasure and replay behavior; it passes at 36.111 seconds.
Full isolated KTEST passes 51,888/0 at 151.563 seconds and exits normally.

The native fixture prepares a private owner image, admits it on consumer wos-3,
checks ETXTBSY for that image and an independent local control, then resets only
the validated consumer VM or temporarily lowers its WKI link. Passing TMPFS and
XFS restart/disconnect runs each observe owner views 0 -> 1 -> 0, successful owner
writes after retirement, continued ETXTBSY on the local control, an old command
status of 255, changed session epochs, replacement bindings, and a fresh consumer
reading `released-image`. Disconnect cases preserve the consumer boot epoch.
All 20 existing remote content-stability cases also pass on the recovered mounts.

There is no clean four-case matrix claim. The first candidate fixture incorrectly
compared only channel number and pool-slot generation; valid reuse across session
epochs triggered it. The fixture now checks the session epoch transition. In the
corrected matrix, both TMPFS cases passed, but XFS restart's fresh exec returned
ETIMEDOUT after 60 seconds while the receiver fetched the helper over remote VFS.
The original command was observed until its actual terminal status, without
resubmission. A bounded XFS restart reproduction passed in 0.10 seconds, and the
independent XFS disconnect case passed. The intermittent fetch timeout is still
unexplained and remains a qualification gap. Read-only GDB snapshots after that
failure showed idle compute workers and empty queues; they do not establish the
cause of the earlier wait. The first GDB observer needed a bounded string-decoding
correction and detached on both attempts.

A final remote `cat /proc/wki/netdiag` returned status 0 but only 83,348 bytes,
ending mid-row without the completeness marker. That output is retained as an
incomplete observation; the owner-side capture is complete. Remote output
completion is a separate unresolved diagnostic limitation.

All task VMs are stopped: baseline 945902/945892, KTEST 1024843, candidate
1027120/1027122. Both candidate guests were synced before validated QMP quit, and
all five PIDs were confirmed absent. Baseline pre-stop sync/descriptor archival
failed due a host Paramiko context-manager mistake before the subsequent QMP
stop; no graceful baseline sync is claimed. Candidate and KTEST descriptors,
full logs, passing cases and failed observations are retained under
`build/goal19-exec-consumer-loss`.

Normal kernel: `4f211eb0355c51f3ffd80f7bd32444b29ea42fe12258f7f2bd9965503f4eca1e`.
Isolated kernel: `a12047fb7a563bb7cd6f1df65e0e2a836a27b48a616f762299d2f9521378618c`.

Goal 19 remains active at 6 enforced / 15 pending. These are ordinary OS
implementation and functional lifetime checks, with no penetration testing.
The exact loaded goal and the appended `--no-setup` authorization are unchanged.
Next: capture the intermittent remote executable fetch while it is pending,
then finish the remaining exec and authorization qualification audit. This turn
made verified progress; there is no indispensable external blocker.

## Retired mount recovery after owner restart (9EB)

A live owner restart exposed a stranded remote mount. The original exact unmount
returned EBUSY while an executable view and ordinary descriptor still pinned it.
After both retired, the old anchor remained published with zero open/lifecycle
references and completed resource cleanup. Replacement auto-mount repeatedly
returned EBUSY. Cancellation had already converged, so this was a separate mount
retirement gap, not a reason to reopen the old binding.

The auto-attach worker now retries exact retired mount owners before admitting
replacement mounts. A rotating scan inspects at most32 rows and attempts at most
one unmount per second. A lifecycle reference protects the selected anchor across
VFS teardown outside the registry lock. Only success or exact-owner ENOENT marks
the mount released. Live File pins still return EBUSY; there is no force-unmount,
pathname-based teardown, stale-proxy reactivation, or syscall/wire change.

The native regression retains an executable view and descriptor while the host
restarts only the descriptor-validated disposable owner VM. Both TMPFS and XFS
pass: a concrete new boot epoch is observed, stale pread returns EIO, the old
child exits with SIGSEGV when its backing image becomes unavailable, cancellation
ownership drops from1 to0, the exact replacement mount becomes ready, and fresh
exec reads the replacement marker. Retirement handoffs advance0->1->2. All20
existing remote content-stability cases also pass on the recovered mounts.

The initial fixture incorrectly attempted to recreate the file while live pins
still legitimately prevented unmount, and accepted an unrelated ready proxy.
The corrected protocol closes old references first and requires readiness of the
exact requested mount before its fresh-exec control. The baseline diagnostic
snapshot independently proves the old mount remained stranded after all old
references and cancellation ownership were gone. Both captures are retained.

Normal and isolated builds pass. Eight focused host suites and the authority gate
pass; formatting/clang-tidy complete with existing diagnostics. The actual-mount
KTEST proves busy retry, exclusive retry ownership and preservation of a replacement
at the same path (PASS35.988). Full isolated KTEST passes51831/0 at150.753 and powers
off150.754. All five task-owned baseline/candidate/KTEST VMs are confirmed absent;
the cluster nodes were synced before QMP stop. No unrelated VM was controlled.

Artifacts: `build/goal19-exec-peer-restart`. Native coordinator:
`tests/fixtures/service_exec_peer_restart.py`; compile its shared native fixture
`service_exec_file_stability.cpp` with `-fno-sanitize=safe-stack -fno-pie
-Wl,--no-pie`. It requires the live cluster descriptor and authorized SSH access.
Normal kernel:01fc4f7a160e1fb0a0b85fff145f5705046e692cdf2c38aa1f607efd4a231899.
Isolated kernel:104415f4b9ed9285446ebacebb9dcd839efce3f068052ecafaf80f0b4225a212.

Owner-restart executable lifetime and subsequent mount recovery are qualified.
Consumer-loss and same-boot channel retirement remain separate checks. Full Goal19
remains active at6enforced/15pending; these results do not complete all exec,
authorization or service-isolation requirements.

## Complete authenticated shared-memory frames (9DA)

The ivshmem message receiver used an 8,192-byte buffer, although a complete
WKI v3 frame can occupy 8,986 bytes. Larger frames were consumed from the ring
but truncated before authentication. The baseline native fixture timed out
before exec: CLOSE (0x0403) waited behind an unacknowledged WRITE (0x0402), with
1,295 retransmissions and zero executable-view attempts. Retained ring bytes
include 8,986-byte writes and 8,272-byte read replies.

The receiver now uses `WKI_MAX_FRAME_SIZE`; the transport MTU uses the same
capacity excluding the WKI header. Ring layout, fixed receive budget,
single-drainer exclusion, endpoint authentication/replay checks and allocation
behavior are unchanged. The extra receive storage remains on the kernel stack.
No direct-memory RDMA path is enabled.

Both builds, 17 focused host checks and formatting pass. The actual-ring KTEST
covers the old boundary, the full frame and wrapped positions and passes at
12.537 seconds. The full isolated suite passes 18,499 checks with zero failures
at 101.709 seconds, followed by automatic poweroff. On fresh ivshmem nodes all 20 remote and 22 local
content/lifetime cases pass with fixture cleanup. This verifies authenticated
message-ring execution. Diagnostics still report `rdma=0` and `bulk_rdma=0`.

Earlier notes listing raw RDMA qualification as outstanding were stale:
`peer.cpp` deliberately leaves `rdma_transport` null, as already documented in
[WKI transport rollout](wki_security.md#routing-rdma-and-transport-rollout).
Restoring direct-memory operations requires their own complete authentication
contract; it is not part of this frame-capacity fix.

## Executable-view support admission (9CZ)

VFS adverts now set `RESOURCE_FLAG_VFS_EXEC_VIEW` (0x20) for version 1 PIN
and idempotent CANCEL support. The existing ten-byte advert base and HELLO
layout are unchanged. Before allocating an executable File, token or durable
cancellation record, the consumer checks node, type, resource ID, local
generation, owner boot/incarnation and current flags under one resource lock.
A stale observation returns `ENOTCONN`; missing support returns `ENOTSUP`.
Flag-only updates take effect for existing mounts on their next exec admission.

The feature bit does not replace complete versioned reply validation or grant
file access. Existing mapped read admission, retained owner description,
channel fencing and cleanup ownership remain in force. The source File retains
the proxy identity; no resource lock spans allocation or an RPC.

Both final builds and 15 focused host checks pass. The real registry KTEST
covers old zero-flag adverts, irrelevant flags, support withdrawal, stale
generation/type/owner identity and invalidation. It passes at 12.176 seconds;
all 18,498 isolated checks pass with zero failures at 102.335 seconds, followed
by automatic poweroff. Current/current live checks pass all 20 remote and 22 local stability cases,
with cleanup. Adverts carry flags=32; final consumer counts show 20 successful
cancellations and no outstanding records or failures.

The current consumer also passes real exec refusal against the retained 9CY
owner, whose adverts carry flags=0. Both TMPFS and XFS return ENOTSUP (95) in
about 23 ms, retain unchanged image bytes, allow ordinary writes/fsync, and
clean all fixture files. Consumer cancellation counters remain zero and owner
pins remain at baseline. This older image already implements PIN/CANCEL; the
result establishes compatibility with a peer predating the support advert,
not a booted pre-PIN implementation. Artifacts are in
`/tmp/wos-goal19-exec-support-mixed-pass/`; reproduce with
`tests/fixtures/service_exec_support.py` and the native fixture's
`--unsupported-peer` mode.

Initial host qualification caught remote-command output truncation. Direct
remote PROCFS access is unsupported, so the final coordinator filters the
owner's diagnostic row and completeness marker on that owner. The remote
diagnostic command can itself execute an origin-host image; owner client
attempt totals therefore advance, while retained records and owner pins are
compared at baseline. Consumer totals remain unchanged. Failed initial
coordinator captures are retained separately; kernel code did not change.
Both clusters are stopped. Each peer was synced before validated QMP quit;
consumer rootfs unmounts were clean. No clean peer-unmount claim is made.

## Live cancellation interruption (9CY)

The two-node fixture now holds either cancellation requests or responses after
one remote child image is admitted. It uses the existing test-only WKI
injector on private boot/rootfs images; ordinary boot images keep their current
command line. Held retransmissions coalesce into fixed storage, preserving a
complete trace through the real 60-second RPC timeout.

All four cases pass: request and response interruption on owner TMPFS and XFS.
Each child exits normally after close times out, while the consumer retains one
pending cleanup record. Healing allows an idempotent retry to complete. The
record count returns to baseline, the same peer remains connected, no retirement
handoff occurs, and an explicit owner write/fsync plus fixture cleanup succeeds.
Observed timeout intervals are 59.978–59.981 seconds. Across the four cases,
counters advance through eight attempts, four failures and four successful
cancellations, with zero outstanding records at the final snapshot. Final
source and owner snapshots both show zero registered owner views.

`/proc/wki/netdiag` adds the read-only `wki_vfs_exec` row: capacity, owned,
pending, retrying, registered owner views, attempts, failed attempts, successful
cancellations and retirement handoffs. Counts use the existing registry lock;
there is no new authorization or wire surface. A zero registered-view count
alone does not prove a blocking backend close has finished; the fixture's
successful owner write supplies that evidence.

Both builds, 15 focused host checks, formatting and two diagnostic-consumer
checks pass. The existing parser accepts every captured snapshot. The full isolated
suite passes 18,497 checks with zero failures at 101.843 seconds and automatic
poweroff. These cases establish recovery after bounded
communication interruption on the current connected topology; they do not
substitute for peer replacement, RDMA stage lifetime or unsupported-peer tests.
Artifacts: `/tmp/wos-goal19-cancel-loss-run/`, including native output, complete
before/held/healed diagnostics, injector traces and per-case results. The
reproducible host coordinator is `tests/fixtures/service_exec_cancellation.py`.

## Durable remote cancellation (9CX)

Before PIN, the client reserves one of 1,024 fixed cancellation records and
retains an independent proxy reference. The record covers both the live view
and any subsequent cleanup retries. Exhaustion fails before sending PIN.
Failed PIN and final view close use the same reserved token; cancellation
transfer allocates nothing and never trusts a returned descriptor number.

Healthy close still waits for the owner response. Slot, send, timeout and
malformed-response failures retain the record after the File disappears. A
rotating retry runs on the existing client auto-attach worker. It uses immediate
slot admission that preserves queued callers, a 100 ms send/response wait bound
per phase, and a 100 ms retry interval. It never waits in RX or on the separate
protocol-service worker. Every request checks the original channel identity
under proxy-slot ownership before sending.

The record releases its reference after successful token cancellation, a
concrete obsolete peer boot epoch, or acknowledged exact DEV_DETACH admission.
The latter transfers responsibility to the owner's durable binding-retirement
path; it does not claim the blocking owner File close has already finished.
Proxy inactivity, local channel closure and an absent pending-detach flag do
not establish cleanup. No syscall or wire format changes.

Normal Build WOS, the isolated diagnostic build, 15 focused host checks and
formatting pass. New KTEST coverage exercises real immediate-slot failure,
retained proxy ownership, FIFO fairness, generation-specific retirement and
bounded reservation capacity. The existing real TMPFS owner-pin test now also
models response loss after actual owner cancellation and repeats that token.
The full isolated suite passes 18,497 checks with zero failures at 102.712
seconds and automatic poweroff. The same Normal kernel also passes all 20
remote and 22 local file-stability cases on two nodes, with complete cleanup. Those 9CX checks were not end-to-end transport evidence; section 9CY above
records the subsequently executed request/response interruption cases. Before 9CZ, unsupported peers that refused both PIN and CANCEL could retain
bounded cleanup records until binding retirement. Section 9CZ records the new
pre-allocation admission gate for peers without advertised support.

## Remote implementation (9CW)

`vfs_stabilize_exec_file` now dispatches local and remote inputs. Remote PIN
uses an already-authorized descriptor on its exact channel generation. It does
not reopen a pathname or use serving-worker credentials. The owner checks the
retained read grant, reserves descriptor storage before pinning, and clones a
private local TMPFS/XFS view. The captured metadata and independent descriptor
are published together with a client token. Repeating the same token/source
request returns that view; another source or channel cannot substitute for it.

`OP_VFS_EXEC_VIEW` (`0x041d`) uses a fixed 16-byte version/operation/source/token
request and a mandatory 160-byte PIN reply containing the captured Stat. Existing
HELLO bits and message layouts are unchanged. Only a valid versioned reply is
accepted; unsupported peers do not select an ordinary-read fallback. PIN uses
the existing read plus same-domain policy. CANCEL grants no new access and
releases only the matching channel/token view.

The client allocates its File, context and view before sending PIN. Its token
comes from the same nonwrapping local identity allocator as local executable
views, so remote and local cache entries cannot collide. Reply length is
checked before the generic response copier can truncate it; version, token,
descriptor and regular-file metadata are checked before publication. The clone
retains the exact client mount/nosuid policy. Native exec, spawn, interpreter
loading and boot-input cache reads use the common stabilization API.

Failed or malformed replies trigger token cancellation even if the returned
FD is absent or invalid. A normal final close waits for owner cancellation to
finish before releasing its local reference. The owner extracts registry state
under its lock, then closes the File outside the lock. Ordinary read-only
closes retain their previous asynchronous behavior.

A transport failure can also prevent cancellation itself from being queued or
acknowledged. The original 9CW attempt returned that error and depended on peer teardown
for eventual cleanup. Section 9CX above adds durable per-token ownership and
retry; complete end-to-end partition/loss qualification remains pending.

The four synchronized 9CV remote failures now pass on owner TMPFS and XFS:
writeback reports `ETXTBSY`, each child reads `admitted-image`, and writes succeed
after waitpid. The expanded fixture passes 20 remote cases, covering existing
and new writer descriptions, ftruncate, O_TRUNC, append, mmap plus fsync, invalid
ELF, missing interpreter, retained PROCFS descriptors and worker exit. Buffered
and mapped write denial is observed at explicit fsync; this does not establish
that msync alone waits for remote owner completion. Remote LINK is unsupported,
so hardlink creation remains in the 22-case local matrix, which also passes.

All 305 prior exec checks, 144 spawn checks, 36 remote nosuid checks and four
local/remote mount-reporting rows pass. Both builds, 15 focused host checks and
formatting pass. The owner KTEST exercises actual private pins, read-grant
rejection, exact generation, captured metadata, repeated PIN and independent
CANCEL lifetime. It passes in the isolated VM, whose full suite completes at
102.430 seconds with 18,496 passed, zero failed and automatic poweroff.

## Local implementation (9CV)

`vfs_stabilize_exec_file` replaces an owned local File with an independent
read-only description. The backend acquires a canonical inode content pin and
captures metadata under its existing mutation locks, before the loader reads
ELF bytes. The description retains the exact mount and nosuid policy. Main,
interpreter and spawn admission still use the existing request credential
snapshot; fstat of the private view returns its captured metadata.

TMPFS write/resize and XFS write/truncate reject conflicting content mutation
with `ETXTBSY` before data or allocation changes. Existing writable descriptors
may stay open. Hardlinks, append, O_TRUNC, content-copy destinations, mmap
writeback and WKI owner writes reach those backend guards. XFS close defers
preallocation trimming while contents are pinned. An active XFS swap extent
export excludes executable pins and ordinary file mutation until its retained
File closes; its block writes bypass ordinary file callbacks.

Each continuous pinned-content interval receives a nonzero, nonwrapping
identity. Lazy page-cache equality and hashing include that identity, keeping
ordinary cache entries and older executable intervals separate. The clone uses
uncached direct reads. Unrelated original or PROCFS-reopened descriptors do not
own this private view and cannot extend its lifetime.

Process and worker exit release their own lazy File references after usercopy
quiescence, while cleanup can still block, before waitability or worker CPU
retirement. Shared mmap propagation checks closing under the lazy-range lock
so it cannot refill a retiring task. Other executors retain their independently
owned references; canonical main-thread retirement still waits for workers.
Scheduler GC remains safe when the already-retired range list is empty.

The first candidate correctly preserved bytes but failed eight immediate
post-waitpid write controls because lazy references survived until scheduler
GC. Moving that release into active exit cleanup fixed the observed lifetime
failure. The qualified fixture passes 11 cases on each backend: existing/new
writer, hardlink, ftruncate, O_TRUNC, append, mmap sync, invalid ELF, missing
interpreter, retained PROCFS descriptor and worker-thread exit. Every successful
image observes `admitted-image`; conflicting mutation returns `ETXTBSY`. Writes
succeed immediately after waitpid, without retries, including with ordinary
FDs or mappings retained. Failed exec returns `ENOEXEC` and releases the view.
All uploaded binaries, copies and hardlinks were removed. XFS is `/` after
pivot_root; an initial invocation using nonexistent `/rootfs/tmp` failed setup
and is preserved separately from the successful XFS run.

The existing race fixture now pauses its credential mutator only during
initial/post-denial observations. A diagnostic run established that its own
readlink observer could return `EAGAIN` during churn with IDs and CLOEXEC intact.
Exec itself continues to race; state assertions are unchanged. The corrected
78-case run passes, including 13 denied attempts and six successful replacement
images. The other 227 exec checks, two mount-reporting checks and 144 spawn
checks also pass. Normal Build WOS, the isolated diagnostic build, 11 focused
host checks and formatting pass. The final isolated suite passes 18,495 checks
with zero failures and automatic poweroff. Its first run reported three
assertion failures because negative 64-bit write results were compared to
32-bit expected constants; the corrected expectations use `ssize_t`. No
production code changed for that correction.

FAT's current regular-file policy supplies no execute bits and is unchanged.
Other local backends retain their existing policy. An ordinary remote read
handle alone does **not** establish a stable execution view; the versioned
remote operation above must succeed before executable admission.

## Remote baseline after the local fix (9CV)

The same Normal kernel was run on two WOS nodes using the existing
`same-domain-root` test policy. The fixture's `--write-only` mode checks
existing and new writer descriptions and calls `fsync` before allowing the
child to read. All four remote cases fail on owner TMPFS/XFS: 14 bytes written,
sync succeeds, the child reads `modified-image`, and child exit and cleanup
complete normally. This establishes the remaining remote byte-stability gap.

The first probe omitted fsync and observed the original marker despite writes
returning success; client write buffering made that observation insufficient.
Both sets of logs are preserved. The synchronized probe also reran all 22 local
cases successfully with the final fixture binary. Remote nosuid compatibility
passes all 36 exec cases and both mount-reporting checks, with fixture cleanup.
Those observations preceded the 9CW remote implementation. Peer identity
policy remains unchanged.

All test VMs are stopped. The source unmounted its rootfs cleanly. The peer
refused the remote poweroff command; an ACPI request did not produce shutdown,
so its disposable VM was stopped through validated QMP. Its logs and overlay
are retained; no clean peer-unmount claim is made.

Current source reconnaissance identifies two additional requirements: all 16
HELLO capability bits are already assigned, so stable-read negotiation must
use a compatible extension or explicitly versioned operation; and ordinary
remote read-only closes are asynchronous, so an execution view needs an
explicit owner-release completion contract. Do not reuse an existing capability
bit or silently fall back to an ordinary unpinned read.

## Observed baseline

`tests/fixtures/service_exec_file_stability.cpp` puts a harmless 14-byte text
marker on its own aligned read-only page. A child executes a private copy,
reports readiness through a pipe, and waits. The parent then writes a different
marker at the corresponding ELF file offset. Only afterward does the child
access that page and report its contents.

All three cases failed on both TMPFS and XFS with Normal kernel
`94f38b9e77ed1a1754b1397187ebebf32e64db1e2378d4972756d6cee7c922d4`:

| Write route | Result on each backend |
| --- | --- |
| Writable descriptor opened before exec | 14 bytes written; child reads `modified-image` |
| New writable descriptor after child readiness | Same result |
| Hardlink to the running image | Same result |

Each child exited normally. Every fixture image, hardlink and uploaded binary
was removed. The expected marker is `admitted-image`. The original 9CU check accepted a
write rejected with `ETXTBSY`, or a successful write while the running image
retains its original private bytes. These checks establish byte instability,
not completion of metadata, set-ID, interpreter or remote admission coverage.

The original fixture required successful exec. If executable admission is
changed to reject an already-open writer, add a distinct admission-denial
control, with the exact error and unchanged state, while retaining successful
exec and subsequent byte-stability controls.

## Source map

| Subsystem | Relevant files and entrypoints |
| --- | --- |
| Exec | `syscalls_impl/process/exec.cpp`: `wos_proc_exec_impl`, `wos_proc_execve_impl`, `prepare_file_backed_elf`, `NativeExecImage`, `PreparedNativeExecImage`, `append_exec_lazy_file_ranges`, `commit_native_exec_image`, `handoff_worker_exec` |
| VFS | `vfs/core.cpp`: `vfs_open_file_as`, `vfs_fstat_file`, `vfs_pread_file_direct`, write/pwrite/truncate and metadata mutation paths, `MountOpenFileReservation`, `vfs_put_file`; `vfs/file.hpp`, `vfs/file_operations.hpp` |
| Lazy mappings | `syscalls_impl/vmem/sys_vmem.cpp`: `read_file_mapping_exact`, `file_mmap_cached_page_for_file`, lazy page fault and mmap writeback; `platform/sched/task.hpp`: `LazyVmemRange` |
| TMPFS | `vfs/fs/tmpfs.{hpp,cpp}`: canonical `TmpNode`, open/close, `tmpfs_write_locked`, `tmpfs_resize_locked`, append and file-content copy |
| XFS | `vfs/fs/xfs/xfs_vfs.cpp`: `XfsFileData`, `xfs_file_from_inode`, write/truncate/close, metadata mutations and swap extent export; `xfs_inode.{hpp,cpp}`: retained cached inode |
| FAT | `vfs/fs/fat32.{hpp,cpp}`: mount serialization and retained nodes; full mutation mapping still required |
| Remote VFS | `net/wki/remote_vfs.cpp`: mapped regular read-open admission, direct ordinary and RDMA write callbacks, remote descriptor retention |
| Swap | `platform/mm/swap.cpp`: activation transfers ownership of the backing File; direct block writes bypass ordinary file operations |

Paths in this table are relative to `modules/kern/src`.

The pre-9CV native flow opened with one admitted credential snapshot, statted
and checked the file, then prepared the main image and interpreter. Image
publication compares the admitted credentials with the current store. Lazy
ranges retain a File and historical device/inode/size/mtime/ctime values.

On a lazy cache miss, `file_mmap_cached_page_for_file` constructs a key from
those historical values and `read_file_mapping_exact` reads the current File.
Before 9CV, these helpers did not validate that the bytes still belonged to the
historical metadata. Local pinned views now provide that association; ordinary
remote read handles still lack it. A file handle pins identity and lifetime, not immutable
content. A final restat alone would still leave post-commit faults exposed.

## Locks and ownership

TMPFS canonicalizes hardlinks to one node. Regular I/O uses its sleeping
`io_lock`; namespace/ownership metadata uses the tree mutex. Existing open
truncation takes tree then I/O locks. Content-copy locks two canonical nodes in
address order. A data-stability mechanism must also cover resize and copy,
without blocking reclamation or swap-in of unchanged file contents.

XFS uses a retained cached inode and its sleeping `io_lock` for data mutation,
with the mount metadata lock for relevant transactions. File close may trim
preallocation or commit metadata. Inode-cache lifetime and inactivation rules
must remain intact; a pathname or per-descriptor counter is insufficient for
hardlinks and independently opened descriptions.

WKI owner writes and mmap sync invoke `FileOperations::vfs_write` directly.
A syscall-only write check misses them. An active swap file also permits block
I/O outside that callback, so swap and executable admission need mutually
consistent ownership rules.

The exec transaction owns prepared storage through failure, worker handoff,
publication and old-image retirement. A stability reference must survive every
lazy read and release on the matching final image/mapping retirement. It must
not accidentally live until an unrelated inherited `/proc/self/fd` descriptor
closes. Independent retained descriptions or a dedicated shared lease object
need an explicit ownership design.

## Implementation requirements

1. Bind the admitted file metadata and content under the backend's actual
   mutation serialization. Preserve the supplied credential snapshot and
   current centralized execute-mode policy.
2. Keep the admitted bytes stable through main/interpreter loading and all
   later lazy faults. Cover scripts and cleanup at each recursion level.
3. Cover existing writable descriptions, new opens, hardlinks, truncate,
   append, content copy, mapped writeback, remote owner writes and swap use.
4. Bind shared page-cache fills to the same content version. Timestamp keys
   alone must not allow current bytes to populate a historical version.
5. Retain exact mount/nosuid and inode lifetime. Allocate and perform blocking
   cleanup outside spinlocks; never hold a filesystem mutex across the loader,
   usercopy, scheduler handoff or a WKI reply wait.
6. Specify remote stable-read admission and capability negotiation explicitly.
   Existing authenticated mapped read-open is not proof of an owner-held
   content lease. Preserve peer identity and authorization policy.
7. Effective-ID changes require the Goal 19 9DB precommit rendezvous: all live
   credential-owner workers must acknowledge safe waits before credential
   publication. Bounded nonquiescence returns `EAGAIN` with the old image and
   credentials intact. File stability alone does not establish this exclusion.

The local implementation uses backend execution leases: acquire a retained
read view and metadata atomically, prevent conflicting content mutations, and
release the lease with the final image reference. These requirements also apply to remote source admission and its failure
paths; healthy-peer validation alone does not fulfill the complete contract.

## Validation plan

Cross-compile the fixture using the current WOS sysroot, `-fno-pie` and
`-Wl,--no-pie`. The fixture validates ET_EXEC and derives the marker offset from
PT_LOAD headers; it does not assume a fixed file offset. The compiler driver
ignored `-no-pie`, so the explicit linker option is required here.

Run the existing fixture runner on TMPFS and XFS in a disposable rootless WOS
VM. Add admission-denial/release controls, failed ELF/interpreter cleanup,
shared cache version checks and actual backend KTEST cases before accepting a
production fix. Extend to mapped writes, remote owner writes and the supported
FAT policy. Run Normal Build WOS, isolated KTEST, existing exec/credential/VFS
host checks and relevant local/two-node runtime regressions.

No user-run debugging is required to reproduce the six current failures:
`--no-setup` VM operation is authorized and was used successfully. The remaining
unknowns are implementation contracts, not a runtime blocker.
