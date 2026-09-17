# Credentials and authorization migration

## Remote symlink identity (Goal 19, 9DF)

Remote symlink creation now sends one admitted caller snapshot, literal target
text and the raw destination or retained directory handle to the owner. The
owner checks the destination parent and assigns caller UID and effective or
parent-setgid GID; symlink mode remains 0777 regardless of umask. Target text
is never resolved. Exact-binding feature support precedes one mutation, and
legacy callerless symlink requests are refused.

Normal Build WOS, 16 host checks and formatting/tidy pass. All 1,152 native
TMPFS/XFS cases, three original controls and 1,466 existing regressions pass.
An older owner returns ENOTSUP with no child or parent metadata change.
Deferred attachment no longer calls mkdir on host roots, avoiding a confirmed
boot-time feature-query wait behind its own queued withdrawal. The isolated
KTEST suite passes all 18,539 checks, including the mapped-owner symlink test,
and powers off automatically. Other namespace mutations remain pending.

## Remote mkdir identity (Goal 19, 9DE)

Remote mkdir now keeps one caller snapshot through lookup and sends it to the
owner for parent search/write checks and directory ownership inheritance.
Umask is applied once; parent setgid and supplementary groups retain their
existing meaning. Raw components and retained directory handles survive
rename. A read-only feature query and the single mutation share one exact
channel binding. Unsupported owners receive no mkdir request.

All 384 native TMPFS/XFS cases and the original ownership controls pass,
alongside 1,082 existing chmod, timestamp, directory, set-ID and file-stability
checks. The older-owner check returns ENOTCONN with no child or parent metadata
change. Normal Build WOS and 16 focused host checks pass. The final isolated KTEST suite passes
all 18,538 checks, including the mapped-owner mkdir test, and powers off
automatically. Live debugging caught and fixed recursive host-root
attachment caused by mkdir before mount publication. Other namespace mutations
and broader authority-domain qualification remain pending.

## Remote timestamp identity (Goal 19, 9DD)

Remote pathname timestamp changes now carry a copied caller snapshot and retain
raw path components or the exact remote directory description. The owner maps
same-domain write authority, prepares its clock value once, and applies the
existing timestamp policy under metadata locks. Explicit and mixed NOW/OMIT
requests require ownership or FILE_OWNER; NOW/NOW also permits callers with
write access; OMIT/OMIT preserves timestamps. Public utimensat keeps its copied
credentials and time arguments across precommit lookup retries.

The new UTIMENS path operation uses exact resource support admission and refuses
legacy callerless timestamp requests. The baseline allowed an unrelated user to
set a UID1001 file's explicit timestamps. Normal and isolated builds and sixteen
focused host checks pass. All 576 native TMPFS/XFS timestamp cases and the
original three ownership controls pass, along with 506 existing chmod,
directory, set-ID and file-stability checks. The isolated suite passes all
18,537 checks and powers off automatically, including the mapped owner test
with full/zero authority ceilings and all six time intents. Denied and
OMIT/OMIT updates preserve atime, mtime and ctime. Descriptor-only remote
futimens and other callerless mutations remain pending.

## Remote chmod identity (Goal 19, 9DC)

Remote chmod and pathname fchmodat now carry the caller's immutable snapshot
to the owner, which applies the existing owner/group policy under its metadata
lock. Root and file owners retain legitimate access; unrelated users receive
EPERM with mode, owner, group and ctime unchanged. A chmod-specific lookup
intent preserves raw ancestor search and remote directory handles through
rename, and enforces trailing-directory requirements before mutation.

The versioned request requires exact resource support and authenticated
same-domain write policy. Legacy callerless chmod is refused without a
fallback. All 96 native TMPFS/XFS cases pass, including supplementary-group
setgid behavior, final symlinks, missing/trailing paths, and renamed directory
handles with replacement files. Normal and isolated builds and 16 focused
host checks pass. The isolated suite passes all 18,536 checks, including
full/zero authority ceilings, restricted-root denial, owner/group controls,
and unchanged metadata on denial. The initial kernel fixture incorrectly
expected unrestricted root behavior with a zero authority ceiling; the fixture
was corrected without relaxing production policy. The full authorization goal
remains incomplete.

## Set-ID exec rendezvous (Goal 19, 9DB)

Effective-ID-changing exec now closes thread admission and waits for every live
credential-owner worker to reach a safe user-return boundary or the explicit
worker exec handoff wait before publishing replacement credentials. The
canonical process owner commits the image, then retires the old workers before
entering userspace. The rendezvous holds no VM, action, registry or resource lock
while waiting. Commit revalidates the admitted immutable credential snapshot.
Epoch release and safe-wait marker withdrawal share the resource lock, so a
second exec cannot count a waiter that already decided to leave after rollback.
Rejected worker handoffs use the same release protocol. A bounded five-second
failure returns EAGAIN and task-owned cleanup releases the rendezvous without changing the old image, credentials or CLOEXEC state;
an interrupted sibling syscall may return EINTR.

All 72 native local and 72 WKI-mounted-file cases pass, including main and worker
set-ID exec with runnable or pipe-reading siblings, canonical PID, saved IDs,
CLOEXEC, no-new-privileges and retained nosuid policy. The previous kernel
returned EAGAIN in all 12 unsuppressed multithreaded set-ID cases. Existing exec
and 144 spawn regressions pass. The historical `service_exec_denial` fixture
now requires `--legacy-eagain` and an older kernel; it is not a current success
criterion. Normal and isolated builds, all 15 focused host checks and formatting
pass. The final isolated suite passes all 18,535 checks with zero failures and
automatic poweroff, including the forced rollback/new-epoch marker test. The
broader Goal 19 remains incomplete. Earlier sections record historical qualification; 9DB
supersedes their blanket set-ID EAGAIN guard and pending-quiescence notes.

Goal 19 is in progress. Production tasks now read immutable process credential
snapshots. Credential publication and the remaining authorization/service/WKI
migrations below are separate completion requirements.

## Authenticated shared-memory frame capacity (Goal 19, 9DA)

The shared-memory message receiver now preserves complete WKI v3 frames,
including large VFS reads/writes and the authentication trailer. The old
8,192-byte buffer truncated these frames and stalled copying an executable
before PIN. Both builds, 17 host checks and all 42 native file-stability cases
pass. All 18,499 isolated KTEST checks pass with zero failures and automatic
poweroff. Existing MAC/replay checks and
raw-RDMA denial are unchanged. Earlier exec notes treating direct RDMA as a
supported path awaiting qualification were stale. The broader authorization
goal remains incomplete.

## Executable-view support admission (Goal 19, 9CZ)

Remote exec now requires the owner's VFS executable-view feature bit in an
exact live resource observation before allocating a token, cleanup record or
sending PIN. Unsupported peers return ENOTSUP without starting executable
state. Existing credential/mapping decisions, channel identity and wire layouts
are preserved. Both builds, 15 host checks and all 18,498 isolated KTEST checks
pass. All 42 current/current file-stability cases pass. Two current/older-owner
checks return ENOTSUP promptly with unchanged consumer cleanup state and
ordinary remote I/O still usable; both backends clean their fixtures. The full
goal remains incomplete. See the [exec stability record](exec_file_stability.md).

## Live executable cancellation recovery (Goal 19, 9CY)

Four live cases now hold cancellation requests or responses through the real
60-second close timeout on owner TMPFS and XFS. Each retains a cleanup record,
then completes through retry after healing, with no peer-retirement handoff.
The peer remains connected, record counts return to baseline, owner writes
succeed and fixture state is removed. Read-only `wki_vfs_exec` diagnostics make
these transitions observable. Both builds, 17 host checks and formatting pass;
all 18,497 isolated KTEST checks pass with zero failures and automatic poweroff. Details and remaining mixed-peer,
RDMA/fence and set-ID requirements are in the [exec stability record](exec_file_stability.md).

## Durable executable-view cancellation (Goal 19, 9CX)

Remote PIN now reserves bounded cleanup storage and an independent proxy
reference before contacting the owner. Failed cancellation retains its token
for paced retries on the separate client worker. Successful CANCEL or proven
exact binding retirement releases that ownership; mere local inactivity does
not. Existing credential decisions, mount policy and wire layouts are preserved.
Both builds, 15 focused host checks, formatting and all 18,497 isolated KTEST
checks pass. All 20 remote and 22 local two-node file-stability cases pass.
Remaining compatibility/transport cases are tracked in the
[exec stability record](exec_file_stability.md). The full goal remains pending.

## Remote executable views (Goal 19, 9CW)

WKI can now clone a channel-bound read description into an independent
owner-held TMPFS/XFS execution view. The versioned PIN/CANCEL operation retains
captured metadata, uses a unique client cache identity, and preserves mapped
read policy, exact channel generation, mount/nosuid and the admitted credential
snapshot. Final view close waits for token-scoped owner retirement. Existing
HELLO bits and ordinary read-open behavior are unchanged; unsupported peers
cannot silently supply an unpinned executable input.

The four synchronized remote content failures now pass, as do the expanded
20 remote lifetime/error cases, 22 local cases, 305 prior exec cases, 144 spawn
cases, 36 remote nosuid cases and four mount-reporting rows. Both builds, 15
focused host checks and formatting pass. The isolated suite also passes all
18,496 checks with zero failures and automatic poweroff. The [implementation record](exec_file_stability.md)
also identifies an unfinished error path: if cancellation cannot be delivered,
its token needs durable retry while the owner remains connected. Mixed-peer,
RDMA/fence validation and precommit set-ID quiescence remain pending.

## Local executable byte stability (Goal 19, 9CV)

TMPFS/XFS execution now retains a private inode content view and captured
metadata before ELF loading. Conflicting backend writes and truncation return
`ETXTBSY`; lazy cache keys include the pinned content identity. Process and
worker exit release their references before publishing death, so unrelated
ordinary descriptors cannot keep a finished image pinned. Mount/nosuid,
credential snapshots and current set-ID/shared-VM guards remain unchanged.

The [exec file-stability record](exec_file_stability.md) preserves six baseline
failures, the first candidate's delayed-release failure and the corrected
22-case native result. Existing 305 exec checks, two mount-reporting rows and
144 spawn checks pass; both builds and 11 focused host checks pass. The final
isolated suite passes 18,495 checks with zero failures and automatic poweroff. The race
fixture now observes denied state while its credential mutator is paused,
after diagnostics proved readlink itself could return EAGAIN during churn.
Remote source leases and full precommit quiescence remain pending. A two-node
probe now reproduces four remote byte-stability failures after successful
fsync; all 36 remote nosuid cases and two mount-reporting checks still pass.

## Spawn action errno and fallback flags (Goal 19, 9CT)

Kernel spawn DUP2 now returns specific descriptor errors through the public
spawn result. Missing sources and destinations outside the child limit report
`EBADF`; a reserved destination reports `EBUSY`. Failed replacements preserve
references and flags. Successful installs update descriptor usage, and displaced
files are released outside the table lock. A same-descriptor DUP2 still clears
CLOEXEC on an inherited descriptor above a subsequently lowered limit.

The generic libc fork/exec fallback now captures errno from failed libc calls,
checks F_GETFD before using its result, and preserves OPEN CLOEXEC when moving
the descriptor. It checks OPEN's destination against the child limit before
closing the destination or creating/truncating a file. The error pipe's final
CLOEXEC setup and child signal-mask result are checked. Earlier successful file
actions retain their sequential effects; failure leaves parent descriptors,
real/effective/saved IDs, supplementary groups and output PID unchanged.

The 22-case baseline failed five DUP2 cases per backend through fast libc/raw
kernel spawn and fourteen cases per backend through forced fallback. The expanded
candidate passes 144 native checks: 24 cases on TMPFS and XFS through each of
three interfaces. These include two additional invalid-destination OPEN cases
that verify no file is created, ordered create-then-failure and failure-before-
create controls, private PROCFS descriptor reopening, and pipe relocation.
Empty SETSIGDEF attributes force the fallback using the public API; the local
libc has no public CHDIR/FCHDIR action constructors, so those dormant branches
receive source checks only. The baseline used the original UID/FD parent checks;
the candidate additionally checks saved IDs and supplementary groups.

Both Normal Build WOS and the isolated diagnostic build pass, as do eleven
focused host checks and formatting. The existing 305 exec cases and two local
mount-reporting checks pass. The isolated suite passes 18,465 checks with zero
failures and automatic poweroff, including expanded actual DUP2 helper checks
for errors, reservations, reference counts and flags. All task VMs are stopped.
Normal candidate shutdown encountered the previously observed rootfs busy
condition (nine open files) and performed its final sync; this slice does not
claim to fix that shutdown issue.

Fallback still requires two parent descriptors for its error pipe. A completely
full parent table returns `EMFILE` before any action; the raw kernel can replace
a child descriptor without that pipe. This remains an explicit compatibility
limit. Full set-ID precommit quiescence, stable executable metadata/content and
WKI sender admission remain separate pending work.

## Spawn OPEN authorization (Goal 19, 9CS)

Bounded, supported `posix_spawn` OPEN actions now use the kernel spawn path for
all paths and flags, including CLOEXEC. The libc collector still limits requests
to 32 actions and its supported attributes; a submitted syscall result never
selects a second fork/exec attempt.

The kernel performs OPEN using the unpublished child's inherited credentials,
root/cwd, routing rules and umask through `vfs_openat`. It checks the destination
against the child's descriptor limit before any file mutation. Closing the
private destination first lets a full table reuse that slot. VFS reserves a
slot before backend create/truncate; moving the result to the requested slot
uses the fixed private table without allocation or blocking. Descriptor flags
survive this move, and an explicit subsequent DUP2 clears CLOEXEC. Actual OPEN
errors reach libc. The parent table remains unchanged; successful earlier file
actions retain their ordered side effects if a later action fails.

PROCFS can retain an explicit caller's own Task before that child is in the
global PID table. This hint is accepted only when the requested PID, caller PID,
and retained process owner's PID match. Other targets use the existing global
lookup. Target references, private-data authorization and credential-generation
revalidation remain required. This preserves inherited `/proc/self/fd` grants
and observes earlier CLOSE actions in the child's private table.

The qualified baseline failed eight of 14 raw OPEN cases on each of TMPFS/XFS,
including inappropriate success through a private ancestor and create/truncate
side effects. Public libc's old fallback separately failed nine of 14 on each
backend; those are not kernel-path results. The candidate passes 64 OPEN cases
(16 per backend/interface), 305 existing exec cases and two mount-reporting
checks. A first candidate missed one numeric PROCFS group lookup; the expanded
reopen test exposed it and passes after correction. Both builds, eleven host
checks and formatting pass. The final isolated suite passes 18,465 checks with zero failures and automatic
poweroff. Two-node execution pinned to the source passes 4,560 PROCFS open and
1,200 readlink checks, plus 36 remote-file nosuid exec cases and two additional
mount-policy reporting checks. All fixture cleanup completes. An initial
isolated run reported one obsolete unpublished-self realpath expectation;
the corrected test retains foreign-PID denial and root-confinement checks.
The goal ledger preserves every attempt and artifact hash.

Unsupported libc attributes still use the generic fallback; its errno and
OPEN flags are covered by the 9CT work above. Full set-ID precommit quiescence,
executable metadata/content versions and WKI sender admission also remain open.
The exec and file-open domains remain pending in the manifest.

## Exec input admission (Goal 19, 9CR)

Native exec captures one credential snapshot before copying user arguments.
The request owns it through script recursion, executable and interpreter reads,
and prepared-image handoff. Final credential preparation compares the admitted
snapshot with the current store under the existing VM publication guard before
changing image/debug state or waking the process owner. A mismatch returns
`EAGAIN`; it cannot restore authority discarded while the image was loading.
Spawn also captures once at its syscall wrapper and carries that value through
script recursion and private child/interpreter construction. Execute-mode checks
use the central file policy and preserve the existing execute-bit requirement.

`vfs_open_file_as` reads with explicit credentials and retained component lookup,
without reserving or installing an FD. It preserves PROCFS descriptor grants,
uses retained-target generation checks for synthetic files, and opens remote
regular inputs through the exact pinned mount outside namespace publication.
It accepts only `O_LOCAL`, `O_NOTIFY_CACHE_CHANGE`, and `O_NO_CACHE`; bounded
context retries return `EAGAIN`. Executable open, stat, and mode errors now reach
spawn callers instead of becoming a generic failure.

The native path fixture reproduced eight failures among 27 baseline cases:
four spawn ancestor-search omissions and four wrong-error results. The candidate
passes all 27, including main/worker exec, spawn, relative cwd, symlink and
`/proc/self/fd` controls, plus the existing 242 exec cases and 36 local nosuid
cases. A two-node run also passes all 36 remote-file nosuid exec cases and
two remote mount-policy reporting checks, with execution pinned to the client.
Credential-churn retries verify the old saved UID, executable and CLOEXEC
state before retrying, and still require exactly one successful image. Both
Normal and isolated diagnostic builds, eleven host checks, and formatting pass.
The full isolated suite passes 18,464 checks with zero failures and automatic
poweroff. Detailed runtime results and hashes
are recorded in the Goal 19 ledger and `/tmp/wos-goal19-exec-admission-*`.

This does not complete the exec domain. Full set-ID precommit quiescence,
executable metadata/content versioning, remaining spawn action/fallback
qualification, and WKI sender identity reservation remain pending. Read permission requirements
and interpreter error behavior are preserved by this change.

## Snapshot contract

`platform/security/credentials.hpp` owns a bounded value snapshot with real,
effective and saved UID/GID, eight supplementary groups, an authority ceiling,
the irreversible no-new-privileges flag, and process-wide dumpability. Its
default identity has no root authority and is not dumpable. Only explicit trusted bootstrap uses `bootstrap_root()`.

Snapshots have no mutable field access. Preparing a change creates a separate
value; an error returns the complete original value. This allows callers to
check dependent UID population admission before publishing any credential
change. `CredentialStore` copies the complete value under a short IRQ-safe
spinlock. A returned copy remains valid independently of subsequent updates or
store destruction. It needs neither heap allocation nor deferred reclamation.

Task owns the store; user threads retain their process owner and share that
store. Unbound threads read an unprivileged snapshot. Live changes use
`resource::change_credentials()` under the shared-VM publication guard, with
lock order population → credential → resource. Real-UID changes admit and move
every attached thread charge before publishing the complete snapshot, or leave
all charges and credentials unchanged. The standalone store's `update()` must
not replace this live-process transaction. Trusted construction uses a separate
initialization API that rejects already-published tasks.

The internal authority ceiling only limits root authority. A bit does not
grant a nonroot user that authority. Bits may be discarded and never restored
by a credential transition or set-ID exec. No-new-privileges suppresses set-ID
exec; ordinary saved-ID operations retain their existing POSIX meaning. A
service must drop supplementary groups, GID, and every UID before exec and
enable no-new-privileges to prevent set-ID binaries from regaining authority.

Exec saves its resulting effective IDs for every executable, including files
without set-ID bits. This follows the
[POSIX exec contract](https://pubs.opengroup.org/onlinepubs/9799919799/functions/exec.html).
Exec derives set-ID metadata from the retained executable and prepares the
replacement before image publication. Tracing suppresses set-ID changes.
The executable File also supplies its retained mount's immutable `nosuid` policy.
Native main/worker exec and spawn preserve that policy through preparation and
suppress both setuid and setgid changes while refreshing saved IDs normally.
Init translates fstab `suid`/`nosuid` tokens into mount flags, with the last token
winning and other backend options preserved. Init reads the complete bounded
table before acting on any entry and rejects oversized or NUL-containing input.
An overlong field rejects its row instead of dropping a policy suffix or shifting
columns. The existing required `/tmp` and `/run` fallback remains separate from
configured mount success. Swap entries now reach shared line advancement.
The production parsing helpers pass boundary, short-read and error tests. The
final init build boots its valid fstab and starts services, and the 36-case
mount policy fixture passes again. Its shutdown stopped the VM but reproduced
the existing rootfs EBUSY/open-files-9 issue; this change does not resolve that
separate lifetime problem.
Explicit `wki://` mounts also preserve the caller's local mount flags through
the synchronous remote attach callback. A two-node regression reproduced nine
set-ID failures when that callback passed zero flags; the fixed client passes
all 36 remote-mount exec cases and reports the policy on both path and retained
file statistics. The same candidate passes 36 local mount-policy cases, all
242 established exec regressions on a single-node VM, 14 focused host checks
and 18,442 isolated kernel checks with zero failures. Both builds and formatting
pass; the final source and owner VMs stop with clean rootfs unmounts.
Modes are prepared on the owner because mapped chmod remains
unimplemented; exec is pinned to the client. Owner policy advertisement and
automatic remote-mount defaults are separate pending work.
Mount snapshots, `/proc/mounts`,
`statvfs` and `fstatvfs` report the policy; an opened File reports its original
mount even after another filesystem is mounted over its historical path.
The native 36-case matrix passes ordinary/nosuid mounts, NNP, setuid/setgid/both
and main/worker/spawn execution, plus both mount reporting checks. Other mount
flags and policy forwarding across WKI are separate pending work.
Effective-ID changes currently return EAGAIN when old-image siblings or a user
thread owner could retain the new authority; complete precommit sibling
quiescence remains outstanding. This restriction is a temporary fail-closed
migration measure, not full exec qualification.

Native image publication now has one explicit `commit_native_exec_image`
transaction. It validates credentials and admission, publishes the staged debug
image, and replaces image metadata, credentials, registers and pagemap under the
existing locks. Old storage is captured from that same task and retained in the
task-owned transaction for unlocked cleanup. `PreparedNativeExecImage` now owns the fully staged image:
path/register/stat copies, executable metadata and file, name, pagemap, TLS/stack
metadata, lazy mappings and the staged debug entry. Its view contains no process
ownership, kernel stack or syscall scratch state. Allocation failure preserves
the original image; failed commit destroys the preparation, and successful
commit releases its ownership to the task before commit locks are dropped.
Completed preparations now have a sole-owning task slot, recovered by both
process and worker exit while the executor remains ACTIVE. Cleanup keeps that
slot published while VFS/MM release can block, clears it before deleting the
wrapper, and defers reentrant fatal/group exit without consuming pending requests.
The same guard protects partial image publication through replacement CR3/FPU/TLS
activation. The transaction owns both detached old-image storage and temporary
exec admission, and stays registered through sibling retirement and final
signal/descriptor cleanup. Successful cleanup releases old resources before
reopening admission; aborted preparation releases admission before private
VFS/MM cleanup. Permanent exit admission stays closed. Explicit preparation,
publication, activation and discard phases exclude cleanup of the live old root.
Destruction runs outside commit locks and before the final return-frame patch.
Recursive native loading now returns an adopted preparation to the syscall
wrapper before committing or activating the image. The wrapper invokes
`finish_native_exec_image` only for a successful local preparation; remote
success and errors keep their existing cleanup paths. Completion copies startup
registers, addresses and timing data before destroying the owners. This separates
loader stack lifetime from the future worker-to-owner transfer, while keeping
publication, activation, sibling retirement and final cleanup in one implementation.
Native exec borrows arguments from the bounded syscall-entry kernel snapshot;
it no longer duplicates every vector/string into unchecked heap allocations.
Null vectors retain the prior nonnull empty-vector remote-submission behavior.
The task now owns `ExecRequestArguments` before the first faulting usercopy.
That owner contains the root snapshot, input-address spill storage and four
bounded shebang frames with copied interpreter, option and script-path bytes.
Cleanup destroys the image before the arguments under the existing teardown
guard. Recursive returns do not touch argument storage after an inner native
commit has destroyed it. Native forwarding allocation failure returns ENOMEM;
the legacy spawn convention is unchanged.
The 20-case argument fixture passes on both prior and current Normal kernels,
including recursive ordering, empty environments and late-pointer EFAULT.
A KTEST exercises partial allocations, address-vector spills, conflicting
adoption, guarded cleanup and destruction inside a recursive callback.
The request also owns a separate native scratch slot for each allowed recursion
depth: file-backed ELF tables, main/interpreter lazy-range scratch vectors and
replacement-stack address arrays. `take_task_metadata()` transfers persistent
ELF bytes separately; scratch cleanup must not free that transferred storage.
The file-backed loader retains its guard against registering borrowed section
names in the debug registry. These ownership checks do not force fatal delivery
at every phase. Early File/pagemap/Thread ownership and allocations inside loader
helpers still need recovery before the completed-image slot is adopted.
The latest candidate passes 149 argument/denial/shared-state/signal cases, including
both ordinary and 8 MiB file-backed executables, without
allocator warnings or userspace faults and unmounts rootfs cleanly; the historical
intermittent rootfs EBUSY/open-files pin issue remains unresolved.
Worker exec now queues its adopted image for the canonical process owner after
loader frames unwind. Temporary exec admission excludes competing commits and
new thread bindings. The owner claims the image at a validated userspace-return
boundary or its retired-main loop; queued and active requests are cancelled
before process exit waits for workers. The worker retains its CPU contribution
until acknowledgement, then retires explicitly. Completion merges its pending
signals and mask before synthetic sibling-exit requests, and keeps the owner's
PID, credentials, descriptors, child/job relationships and kernel stack.
Credential mutation now checks the calling worker's address space against its
canonical owner while holding the VM publication guard. A worker from an image
already replaced by exec queues cooperative retirement and cannot update the
new image's credential store. The syscall unwinds before the existing safe
userspace-return path consumes that request. Matching-image and precommit
mutations retain their existing behavior. This closes a native SETEUID race
that left the replacement with real/effective/saved UID `0/0/1000`.

The native concurrency matrix passes 78 cases: competing worker/owner exec,
process exit, thread creation and effective-UID publication, across root and
restricted identities where applicable. The final exit sample observed eight
exit wins and ten replacement wins. It also observed 124 thread-creation
rejections without a published pthread handle; WOS's existing ENOMEM/EAGAIN
results are preserved. The replacement checks IDs, saved IDs, NNP/authority
ceiling, PID/TCB, executable path, CLOEXEC and fresh thread creation. Timing
samples do not force every queued/claimed cancellation interleaving.

The replacement TCB uses the canonical thread ID. Final owner entry reconstructs
userspace frame provenance after blocking cleanup, resets the canonical kernel
stack and clears abandoned timer FPU state before entering userspace.

The owned-image KTEST overwrites and destroys its staging scope, then checks the
retained metadata and exactly one final File close across repeated cleanup.
The `service_exec_denial` fixture passes 30 late-denial cases: root/nonroot
callers with a live worker attempt setuid, setgid, and combined images, reaching
the existing EAGAIN guard after staging. Real/effective/saved IDs, executable
identity, CLOEXEC descriptors, caught disposition/mask and the worker all remain
unchanged. The same 30 denial cases also pass from a worker executor.
The isolated suite passes 18,442 checks, including task-slot adoption,
conflict refusal, reentrant cleanup, worker admission recovery and retired-image
release with replacement references preserved, and scratch cleanup with transferred
metadata still readable, plus queued/claimed handoff cancellation with a single
image owner. The replaced-image worker regression also checks all 13 credential
mutation types against the actual process store and preserves existing exit
status. The retained-mount regression checks that an open File keeps nosuid
policy and filesystem statistics across a child overmount; owned-image staging
also preserves the copied policy. The existing native signal tests pass. The strengthened
worker matrix now passes all 15 cases, including live, retired, CPU-bound and
signal-handler owners, with kernel and TCB thread identity checked. All 15 also
verify the calling worker's blocked mask and pending signal in the replacement.
The first added signal fixture used libc `raise()`, which targets the process
owner in this tree, and seven cases died from SIGUSR2 before replacement. The
corrected fixture uses WOS positive-task-ID `kill(gettid(), SIGUSR2)` to target
the worker; the failed run is retained separately from the passing matrix.

Repeated native runs also exposed allocator instability: the candidate encountered
an already-freed page in the page-table pool during fork, and a later run logged
a duplicate pool release followed by a fault/reboot during a debugger probe.
The exact pre-transaction kernel reproduced duplicate pool release in a bounded
comparison. That proves duplicate release predates the transaction extension;
it does not establish the cause of every observed failure or qualify allocator
stability. A baseline late-denial case also failed its executable-identity check.
The duplicate-release investigation identified synchronous process-exit cleanup
and deferred task GC releasing one resident root. GC had used an
execution-eligible Task lookup, which hides EXITING publishers. GC now scans
all registered pagemap publishers under the registry lock, and non-owning
zombie cleanup detaches its reference before retaining only status metadata.
Pool slots retain bounded release provenance for future duplicate diagnostics.
The new KTEST exercises ACTIVE/EXITING/DEAD publishers and zombie detachment;
the isolated suite passes 18,340 checks. A fresh Normal VM passed 720 native
denial/shared-state cases plus the 19 existing signal/disposition cases without
allocator warnings or userspace faults. At that checkpoint the strict thread
matrix had three main-thread passes and six worker failures. Historical freed-pool and userspace
faults are retained as separate
observations; passing retries alone do not establish their complete cause.

Successful exec preserves ignored signal dispositions, pending signals and the
calling thread's blocked mask. It resets caught dispositions and clears old
handler/restorer metadata, alternate-stack addresses and transient signal-return
state only after successful image commit. For main-thread native exec, final
signal reset and close-on-exec cleanup follow cooperative sibling retirement,
with thread admission still closed. Signal reset holds the process action mutex;
no old worker can reinstall an old-image handler after that reset. Failed exec
leaves signal state and descriptors unchanged. The native `service_exec_signal` fixture covers direct exec and exec
from an alternate-stack handler under root, nonroot and reduced-root identities;
sibling retirement remains the separate lifetime requirement above.

Exec's close-on-exec scan and stdio fallback resolve the retained process FD
owner before inspecting or changing descriptors. A worker's obsolete local
table cannot hide `FD_CLOEXEC` entries or receive fallback descriptors that VFS
would not use. The native `service_exec_threads` fixture verifies descriptor
closure, original process PID, executable identity and sibling retirement for
main-thread exec, worker exec and worker exec after main-thread retirement.
Descriptor closure passes in all tested identity/mode combinations. Main-thread
exec now closes temporary thread admission during commit, requests cooperative
same-group sibling exit after activating the replacement address space, and
waits for their CPU contributions to end before new userspace entry. The wait
holds no publication or signal-action locks. Releasing temporary admission never
reopens permanent process exit. A private startup-register snapshot prevents
kernel preemption during retirement from changing the new image's registers.

All 15 cases now pass, including correct executable and thread identity,
old-executor retirement, and fresh pthread creation/join in the replacement image.
The first handoff candidate halted at FPU restoration because cleanup had left a
parked-kernel saved frame; explicit final userspace provenance fixed that observed
failure. The failed run remains recorded in Goal 19.
Precommit process-wide atomicity and forced concurrent exec/exit/creation,
credential and signal races remain requirements. A queued handoff can interrupt
an owner's old syscall; rejection after that wake needs further no-partial-state
qualification. The effective-ID EAGAIN safeguard stays in place. Thirty added
worker-originated set-ID denial cases preserve credentials, image, CLOEXEC,
signal state and an admitted sibling. Existing main-thread denial and signal
fixtures also pass. Remote executor handoff is not qualified by these local runs.

The `service_exec_shared_state` race fixture repeatedly installs an old-image
caught handler while main-thread exec replaces the image with 200 CLOEXEC
entries. The baseline retained that handler in all 30 cases across root,
nonroot with no-new-privileges, and reduced-root identities. Moving final signal
reset and descriptor cleanup after sibling retirement passes all 30 cases,
while preserving ignored dispositions, the caller's blocked mask, identity and
bounded child reaping. The existing nine exec/signal and ten disposition cases
also pass. This qualifies that main-thread signal race; worker-exec ownership,
concurrent credential publication and complete image/version atomicity remain
open.

Effective UID/GID changes disable dumping in the same credential publication.
Exec enables dumping only when its retained executable is readable to the old
identity and does not change effective IDs. Core admission and recorded IDs use
one snapshot. PR_SET_DUMPABLE changes the shared process value, not one thread.

Ptrace attachment serializes with credential/exec publication. The local policy
requires matching real/effective/saved target UID/GIDs, target supplementary
groups covered by the caller, and dumpability, unless explicit PTRACE authority
permits the identity/dumpability override. Every debugger also needs an authority
ceiling covering the target and cannot bypass no-new-privileges via another
process. Existing sessions recheck policy on requests and after wait wakeups;
an attachment may still be released after permission is lost. PR_SET_PTRACER is
a validated compatibility no-op because WOS has no Yama relationship policy;
it never edits an active attachment. Register transfers now require a stopped
user context. GPR writes validate WOS selectors, user address bounds and the
same permitted RFLAGS as signal return before changing state. FPU writes reject
unsupported XSTATE bits, nonstandard/reserved headers and MXCSR bits outside
the trusted hardware mask; transfers wait for CPU ownership to end by returning
EBUSY to a caller that arrives before the stop completes. Hardware breakpoints
accept only user addresses and supported access/length combinations. Async
syscall snapshots copy the saved user register block after CPU handoff, instead
of exposing kernel resume registers. Live nonroot register denial/positive probes and root strace pass. Remote
identity and full exec qualification are still required before ptrace is marked
enforced.

SysV SHM stores the creating snapshot's effective owner/creator IDs. Existing-key
lookup, attachment and stat use one caller snapshot and classed read/write mode
checks; removal requires owner/creator identity or FILE_OWNER authority. A
DAC_OVERRIDE holder can bypass read/write mode restrictions. Denials precede
attachment pins, page mapping, output copy and destroy marks. Permission mode
contains only its low nine bits, preserving IPC_PRIVATE and delayed removal.
SHM_RDONLY mappings carry PAGE_WRITE_RESTRICTED on each present shared leaf.
The tag uses software bit 11; nonpresent leaves still use that bit solely as
PAGE_RESERVED. Whole-range mprotect preflight denies write upgrades with EACCES
before changing metadata or any leaf. Low-level protection and write-fault paths
also refuse upgrades. Fork copies the tag, CLONE_VM shares it, and PROT_NONE
retains it in a present supervisor-only leaf. Replacement/unmap retire it with
the old translation, independently of segment or process metadata cleanup.
Live lifecycle/denial probes and diagnostic kernel tests pass. IPC_SET remains
unsupported with EINVAL.

Tmpfs chmod/chown now take the retained caller snapshot explicitly. The backend
holds its tree lock while checking the canonical object's owner/group/mode and
committing the prepared metadata. Foreign callers receive EPERM before any
owner, mode, ctime, output or VFS cache update. Path consumers retain lookup
identity; descriptor consumers retain the file, including after unlink, and
serialize cache publication with path metadata changes. Hardlinks use the same
canonical object. Chmod requires effective ownership or FILE_OWNER authority;
an unprivileged caller cannot set setgid for a group it does not belong to.
Chown requires ownership and an unchanged UID plus unchanged/member GID, unless
FILE_OWNER permits an override. Successful chown clears set-ID bits on executable
regular files, including no-op ID requests and privileged callers; directory
inheritance bits remain intact. The -1 ID sentinel retains that ID.

XFS chmod/chown use that same policy through retained lookup and descriptor
adapters. Inode I/O locking precedes the mount metadata lock, excluding readers
while transaction undo may replace fork allocations. Authorization precedes
transaction allocation; capture precedes inode mutation. UID/GID are now part
of the transaction before-image alongside mode/ctime/dirty and fork state, so
cancellation restores ownership too. Successful metadata operations update
ctime and publish stat/cache output only after commit. XFS chown now changes
ownership instead of reporting a no-op success. WKI still lacks a chown wire
operation, so remote chown returns EOPNOTSUPP until owner-side support exists.

Tmpfs descriptor/stream stat now copies owner/group/mode under the backend tree
lock. Legacy resolved path stat uses a retained lookup. Shared File stat caches
serialize stat and validation-stamp replacement, lookup and promotion with a
per-file mutex, released before backend stat. Fast fstat/fstat-close executes
under the fd-table spinlock and only tries that mutex; contention takes the
existing cache-miss fallback. XFS cached-path inode stat holds the mount metadata
lock after dropping the inode-cache spinlock and before copying the stat. These
read locks prevent mixed ownership/set-ID tuples from reaching exec or a cache.

Devfs PTY path lookup now acquires a node reference under the PTS namespace
mutex. Open files retain their node through device close; unlink drops only the
namespace reference, so a replacement path cannot retarget retained metadata.
The final atomic release deletes the node without accessing its borrowed
Device, whose PTY endpoint lifetime is separate. Stat uses the node's cached
device kind, avoiding a retired Device dereference. Existing WKI retention is
unchanged.

Devfs chmod/chown now use the same central owner/group policy as tmpfs and XFS.
The per-node IRQ-safe metadata lock covers policy preparation, commit and stat
copy; timestamp storage and published metadata replacement share that lock.
VFS metadata lookups retain a node and revalidate its path binding under the
PTS/WKI namespace lock through commit. Stale bindings return EAGAIN; descriptors
retain their original object after unlink. Denials leave IDs, mode, ctime,
output and caches unchanged. Resolved VFS metadata helpers require an explicit
credential snapshot, including owner-side and fallback consumers. Device
chown preserves set-ID bits because the executable-regular-file clearing rule
does not apply to character/block nodes. Malformed devfs files without a backing
node return EBADF instead of synthetic root-owned metadata.

FAT keeps a fixed `0:0` identity and attribute-derived modes. Chmod/chown first
apply the central owner policy, then accept only results already represented by
that fixed metadata. Foreign callers receive EPERM even for no-op requests;
authorized unrepresentable changes receive EOPNOTSUPP. These operations never
write FAT entries, mutate per-open identity or publish a cache change. Path
stat now honors the read-only attribute, agreeing with the mode captured by
open/fstat. Arbitrary POSIX ownership/modes are not persisted through a sidecar
or a runtime-only permission table.

Timestamp requests retain validated NOW/OMIT/explicit intent in an immutable
value, together with the captured current time. Tmpfs, devfs and XFS authorize
against one credential snapshot and the locked target metadata before changing
any timestamp. An owner or FILE_OWNER may use explicit or mixed requests;
current write permission or DAC_OVERRIDE additionally permits both NOW (including
null times). Both OMIT is a no-op with no ctime change, while WOS retains its
existing path and descriptor validation. These distinctions follow the
[POSIX timestamp permission rules](https://www.man7.org/linux/man-pages/man3/futimens.3p.html).
An inherited writable descriptor does not replace current mode authorization.
Retained paths validate their original binding and descriptors keep their object
after unlink. XFS holds the inode I/O lock before its mount metadata lock and
captures transaction undo state before modifying times or trimming preallocation.
Failed transactions restore timestamps alongside inode ownership and forks.
FAT applies the same policy but returns EOPNOTSUPP for authorized writes it cannot
persist; an unchanged OMIT request succeeds. VFS cache updates occur only after
successful changes under the namespace publication mutex. WKI timestamp wire
fields remain unchanged; configured remote identity mapping is still required.

XFS xattr path, descriptor and retained-lookup APIs require the request's explicit
credential snapshot. User-namespace read/write checks and trusted/security
namespace authority use the central file policy; list access and privileged-name
visibility share that same value. A missing backend Task cannot bypass policy.
Trusted pre-scheduler VFS callers and kernel fixtures pass an explicit bootstrap
snapshot. User xattr writes to symlinks remain EPERM; privileged namespaces still
require FILE_OWNER independently of DAC_OVERRIDE. Denials precede attribute lookup,
transaction allocation or output writes. Local fd mutations retain the VFS
publication lock through cache invalidation, and XFS mutations take inode I/O
before mount metadata so fork rollback cannot race I/O. Remote dispatch retains
its existing wire and confinement contract and does not hold the new local locks
across WKI waits.

Tmpfs opens now authorize the retained caller under the tree lock before creating
or truncating. Existing opens check canonical inode access, truncation also
requires write authority, and creation requires parent write/search permission.
Creation initializes effective UID/GID (or setgid-parent GID) before the dentry
is published; the supplied mode already includes VFS umask. A newly created
mode-zero file can return a usable descriptor, with its mode governing later
opens, consistent with [open creation semantics](https://man7.org/linux/man-pages/man2/open.2.html).
Nonmember creators do not acquire setgid through the requested mode.
File allocation precedes both creation and truncation. The legacy backend path
uses the same retained consumer and no longer creates missing ancestor directories.
VFS removed the late tmpfs check and duplicate truncation, and synthetic mount
fallbacks cannot replace backend EACCES or other authoritative failures.
Complete pathname component traversal and other backend open/create migration
remain required.

XFS legacy-path and retained-lookup opens also consume an explicit snapshot under
the mount metadata lock. Parent search and final inode access precede opening;
missing targets require parent write/search before inode allocation or directory
mutation. Both paths initialize creator UID/GID, setgid inheritance and the exact
masked mode, including zero. They allocate File and per-open storage before
committing a creation. The legacy path preserves its authoritative exclusive
lookup, stale-entry repair and inode-cache collision checks. Retained owner-beneath
opens keep their original snapshot and binding. An authorized File retains its
open rights after later chmod; generic truncation still takes inode I/O before
mount metadata, outside the backend open guard. Denied opens return no File and
cannot reach that truncation.

FAT32 open now receives the same explicit caller snapshot and returns backend
errno to both VFS dispatchers. Its fixed 0:0 ownership and 0755 directory projection
deny nonroot creation; final file permissions are checked before a File can reach
generic truncation. Exclusive create, directory requirements and invalid access
modes fail before publication. Creation preallocates File/Node storage before
writing the directory entry. FAT continues to project created files as 0:0/0644
regardless of requested mode; it cannot persist Unix ownership or arbitrary mode
bits. FAT operations now share a sleeping mount mutex and retain shared open nodes; full traversal and remaining namespace/cache qualification are still required.

Devfs opens take an explicit snapshot and check node access, directory requirements,
exclusive creation and nofollow before any Device::open callback or open-reference
publication. Authorization holds the bounded node metadata lock; device callbacks
run after that lock is released. Existing PTS namespace protection and WKI pins
remain in place. Callback failures retain their negative errno and release the
wrapper/pins without calling close on a failed open. Exec console fallback uses
the subject task's snapshot, and boot console setup passes explicit bootstrap
credentials. Devfs readlink now reads pinned symlink metadata directly, avoiding
PTY allocation or other device effects during a non-symlink probe. This does not
yet enforce device-specific control authority, default device permissions, complete
directory traversal or retained devfs open bindings.

Global procfs nodes now have an explicit classification. Perf draining and kernel
address diagnostics require KERNEL_DEBUG, as do allocator tracking and reclaim
controls. WKI fault controls require WKI_ADMIN. Public aggregate statistics retain
their existing read access and text formats. Both VFS open paths supply the captured
snapshot; protected FileOperations recheck the active caller before cached output,
allocation, ring draining or any command effect. A descriptor opened by root does
not retain diagnostic authority after a credential drop. Protected content uses a
per-open mutex; command grammar and runtime chaos gates remain unchanged.
Metadata-only lookup remains available and reports 0400 for protected diagnostics
and 0600 for controls. Private process views use the additional target policy
below; public status/stat/limits and remote caller mapping remain separate audits.

Procfs descriptor reopening and FD readlink now share central private-process
policy. Same-process access remains available when nondumpable; cross-process
access requires the complete trace identity, dumpability and authority-limit
checks. The target Task is retained while a credential observation authorizes
FD retention. A second observation rejects a changed generation with EAGAIN,
releasing the temporary File reference before publication or link output. This
also detects a change away from and back to identical credential values. No VM
mutex is added beneath VFS namespace locks. Both VFS open callers preserve an
authoritative denial, and procfs/prefix symlink resolution propagates it so openat
and task aliases cannot bypass the policy. Open-intent lookup preserves a final procfs FD link instead of resolving it to
readlink display text. This lets pipes and protected inherited files reopen as
actual descriptions; ordinary and prefix aliases still resolve. Thread aliases
validate group membership, and nofollow/exclusive/directory flags are checked
before descriptor installation. Confined remote-export OPEN rejects preserved
procfs FD links: pathname exports do not authorize transfer of a serving process's
open descriptions. File reference ownership and shared
open-description behavior remain unchanged.

Private cmdline/maps/images/exe/cwd/root views and FD directories retain their
exact target Task until close. Opens, reads, link copies, directory enumeration
(including dot entries) and seeks recheck private-process authority. Cached data
is tagged with the target credential generation and regenerated after a change;
revoked access leaves the cache and output untouched. Generation races return
EAGAIN and exited targets return ESRCH, including reads of previously primed
content. The Task reference preserves identity, while maps/images separately pin
the page map against exit teardown under the existing VM publication guard.
Exit closes self-referencing procfs descriptors before retirement and releases
address-space and process-count resources independently of the retained Task.

Bounded Task path and File path locks protect private link copies and published
path writers. Allocations, file release and retired-string deletion occur outside
these spinlocks. Exec publishes its path and credential generation under the
Task path lock, avoiding a VM mutex beneath VFS namespace lookup. Cmdline/name
copy and PR_SET/GET_NAME use VM publication protection; usercopy and retired-name
deletion stay outside that guard. VFS propagates procfs enumeration errors and
continues calling its backend after cached EOF so cached directory length cannot
bypass later revocation. The mlibc directory wrappers propagate refill errors:
readdir sets errno and returns null, while readdir_r returns the positive error
and clears the result pointer. Failed refills preserve stream indices and the
caller-supplied readdir_r entry instead of asserting. Private content caches use the existing per-open mutex.
Public status and stat copy bounded process names under VM/path publication
protection. Status Name exposes a basename, and both public name fields replace
control characters so a name cannot inject apparent credential rows. Public
status/stat/statm/meminfo and protected memacc accounting pin the Task's page map
against exit while holding the VM publication guard; retired address spaces
contribute zero memory. Memacc command/name fields use bounded copies while
retaining their existing diagnostic authority requirement. Every ordinary procfs
content cache and cached-size seek now uses its per-open mutex; private and limits
readers retain their own nonrecursive lock paths.

Public WKI launcher/runner/remote-PID views retain their target Task while copying
identity. Submitter hostname and proxy/remote PIDs use bounded value snapshots
under the Task path lock; mapped published writers use the same lock. Peer and
local hostname observations use a copy API under the peer lock, with formatting
and Task release outside these lock spans. A vanished target yields ESRCH on
first generation without output; previously generated public content retains
its existing per-open snapshot semantics. Placement metadata confers no remote
authority, and credential mapping/authentication and wire layouts are unchanged.
Legacy borrowed hostname consumers, other WKI identity readers, public
wait-channel/scheduler scalars and general Task/path readers still need their
own audits; these locks do not synchronize unconverted readers. WKI preflight
path staging retains its existing semantics, with bounded path-copy protection;
authenticated remote identity mapping and complete coredump capture qualification remain pending.

Coredump output uses the kernel worker's explicit Task and an exclusive,
no-follow create with mode 0600. A pre-existing file, hardlink, symlink or
directory cannot receive private dump bytes or be truncated; a name collision
drops that best-effort output. Files retain administrator ownership because the
format includes kernel diagnostic addresses. Filename syntax, v4 bytes,
dumpability admission and resource accounting remain unchanged. A write failure
leaves a private partial artifact and logs the failure; the worker cannot safely
unlink by pathname after the original entry may have been replaced. Producer
credential-generation/exec races and fault-register journal disclosure still
require separate audits before coredump authorization can be marked complete.

Global and module journal device reads require central KERNEL_DEBUG authority.
Readable opens and every read capture current credentials, including zero/short
reads and inherited descriptors; denial leaves bytes and the sequence cursor
unchanged. Read polls return POLLERR after authority loss without exposing record
availability, and denied waiter registration does not modify the list. Write-only
opens, userspace log submission and write readiness remain public. The 608-byte
JournalRecord ABI, module filters, emission and serial routing remain unchanged.
Journald validates retained regular, single-link files owned by its current
identity before appending, rejects final symlinks, and sets files to 0600 and the
journal directory to 0700. Existing archive modes are tightened at startup.
Bootstrap/staging directory creation uses 0700 too. A historical descriptor
opened before a mode change cannot be revoked by chmod; legacy descriptor
migration, namespace races and service identity changes need further qualification.
Fault details still reach administrator diagnostics; dumpability-sensitive
emission and capture-generation races remain separate work.

This is partial VFS migration. Full traversal, other backend creation ownership, write-side
set-ID clearing and remote identity mapping remain required before the VFS
inventory can be marked enforced. General devfs namespace synchronization,
especially `/dev/nodes`, also remains pending; node metadata locking does not
replace a tree lock.

## Version 1 authority inventory

This inventory records required decisions and historical audit findings. Current
per-domain status is in the machine-readable manifest: ten enforced and eleven
pending after9GP. Earlier notes below can precede later qualification. Each domain
needs production review, denial-side-effect and positive compatibility evidence
before its status can become enforced.

The machine-readable contract is
[`credential_authority_v1.json`](../tests/security/credential_authority_v1.json).
Run `python3 scripts/test/credential_policy_audit.py` to check the exact remaining
root comparisons (currently zero legacy exceptions). The gate scans C/C++
source outside the central security policy and kernel test fixtures, ignores
literals/comments, and rejects added,
duplicated, or stale exceptions. Existing exceptions are migration debt, not
approved permanent policy. `--require-complete` additionally rejects any pending
operation or remaining legacy check. Semantic denial tests and code review are
still necessary: a spelling-based source gate cannot prove authorization.

| Domain | Source entrypoints | Required decision and historical audit finding |
| --- | --- | --- |
| Credential IDs/groups | `syscalls_impl/process/process.cpp`, `platform/sched/task.cpp` | Stable process-wide transition, saved IDs, eight groups; production transitions now publish one process snapshot; live syscall qualification is in progress. |
| Fork/clone/exec | `syscalls_impl/process/{process,exec}.cpp` | One inherited snapshot, atomic image/credential commit; ordinary exec refreshes saved IDs and respects retained mount nosuid; effective-ID changes require precommit sibling safe waits; broader forced interleaving qualification remains pending. |
| UID population and limits | `platform/sched/{resource,resource_process,service_resource}.cpp` | Preserve NPROC transfer admission, inherited ceilings and service budget ownership; no partial charge on credential failure. |
| Traversal/access | `vfs/{core.cpp,lookup.hpp}` | Check every directory with one identity, real IDs for access semantics and effective IDs for ordinary operations; lookup now retains the complete snapshot; traversal/backend coverage remains pending. |
| Open/create/truncate | `vfs/core.cpp`, `vfs/fs/{xfs/xfs_vfs,tmpfs,fat32}.cpp` | Check before object creation/truncation; preserve owner/group/umask and retained path bindings. Backend coverage requires audit. |
| Namespace mutation | `vfs/core.cpp`, backend lookup consumers | Parent write/search, sticky-directory ownership, rename/link/unlink on retained objects; no denied partial mutation. |
| Metadata/xattrs | `vfs/core.cpp`, `vfs/fs/xfs/xfs_vfs.cpp` | Owner/group membership, chown authority, set-ID clearing, trusted xattr visibility; XFS trusted namespace now uses the FILE_OWNER authority predicate; metadata semantics remain pending. |
| Mount/root changes | `vfs/mount.cpp`, `syscalls_impl/vfs/sys_vfs.cpp` | Root/mount authority before namespace or device mutation; audit trusted boot callers separately. |
| SHM | `syscalls_impl/shm/shm.cpp` | Enforced captured owner/creator IDs, lookup/attach/stat/removal permissions, and retained SHM_RDONLY write restrictions through protection changes, fork/shared address spaces and mapping replacement. |
| Signals | `syscalls_impl/process/process.cpp`, scheduler group delivery | Caller real/effective UID versus target real/saved UID; SIGCONT session exception; sig=0 and every group recipient checked before forwarding or queueing. Local kill now checks each recipient before queueing/forwarding; remote mapping still needs qualification. |
| Ptrace | `platform/debug/ptrace.cpp` | Stable caller/target identity, saved IDs, privilege-changing exec and dumpability; local attachment and existing requests now check all IDs, target groups, authority limits and dumpability; bounded register-state validation is implemented; live register tests pass; remote qualification remains pending. |
| Priority | `syscalls_impl/process/process.cpp` | Target ownership and priority-raising authority; enforced ownership and raising policy; serialized check/commit prevents concurrent priority regain, and denied writes preserve priority. |
| Power | `platform/power/power.cpp` | Explicit power authority before shutdown state; enforced by central POWER authority, including preparation and command no-ops; live denial preserves IDLE state. |
| Init control | `syscalls_impl/process/init_control.cpp` | Central control authority and authenticated PID 1 publisher; preserve public status/private resource observations. |
| Procfs/coredump | `vfs/fs/procfs.cpp`, `platform/dbg/coredump.cpp` | Stable owner/debug authority, repeat checks on retained descriptors, no privileged core disclosure. |
| Devices/debug | `vfs/fs/devfs.cpp`, `platform/sys/syscall.cpp` | Device-specific control policy; DEBUG cli/sti now requires KERNEL_DEBUG before either instruction; devfs device controls remain pending. |
| Network/hostname | `syscalls_impl/net/sys_net.cpp`, `syscalls_impl/process/process.cpp` | Raw/control socket and configuration authority; hostname and network changes need individual audit. |
| WKI control/admission | `net/wki/{auth,remote_compute}.cpp`, process placement/control | Authenticated node plus configured identity mapping; identity and resource admission must precede remote work. |
| WKI VFS/IPC/devices | `net/wki/{remote_vfs,remote_ipc,dev_server}.cpp` | Explicit mapped request identity; worker credentials and peer authentication cannot confer arbitrary root. |
| Init service accounts | `modules/init/src/{service_manifest,services}.cpp` | Deterministic user/group resolution, bounded supplementary groups, umask, fail-closed drop order, no exec after partial failure. |
| Rootfs ownership | `scripts/build/`, `configs/rootfs/` | Deterministic account IDs and least-privilege ownership, explicit service device/control needs; deeper account generation audit pending. |

## Integration and verification order

1. Qualify the value model and IRQ-safe publication in host sanitizer tests and
   isolated KTEST. Keep this evidence distinct from production syscall tests.
2. Replace Task credential storage and migrate fork/clone/exec/syscall readers
   and writers, including NPROC transaction ordering and sibling publication.
3. Migrate one authorization domain at a time, with an operation manifest and
   a static gate that rejects new raw root checks outside the policy layer.
4. Resolve service account/device requirements before enabling privilege drops.
   Run local denial probes and successful service operations together.
5. Negotiate/map WKI credentials fail-closed and qualify mixed policy, rotation,
   denial, and multi-node service behavior using rootless live VMs.

Normal `Build WOS`, ABI parity, host sanitizer tests, isolated rootless KTEST,
and local/distributed runtime evidence remain required. A passing model does
not establish production enforcement.

Tmpfs and XFS unlink/rmdir now authorize the lookup credential snapshot against
the retained parent and canonical target while holding the metadata writer lock.
The parent requires write and search access; sticky directories additionally
require parent ownership, target ownership, or FILE_OWNER. Symlinks use their
own inode owner. Deleting a name does not require target write access. Denials
precede detachment, link changes, emptiness scans and XFS transaction allocation;
existing file descriptors retain their inode after successful removal. Legacy
tmpfs pathname deletion now uses the same retained lookup backend. FAT applies
its immutable root-owned 0755 parent projection before cluster/directory writes.
Local, legacy and confined owner dispatch pass an explicit snapshot. This does
not yet establish authenticated WKI identity mapping, full ancestor traversal,
or authorization for rename; namespace enforcement
therefore remains pending.

Tmpfs and XFS directory descriptors recover their current path from retained
backend identity when their original pathname is absent or obsolete. Recovery
holds namespace and backend metadata locks, copies the exact retained mount
path, and checks parent links plus indexed XFS directory entries. Removed
directories fail without inventing a path. Dot/parent and symlink fallback use
the recovered base; failed walks recheck namespace generation too, so a
concurrent rename requests a bounded retry. Published File path text is never
refreshed in place.

An open file cannot republish shared metadata under an obsolete pathname.
Shared publication checks namespace/mount binding under the namespace mutex;
optional close/fstat publication skips on contention. Successful metadata
changes on multiply linked non-directories invalidate all cached name aliases.
Retained tmpfs/XFS fstat caches also miss when their original namespace binding
is stale, including the nonblocking fast path, and read the retained backend.
This conservatively loses descriptor-cache hits after unrelated namespace
mutations; correctness currently takes precedence over preserving those hits.
Successful opens record the namespace generation after backend creation;
existing-file create/truncate opens preserve that generation. Failed creates
conservatively invalidate because a backend can fail after disk publication.
The publication mutex remains held through cache updates by the
enclosing publication operation, preserving ordinary created-file close caches.
These changes preserve credentials, backend ownership and syscall/wire layouts;
full ancestor authorization and remaining namespace operations are still pending.

Tmpfs and XFS mkdir now require parent write/search permission using the
retained lookup Snapshot under the backend writer lock, before allocating a
directory or transaction. A cached existing-name result cannot bypass that
policy. New directories use effective creator IDs, inherit a setgid parent's
group and directory setgid bit, and preserve explicit mode 0000. Process mkdir
applies the task umask once; confined owner operations receive the already
masked mode and explicit lookup credentials. XFS commits those IDs with the
new inode while retaining its existing parent-link rollback. Denied operations
leave backend parent metadata, directory generation, output and namespace
unchanged.

An internal directory-creation lookup intent preserves the final name instead
of following a final symlink, including with trailing separators. Existing
regular files and symlinks return EEXIST after parent authorization; dangling
symlink targets remain absent. Intermediate directory symlinks still resolve,
and creation through renamed directory descriptors uses the retained parent.
This does not yet establish full ancestor checks or authenticated remote identity
mapping. FAT directory creation remains unsupported.

Tmpfs and XFS symlink creation uses one explicit credential Snapshot and checks
parent write/search permission under the backend metadata lock before target,
inode or transaction allocation. New links use the creator's effective UID and
GID, with the parent's group when its setgid bit is set. Link mode stays 0777,
independent of umask and without inheriting directory setgid. Target text is
stored literally; creating a link does not open or authorize its target.
Legacy tmpfs dispatch now uses a retained lookup and copied metadata instead of
mutating an unlocked borrowed parent. Local, legacy and confined-owner paths
all pass explicit credentials, preserving the existing syscall and wire layouts.
Denied backend operations leave outputs, parent metadata and namespace unchanged.

Symlink creation has a separate internal lookup intent that never follows its
final entry, even with trailing separators. After parent authorization, an
existing name keeps its backend collision errno and a missing directory-required
name fails ENOENT without creating a link. Remote requests preserve that trailing
requirement in path text on the existing wire, including after a local prefix
redirect. Other lookup intents retain their existing semantics.

Hardlink creation on tmpfs/XFS requires source-parent search and destination-parent
write/search with one snapshot under the backend metadata lock. Directories
remain forbidden. The caller must own the source or hold FILE_OWNER; otherwise
only a regular source with caller read/write access is permitted, with either
set-ID bit rejected, matching WOS exec even when group-execute is clear. Foreign symlink/device sources
are rejected too. This prevents services from pinning privilege-bearing or
inaccessible foreign inodes under names they control. Parent denials use EACCES
and protected-source denials use EPERM. Hardlinks retain canonical source
UID/GID/mode and content, including for sticky-directory removal.

Legacy tmpfs link dispatch retains both lookups through validation and creation.
XFS keeps its source/parent transaction before-images and publishes outputs only
after commit. Paired VFS consumers reject different task IDs or credential
snapshots before mutation. Destination lookup never follows its final entry;
missing trailing-slash destinations fail ENOENT and existing names EEXIST.
Source directory requirements are retained, and explicit source-symlink follow
still authorizes the resulting inode. Full ancestor traversal, rename and remote
identity qualification remain pending; FAT/remote hardlinks remain unsupported.

Successful XFS hardlink creation invalidates cached metadata for all aliases
before publishing the new name, including when retained path text is obsolete.
Path-only invalidation could leave a warmed older alias with an obsolete link
count. This uses the existing conservative alias invalidation shared by metadata
changes; denied or failed link operations do not invalidate or publish.

Tmpfs rename uses one explicit Snapshot for source-parent write/search and sticky
source-owner checks, destination-parent write/search and sticky displaced-owner
checks, and source-directory write permission when moving between parents. Both
lookups remain pinned under the tree lock. Authorization, root/dot rejection,
type compatibility, destination emptiness and bounded ancestry checks precede
allocation or detachment. Destination child capacity is reserved before mutation,
so attachment cannot allocate after a replacement has been removed. Retained
replaced inodes remain alive; moved ownership, mode and data remain unchanged.
Failure leaves caller outputs, parent generations and entries unchanged.

Canonical same-inode rename is a no-op even for distinct hardlink entries. Its
success output reports no namespace change, so local, legacy and confined-owner
VFS consumers preserve both path cache entries. Real changes invalidate source
and destination metadata, including other hardlink aliases affected by canonical
ctime or displaced link-count updates. Legacy tmpfs rename now uses retained
backend lookups and copied metadata; its borrowed-parent and raw-stat helpers
have been removed.

Rename lookup preserves the final entry with trailing separators and retains its
directory requirement. Lexical final dot/dotdot components are rejected before
canonicalization can erase their meaning. Full ancestor traversal and remote
identity mapping remain pending. The VFS mutation manifest therefore remains pending.


XFS pathname rename now acquires retained pairs and uses the same credential
consumer as local and confined-owner lookup paths. Metadata locking covers both
parent/sticky decisions, cross-parent directory write, type/empty/cycle checks,
and parent link-count bounds before a transaction is allocated. The transaction
captures the moved inode, displaced inode and both parents before changing
entries, timestamps, parent references or link counts. Replaced directories have
zero links; moved inode ownership, mode and content remain unchanged.

The directory reparent helper validates authoritative `..`, preserves shortform
parent widths when possible, and converts to block format when the parent's
high-inode classification changes. Block/leaf/node parent updates reuse the
transactional directory remove/add helpers for data, index and checksum changes.
Cancellation restores captured inode forks, directory generations and buffers;
caches are invalidated with the existing rollback mechanism. Same-inode rename
reports no namespace change; real changes invalidate metadata for both affected
paths and canonical hardlink aliases before publication. Syscall and wire
layouts are unchanged. Remote identity mapping and full VFS authorization
qualification remain outstanding.


FAT rename uses the same explicit credential snapshot and fixed root-owned
parent/object projections. Parent authorization precedes root, type, trailing
separator, emptiness and bounded ancestry checks. Directory read failures and
corrupt/cyclic chains propagate errors; they cannot prove a destination empty.
Same-SFN rename, including case-insensitive aliases, preserves both cache keys.
FAT metadata and I/O now share a sleeping mount mutex. Internal parent lookup
uses a private already-locked open helper to avoid recursive acquisition.

Rename prepares directory and replacement-FAT before/after images before any
write. It preserves source attributes, data references, size and timestamps,
updates a moved directory's verified `..`, and handles LFN deletion/insertion
across existing directory clusters. A full destination fails ENOSPC without
source removal; directory extension remains unsupported. Replacement-chain
reclamation rejects source-chain overlap. Reported write or flush failure
restores every attempted image, including a potentially partial failed write.
If restoration also fails, the operation reports EIO and freezes the mount
read-only. This is recovery from reported I/O errors, not crash atomicity.
FAT public opens now share a node for each linked SFN location. Successful
rename relocates the source node and marks a retained replacement unlinked;
private parent lookups remain temporary. Unlink and rmdir use prepared removal
and defer reclamation while descriptors remain. An unlinked node reports zero
links and writes only its retained data, never a reused historical SFN. Final
close reclaims its chain; a reported I/O error retains a zero-reference orphan
for sync/unmount to retry. Failed unmount leaves the mount context intact.

FAT descriptor stat caches reject stale pathname bindings. Stream identities
use the retained mount; writes invalidate that mount's stream cache, and a
write through an obsolete pathname invalidates metadata conservatively without
publishing under that name. Open file positions and flags remain per-File.
General FAT write/truncate I/O failure handling, append atomicity, full ancestor
traversal and authenticated remote identity qualification are still required.

The current remote rename response cannot distinguish a same-inode no-op.
Successful remote rename therefore invalidates both path keys without publishing
a fabricated missing-source entry. Authenticated remote credential mapping and
multi-node qualification remain separate pending requirements.

Tmpfs and XFS retained lookup acquisition now takes the request's explicit
credential snapshot and checks each directory's search permission before looking
up its next component. XFS acquisition walks authoritative directory entries
instead of treating a cached parent pathname as permission evidence. Common VFS
acquisition captures the snapshot before resolution and passes it through to the
backend and consumer. DAC override permits directory search even when its execute
bits are zero; executing a regular file still requires an execute bit.

The common local walker now checks original components before lexical reduction
or symlink-prefix caches. It captures one credential snapshot, searches each
current directory, and binds the next component from retained TMP/XFS objects.
Relative symlinks restart at their containing directory; absolute symlinks restart
at the copied task root. Dot components retain directory identity, and mount-root
ascent validates the final mount instead of requiring the original FD's mount.
Directory FDs remain usable below an inaccessible ancestor while search on the
retained directory itself is enforced. Root containment is checked explicitly;
`strip_task_root_prefix` alone does not establish containment.

Native binding steps hold namespace publication individually and validate one
namespace/mount generation throughout the walk. Procfs lookup and symlink reads
run outside that mutex because releasing a borrowed remote FD can wait for its
owner. The explicit procfs readlink entrypoint receives the walk's credentials
and caller process ID. Final publication and consumers revalidate bindings. After
four optimistic open retries, TMPFS/XFS paths get one attempt with namespace
publication held across the complete walk and consumer. That fallback declines
other backends before their lookup/read callbacks and preserves all credential,
mount and native binding checks. XFS separately tracks lookup invalidation from
its sync mutation counter. Transactions invalidate lookups by default; only
regular-file data allocation, writes, truncation and dirty-file flushes preserve
the lookup counter. Captured identity or link-count changes override that
exception. Namespace and permission transactions still invalidate retained
lookups, while sync observes the same dirty-data counter as before.
`chdir` and `fchdir` use this lookup and require final directory search before
publishing cwd. Access consumers use the retained snapshot and complete mode.
Removal preserves final symlinks and rejects incompatible trailing-directory
syntax before detaching an entry.

DevFS permits devices with child namespaces, including journal module endpoints.
A metadata-locked marker records that kernel-declared namespace. Traversal uses
its public directory mode; device open/read authorization retains its own mode
and policy. Ordinary devices without children still reject traversal.

The latest normal VM passes all 53 credential subprocess cases (1,449 checks).
Earlier common-walker qualification also covered 33 focused traversal checks
and retained-dirfd identity stress. The latest isolated diagnostic KTEST run
passes 10,086 checks with zero failures.
Native cwd established by `chdir`/`fchdir` now retains its TMP/XFS directory
identity with mount busy accounting. Threads use their retained process owner;
fork and local exec inherit the anchor under the owner path lock. Relative
lookups retain that identity after rename or pathname reuse. `getcwd`, procfs
cwd links and compatibility text producers recover its current authoritative
path. Native recovery uses filesystem locking and namespace/mount epoch checks;
procfs consumers do not recursively lock namespace publication. The syscall
getcwd wrapper adds one serialized native recovery after four epoch retries;
the procfs-callable helper continues to validate its epoch without that lock. Preparation
requires directory search, without granting a descriptor or requiring directory
read access. Active exit drops process anchors after descriptor/child cleanup.

The native cwd probes cover rename/reuse, thread sharing, fork/exec inheritance,
execute-only search, deleted-cwd denial and mount busy-pin release. Concurrent
cwd stress completed 3,200 renames and 4,800 retained reads with no mismatches or
retries. Initial cwd supplied only as text, root inode retention, remote/nonnative
cwd anchors and root-domain transitions still need migration. Routed/remote
paths, owner-pre-resolved paths, nonnative directory-FD fallback and synthetic
final paths also remain pending. The traversal manifest remains
pending until those paths and authenticated remote identity mapping are covered.

Rootfs staging declares the installed root directory as `0755`, independent of
`mktemp`'s private staging mode. Both full and incremental manifests track that
mode; incremental updates encode the root as `.` and keep `/` out of removable
managed paths. Root updates are restricted to verified directory metadata.
Incremental staging also preserves the existing journal directory policy `0700`.

Public `mount`, `umount` and `pivot_root` now share
`authorize_mount_administration`: effective UID zero and a retained `MOUNT`
authority are both required. Each admission decision uses one immutable
credential snapshot and rejects callers before target creation, device or peer
resolution, cache invalidation, export draining or namespace mutation. Existing
argument staging and shutdown errors remain in their original order. Trusted
internal boot and automatic mount APIs remain separate. This admission check
does not yet qualify snapshot propagation through mount preparation, concurrent
credential changes, internal automatic-mount policy or authorized-failure rollback;
the mount manifest therefore remains pending.

Public mount now requires an existing directory target. It returns ENOENT for a
missing target and ENOTDIR for a regular-file target, without implicitly creating
a directory. Init fstab and required tmpfs setup already create their targets
explicitly. This corrects the former missing-target success behavior so failed
mount preparation cannot leave an unintended namespace entry behind.

The target is retained and checked before peer discovery, device resolution or
backend preparation. Its lookup uses the same admitted Snapshot in both native
component traversal and fallback attempts. Final publication revalidates the
lookup and central MOUNT authority; native target metadata comes from retained
TMP/XFS objects. Remote attach holds no namespace lock and uses the same retained
target at publication. Nonnative target metadata still uses the existing shared
stat adapter; it is not yet evidence of complete nonnative Snapshot propagation.
Automatic mounts, complete root/credential commit serialization, device-probe or
filesystem-recovery side effects, and authenticated remote mapping remain pending.

Userspace can now activate irreversible process restrictions through the existing
WOS `prctl` operation (`ker::process::prctl`, passing all five arguments).
`CredentialControl` and the fixed `CredentialAuthority` domain numbers are declared
in the mirrored kernel/libc process ABI headers.

| Control | Selector | Arguments and result |
| --- | --- | --- |
| SET_NO_NEW_PRIVILEGES | 38 | arg2=1, remaining arguments zero; returns zero |
| GET_NO_NEW_PRIVILEGES | 39 | all arguments zero; returns zero or one |
| GET_AUTHORITY_CEILING | 0x574f5301 | all arguments zero; returns the 16-bit ceiling |
| DROP_AUTHORITY_CEILING | 0x574f5302 | arg2=new subset ceiling, remaining arguments zero |

Unknown bits and nonzero unused arguments return EINVAL. Reintroducing a removed
bit returns EPERM without changing state. These controls require no privilege to
restrict the caller's own process. Siblings share the policy; independent children
inherit it without sharing subsequent changes. Fork and local exec preserve both
restrictions. No-new-privileges prevents set-ID exec from changing effective IDs;
it does not remove existing saved IDs or change their syscall semantics. Ceiling
bits restrict effective-root authority and never grant authority to nonroot users.
A ceiling alone does not suppress set-ID identity changes; service launch must also
set no-new-privileges and discard saved IDs when adopting an unprivileged account.

The current WKI task identity carries six UID/GID values but omits supplementary
groups, dumpability, NNP, the authority ceiling and sealed service privileges.
Sender admission now compares its entire stable snapshot with the exact value the
receiver reconstructs from those six IDs, using the same pure reconstruction
function at both ends. Only an exact round-trip is admitted. This prevents group
loss from selecting more permissive other-mode bits and prevents reconstruction
from changing dumpability. Nonroot processes made dumpable by ordinary exec also
need the future credential format; the compatibility guard rejects that mismatch.

Unsupported snapshots stay local for optional placement; strict placement fails
with ENOTSUP before IPC export. Both direct submit paths reject them before
peer/resource/payload work and serialize the same admitted snapshot. Saved IDs,
wire layout and existing cleanup remain unchanged. Reconstruction rejects every
sentinel ID without changing its output. This remains an interim sender guard;
authenticated peers still need configured receiver identity mapping, and remote
VFS workers still need explicit mapped authority. Full credential transport and
remote policy qualification remain incomplete.


Init now requires explicit user, group, supplementary groups, umask, NNP and
authority ceiling for every service. Bounded account parsing rejects duplicate
names/IDs and unknown references. The manifest and account files must be regular,
root-owned, non-group/world-writable, and opened without following a final
symlink. Every identity resolves before resource configuration or service launch.
The forked child sets NNP, replaces supplementary groups, permanently sets GID
then UID, applies umask and ceiling, and verifies all resulting values before
exec. Supported privileged `setgid`/`setuid` set real/effective/saved IDs together;
WOS libc currently does not implement `setresgid`/`setresuid`. A failed drop exits
through the existing setup-error pipe and does not change PID 1 credentials.

HTTPD runs as deterministic locked UID/GID100, no supplementary groups, umask0077,
empty environment, null stdio, NNP and zero authority. Its mount endpoint returns
403 before any mutation; file permission denials return403. Other service
accounts and domain reductions remain pending. See `docs/init_supervisor.md` and
the disposable service identity fixtures for the launch contract and probes.
The init-accounts/rootfs manifest entries remain pending until all service and
remote requirements are qualified.

Local network syscalls now require `NETWORK_ADMIN` for legacy interface/IPv4
route mutations, NETCTL IPv4/IPv6 address and route changes, link changes, and
device queue affinity. Each fixed-size request is copied before a single
credential snapshot is admitted, preserving EFAULT for bad request memory.
Denied callers return EPERM before device lookup, callbacks, route/interface
mutation, or WKI notifications. Getters and interface/address/route enumeration
remain public.

Raw socket creation validates family/protocol, then requires the same authority
before reserving a descriptor, allocating buffers, or registering a raw receiver.
The base socket type is checked after removing creation flags. Ordinary TCP/UDP
creation and existing bind/listen/connect behavior are unchanged. A caller must
have effective UID0 and retain the NETWORK_ADMIN ceiling bit; saved UID0 or
other retained authority bits do not suffice. These gates do not change internal
kernel network setup or revoke an already delegated raw descriptor. Remote
control, descriptor delegation, hostname qualification, and authorized
multi-field transaction/race behavior remain pending; the network audit entry
is not marked complete.


### Bounded service privilege delegation (stage 9D)

Two additive prctl controls preserve the existing 16 root authority bits:
SET_SERVICE_PRIVILEGES (0x574f5303) accepts a two-bit mask in arg2 with all
spare arguments zero. GET_SERVICE_PRIVILEGES (0x574f5304) takes zero arguments
and returns that mask plus bit8 when sealed. Unknown bits/arguments yield
EINVAL. Bit0 grants journal reads only, bounded by KERNEL_DEBUG in the ceiling;
bit1 grants existing network administration/raw-socket creation, bounded by
NETWORK_ADMIN. Neither changes effective UID or grants general diagnostics.

Installation requires NNP. The first nonzero set requires effective-root
INIT_CONTROL and the requested operations' root ceiling bits. Every first set,
including zero, seals the mask. Subsequent sets only reduce it (EPERM on gain),
including while root retains INIT_CONTROL. Credential transactions publish the
mask/seal atomically and clear dumpability on a change. Fork/clone/local exec
inherit the fields; NNP suppresses set-ID exec and rejects legacy remote exec.
Dropping a matching ceiling bit permanently disables that delegated operation.
Journal reads/polls reauthorize retained descriptors; raw creation admission
does not revoke already-created raw sockets.

Tracing/private process descriptors compare effective service permissions,
including equivalent root permissions, and cannot regain an unlocked installer
from a sealed caller. Existing ID/group/dumpability/ceiling/NNP rules still apply.
Init requires explicit service-privileges and verifies the mask/seal before exec.
Stage9D initially left production grants at none; stage9E below enables the
production journal profile; stage9F adds the production network profile. Disposable service grant
fixtures exercise nonroot operations and set-ID suppression separately.


Stage9E transitions production journald to UID/GID101, NNP, sealed journal-read
and ceiling16384. Trusted init prepares its declared private state before any
service starts, preserving active/archive contents and0700/0600 modes while
migrating root:root or validating existing service ownership. Full/delta image
account generation agrees; images retain initial root-owned state until this
bounded boot transition. No recursive chown or host staging ownership change
occurs. Stage9F below covers netd; remaining services are still pending.


Stage9F assigns netd UID/GID102, sealed NETWORK_ADMIN (mask2), ceiling4096,
NNP, empty environment, null streams, no supplementary groups and umask0077.
Trusted init creates fresh runtime resolver state from a bounded root-owned seed
before any service launches. `/etc/resolv.conf` is a root-owned symlink to the
owned0755 `/run/netd` directory's public0644 resolver. The single DHCP writer uses
retained directory descriptors, exclusive/no-follow temporary creation, checked
writes, and atomic rename; unsafe existing files reject publication. Rootfs
account generation is deterministic in both staging paths. This does not grant
netd DAC override, ptrace, journal reads, power, init control or WKI administration.

Fatal signal teardown must also preserve the scoped resources held while a
restricted service is in a syscall. Handoff delivery requires a validated
USER_RETURN frame before consuming a fatal signal or process-exit request.
A parked kernel frame resumes its syscall instead; interruptible waits return
EINTR and release retained files/reservations before ordinary user-return signal
delivery exits the process. Without this boundary, killing a daemon during
accept could leave its listener binding alive after waitpid reported its death.
The disposable-guest regression is `tests/fixtures/service_socket_exit.py`.


SIGKILL is checked ahead of active-handler deferral, ordinary pending-signal
selection, and signal-frame restoration at safe user-return boundaries. Incoming
handoffs leave it pending until the target owns the current kernel stack. Handler
masks and restored user signal-frame masks exclude SIGKILL and SIGSTOP, matching
existing sigprocmask/sigsuspend/exec admission. Other mask bits and the176-byte
signal-frame ABI are preserved. The native guest fixture
`tests/fixtures/service_signal_handler.c` covers syscall, parked, and userspace
handler loops with empty/full handler masks and controlled sigreturn mask edits.
Remote signal policy and broader signal races still require qualification.

STOP bypasses active-handler deferral without creating another signal frame.
Scheduler selection moves stopped candidates back to waiting even after generic
wakes; timer scheduling also parks an already-current stopped task. Late-return
stop delivery requests a timer switch without setting a syscall transition flag
that could indefinitely prevent timer preemption. Child-event slot consumption
retries deferred publication after releasing the lifecycle lock. KILL bypasses
event backpressure. New STOP/CONT generation atomically discards opposing pending
bits; retrying an older event preserves a newer CONT.

The native `tests/fixtures/service_signal_stop.c` verifies24 single-threaded child
cases using explicit SysV shared progress: syscall, sleeping and pure userspace
handlers, empty/full masks, benign signal wakes, quiescence after repeated stops,
CONT progress and KILL with a continued-event slot still occupied. The previous
baseline failed18 stop cases; the ledger retains the failed intermediate
candidates that demonstrated why signal priority alone was insufficient.
SIGSTOP and SIGCONT now use a process-owned controller with retained thread
membership. Signal generation and thread admission share the owner resource
lock. Existing members acknowledge STOP only after reaching a safe stop boundary;
new members join an active stop without permission to execute userspace. Parent
STOP publication waits for every member. Deferred event publication runs outside
runqueue locks, retains the owner, and preserves continued-event backpressure.
CONT releases all members while queuing its catchable signal to one recipient.
KILL still uses the existing owned-kernel-stack teardown path. Failed thread
publication restores its earlier local gate and job-control pending bits.

`tests/fixtures/service_process_stop.c` previously failed 18 of 24 cases: PID
STOP reported completion while a worker ran, and PID CONT failed to resume a
worker stopped through its process group. All 24 cases now pass on the normal
kernel. `tests/fixtures/service_process_stop_race.c` additionally passes 96
cycles across a pure userspace worker, concurrent thread creation/exit, and a
41-member process exceeding the controller's 32-member wake batch. It checks
quiescence immediately after waitpid reports STOP, benign wakes while stopped,
resumed progress, exactly one CONT handler, and final KILL/reap. Five controller
KTEST cases cover partial-stop cancellation, all-member acknowledgment, event
backpressure, KILL escape, publication rollback, and CONT between local gate
establishment and acknowledgment. The local gate is established under the same
lock as CONT release so delivery cannot reassert a canceled stop afterward.

Default TSTP/TTIN/TTOU delivery now enters the same process controller. Their
ordinary generation does not stop a task: masks, caught handlers and ignored
dispositions are handled before a default action is accepted. Each member keeps
pending default-action eligibility under the owner resource lock; CONT clears
it so a previously selected but canceled default stop cannot restart the
barrier. Admission and failed-publication retry preserve earlier pending stop
eligibility. Internal member requests remain uncatchable, while the parent
STOP status retains the original TSTP/TTIN/TTOU reason. Process-group, TTY and
WKI signal producers select one process recipient for signals18..22.

The process and stress fixtures accept an optional19..22 signal argument. On
the final kernel all96 process cases and384 stress cycles pass across the four
signals, including immediate quiescence, thread churn, 41-member processes,
exactly one CONT handler, and KILL/reap. The three catchable-stop baselines each
failed12/24 process cases. `service_job_control_disposition.c` passes18 controls
for caught, ignored and blocked TSTP/TTIN/TTOU and CONT-before-unblock
cancellation; the baseline invoked two handlers in each of its three caught
group-directed cases. Four additional controller KTEST cases cover deferred
default action, original stop reason, cancellation across a new SIGSTOP, and
pending-stop preservation across publication retry.

These runs qualify the exercised local paths. Ptrace interactions, session and
orphaned-group rules, additional concurrent producer interleavings and
authenticated remote signal isolation remain qualification work. Selecting one
recipient does not establish general process-wide signal-disposition or
unblocked-thread selection semantics.

The separate `tests/fixtures/service_pipe_signal.c` checks an observed
`pipe_read` wait before KILL, TERM, data or EOF, with and without a child-exit
wake, at UID/GID 0 and 100. Its 480-case run passed with varied child-event
delays. This does not establish that the earlier stuck pipe waiter is fixed;
the original failure remains unreproduced on the current kernel.


Host-key generation now runs as locked UID/GID103 (`keygen`), with NNP, no
supplementary groups, a sealed empty grant set, zero authority, umask0077 and
an empty environment. Init prepares the reserved `/etc/dropbear` namespace
before starting any service, using the same bounded retained-inode transaction
as journal state with key-specific names and guarded legacy0755 migration.
Files remain0600 and the owned directory0700; existing key contents are never
rewritten by migration. Unsafe namespace/metadata rejects before mutation;
operational failures roll back ownership and any directory-mode tightening.
See `docs/init_supervisor.md` for the exclusive-startup and rollback limits.

Live validation generated an RSA key after a root-owned set-ID probe confirmed
21 identity/no-regain/denial checks, passed15 synthetic state-adapter checks,
and restored the original key/production manifest unchanged across reboot with
SSH available. Both rootfs staging paths declare the same locked account and
private directory mode. Dropbear and disabled testd remain root/full-authority;
login/session privilege separation and authenticated remote identity mapping
are still required before this goal's service policy is complete.


Dropbear's authentication service field now requires the exact bytes and length
of `ssh-connection` before account lookup. The prior combined mismatch check
accepted incorrect equal-length names and valid prefixes with trailing bytes.
The live `tests/fixtures/service_ssh_auth_service.py` regression sends only
unauthenticated requests: seven malformed strings close the connection, and
valid controls receive the ordinary authentication-method failure response.
Normal Build WOS, authenticated SSH command execution and2041 native credential
checks passed. This closes a protocol-validation gap; the privileged network
parser and broker/session separation requirements remain open.


The integrated `svr_verify_pubkey_auth` API now verifies a complete signed login
request against explicit trusted account, authorized-key and session-ID inputs.
It checks exact signature framing before cryptographic verification, rejecting
incorrect container lengths and unsigned trailing bytes that the previous
embedded check accepted. It does not authorize keys or change authentication
state; the connection broker performs account/key policy lookup and owns
the fresh connection session ID. Existing security-key option flags remain part
of the trusted decoded key. Live agent-signed proof cases passed12/12, the
native verifier passed1141 checks across Ed25519/RSA/ECDSA, and native credential
checks passed2041/2041. Hardware security-key runtime coverage and full
broker/worker separation remain outstanding.


WOS now enables a persistent per-connection Dropbear KEX broker. It receives
bounded requests over inherited anonymous pipes, resolves algorithms locally,
generates fresh server ephemeral keys, constructs/signs the exchange hash and
retains the first session ID across rekeys. The worker keeps public host-key
objects and uses the returned connection secret with the existing transport
key derivation. Broker loss closes the connection with no local signing
fallback; EOF releases the helper. No new syscall or SSH wire layout is added.

Live tests cover five enabled classical algorithms with three rekeys each,
three hybrid algorithm names with authenticated64 KiB transfers and forced
rekeys, broker-loss/EOF cleanup, the21 authentication rejection cases and2041
credential checks. The broker also owns authentication as described below.
Privileged session creation still runs in the network worker. Moving it into
the broker, then enforcing worker identity/authority drops and inspection
limits, remains necessary before calling this full privilege separation.

The worker now forwards complete login requests over the inherited private
pipes before account lookup. The broker runs the existing account/root/group/
shell checks, authorized-key ownership and options policy, signature verifier,
failure delay and attempt limits against its retained first session ID. It
retains the successful account and key options and exports a bounded identity
record without password hashes. The worker installs that record only after
decoding the complete success response. Forced commands, PTY/forwarding flags,
security-key flags and permitopen list semantics survive the transfer. WOS's
disabled interactive PAM profile is unchanged; enabling PAM together with
this broker is explicitly rejected at compile time.

Live `service_ssh_broker_policy.py` passed13 checks, including unlisted-key
denial, retained failure counts, fixed usernames, forced-command/PTY/forwarding
policy, multiple permitopen entries, nonroot UID/GID/home, nonroot rekey and
private-host-key access denial. Its generated private keys stay in host memory;
only public keys reach the disposable VM. It restores account/key file contents,
ownership and modes and removes the temporary account home. Normal Build WOS,
the21 service/proof cases, five classical algorithms with three rekeys each,
three broker lifecycle checks and2041 native credential checks passed.

This transfers login authorization, not all session authority. PTY setup,
session fork/exec, signals, exit status,
stream transfer and teardown still need broker-owned state before the worker
can permanently discard login authority. Root logins still leave the worker
at root, and nonroot logins use the existing worker-side credential transition.

Login group membership is now retained with the broker's first account-policy
lookup. Restricted-group authorization and the eventual credential transition
use that same bounded list; later changes to `/etc/group` affect fresh
connections. Group overflow or database failure rejects authentication before
worker credential mutation. PTY account ownership uses retained UID/GID instead
of a second `getpwnam` lookup. This is a stable connection identity after
capture, not an atomic transaction spanning passwd and group files.

Live testing exposed root-owned default PTY slave nodes, which prevented
nonroot SSH PTY setup. Devfs now offers a credential-aware device-open callback;
PTMX uses the same snapshot that authorized the open to initialize its slave
UID/GID and mode0600 before namespace publication. Owned-node creation rejects
replacement of an existing name. Other device registration defaults, namespace
and endpoint lifetimes, and syscall/wire formats are preserved. The expanded
live policy fixture passes20 checks, including membership changes between the
method query and signed proof, account-ID changes before PTY creation, fresh
login updates, all eight groups and overflow rejection. The initial nonroot
PTY failure and subsequent passing run are recorded in the goal ledger.

Normal Build WOS passed. The corrected isolated diagnostic KTEST run passed
12040 checks, including supplied-snapshot PTY ownership, cross-user open/chmod
denials, and recycled-number ownership with a retained old inode. The first
KTEST run failed because the new fixture used an absolute path for the internal
devfs lookup; correcting that fixture required no production-code change.
The21 authentication service/proof cases, five classical algorithms with three
rekeys each, three broker lifecycle checks and2041 native credential checks
also passed. The authorization inventory now includes the PTY callback path
and corrects stale signal/keygen progress descriptions; broad device and service
domains remain marked pending.

PTY setup/cleanup still runs in the worker, including the existing tty-group,
mode and login-record policy. Broker session/stream ownership and permanent
worker privilege drops remain outstanding.

Command and subsystem decisions now use broker-owned per-channel records. The
worker forwards the complete SSH channel request; the broker checks its channel
binding, exact request name and complete command body, then applies its retained
forced-command policy and trusted SFTP path. Embedded NULs and trailing command
fields are rejected. Only a validated candidate replaces a prepared record.
Records have monotonic private IDs, are bounded by `MAX_CHANNELS`, and are
released on channel cleanup or connection EOF. Whole-connection cleanup closes
the broker pipes before channel cleanup, avoiding recursive RPC during failure.
Both ends of the private pipes are close-on-exec. SSH wire and syscall layouts
are unchanged. Actual process creation and stream ownership remain in the worker.

Live checks passed for ordinary commands and exit status, login shell, SFTP,
seven malformed/unsupported requests with subsequent channel recovery, 1,024
sequential channel creations/releases, and commands after rekey. A separate run
also passed rekey while an active command waited for input. The account/key
fixture now passes21 checks, including forced-command replacement of SFTP;
the21 authentication service/proof cases, five classical algorithms with three
rekeys each, three lifecycle checks (including broker loss during an active
command), and2041 native credential checks passed. These qualify command
preparation and cleanup, not broker-owned execution or permanent worker drops.
Normal Build WOS passed after the final client-only compile guard; the server
`.text` section matches the live-tested build byte for byte. The client-only
common-session object also compiles without an undefined broker reference.

Non-PTY command children now run under the broker. A spawn request names only a
retained session record, which permits one successful fork; the broker chooses
its retained command, applies the retained login identity/groups explicitly in
the child, and owns the child PID and three stream descriptors. The child closes
all inherited non-stdio descriptors before account/environment setup, and the
existing execution helper wipes private host keys and reseeds the PRNG. Original
socket addresses supply SSH_CONNECTION and SSH_CLIENT. Signals name a private
session ID and an exact SSH signal name; the broker rejects forced-command
signals and signals to reaped children.

The worker uses three local adapter pipes with the existing channel window and
EOF code. Bounded request/reply IO transfers at most16KiB per stream, advertises
output capacity and acknowledges consumed stdin. Child IO is nonblocking; a
blocked stream retains one bounded chunk without stalling other channels. The
worker caps select waits at10ms while adapters exist. This polling design adds
idle overhead; it can later use event notifications without changing ownership.
Exit status becomes visible to channel cleanup only after buffered output enters
the adapter pipes and their writers close. Channel release/connection EOF closes
broker pipe ends, preserving non-PTY background-child EOF semantics.

PTY allocation, children and accounting now belong to the broker as described
below. Forwarding endpoints and permanent worker credential/authority drops
remain pending. WOS
currently rejects AF_UNIX in its socket syscall, so agent-forwarding listener
setup fails before command spawning. Its environment transfer remains unqualified
at runtime; no working agent-forwarding claim follows from the relay tests.
The broker rejects agent metadata when its retained key policy forbids forwarding.
X11 and vfork broker builds are explicitly unsupported; WOS uses X11 disabled and
fork. SSH wire and syscall layouts are unchanged.

Non-PTY broker execution now passes the strict live qualification workload:
broker child ancestry, private-FD closure, 1MiB duplex IO, independent progress
under backpressure, rekey, signal handling, background-child EOF and 128
signal/background/channel-reuse cycles. The final normal kernel also passes
2,041 credential checks, 22 account/key policy checks (including forced-command
signal denial), three lifecycle checks and all 21 service/proof cases. The
account/key fixture restores its original files.

The earlier stress failures exposed unrelated pathname lookup retries during
existing-file opens, real namespace churn and XFS data writes. The VFS/XFS
corrections described above now pass all three native 10,000-iteration workloads
and the previously failing SSH stress workload. Bash supplies its own getcwd
using stat/opendir/readdir/lstat, so its shell-init error alone did not identify
a failure in the kernel getcwd syscall. Temporary kernel and loader diagnostics
were removed before final live qualification. These results qualify the tested
non-PTY broker path; subsequent PTY qualification follows below. Permanent
worker isolation remains pending.

Final isolated diagnostic KTEST passes 12,043 checks with zero failures,
including retained XFS lookup validation across real data and permission/name
changes. The slow local fallback can reduce concurrency during heavy namespace
churn; remote/device/procfs paths retain their existing retry behavior. These
results do not complete the broader filesystem/process/WKI authorization audit.

PTY requests and window changes now carry the full original SSH frame and a
private session-ID/channel binding to the broker. The broker validates exact
request names, terminal string bounds/NULs, complete dimensions and terminal-mode
encoding before allocating anything. Invalid, forbidden, duplicate and post-start
PTY requests preserve the existing session. A PTY can precede command preparation;
preparing a command preserves its terminal, and spawning requires a successfully
prepared command. Accepted terminal-mode END and future-opcode termination retain
SSH behavior. Malformed resize bodies do not change dimensions.

The broker owns master/slave descriptors and creates a controlling-terminal child
through the shared login/MOTD/exec helper. Ownership is set through the slave FD to
the retained login UID/GID with mode0600. Accounting uses the retained identity,
not a later passwd lookup. The worker receives public TERM/SSH_TTY metadata and
bounded adapter pipes. It never receives a PTY descriptor. Master read/write
aliases close together once; teardown closes owned FDs without chmod/chown through
a potentially reused terminal pathname. On release, the broker can send SIGHUP
only to its unreaped child PID. It never uses a slave-supplied foreground group
as a privileged signal target. WOS resize currently changes dimensions without
SIGWINCH; this change does not add signal delivery or qualify kernel tty ioctl
access policy. A child retaining slave descriptors can retain its node lifetime.

Large PTY reads exposed a stream decoder which incorrectly applied Dropbear's
text-string limit. It now validates the binary16KiB limit before copying into the
existing bounded relay chunk. Live qualification passes nine PTY test groups:
shared broker ancestry, retained/private ownership and FD closure, exact modes and
resize including malformed-body atomicity, malformed admission without terminal
allocation, login shell, abandoned/running connection cleanup,256KiB binary duplex
with merged stderr,2MiB backpressured output with independent commands/rekey, and
EOF/signal/status cleanup. It also passes128 non-PTY reuse cycles,23 account/key
policy checks (including no-PTY allocation denial, PTY forced commands and retained
nonroot UID/GID/mode after passwd changes), and three broker lifecycle checks.
The account/key fixture restored all original files. Normal Build WOS and31 host
authorization-audit tests pass. Kernel source did not change in this PTY slice;
the12,043 KTEST result above belongs to the preceding VFS/XFS qualification.

The network worker now retains a dedicated identity before and after login as
described below. TCP and agent forwarding ownership/policy still need broker
admission and kernel-policy qualification; terminal/session policy gaps remain.

The WOS rootfs declares a locked no-login `sshd` account104:104, with no home or
supplementary memberships. `DROPBEAR_SVR_BROKER_USER` selects this account in the
WOS build. Broker-enabled server startup requires a privileged guest listener and
a worker account with nonzero UID/GID; missing/root identities fail before client
input. The host VM launch still uses --no-setup without root setup privileges.
The broker forks after constructing initial server KEXINIT but before the worker
enters its input loop. The worker replaces private host-key objects with public
objects, sets NNP, seals empty service grants, clears supplementary groups, drops
GID/UID, lowers the authority ceiling to0, disables dumping, changes cwd to/ and
sets umask0077. It verifies every real/effective/saved ID and each restriction
before processing packets. WOS privileged setgid/setuid clear all three IDs;
libc setresgid/setresuid are currently unimplemented and are not used here.

The broker validates and retains the first bounded client identification from
the initial KEX transcript and requires exact identity on every rekey. It closes
its inherited network/signal/listener-slot FDs and owns authentication and login
children. Its control descriptors join the child-close bound before fork, which
also covers a PTY as the very first channel. Root and nonroot authentication no
longer change worker credentials. Login-child UID/GID/groups and saved-ID/exec
semantics come from the broker's retained authenticated snapshot. SSH reserved-port
forwarding admission uses the retained login UID instead of the dedicated worker
UID or an assumed Linux bind restriction. The forwarding operations themselves
still reside in the worker; this is not complete forwarding isolation.

Live worker qualification observes104 for every UID/GID before any client
identification/KEX input and after root authentication. An equivalent dropped
native process passes27 checks covering all credentials, failed authority/group/
UID/GID/NNP recovery, access/ptrace denials against the actual identified broker
and worker, unchanged output buffers on denial, private host-key open denial,
and preserved credentials. Its root-owned06755 exec passes five more checks:
NNP preserves104 IDs, empty groups,zero ceiling and sealed-empty grants. The
actual connection and root login survive these probes and rekey. Missing,
UID0 and GID0 worker-account configurations reject new connections before their
banner; the fixture restores passwd and verifies recovery.

Normal Build WOS and45 host checks pass. Live regressions pass five KEX families
with three rekeys each,ten PTY groups including first-channel descriptor closure,
seven non-PTY groups including128 reuse cycles,25 restored account/key-policy
checks including root/nonroot reserved-port forwarding,three broker lifecycle
checks,nine auth-service cases and12 signed-proof cases. No kernel source changed
in this worker slice, so no new KTEST result is claimed. The preceding12,043/0
kernel result remains attributed to its VFS/XFS change. The listener/broker still
retain guest root authority needed for login services; the remaining terminal,
process,filesystem,testd and authenticated WKI/multi-node policy work is not
complete merely because the SSH worker profile now drops correctly.


Terminal allocation authority now uses a node-local `TerminalIdentity`: a
nonzero, monotonically issued generation plus the public PTY number. Allocation
stops at generation exhaustion instead of recycling identities. Task bindings
publish the whole value atomically; fork/clone/exec copy it and setsid/remote
receipt detach it. `/dev/tty` retains a slave endpoint only when the allocation
matches under the existing tree-to-pair lock order. A failed lookup clears only
its observed old binding, preserving a concurrent replacement. Procfs and the
existing WKI identity payload continue exposing the numeric PTY index.

The line discipline snapshots that allocation identity with the foreground
group. Both local group delivery and WKI-proxy fallback require the target's
matching terminal binding before forwarding or generating a pending signal.
A foreign numeric group or a same-numbered replacement PTY therefore cannot
acquire terminal-generated signal authority. Task references and existing
signal/wake ordering remain unchanged; no PTY lock spans scheduler/WKI work.

The subsequent session admission change below extends this allocation policy.
The existing remote ioctl payload does not identify an originating session;
authenticated remote actor mapping remains required.

Terminal identity qualification: Normal Build WOS passed. The final isolated
KTEST run passed12,071 checks with zero failures. Its first attempt found a
fixture setup error (early KTEST has no current task); the corrected test checks
real PTY retirement/reuse and isolated atomic binding publication. Native live
checks pass as root and after permanent UID/GID104 drop: no ordinary signal
authority over the root victim, no foreign-terminal INT/QUIT/TSTP delivery,
working explicitly delegated terminal signals, stale /dev/tty ENXIO, and fresh
binding inheritance through fork/exec. All ten existing SSH PTY regression
groups pass. Focused host and manifest checks pass; no new raw root checks.
The task's normal and diagnostic VMs have both completed their test runs.


Session and process-group state now live in one process-owned snapshot shared
by threads. A sleeping mutation coordinator serializes session/group and
terminal admission with exec publication; IRQ-safe snapshot copies remain
bounded and never allocate or block. Fork inherits the parent's actual session
and group; exec completion prevents later parent-directed group changes.
`setsid` rejects group leaders. `setpgid` checks parent/child relationship,
session, leader status, exec completion and existing destination group before
committing or waking waiters.

`TIOCSCTTY` requires a session leader and an unowned or same-session terminal;
even root cannot steal a foreign session's terminal through this interface.
An idempotent claim preserves the foreground group. `TIOCSPGRP` requires the
caller's controlling allocation and SID plus an existing same-session group.
`TIOCNOTTY` detaches a nonleader process or revokes a leader's terminal session,
with HUP/CONT sent to its old foreground group outside PTY/store locks.
Aliases and terminal-generated signals now check both SID and allocation.
Legacy WKI claim, foreground and detach mutations return `ENOTSUP` at sender
and receiver until the protocol carries an authenticated originating actor.
Other existing PTY IO remains available; this is a remote compatibility limit.

Spawn group admission precedes all descriptor actions. An invalid group leaves
the unpublished child's cloned descriptors unchanged and reports its precise
error. The libc fallback now performs `SETPGROUP` before descriptor actions;
once the fast syscall is issued, libc returns its result without retrying a
denial through fork/exec. This preserves the output PID on failure and prevents
a denied group request from reaching a truncating OPEN action. ABI structures,
saved credential semantics and ordered descriptor actions remain unchanged.

These admission checks did not complete session lifetime policy. The terminal
lifetime section below adds leader-exit/master-hangup revocation and attachment
generations; further publication and retained-group races still need review. Service-session isolation is qualified below while
preserving the POSIX same-session SIGCONT exception. Authenticated WKI mapping
and distributed qualification remain outstanding.

Session admission qualification: both Normal Build WOS and isolated diagnostic
builds passed with the libc fix. Isolated KTEST completed 12,120 checks with
zero failures and powered off. The native session fixture passed as root and
UID/GID104, covering both fast and fallback spawn EPERM/EINVAL, preserved output
PID and file contents, valid spawn, parent/exec/group admission, terminal detach,
foreign-session denial and 16 competing-thread iterations per identity. UID104
requires a private writable TMPDIR; its first attempt stopped at fixture file
creation in the production /tmp and exercised no spawn assertion. The rerun
used a temporary UID104-owned directory and removed it afterward. Existing
terminal-allocation checks also passed as UID104. All ten SSH PTY regression
groups and the worker's 27 policy plus five set-ID exec checks passed; account
failure probes restored the original passwd file. The VMs shut down cleanly.
Focused host tests passed 108 checks plus the separate exec source gate; the
authority audit reports zero legacy raw-root checks. Three subsequent brace-only
style fixes do not change these paths' behavior; both final builds passed and
are recorded separately in the goal ledger. The broader goal remains in progress.


Service sessions now prevent the POSIX SIGCONT exception from crossing init
service identities. Each child performs setsid and waits for an explicit PID1
ACK after SID/group verification. Before ACK, cancellation closes the channel
and signals only that child; afterward the existing generation-owned group
cleanup applies. The handshake adds no public ABI and retains nonblocking PID1
polling, resource/WKI selection restoration, identity drop and exec ordering.
Its parent read guard prevents an ACK write from delivering SIGPIPE to PID1.

Each SSH connection worker already owned a session. Its privileged broker now
creates a separate one and reports readiness before the worker drops authority
or reads client input. Non-PTY login children create their own sessions before
account switching and exec; PTY children retain their existing controlling-tty
session setup. No worker-provided session or credential is accepted as authority.
The private startup byte precedes the existing broker frames; no SSH wire format
changes. Broker ancestry, retained login policy and bounded relay ownership stay
unchanged.

Normal Build WOS and 40 focused host checks passed, including 12 real-pipe
admission/cleanup scenarios. The separate host supervisor target passed 42 tests.
Live procfs checks show distinct SID=PGID=PID for all four running production
services; four HTTPD replacements and stop/start preserve isolation and PID1's
FD set. Root/UID104 session tests pass, including an explicit same-session
cross-UID SIGCONT positive control followed by denial after setsid. SSH passes
four login-session checks, 30 worker policy checks, five set-ID exec checks, ten
PTY groups, seven non-PTY groups and three broker lifecycle groups. Account
failure probes restore passwd and recover a fresh login. The satisfied keygen
oneshot and disabled testd were not separately launched in this live run.
Kernel code was unchanged in this service slice; no new KTEST result is claimed.
Terminal lifetime/revocation, authenticated WKI mapping and broader service/
remote authority qualification remain pending.

The service-session VM powered off after ordered shutdown and final sync. It
reported busy root and temporary mounts rather than a clean unmount. The
preceding session VM reported the same warning class; differing workloads and
open-file counts do not rule out a remaining lifetime leak.


Terminal lifetime now distinguishes an allocation from each successive session
attachment. Every first claim advances a pair-local generation; exhaustion fails
with EOVERFLOW without changing ownership. The immutable process job snapshot
carries that generation through fork/exec. Alias opens, terminal controls and
terminal-generated signals require matching allocation, SID and generation, so
an old unpublished copy cannot regain authority after same-SID reattachment.

The process-exit path revokes a session leader's attachment before descriptor
teardown. Final master File close also revokes it, while duplicate FDs keep it
alive. Both clear the pair's SID/foreground group and published processes' matching
bindings, then notify the former foreground group with HUP/CONT outside spinlocks.
CLOEXEC close uses the session mutex without reacquiring the VM publication mutex.
A closed master cannot be claimed or opened through /dev/tty. The public terminal
index, UID/GID ABI, saved-ID policy and WKI wire layout are unchanged.

Root and UID104 live checks cover leader exit, final duplicate close and a master
closed by exec's CLOEXEC processing. In each case the stopped foreground process
resumes, handles HUP/CONT and loses /dev/tty; another session can reclaim a
still-open allocation after leader exit. Existing session and allocation tests
also pass. SSH EOF already closes its shared PTY master; this now produces HUP.
The updated fixture checks default hangup death, a HUP handler, ignored-HUP clean
read EOF and explicit TERM. PTY, non-PTY, broker cleanup and worker policy
regressions pass. Normal and isolated diagnostic builds pass; the final isolated
KTEST run reports 12,147 checks passed, zero failed, and powers off. Exact kernel
hashes and normal-runtime build attribution are recorded in the goal ledger.

This does not qualify every lifetime race. Lazy alias detachment and late fork
publication need further notification-race review. Attachment generations prevent
stale terminal authority independently of those notification questions.
Authenticated remote identity mapping remains pending.

### Main-thread retirement and final cleanup

`THREAD_EXIT` from the main thread now retains the process owner as a kernel
cleanup executor until the last admitted worker retires. Credentials, process
identity, job control, shared descriptors and child ownership stay available to
surviving threads. Main-thread CPU accounting freezes at retirement; remaining
threads continue contributing to the process. The final zero-thread check closes
admission under the resource lock before entering ordinary process cleanup.

The retired executor handles KILL/STOP at its dedicated safe point and wakes
eligible workers for other pending signals. Those occurrences remain in the
retained process owner's pending set until a worker commits delivery. A worker
that exits before delivery cannot take the signal with it. Masked workers can
observe the shared pending set through `sigpending`.

Signal delivery reserves one pending bit during frame preparation. Competing
workers cannot commit the same occurrence; failed preparation releases only the
reservation, leaving the pending set unchanged. This also preserves STOP/CONT
cancellation during preparation. Local and process-owned pending occurrences
remain separate, and a new occurrence after consumption survives the old claim.
Claims are committed or released before teardown that cannot return. Neither
retired userspace TCB nor handler frame is entered. Signal dispositions now
resolve the retained process owner; delivery uses one complete snapshot, as
described below.

Final cleanup also makes its scheduling boundary explicit. SHM attachment
retirement and mapped-file writeback run while the executor remains ACTIVE and
its pagemap remains attached. After usercopy and WKI wait quiescence, the
nonblocking exit tail disables interrupts before publishing EXITING. This keeps
a timer from abandoning a parent wake dispatcher halfway through notification.
Page-table reclamation still follows wait-status publication.

The native `tests/fixtures/service_thread_owner_lifetime.c` regression first
reproduced premature FD teardown and missing final wait publication. The fixed
kernel passes all sixteen cases as root and UID104: sole and surviving-thread
retirement, ordinary-exit controls, new thread creation, worker process exit,
caught/blocked/default/KILL/STOP/CONT signals, worker fork/wait and live-child
reparenting, and dirty mapped-file writeback before sole-main/final-worker wait
publication, and 100 deliveries racing eligible-worker retirement per identity.
The UID104 file tests use an owned TMPDIR. Twelve alternating root/UID104
worker-retirement runs cover another 1,200 races. Fifty terminal/session regression checks pass. A subsequent read-only
live snapshot finds no remaining fixture tasks, EXITING tasks or active wake
dispatchers or reserved signal occurrences. The isolated diagnostic KTEST suite
reports 12,174 passed and zero
failed. SSH PTY, non-PTY, worker-isolation and broker-cleanup regressions also
pass. Exact artifacts and hashes are recorded in the goal ledger. These checks qualify this local lifetime slice, not the full
credential or remote-identity policy.


### Shared process signal dispositions

Existing workers observe subsequent `sigaction` updates through their retained
process owner. The four disposition fields are copied or published together
under a short IRQ-safe lock. All syscall, interrupt, synchronous-fault, handoff,
deferred and wait-loop readers use value snapshots, with no store lock held
while copying a signal frame or applying an action. Thread masks, pending bits
and alternate stacks remain private. Fork and process-style CLONE_VM children
get independent copies of the process disposition table.

A per-process sleeping mutex serializes sigaction's old-action result and new
action publication. Input is copied first; failed old-action output leaves the
installed action unchanged. Faulting usercopy never holds the disposition
spinlock. Exec takes the transaction mutex before VM publication and resets the
shared table at image commit. Final exit clears the owner's table only after
worker retirement. No signal, UID/GID or WKI wire layout changes.

`tests/fixtures/service_signal_dispositions.c` passes five cases as root and
UID104: updates visible to an already-created worker in both directions,
2,000 concurrent exchanges per identity forming one complete predecessor
sequence, cross-page EFAULT without action mutation, independent fork
inheritance, and retired-main delivery using a later-installed handler.
The existing 32 thread-lifetime/pending/writeback cases, 50 terminal/session
checks, 19 active-handler cases and SSH PTY/non-PTY/worker/broker regressions
also pass. The final isolated diagnostic KTEST run reports 12,190 passed and
zero failed. A final read-only snapshot of the remaining user/service tasks
finds no fixture tasks, held disposition mutexes or spinlocks, reserved pending
claims, or active wake dispatchers. Exact artifacts are in Goal19 section 9AH.

This checkpoint did not complete signal/exec qualification. The subsequent
exec signal-state change described above preserves SIG_IGN and pending signals.
Full sibling retirement and worker process-ownership transfer remain open.
Default job-control delivery from retired-owner pending pools is qualified
below; broader signal and exec compatibility remains open. Remote identity
mapping and the wider authorization manifest remain pending.


### Retired-owner default-stop delivery

A default TSTP, TTIN or TTOU signal can remain in the retired process owner's
pending pool while a surviving worker delivers it. The delivery reservation
now identifies both its pending pool and signal. Under the owner's resource
lock, job control validates that identity, consumes the pending occurrence and
the same source task's default-stop marker, and publishes the process stop.
CONT generation uses that lock too, so cancellation cannot revive a stale stop.
Local and process-owned occurrences remain separate. All syscall, IRQ, handoff
and deferred default-stop paths pass the reservation through this transaction;
no reservation or resource lock spans parking. No ABI or credential change.

The extended `service_job_control_disposition.c --retired UID` fixture waits for
WOS /proc's `thread_exit` channel before sending signals. A userspace join alone
can precede kernel retirement. The baseline missed all 24 default/immediate or
blocked-then-unmasked stops across root/UID104 and PID/group routing. The fixed
kernel passes all 60 retired-owner cases plus 18 live-main caught/ignored/
CONT-canceled controls. Observation windows use monotonic deadlines because a
single nanosleep can return early on a signal/event wake; no assertion was
relaxed. Bounded live debugging confirmed that an earlier apparent CONT failure
was premature observation, with both stop gates released and the worker resumed.

The existing stop/churn fixture passes 384 cycles across signals 19 through 22,
including 40-worker groups. Thread-owner32, shared-disposition10 and SSH PTY10
regression groups also pass. A final read-only snapshot shows no remaining
fixture tasks, stop gates, reserved occurrences, held disposition locks or active
wake dispatchers among the inspected service/user tasks. This normal VM also
unmounted /rootfs before powering off. The isolated diagnostic KTEST run passes
12,270 checks with zero failures, including actual controller/claim tests for
all three signals, CONT after reservation, separate local/shared occurrences
and wrong-pool/wrong-signal rejection. Seven host claim tests and 63 focused
source/policy checks pass. Exact evidence is in Goal19 section 9AI; full exec,
remote identity mapping and the broader policy manifest remain open.


The exact legacy WKI credential guard was verified with two host regressions
that fail against the old admission policy: supplementary-group removal can
change DAC class selection, and omitted dumpability cannot round-trip. The full
credential host suite passes 54 cases; 68 credential/WKI source checks pass. Normal
Build WOS and the isolated diagnostic build pass. Live KTEST with --no-setup
reports 13,556 assertions passed, zero failed, including all six-ID root/nonroot slot
combinations, sentinel rejection with unchanged output, one to eight supplementary groups,
and real inline/VFS submit denials with unchanged credentials/resources/FDs.
The VM powered off after testing. Initial libguestfs partition-device packaging
failure cleared on retry without packaging-script changes. Full receiver identity
mapping and authority on cached/uncached remote executable reads remain pending.


Receiver compute identity now requires explicit per-peer shared-namespace trust
in addition to the compute operation bit. Default provisioning denies task and
fragment admission. The nonroot choice rejects UID/GID 0 in every ID slot and
supplementary group; the root-inclusive choice explicitly trusts those identities.
The deferred receiver validates bounded identity strings, reconstructs and maps
one credential before source fetch or process reservation, and publishes that
same value. The authenticated policy is copied under the peer lifecycle lease into
the compute session token; deferred mapping never reads the shutdown-cleared
boot credential table. Existing retirement checks still guard publication.
Mapping itself preserves groups, NNP, ceilings, dumpability and service
privileges; legacy transport still cannot carry all of those fields. Host policy
validation runs before writing any credentials. See docs/wki_security.md for the
receiver-specific configuration and compatibility changes. Explicit mapped VFS,
interpreter/cache read authority and full remote credential transport remain open.


Receiver trust validation now includes 29 cluster setup tests, including provisioning, credential and
WKI policy host suites, 88 source checks, and 14,792 live diagnostic assertions
with zero failures. A four-node authenticated cluster passed 18 strict-exec cases:
missing mapping and nonroot mapping returned EACCES for root, while explicit root
mapping executed on the named receiver. Each case ran three times with ordinary
and fragmented submissions. Readiness requires connected peers and visible
executable exports, because authentication precedes resource publication.

That live run also exposed an init mount race: concurrent WKI namespace changes
made the rootfs mount return EAGAIN after rolling back its prepared filesystem.
Init now retries only EAGAIN, with at most 100 attempts and a nominal 50 ms pause;
permanent errors and successful publication are never retried. Both fstab and
required tmpfs mounts use the same bounded helper. Four host behavior tests and
simultaneous four-node boot verify this: two nodes exercised retry, and all four
mounted and pivoted successfully. The kernel authorization and rollback checks
remain intact. Normal and isolated builds pass. The subsequent live diagnostic run includes the init retry and session-policy
capture fix: 14,792 assertions passed with zero failures, including real session
capture, reuse, retirement, and cancellation isolation. Full credential wire
transport and mapped authority on VFS/interpreter/cache reads remain pending.

Interpreter cache read admission is now enforced for every construction request.
`task.cpp::read_boot_file_fully` opens the interpreter before consulting cached
bytes and obtains freshness from that retained file. The removed shortcut used
only `stat`, which allowed a different caller to reuse bytes without read access.
A live WOS SPAWN regression reproduced this on the previous kernel: root warmed a
mode-0600 interpreter, then effective UID 1000 successfully spawned its payload.
With the fix, direct open returns EACCES, stat still succeeds, and the cached spawn
is denied. Changing the interpreter to mode 0644 permits the nonroot control.
Fixtures are `tests/fixtures/service_interp_cache.{cpp,py}` and
`service_interp_payload.c`; compile the payload with
`-Wl,--dynamic-linker=/tmp/goal19-interp-private/ld.so`. The Python driver stages
and removes private test paths in a disposable VM. Normal Build WOS and focused
source checks pass. This closes cache bypass of existing VFS read admission;
remote worker credential mapping remains required. Opening remote files may
prefetch bytes on cache hits, so this deliberately gives up that optimization.
The same kernel passed all 18 ordinary/fragmented WKI identity cases after
sender-first startup. An earlier simultaneous-start run timed out opening the
remote interpreter and logged failed sender-export mounting; that startup issue
remains unresolved and is not counted as a passing run. No read check was removed
for the successful comparison. These native checks directly exercise construction;
the isolated KTEST result above belongs to the preceding session-policy change.

Remote Task construction now installs the complete mapped snapshot before image
mapping and interpreter construction, for both buffered and file-backed ELF
paths. NPROC reservation uses that snapshot's real UID. Late identity setup
verifies the same value instead of replacing credentials after loading. This
removes the temporary bootstrap effective/saved IDs and authority in the child;
ELF source and interpreter reads now use explicit mapped request authority as
described below. No credential wire format or syscall ABI changed.

Normal and isolated diagnostic builds pass, as do the credential host suites and
exec/WKI source gates. A staged-start cluster passed the existing 18 identity
cases and 24 new credential cases using small and greater-than-8-MiB dynamic ELFs,
ordinary and fragmented requests, and root/nonroot identities. Allowed receivers
reported all six expected IDs, zero supplementary groups, and dumpability 1 for
root or 0 after a nonroot ID drop. Missing mapping returned EACCES; nonroot-only
mapping accepted nonroot and rejected root. The new fixture deliberately retains
legacy-representable dumpability: changing it still produces sender ENOTSUP.
`tests/fixtures/service_wki_credentials.{cpp,py}` uses an explicit source-node
path because a plain /tmp pathname resolves in the receiver's namespace. Create
the large ELF fixture by appending 9 MiB of zero padding to the compiled dynamic
binary; the loadable segments are unchanged. Source gates establish the early
initialization order in both constructors; the live cases verify resulting
credentials and denial behavior, not exact instruction coverage.


### Explicit reads and complete credential records

The internal `vfs_open_read_file_as` API copies a supplied snapshot once and
uses it for traversal, retained lookup validation, and the final regular-file
open. It validates the borrowed path-context task's identity/root/cwd without
comparing against or changing that worker's process credentials. Ordinary VFS
consumers retain their existing credential validation. The owner-side
`vfs_open_read_file_resolved_beneath_as` variant accepts a physical backing path
and confines the retained target to the export, without applying the worker's
root or routing a second time. The local owner supports TMPFS/XFS; the ordinary explicit reader also supports
negotiated WKI reads. Special backends are rejected. Errors leave outputs
unchanged, and success returns normal mount-owned File references without
installing a descriptor.

`platform/security/credential_record.hpp` defines canonical version1, 80-byte
little-endian records containing all six IDs, eight bounded supplementary-group
slots, no-new-privileges, dumpability, authority ceiling, service privileges,
and their sealed state. Decoding rejects unknown versions/bits, malformed
lengths, ID sentinels, nonzero unused group slots, and inconsistent service
locks before assigning the output. It restores a value through
`Snapshot::restore_fields`; neither operation publishes process credentials or
authorizes a peer. A dropped ceiling may leave stored service grants present
but ineffective, which is preserved exactly during the roundtrip.

The negotiated WKI regular-read operation now carries this record and maps it
using the policy copied at authenticated RX admission. Interpreter construction
uses the child's installed credentials through this explicit API, preserving
existing root/routing choices and open-before-cache admission. ELF source reads
now use the same explicit API before cache access. Full compute record negotiation
and broader remote mutation mapping remain pending. The legacy compute format guard remains mandatory and unchanged.
Local interpreter reads on FAT32 and other unsupported backends currently fail
closed; those backends still require an explicit authority adapter.
Host coverage includes fixed golden bytes, every truncation, oversized input,
malformed fields, maximum groups, copied lifetimes, and no-regain behavior.

Validation of this foundation: Normal `Build WOS` and the isolated diagnostic
build passed; all 58 credential host tests passed with normal sanitizers in
host execution. Live KTEST via `--no-setup` finished with 15,021 passed and zero
failed, including explicit request-versus-worker credentials, full record
restrictions, and root/nonroot export-escape denial. Formatting, the authorization
audit, and exec/WKI source gates passed. The test VM powered off automatically.
These foundation results preceded the negotiated WKI integration; subsequent
remote validation is recorded below.


The negotiated read integration passed Normal `Build WOS`, the isolated
KTEST build, nine WKI host policy tests, formatting and the credential/WKI/exec
source gates. Live KTEST again reported 15,021 passed and zero failed, including
fixed deferred storage copying and recycling the admitted policy. A staged
four-node cluster passed 18 identity cases, 24 credential cases for small and
large ELFs, and six explicit remote interpreter cases. Root warmed a mode0600
interpreter on the source node; the receiver denied UID1000 despite its root
worker, then admitted UID1000 after chmod0644, for ordinary and fragmented
submission. The existing remote construction failure mapping reports EIO to
the submitter; the explicit VFS call preserves the owner's permission errno.
The native cached-interpreter denial/readable control also passed.

`tests/fixtures/service_wki_interp.py` uses a normal locally linked launcher
and a separate `service_wki_credentials.cpp` payload linked with
`-Wl,--dynamic-linker=/wki/wos-0/tmp/goal19-wki-interp/ld.so`. Pass both with
`--launcher` and `--binary`, plus `--interpreter toolchain/sysroot/lib/ld.so`.
The launcher fixture accepts an optional final payload pathname; existing
four-argument credential cases are unchanged. An initial plain /tmp interpreter
failed on the receiver because it was staged only on the source; an explicit
self-node interpreter could not launch through the existing local execve path.
The separate payload makes the distributed owner-read boundary explicit.
Declare reverse peer identity trust on the source node for permitted readers;
compute trust on a receiver does not grant permission in the reverse direction.
Full compute records, other backends, cache content freshness and remaining
manifest surfaces are still pending.

The test cluster exited after shutdown. Its source rootfs reported EBUSY with
nine open files before final sync, so this run does not establish clean rootfs
unmount or complete retained-file cleanup. The private overlays and logs remain
available for that separate lifetime investigation.


Remote ELF source admission now opens with one copied mapped Snapshot before
metadata inspection or shared-cache lookup. Buffered reads and freshness checks
use that retained File; large ELF construction receives ownership of the same
File. Bounded retries reopen the same path with the same snapshot, and permission
or unsupported-reader errors terminate without selecting a different source.
The same-metadata local-file substitution has been removed. Submitted root and
VFS routing policy are validated and scoped before source loading. Both VFS_REF
and RESOURCE_REF preserve source admission errors. Worker credentials, UID/GID
ABI and wire layout remain unchanged. Shared-cache content freshness, unsupported
local reader backends and full compute record negotiation remain pending.

The source-read regression fixture is `tests/fixtures/service_wki_source.py`.
After the four-node identity readiness check, pass a normal linked
`service_wki_credentials.cpp` launcher, small payload and payload padded past
8 MiB. It stages receiver-owned executables with mode 0711: the sender's existing
legacy remote open can read, and its execute check passes, while the receiver
must deny the mapped nonroot read. Root warmup precedes each denial and mode
0755 supplies a readable control. Check receiver serial output for the matching
VFS_REF load rejection to distinguish receiver admission from an earlier syscall
failure. This fixture does not qualify the sender's legacy remote open policy.

Validation of mapped ELF source admission: Normal `Build WOS`, isolated diagnostic
build, eight selected host suites and formatting checks passed. Live isolated
KTEST completed with 15,021 passed and zero failed. The staged-start four-node
cluster passed 18 identity, 24 credential, 12 source-read and six interpreter
cases; the native cached-interpreter regression also passed. Receiver serial
logs recorded UNAUTHORIZED for each of the four nonroot private-source attempts,
covering buffered/file-backed sizes and ordinary/fragmented requests after root
warmup. These checks establish source-read denial at the receiver; cache content
freshness, complete compute credential transport and the other pending manifest
surfaces remain separate work.


Executable cache metadata now selects a candidate only. Both the interpreter
cache and shared remote ELF cache compare every candidate byte against uncached
positional reads from the newly authorized File and require exact EOF before
reuse. The shared cache pins its immutable buffer before releasing the compute
lock; comparison occurs outside that lock and checks request cancellation and
deadlines between bounded reads. A mismatch invalidates that entry and releases
only the comparison reference. Freshly fetched bytes publish only into the
original loader token, without a second unverified cache substitution.

Explicit credential read Files use O_NO_CACHE internally. Positional WKI reads
alone can use bulk prefetch caches, so they were insufficient to guarantee a
fresh comparison. Legacy wire OPEN flags and ordinary VFS callers are unchanged.
Comparison uses at most 64 KiB of temporary scratch per reader, accepts partial
positive reads, and rejects errors, premature EOF, extra bytes and invalid read
counts. It adds source I/O on cache hits; authoritative content-version tokens
could avoid that cost in a future optimization.

This does not provide atomic image snapshots against concurrent writers. Remote
fstat still has a pathname-STAT fallback, so earlier descriptions of fully
retained-object metadata checks were too broad for WKI. The new comparison reads
through the owner FD; remote FD-stat/version semantics and mutation exclusion
remain to be qualified separately. Full compute snapshot transport and remaining
manifest policy surfaces are still pending.

The kernel cache regression injects wrong bytes with exactly matching source
metadata, calls the production loader with an explicit standalone path context,
and verifies both fresh replacement and subsequent shared-buffer reuse/cleanup.
KTEST runs before scheduler task startup, so this fixture does not borrow a
current task. The native interpreter fixture now also corrupts the ELF magic
without changing length after root warmup, requires construction failure, restores
the original byte and verifies recovery before its private-read denial controls.

Validation of content-checked cache reuse: Normal `Build WOS` and isolated builds
passed, as did nine selected host suites and formatting checks. After correcting
the new fixture's pre-scheduler task assumption, isolated KTEST completed with
15,022 passed and zero failed. The native same-size interpreter mutation,
restoration and permission cases passed, followed by all 60 distributed identity,
credential, source-denial and interpreter cases. The broader host registration
gate still reports the pre-existing custom `kcov_runtime_test.cpp` entrypoint as
missing GTest cases; it is not a failure of the new registered comparator tests.

### Complete compute credential transport

WKI task submission now negotiates `WKI_CAP_COMPUTE_CREDENTIALS` (0x0400).
The canonical 80-byte record follows the unchanged identity and resource records,
and its six IDs must match the original prefix. One sender snapshot supplies
both copies. The receiver uses negotiated capability bits captured with its
authenticated session policy, checks the exact format and record before source
reads, and installs the mapped snapshot before either ELF construction path.
Groups, NNP, dumpability, authority ceilings and sealed grants are preserved.
Active service grants require root-inclusive peer trust; nonroot trust can accept
stored grants whose authority ceiling has made them ineffective.

Peers without this capability retain exact legacy round-trip admission. Strict
placement rejects unrepresentable restrictions with ENOTSUP; optional placement
can stay local. Neither malformed full records nor peer rejection trigger a
legacy-format retry. `wki.compute_credentials_off` tests this compatibility path
without changing local enforcement or resource negotiation.

Normal Build WOS and the isolated diagnostic build passed. Nine selected host
suites passed with normal sanitizers; the initial VFS source assertion expected
the previous capability list exactly and was updated for the added bit. Formatting
and the credential authority audit passed. The isolated VM passed 15,023 checks
with zero failures, including all three delivery-mode parsers, all 80 truncations
of the credential suffix, each of six prefix-ID mismatches, and record/version/
capability-length rejection cases.

`tests/fixtures/service_wki_snapshot.{cpp,py}` passed 36 four-node cases using
small and greater-than-8-MiB ELFs with ordinary and 16-KiB fragmented arguments.
Profiles cover root with ceiling zero and an empty sealed grant, nonroot with
inert stored grants and ceiling zero, and nonroot with active service grants.
All carry eight supplementary groups, NNP and explicit dumpability. The payload
checks exact state and failed attempts to raise the ceiling, clear NNP, regain
UID zero or re-add a permitted service grant after dropping it. Nonroot-only
trust admits the inert profile and denies active grants; root-inclusive trust
admits all three. The existing 60 distributed identity, credential, source-read
and interpreter cases also passed, as did local interpreter corruption/recovery
and cached permission checks. The first cluster unmounted its rootfs cleanly.

Other manifest entries remain pending, including broader remote mutation/device/
IPC identity mapping and other local read backends. Content comparison does not
provide atomic executable snapshots against writers or owner-FD metadata versions.

A second four-node run disabled complete-record advertisement only on the
root-inclusive receiver using a private boot overlay. All 24 restricted strict/
optional cases passed: strict requests returned ENOTSUP, while optional requests
reported the exact snapshot and no-regain checks on the source node. The same
cluster passed 18 identity and 24 legacy-compatible credential cases, proving
that representable identities still execute remotely across the mixed pair.
The capability switch does not disable the explicit VFS-read identity format.

### FAT32 explicit regular-file reads

The explicit ELF/interpreter reader now supports FAT32 alongside TMPFS, XFS and
negotiated WKI reads. It retains the same request snapshot, root/cwd checks,
namespace/mount validation, mount reservation and success-only output assignment.
FAT32 repeats traversal and final-file authorization under its sleeping mount
mutex and requires a regular file before allocating/publishing an open node.
This internal regular-read mode accepts only flags zero, so it cannot create,
truncate, write, or return a directory. Ordinary FAT directory opens keep their
existing behavior. FAT ownership remains UID/GID zero with attribute-derived
0644/0444 file modes and 0755 directories. No UID/GID or WKI wire ABI changes.

Live validation also exposed an unsupported-operation deadlock: FAT32 `mkdir`
entered public stat while the retained lookup consumer held namespace publication.
A saved SFTP kernel stack showed `vfs_mkdirat` → `consume_lookup_locked` →
`vfs_mkdir_resolved_path` → `vfs_statat` → `local_walk_bind` → the same mutex.
FAT32 now checks existence directly on the retained backend, preserving EEXIST
for an existing target and ENOSYS for unsupported creation. This does not add FAT
directory creation. The native fixture checks the denial leaves the path absent
and uses the boot volume's existing EFI directory for interpreter-type rejection.

Final validation passed Normal Build WOS, the isolated diagnostic build, seven
selected host suites, formatting, the authority audit, and 15,063 KTEST checks
with zero failures. The full native FAT matrix passed 39 interpreter cases plus
three unsupported-mkdir checks. It covers local POSIX spawn, receiver-local
FAT reads selected by a serialized `/boot` local routing rule, and explicit
`/wki/wos-0/boot/...` owner reads. Small and greater-than-8-MiB local/receiver
images, ordinary/fragmented remote submissions, all three full credential
profiles, missing and directory interpreters, and recovery are exercised.
The receiver-self `/wki/<own-name>` path is not used to select local reads.
Local spawn retains its existing EAGAIN construction-failure result; remote
construction retains EIO. No runtime credentials or permission checks were
weakened to make a fixture pass.

Earlier matrix attempts encountered intermittent `WKI_ERR_NO_ROUTE` while
opening the source ELF, also reproduced by the existing credential-only fixture.
Those failures occurred before the FAT interpreter read and remain unresolved
transport evidence. A later complete FAT matrix passed without retries inside
the test cases. The fixture now collects independent execution failures and
reports them at the end rather than losing coverage after the first failure.
The FAT32 patch covered the observed backend; the synthetic-backend follow-up
is described below.

After the complete FAT matrix, the existing 36 full-snapshot/no-regain cases
also passed on the same final kernel. Eighteen authenticated identity cases and
the local interpreter corruption/recovery and cached permission controls passed
on this cluster as well. These successful runs do not resolve or erase the
earlier transient routing failures.

The final cluster launcher exited successfully after shutdown. Subordinate
mounts unmounted, but `/rootfs` reported EBUSY with nine open files and power
issued a final sync. This is not evidence of a clean root-filesystem unmount.

### Synthetic filesystem mkdir denial

DEVFS and PROCFS mkdir now inspect final entry metadata outside namespace
publication, then validate the retained lookup before returning the negative
result. PROCFS metadata cleanup can release a borrowed remote descriptor; it
must not run while publication is held. The helper invokes no device open
callback and does not follow the final symlink. Existing entries return EEXIST,
including dangling symlinks; absent entries retain ENOSYS. The component walk
still controls ancestor search, missing-parent and non-directory errors.

Both local mkdirat and the confined owner operation use this path. Bootstrap
mkdir performs its metadata probe before acquiring a publication guard. The
resolved helper rejects accidental locked synthetic probes with EAGAIN, and
the generic public-stat fallback has been removed. Denial changes neither
credentials nor namespace/cache generation; no syscall or wire layout changes.

The regression covers root/nonroot callers, directories, device nodes, procfs
files and symlinks, absent entries, trailing separators and invalid ancestors.
It checks preserved credentials, FD tables, node references, device callback
counts, metadata, and namespace/mount generations. Native syscall coverage is
provided by tests/fixtures/service_synthetic_mkdir.cpp.

Validation passed Normal Build WOS, the isolated diagnostic build, seven host
suites, formatting, and the authority audit. KTEST completed 15,122 checks with
zero failures (new denial/state regression passed). The normal kernel passed
24 native root/nonroot syscall cases. Both test VM launchers exited; the normal
VM unmounted its root filesystem at 33.866 seconds. These checks do not prove
completion of the remaining remote mutation/IPC/device identity mapping.

### General explicit owner open (wire migration prerequisite)

The confined explicit VFS API now accepts ordinary native file/directory open
flags and an already umask-adjusted mode. It carries the supplied snapshot
through acquisition, export-boundary checking, namespace validation and backend
authorization. Worker credentials remain unchanged. Existing regular-read APIs
retain their regular-only behavior and cache-content safeguards.

The shared implementation reserves mount lifetime and long-path storage before
backend mutation, transfers the prepared path without allocation, and completes
VFS-level truncation before returning the File. TMPFS handles truncation in its
backend; XFS/FAT32 use the VFS completion step. Synthetic/device files and remote
recursion are rejected, as are invalid access modes, DIRECTORY+CREATE, and the
private known-absent hint. Authorization denials leave the output and target
unchanged. This prerequisite was subsequently connected to ordinary WKI OPEN as described
below.

Validation: corrected Normal Build WOS and isolated diagnostic builds passed,
as did seven selected host suites, formatting, and the authority audit. The
corrected isolated KTEST VM completed 15,196 checks with zero failures. New
TMPFS cases cover denied create/truncate, admitted ownership/groups, directories,
flags/symlinks, unchanged output pointers, long-path ownership and confinement.
The disposable XFS case proves denied truncation preserves eight bytes and an
allowed truncating open produces size zero. Existing explicit-read regressions
also passed. The VM exited; no ordinary-WKI-open runtime claim is made yet.


## Ordinary WKI OPEN admission

Ordinary remote opens now require the negotiated complete credential record
(capability 0x0800, operation 0x0419). Both VFS entrypoints forward their captured
snapshot and preserve the owner's errno. RX carries authenticated policy and
capabilities into fixed deferred storage; the worker maps that record before
calling the explicit confined owner-open API. Read/write policy bits and identity
trust are checked separately. Legacy OPEN is refused without ambient fallback.
Retained descriptions enforce access direction for scalar, bulk and RDMA I/O,
and write-only OPEN cannot request prefetch. UID/GID syscall layouts, saved-ID
semantics, exact channel ownership and CLOSE cleanup are unchanged.

Remaining path metadata/mutation operations, metadata-batch CREATE_CLOSE, IPC
and device identity mapping remain pending. Validation for this iteration is
recorded in the goal ledger; this section alone makes no runtime success claim.

Corrected validation: Normal Build WOS, isolated diagnostic build, ten targeted
host suites, authority audit and formatting passed. The corrected KTEST VM
passed 15,225 checks with zero failures. The native fixture
`tests/fixtures/service_wki_open.py` passed 52 identity/group/ownership/create/
truncate and denial-preservation cases through current TMPFS exports; the
existing full snapshot/no-regain fixture passed all 36 cases. Tests use private
0777 workspaces because WOS mounts /tmp as root-owned 0755. Cleanup removes the
fixture files and directories.

Live testing found a generic denial-reply bug: the resource-channel-only send
helper rejected dynamic VFS channel identities. Replies now use the actual
admitted channel/generation, and OPEN normalizes internal transport statuses to
errno. A separate startup fence and stale root-export replacement/discovery
problem prevented root-export readiness in two launches. Those failures remain
in the goal ledger; passing subexport tests do not establish automatic recovery.
RDMA and malformed-peer FD requests were not exercised live in this iteration;
central mode tests and source-order checks cover the new handler gates.

The separate capability-disabled receiver also passed nine root/inert/service
read/create/directory ENOTSUP cases, with no creation target left behind. All
36 remote snapshot/no-regain checks still passed in that configuration, showing
that the dedicated regular-file reader remains usable. These final matrices
provide 133 passing live cases across the two configurations. The overall goal
remains open for remaining metadata/mutation, IPC and device identity work.

### Explicit owner metadata lookup

`vfs_stat_resolved_beneath_as` accepts one admitted credential snapshot and a
physical export path. It checks directory search authority without requiring
read permission on the final file, retains the lookup and mount (with native
or DEVFS node bindings where available), and publishes staged metadata only
after namespace, mount and request-context validation. It supports TMPFS, XFS, FAT32 and DEVFS metadata; PROCFS is rejected
before process/FD lookup until remote process identity is represented.

Explicit physical owner requests now use the credential-aware component walker
for both OPEN and STAT. Every original symlink ancestor is checked, and absolute
symlinks resolve from the physical root without modifying the serving task's
root, cwd or credentials. After pivot_root, physical mount ancestors such as
`/` may have no backing filesystem; the walker uses synthetic search directories
only when real child mounts exist. Declined walks cannot fall back to ambient
worker resolution. REMOTE recursion is refused. The old resolved-path route skipped
this walker; the new denial regression specifically covers an inaccessible
symlink parent whose target is otherwise accessible to the caller.

This is a metadata adapter prerequisite. Scalar and batch metadata wire callers
still need migration. The subsequent retained owner-descriptor FSTAT change is
described below.

Final validation passes Normal and isolated builds, ten host suites, formatting,
the authority audit, and 15,290 KTEST checks with zero failures. A disposable
post-pivot cluster passes 60 ordinary OPEN cases (including eight symlink search
and nofollow checks across both identity mappings) and 36 full credential
snapshot/no-regain cases. The ledger retains the initial KTEST and post-pivot
failures that drove these corrections. These results do not qualify the pending
metadata wire migration or retained owner-FD FSTAT behavior.


### Retained owner-descriptor metadata

Remote FSTAT uses negotiated `OP_VFS_FSTAT` (`0x041a`) and
`WKI_CAP_VFS_FD_METADATA` (`0x1000`). Its request names the existing channel FD;
the successful reply retains the 144-byte Stat layout. The receiver validates
its captured capability and exact peer/channel/generation binding before reading
that File. Metadata requires no additional data-read permission, so write-only
descriptions retain this operation and a subsequent credential drop does not
replace an existing descriptor grant.

The client serializes pending-write flush and metadata with its descriptor I/O
lock, retains its file position and mount identity, and publishes only a complete
validated reply. FSTAT bypasses pathname and File stat caches; it never reopens
by name or substitutes synthetic metadata on failure. Error replies carry no
metadata, and malformed or clipped successes are rejected. Capability-disabled
peers return ENOTSUP. Remote executable loading preserves that refusal at every
source-metadata checkpoint instead of reporting a missing executable or I/O
failure. The following section covers the subsequent pathname metadata migration.


Validation passes Normal and isolated builds, ten host suites, formatting,
the authority audit and 15,290 KTEST checks. Live normal tests pass five retained
metadata scenarios with 85 checks, 60 ordinary OPEN cases and 36 credential
snapshot cases. The capability-disabled receiver passes three metadata scenarios
with 43 checks and 36 mixed credential cases, including twelve strict ENOTSUP
refusals when receiver 3 cannot obtain source metadata. No testcase retries were
used. Exact malformed-reply and channel-binding rules have host/source coverage;
raw malformed-peer requests and RDMA were not tested live. Normal shutdown after
peer cleanup unmounted the root filesystem; the earlier busy-root shutdown is
retained in the goal ledger and no shutdown-code fix is claimed.


### Caller-authorized remote pathname metadata

Scalar STAT/LSTAT/access use `OP_VFS_STAT_AS` (0x041b), negotiated with
`WKI_CAP_VFS_STAT_IDENTITY` (0x2000). The request contains the canonical 80-byte
credential record, follow/directory flags, a zero reserved byte, a 16-bit path
length and the exact relative path. Legacy STAT cannot convey a caller and
returns ENOTSUP. The receiver maps the captured authenticated peer policy before
an explicit owner lookup; STAT requires ancestor search but no final file-read
grant. ACCESS applies WOS effective-credential permissions to the returned
metadata with the same admitted snapshot. Neither path uses remote pathname
cache entries as authorization. Replies keep the 144-byte Stat ABI, require
canonical lengths/status and publish output only on success.

STAT_FOLLOW batches use version 2: the unchanged eight-byte header is followed
by one canonical credential record and the existing bounded path entries. All
chunks reuse that snapshot. The receiver preflights the complete chunk, maps the
identity once and uses it for every item. Error items have zero metadata; replies
retain the existing header and status/Stat item layout. Legacy version-1 STAT
batches return ENOTSUP. Version-1 mutations retain their existing no-replay
contract and remain pending identity migration. Batch construction retains the
mount selected by the source-side authorized lookup. No namespace lock spans
a network wait. The private `wki.vfs_stat_identity_off` diagnostic flag tests
unsupported-peer behavior without enabling legacy fallback.

Normal and isolated builds, ten host suites, formatting and the authority audit
pass. Isolated KTEST passes 15,294 checks with zero failures. The normal cluster
passes 106 scalar/batch metadata cases, 62 ordinary OPEN cases, five retained
FSTAT scenarios (85 checks), and 36 full-snapshot/no-regain cases. The metadata
fixture initially hit unsupported remote CHOWN during setup; it now stages
UID1000/GID2000 directories with mapped owner-local execution and verifies their
identity. Mixed-capability validation passes 84 metadata cases (including 30
strict ENOTSUP refusals), another 62 OPEN cases, five FSTAT scenarios (85 checks),
and 36 snapshot cases. The normal source shutdown reported nine root-backed
files and EBUSY after peer cleanup; the mixed source unmounted cleanly. Live GDB
confirmed the mixed source had no remote owner FDs, accounted for all root-backed
files in normal services, and observed zero root files at the shutdown boundary.
The intermittent shutdown result remains unresolved; no shutdown-code fix is
claimed. Both clusters and the temporary debugger session are stopped.
READLINK/realpath origin search, other remote metadata/mutations, remote process
identity and IPC/device mapping remain separate pending work; this is not an
overall enforcement-completion claim.


### Explicit owner symlink text adapter

`vfs_readlink_resolved_beneath_as` accepts an explicit path-context Task and
credential snapshot, checks ancestor search with the physical owner walker,
retains the symlink and validates the export boundary before copying its text.
The final target is not followed: an absolute target outside the export may be
read as text, while an intermediate symlink escaping the export is rejected.
TMPFS/XFS use retained backend lookups; DEVFS uses a pinned-node reader with its
WKI and metadata locks, without opening a device. PROCFS remains unsupported
until process-identity mapping. FAT32 keeps its native unsupported result.
The new kernel-only adapter accepts a bounded buffer of up to 512 bytes, preserves
it on failure, and copies only the successful byte count without a terminator.
This adapter is the prerequisite used by the mapped READLINK operation below;
realpath-wide traversal/cache identity migration remains pending. The directory-required case checks metadata
on the same retained object, preserving ENOTDIR for a nondirectory and EINVAL
for a directory without relookup. Normal Build WOS, the isolated diagnostic
build, all 10 focused host checks, formatting and the credential audit passed.
The corrected isolated VM reported 15,357 passed and zero failed, then powered
off normally. Its first run exposed the directory errno bug and test integer
width mismatches; both were corrected before this successful run.


### Local aliases into mapped remote exports

A local symlink leading into a remote export must preserve the remote remainder
until the owner performs its mapped operation. Resolving a remote intermediate
symlink on the source can otherwise remove an inaccessible ancestor before the
owner sees the pathname. A live regression reproduced this: restricted STAT of
`/tmp/alias/closed/link` succeeded although `closed` was not searchable.

The resolver now has a separate `stop_at_remote_mount` flag for mapped OPEN,
STAT/LSTAT and access lookups. It returns before remote prefix or final READLINK
calls after following the local alias. The procfs descriptor-link flag retains
its separate OPEN-only purpose. This corrects the earlier goal-ledger claim that
the procfs flag already stopped remote traversal. Original directory/follow
requirements, the admitted credential snapshot and owner confinement remain in
place. No wire or userspace ABI changes are introduced by this correction.

The live fixture includes local-alias denial and allowed-path controls for
STAT/LSTAT, access, directory OPEN, missing and trailing-slash paths, batch
metadata and privileged warmup. Mixed-capability validation also probes local
aliases. General local overmounts below remote prefixes and legacy realpath
resolution remain pending; the mapped READLINK wire operation is described
below.

Validation passed Normal Build WOS, the isolated diagnostic build, all ten
focused host checks, formatting/audit and 15,357 KTEST checks with zero failures.
The normal and capability-disabled clusters passed 148 and 134 pathname cases,
including 60 capability-disabled checks, plus 124 ordinary OPEN cases, 170
retained-descriptor checks and 72 complete snapshot/no-regain cases. The failed
baseline remains recorded alongside the successful reruns in the goal ledger.
Both sources unmounted rootfs cleanly after peer drain, and all task VMs are
stopped. This does not resolve the earlier intermittent busy-root shutdown.


### Mapped remote READLINK

READLINK now sends a canonical caller snapshot through the negotiated path
operation envelope and maps the peer policy captured at RX admission. The
owner calls the explicit physical adapter; legacy READLINK is refused. Public
readlink/readlinkat captures one snapshot across lookup retries and preserves
the original remote remainder after local aliases. No proxy positive/negative
cache or mount-root shortcut substitutes for owner authorization. Successful
text is staged and checked before publication, including its exact wire length;
errors leave the caller buffer unchanged. Directory requirements and no-NUL
short-buffer semantics are retained.

Each lookup now captures its admitted credential snapshot before native retries
and carries that same value through routed fallback and intermediate/final remote
symlink reads. The resolver captures the explicit/current Task only when no
admitted snapshot was supplied. This prevents a routed retry or helper from
substituting later task credentials; it does not by itself authorize local
ancestors skipped by the legacy routed walker.

Legacy resolved helper calls pass an admitted snapshot when available or
capture the explicit/current Task once for remote reads. Their remote policy
and unsupported errors propagate. This does not complete kernel realpath's
local prefix-cache and whole-operation snapshot migration. Native cache
storage remains for existing diagnostic tests, but mapped READLINK never uses
it. Validation is recorded in the goal ledger as the live runs finish.

Local READLINK now checks the retained TMPFS/XFS/DEVFS object's directory type
before consuming link text. A trailing slash resolving to a regular file returns
`ENOTDIR`; a resolved directory returns `EINVAL`. The native component walker
retains a `DevFSNodeRef`, and READLINK now consumes it directly, preserving its
namespace provenance and avoiding a second path lookup, link-cache result, or
device open callback. The existing admitted credential snapshot and namespace
validation remain in force. Fallback lookup also retains DEVFS for READLINK when
a different submitter requires fallback routing, preserving the same type/error
behavior. For a local or same-node submitter, both supported rule types (`LOCAL`
and `HOST`) leave the path unchanged. Such rules now preserve native component
walking, including ancestor search under the admitted snapshot. The check applies
to mixed and unrelated rules as well as a single local rule.

Different-submitter paths that select a local destination now use the retained
component walker too. Route selection uses normalized task-visible text, but a
local walk keeps the original components and checks search before following links
or consuming `..`. After each followed link the walker checks the destination
again. A remote transition publishes a handle with the admitted snapshot and
validated namespace/mount epochs; remote discovery and owner waits run outside
namespace publication. A local alias restarts at the task root. Initial mapped
WKI and HOST paths now use this handoff too, including remote mounts reached by
common local callers. Their original components reach the owner. Self aliases
restart at the captured task root; route comparison uses task-visible paths so
parent steps remain clamped there. Repeated separators can be collapsed for route
matching without removing names or dot components. Remote-directory-FD lookup,
whole-realpath snapshots, and PROCFS retained-target migration still need further
qualification. Route-changing links retain the
raw routed suffix when its normalized destination matches the selected route;
ambiguous raw/canonical route choices return `ENOTSUP` rather than discarding
ancestors. Mapped wire operations negotiate `WKI_CAP_VFS_COMPONENT_PATHS` for dot
components, leaving their interpretation to the confined owner. Missing support
returns `ENOTSUP` for those paths without a normalized or legacy retry.

Explicit owner OPEN, regular-file read, STAT and READLINK now start their physical
component walk at the export boundary. Each step uses the admitted credential
snapshot. Search permission is checked before consuming `..`; stepping above the
export returns `EPERM`, even if later components would reenter it. Absolute owner
symlinks retain physical-path semantics and must name a path inside the export
before traversal starts. Relative targets remain subject to the same boundary.
READLINK without following the final link may still return an outside target as
text. Final confinement and namespace validation remain required before output
or backend mutation. The serving task's root, cwd and credentials are unchanged.
Legacy wire path validation still rejects dot components. Negotiated component
support applies only to explicit credential operations, including version-2
batch STAT; legacy mutation batches remain strict. Normal and isolated builds,
the focused host suites, formatter/audits and KTEST (15,715 checks, zero failures)
passed. Normal and component-capability-off clusters each passed 757 native
checks. The formerly bypassed HOST-transition STAT/access/OPEN and the added
mapped READLINK return `EACCES` with support enabled and `ENOTSUP` without it;
output remains untouched. The initial-path extension passed the builds, focused
host checks, formatter/audits and KTEST (16,102 checks, zero failures), including
common/foreign callers, confined roots, raw lookup handles and task-root clamping.
Initial explicit paths also retain the most specific raw-path export, matching
consumer validation when a host advertises both its root and `/tmp`. A first
native run exposed an `EAGAIN` mismatch here; the correction and its nested-export
KTEST passed. Normal and mixed clusters each passed 897 distinct native checks,
including 140 initial-route cases. The first normal snapshot matrix encountered
an `OPEN_READ_AS` `NO_ROUTE` send failure; its repeat passed all 36 cases. That
intermittent transport failure remains unresolved. Mixed-profile shutdown also
reported nine open rootfs files and used final sync after `EBUSY`; the normal
profile unmounted cleanly. A separate remote-directory-FD
probe passed 10 of 15 cases: five `private/../public` operations still bypass the
private ancestor or return the wrong errno. That probe identified the
descriptor-relative gap addressed below.
These results do not establish forced-race, malformed-peer or RDMA qualification.

The confined owner OPEN, STAT and READLINK adapters also accept an optional
retained directory `File` and a relative path. TMPFS and XFS recover that inode's
current physical location for export and mount validation, then start traversal
at the retained directory. Ancestors above that grant are not searched again;
every directory below it, including one before `..`, uses the admitted snapshot.
Absolute owner links still restart at the export boundary and require search
there. A replacement mount at the recovered anchor returns `EAGAIN` before any
inode/backend mismatch. These adapters leave the worker's credentials, root,
cwd and FD table unchanged. At that stage caller/wire integration was still
required to fix the remote-directory-FD baseline; the owner API alone did not
change it. Both builds, ten focused host checks, formatter/audits, KTEST
(16,364 checks, zero
failures), and 897 normal native regressions passed. The owner-adapter-stage
remote-FD probe still passed 10 of 15 cases, confirming that wire integration
was necessary.

Descriptor-relative READLINK and STAT/LSTAT/access now retain the remote
`File` and original relative bytes in the lookup handle. `READLINK_AT` and
`STAT_AT` selectors in `OP_VFS_PATH_AS` carry that channel's directory FD and
one admitted credential snapshot. Reliable RX classifies both selectors as
requiring VFS read and same-domain identity policy; the owner validates framing,
maps the supplied identity, retains the exact channel-bound FD, and resolves
beneath the export using the retained-directory adapters. The client uses the
FD's existing proxy rather than selecting another lane. Replies are staged and
validated before publishing metadata or link bytes. Unknown selectors remain
rejected; old permission classifiers may return `EACCES`, and the diagnostic
`wki.vfs_directory_paths_off` owner returns `ENOTSUP`. Neither error triggers a
pathname fallback. Directory-relative OPEN was the remaining gap at this checkpoint;
its implementation and verification follow below. The first live candidate exposed missing RX selector classification;
the correction adds coverage to both the rebuilt host policy test and KTEST.
Both builds, rebuilt host checks, formatting and KTEST (16,365 checks, zero
failures) pass. The new 224 native cases pass before and after owner rename and
ancestor chmod, including private-component denial, group controls and unchanged
outputs. All 897 existing normal native regressions pass. The original probe
now passes 14 of 15 cases, with only directory-relative OPEN still failing.
The selector-off profile also passes 224 new cases and all 897 existing cases;
four existing remote `readlinkat` checks explicitly expect `ENOTSUP` under that
flag. This totals 2,242 native checks across the two profiles. The initial
mixed regression run stopped on the old success expectation; the fixture was
corrected without changing the kernel.

XFS's symlink reader also follows the byte-count READLINK contract: it fills the
requested capacity without reserving or writing a NUL byte, including short
buffers and empty targets. String-based internal resolution reserves space and
terminates explicitly. This corrects the short-buffer behavior exposed by the
local native fixture; remote owner staging had hidden it from earlier wire tests.


Directory-relative OPEN now uses `OPEN_AT` (selector 4) in `OP_VFS_PATH_AS`.
The request contains the admitted credential record, retained channel directory
FD, bounded flags and mode, and the original relative components. Both read/write
peer policy and owner filesystem authorization use those flags and that snapshot.
The owner performs the retained-directory walk beneath its physical export;
`O_NOFOLLOW`, exclusive creation, directory requirements and truncation are handled
there. The sender uses the existing directory proxy and rejects unsupported
selectors without retrying by pathname or on another lane.

Client descriptor, mount reference, File/path storage and remote context are
reserved before sending. The owner reserves an invisible FD row before a possible
create or truncate and publishes it only after the open succeeds. Fencing also
retires those reservations. Failed admission releases them; successful replies
must have the exact header and eight-byte FD/type result with zero reserved bytes.
The returned File keeps compatibility path text for display and directory handles,
but cannot use that text as a stream or notification cache key. The OPEN syscall
retains one admitted snapshot across optimistic retries and its serialized local
fallback. Transport failure after a committed owner mutation still has the existing
ambiguous RPC outcome; this is not a distributed rollback protocol.

Verification for this checkpoint is being recorded in Goal 19. The first KTEST run
passed 16,366 checks and failed the existing serialized-lookup test because the
helper's default interface passed a task with no snapshot. Capturing one snapshot
at that helper's entry fixes the mismatch. The source scratch audit was also
updated for existing credential-aware readlink calls and remote denial returns,
while preserving its complete-buffer-production checks. Both rebuilt kernels and
all twelve selected host suites pass. Fresh isolated KTEST passes 16,367 checks
with zero failures and powers off automatically. The normal cluster passes 180
OPEN_AT checks each on TMPFS and XFS, the original 15-case reproduction, and all
1,121 existing regressions. The unsupported-selector profile passes the same
360 OPEN_AT checks and 1,121 regressions. Its first remote-compute snapshot run
returned ETIMEDOUT instead of EACCES on node 2; the complete focused 36-case repeat
passed. The timeout remains an unresolved transport/compute observation, not a
claim of a clean first run. No OPEN_AT fixture or owner-content check failed.


A subsequent direct REALPATH syscall reproduction confirms the pending legacy
resolver gap: on both TMPFS and XFS, a UID 1000 caller outside group 2000 can
resolve `private/../public`, `private/.`, and a link through that private directory;
`private/missing` returns ENOENT instead of EACCES. Public and supplementary-group
controls pass (16/24 total). This was separate from the verified OPEN_AT fix;
the native correction follows below. Whole-realpath snapshot consistency and the wider
PROCFS, namespace-race and remote-identity qualification remain pending.


Native REALPATH now uses the retained component walker for TMPFS, XFS and DEVFS.
It captures one admitted snapshot before lookup, preserves the original path
components through four bounded attempts, and validates the retained final object
under namespace publication. Root stripping uses matching task-root text copied
under the path lock. The path and length are staged until validation and capacity
checks pass; native denials never fall through to legacy pathname caches. The
existing routed-visible/backing ENOSYS contract remains. Remote, PROCFS and other
compatibility backends still require the separate remaining resolver migration;
this checkpoint does not claim whole-REALPATH snapshot or race qualification.

The first and final Normal/isolated builds, twelve existing host suites, the new
connected source audit, and formatting pass. The integrated selftest covers an
explicit restricted snapshot against bootstrap task credentials, group and rooted
controls, privileged cache warmup, and unchanged short-buffer output/length.
The exact final diagnostic kernel passes 16,368 checks with zero failures and
powers off automatically. The expanded native fixture passes 200 cases
across TMPFS/XFS, absolute/relative paths, an inaccessible cwd, LOCAL and mixed
LOCAL/HOST rules, and supplementary groups, with an unreadable final file to
distinguish search permission from read permission.


Live validation also passes the original local 24-case reproduction and all 1,481
existing mapped VFS, directory-grant, OPEN_AT and credential-snapshot regressions;
the 36-case snapshot matrix passes without a retry in this run. The fixture's
initial chroot setup returned ENOSYS because WOS libc has no implementation;
160 actual path cases passed before that setup issue was diagnosed. Rooted task
paths remain covered by KTEST. The unsupported live setup was replaced with
absolute lookup from a restricted cwd; the final 200-case fixture fully passes.

A separate remote REALPATH probe passes 12 of 16 cases on owner TMPFS/XFS.
Restricted `private/../public` and `private/.` incorrectly succeed on both
filesystems; `private/missing` correctly denies search and group controls pass.
These four remote failures remain the next concrete resolver gap. No remote
canonicalization fix or whole-goal completion is claimed by the native change.


### Managed rootfs ownership during incremental updates (Goal19 9DX)

Full rootfs creation installs managed entries as root:root, independently of
host staging ownership. The raw XFS incremental updater applies that same UID0/
GID0 policy when replacing a managed regular file or updating a managed
directory, including the filesystem root. It preserves the requested mode and
file contents/size policy and recalculates the inode CRC after metadata writes.
Previously it changed contents/mode while leaving an old inode's UID/GID intact,
so managed account/configuration files could differ from a full installation.

Only changed managed paths are updated. Existing source-cache rules still
preserve unchanged paths, and unrelated persistent service state is untouched.
Init remains responsible for validating and migrating declared journal/keygen
state and preparing netd runtime state before service launch. This update does
not replace those service-specific checks or change filesystem/kernel ABI.

A real sparse XFS fixture reproduces the previous owner mismatch and verifies
root:root on updated files, parent directories and root; exact contents/modes,
repeat application, unchanged unmanaged service state, pre-write refusal for
wrong inode type/insufficient capacity, and a clean xfs_repair -n result.

### Default service ownership qualification (Goal19 9DY)

The rootfs ownership manifest row is now enforced against requirement 7 of
Goal19. Full and incremental account generation agree; managed installation uses
numeric root ownership; init rejects malformed accounts or declarations before
launch and prepares all declared private/runtime state before starting services.
The existing changed-path incremental policy does not repair unrelated disk data.

`tests/fixtures/service_runtime_state.{cpp,py}` adds read-only inspection through
the public init-status ABI, procfs IDs and inode metadata. It checks the executing
node's hostname, forces node-local VFS routing, correlates each published service
PID with its real/effective/saved UID/GID, and checks that service generations
remain stable during inspection. It verifies account/configuration files,
root SSH directory and authorized_keys permissions, the netd resolver symlink
and owned state, private journal state, and private keygen state. It never reads
private-key contents or changes service configuration.

On the qualified fresh two-node cluster, all 84 checks pass: 45 on the LAN node
and 39 on the WKI-only node. Journald runs as101, netd as102 and LAN HTTPD as100;
the Dropbear listener retains its separate privileged broker role. Testd is
disabled. The WKI-only node's netd runs without network readiness, leaving HTTPD
and Dropbear waiting for their declared dependency. Requiring netd itself to be
READY was an initial fixture mistake, corrected from source and live evidence.

All ten focused host suites pass, including actual XFS ownership tests and
strict account/manifest rejection. The fixture compiles against current WOS
headers and passes clang-format; it has no clang-tidy compilation database entry.
Production source and kernel hashes are unchanged, so this checkpoint required
no Normal or isolated KTEST rebuild. Both disposable VMs were stopped afterward.

Current procfs status exposes IDs but not groups, NNP, sealed grants or authority
ceilings. Init checks those fields before exec; this read-only runtime fixture
does not directly measure them in an already-running daemon. Device authorization
and full service isolation remain separate pending manifest requirements. The
old testd-reduction note was stale because the default service is disabled.

### Credential foundation qualification (Goal19 9DZ)

The credential manifest row is enforced against the immutable-snapshot
requirement and the credential-publication portion of fork/clone/exec semantics.
`Snapshot` exposes copied IDs and const group views; `CredentialStore` copies a
complete value under its IRQ-safe lock. The Task store is private, and the
resource integration is its only publication access. Thread credentials come
from a retained canonical process owner. UID population transfer preflights all
thread charges and publication before committing either; failures preserve both.
Fork/clone initialize private children from one admitted value. Exec revalidates
that value under VM publication and rejects updates from replaced workers.

Current validation passes all 66 groups in `testprog credentials`, 78 native
exec concurrency cases, 32 root/UID104 process-owner lifetime cases, and 144
local/remote-file set-ID cases with four mount-reporting checks. These include
active and blocked old-authority observers during main/worker exec. The lifetime
runner supplies private owner-specific scratch directories: the default `/tmp`
is root-owned0755, so assuming universal creation rights was invalid. All
fixture files and mounts were cleaned up.

The only source change is in the native regression test. A nonexistent strict
WKI target returns EHOSTUNREACH before receiver credential-capability negotiation;
the old ENOTSUP expectation was stale. The test now checks the actual contract,
unchanged IDs, exact supplementary-group contents, NNP and ceiling after failure,
then verifies the original hostname after optional local fallback. It retains
the existing no-regain checks. The first candidate assumed empty supplementary
groups, which the root SSH caller does not have; capturing and comparing the
original list fixes that assumption without relaxing preservation requirements.

Normal and isolated builds, seven focused host suites and formatting pass.
Clang-tidy completes with nonfatal style diagnostics. Fresh isolated KTEST passes
51,830 checks with zero failures and automatically powers off, including atomic
snapshot, process-wide UID transfer, replaced-worker, stale exec admission and
quiescence rollback cases. Initial packaging failed in host supermin before
boot; a retry using workspace temporary storage succeeded. Kernel hashes remain
unchanged. All nine task-owned VMs are stopped; logs, scoped diff, hashes and
the requirement audit are in `build/goal19-credential-qualification`.

This qualification leaves the separate exec-image/version/fence and WKI mapping
requirements open. The whole goal remains active with six enforced manifest
rows and fifteen pending. The nosuid fixture's old comment that mapped chmod
is unimplemented is also stale; its owner-prepared file modes remain valid,
and this run does not depend on that comment.


## Early interpreter construction qualification (9EA)

The expanded `service_interp_cache.cpp` regression passes 26 SPAWN attempts on
current WOS: root warmup, eight same-size corruption/recovery pairs, eight
unreadable cached-interpreter denials under effective UID1000, and a readable
control. Every attempt compares the caller's real/effective/saved UID/GID,
complete supplementary-group list, no-new-privileges, authority ceiling, service
grants, PID and executable path. A caller-owned pipe preserves its CLOEXEC flag
and sentinel byte. Child waits are bounded. Successful writes after failed
construction and successful restored-image spawns establish ordinary cleanup
and retry behavior. The existing readable-interpreter policy is unchanged.

Source review follows the admitted snapshot through unpublished-child ownership,
regular-file interpreter admission, stable content pin, exact cache-byte
comparison/full read, ELF loading, and failure teardown. The initial suspicion
that early-loader close could destroy a borrowed procfs FD was incorrect:
`vfs_open_read_file_as` returns private native/negotiated remote regular-file
handles and rejects synthetic backends; the distinct `vfs_open_file_as` API
preserves procfs FD grants. No kernel change was warranted. Fatal default signals
and group-exit handoffs defer until validated user return; cooperative daemon
shutdown defers while an unpublished child remains owned. This source review
is not a claim of forcing every fatal interruption point live.

Current content-view regression also passes all42 cases:11 each on local TMPFS
and XFS,10 each on remote TMPFS and XFS. It covers existing/new writers,
truncate/append, mapped writes, invalid ELF/missing interpreter, descriptor exec,
worker reads, and final lease release; local-only hardlink cases stay local.
Initial fixture build was PIE although its marker lookup requires ET_EXEC;
that setup run failed before case evidence. Rebuilding with `-fno-pie
-Wl,--no-pie` produced the passing run. Both captures are retained.

Normal Build WOS passes (no kernel rebuild needed), as do seven focused host
suites, the authority audit and fixture formatting. Standalone checker/payload
and content fixture compile successfully. No matching clang-tidy compilation
unit exists for the standalone fixture. No isolated build or KTEST was run in
this iteration because kernel and KTEST sources are unchanged; the earlier
51830/0 result belongs to9DZ. Both disposable VMs were synced, stopped through
validated QMP and confirmed absent. Artifacts: `build/goal19-interpreter-qualification`.

Local early-loader admission and ordinary failure cleanup are now qualified.
Exec remains pending for peer-fence lifetime and the remaining forced
exec/exit/credential interleavings. Full Goal19 stays active at6enforced/15pending.


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

## Remote output completion qualification (9ED)

Previous9EC was verified progress. A new fresh rootless cluster completes all four
TMPFS/XFS consumer restart/same-boot disconnect cases in one run (native80194exit0).
Boot epochs4100786105->1013934708->938479942 change only on resets; old commands255,
owner views0->1->0, independent local ETXTBSY and fresh replacement reads pass.
The bounded observer3076exit0 saw no fresh command delayed>=3seconds. This does
not resolve the earlier intermittent remoteVFS helper-fetch timeout.

The truncated remote diagnostic was isolated with new native service_exec_output
C++/Python fixtures. Exact local/remote sizes4096,131072,1048576 and independent
stderr receipts separate producer completion from SSH receipt. On the final binary,
all local immediate cases pass but every remote immediate case loses stdout or
stderr despite status0. Remote stdout counts4096/0/1048576 have missing/missing/
truncated receipts. Holding the same producer alive1000ms after writing makes all
six cases pass (84423exit0). The subsequent immediate control again fails. Initial
remote stdout prefixes61440/131072 and110592/1048576 also remain recorded.

Source flow identifies the ordering gap: IPC writes can remain in deferred home
pipe backlogs after TASK_COMPLETE on the resource channel publishes proxy child
exit; Dropbear then flushes currently available bytes and closes an empty reader
without requiring EOF. Existing pipe close byte counts preserve backlog lifetime
but do not prevent early child-status publication. No production fix, arbitrary
sleep, or collector workaround was landed. A correct completion barrier must cover
a finite successful-write prefix, preserve background/shared-pipe lifetime and
allow IPC progress without holding exclusive peer lifecycle while waiting.

Details, source entrypoints and next implementation constraints are in
`docs/remote_exec_output.md`. Artifacts: `build/goal19-exec-fetch-observation`.
Standalone fixture build, Python compilation, formatting and authority audit pass.
Initial compile lacked the explicit existing libc++ include path; corrected build
passes. No matching clang-tidy fixture unit. Kernel sources/hashes unchanged, so
Normal Build WOS and isolated KTEST were not rerun this turn. Both guests synced;
launcher80648exit0 and taskPIDs1037490/1037487 confirmed absent; descriptor archived.

Goal remains active6enforced/15pending. This turn adds a reproducible failing
regression and changes the next action from vague output truncation to a concrete
completion-ordering implementation. No external blocker; exact loaded goal and
--no-setup authorization intact. Ordinary OS functional work, no penetration tests.

## Hostname authorization and publication (Goal 19, 9EE)

Hostname changes now use the named central HOSTNAME policy. The syscall retains
its original immutable caller snapshot and permission-before-usercopy ordering;
network-admin service grants do not grant hostname authority. The global hostname
is published and copied under a bounded IRQ-safe lock. `gethostname`, `uname`,
boot logging and WKI initialization consume owned, terminated snapshots, with no
usercopy, allocation, VFS calls or nested peer lock inside the hostname lock.
WKI still selects its identity once at initialization.

The unchanged native fixture reproduced 1,696 concurrency failures on the prior
kernel, then passed on the candidate with zero failures across 24,000 writes and
48,000 reads. All four identity profiles pass: nonroot, root with a zero ceiling,
nonroot with the sealed network-admin service grant, and root with only HOSTNAME
remaining. Denials preserve the original name and precede invalid-pointer access.
Both runs restored the hostname and removed their helper. Both builds, six focused
host suites, the authority audit and formatting pass. Final isolated KTEST passes
51,965 checks with zero failures, including the new invalid-publication and
independent-snapshot test, then powers off automatically. All owned VM processes
are confirmed stopped.

This qualifies hostname policy/publication, not all network administration or the
full goal. The remote exec output and intermittent executable-fetch issues remain
open. Artifacts and the pre-edit patch contract are in
`build/goal19-hostname-policy`.

## Interface rename admission (Goal 19, 9EF)

At the 9EF checkpoint, `NETCTL_LINK_SET` admitted and published the requested
interface name before changing other fields or invoking driver callbacks.
The 9EG staging contract below supersedes that publication ordering. Collision checking and
publication share the network registry lock, including explicit registration;
automatic registration skips names already occupied. Both native rename APIs
retain the exact registration identity. Registry retirement rejects late rename
publication, and no callback or network wait runs under the registry lock.
The syscall's original immutable NETWORK_ADMIN snapshot remains the decision
identity. Service privileges, usercopy ordering and ABI layouts are unchanged.

The diagnostic baseline forced a competing rename inside a fake driver's open
callback. The old control flow changed other fields and then rejected its final
rename; the regression failed while all 51,965 existing checks passed. This was
an expected selftest-failure panic, followed by validated QMP shutdown. Five native
permission profiles pass on the candidate, including restricted root and both
service grants, unchanged interface records on refused updates, raw-socket policy
and public datagram creation. Both builds and six host suites pass. Final isolated
KTEST passes51,989 checks with zero failures, including both new regressions, and
powers off automatically. All task VM processes are confirmed stopped.

This addresses rename refusal after partial effects. It does not qualify driver
open error propagation, the statusless MAC callback interface, all concurrent
multi-field reads, or remote device-control mapping. These remain explicit work
in the active network-hostname domain. Artifacts: `build/goal19-net-link-policy`.

## Driver open refusal and staged interface names (Goal 19, 9EG)

The interface flag helper now returns driver open errors before publishing local
UP state or other flags. Both NETCTL_LINK_SET and legacy SIOCSIFFLAGS return that
status. WKI's existing proxy callback already returns an owner-side open refusal;
the syscall layer now preserves it instead of reporting success.

Combined rename/open updates reserve the requested name while retaining the
exact device registration. The old name stays visible until successful commit.
Failure cancels the claim before changing the other requested fields. Collision
admission covers registration and automatic naming as well as rename. No registry
lock crosses a driver callback, wait, usercopy or allocation. The fixed claim pool
and competing claims on one registration return EBUSY before effects. A retained
claim survives concurrent registry removal; completing it releases ownership and
cannot restore the retired device to the registry. Commit/cancel are idempotent.
This supersedes 9EF's immediate name publication before driver callbacks.

The unchanged-control-flow diagnostic baseline fails the new driver-refusal test
while all51,989 existing checks pass. Five existing native permission profiles
pass on the candidate. Both builds, six focused host suites and the authority
audit pass. Final isolated KTEST passes 52,063 checks with zero failures and
automatically powers off. All three task VM processes are confirmed stopped.
Other WKI transport errno classification, remote control
identity mapping and statusless MAC/close callback behavior remain pending.
Artifacts: `build/goal19-net-open-policy`.


## Remote NIC open errno domains (Goal 19, 9EH)

A missing local proxy returns ENODEV. Preparation, send and wait failures are
translated from WKI transport codes into errno using the same classifications
as remote VFS. Owner-returned operation errno bypasses that translation because
the two numeric domains overlap. Cancellation drains the stack waiter before
returning; state and initial RX-credit publication still require owner success.
There is no wire or credential policy change.

Both builds and six focused host suites pass. The baseline private-object
regression fails as expected; candidate full KTEST passes 52,065 checks with
zero failures, then automatically powers off. Both task VMs are confirmed stopped.
No fake NIC or packets are needed for the lookup, retirement and invalid-send tests.
Remote actor mapping, statusless close/MAC, and response-consumption ordering
against peer retirement remain pending. Artifacts: `build/goal19-net-open-errno`.


## Remote NIC completion ownership (Goal 19, 9EI)

Each synchronous NIC operation now owns its result until consumption. A claimed
response writes the caller's completion before waking it; peer cleanup may clear
the proxy slot but cannot replace an already completed owner error or response
length. Duplicate replies preserve the pending owner, and late consume/cancel
operations cannot clear a successor. Open, close and MAC callbacks use the same
contract, while asynchronous statistics polling stays separate. Wire framing,
credential decisions and driver callback signatures are unchanged.

The unchanged-result-storage baseline reproduces the retirement race. Both
candidate builds, six host suites, formatting and the authority audit pass.
Full isolated KTEST passes 52,067 checks with zero failures and powers off
automatically; both task VM processes are confirmed stopped. The tests use private proxy state and actual
publication/cleanup/cancellation helpers, without registering a fake NIC.
This supersedes the pending response-consumption race noted in9EH. Explicit
remote actor mapping, statusless MAC/close error reporting and post-success
local state publication versus retirement remain open.
Artifacts: `build/goal19-net-op-completion`.


## Explicit remote NIC OPEN identity (Goal 19, 9EJ)

Both local network-control entrypoints pass one admitted credential snapshot to
the remote NIC open callback. The owner maps its canonical record under the
peer policy captured before reliable ACK, then applies central NETWORK_ADMIN
before native driver work. Root and active service privileges still require
existing SAME_DOMAIN_ROOT trust. Worker task credentials grant no authority.
A fixed worker slot retains the exact device binding and request; RX neither
allocates that work nor blocks on the driver. The internal optional open_as hook
preserves existing native driver callbacks and the public syscall ABI.

NET attach support is required before a mapped OPEN is sent. An older owner is
refused with ENOTSUP; legacy credentialless owner OPEN also returns ENOTSUP.
There is no fallback. Driver denial precedes subscription/state/credit effects,
and existing caller-owned completion preserves exact owner errors.

Six native credential profiles pass through NETCTL and legacy IOCTL on both a
current two-node cluster and an older-owner/current-consumer cluster. Authorized
root/network-service callers succeed with the current owner and get ENOTSUP with
the older owner. Four profiles without local authority get EPERM in either case.
Whole interface records remain unchanged on both nodes. The fixture reopens an
already-UP NIC and stores its result on the consumer, avoiding dependence on the
separate remote-exec output delivery issue. Both clusters are synced and stopped.
Normal/isolated builds, ten host suites, formatting/tidy and authority audit pass.
Full isolated runtime qualification is recorded in the goal ledger.

CLOSE and SET_MAC still need explicit actor mapping and fallible callback
contracts. Combined driver transactions and post-success concurrent publication
remain pending; the network-hostname domain is not yet marked enforced.
Artifacts: `build/goal19-net-open-identity`.


9EJ final isolated KTEST passes **52,069 checks, zero failures**, followed by
automatic poweroff. The first run exposed a fixture expectation error: registered
NICs already have broadcast/multicast flags. The corrected test compares against
those registered defaults on denial and requires their preservation on success.
No production change was needed. All six VMs used for this checkpoint are
confirmed stopped; final host checks and authority audit pass.


## Explicit remote NIC CLOSE identity (Goal 19, 9EK)

NETCTL and legacy IOCTL now pass their admitted snapshot to a fallible close_as
callback. Local UP state and other requested fields remain unchanged until the
callback succeeds. The remote proxy preserves owner errno and converts only
transport errors, drains canceled stack waiters, and requires independently
advertised CLOSE support. Legacy owner CLOSE returns ENOTSUP without effects.
The fixed pre-ACK worker retains the exact NET binding and admitted peer policy;
shared caller mapping and NETWORK_ADMIN run before native close. Successful close
publishes DOWN and removes the subscription. Existing root/service peer trust
and native callback signatures remain unchanged.

Six profiles through both APIs pass on current nodes and with an archived9EJ
owner. Current root/network-service callers close and reopen over a separate
WKI NIC; denied callers receive EPERM without field changes. Older-owner CLOSE
returns ENOTSUP even though OPEN works. Full records are restored after success
on owner and consumer. Both clusters are synced/stopped and temporary files are
removed. Builds, ten host suites, audit and formatting/tidy pass; full KTEST
qualification is recorded in the goal ledger.

MAC mapping, combined driver transactions and concurrent publication remain
pending. A post-send timeout still has an ambiguous owner outcome, so failure
propagation does not establish rollback of an already committed remote action.
Artifacts: `build/goal19-net-close-identity`.


9EK full isolated KTEST passes **52,072 checks, zero failures**, followed by
automatic poweroff. New caller-preservation, owner mapping and actual transport
failure cases all pass; existing completion/retirement tests now cover the mapped
OPEN/CLOSE opcodes. All five VMs used for this checkpoint are confirmed stopped.


## Combined NIC link and MAC admission (Goal 19, 9EL)

A request carrying both state and MAC now uses one mapped owner operation.
Canonical caller mapping, NETWORK_ADMIN and native MAC support checks precede
all driver effects. OPEN is the only fallible native commit phase; MAC commits
only after it succeeds. Local flags, MTU, queue length and a reserved name remain
unchanged until the combined result succeeds. Standalone legacy MAC IOCTL retains
the same admitted snapshot. Credentialless MAC requests refuse ENOTSUP.
Native check_mac prevents unsupported drivers from silently reporting success
or changing earlier fields in a combined request. Existing public syscall and
native commit callback ABIs remain unchanged.

The full isolated suite passes52,074checks/0failures. Both builds, elevenhost
suites, formatting/tidy and authority audit pass. Live modern native12, mapped
current18, older-owner18 and legacy native12 requests pass across six profiles.
Modern MAC changes are reported unsupported; owner no-op assignments exercise
the mapped combined commit. Supported legacy virtio performs and restores real
native MAC changes through both APIs. All fixture files are removed and all
nine task VMs are stopped, including one preboot TAP mismatch that was corrected
with a matching multiqueue attachment. No production changes followed validation.

Modern virtio runtime MAC control, concurrent writers/readers, post-send outcome
receipts, and remote LAN recovery after physical MAC changes remain unqualified.
In particular, shared-NIC forwarding excludes owner-unicast frames while proxy
and owner MAC roles are distinct; the new actor mapping does not resolve that
existing data-plane behavior. See the protocol contract in docs/wki_security.md.
Artifacts: `build/goal19-net-link-identity`.


### 9EM — serialize admitted NIC control requests

`NetDeviceControlGuard` gives each device one nonblocking control-request owner.
NETCTL link updates and the five mutating link ioctls acquire it after the
captured caller's NETWORK_ADMIN check, before namespace admission, native MAC
admission, driver callbacks or field publication. A competing request returns
EBUSY. The guard uses one lock-free atomic compare/exchange, allocates nothing,
and keeps interrupts and preemption enabled across existing remote waits.
The caller's retained device or exact owner binding outlives its guard.

Mapped WKI OPEN/CLOSE/LINK use the same device guard after identity mapping and
central authorization. Their handler owns the guard through subscription updates
and notifications, preventing a second admitted writer from entering between
hardware commit and subscription publication. UID/GID and syscall record layouts,
wire records, peer trust policy and native driver callback signatures are unchanged.
Authorization refusals retain precedence over contention, and an unsuccessful
acquisition never releases another request's ownership.

New deterministic KTEST coverage enters competing complete link requests from
inside a driver callback and checks no name/MTU/queue effects, EPERM before EBUSY,
release after negative/positive driver errors and success, independent devices,
and owner preservation after failed acquisition. Existing owner identity matrices
now probe contention for all OPEN/CLOSE/LINK caller/policy combinations.

This checkpoint serializes admitted control requests. The low-level namespace
API retains its existing name-claim contract. Asynchronous owner state notifications,
coherent field reads, proxy/physical MAC semantics and ambiguous post-send outcomes
remain pending; this guard is not a reader lock or a distributed rollback mechanism.
Artifacts: `build/goal19-net-control-writers/`. See the goal ledger for validation results.


9EM validation completed in this task:
- Normal Build WOS session10289 and isolated diagnostic build14821 exited0.
- Eleven focused credential/network/WKI/virtio host tests96336 passed; formatting
  and clang-tidy29950 passed with nonfatal style warnings; authority audit passed.
- Isolated KTEST61664 exited0: 52,084 passed/0 failed at150.369, automatic poweroff
  at150.370. New writer-overlap and guard-ownership tests passed at33.710/33.714;
  mapped owner OPEN/CLOSE/LINK matrices passed at40.294/40.298/40.305.
- Current two-node cluster42692: all six service profiles passed, 18 primary
  combined/legacy control requests plus restorations; complete owner interface
  records unchanged. This is functional authorization evidence, not a test of
  LAN recovery after a physical MAC change or cross-node concurrent readers.
  Fixture files removed, both guests synced, validated launcher exited0.
- All three task QEMU processes2080038/2082304/2082293 are confirmed absent.
- Normal SHA256 a019c290bbaebefdcb3508cb26c7c63cbb72ca690ca30f595fc863d3e380deda;
  isolated SHA256 c1018f28d78607d36c0cf0264d9bde40195dc2499c7690cbfea62d0196c98ce5.
  No production edits followed the qualified builds; final hashes match.
Goal19 remains active: 6 enforced domains, 15 pending. Continue coherent field
publication/owner notifications and explicit proxy/physical MAC semantics;
post-send outcome ambiguity remains unresolved. No penetration testing performed.


### 9EN — coordinate proxy owner notifications with control completion

`NetDeviceControlGuard` now uses a short publication lock around acquisition and
release. The admitted interval still spans driver/control work with no spinlock
or IRQ-disable interval held across callbacks or remote waits. An optional internal
`finish_control` hook performs bounded metadata publication under that lock before
clearing the busy flag. This closes both notification-before-admission and
notification-at-release handoff gaps; a busy-bit check alone cannot do that.

WKI notifications retain the latest validated owner cache while a proxy control
request is active. Link state and nonzero MTU updates become visible when the
control interval ends. Repeated notifications coalesce within existing bounded
proxy storage; zero MTU retains its existing no-update meaning, including after a
previous pending nonzero update. Late notifications do not change retired proxies.
Retirement still fences new operations immediately, but its visible DOWN store
waits for an admitted writer's completion if necessary. Completion forces DOWN
and discards pending notification projection for retired devices. Attach-time
owner-cache consumption follows the same publication order.

The lock order is existing proxy registry (retirement paths), device publication,
then proxy state. The finish hook does not take the registry lock, allocate, send,
log or invoke hardware. All caller/binding pins still outlive the guard. Canonical
caller snapshots, errno domains, UID/GID/syscall/wire layouts and native driver
callback signatures remain unchanged. This synchronizes writers; public multi-field
read snapshots, receive-order versus remote commit ordering, proxy/physical MAC
roles and post-send outcome ambiguity remain pending. Received notifications are
not represented as proof of an invented global commit order.

New KTEST cases cover notifications before/during/after control, multiple queued
updates, failed competing acquisition, zero MTU, retirement with/without pending
state, late notifications, and idle retirement. Host source gates enforce lock
order and release/retirement routing. Initial host77110 exposed an old direct-store
assertion; the updated gate checks the stronger publication contract. Initial
builds57409/25067 passed, then review added attach synchronization. Final builds
16879/76348 pass; eleven final host checks19383 and tidy41070 pass. Runtime results follow. Artifacts: `build/goal19-net-state-publication/`.


9EN validation completed in this task:
- Final Normal Build WOS16879 and isolated diagnostic build76348 exited0.
  Eleven focused host checks19383 and formatting/tidy41070 passed; the final
  authority audit found zero documented legacy raw-root checks.
- Full isolated KTEST48174 exited0: 52,085 passed/0 failed at150.840; automatic
  poweroff150.841. New WkiRemoteNetState publication case passed40.381 and prior
  writer overlap/guard ownership cases passed33.802/33.805.
- Current two-node cluster84035 passed six-profile CLOSE/reopen (12 primary
  requests) and combined-link (18 primary requests) fixtures, plus restorations.
  Complete owner interface records matched before/after each fixture. Files were
  removed; both guests synced; validated launcher2167367 exited0. All three task
  QEMU PIDs2164910/2167474/2167491 are confirmed absent.
- Normal SHA256 b24f4418123a7485a736a33d51bc69eb1d089ed814e916ec5c6e642011ce4cbd;
  isolated SHA256 0b27fd3383cff50deb08af4d3d9466a51e3abe228d27b616acd2af139d26a7ea.
  Qualified artifacts remained unchanged after runtime checks; no subsequent
  production edit was made.
Goal19 remains active at6enforced/15pending. Coherent read snapshots, native CDC
hotplug publication, proxy/physical MAC semantics, and post-send outcome ambiguity
remain unqualified. `next-work.txt` records exact current source entrypoints.
All checks were ordinary OS functional validation; no penetration testing occurred.


### 9EO — owned interface configuration and generic retirement fencing

Interface list/legacy queries, registry diagnostic link fields, WKI owner link
metadata and resource advertisements now consume `NetDeviceConfigSnapshot`.
It owns name, MAC, MTU, queue length, flags, ifindex and state. Admission captures
one pre-control record under the publication lock; a reader uses that record
while a control request owns the live fields, and copies live configuration under
the same lock otherwise. Reads therefore neither observe a partially committed
configuration nor wait for a remote operation. Advertisement name and link fields
come from the same owned record. Name commit joins registry-before-publication
lock order. This is per-device link coherence, not whole-enumeration membership,
statistics, L3 address-table or packet-data-path coherence.

Generic retirement fences future control admission and makes snapshot state DOWN
immediately. An active native callback can finish its field writes, but guard
completion forces live state DOWN. Successful fresh registration resets the fence
after old retained users drain. CDC now initializes UP before registration and uses
the retirement marker before quarantine/unpublication. Review also moved its
immutable remotable-operations pointer clear from quiesce to detach, after the
existing netdevice reader drain, so retained control completion cannot race pointer
removal. Immediate CDC I/O fencing and external resource withdrawal are preserved.

Each WKI notification helper defers only its existing notification type while a
control owns the device. Idempotent guard completion releases ownership and the
short publication lock, then flushes the requested owner-state and/or resource
advertisement notifications. Owner OPEN/CLOSE/LINK explicitly finish before their
response; these operations retain their existing owner-state-only notification
policy. Existing caller snapshots, authorization ordering, errno domains,
UID/GID/syscall/wire records and native driver signatures remain unchanged. No
lock spans driver calls, remote waits, usercopy or notification send. The full
network domain remains pending for L3/owner-cache/packet metadata reads,
physical-versus-proxy MAC roles and ambiguous post-send outcomes.

The new KTEST reads between live field writes and rename commit, checks registry
rows, retires during an owned control, checks visible DOWN despite late native UP,
and verifies fresh-generation reuse. Initial builds61998/59138 passed; host7133
caught the old retirement-helper source expectation, and host23470 caught a new
gate parser that only recognized auto functions. The parser was corrected to also
handle void functions; all12 host checks50820 passed. Initial full KTEST50637
passed52,105/0 at151.995 and powered off151.996; PID2244083 absent. Final CDC
operations-lifetime follow-up builds8060/69489 and12host47269/tidy3730 passed.
The corrected runtime qualification is recorded below.
Artifacts: `build/goal19-net-config-snapshot/`.

9EO validation completed:
- The first two-node run exposed a notification regression: generic completion
  emitted a resource advertisement after owner CLOSE, which rotated the resource
  generation and replaced the proxy before reopen. The failed close report and
  both node logs remain in remote-close/ and supported-vm/. The correction
  defers each existing helper's own notification type and explicitly finishes
  owner control before its response. A new KTEST covers notification types and
  idempotent completion. No failed fixture is counted as passing.
- Corrected Normal Build WOS39829 and isolated diagnostic build58259 exited0;
  all12 focused host checks20508 and formatting/clang-tidy36234 passed. The
  authority audit reports zero documented legacy raw-root checks.
- Corrected full isolated KTEST41626 exited0: 52,116 passed/0 failed at152.826,
  automatic poweroff152.827. Snapshot/retirement and notification completion
  cases passed34.024/34.028. PID2390701 is absent.
- Corrected two-node cluster15972 passed six-profile CLOSE/reopen (12 primary
  requests) and combined-link (18 primary requests), plus restorations. Complete
  owner interface records matched before/after each fixture. Eight QEMU USB NIC
  cycles also produced eight guest CDC ready/detached pairs and restored the
  complete interface records. All fixture files were removed.
- Both guests synced; validated launcher2395096 exited0. All seven checkpoint
  QEMU PIDs2244083/2314060/2317041/2317044/2390701/2395203/2395208 are absent.
- Normal SHA256 afcd93dfe36c5b534f339c1eb2cb4d936e2f012eee270dfe156dc21424bf9225;
  isolated SHA256 533dde54a676a8216926d238ff5f78e5c06fef4f0fb9a9aa8a03dad3a9946f61.
  Production/test source and kernel hashes remained unchanged after runtime.
Goal19 remains active at6enforced/15pending. L3 table/owner-cache snapshots,
packet metadata, physical/proxy MAC roles and post-send ambiguity remain open.
These were ordinary OS functional tests; no penetration testing was performed.


### 9EP — one owned owner-address record for attachment policy

WKI post-attach policy now obtains `WkiRemoteNetOwnerL3Snapshot` by retaining the
exact live proxy, copying its IPv4 address/mask and full IPv6 suffix under the
existing proxy state lock, then releasing the reference. Missing or retired
proxies return an invalid empty record. Both sparse attachment decisions and
ATTACH_ALL route construction consume that single record; attachment policy no
longer casts private_data to inspect mutable owner fields.

This preserves authenticated peer/resource lifecycle checks, wire and syscall
layouts, sparse mode's no-route policy and the separation between received owner
addresses and local interface configuration. No lock spans route insertion,
logging or network operations. A copied record is stable across subsequent
notifications, including while link projection is deferred by an admitted control.
The snapshot describes one received record; it does not establish global owner
commit order or make subsequent route insertion atomic with retirement.

The new deterministic KTEST checks empty-but-valid state, missing proxies,
IPv4/IPv6 replacement during control, retained-copy stability, no local address
publication and retired-state refusal. Source gates enforce retained copying and
one-record attachment decisions. Normal Build WOS21756, isolated build81763,
12 focused host checks and formatting/clang-tidy23780 passed. Runtime qualification is recorded below. Artifacts: `build/goal19-net-owner-l3/`.
Native IPv4 table readers/writers and their advertisement/overlap consumers still
need coherent access; no broader network-domain completion is claimed.

9EP validation completed:
- Normal Build WOS21756 (RelWithDebInfo) and isolated diagnostic build81763
  (Debug) exited0. All12 focused host checks and formatting/clang-tidy23780
  passed; the authority audit reports zero documented legacy raw-root checks.
- Full isolated KTEST88702 exited0: 52,117 passed/0 failed at151.783, automatic
  poweroff151.784. The new owner-L3 snapshot case passed40.640; PID2473762 absent.
- Fresh two-node cluster74137 automatically attached the owner NIC and passed
  six-profile CLOSE/reopen (12 primary requests) plus combined-link (18 primary
  requests), including restorations and complete owner-record comparisons.
  Fixture87601 exited0; files were removed. Both guests synced, validated
  launcher2477245 exited0, and PIDs2477378/2477384 are absent.
- Normal SHA256 4121344185a870268b59e1d4a0598eab03e6a29d5080cb53be9dd13acc065e92;
  isolated SHA256 54e9756d342554b2814b8139d218c93d8456568f20152e7fb00a1aa796d9fad9.
  Final source/artifact hashes match the tested builds; no later production edit.
Goal19 remains active at6enforced/15pending. Native IPv4 table synchronization,
packet metadata, physical/proxy MAC roles, post-send ambiguity and the other
pending authority domains remain unfinished. The new record does not claim
atomic route insertion versus withdrawal or global owner commit ordering.
All runtime checks were ordinary OS functional validation.


### 9EQ — synchronized IPv4 configuration and owned policy reads

Native IPv4 address add/set/delete and both legacy primary-address setters now
serialize their fixed array and count under one short per-interface IRQ-safe
lock. `IPv4ConfigSnapshot` copies the full bounded record before releasing that
lock. NETCTL/legacy queries, legacy route selection, WKI owner metadata and
advertisements/overlap checks, devfs interface reports and ARP/IPv4/raw/TCP/UDP
address selection all consume owned records. The address lookup scans the same
bounded live interface registry and copies each candidate's address record.
A source gate rejects raw IPv4 array/count access outside netif.cpp/netif.hpp.

User operations retain their existing credential snapshot and authorization-before-
mutation ordering. Primary address creation still defaults to /24; matching
nonreplace requests return EEXIST, unavailable deletion/mask requests return
EADDRNOTAVAIL, capacity errors and other native errno remain unchanged. Primary
address and mask changes preserve the other field atomically. Allocation and
interface publication precede the IPv4 lock; notifications, packet work, logging
and usercopy occur after unlocking. Interface lifetime/retirement, UID/GID ABI,
wire records and local/owner address separation are preserved. IPv4, IPv6, link
configuration, registry membership and route commits remain separate observations.

A new KTEST covers primary updates, owned-copy stability, failed mutations,
capacity/refill order and retirement. The new native service_net_ipv4 fixture
checks six credential profiles, two legacy operations and NETCTL add/duplicate/
delete on each node, with per-profile restoration. Normal Build WOS41965 and
isolated build11150 passed; all12 focused host checks and formatting/clang-tidy
51715 passed. The initial host-check process could not start because the sandbox
failed synthetic mount creation with a quota error. Filesystems had ample free
space/inodes; the checks succeeded outside that sandbox, and no files were deleted.
Final isolated and two-node evidence follows. Artifacts:
`build/goal19-ipv4-config/`.

9EQ validation completed:
- Normal Build WOS41965 and isolated diagnostic build11150 exited0. All12 host
  checks and formatting/clang-tidy51715 passed. The authority audit reports zero
  documented legacy raw-root checks. The sandbox startup quota error was a host
  environment failure before test execution; no generated artifacts were deleted.
- Full isolated KTEST95353 exited0: 52,158 passed/0 failed at181.155; automatic
  poweroff181.157. New NetIPv4Config copy/update/denial/retirement case passed33.895.
  PID2558804 is absent. Coverage output took longer than the preceding run; the
  same live handle was observed to completion without restarting the VM.
- Fresh cluster8748 passed60 new IPv4 authorization checks: six profiles on
  each of wos-0/wos-3 exercise legacy primary address/mask, NETCTL add, duplicate
  refusal and deletion. Root and sealed network-admin are allowed; nonroot,
  zero-ceiling root, journal-only and network-admin with dropped ceiling are
  denied with EPERM. Each profile restores the original loopback records.
- Existing mapped close/reopen12 and combined-link18 primary requests pass six
  profiles each, with restorations and complete owner-record equality. DHCP,
  owner NTP synchronization and automatic WKI NIC attachment also succeeded.
  Fixture files were removed; both guests synced; validated launcher2563669
  exited0. Cluster PIDs2563815/2563812 are absent.
- Normal SHA256 765ecc87802a74c34cf1b083a44c3ff2f87570cae16a42ec9aa91e7fe5357fd5;
  isolated SHA256 8dbfc6d0918a578c8ad2ed7ebedb29e549317d3cf5da16379951fa8f36ea321c.
  Production/test source and kernel hashes match the validated build inputs;
  no production changes were made after those builds.
Goal19 remains active at6enforced/15pending. IPv4 record synchronization is now
qualified; separate-family/link/global-membership and distributed commit ordering
are not newly claimed. Remaining authority domains require requirement-level
review rather than treating unrelated future driver features as prerequisites.
All checks were ordinary OS functional validation; no penetration testing occurred.

### 9ER — service resource management retains credential authority

Service configuration and selection previously checked only the original PID 1
role. They now require that role plus INIT_CONTROL and RESOURCE_LIMIT from one
immutable caller snapshot. Authorization precedes usercopy, service locking and
pending-selection changes. Credential denial returns EPERM and preserves the
previous selection. Existing authorized invalid-selection clearing, immutable
configured slots, service accounting, lock order, UID/GID ABI and syscall records
remain unchanged.

The central helper lives in platform/security/authorization.hpp. The actual
service_resource.cpp request bodies are exercised by a kernel test using private
Task objects, including PID 1 after either authority bit is dropped, nonroot PID 1,
non-init callers and threads. The real init process is never modified. Host
policy/source tests verify the authority matrix and check-before-mutation order.
New service_resource_authority.{cpp,py} fixtures check public configuration and
selection denials, protected status output, process-local hard-limit no-regain,
permitted soft-limit raises and invalid-limit state preservation.

Validation completed:

- Normal Build WOS90851 and isolated diagnostic build71835 exited0. All eight
  focused rebuilt host suites passed. Production formatting/clang-tidy87143,
  fixture formatting/check, Python parse and credential source audit passed;
  zero documented legacy root checks remain.
- Full isolated KTEST2386 passed52,159/0 at152.793 and powered off152.794. The
  new Resource/ServiceManagementRequiresRoleAndRetainedCredentialAuthority
  test passed34.238. Existing population transfer and whole-process credential
  tests ran in this same suite. Launcher exited0; PID2648649 is absent.
- Fresh --no-setup cluster28748 passed108 resource authorization checks across
  wos-0 and wos-3: root, UID1000, zero-ceiling root and HTTP/journal/network
  credential profiles. All denied output and hard-limit checks preserve state;
  child-local CORE limits isolate changes from parent and services.
- Existing runtime-state fixture passed84 service identity/ownership checks
  (45 LAN,39 WKI). Native testprog credentials passed all66 groups with exit0
  and empty stderr, including existing ptrace policy/register controls.
  This is ordinary OS functional validation, not penetration testing.
- Fixture files were removed and both guests synced successfully. Validated
  launcher2652559 exited0; cluster PIDs2652710/2652703 are absent. No production
  sources changed after the builds. Both kernel hashes still match:
  Normal fe49dfa19c0d7fcc5602e78c249f07a44d149c0717fecfb58e447261f4c00d83;
  isolated 8607f906cfc9b5f690c0defb6dcc76b470a2230f1300a9569c16e2e260a24977.

The resources manifest row is now enforced for local credential/role decisions,
process limit no-regain, service policy admission and atomic UID population
transfer. Remote resource identity/session mapping remains in wki-resources;
this does not qualify unrelated resource-governance features. Goal19 remains
active with7 enforced/14 pending domains. The prior resources row's one-line
pending description is superseded by this source audit and current evidence.
Artifacts: build/goal19-service-resource-authority/ (patch-contract.txt,
resource-audit.txt, implementation.diff, build/host/runtime logs and descriptors).

### 9ES — ptrace image capture retains its admitted target credentials

GET_IMAGES and GET_IMAGE_CATALOG used to authorize before tracer input copying,
allocation and the image publication lock. An intervening credential-changing
exec could therefore make the later capture describe a different authorization
state. The original decision now saves the complete target Snapshot. Under the
existing SharedVmemPublicationGuard, snapshot_target_images compares current
target credentials with that saved value before reading target metadata/memory.
A changed value returns EPERM without publishing image records, count or status.
A fresh authorized request can still capture the current image.

Changes are confined to ptrace.cpp/.hpp, its KTEST registration and the authority
source gate, plus new service_ptrace_images.{cpp,py} functional fixtures. No new
locks, authority bits, syscall records or wire formats were added. Initial
EPERM-before-usercopy behavior remains. Tracer usercopy and allocation stay outside
the existing image lock; the target reference and immutable captured image records
retain their previous lifetimes. Other ptrace operations are unchanged.

Validation completed:

- Normal Build WOS17839 (RelWithDebInfo) and isolated diagnostic build92817
  (Debug) exited0. Five focused host suites and formatting/clang-tidy2821 passed.
  No host runtime_images target exists in this configured tree; compiled loader
  regressions are included in the full kernel suite, not claimed as a host run.
- Full isolated KTEST28866 passed52,160/0 at153.065 and powered off153.066.
  PtraceImages/ChangedCredentialsPreserveDeniedSnapshot passed34.071: the actual
  capture helper rejects changed fake-target credentials without altering its
  array/count/status, then accepts a fresh decision. This is a deterministic
  helper regression, not a claimed live timing-race reproduction. Launcher
  exited0; PID2740991 is absent.
- Fresh --no-setup cluster31864 passed28 public image-list checks across wos-0
  and wos-3. Both ABI versions work as root and matching-user tracers. Invalid
  catalog versions preserve output; target dumpability revocation denies both
  APIs with byte-for-byte unchanged headers/arrays. Detach succeeds and both
  targets are reaped. The native credential suite again passes all66 groups,
  exit0 with empty stderr. Fixture files were removed.
- Both guests synced; validated launcher2745143 exited0. Cluster PIDs2745284
  and2745283 are absent. Production and fixture source hashes still match.
  Normal SHA256 7ce3dd22e22c3de5f7d11de473d8b24ec4b301c757578815269c46c432212bc3;
  isolated 0410b6fbefb7f5e23fb4c94419bce86cd421113cb2110773944774ce3de5ffac.

The ptrace row remains pending: image-list capture is qualified, while memory,
register and other target-state admission paths still need their requirement-level
review. See build/goal19-ptrace-image-authority/next-work.txt for specific source
leads, which are not claims of newly demonstrated live failures. Goal19 remains
active with7 enforced/14 pending domains. All validation was ordinary OS functional
work; no penetration testing occurred. Artifacts, patch contract, scoped diff,
source hashes, build/test logs and archived descriptors are under
build/goal19-ptrace-image-authority/.

### 9ET — ptrace memory copies retain their admitted target credentials

PEEKDATA, POKEDATA, READ_MEM and WRITE_MEM now retain the target Snapshot from
their original authorization decision. Page preparation may fault outside the
image publication mutex. Every preparatory pin is released before acquiring that
mutex, because exec holds it while waiting for pagemap-access pins. The actual
copy/word helper then compares target credentials with the admitted value and
uses only nonfaulting, nonwaiting stable page pins under the existing guard.
Credential changes cannot redirect the copy into a different authorization state.

Scalar words still pin both halves before writing. Bulk transfers preserve their
existing partial-EFAULT contract: a valid mapped prefix survives a later page
fault. An initial credential denial leaves caller buffers and the MemIo header
unchanged. If an admitted view is invalidated after an authorized prefix has
already transferred, the operation stops and reports EFAULT plus that prefix's
exact byte count; it never reports an initial EPERM after partial effects. Tracer
usercopy, target fault preparation, allocation and waits stay outside the copy
lock. No syscall/record/wire format, new authority bit or new lock is introduced.

Changed files: ptrace.cpp/.hpp, ptrace_ktest.cpp, the credential-policy and
usercopy source gates, and service_ptrace_memory.{cpp,py} functional fixtures.
The actual prepared-copy/word helpers have a deterministic stale-credential test
with unchanged buffers/progress, fresh invalid-page controls and released access
counters. The native fixture covers root and matching-user access, COW isolation,
word/page boundaries, valid-prefix faults, dumpability revocation and cleanup.

Validation completed:

- Normal Build WOS23222 and isolated diagnostic build91922 exited0. The first
  host run found that the existing usercopy source gate still inspected the old
  helper layout. Its checks now follow the split helpers while retaining the
  dirty-pin commit and whole-word assertions. All seven focused host suites and
  formatting/clang-tidy10525 passed. Fixture formatting/check and Python parsing
  passed. The credential audit still reports zero legacy root checks.
- Full isolated KTEST38040 passed52,161/0 at153.725 and powered off153.726.
  PtraceMemory/ChangedCredentialsPreserveDeniedCopyAndWord passed34.463. Launcher
  exited0; PID2827467 is absent. No live timing-race reproduction is claimed by
  the deterministic helper test.
- Fresh --no-setup cluster71112 passed48 native memory checks across wos-0 and
  wos-3, including all four APIs, cross-page scalar and bulk data, partial fault
  counts, denied header/buffer preservation and target-verified final contents.
  Parent mappings remain unchanged after target COW writes. Targets were reaped.
  Existing image-list28 checks and native credential66 groups also passed,
  with native exit0 and empty stderr. Fixture files were removed.
- Both guests synced successfully; validated launcher2832807 exited0. Cluster
  PIDs2832962/2832937 are absent. All production/fixture source hashes match.
  Normal SHA256 65ac74f1183845c19bece0f35f63ee5e8be335bfb00485bf7cf58164a45219b4;
  isolated f92241715279749c6428d14209e9643e35d147daad83618af41d564bdb021fe8.

The ptrace row remains pending for register, control and other target-state
admission/lifetime review; image and memory capture changes do not establish
whole-domain completion. Goal19 remains active with7 enforced/14 pending domains.
All work was ordinary OS implementation and functional validation; no penetration
testing occurred. Artifacts: build/goal19-ptrace-memory-authority/, including the
pre-edit patch contract, scoped diff, hashes, logs and archived descriptors.

### 9EU — ptrace register authority and exception-stop handoff

GETREGSET/SETREGSET and hardware-breakpoint requests retain the target Snapshot
from their original authorization decision. Bounded capture/apply helpers compare
it under SharedVmemPublicationGuard before reading or changing GPR, XSAVE or DR
state. XSAVE reads now copy an owned kernel buffer to the tracer. Caller usercopy
stays outside the guard; register/XSTATE/breakpoint validation remains before
mutation. Short KILL/CONT/SINGLESTEP/SYSCALL/INTERRUPT/SETOPTIONS operations keep
the decision and update under the existing guard. DETACH serializes release while
remaining available to the binding owner after losing inspection authority.

Live positive stepping exposed a separate stop-path defect: notification followed
place_task_in_wait_queue, whose idle path never returns, and its switch path used
the generic exception epilogue for a kernel continuation. The sole-caller helper
now invokes a bounded stop callback after context/FPU/wait publication, outside
the runqueue lock, before handoff. It uses the existing normalized context-switch
return and first-run daemon handling, matching conditional exception waits. The
saved exception frame clears TF. No new lock, authority bit, syscall/record or WKI
wire format is introduced; reserved-current wake handling and task lifetimes are
preserved. This fixes ordinary debugger execution needed to validate authority.

The initial fixture incorrectly assumed anonymous MAP_SHARED stays shared across
fork. It now explicitly attaches SysV SHM in each process, removes the segment
while attached, uses bounded waits and cleanup, and tests both user-code and
syscall-loop initial stops on both nodes. Failed initial reports/binaries remain
in the artifact directory. They are not successful runtime evidence.

Changed files: ptrace.cpp/.hpp, scheduler.cpp/.hpp, ptrace_ktest.cpp,
credential_policy_audit_test.py and service_ptrace_registers.{cpp,py}. Deterministic
kernel tests exercise actual stale-Snapshot capture/apply helpers with unchanged
outputs/state and fresh valid controls. Source gates cover admission propagation,
usercopy outside guards and stop publication before nonreturning handoff.

Validation completed:

- Corrected Normal Build WOS15510 and isolated diagnostic build8896 exited0.
  Nine focused host suites passed, including both scheduler suites. Formatting
  and clang-tidy32682 passed with existing nonfatal diagnostics. The audit reports
  zero legacy root checks. Native fixture compilation and Python parsing passed.
- Initial isolated KTEST59831 passed52,162/0 at152.648 and powered off152.649.
  After the stop-path correction, full isolated KTEST30051 again passed52,162/0
  at151.744, powered off151.745 and exited0. The actual register Snapshot test
  passed34.471. PIDs2914521 and2999866 are absent. These deterministic helper
  tests are not claimed as a live timing-race reproduction.
- Initial cluster89255 passed48 memory,28 image and66 credential checks, but the
  new register/control fixture failed as described above; those positive stepping
  failures are retained. Corrected cluster19067 passed128 register/control checks
  across wos-0/wos-3 and user/syscall initial-stop modes. GPR/FPU read/update/restore,
  hardware breakpoint install/remove, stepping, continue, interrupt, syscall stop,
  irreversible ceiling drop, unchanged denied buffers and stopped progress, detach
  after authority loss, resumed progress and target reaping all passed.
- The corrected kernel also passed the existing48 memory/COW/boundary checks,
  28 image-list checks and all66 native credential groups. Native exit0, stderr
  empty. Fixture files were removed and tracees reaped. No user-page-fault or panic
  entry was observed in the corrected cluster serial logs.
- Both corrected guests synced successfully; validated launcher3002747 exited0.
  PIDs3002904/3002899 and all initial task VMs are absent. Source and kernel hashes
  match the tested files. Normal SHA256
  29c1965737b1be71df5e466eae6fbbbc11f8fa8f6567af0dd2bf3ae3d35115b2;
  isolated 2f2482df64c877de6fc9d864b0b160c144eee7bdb3b428ead81db83c51f6c769.


The ptrace row remains pending for LIST_THREADS, GETEVENTMSG, GET_REMOTE_INFO and
SYSCALL_WAIT admission/capture/claim review. Image/memory/register qualification
alone does not establish whole-domain completion. Goal19 remains active with
7 enforced/14 pending domains. Artifacts: build/goal19-ptrace-register-authority/.
All work was ordinary OS implementation and functional validation; no penetration
testing occurred. Live launches used --no-setup under the user's authorization.

### 9EV — remaining ptrace observations qualified; ptrace row enforced

LIST_THREADS retains the original target Snapshot across caller preparation and
compares it under SharedVmemPublicationGuard before capturing IDs with Task lifetime
references. GETEVENTMSG authorizes and copies an owned Event under the same guard.
GET_REMOTE_INFO captures bounded target metadata under its admitted credentials;
the WKI query resolves only that captured submitted-task ID outside SharedVM.
The existing Task-based query delegates to this owned-ID helper. No WKI wire,
record layout, syscall ABI, authority bit or new lock is introduced.

SYSCALL_WAIT now compares initial admission before resume or initial-exit capture.
Each later stop has a fresh guarded policy decision and an owned StopInfo capture,
allowing legitimately authorized exec credential changes. It claims the event
before copying output. Failed copy releases the claim; successful copy commits it
once. Claimed kind/status determines exit reporting, and parent/tracer reaping
semantics remain intact. No guard spans usercopy or parking. Initial authority
denial changes no target control state or caller output. An authorized resume is
not rolled back if a later stop loses authority, preserving the existing rich-wait
contract. Thread enumeration retains its existing best-effort topology semantics.

Changed files: ptrace.cpp/.hpp, remote_compute.cpp/.hpp, ptrace_ktest.cpp,
credential_policy_audit_test.py, usercopy_source_test.py and the new
service_ptrace_observation.{cpp,py} functional fixture. The actual kernel helpers
have a deterministic stale-Snapshot test preserving outputs/control state, fresh
metadata/initial-exit controls, a competing event claim and released-claim retry.
All fixture tasks are private and marked EXITING to prevent scheduler publication.

Validation completed:

- Normal Build WOS53770 and isolated diagnostic build72379 exited0. Ten focused
  host/source suites passed, including WKI remote-compute, scheduler, credential,
  usercopy and ABI checks. Formatting/clang-tidy31067 passed with existing nonfatal
  diagnostics. Final native fixture compilation, formatting and Python parsing
  passed. The credential audit reports zero legacy root checks.
- Full isolated KTEST45815 passed52,163/0 at153.217, powered off153.218 and
  exited0. PtraceObservation/ChangedCredentialsPreserveOutputsAndClaimsAreRetryable
  passed34.106. This tests actual helper/claim transitions, not a claimed live
  timing-race reproduction. PID3085273 is absent.
- The initial proxy fixture inherited its report descriptor and remote launch
  correctly rejected unsupported FD export with ENOTSUP. After closing that FD,
  both wos-0 modes passed; the wos-3 reverse launch still inherited forwarded
  stdio. The final no-I/O payload closes all harness descriptors before exec.
  Initial failed reports/logs are retained. The first transport timeout did not
  trigger a VM restart: guest ps confirmed the original helpers had exited,
  their report was saved, and only their exact temporary files were removed.
- Final cluster51012 passed180 observation/register/control checks: user and
  syscall initial-stop modes on wos-0/wos-3, metadata outputs and size queries,
  self access, rich syscall and exec/exit/reaping, PTRACE_KILL cleanup, real proxy
  lookup in both directions, irreversible authority-drop denials with unchanged
  output/state, detach, resumed progress and target reaping.
- All204 previous ptrace checks passed (128 register/control,48 memory/COW/fault,
  28 image-list), plus all66 native credential groups with exit0/empty stderr.
  WOS strace of true passed on both nodes with normal exit reporting. Fixture
  files were removed. Both guests synced; validated launcher3091164 exited0.
  PIDs3091387/3091386 are absent. Source and tested kernel hashes match:
  Normal ee196696adb7ced0573065c1191eb1439b9de6f758d1ac0fd8dbe8805ee006f7;
  isolated f17cdf8a46178722b212aa12b35ecc846d7dbd3f2a7639f0b5e69c7888632416.

The request-by-request audit covers all25 ptrace request numbers, including
unsupported GET_MAPS and invalid-request behavior, central all-ID/group/dumpability
and irreversible-authority policy, retained Task lifetimes, admitted capture,
traced set-ID suppression and release/reaping. With current source and runtime
evidence, the ptrace manifest row is now enforced. This supersedes the earlier
pending annotations for ptrace; it does not close WKI identity or other domains.
Goal19 remains active with8 enforced/13 pending domains. All work was ordinary OS
implementation and functional validation; no penetration testing occurred.
Artifacts, contracts, scoped diff, hashes, audit and logs:
build/goal19-ptrace-observation-authority/.


## Service credential observations (Goal19 9FW)

A newly generated `/proc/<pid>/status` view reports these public fields from
the same canonical process credential snapshot as its existing `Uid`/`Gid`
rows:

| Field | Representation |
| --- | --- |
| `Groups` | Space-separated supplementary GIDs; empty when none |
| `NoNewPrivs` | Decimal 0 or 1 |
| `WosAuthorityCeiling` | Decimal mask using `CredentialAuthority` bit positions |
| `WosServicePrivileges` | Decimal bounded service-grant mask: journal-read=1, network-admin=2 |
| `WosServicePrivilegesLocked` | Decimal 0 or 1 for sealed service grants |

Ceiling bits bound root authority and service grants; they do not independently
grant nonroot authority. The status view retains its existing per-open cache.
Open a fresh view to observe later credential changes. Private procfs admission,
status permissions, existing ID fields and syscall/wire formats are unchanged.

The extended service-runtime fixture passes188 checks on two current-kernel
nodes:33 owned-child transition checks on each, plus70 source and52 peer
service/ownership checks. It verifies empty service groups, declared NNP, exact
ceilings and grant/seal values, stable service generations, and unchanged
state ownership. Owned controls cover eight supplementary groups and irreversible
ceiling/grant drops. The listener's declared root credentials are measured
separately from the reduced journald, netd and HTTPD credentials. The fixture
does not measure inactive keygen or waiting network consumers.

Normal/isolated builds, two host suites, source/policy checks and final
formatting/tidy pass; KTEST reports52,195 passes and zero failures. The initial
standalone fixture accidentally used the compiler's default SafeStack profile
and faulted before its assertions. Rebuilding with the repository's normal
userspace flags passed on the same guests. Both initial processes were verified
retired, all helpers removed, guests synced and exact host processes stopped.
This replaces9DY's historical IDs-only observation limit; broader service and
procfs authorization coverage remains pending.
Evidence: `build/goal19-service-credential-status/validation.txt`.


## Remote IPC descriptor consumer authority (Goal19 9FX)

An IPC export is an already-issued File grant to one immutable consumer node.
Control admission matches that node and resource ID under the export registry
lock before pinning the File. PTY, socket and epoll controls also match their
resource type; poll accepts any issued export type. Close mutation repeats the
same owner check under its mutation lock. A known mismatched PTY close or read
close cannot queue new state, close a File or retire another consumer's export.
Callbacks execute outside the registry lock with the retained File reference.
This preserves open-descriptor authority across credential drops; it does not
substitute the ambient RX worker's credentials for the grant. Wire layouts,
syscall ABI and existing unavailable-endpoint errors are unchanged.

Actual kernel ownership/reference/close tests and52196 isolated selftests pass.
Normal two-node functional validation passes68 socket/PTY assertions for root
and UID100, plus10 exact output and pipe-close cases. Original failed fixture
evidence is preserved: SO_KEEPALIVE lacks a TCP getter in current WOS; the
corrected fixture uses the implemented SO_RCVBUF pair. Both guests were synced
and stopped. Peer netd restarted during the run; its cause remains unproven.
Detailed artifacts: build/goal19-ipc-consumer-authority/validation.txt.

Remaining IPC work includes legacy epoll_ctl's ambient worker FD lookup (epoll
export is already refused), raw physical FUTEX_WAKE without an issued futex
grant, the wrapping resource ID allocator, and further caller/lifetime mapping.
These are separate gaps; the IPC authority domain remains pending.


## Ungranted legacy controls and IPC ID exhaustion (Goal19 9FY)

Legacy OP_EPOLL_CTL and OP_FUTEX_WAKE now return ENOTSUP with their original
operation, resource ID and completion cookie. The former carried FD numbers
without an exporting task namespace; the latter carried a raw physical address
without an issued futex grant. Neither dispatches a native callback or looks up
a local object. Current export planning already refuses epoll and issues no
futex grants. Local epoll/futex syscalls are unchanged. Distributed support
requires an explicit retained object grant and compatible identity protocol.

IPC export IDs are reserved as one range under the registry lock. A range that
would exceed UINT32_MAX fails with EOVERFLOW before RDMA registration, export
publication, handoff references or output map writes. The last usable ID may be
issued exactly once; zero marks permanent exhaustion. Empty export plans still
succeed. Abandoned reservations may consume IDs, but retirement and failed
preparation never recycle them within a boot. Existing RDMA eligibility excludes
IDs larger than its16-bit doorbell field, so message IPC continues past that
boundary. This does not change wire layouts or replace peer/session fencing.

This supersedes9FX's legacy callback and wrapping-ID gaps. Descriptor import
provenance remains to be qualified: attach_task_fds currently accepts home_node
from a submitted map without a source-identity argument. The current export path
sets every entry's home to the submitting node; forwarding and lifecycle must
be mapped before changing receiver admission. Full IPC authority remains pending.

9FY validation: Normal and isolated builds pass;52198 KTEST assertions pass with
zero failures, including actual resource-range boundaries and matched refusal
completion. The source gate confirms no native epoll/futex callback remains in
remote IPC. The current-kernel two-node fixture passes124 assertions in10 cases
atUID0/100, including normal local epoll/futex and preserved epoll interest after
remote-exec ENOTSUP. Ten exact output/close cases pass. All fixture receivers
retired, helpers were removed, service PIDs stayed stable, and both guests were
synced and stopped. Evidence: build/goal19-ipc-grant-boundaries/validation.txt.


## IPC descriptor import provenance (Goal19 9FZ)

TASK_SUBMIT validates the complete IPC map against the authenticated submitter
after bounded context decoding and before identity-scoped source fetch or image
construction. Every nonempty map requires a nonzero, nonbroadcast source; each
entry must name that source as home and a nonzero resource ID. Descriptor
numbers and supported resource types remain bounded, and both duplicate FD
numbers and duplicate endpoint IDs within a map are rejected. Current exports
issue a separate ID per descriptor, including aliases, and name the exporter as
home. The receiver cannot be asked to use a third node's grant on the submitter's
behalf.

Private descriptor installation requires the same source argument and repeats
the validation before allocating proxy/File objects. Provenance failure maps
to UNAUTHORIZED/EACCES; malformed entries map to RESOURCE_INVALID. Existing
valid grant authority, NOFILE inheritance, CLOEXEC replacement, atomic FD
installation/rollback, output completion and pending delivery handling stay
unchanged. Wire records and syscall ABI are unchanged.

This supersedes9FY's import provenance question. It does not add direct
re-export of proxy descriptors, which current export planning already refuses.
Broader IPC peer/session lifetime qualification remains pending.9GB checks the
resource channel and complete declared reply length before proxy lookup or
completion.9GC retains each unanswered (operation,cookie) identity across local
timeout, so wrap skips requests whose replies may still arrive. Only an exact
reply or a definitive failure before reliable-send acceptance releases it.
The fixed64-entry table returns EAGAIN before request publication when full;
an exact late reply restores capacity. Healthy controls release each identity
and can repeatedly wrap without a descriptor lifetime limit. Channel-generation
admission across peer retirement still needs separate qualification.

9FZ validation: both builds, focused host/source/audit/format checks and52199
KTEST assertions pass. Ten live descriptor cases/124 assertions and10 exact
output/close cases pass. The additional B->A->B matrix passes11/12 stages;
UID100 worker's third receiver accepts query/signal but produces no observation
marker and does not complete normally. Captured B125 has pendingUSR2/ALRM/KILL
in kern_yield; it retires before caller-stack capture. Source163 is later reaped
with signal9. Peer heartbeat failures and a netd restart were also recorded,
with causes unproven. Original evidence is retained, owned processes/files were
cleaned, and both guests were synced/stopped. This remains an unresolved
execution/lifetime requirement. See build/goal19-ipc-import-provenance/validation.txt.

9GA preserves the9FZ handoff failure as unresolved. Three unchanged-kernel
matrices pass36 stages after the same descriptor/output workload, with normal
source reaping, all stage receivers absent and full helper cleanup. The new
host observer can capture a stage still pending2seconds after admission; no
stall occurred in these matrices, so its automatic trigger was not exercised.
The saved-stack reader itself was validated on a waiting HTTPD service and
recovered four kernel frames plus the exact PerfCallsiteInfo source location.
The artifact is build/goal19-worker-handoff-stack/cluster/capture-stacks.py;
run-observed.py owns the original fixture process and preserves its outcome.

Source review explains that pending interrupting signals make WKI process park
ineligible, so kern_yield can be part of an uninterruptible wait until response
or timeout. It does not identify the failed receiver's caller. The configured
netd readiness timeout is180seconds with restart-on-failure; observed restart
intervals are consistent with that policy on the WKI-only node, but no matching
lifecycle event was captured. Do not use restart alone as transport-failure
evidence. Both guests were synced and stopped. No kernel build or KTEST was
rerun because this increment changed only diagnostic artifacts and evidence.


9GB validates IPC control responses before proxy lookup and waiter effects.
Both completion entrypoints require RESOURCE and a complete declared payload
using wide size arithmetic; poll wakeup requires both declared fields. Invalid
channel, truncated header/body, absent resource ID and oversized declared
length leave waiters, response bytes and references intact. The current valid
reply, busy-slot consumption and duplicate handling contracts are preserved.
No wire/syscall ABI or cookie allocation changed.

Normal/isolated builds, focused checks and52200 KTEST assertions pass. Both
live kernel IDs match;10 descriptor cases/124 checks,10 output/close cases and
one12-stage return-home matrix pass. Four source children are normally reaped,
all stage receivers retire, fixture files are removed and both guests are
synced/stopped. The observer saw no stall. Cookie freshness and the9FZ handoff
failure remain pending. Evidence: build/goal19-ipc-response-envelope/validation.txt.


9GC retains timed-out control identities until an exact reply resolves them.
It distinguishes reliable acceptance from definitive pre-send failure; an
initial physical TX failure still returns WKI_OK when retransmission owns the
request, and therefore keeps its identity. No retained record contains a Task
or stack pointer. Timer-owned completion is never overwritten by a late reply.

Actual handler tests cover late-reply isolation, wrap, saturation/recovery and
failed-send retirement.131075 successful identity-model cycles cross two full
cookie spaces. Normal/isolated builds and52202 KTEST checks pass, followed by
current-kernel10 descriptor cases/124 checks,10 output cases and12 return-home
stages with complete owned cleanup. Both guests are synced/stopped. No handoff
stall was reproduced. Peer/channel generation across retirement remains a
separate pending boundary;9FZ remains unresolved. Evidence:
build/goal19-ipc-cookie-retirement/validation.txt.

9GD binds imported IPC control requests to the admitted compute session's
RESOURCE channel allocation. Import rejects a missing, mismatched or retired
identity before allocating proxies. Socket, PTY and epoll controls retain that
identity through reliable-send commit; deferred replies carry the admitted RX
identity by value across the File callback. Neither path selects a replacement
channel by numeric node/channel lookup.

Connected HELLO and HELLO_ACK epoch changes now mark IPC endpoints inactive
and close worker admission before resetting channels. This bounded marker
retains references and does not allocate, wake or wait in RX context. Deferred
IPC cleanup follows compute cleanup, drains claimed workers and endpoint
references, and completes before DEV/RESOURCE admission reopens. Existing
cookie retirement, descriptor publication/rollback, peer policy and wire/syscall
layouts remain unchanged. Data sends, pumps, deferred closes and RDMA still
need their own generation-lifetime audit; these control changes do not establish
all IPC session boundaries or resolve the intermittent9FZ handoff failure.

Both builds, three focused host suites, seven source gates and52204 KTEST
checks pass. The new KTEST cases exercise real marking/draining and synthetic
channel retirement, including unchanged reference/waiter state before drain.
Current-kernel live checks pass10 descriptor cases/124 assertions,10 output
cases and12 return-home stages. All owned processes and helpers retire; both
guests are synced and stopped. No handoff stall occurs. These stable-cluster
checks do not establish a live connected-epoch reset. Evidence:
build/goal19-ipc-epoch-retirement/validation.txt.

9GE extends imported descriptor identity to DATA. Task-context capture checks
the admitted RESOURCE generation before and after capturing DATA; import
requires both exact channel identities before proxy/File allocation. Pipe and
socket writes retain DATA through reliable commit. Accounted pipe sends keep
their accepted-byte update under the existing channel lock; retired-channel
failure terminates the blocking write through its credit-refund path.

Immediate proxy closes use the retained channel. Queued closes copy the DATA
identity and message bytes, so work already popped before peer cleanup cannot
select a replacement allocation. Export-side discard scheduling also captures
once for its queue and bounded fallback, but still needs the original export
identity to reject an exporter that reaches scheduling after a reset. Pump,
receipt and missing-export cancellation ownership therefore remain pending,
along with RDMA session lifetime. No wire/syscall layout or descriptor-grant
semantics change.

9GE validation passes both builds, three focused host suites, source/audit
gates and52205 KTEST checks. The new test exercises real popped-close ownership
and blocking pipe-write refund/accounting against a retired synthetic channel.
Current-kernel live checks pass10 descriptor cases/124 assertions,10 output
cases and12 return-home stages. All owned processes/helpers retire and both
guests are synced/stopped. No handoff stall occurs;9FZ remains unresolved.
Evidence: build/goal19-ipc-data-retirement/validation.txt.

9GF captures an original RESOURCE/DATA pair before export publication and
retains it in each export, submission guard, submitted task and cleanup
snapshot. Nonempty export plans require live matching channels; a peer cleanup
epoch change during preparation rejects publication. Both inline and VFS_REF
submission compare prepared exports with the final TASK_SUBMIT allocation
before publishing the submitted task. File handoff-pin release does not erase
the independent channel identity needed for later cancellation.

Pumps retain DATA across native reads and export retirement; receipt workers
retain RESOURCE across their callbacks. Discard scheduling accepts an explicit
DATA identity, including cleanup after the export object has gone. It cannot
capture a replacement channel. An ordered incoming close may use its admitted
DATA token; RESOURCE cannot be reinterpreted as DATA. Missing-export error
receipts capture their companion RESOURCE allocation under the admitted DATA
generation. All IPC message sends now use exact identities. Wire/syscall
layouts, descriptor grants and publication/rollback remain unchanged.

The separate software RoCE transport still needs authenticated region/session
ownership qualification. This message-channel work does not establish that
boundary or resolve the earlier9FZ handoff failure.

9GF validation passes Normal and isolated builds, three host suites, source and
authority gates, and52207 KTEST checks. New direct kernel tests cover original
export identity after retirement and cleanup identity through submitted-task
moves. Current-kernel live checks pass10 descriptor cases/124 assertions,10
exact-output cases and12 return-home stages. Both guest build IDs match; all
fixture processes/files retire and both guests sync and stop. No handoff stall
occurs. Full Goal19 remains8 enforced/13 pending.
Evidence: build/goal19-ipc-export-retirement/validation.txt.

9GG makes RoCE receive admission honor transport publication before parsing or
opcode effects. Secure WKI already withholds initialization; the cmdline token
alone does not enable RoCE, correcting the earlier9GF reconnaissance note.
Receive and lookup acquire the flag published with release after callback
initialization. Existing packet consumption and wire/syscall ABI are preserved.

Both builds, focused host/source/audit checks and52208 KTEST checks pass. The
new direct kernel lifecycle case preserves region bytes/completion state for
all four opcode values while uninitialized, verifies four packet releases and
exercises actual region helpers as positive controls. The isolated VM powers
off and its host process is absent. No new Normal multi-node run is claimed.

The older blanket zone-denial claim in docs/wki_security.md was also stale:
ZONE_RDMA peer policy can admit message-based zone operations, and backing
selection falls back to the primary ivshmem transport. A null dedicated RDMA
pointer therefore does not prove shared-memory data paths disabled. Deferred
zone admission and shared-memory authority are the next source-qualified audit;
9FZ and the full authorization goal remain open. Evidence:
build/goal19-roce-receive-admission/validation.txt.

9GH retains the exact ZONE_MGMT allocation in every zone and deferred create
record. The setup worker reacquires peer lifecycle ownership and rejects a
retired channel or pending reset before allocation or waiter completion. Setup
can allocate/yield locally under that gate, but performs no peer ACK wait.
Initiating creation uses nonblocking admission because device callers may hold
binding references needed by cleanup; contention returns internal WKI_ERR_BUSY
before publication. Its gate is released before waiting for the create ACK.

All message sends in zone.cpp use retained identities, and all nine direct
zone RX handlers compare the admitted allocation before lookup/effects and
match the selected zone before memory, callback or waiter changes. Connected
resets now pause zone messages through deferred cleanup; device users drain
before remaining zones retire and admission reopens. Wire/syscall layouts,
zone access bits and existing reference/waiter ownership remain unchanged.

This does not qualify primary ivshmem data access, raw backing-pointer lending,
RoCE unregister-before-free, same-session zone-ID/cookie reuse or device-layer
numeric notifications. Those remain in the follow-on source map at
build/goal19-zone-channel-lifetime/next-recon.txt.

9GH validation passes both builds, four focused host/source suites, nine
source/audit-unit gates and52209 final KTEST checks. The first KTEST had one
fixture-model failure: reactivation failed to advance allocation generation.
Its original kernel/logs remain preserved. Correcting that synthetic allocation
step makes the unchanged production implementation pass actual setup, backing,
message and waiter lifetime checks. Both owned VMs are stopped. No live
connected-epoch reset or Normal multi-node result is claimed this increment.
Evidence: build/goal19-zone-channel-lifetime/validation.txt.


9GI replaces the block server's borrowed backing pointer with an exact
WkiZoneRef, transferred from creation before the initiating slot pin releases.
Local acquisition checks expected peer and local access; new ring work checks
binding retirement and original zone/channel liveness. Existing users retain
backing through completion. Both binding moves transfer ownership, ring setup
precedes publication, and cleanup withdraws diagnostic pointers under the
registry lock before exact destruction/release outside it. The old raw get_ptr
API is removed. Wire/syscall layouts and block geometry remain unchanged.

The actual kernel case passes local grant checks, binding moves, a successful
FLUSH callback, retirement refusal, same-ID successor preservation, channel
retirement/reallocation refusal and final backing release. Both builds, focused
host/source/audit gates, formatting and full isolated KTEST52210/0 pass; the VM
automatically powers off and is absent. No Normal multi-node runtime evidence
is claimed for this increment. Proxy borrowed zone access, primary ivshmem
data/session authority, RoCE unregister ordering, wire ID/cookie reuse, device
notifications and live reset qualification remain open, as does9FZ. Full Goal19
remains8 enforced/13 pending. Evidence: build/goal19-block-zone-retention/validation.txt.


9GJ converts proxy attach/resume and cleanup to WkiZoneRef. Acquisitions name
the expected owner and local read/write grant; pointer publication/withdrawal
is serialized with diagnostics under the registry/state locks. Late rkey
refresh reads the pinned allocation under its zone lock and checks channel
liveness outside it. New I/O checks the grant after io_lock acquisition, and
ring validators retain their geometry/range checks while refusing expired
grants. Failed setup and cleanup destroy exact retained allocations. Resume
drains old local users before replacing a binding. Numeric zone lookup and
destruction are absent from dev_proxy.cpp. Wire/syscall layouts and published
bdev storage remain unchanged.

The new direct kernel case and adapted SQ-fence/batch-atomicity cases pass on
real retained backing. Both builds, four host/source suites, ten source/audit
gates, formatting and full isolated KTEST52211/0 pass. The test VM powers off
and is absent. This proves local ownership/admission; live remote block
attach/reset, primary ivshmem data authority, device notification identity,
RoCE unregister/raw callback ownership and same-session wire reuse remain
open. The9FZ handoff caller remains unknown. Full Goal19 remains8 enforced/13
pending. Evidence: build/goal19-proxy-zone-retention/validation.txt.


9GK routes device reliable zone notifications through the retained zone's
original management-channel allocation and original zone ID. READ/WRITE
notification payloads remain zero-range, and rkey exchange remains a separate
setup operation. Server raw completion wakes now check zone liveness as well.
No numeric channel fallback remains in these notification functions; existing
pins, native wake optimizations, duplicate handling and wire/syscall layouts
remain. The old proxy native-doorbell comment was corrected to match its
existing reliable-message implementation.

Both builds, formatting, four host/source suites, ten source/audit gates and
full isolated KTEST52212/0 pass. The new direct test covers notification
preparation and retirement without enabling WKI or transmitting network frames;
initialized authenticated TX and live reset are not established by it. The VM
automatically powers off and is absent. Evidence: build/goal19-zone-notification-authority/validation.txt.

Goal25 owns separate RDMA cryptography/replay/revocation work and explicitly
distinguishes Goal19 user identity. The next Goal19 audit uses its nine numbered
requirements to classify the thirteen pending rows and validate current source
and runtime evidence. This does not excuse enabled unauthorized paths or claim
Goal25 complete. The old inventory status paragraph above was stale and now
points to the current manifest; historical per-domain notes remain labeled as
such. Scope map: build/goal19-zone-notification-authority/scope-audit.txt.

### Goal19 9GL — retained procfs limits admission and complete inventory gate (2026-09-13)

The completion audit found that `/proc/<pid>/limits` discarded the caller
snapshot captured by the read dispatcher and re-read ambient credentials. The
limits helper now receives that original Snapshot and process PID, uses the
Task retained by ProcFileData, and applies own-process/RESOURCE_LIMIT policy
before accounting, allocation, cached output and EOF. Missing or retired targets
still return ESRCH. Per-open cache/text, canonical thread Owner handling, lock
ordering and syscall/wire ABI remain unchanged.

The schema-v1 authority audit now requires all 21 declared domains. Removing a
pending row cannot satisfy completion; tests reject every required omission in
both audit modes and retain positive complete-inventory and legacy-drift checks.
This gate does not prove the adequacy of free-form historical evidence.

Normal Build WOS and the isolated diagnostic build pass. The new actual procfs
dispatcher KTEST passes at 2.296s; the full isolated run reports **52,213 passed,
0 failed** at 162.122s and powers off at 162.123s. Uncached/cached/zero-count/EOF
refusals preserve output, cache and accounting; self/admin reads and target
retirement/reference cleanup are exercised. Formatting/tidy, four focused
host/source checks, VM source gate and credential policy audit/unit pass.
VM 2945621 and launcher 2877175 are absent; session 73934 exited 0.

Artifacts: `build/goal19-procfs-limits-snapshot/` (patch contract, scoped diff,
review, build/test logs, exact kernel hashes and runtime logs). Normal kernel
SHA256 `247e9bdcf6771a729e17bcd946da558c0223f59d0fb8a84c3c82277e7b57733a`;
isolated `92780fc4f37888f1df8f5a9acb51167478efbde97178b0e1edc631ad4842615b`.

Full Goal19 remains active: **8 enforced / 13 pending**. Current
`--require-complete` correctly rejects pending exec. No new multi-node runtime
qualification or resolution of the earlier9FZ unknown handoff caller is claimed.
Continue the domain audit; the journal device still selects its ambient-caller
open adapter although devfs offers an admitted-snapshot callback. Read its
retained-descriptor policy before changing that handoff. Keep separate Goal25
transport requirements separate from Goal19 authorization requirements.
Only ordinary OS functional tests were run; live `--no-setup` authorization
persists.

### Goal19 9GM — journal open consumes devfs admission (2026-09-13)

The shared journal CharDeviceOps table now uses `open_with_credentials`, carrying
the immutable Snapshot that devfs admitted through the main journal and module
node callback. The ambient-caller open adapter is removed. Read opens still
require KERNEL_DEBUG or effective JOURNAL_READ; write-only open remains public.
Refused opens preserve the cursor. Later reads and polls independently capture
and check current authority as before. No record ABI, logging/emission path,
ring/waiter locking, cursor accounting, service policy or wire changes occur.

The new real devfs-dispatch KTEST covers five identities across three access
modes, including service101 with its journal grant and the same identity after
dropping its authority ceiling. Every successful File is closed, endpoint refs
return to baseline and the temporary node is removed. Existing record/cursor/
waiter denial tests now call the actual operation table for open. Both journal
cases pass (1.986s and1.989s). Normal Build WOS and isolated diagnostic builds,
formatting/tidy, three focused host models and policy source/audit checks pass.
Full isolated runtime: **52,214 passed, 0 failed** at161.218s, poweroff161.219s,
session12916exit0; VM3024273/launcher2955352 verified absent.

Fresh current-kernel two-node service qualification passes **188 checks**:33
owned credential/no-regain transitions per node plus70 source/52 peer service
identity and ownership checks. Journald is101 with NNP1/ceiling16384/grant1/
sealed1 on both nodes. Each daemon persisted a valid608-byte journal record
matching its current boot ID (source sequence490 at70.289457s, peer397
at31.940381s), with state owned101:101. Both live kernel build IDs match the
archived Normal kernel; peers are connected with configured policy4095.
After-fixture inventories contain expected services only. Helpers are removed,
both guests synced, cluster session59510exit0, VMs3027656/3027630 and launcher
3027514 verified absent. Initial standalone sync-helper compile omitted libc++
headers; corrected project include and `-fno-sanitize=safe-stack` flags compiled
both native fixtures before execution. The initial error log is retained.

Artifacts: `build/goal19-journal-open-snapshot/` includes contract, review, scoped
diff, source hashes, build/host/runtime logs, journal persistence metadata,
matching live captures and cleanup proof. Normal kernel SHA256
`bedec13053f384c2c0efc64cb8e963c53d07fb41e1354e7c4df85afea5377c0b`; isolated
`8d1a48946b530e511cc7dbae808cc453c326637e454c8ad25baca9d1f6f11b62`.

Goal19 remains active, **8 enforced /13 pending**. This qualifies journal open
and current service-state behavior; it does not close every device/session/WKI
authorization requirement or resolve the earlier9FZ unknown handoff caller.
Continue the domain audit. Native no-op device opens and PTY slave lifecycle
checks do not re-read credentials. `/dev/tty` instead selects current job/terminal
identity; inspect explicit-task devfs callers (including unpublished spawn and
remote paths) before deciding whether that actor handoff is adequate. Keep
Goal25 transport scope separate. Only ordinary OS functional tests were run;
`--no-setup` authorization persists.

### Goal19 9GN — explicit process authority for device opens (2026-09-13)

Task-aware VFS opens now pass their retained opener through devfs and the
credential-aware CharDeviceOps callback. `/dev/tty` reads that opener's one job
snapshot and validates its terminal/SID/attachment generation; it no longer
substitutes the executing task. This matters for unpublished spawn children
whose inherited terminal differs from their parent's later state. A missing
opener supplies no terminal authority. Credential/path-only explicit reads and
resolved backing reads do not borrow a worker's terminal. PTMX and journal
callbacks retain their original credential policy and ignore the new actor
argument. No user/wire/JournalRecord ABI or lock ordering changes.

Actual `vfs_openat` KTEST uses unregistered process fixtures, opens the child's
exact inherited slave device, checks FD and endpoint cleanup, and refuses
absent/stale leases. A parent's renewed lease cannot replace the child's old
lease; conditional stale clearing touches only the child. After explicit child
refresh, open succeeds again. Credential/path-only read refuses with unchanged
output. Existing PTY/session/journal cases remain.

Normal Build WOS and isolated diagnostic builds pass, as do formatting/tidy,
PTY/journal/credential source gates, policy audit and three focused host models.
The first suite completed52,251passed/2failed: the new test compared the
per-endpoint identity API's Device* with its containing PtyPair*. Inspection
confirmed the API returns Device*. Correcting the expected pointer to the exact
`slave_dev` preserved the assertion's purpose; production code was unchanged.
That kernel/source/log/diagnosis and verified manual-stop cleanup remain in
`first-run/`; session25553exit0 after manual stop is not passing evidence.

The final new case passes at36.156s. Full isolated runtime reports **52,253
passed, 0 failed** at162.847s and automatic poweroff162.848s; session68919exit0,
VM3185275/launcher3117183 absent. First-run VM3113609/launcher3044916 are also
absent. Normal hash
`63de3a5529c9fcb54757862b949e00bdecc14e3e4e946b229ad86f49332239bb`;
final isolated
`003c621e3dab16f9b387eb6ad145ccc0953e0cbe285fd413224545b56133efe4`.
Artifacts: `build/goal19-device-open-actor/` includes contract, review, scoped
diff, both runs and current validation. The9GM two-node service evidence is
historical; no new Normal/multi-node runtime is claimed for9GN.

Full Goal19 remains active, **8 enforced /13 pending**. Continue the device
domain completion audit: main-thread `pthread_exit` retains the canonical owner
and cleanup stack until the last worker, but existing native owner-lifetime
checks do not assert controlling-terminal access or final-worker revocation.
That is the next bounded runtime qualification. Preserve9FZ unknown caller
evidence and separate Goal25 transport scope. Only ordinary OS functional tests
were run with the authorized `--no-setup` workflow.

### Goal19 9GO — terminal worker lifetime and device/debug qualification (2026-09-13)

`service_terminal_lifetime.c` now checks the main-thread retirement boundary:
a worker joins the retired main thread, opens `/dev/tty`, verifies its process
identity and foreground group, then waits for an explicit exit gate. Final-worker
exit must revoke the terminal, deliver HUP/CONT to the owned stopped foreground,
refuse its stale alias and permit a new session claim. Waits are bounded and
worker state does not reside on the retired main stack. All four cases pass as
root and UID104 on the current Normal kernel.

`service_device_authority.cpp` adds 24 native assertions across ordinary root,
root with an empty authority ceiling, UID100, UID101 with the journal grant,
UID102 with the network grant and UID104. Actual DEBUG selector1 and journal
open decisions match policy; real/effective/saved IDs, groups, ceiling and grants
remain unchanged. No fixture invokes DEBUG's disable-interrupt selector or reads
journal records. The initial helper compile required adding `<signal.h>`;
the corrected binary compiled before execution.

The finite device audit covers all current `CharDeviceOps` definitions. Existing
public null/zero/random/console data endpoints retain their policy; network,
WKI resource and node status callbacks have no write/control operation. PTY
session control is bound to the canonical process and nonwrapping attachment
generation. Revoked copied leases cannot regain authority even if published
after a revocation scan. Legacy WKI session mutations remain fail-closed at both
ends; implementing a new mapped-control protocol is not a completion requirement.
Journal authority cannot be exported as a tty-like File. WKI management and
VFS traversal/metadata remain tracked by their own manifest rows.

The `devices-debug` row is now **enforced**: **9 enforced /12 pending** overall.
This supersedes its older pending notes, including main-thread lifetime and
unspecified broader device/debug work. It does not claim every POSIX notification
interleaving is tested. Detailed callback/source review is in
`build/goal19-terminal-worker-lifetime/review.txt`.

Normal Build WOS passed (no production rebuild needed), both native fixtures
compiled, formatting and policy/PTY source gates passed. The live build ID
matched; final task inventory contains only init, declared services and netd's
worker. Helpers were removed and the guest synced before the owned VM3197149 and
launcher3197016 were stopped. No new isolated KTEST or multi-node run is claimed;
9GN's52253/0 diagnostic result is historical. Production code, syscall/wire ABI
and lock order are unchanged. Full Goal19 remains active;9FZ's handoff caller
uncertainty remains unresolved. Evidence: `build/goal19-terminal-worker-lifetime`.

### Goal19 9GP — init/account startup admission (2026-09-13)

The finite review confirms that init reads bounded, root-owned regular files
without following final symlinks and rejects writable configuration. Manifest
validation, account parsing and resolution of every declared identity precede
private state preparation and launch. In forked children, NNP/grants precede
groups, GID, UID, umask and ceiling; the final verification checks all saved IDs,
groups and restrictions. Any failure reports an identity error and exits127;
there is no fall-through to exec.

Six independent WOS boots with disposable rootfs overlays now qualify startup
rejection for unknown user, primary group and supplementary group, duplicate
UID, invalid umask and a writable account file. The supplementary-group case
belongs to disabled testd, demonstrating validation of all declarations.
Every case logs its expected failure after successful root pivot, launches no
service and retains only live PID1 in the matched-kernel GDB process inventory.
The normal rootfs account/configuration files were not changed by these fixtures.

The valid two-node control passes188 native credential/state/no-regain checks.
Both nodes have connected authenticated peers with policy4095. Four HTTPD
restarts and stop/start preserve its declared UID100 and independent session,
with PID1's descriptor set unchanged. An ordinary authenticated SSH connection
shows worker158 at UID/GID104 with NNP1, zero ceiling, empty groups and sealed
empty grants; root broker159 has the correct ancestry and its own SID/PGID.
The privileged listener/broker role remains declared and separate from the
network-input worker. Device/session lifetime is qualified by9GO; WKI identity
and resource controls remain distinct manifest obligations.

The `init-accounts` row is now **enforced**, making the current inventory
**10 enforced /11 pending**. Older generic pending notes about service lifecycle,
terminal races and forwarding do not create additional account/drop requirements.
This does not mark the remaining WKI/VFS domains complete. Existing keys suppressed
keygen in this control; current host tests cover its strict profile and state
adapter, while the earlier21identity/15state runtime checks remain historical.

Normal Build WOS passed; the current init_supervisor target and six focused host
checks passed, including parser bounds, unknown account handling, each of eight
drop-failure boundaries, private-state transactions and real-pipe session admission.
No production source, credential/syscall/wire ABI or lock order changed, and no
new isolated KTEST result is claimed. All six rejection VMs/launchers and the
control VMs3216096/3216086 with launcher3215937 were verified absent after cleanup;
control guests were synced and uploaded helpers removed. Final inventories
contained only declared processes.

Initial fixture-adapter errors were retained and corrected: guestfish requires
0644 rather than0o644; the SSH inspector needed native cat for dynamic procfs
and the documented `us` suffix on StartTime. No production policy was relaxed.
Full Goal19 stays active and9FZ's handoff observation remains unresolved.
Evidence and reproducible boot scripts: `build/goal19-init-account-admission`.
Only ordinary OS configuration, credential and lifecycle checks were run.

### Goal19 9GQ — admitted mount credentials (2026-09-13)

Public mount now carries the original Task and immutable Snapshot into initial
retained-target validation and both local and synchronous WKI publication. The
internal consume helper uses explicit request-context validation; it no longer
substitutes the ambient task's current credentials. Task identity, root/cwd hash,
namespace/mount generations, retirement and backend validation still apply.
Preparation, WKI waits and teardown remain outside the namespace publication
mutex. Mount flags and syscall/wire ABI are unchanged.

The new production-helper KTEST changes a private task's live credentials after
admission. The original authorized snapshot still succeeds; wrong task, wrong
snapshot and changed cwd return EAGAIN. A snapshot without MOUNT remains denied
with EPERM after the live task regains authority. Refusals preserve mount and
namespace generations; cancellation releases the prepared backend/reservation,
and retry publishes the exact nosuid mount. The private fixture cleans up only
its own directory and mount.

Normal Build WOS and the isolated diagnostic build passed. The full KTEST run
passed52254/0 at163.422 seconds and powered off at163.423; the new case passed
at2.111. Three existing credential/WKI host models, both focused source gates,
the policy audit and formatting checks passed. Tidy exited0 with771 diagnostics,
including a new single-line-if braces warning; this is not a warning-free result.
The native fixture needed an explicit -lunwind link, then compiled successfully.

The current Normal kernel passed72 local and72 remote nosuid/set-ID/NNP exec
cases across six execution methods, plus four mount-reporting checks. Both live
kernel build IDs matched before and after testing, with connected authenticated
peers at policy4095. Final inventories contained only init and declared services
(including netd's worker). Fixtures were removed, guests synced, and all five
owned VM/launcher processes verified absent. The cluster launcher exited0;
KTEST's terminal handle was unavailable after compaction, so its successful
serial summary, poweroff and process absence are the completion evidence.

The mount row remains pending: nonnative target bindings and task-root-aware
preparation still need review against the retained target. The current inventory
remains10 enforced/11 pending. This evidence does not resolve9FZ's handoff
observation or complete Goal19. Only ordinary OS functional checks were run.
Evidence: `build/goal19-mount-admitted-snapshot`.

### Goal19 9GR — retained physical mount destination (2026-09-13)

Public mount preparation now copies the admitted LookupHandle's physical path
for both local and synchronous WKI publication. It no longer strips a task root
and then applies a later ambient root observation. The publisher also compares
the prepared path to the admitted target and returns EAGAIN on mismatch before
mutating the mount table. Internal callers retain their default task-visible
path resolution. Original Task/Snapshot, root/cwd, namespace/mount-generation,
retirement, pivot epoch and reservation checks remain in place. No syscall/wire
ABI, mount flags, production allocation or lock ordering changed.

The new actual-helper KTEST uses an unpublished private task with its own root.
It resolves /child to the physical target, rejects a relative retained path,
refuses a prepared row for another destination with generations unchanged,
cancels and re-prepares that destination to prove reservation release, rejects
a changed request root, then publishes the exact nosuid mount. Cleanup owns all
partial directories and either possible fixture mount. The ambient task's root
is never changed. The new case passed at2.077 seconds; the full diagnostic run
passed52255/0 at162.819, powered off at162.820, and its launcher exited0.

Normal Build WOS, isolated diagnostic build, four host checks (three existing
compiled credential/WKI models and current mount lifetime source checks), the
credential/lookup source gates, policy audit and formatting checks passed.
Tidy exited0 with791 diagnostics; none pointed to this increment's changed
lines. The native nosuid fixture binary was reused from9GQ, not rebuilt.

The current Normal kernel passed72 local and72 remote set-ID/NNP/nosuid exec
cases across six execution methods, plus four mount-reporting checks. Both live
build IDs matched before and after tests, with connected authenticated peers at
policy4095. Final inventories contained only init and declared services,
including netd's worker; fixture runners reported complete cleanup.

This resolves the earlier task-root-aware preparation item. Nonnative retained
target metadata and namespace membership still require a finite review: devfs
has distinct dynamic namespace locks, and procfs metadata intentionally outlives
target exit. Merely replacing the fallback stat call cannot prove that binding.
The mount domain remains pending, with10 enforced/11 pending overall. Full
Goal19 stays active;9FZ's handoff observation remains unresolved. Only ordinary
OS functional tests were run. Evidence: `build/goal19-mount-physical-target`.

Both Normal guests were synced and the temporary sync helper removed. Cluster
launcher3393935 exited0 (session18758), and all five owned VM/launcher processes were verified
absent. No task-owned VM remains running.

### Goal19 9GS — selected procfs mount directory (2026-09-13)

MOUNT_TARGET now retains the procfs metadata File selected by both acquisition
paths. Final validation checks MOUNT on the admitted Snapshot, then reads that
File's immutable directory flag. A missing binding returns EAGAIN and a regular
file returns ENOTDIR. It no longer allocates, reopens an ambient PID path, reads
a different target or closes a temporary File under namespace publication.
Existing retained public metadata lifetime is preserved; this check grants no
access to private procfs content and adds no ACTIVE-task restriction.

The new actual-helper KTEST follows /proc/self for an unpublished private
process, verifies both retained target pins and absent lazy content, tests
regular-file ENOTDIR and restricted-snapshot EPERM before and after preparation,
and proves unchanged generations on refusal. Successful publication selects the
exact nosuid target, and both target references return to their initial count.
The case passed at2.154 seconds. Full KTEST passed52256/0 at162.703, powered off
at162.704 and its launcher exited0.

Normal Build WOS, isolated diagnostic build, new native fixture compilation,
four focused host checks, credential/lookup source gates, policy audit and
formatting checks passed. Tidy exited0 with773 diagnostics, including two style
warnings on the new validator. No warning-free build is claimed.

The current Normal kernel passed20 native checks across four owned children:
normal root, UID/GID104 with NNP, root with zero authority ceiling, and root
restricted to MOUNT. Authorized profiles mount their own procfs directory via
self, read the same marker through the numeric PID path, report nosuid through
path/File APIs, and unmount to restore procfs. Denied profiles preserve target
metadata; all profiles preserve real/effective/saved IDs, groups, NNP, grants and
ceiling. The fixture and runner complete cleanup. Live kernel build IDs matched
before and after testing; final inventory contains only init, declared services
and netd's worker. No new multi-node result is claimed for this procfs change.

Task/Snapshot/root/cwd/generation/pivot and exact physical-path checks from9GQ
and9GR remain, with no new production allocation, lock, syscall or wire change.
Procfs retained-target validation is now qualified. Devfs namespace binding and
FAT final metadata still need their finite review, so mount remains pending and
the inventory stays10 enforced/11 pending. Source reconnaissance confirms that
/dev/nodes removal unlinks entries without freeing their storage; namespace
synchronization still needs review, but immediate-free claims would be wrong.
Full Goal19 remains active and9FZ's handoff observation unresolved. Only ordinary
OS functional tests were run. Evidence: `build/goal19-mount-procfs-binding`.

The Normal guest was synced and its uploaded fixture removed. Launcher3481041
exited0 (session75859); all four owned VM/launcher processes were verified absent.

### Goal19 9GT — final FAT mount metadata (2026-09-13)

Final FAT32 mount validation now reads metadata through the admitted MountRef
and its FAT context instead of the generic cached path stat. Central MOUNT
authorization still runs first against the admitted credential snapshot.
Existing Task/root/cwd/mount/namespace generation checks, physical target
binding, mount flags and publication ordering remain. No syscall or wire ABI
changes. Direct FAT validation can perform additional reads and temporary
cluster allocation compared with a cache hit; the existing FAT context mutex
and block-read path already ran on cache misses.

The new actual-helper KTEST mounts a private RAM FAT volume, retains target
lookups for root and a zero-ceiling snapshot, and seeds an observable stale
directory cache entry. Changed regular/missing/unreadable backend metadata
produces ENOTDIR/ENOENT/EIO from the final prepared consumer. Authority denial
returns EPERM without a device read. Refusals preserve mount/namespace
generations and never write the volume. Restoring the directory publishes the
exact nosuid target with one namespace generation increment. Cleanup covers
both expected and unexpectedly successful publication.

The first suite reported52256 passed/1 failed because the fixture seeded the
wrong normalized cache key. The second attempt passed the direct final check
but stopped at an inappropriate public-wrapper assertion: this KTEST phase
precedes scheduler task readiness, so the wrapper returns ESRCH. That attempt
was stopped before suite completion. Corrected tests use the actual prepared
mount consumer with an explicit private Task/Snapshot, preserving production
wrapper behavior. Both attempts and their corrections are archived. No new
native public-syscall or multi-node run is claimed for this increment.

The corrected case passed at2.165 seconds. Full isolated KTEST passed52257/0
at165.166, powered off at165.167 and launcher3686800 exited0 (session11331).
Normal Build WOS and the isolated diagnostic build passed; four focused host
checks (three existing compiled models and the mount lifetime source check),
credential/lookup source gates, policy audit and formatting passed. Final tidy
exited0 with773 diagnostics, none on this increment's changed lines. Both
failed attempts were manually stopped; their launcher exit0 is not a passing
suite result. All six owned VM/launcher processes were verified absent.

Devfs namespace binding remains the next finite mount item. Its PTS and WKI
locks and generic child-vector readers/writers must be coordinated; retaining
a borrowed pointer alone is insufficient. /dev/nodes removal unlinks entries
without freeing their storage. The source/context map is in devfs-recon.txt.
Mount remains pending and the manifest stays10 enforced/11 pending overall.
Full Goal19 remains active;9FZ's handoff observation remains unresolved. Only
ordinary OS functional tests were run. Evidence:
`build/goal19-mount-fat-metadata`.

### Goal19 9GU — peer devfs namespace transaction (2026-09-13)

The /dev/nodes peer hierarchy now has a scoped namespace mutex shared by
peer add/remove, matching path lookup and metadata consumers, open selection,
directory reads, and matching generic kernel writer APIs. Peer creation uses
an internal already-locked helper, avoiding recursive public-writer locking.
This serializes stale-binding validation and metadata commit with peer alias
replacement. Open captures the selected type and borrowed Device under its
metadata/authorization guard and uses that endpoint after dropping the nodes
and WKI namespace guards. PTS still holds its own mutex through endpoint open;
no Device callback or remote ACK wait runs under the new nodes mutex.

The lock is deliberately scoped: journal registration can add /journal nodes
while holding an IRQ-safe journal lock, so a broad sleeping tree lock would
be unsafe. Existing /dev/nodes removal only unlinks entries; it does not free
nodes or Device contexts. This preserves existing opened-identity lifetime
and does not introduce new ownership or reclamation semantics. Generic devfs
pinning, root child-vector synchronization and final mount binding remain
pending. No syscall/wire ABI, mount flags, UID/GID or authority policy changed.

Two actual-helper KTEST cases pass: denied chmod/chown leave metadata unchanged,
removed/replaced aliases reject stale metadata requests with EAGAIN, retained
opened identity remains distinct from the replacement, the unlinked directory
becomes empty, and an open callback reenters the same namespace while its
admitted File retains its selected endpoint. Cases passed at1.919 and1.923.
Full isolated diagnostics passed52290/0 at163.431, powered off at163.432,
and launcher exited0. Normal Build WOS, isolated build, native fixture compile,
five focused host checks (three existing compiled models plus devfs/mount
source checks), policy/credential gates and formatting passed. Tidy exited0
without emitted diagnostics for this increment's production devfs source.

Both Normal VM kernels matched the built ELF before testing, with connected
active peer policy4095. An initial remote poweroff exec was denied127/EACCESS;
that result is archived and enforcement was preserved. Its timed observer
retired and the next inventory contained only declared tasks before retry.
The final runner synced the peer through the unchanged UID/GID100 sync helper,
then issued QMP quit to the exact owned peer PID/startticks/overlay. This was
a synced host-side VM stop, not a claim of guest-graceful shutdown. The native
observer passed after1019 namespace scans: current peer path ENOENT, retained
identity0x0004 and metadata stable, retained directory empty. Both uploaded
helpers were removed. The remaining kernel matched afterward, peer state was
FENCED with inactive policy0, and final tasks were only init, declared services
and netd's worker. The local guest was synced and launcher exited0. All five
owned VM/launcher processes were verified absent; the exited peer was first
a zombie until the launcher reaped it, not a still-running VM.

Source correction:9GT reconnaissance said child-array growth frees the previous
array. Current ensure_children_capacity actually retains the old allocation;
that note was stale. Concurrent slot/count changes still require serialization.
Mount remains pending, with10 enforced/11 pending overall. Full Goal19 remains
active and9FZ's handoff observation remains unresolved. Only ordinary OS
functional tests were performed. Evidence: `build/goal19-devfs-peer-namespace`.

### Goal19 9GV — journal module publication context (2026-09-13)

Journal emission and output-route configuration now queue module registration
under the journal IRQ-safe lock. A single publisher selects work under that
lock, then releases it before device registration and devfs publication.
Bootstrap drains remain synchronous outside the journal lock; a dedicated
kernel worker handles later modules with a 100ms polling interval under normal
scheduling. Records remain immediately available; newly encountered module
device aliases appear asynchronously. Successful aliases are published once,
so subsequent messages do not reset their permissions. Failed publication is
left pending for retry. Fixed module storage and endpoint ownership are unchanged.

The journal record ABI, sequence/filter/output behavior, admitted open Snapshot,
and dynamic read/poll authorization are preserved. Worker startup follows init
task creation, preserving PID1. This removes the journal-to-devfs allocation
and namespace-lock edge while holding the journal lock. It does not establish
generic devfs lifetime, root child-tree synchronization, device-registry writer
serialization, or final mount binding; those remain separate pending work.

The new actual-helper KTEST verifies immediate records before publication,
exactly one registered alias, and preserved authorized metadata after later
emission. It passed at2.087; full isolated diagnostics passed52291/0 at163.407,
powered off at163.408 and the launcher exited0. Normal Build WOS, isolated
build, native compile, five focused host checks (three existing compiled
models plus devfs/mount source checks), policy/credential gates, Python syntax
and formatting passed. Tidy exited0 with13 diagnostics, six on changed lines,
all six requesting braces; these style warnings remain recorded.

The current Normal single-node native fixture passed four credential profiles:
root with discarded ceiling and ungranted UID1000 cannot read the module alias;
UID101 with JOURNAL_READ and normal root can read its actual608-byte record.
Write-only opens remain public and credentials remain unchanged. The new alias
appeared after two20ms polling attempts. Later emission preserved inode identity,
owner/group and mode0600, then the fixture restored0666 and removed its upload.
Live build IDs matched before/after; journal_publish was present and final
processes were only init, declared services and netd's worker. The guest was
synced, its launcher exited0, and all four owned VM/launcher processes were
verified absent. No new multi-node result is claimed for this increment.

Mount remains pending and the inventory stays10 enforced/11 pending. Full
Goal19 remains active;9FZ's handoff observation is unresolved. The prior user VM
build conflict was resolved when the user stopped those VMs; no external blocker
remains. Only ordinary OS functional tests were performed, using --no-setup.
Evidence: `build/goal19-journal-registration-context`.

### Goal19 9GW — shared devfs root publication (2026-09-13)

Devfs root child lookup, insertion, removal and directory enumeration now share
an IRQ-save guard across otherwise independent journal, PTS, peer and WKI
namespace paths. Root growth allocates a spare outside the guard, then rechecks
the current array before copying or appending. If another publisher grew the
array meanwhile, its storage and entries remain authoritative. Retired or unused
root arrays are freed after unlocking. Non-root child arrays retain their
existing behavior. Directory type is copied under node metadata protection using
the cached device kind, without dereferencing a borrowed Device pointer.

The new guard follows existing namespace guards and precedes metadata locks.
It does not cover allocation, deallocation, logging, Device callbacks or remote
waits. Node/Device ownership, WKI/PTS references, credential decisions, errno,
syscall and wire ABI are unchanged. This fixes root array races; generic node
pins, complete pathname transactions, duplicate generic-name publication and
final devfs mount binding remain pending. A borrowed result from find_child
still requires its namespace/lifetime protection after the root guard ends.

The actual-root diagnostic test fills the current capacity with owned nodes,
selects a spare, lets another insertion win growth, then commits the original
insertion. It verifies preserved winning storage, both exact identities and
one directory entry each, then removes its private nodes. This is a deterministic
interleaving, not concurrent CPU stress. The case passed at 1.923 seconds;
full isolated diagnostics passed 52,292/0 at 162.394, powered off at 162.395,
and the launcher exited 0. Normal Build WOS and the isolated build passed.
Formatting/tidy passed with no emitted tidy diagnostics. Seven focused host
checks passed: three existing compiled models, three source checks and an
existing local parser smoke test. Policy and credential source gates passed.

The current Normal single-node kernel matched the ELF before and after the
unchanged journal publication fixture. All four credential profiles passed;
the alias appeared after four 20ms polling attempts, later emission preserved
inode/permissions, and credentials remained unchanged. The fixture restored
permissions and removed its upload. The reused native binary and unchanged
fixture sources are hashed; no new native compilation or multi-node result is
claimed. The publisher worker was present and final processes were only init,
declared services and netd's worker. The guest was synced, its launcher exited
0, and all four owned VM/launcher processes were verified absent.

Mount remains pending; the inventory stays 10 enforced/11 pending. Full Goal19
remains active, with no external blocker. The 9FZ handoff observation remains
unresolved. The next devfs lifetime and final binding map is recorded in
next-recon.txt. Only ordinary OS functional checks were performed; VM scripts
used --no-setup. Evidence: `build/goal19-devfs-root-publication`.

### Goal19 9GX — generic devfs metadata lifetime (2026-09-13)

Generic devfs paths now use a namespace mutex through path selection and
metadata-reference acquisition, retained identity validation and metadata
commit, directory reads, and generic writers. Acquired paths and admitted
opens retain local node metadata. Unlink drops the namespace reference; node
and current child-array storage are deleted only after the last retained path
or File releases its reference. Move/reset and failed-open cleanup transfer
or release that ownership. Borrowed Device ownership remains with its provider.

The generic mutex is excluded from PTS, WKI and /nodes paths, which retain their
existing namespace guards. /nodes acquired/open wrappers use local metadata
references, while peer retirement preserves its historical unlink-only storage
behavior. PTS and WKI counters are unchanged. Generic open releases its guard
before Device callbacks; PTS still holds its guard through endpoint admission.
Partition population drops its guard before calling the public symlink writer;
network population runs under the generic guard. Journal registration uses the
9GV task-context publisher. The 9GW shared root-array guard remains in place.

Removal now refuses every node with children, including DEVICE namespaces such
as journal/modules, preventing children from retaining a freed parent. This is
an intentional kernel-helper result change for populated device nodes. User
syscall errno, UID/GID and saved-ID behavior, authorization policy, journal
records and wire ABI are unchanged. Repeated leading separators select the
same PTS/WKI/nodes guard and reference kind as the path walker.

Three new actual-helper cases pass: retained directory metadata and open identity
survive unlink/name replacement, stale chmod returns EAGAIN without changing
either node, unauthorized chmod preserves metadata, and reference counts track
open/close/move/reset correctly; a failing Device open callback reenters generic
devfs and releases its pin; a device with children cannot be removed. Cases
passed at 1.986, 1.989 and 1.993 seconds. The PTS retirement case also verifies
///pts selects its PTS pin and passed at 1.996. Full isolated diagnostics passed
52,337/0 at 162.538, powered off at 162.539, and the launcher exited 0.

Normal Build WOS and the isolated build passed. Five focused host checks
(three existing compiled models and two current source checks), credential
source/policy gates and formatting passed. Tidy exited 0 without emitted
diagnostics. Header initializer formatting was corrected before builds.
The current Normal single-node kernel matched its ELF before and after the
unchanged journal publication fixture: all four credential profiles passed,
the alias appeared after four 20ms polls, later emission preserved inode and
permissions, and credentials were unchanged. The fixture restored permissions
and removed its upload. Reused native binary/source hashes are archived; no new
native compilation or multi-node result is claimed. Final processes were only
init, declared services and netd's worker, with journal_publish present. The
guest was synced, its launcher exited 0, and all four owned processes are absent.

Final devfs MOUNT_TARGET binding remains pending. The next audit also records
preexisting generic-writer access to the WKI namespace without its dedicated
ownership/locking contract, and generic block-device identity/provider lifetime.
Those are not solved by metadata pins. Inventory remains 10 enforced/11 pending;
full Goal19 stays active, with no external blocker and the 9FZ handoff observation
unresolved. Only ordinary OS functional checks were performed, using --no-setup.
Evidence: `build/goal19-devfs-local-lifetime`.

### Goal19 9GY — retained devfs mount publication (2026-09-13)

Both devfs MOUNT_TARGET acquisition paths now retain the selected DevFSNodeRef.
The final mount consumer checks central MOUNT with its admitted Snapshot, then
validates the exact node and directory kind under the matching devfs namespace
guard. That guard remains held through prepared mount publication. Root-array
and metadata spin guards end before the callback, which may grow the mount
table but performs no Device I/O or remote wait. Existing Task/Snapshot, root/cwd,
mount/namespace generation and physical-target checks remain in force.
Missing/stale bindings return EAGAIN, non-directories ENOTDIR, and discarded
mount authority EPERM before backend consumption. Other filesystem branches,
UID/GID/saved-ID behavior, syscall/wire/record ABI and Device ownership are unchanged.

Generic directory/symlink/device/remove helpers now reject the WKI subtree
with null/false, including repeated leading separators. Dedicated WKI resource
APIs remain its mutation path; current production generic callers use boot,
journal, PTS and nodes paths. Bulk WKI population also guards initial directory
creation and its diagnostic count snapshot, releasing that guard before callbacks
enter per-resource writers. The kernel-helper WKI refusal is an intentional,
documented API boundary; peer authority and wire behavior are unchanged.

Actual prepared-mount KTEST verifies admitted-directory retention, authority
denial, in-place type conversion to ENOTDIR, removed/replaced alias EAGAIN,
unchanged generations on refusal, and exact nosuid publication from a fresh
binding. A second case verifies generic/WKI namespace guards remain held during
the callback while metadata/root locks are free, and generic WKI mutation
refusals preserve its actual root binding and metadata. It exercises the public
bulk population entrypoint. Final cases passed at 2.140 and 2.143 seconds.
Full final diagnostics passed 52,339/0 at 164.059, powered off at 164.060, and
the launcher exited 0. An earlier kernel also passed 52,339/0 at 163.942 before
the review added the bulk-population guard; that run is archived separately
under pre-population and is not substituted for final-source evidence.

Final Normal Build WOS and isolated builds passed, as did six focused host
checks (three existing compiled models and three source checks), credential
policy/source gates, native compilation, Python syntax and formatting. The
source gate's exact old intent-list expectation was updated for MOUNT_TARGET.
Tidy completed with 774 diagnostics, one on a changed line requesting a
designated initializer; that style warning remains recorded.

The current Normal single-node fixture passed 20 native checks across normal
root, UID/GID104 with NNP, root ceiling0, and root MOUNT-only ceiling1024. It
preflighted a plain devfs directory at /dev/disk/by-partuuid, verified permitted
and denied mounts plus non-directory refusal, marker I/O, path/File nosuid,
restored inode/metadata and unchanged credentials/groups/grants/ceiling/NNP.
Only its disposable guest was used; the fixture removed its marker, mounts and
uploaded helper. Live build IDs matched before/after, and final processes were
only init, declared services and netd's worker. The guest was synced, its launcher
exited 0, and all six owned VM/launcher processes were verified absent on the
host. No new multi-node or concurrent CPU stress result is claimed.

The devfs final target-binding gap is closed. The mount domain remains pending
for generic block-source provider identity/lifetime and the automatic mount,
device lifecycle and forwarding audit. Inventory stays 10 enforced/11 pending;
full Goal19 remains active, with no external blocker and 9FZ's handoff observation
unresolved. Only ordinary OS functional checks were performed, using --no-setup.
Evidence: `build/goal19-mount-devfs-binding`.


### Goal19 9GZ — mount source and provider audit (2026-09-13)

Source reconnaissance is recorded in `docs/mount_source_authorization_audit.md`
and `build/goal19-mount-source-audit/source-hashes.json`. It traced public mount
source selection through devfs, both device registries, filesystem preparation,
and actual AHCI/partition/WKI registration and retirement callers.

Published WKI BlockDevice storage is permanent, and current boot providers are
not reclaimed at runtime. The separately deleted generic Device wrapper is not
a demonstrated production devfs use-after-free: devfs boot enumeration precedes
WKI and WKI discovery uses its own pinned backing. This corrects the tentative
provider-lifetime concern in 9GY's next-recon note; it does not make raw registry
selection safe against concurrent publication.

The concrete remaining source gaps are ambient Task/Snapshot recapture during
source traversal, global same-name provider reselection after pathname lookup,
WKI existing-provider reuse without matching the selected resource identity,
and unsynchronized block/Device registry access. The audit maps locks and
caller contexts, preserves PARTUUID semantics and boot resource-index identity,
and specifies actual source-binding/registry tests for implementation. A blanket
provider-refcount retrofit or sleeping registry mutex under proxy locks is not
justified by the current call flow.

This turn changed documentation/evidence only. No kernel build, VM, native test,
packet injection or penetration test was run. Manifest remains 10 enforced and
11 pending; mount and the full Goal19 remain incomplete. No external blocker.
Live execution remains authorized through --no-setup when needed.


### Goal19 9HA — Device registry publication (2026-09-13)

The actual device.cpp host regression reproduced a baseline ASan heap-use-after-
free in SmallVec::grow during concurrent dev_register. The shared generic Device
registry now uses preallocated linked entries and one IRQ-save guard for
publication, lookup, count/index access and unlink. Entry allocation precedes the
guard; deletion, logging and accounting follow it. Device objects are not owned
or reclaimed by this registry. Registration order, first-match removal/lookup,
duplicate names/numbers/pointers, null errors and Device/syscall/wire layouts
remain unchanged. Count/index calls are individually coherent, not a snapshot.

This closes the Device-registry dependency identified in 9GZ. Registration now
allocates one small entry instead of using inline vector capacity; index lookup
is linear (the current external consumer enumerates at devfs boot). Those costs
avoid allocation and callbacks under the registry guard. Raw returned pointers
still require independent lifetime protection. The block registry, retained
mount-source Snapshot/path/provider binding and existing WKI provider identity
comparison remain pending; no blanket provider-refcount scheme was introduced.

Three host GTests compile actual device.cpp with ASan/UBSan and a nonrecursive
host lock. They pass ordering/duplicate/borrowed-storage checks, 9,216 concurrent
registration/removal cycles with lookups, reentrant accounting, and allocation
refusal with unchanged state. The allocation hook also checks registry lock
absence. Normal Build WOS and the isolated diagnostic build passed. Six related
host checks and credential policy/source gates passed. The separately run global
host manifest check fails on the existing untracked kcov_runtime_test.cpp using
standalone main rather than GTest; that unrelated source was left untouched.
Formatting passed. Tidy exited0 with two new nonfatal warnings: RegistryEntry's
member default initializer and RegistryGuard's flags_ naming.

The current isolated kernel booted using --no-setup, passed actual journal
publication cases (JournalRecordsPrecedeDeferredDevicePublication at2.474), and
finished 52,339/0 at164.773 with automatic poweroff at164.773. Its launcher exited0;
owned launcher190171 and VM258758 were host-verified absent. Current Normal and
isolated kernel hashes and validated source hashes match the recorded inputs.
No new Normal userspace or multi-node runtime result is claimed. No packet
injection or penetration testing was performed. Unrelated host VMs were untouched.

Full Goal19 remains active, 10 enforced/11 pending, with no external blocker.
The unresolved 9FZ handoff observation is unchanged. Evidence, baseline failure,
patch contract and scoped patch: build/goal19-device-registry-publication.


### Goal19 9HB — block registry publication (2026-09-13)

The actual block_device.cpp host concurrency regression reproduced a baseline
ASan double-free in SmallVec growth. (An initial commentary called this UAF; the
subsequent update corrected it to the recorded double-free.) The registry now
preallocates an entry containing its generic Device wrapper and reserves a
unique name/pointer under an IRQ-save guard. PREPARING and RETIRING entries are
hidden from block lookup, enumeration and PARTUUID selection while their keys
remain reserved. Successful wrapper registration publishes the block entry;
failed registration removes its exact reservation. Unregister hides the block
provider before removing the wrapper and then releases the reservation.

No block guard spans the Device registry, allocator/free, accounting/logging,
provider I/O, writer-lease lock or remote wait. Read-only snapshots/overlap
propagation share the registry guard. Boot enumeration uses guarded count/at
queries and keeps its existing boot-only sequencing assumption; index queries
are now linear. Name/provider collisions intentionally return -1 atomically;
duplicate major/minor values remain supported. Current provider/parent lifetime,
boot provider order/resource IDs, BlockDevice/Device layout, syscall/wire ABI
and authorization policy remain unchanged. Partition creation now propagates
allocation/registration failure and frees the unpublished object.

Seven host GTests compile actual block/device registries and GPT code, replacing
only external host environment and decimal formatting. ASan/UBSan tests pass
pending/retiring reservations, exact wrapper-OOM rollback with unrelated
insertion, 3,840 concurrent registration/lookup/read-only/removal cycles, eight
same-name competitors with one winner, ordering/PARTUUID/numeric aliases,
read-only and I/O callback reentry, writer leases and partition refusal. The
allocator hook rejects allocation under either nonrecursive host registry lock.
Seven related host targets, credential policy/source gates, final Normal Build
WOS and isolated build passed. Formatting/final tidy passed with zero warnings
in changed production files after correcting initial brace warnings. The global
host manifest check still fails on the existing standalone kcov_runtime_test.cpp
without a GTest case; that unrelated file was not changed.

After the user stopped the normal instances, host inventory confirmed no WOS VM
or normal-disk user remained; unrelated OPNsense was untouched. The owned isolated
VM launched using --no-setup, passed block I/O/writer-lease and filesystem tests,
and completed 52,339/0 at163.639, powering off163.640. Launcher exited0; host
verified launcher633692 and VM702402 absent. Current kernels and validated source
hashes are retained with the evidence. No new Normal userspace or multi-node
runtime result is claimed, and no packet injection or penetration test ran.

Device and block registry container dependencies are addressed. Original
Task/Snapshot mount-source traversal, retained path/provider binding and exact
WKI existing-provider resource identity remain incomplete. Full Goal19 stays
active, 10 enforced/11 pending, no external blocker; 9FZ's handoff observation
remains unresolved. Evidence and scoped patch: build/goal19-block-registry-publication.


## 9HC: exact existing WKI block-provider identity

Canonical devfs remote block reuse now requires the selected owner/resource,
observation generation and both incarnation fields to match the published,
active, unfenced routable proxy and its currently registered BlockDevice pointer.
Reset/cleanup/resume/detach transitions reject reuse. Zero legacy tokens compare
exactly rather than acting as wildcards. A mismatch returns null without an
attach request or provider I/O. Local-provider behavior and fresh attach are
unchanged. Successful attach now publishes ever_published under the proxy
registry lock shared by readers.

Lock order remains proxy registry -> state -> leaf block registry; the helper
adds no allocation, callback, peer admission, remote wait or I/O under those
locks. Existing I/O leases still revalidate peer/resource liveness. Published
provider storage, boot resource IDs, credential policy and syscall/wire ABI
are preserved. The kernel fixture uses a private registered synthetic provider,
checks exact/mismatched identity, every transition, unregistered/local/null
providers and zero tokens, then unindexes/unregisters before freeing its storage.
It issues no network requests. The source gate verifies devfs calls the helper
after dropping its node pin.

Normal Build WOS, isolated build, eight focused host/source targets, policy and
credential/devfs source gates, and final formatting passed. Tidy completed with
33 diagnostics, none on this increment's changed lines. Source and both kernel
hashes are retained in build/goal19-wki-block-reuse. The previously observed
global host manifest failure on standalone kcov_runtime_test.cpp is outside this
change; no new global-manifest success is claimed.

Current isolated VM launched with --no-setup after the user stopped the normal
instances. WkiDevProxyReuse/ExistingProviderMatchesSelectedObservation passed at
37.915; full KTEST completed 52,340 passed/0 failed at163.700 and powered off at
163.701. Launcher exited0. Host verification found owned launcher753664 and
VM825791 absent. Serial log, process identities and kernel ELF are archived.
No new Normal native or multi-node runtime result is claimed.

The canonical devfs fallback now compares exact existing-provider identity.
Core's earlier direct global-name selection, original Task/Snapshot source
traversal, retained source/backend binding and generic devfs provider capture
remain pending, along with the broader mount lifecycle/forwarding audit. Goal19
remains active, 10 enforced/11 pending, with no external blocker. The earlier
9FZ handoff observation remains unresolved. No penetration test or packet
injection ran. Evidence: build/goal19-wki-block-reuse.


## 9HD: admitted mount-source and provider binding

Public /dev source acquisition now uses the original Task/Snapshot and retains
a MOUNT_SOURCE LookupHandle. Final consumption validates request/root/cwd,
namespace and mount identity, authorizes MOUNT, and captures the actual devfs
provider under namespace/metadata guards before probing or attachment. Missing
sources return ENOENT, non-block/non-devfs sources ENOTBLK, stale context/node
bindings EAGAIN and trailing-directory mismatch ENOTDIR; remote local-consumption
fails EXDEV. Core no longer strips an ambient root and globally reselects a
name. PARTUUID and explicit wki:// selectors, target binding, flags, errno for
unchanged paths, UID/GID and syscall/wire ABI remain unchanged.

Generic devfs publication caches the actual permanent BlockDevice associated
with its owned registry wrapper. Leaf registry queries compare wrapper/provider
identity without dereferencing the caller's pointer. Retained source capture
uses this cached pointer under the matching devfs namespace/metadata guards;
wrapper removal or same-name provider replacement cannot redirect the source.
Current borrowed provider lifetime is preserved; this is not a new lifetime
contract for future reclaimable hardware. The shared devfs resolver (including
swap) uses the same guarded snapshot; WKI identity copy precedes unlock and
observation/incarnation/attach work follows unlock. No callback, allocation,
provider I/O or remote wait was added under these guards.

Actual registry host coverage adds provider/wrapper identity and publication/
retirement checks. Actual kernel MountSourceKeepsAdmittedContextAndProvider
passes at37.222: admitted credentials survive a live drop; wrong task/root/
Snapshot, missing source despite registry name, wrong backend/kind, trailing
slash, node upsert/removal, retired provider and same-name replacement behave as
specified; failures preserve the output and issue no device reads. Symlink
selection and successful exact provider capture also pass. Both builds, eight
focused host/source targets, policy/credential gates and formatting pass.
Initial source gates expected the removed name-resolution implementation; they
were updated to require the new binding/admission order and passed. Tidy exits0
with775 diagnostics, one changed-line include-cleaner warning for ENOTBLK even
though core includes cerrno; no clean-warning claim.

Isolated full KTEST passes52,341/0 at166.532, automatic poweroff166.533, launcher0.
Normal native source fixture compiles and executes against a matched live kernel
build ID, verified again afterward. It passes22/24 checks across ordinary root,
UID/GID104+NNP, root ceiling0 and root MOUNT-only1024. All source selection,
non-block/missing/trailing errors, MOUNT denials, denied-target preservation,
credential preservation and permitted boot reads/cleanup succeed. Two checks
remain FAIL: both allowed profiles request MS_RDONLY but statvfs flags are2
(nosuid only), despite successful reads and unmount. This is a real remaining
mount-policy gap, not a passing native qualification. The fixture retains these
failing assertions. No mutating guest file operation was issued on the mounted
boot filesystem; requested read-only mount enforcement cannot be claimed.

Initial runner assumed /etc/fstab survived pivot-root; it does not, so the runner
now accepts the boot alias from this image's packaging manifest. A private-root
userspace setup also returned chroot ENOSYS; the fixture now uses only available
WOS interfaces. Its root-context evidence is the actual kernel case, not a
native chroot claim. These initial results are archived. All uploaded helpers
and fixture paths were removed. Final task inspection showed only init and the
declared services/netd worker. Guest synced, normal launcher exited0 and host
verified all four owned processes842915/912993/917452/917351 absent. No multi-node
source lifecycle qualification or penetration/packet-injection test is claimed.

Next concrete gap: prepare_mount_filesystem derives read_only only from the
underlying device and ignores MS_RDONLY. FAT32 initialization and XFS VFS init
also derive their context mode only from the device. Repair must carry requested
read-only policy into backend initialization before any recovery/cleanup writes,
respect writer-lease arbitration, preserve device-wide policy, and verify both
VFS denial/reporting and backend no-write behavior. Existing successful readonly
Device coverage is insufficient for a readonly mount of a writable device.
Automatic mount/device lifecycle, forwarding/quiescence and ordinary multi-node
source qualification remain pending. Goal19 stays active,10 enforced/11 pending;
9FZ remains unresolved and there is no external blocker. Evidence/scoped patch:
build/goal19-mount-source-binding.


## 9HE: requested read-only mount policy for block filesystems

9HD's native MS_RDONLY failure is corrected at mount preparation and backend
initialization. Requested read-only OR device-wide policy is stored before
filesystem initialization; FAT/XFS receive it explicitly while preserving their
own device-policy check. Writable mount writer leases are unchanged; readonly
mounts do not acquire a writer lease. No shared BlockDevice flag is changed.
FAT readonly sync/unmount skip device flush as well as FAT writes, and XFS
readonly sync skips inode/journal/device flushing. Dirty XFS readonly-log
rejection remains before recovery. Statvfs/fstatvfs includes retained mount
readonly policy, and init parses ordered ro/rw alongside suid/nosuid while
preserving backend tokens and transactional overflow refusal.

No new lock or lock order, UID/GID/syscall/wire layout, source/target admission
or published provider lifetime change. Existing default initializer arguments
preserve internal device-based policy. Initializer signatures are internal
kernel C++, rebuilt with all callers. The normal kernel is unchanged by the
later selftest-only correction.

Normal Build WOS, isolated builds, nine focused host/parser/source checks,
credential audit/gates and formatting passed. Tidy completed with1,236 existing
and current diagnostics, including two changed-line braces-style warnings;
no warning-free claim. Actual init parser tests cover ordered ro/rw/no-suid
composition, preservation of unknown backend tokens and unchanged output on
overflow. Production and test sources plus both kernel hashes are archived.

Actual prepared FAT KTEST uses writable private RAM media with counted write/
flush callbacks, an independent REMOTE_BINDING writer lease, cancellation,
publication, denied write-open, sync and unmount. It observes zero callbacks
and unchanged device policy. Actual XFS KTEST mounts a clean disposable scratch
filesystem through a private counted wrapper, retains requested readonly and
checks missing-path ENOENT, existing-file write-open EROFS, sync/unmount and
zero write/flush callbacks. The first run passed52,371/failed1 at171.238 and
entered the standard selftest-failure panic because its missing-path assertion
incorrectly expected EROFS. That result and ELF are archived; the corrected test
preserves backend errno and separately exercises an existing file. The failed
VM was explicitly stopped after the completed suite; launcher exited0.

Normal native fixture now passes all28 checks, including the24 original source
cases and four added retained-file readonly/write-open assertions. Both allowed
profiles (root and MOUNT-only root) report readonly+nosuid, read boot content,
reject writable open with EROFS and unmount cleanly; UID/GID104+NNP and zero-
ceiling profiles retain EPERM/unchanged target and all credentials are preserved.
The live kernel build ID matched before/after. Only init and declared services/
netd worker remained. All uploaded helpers and fixture paths were removed;
guest synced, normal launcher exited0 and owned1016429/1016330 were host-verified
absent. No fresh native private-root or multi-node claim.


Final isolated run: requested-readonly FAT case PASS36.665, corrected XFS case
PASS107.627, full52,374 passed/0 failed at169.559 and automatic poweroff169.560.
Final launcher exited0. Host verified all six owned processes940890/1011425
(initial KTEST),1019591/1089324(final KTEST),1016429/1016330(normal VM/launcher)
absent. Current source and kernel hashes are unchanged from qualification.
Artifacts, initial failing evidence, final serial log and scoped patch:
build/goal19-readonly-mount-policy.

This closes the reproduced block-filesystem requested-readonly initialization
and native reporting/write-open gap. Full mount enforcement remains pending:
shared VFS single/pair lookup consumers do not yet gate namespace mutations on
mount read_only, and tmpfs namespace operations lack this per-mount policy.
Next work must enforce those boundaries while preserving mounting over readonly
directories, stable request snapshots, paired operations and errno ordering.
Automatic mount/device lifecycle, remote forwarding/quiescence and ordinary
multi-node source qualification remain pending. No new multi-node or native
private-root result is claimed. Goal19 stays active,10 enforced/11 pending;
9FZ's earlier handoff observation is unresolved and no external blocker exists.
