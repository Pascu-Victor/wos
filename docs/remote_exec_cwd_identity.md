# Retained cwd identity across remote exec

Status: the original ordinary retained-cwd regression passes all18 two-node
TMPFS/XFS cases since9KG. Negotiated legacy-root return-owner transfer and native
parent-cover publication across boot pivot preserve the selected cwd through
rename. General native-source, owned-root and third-owner transfer, plus broader
readonly/nosuid and placement evidence, remain pending.
This is Goal19 requirement5 and the pending VFS traversal domain. The9LT
[exec authority audit](goal19_exec_authority_audit.md) qualifies supported exec
authority and explicit refusal of unsupported transfers; it does not implement
general root/cwd transfer. Declared routing is preserved, including LOCAL `/`
selecting the receiver root.

`tests/fixtures/service_exec_cwd_identity.py` runs the corresponding C++ fixture
on two WOS nodes. The source enters a directory on the execution owner, renames
it, and creates a replacement at its old name. The retained directory contains
`seed`; the replacement contains `fake`. Both executable images are provisioned,
and an explicit LOCAL VFS rule makes the destination of localized paths
unambiguous. The fixture uses ordinary filesystem, fork, pthread and exec calls.

The 9IA baseline has two passing remote exec controls before rename and eight
passing local exec controls after rename. All eight remote cases after rename
read `fake` instead of `seed`: root/UID100, main/worker, TMPFS/XFS. All observed
payload identities are correct. Cleanup completes on both backends. Evidence is
`build/goal19-retained-cwd-exec/identity-matrix-v2/`; this failure is not a claim
that UID mapping was bypassed or that a directory transfer fix exists.

## Current call and ownership flow

Since9KH, another remote exec from an inherited native cwd view returns
`ENOTSUP` until its retained boundary and policy can be transferred. Stale
root/cwd labels return `ESTALE`. Both submitters reject before binding a peer
or issuing a filesystem grant; credentials, anchors and admission output stay
unchanged. Ordinary unscoped native cwd routing and negotiated remote
owner-return transfer keep their current behavior. The isolated diagnostic
suite passes54217/0, including64 actual installed-view profiles; this stage
does not qualify general onward transfer or rerun the two-node matrix.

- Since9KC, both compute senders issue actual grants for a retained remote cwd
  returned to its native owner. The separately negotiated return-owner capability
  initially installed a global-root cwd context before loader reads and Task acceptance.
  Since9KF it also negotiates explicit legacy-root paths;9KG supplies native
  parent covers across boot pivot, enabling the normal TMPFS/XFS cwd matrix.
  Source and receiver owners survive waits; peer/session/boot/channel identity
  is revalidated and relative readonly/nosuid policy stays attached to the view.
  General native-source, owned-root and third-owner transfer remains pending.

- Since9JA, native `DirectoryView` factories and `PreparedFilesystemContext`
  can stage exact root/cwd identities on different proven TMPFS/XFS mounts.
  Each anchor uses its own backend, and captured cover references retire after
  publication unlocks. These preparations do not install Task roots or transfer
  a grant; native scoped lookup integration is described under9JG below.
- Since9JB, native ancestry preparation reuses one retained covered File and
  exact child mount per mount-table epoch. Eight edges fit inline; more use
  fallible blocks whose final releases occur outside publication. Repeated
  membership and search decisions remain fresh; storage no longer has a
  two-pass visit limit. Scoped typed consumers now use that same owner under9JG.
- Since9JC, `LookupHandle` can retain an immutable native membership owner
  through single/pair consumption. Consumers borrow recorded edges only and
  recheck current cwd search. The original root/cwd endpoint gate is superseded
  by9JG selected-directory/parent ancestry checks; cookie presence grants no
  membership. Explicit acquisition stages outer ownership.
- Since9JD, ordinary component walking validates public native mount entry
  and `..` against retained covered-directory objects. Known stale attachments
  return EAGAIN; missing generic/boot/post-pivot covers retain the existing
  unscoped compatibility path. Normal and serialized walkers keep capture owners
  outside publication, then publish a frozen proof with the lookup. Since9JG,
  scoped views share native crossing with strict refusal of missing covers.
- Since9JE, staged context preparation recovers both typed anchor paths with
  one outer native membership owner. The anchor must match the view's exact cwd
  inode and mount before independent root/cwd membership recovery. Name output
  publishes after epoch checks. Mixed-mount callers without an outer proof are
  refused before backend-private casts;9JF/9JG connect the supported ambient
  cwd/PROCFS queries and scoped walking to explicit outer ownership.
- Since9JF, public cwd queries and structured raw PROCFS reads can recover a
  mixed native cwd using an outer proof owner and a nested publication guard.
  Their supplied credential snapshot already requires outside-publication
  context. Null-snapshot text-only callers refuse a mixed anchor before retaining
  it. Task-context changes preserve outputs and retire captured references after
  the guard; production root installation remains pending.
- Since9JG, explicit views, native dirfds/cwd and serialized directory opens
  share a recorder across scope admission and native walking. Shared consumers
  prove the actual selected directory, or the parent of a regular/missing target,
  against the retained root using readonly recorded edges. Missing or stale
  native covers are refused; no Task-root installation or wire grant is added.
- Since9JH, process statvfs uses retained caller lookup and inherited view policy;
  protected/missing paths and mixed native cwd use the shared authorization path.
  Path and File statistics stage output. Remote admission reuses authenticated
  stat while retaining synthetic filesystem totals; no transfer grant is added.
- `vfs/core.cpp`: `change_directory_at` acquires a search-only directory and
  publishes a `DirectoryAnchorRef` together with root/cwd text under the
  canonical filesystem owner's `path_metadata_lock`. Relative lookup uses the
  retained backend/File; `directory_anchor_path` recovers its current name.
- `syscalls_impl/process/exec.cpp`: since9IB, `try_process_remote_exec` retains
  the owner's root/cwd/anchor together in `RemoteExecPlan::filesystem` through
  `Task::capture_filesystem_context`. Local exec preserves the anchor. The
  separate `wos_proc_exec` spawn path uses that same capture for child
  inheritance, then supplies the private child's cwd to placement. Its previous
  use of the parent's obsolete per-thread cwd is fixed. Since9IC the plan also supplies its captured context to both submission
  encoders. Both entrypoints supply the captured filesystem to compute admission,
  which carries supported return-owner cwd grants.
- `net/wki/remote_compute.cpp`: `build_submit_context_info` and
  `copy_submit_context_data` size and copy a cwd string. Since9IC both encoders
  select root/cwd from the supplied exec capture; the fixed identity encoder
  no longer rereads task-local root. Direct/legacy submissions without a supplied
  capture copy root from the canonical filesystem owner under its path lock,
  preserving the explicit cwd argument without acquiring a directory reference.
  `TaskSubmitPayload`
  carries its length. Legacy unanchored submissions still localize that text.
  Supported retained remote cwd instead uses the negotiated grant context and
  native receiver installation described under9KF/9KG.
- `net/wki/remote_vfs.cpp`: existing directory PINs retain search-only native
  Files in `g_remote_fds`. Rows are scoped to a captured `WkiChannelIdentity`
  and a purpose-specific `directory_pin_token`. CANCEL removes the registry
  reference; admitted references can survive until their own final release.
  Existing directory RPCs assume deferred server-binding admission.
- `platform/sched/task.*`: process threads resolve filesystem state through
  `filesystem_owner()`. An anchor's final release may take filesystem locks or
  wait for remote cancellation; it must occur outside path spinlocks and
  namespace publication. Thread-local copies of path text are not authoritative.

9IB adds the owned source-context prerequisite and fixes the separate worker
spawn context error. The unchanged raw-spawn matrix moves from12passing/4failing
cases to16passing cases across both backends. The original rename/remote-exec
defect still requires the transfer below; retaining a source reference in the
plan does not install that reference on the receiver.

## Client mount policy regression (9ID)

`service_exec_cwd_policy.cpp/.py` isolates the source-view restriction independently
of rename. With a canonical peer root mount and a separate writable owner view,
all32payloads retain the expected inode/content and six IDs. The24controls pass;
all8remote exec cases from a read-only cwd change owner data despite a successful
pre-exec EROFS check (TMPFS/XFS, root/UID100, main/worker). An owner-native anchor
without the source view's restrictions would still fail this requirement.
NOSUID/set-ID behavior is not qualified by this regression.

Evidence is `build/goal19-exec-cwd-mount-policy/canonical-matrix-v2/`. This is
qualified policy-failure evidence, not a passing whole fixture: peer mount
restoration failed with EXDEV. The compiled native-parent restoration fix has
not run because automatic approval review rejected the retry, including after
private-disk isolation was demonstrated. All owned VMs are stopped. See the
stage's `validation.json` and `approval-review.md` for limits and the approval
needed before another mount-hierarchy retry. Earlier symlink-alias ENOENT cases
and mount-parser/setup failures are excluded from policy conclusions.

## Required transfer contract

This is an implementation contract, not a description of implemented support.

The retained File prerequisite now has a `MountPolicy` value, initialized before
sharing. Mount attachment and policy reads combine restrictions by union with
the pinned mount. FD metadata, write/pwrite, truncate, xattr, fstatvfs and the
existing exec nosuid consumers use that effective policy. This permits an
independently staged File to preserve a restrictive client view even when its
native owner mount is writable. It does not install that view on remote exec:
cwd/lookup propagation, grant transfer and receiver adoption are still required.
Since 9IF, scalar/RDMA remote VFS owner writes admit and retain the exact File
under the registry lock, check its write mode/offset/view policy before touching
registry activity, then use the common positional-write adapter outside the
lock. RDMA admission errors finish the tagged region and active write receipt.
Since 9IG, mapping writeback and fallback compute-output replay also use the
common retained positional-write adapter. Each writeback chunk validates its
entire offset range before its first write; replay retains stdout through descriptor removal
and preserves its existing offset-zero behavior. Since 9IH, file_allocate retains
one File before fstat and uses it through policy admission, lazy/cache/eager
materialization and shared-range publication. A read grant is required even for
PROT_NONE; shared writable mappings also require a write grant and a writable
retained view. Private writable mappings remain allowed on readable, read-only
Files. Denial precedes MAP_FIXED replacement or address reservation. mprotect
checks every overlapping retained shared File before changing lazy metadata or
PTEs, including unmaterialized pages. Non-regular shared mappings retain policy
without gaining regular-file writeback. Clone and split preserve these records.
The admitted File's final release follows publication unlock. Since 9II,
writeback preflights all retained File grants/view policy and whole requested
offset intervals before any backend write, including one aggregate preflight at
quiesced teardown. Clean rejected ranges remain excluded no-ops for that call.
Standalone msync serializes mapping identity through I/O with shared VM
publication. This does not make backend state/I/O failures transactional.
The native userspace fixture passes 72 cases on TMPFS/XFS as root and UID/GID100,
covering descriptor modes, shared upgrades, private writes, FD reuse and correct
msync destinations. WKI mappings, full inherited-view behavior and remote cwd
adoption remain separate qualification obligations. Ordinary native File export/import remains unsupported;
these prerequisites enable no new wire feature. Synthetic REMOTE `ST_RDONLY`
reporting remains unchanged.

The lookup scope must distinguish native mount flags from an inherited export
view. Existing native lookup selects the final mount after `..`, mount crossing
or an absolute symlink; its old mount's readonly flags do not automatically
constrain a different mount. `File::mount_policy` currently also captures native
mount flags, so blindly copying it into every later lookup would overconstrain
those paths. Represent inherited restrictions together with their retained
export/root boundary, and preserve native mount selection when lookup leaves a
native mount. This distinction must be covered by positive cross-mount controls
as well as client-view denial cases before implementing cwd propagation.

LookupHandle now snapshots the selected native mount's readonly/nosuid policy
when the builder installs its retained MountRef. Direct handle mutation and
metadata consumers use that value; moving transfers it and reset clears it.
The native crossing regression also exposed a path recovery error: after a
namespace change, a dirfd at a mount root recovered `/mount/` instead of
`/mount`, making subsequent `..` stay inside that mount. Dirfd recovery now
uses the exact mount path when the backend reports its root, matching cwd
anchor recovery. Other backend suffixes and user components are unchanged.
Since 9IK, hybrid open admission uses the stable lookup's selected policy and
refuses a different mount before backend effects. Mount/File reservation captures
the admitted policy and combines it with native flags and any existing private
File restriction before publication. Native, mapped remote, fresh procfs, owner
open/pin and remote cwd preparation use this path; shared procfs FD reopens keep
the existing description unchanged. Executable File replacement also copies the
source effective policy, including repeated replacement and a source without a
native mount. Both exec entrypoints already capture nosuid before replacement;
the discovered clone gap alone is not evidence of a current set-ID bypass.

Since 9IL, all eleven XFS retained-lookup mutation APIs accept an explicit
readonly restriction at their existing native readonly check. All 24 VFS calls
pass the selected LookupHandle policy. Handle-based rename/link combine source
and destination restrictions for XFS and TMPFS; native resolved-path fallbacks
retain their existing same-mount contract. XFS still checks invalid/mismatched
mounts before readonly and readonly before lookup validation, credentials,
no-ops, metadata locks and backend transactions. Caller policy never changes the
shared native mount. The writable-XFS regression qualifies these backend denials
and unchanged outputs/state, with positive native writes afterward. Cross-mount
EXDEV ordering is source checked. This does not enable an inherited scope.

Since 9IM, lookup authorization also captures the canonical owner's cwd anchor
identity under the path lock. Single/pair consumption and chdir publication
compare that identity together with existing root/cwd hashes and credentials.
An independently published anchor therefore invalidates an older request even
when both strings and the namespace are unchanged. The immutable local serial
is never reused, and serial exhaustion refuses new anchor allocation. Copies
of an anchor preserve its identity. Lookup stores only the value, avoiding new
filesystem or remote-cancellation cleanup under validation/publication locks.
This serial is not an owner-issued wire grant or an export boundary; none of
those capabilities can be inferred from it.

Since 9IN, `vfs_directory_path_beneath_directory` can recover a directory's
relative name beneath an exact retained native root File. Both TMPFS and XFS
walk authoritative parent membership to that root object, preserving it across
root rename/name replacement and refusing a directory moved outside it. Removed
roots/directories are rejected; every error leaves output bytes/length unchanged.
The caller retains both Files; this query creates no new ownership or DAC grant.
The result describes membership at the observation point: later operations must
still retain/revalidate the root and namespace, not use the returned text as
authority. The current caller's search checks belong to operation admission.

This is the same-native-mount part of boundary validation. Cross-mount queries
return EXDEV. Full views must also retain covered-mount ancestry, carry their
owned root/policy through lookup and File/cwd publication, and integrate cache
and fallback dispatch before activation. Ordinary path recovery retains its
prior behavior. No inherited view or transferable grant is enabled by this query.

Since 9IO, `DirectoryViewRef` owns search-only root/cwd Files together with
immutable, explicitly inherited policy. Preparation captures one Snapshot;
narrowing verifies the new root is still under the old root and unions policy
restrictions. Current cwd search and membership are checked under the native
backend metadata lock. Existing ancestor search is not re-required for an
already retained cwd. Native mount flags remain per-lookup and are not inferred
as inherited policy by this factory.

Candidate creation and output replacement occur outside namespace publication.
Copies keep one view object alive; narrowing owns its own File references and
has no parent-view ownership chain. Last release can close Files and must stay
outside path, registry and publication locks. These are owned descriptions:
since 9IT they can be attached to native directory Files/cwd, but are not serialized. Borrowed root/cwd Files
must never substitute for the complete view when forwarding authority; its
policy and boundary must travel together through the future grant/adoption path.
The factory caller must supply already-admitted source-view policy.

Since 9IP, the serialized open fallback keeps its completed LookupHandle and
its walk's cwd/dirfd captures outside the namespace publication guard's lifetime.
Successful and failed acquisition therefore release their final Files after
unlock, even when a descriptor was concurrently withdrawn or a remote dirfd was
declined before backend dispatch. Ordinary component walks retain local
ownership. The complete native walk and consumption remain serialized; remote
cwd refusal and credential/context validation are unchanged. This fixes cleanup
ordering without installing the owned directory view into lookup state.

Since 9IQ, `vfs_acquire_directory_view_lookup` resolves an owner-relative
name through a retained view and stores that complete view in its LookupHandle.
It captures one admitted Snapshot, revalidates root/cwd membership and current
cwd search, then performs the existing native owner-confined component walk
under namespace publication. Physical root text is recovered from the retained
object for mount selection; no historical text/cache fallback is used. Absolute
symlinks retain the existing owner API's physical confinement semantics; this
is not a Task-visible root or cwd installation API.

Since9JG, scoped native walks cross exact retained covered-directory edges;
unsupported or missing-cover transitions return EXDEV without fallback. Shared
consumers validate the selected directory/parent beneath the exact scope root.
Handle policy unions selected native flags and inherited view bits;
move/reset preserves/releases the complete view. Single and paired consumers
check current view membership/search after their existing context/generation
validation and before callbacks. Acquisition failure preserves prior output;
output replacement and final retained-view release occur after publication
unlock, including source/output aliasing. Scoped handles must be retired outside
path, registry and publication locks. TMPFS directory UID/GID/mode changes now
advance a separate search-authority generation under the metadata lock. VFS
captures it before component/fallback search and checks it in request/ambient
consumption and final chdir publication. A changed ancestor therefore invalidates
prior path-search admission even if the view cwd remains searchable. Backend
namespace membership handles keep their current-DAC contract. Regular-file
metadata and unchanged directory authority do not advance this generation. XFS already invalidates directory metadata transactions.
File/cwd publication now carries the view as described below; wire grants still
need to carry its complete authority rather than borrowed Files or policy bits alone.

`vfs_rebase_directory_view` now derives an independent cwd view from a scoped
lookup target. It validates the existing admission under namespace publication,
pins the target, then releases publication before narrowing the source view onto
the new pin. SEARCH remains the default: native pinning and final admission both
check current target search. Explicit IDENTITY_ONLY retains a directory without
requiring target execute permission; a read-opened directory can be listable
without being searchable. Both modes check parent/path admission, directory
type and exact root membership, including a move outside the root after pinning.
Identity-only retention grants no OPEN, data, enumeration or CHDIR authority.
Every subsequent view lookup and consumer still checks current directory search;
the admission choice is not stored as a permanent access grant. Default base and
narrowing factories continue to require search. Errors preserve the previous
output and staged pins retire after unlock. The detached identity File holds
native mount policy; inherited restrictions stay in the new immutable view.
Repeated derivation owns root/directory Files directly, without predecessor-view
chains or File/view cycles. The publication integration below uses this helper;
exec transfer must still preserve the complete view. It transfers no authority
over WKI.

Since 9IT, native directory Files and cwd anchors can carry a complete owned
view. Directory OPEN stages an independent IDENTITY_ONLY rebase before
publication; CHDIR stages SEARCH admission. The detached identity Files never
carry another directory view, avoiding File/view ownership cycles. A shared
File description preserves the view through FD duplication, removal and reuse.
Later relative lookup captures the complete File/cwd view, checks current search
and exact membership, and intersects its boundary with the current Task/export
root. Lookup policy unions inherited restrictions with the selected native
mount flags. Empty-view native paths retain their existing mount-crossing rules.

Scoped walks use retained native identity and skip text routing/discovery and
synthetic fallback. Other mounts return EXDEV before backend access until covered
mount ancestry is represented. Process absolute symlinks are translated using
the caller's logical root; physical owner requests keep physical semantics.
Only physical owner request text has its root prefix stripped. At a matching
process root, dot-dot retains ordinary root clamping. Current cwd name recovery
checks membership, follows root renames and rejects a directory moved outside
its retained boundary.

Task-aware kernel File opens share the retained open helper without allocating
a temporary userspace descriptor. Early boot keeps its existing no-Task path;
kernel mode handling keeps its prior no-extra-umask convention. Required path
storage is allocated before create/truncate effects. Optimistic and serialized
open paths stage the complete view; their outer reference owners retire failed
pins and replaced views after namespace publication unlock. Regular Files retain
inherited mount policy without carrying a directory scope.

Since 9IU, intermediate PROCFS fd/cwd links capture an owned native directory
view or compatibility text from one selected FD/cwd. `procfs_readlink_view_as`
shares the existing private-access and target-credential checks; it cannot check
an old FD for a view and then read a reused FD's text. Optional cwd-view capture
uses the same canonical root/cwd/anchor snapshot and validates it before output.
Raw READLINK remains text-only, and final PROCFS FD OPEN still shares the exact
open description. Empty-view links retain compatibility behavior.

When a view is present, the component walker binds directly to its retained
native directory, intersects its boundary with the caller's root, and walks the
remaining components without a link-text restart. It retains the selected
ProcFile metadata and target credential generation separately from the native
backend in LookupHandle. Acquisition and single/paired consumers revalidate that
origin before publication or backend effects. A target credential ABA therefore
invalidates commit even if the caller Snapshot is equal again; descriptor
withdrawal/reuse after capture cannot redirect the retained selection. Lookup
move/reset carries and retires origin ownership outside publication. The existing
serialized component walker still declines PROCFS before reading it; no extra
PROCFS capture or last-reference close is added under that guard.

These are native publication and traversal prerequisites, not completed remote
propagation. Cross-mount ancestry, removed cwd behavior, process-root installation,
negotiated root/cwd grants and authenticated receiver adoption remain pending.
The original remote cwd/client-view failures remain unresolved. No transferable
directory token or imported process-root installation exists yet. Other legacy
getcwd/readlink consumers still require their own lifetime audit when activated
for imported process roots.

1. Capture the canonical owner's cwd anchor, root and applicable routing/mount
   restrictions with the admitted exec credentials. Hold the exact directory
   throughout preparation, receiver admission and source cutover. A current
   `getcwd()` string cannot replace this ownership: another rename can occur
   after the query, and removed directories have no usable pathname.
2. Carry a versioned, negotiated directory-transfer record separately from
   display text. It must name an owner-issued retained object and its exact
   peer/resource lifetime. Raw pointers and a bare fd/channel number are not
   valid transfer identities. Channel allocation generations are local to each
   node; never assume the source generation equals the owner's generation.
3. Map compute credentials through current authenticated policy before adopting
   any transferred reference. Validate the request against the source's live
   directory ownership and exact owner incarnation. Hold binding/peer admission
   while acquiring an independent receiver reference, then release registry
   locks before native VFS work. A stale token must not select a replacement.
4. Construct and validate the receiver's cwd anchor before task publication or
   TASK_ACCEPT. Do not install it through a pathname lookup. Preserve task-root
   confinement and relative component search under each operation's current
   mapped snapshot. An existing cwd grants directory identity, not arbitrary
   file data access or authority to walk outside the retained root.
5. Preserve restrictions inherited from the source view. Returning a cwd from
   a read-only/nosuid client mount to its writable native owner must not shed
   those restrictions. Merely adopting the owner's File/MountPoint would lose
   the client policy. The lookup and resulting File policy must retain the
   applicable restrictions even when the physical backend is native.
6. Cover native cwd staying on the source, remote cwd returning to its owner,
   and a remote cwd whose owner is a third node. These need explicit ownership
   and mapping flows; the demonstrated return-to-owner case does not prove the
   others. No incompatible peer may silently fall back to historical text.
7. Rejection, cancellation, source exit, worker handoff and peer retirement
   release exactly their own staged references. Before acceptance the source
   image/cwd/credentials remain intact. After acceptance the new task owns an
   independent reference before the old image can close its anchor. Never wait
   or destroy the final File while holding path, registry or publication locks.

`TaskSubmitPayload`, fixed identity lengths, context/policy offsets, fragmented
submission validation and both INLINE/VFS_REF encoders must be reviewed together
before selecting the wire extension. Preserve existing saved-ID/group encoding,
signal handoff, resource accounting and peer identity admission. Old-peer refusal
and existing unanchored/root behavior need explicit compatibility tests.

## Verification required for the implementation

Keep the current regression assertions: both backends must report nine passing
cases and successful cleanup. Add deterministic ownership/denial tests around
the production transfer helpers, then live checks for rename during submission,
removed cwd, source reference close, current search revocation, supplementary
groups, worker exec and failed receiver admission. Qualify read-only/nosuid
retention, narrower roots, owner retirement and each supported placement shape.
Use normal functional restart/cleanup where needed; no packet-fault campaign is
required. Build both profiles and run isolated KTEST when kernel changes land.


## Native root/cwd preparation (Goal19 9IV)

`vfs_prepare_filesystem_context` stages a move-only
`PreparedFilesystemContext` from one retained native `DirectoryViewRef` and
one explicit credential Snapshot. It owns separate root/cwd backend anchors,
current physical root and logical cwd display paths, and the preparation's
namespace/mount epochs. Both identities are acquired from native pins at the
empty relative path; no pathname lookup, userspace FD, ambient worker identity,
or Task publication is involved. Root identity retention does not require new
ancestor search permission; current cwd search and exact root membership do.
Inherited readonly/nosuid policy remains separate from native mount flags.

Candidates and the source reference outlive namespace publication. Failure
preserves the previous output, including late path-length failure; replacement
and final native File release occur after unlock. Display text is recovered
from the owned objects. Renaming a root and recreating its old name cannot
redirect preparation. This API still rejects a removed cwd with ENOENT. Its
epochs describe a preparation, not a cached authorization to install it later.

Process installation remains pending. A future commit must validate and publish
both identities under the same guard, and migrate absolute lookup, cwd/dirfd
intersection, realpath/getcwd/chdir conversion, statvfs, PROCFS root links,
lookup context identity, fork/spawn/exit and WKI capture/restore together.
Task currently stores its process root only as text. Applying the current
same-mount native view to every process root would reject ordinary child
mounts such as `/proc` and `/dev`; covered-directory mount ancestry must be
implemented before enabling that behavior. No negotiated grant, receiver
adoption, wire compatibility or original remote failure is resolved by this
staging API.


## Mount lifetime prerequisite (Goal19 9IW)

Parent mount retirement now rejects published or initializing descendants with
EBUSY before changing the ordinary retirement state. Retiring descendants also
count. Child preparation, reservation and publication reject retiring ancestors
under the same mount-table lock. Shutdown rechecks dependencies before backend
teardown so a busy child's rollback cannot destroy its parent. Cancellation
releases initialization dependencies, and similar-prefix siblings are distinct.
These are canonical mount-table topology checks, not directory membership proof.

Exact covered-directory ownership is still required for cross-mount roots.
Public mount publication must retain its admitted target object; generic boot
and remote synthetic mounts need explicit handling. Pivot remapping must update
those edges and retirement must release them outside locks without cycles.
Public native covered edges and object-aware scoped walking are implemented
in9IX–9JG. Generic/post-pivot edges remain incomplete, and
PreparedFilesystemContext is not installed on production Tasks.


## Native covered-directory ownership (Goal19 9IX)

Public mount on TMPFS/XFS now retains a pathless search-only File for the
exact directory selected by its admitted target LookupHandle. It requires
identity retention, not an extra final-directory search grant. The mount
table takes an independent reference after successful publication; failed
preparation/publication closes the outer staged owner after namespace unlock.
`capture_mount_covered_directory` returns the retained native identity and
table epoch. Missing or unsupported proof returns ENOTSUP; this capture is
not a later traversal authorization or a transferable directory grant.

The pin keeps the parent backend busy through child teardown and retained
readers. Pivot discounts only independent cover Files solely owned by the
mount table when testing internal busy references. Ordinary Files and
externally held cover references keep the existing busy behavior. Successful
pivot detaches old relationships before rewriting paths and closes them after
mount unlock; failed pivot preserves them. New covered relationships after
pivot remain unproven until an explicit reconstruction step is implemented.

Generic boot/synthetic/remote mounts and non-native target backends still lack
this producer. Cross-mount owned-view walking, rename topology, process-root installation
and authenticated remote grant transfer remain disabled/pending. This change
does not replace those requirements with pathname-based ancestry inference.

## Intermediate XFS directory search authority (Goal19 9IY)

A native path can search an XFS directory and return through `..` to finish on TMPFS. The retained final backend's lookup sequence cannot detect a later permission change on that earlier XFS directory. A deterministic baseline KTEST reproduced this: after removing directory search permission, the old single lookup and both paired orders succeeded and invoked three callbacks. The full baseline finished with 54,044 passes and that one regression failure.

XFS now publishes a global directory search-authority epoch alongside its existing per-mount lookup sequence. The shared chmod/chown identity transaction advances it only after a successful actual directory UID, GID or mode change, before releasing the metadata lock. File and pathname operations share that helper. Failed/denied updates, no-op directory changes and regular-file metadata preserve the new epoch. Both component and fallback VFS lookup capture it before search; shared single/pair validation and final CHDIR publication reject stale admission. Prepared filesystem-context staging compares it alongside the TMPFS search epoch.

This adds an internal lookup field and atomic operations, preserving existing native lock order, transaction rollback, stable credential snapshots, UID/GID/saved-ID semantics and syscall/wire ABI. Unrelated XFS directory authority changes can conservatively cause retries. Owned DirectoryView mount crossing remains disabled; exact covered-edge membership and process-root installation still require implementation. Original remote cwd/name-replacement and readonly-policy failures remain unresolved. Evidence and final validation: `build/goal19-xfs-search-authority/validation.json`.

Normal and diagnostic builds, 91 credential source checks, seven other focused source suites, manifest audit and scoped formatting/tidy pass. The final full KTEST passes 54,045/0 and powers off; the new native fixture passes at121.333 seconds. Final host inspection confirms all baseline/final VM identities gone and disks free. The goal stays11enforced/10pending. No new normal or multi-node VM fixture was run for this change; it does not qualify concurrent stress or remote behavior.

## Native mount-chain directory membership (Goal19 9IZ)

`vfs_directory_path_beneath_directory` can now recover an exact retained directory's name beneath an exact retained root across native TMPFS/XFS child mounts. It retains each table-owned covered directory, recovers native object segments, and finishes with same-backend object-root membership. No historical pathname selects a root or cwd. The temporary cover array outlives namespace publication on every return; mount references retire atomically and native metadata callbacks run outside the mount lock. The chain is bounded by `MOUNT_PATH_MAX / 2`, and outputs publish only after capacity, namespace/table epoch and retirement checks.

Every covered edge must still agree with the current namespace. Its native covered-object path must equal the child's mount-table path, and the selected mount at that path's parent must be the covered File's owner. A moved ancestor or an inserted intermediate overlay returns EAGAIN instead of inventing a new attachment. Missing generic/post-pivot proof returns ENOTSUP. These path comparisons validate an existing object link; they do not replace native membership with a prefix grant. Recovery remains metadata-only and ignores DAC, while actual traversal continues to check the current caller.

The mixed TMPFS→XFS→TMPFS and XFS→TMPFS→TMPFS fixtures cover child roots, nested descendants, exact intermediate boundaries, outside and removed/replaced objects, restricted mode versus user traversal denial, unsupported cover proof, small outputs, balanced references, stale covered-ancestor rename and fake replacement roots, attachment restoration, inserted-overlay refusal and checked leaf-first cleanup. Review restored the prior invalid-argument precedence and added a regular-File/null-root regression on both backends. Initial full KTEST passes54,047/0; final-image evidence is recorded separately in `build/goal19-native-mount-membership/validation.json`.

DirectoryView admission, native anchor staging and scoped component consumers still require one mount. They must migrate together with object-aware mount entry/.. and references that survive publication guards before process-root ownership is activated. Generic/post-pivot proof production, renamed mount attachment, transferable grants and authenticated receiver adoption remain pending. Original remote cwd/name-replacement and readonly-policy failures are unresolved. This helper provides membership evidence, not a cached authorization decision or a transferable grant.

Final normal/diagnostic builds, 92 credential source definitions, seven other focused suites, manifest audit and scoped formatting/tidy pass. Final full KTEST passes54,053/0; mixed-mount fixtures pass126.458/131.514 seconds and the updated boundary fixture passes56.090. The VM powers off267.180 and launcher exits0. Final host inspection confirms all initial/final VM identities absent and normal/isolated disks free. The goal remains11enforced/10pending; no normal or multi-node runtime fixture is claimed for this internal helper change.

## Native cross-mount view and context preparation (Goal19 9JA)

The native membership implementation now receives an outer reference owner when called under an existing publication guard. Each proof has a mount-path depth bound, and the owner holds at most two maximum-depth passes. No earlier capture is released while publication is held. The public metadata-only wrapper owns this storage before its own guard. View factories use it for root narrowing plus cwd admission; filesystem-context preparation uses it for admission plus current cwd recovery.

Native same-mount admission retains its caller-aware backend path check. Cross-mount admission first proves exact covered-object membership, then checks search on the actual cwd itself. Retained ancestors do not need a renewed X grant. Narrowing and cwd checks share publication and mount/native-search epochs. Each prepared root/cwd anchor dispatches independently from its own File; cwd recovery no longer interprets it using the root mount's backend. Inherited policy only narrows, source/credential aliases are captured before replacement, and failures preserve old output/accounting. Serialized callers without an outer owner retain their existing EXDEV guard. Scoped lookup and Task-root installation are not enabled by constructing a description.

The native tests cover32 mixed root/cwd, caller and inherited-policy combinations, typed anchor identities and paths, independent busy pins, monotonic narrowing, alias/move preservation, ancestor mode000 versus cwd denial, unsupported/outside factories, a late overlong-cwd failure, removed/replaced cwd, stale covered-ancestor attachment and restoration. Fresh File close hooks observe two final native closes with path/publication/mount locks free on each backend arrangement. They also confirm that scoped lookup refuses the new cross-mount descriptions before populating a handle.

Normal Build WOS and isolated diagnostic build pass. All93 credential source definitions, seven other focused suites (including17 mount lifetime definitions), manifest audit and scoped cache-disabled formatting/tidy pass. Full KTEST passes54,055/0 at278.577 seconds; new fixtures pass136.057/141.145 seconds, followed by poweroff278.578 and launcher0. Final host audit2026-09-14T15:29:29.089498+00:00 confirms both exact owned process identities gone, no WOS VMs and normal/isolated disks free. Evidence: `build/goal19-cross-mount-context/validation.json`.

Goal19 remains11enforced/10pending. The staging reference owner's two-pass bound is not a solution for arbitrary repeated walking; serialized lookup ownership, object-aware mount entry/.. and typed consumer validation remain to be integrated. Generic/post-pivot covers, renamed attachment semantics, Task-root commit, negotiated owner grants and authenticated receiver adoption remain pending. Original remote cwd/name-replacement and readonly-policy defects are unresolved. No normal or multi-node VM fixture, peer retry or packet-fault campaign ran in this increment.

## Reusable native mount-reference ownership (Goal19 9JB)

Native ancestry preparation now retains each exact child mount and covered directory once per mount-table epoch. Repeated visits borrow those retained identities instead of consuming another slot or dropping a duplicate File under publication. Eight entries are inline; additional distinct edges use nothrow blocks and can return ENOMEM. A failed retain or allocation preserves output. A successful retain enters the outer owner before later epoch checks. The owner refuses a changed mount-table epoch, and destroys overflow blocks iteratively after publication unlocks. The child MountRef prevents pointer reuse and the covered File keeps its parent backend alive.

Each proof still has its own mount-path depth bound. Native segment recovery, exact root membership, covered attachment and overlay checks, current cwd search, search epochs and output publication remain per decision. The retained edge is not cached authorization. This replaces the previous two-pass storage limit; it does not activate scoped mount crossing. The next integration must supply this outer ownership through serialized lookup/consumption and implement exact object entry/.. plus typed anchor consumers together. Task-root installation, generic/post-pivot covers, renamed mount attachment and authenticated remote grants remain unfinished.

The new native fixtures repeat more ancestry passes than the old owner could hold while checking constant per-edge reference counts. They exercise spill allocation refusal/retry, current user/root search, rename/outside/stale-attachment decisions, real mount-generation invalidation and checked retirement of all spill-held mounts. Current validation is recorded in `build/goal19-membership-reference-reuse/validation.json`.

Normal and diagnostic builds,94 credential source definitions and seven other focused suites, scoped formatting/tidy and manifest audit pass. Full KTEST passes54,057/0 at285.433seconds; new native fixtures pass144.332/149.905seconds. Poweroff follows285.434seconds and launcher exits0. Final host audit at2026-09-14T15:53:26.507412+00:00 confirms both exact process identities absent and both disk sets free. Goal19 remains11enforced/10pending; no normal or multi-node runtime fixture is claimed.

## Lookup-owned immutable membership (Goal19 9JC)

A lookup can now own its native covered-edge references through consumption. The builder takes a non-null cookie/callback only into an empty slot; failure preserves caller ownership. Move transfers the cookie, and reset releases it exactly once under the existing outside-lock handle lifetime contract. The consumer observes only a const owner. The membership adapter records during preparation and borrows during consumption; missing or stale edges return EAGAIN without capturing a File, growing storage or refreshing the owner. Native object membership and cwd search are still checked for each decision.

Shared single and paired lookup validation accepts this proof only for a final mount matching the view's retained root or cwd endpoint. An unrelated final mount is refused even when a cookie is present. The component walker still needs exact mount entry/.. and typed cwd/PROCFS/serialized-open integration before arbitrary native crossing can be enabled. Explicit cross-view acquisition already stages its owner outside publication, so rejection preserves the previous output and retires its new captures after unlock. Successful cookie transfer is also outside the guard. Ordinary same-mount acquisition does not allocate the owner.

Native fixtures assemble retained native handles and admitted proofs to exercise the shared consumption prerequisite; they do not claim that production scoped crossing is enabled. Tests cover actual inode results, constant reference counts during consumption, partial/empty/missing proof, unrelated final mounts, search revocation, attachment rename, mount epoch changes, move/reset ownership, rejected acquisition output preservation and observed final native File closes outside locks. Initial and reviewed final runtime results are separate in `build/goal19-lookup-membership-ownership/validation.json`.

Initial full KTEST passes54,059/0 but predates the endpoint guard correction. Final normal/diagnostic builds,95 credential source definitions and seven other focused suites, scoped formatting/tidy and audit pass. Final full KTEST passes54,059/0 at294.529seconds; native fixtures pass154.159/159.265seconds, poweroff294.529 and launcher0. Host audit at2026-09-14T16:23:55.306889+00:00 confirms all four initial/final process identities absent and disks free. Goal19 remains11enforced/10pending.


## Native component mount crossing (Goal19 9JD)

Ordinary native component walks now enter a published TMPFS/XFS child mount only after comparing the actual parent/name object with that child's retained covered directory. Upward `..` crossing verifies the current native mount-root object and derives its parent from the exact cover. The shared attachment check also requires the cover's current native path and selected parent mount to agree with the table attachment. A renamed covered ancestor plus replacement directories returns EAGAIN with unchanged output. Only missing generic/boot/post-pivot cover capture retains the existing unscoped pathname compatibility path; errors after successful capture cannot fall back.

Each decision uses the admitted Snapshot and visible current-directory search. Hidden covered directories supply identity only; mode000 on them does not introduce a new search requirement. Mounted-root metadata still requires parent search, while a following component or `..` requires search on the visible mounted root. Existing scoped crossing/endpoint guards, inherited/native mount policy, saved-ID and syscall/wire interfaces remain unchanged. Normal and serialized walks keep mutable reference owners outside publication, then freeze nonempty ownership into the final lookup after fallible work. Captured Files remain outer-owned on failure and retire after locks. Native metadata callbacks remain outside the mount spinlock.

The isolated baseline reproduces four incorrect successes (root/user on two native layouts) and finishes54,059passed/2failed. The final mixed TMPFS→XFS→TMPFS and XFS→TMPFS→TMPFS cases check descent, repeated upward crossing, hidden covers mode000, visible branch/root search denial and root controls, exact metadata, normal/serialized proof ownership, borrowed edge identity, move/reset reference balance, retained child-root upward lookup, renamed attachment refusal and checked teardown. The prior deliberately incomplete-proof fixture now binds its selected pinned object directly so automatic full walker proof does not replace its consumer-denial coverage.

Normal Build WOS (RelWithDebInfo) and isolated diagnostic build (Debug) pass. All96 credential source definitions and seven other focused suites, including17 mount-lifetime definitions, pass, as do cache-disabled scoped formatting/clang-tidy and manifest audit (0 legacy raw-root checks). The completion audit still reports pending exec. Scoped whitespace checks pass; the whole-tree check still reports only four preexisting unrelated lines in tests/host/unit/wki_remote_vfs_source_test.py:4121-4124. An evidence parser was corrected to match the actual `selftest complete; powering off` log message; it was not a runtime failure.

Final full KTEST passes54,061/0 at304.980seconds, with the new fixtures passing164.628/169.775seconds, poweroff304.981seconds and launcher0. A separate fresh normal single-node image boots through init/pivot, matches its saved kernel build-ID through local QMP/GDB, and passes all19 ordinary native mount-descendant checks with cleanup. Sync and launcher shutdown both return0. Final host inspection at2026-09-14T16:54:43.895980+00:00 confirms all six exact baseline/final/normal process identities gone, no WOS VMs and both disk sets free.

Goal19 remains active,11enforced/10pending. Scoped typed cwd/PROCFS/open integration, canonical Task-root installation, generic/post-pivot covers, renamed mount topology and negotiated authenticated remote grants remain unfinished. The original remote cwd and readonly-policy defects remain unresolved. No peer retry, multi-node fixture, concurrent stress or packet-fault campaign ran. The original goal command and appended --no-setup authorization are unchanged. Evidence: build/goal19-native-mount-crossing/validation.json, baseline/, review.md, patch-contract.md, next-integration.md, live/, scoped.patch and before/.


## Typed native anchor name recovery (Goal19 9JE)

`directory_anchor_path` now verifies that a retained view's cwd is the anchor's exact native object and mount, then proves root/cwd membership using each File's own backend. TMPFS compares retained nodes; XFS uses its public empty-lookup identity API. A mixed-mount caller must supply an outer recording or immutable borrowed membership owner while holding publication; otherwise EXDEV is returned before backend-private casts. Name recovery remains metadata-only and does not renew directory search authority. Output is staged until namespace, mount and proof checks pass. The helper allocates no reference owner and acquires no publication lock.

PreparedFilesystemContext uses that helper for both root and cwd under its existing publication guard and outer reusable owner. Their proven physical names are then converted to task-visible cwd text. The prefix conversion supplies text only; exact native membership has already been checked. Snapshot admission, inherited policy, source/output alias capture, typed anchor pins, saved-ID semantics, capacity/error handling and final search-epoch validation remain intact. No Task root, credential, syscall or wire layout is added.

The new32 native profiles cover both root-backend arrangements, two cwd placements, root/user callers and four inherited-policy combinations. They verify missing owner EXDEV, incomplete borrowed proof EAGAIN, fresh capture and repeated borrowing without new proof references, constant cover counts, current names after rename/restoration, metadata recovery with root/cwd mode000, wrong same-mount inode/view EBADF, unchanged failure output, removed/replaced cwd and stale covered-ancestor attachment. Existing preparation tests retain narrow/alias/move/capacity checks and two actual last native File closes outside path/publication/mount locks. No pre-fix live crash was induced or claimed; the incompatible-backend dispatch was found by reading the source.

Initial builds passed before the rename/removed-directory fixture expansion and are logged separately. Final Normal Build WOS (RelWithDebInfo) and isolated diagnostic build (Debug), all97 credential source definitions and seven other focused suites (including17 mount-lifetime definitions), manifest audit and cache-disabled scoped formatting/clang-tidy pass. The audit reports0 legacy raw-root checks; the completion gate still reports pending exec. Scoped whitespace checks pass. The whole-tree check still flags only the same four preexisting unrelated lines in tests/host/unit/wki_remote_vfs_source_test.py:4121-4124. Initial process-audit artifact setup needed an empty owned-process registry; the completed audit confirmed no live WOS processes or disk holders before runtime.

Final full KTEST passes54,063/0 at314.909seconds. The new typed-anchor fixtures pass174.620/179.631seconds, poweroff follows314.910seconds and the launcher exits0. Final authoritative host audit at2026-09-14T17:16:21.109674+00:00 confirms both exact owned process identities gone, no WOS VMs and normal/isolated disks free. No separate normal or multi-node VM fixture, concurrent stress, peer retry or packet-fault campaign ran for this increment.

Goal19 remains active,11enforced/10pending. Ambient cwd/PROCFS calls may already hold publication and still need explicit outer proof/retirement ownership. Scoped walking, selected-result boundary validation and serialized open must be integrated before those mixed contexts become ambient. Canonical Task-root installation, generic/post-pivot covered identities, renamed mount topology, negotiated authenticated grants and independent receiver commit/cancellation remain pending; original remote cwd and readonly failures remain unresolved. The original goal command and appended --no-setup authorization are preserved. Evidence: build/goal19-typed-anchor-path/validation.json, review.md, patch-contract.md, next-integration.md, scoped.patch, serial-vm0.log and before/.


## Cwd/PROCFS recovery proof ownership (Goal19 9JF)

Public getcwd queries and structured raw PROCFS readlink can now recover a prepared mixed-native cwd using an outer membership owner. Their supplied credential Snapshot already permits blocking owner recovery and requires outside-publication context; that contract is now explicit in the header. Only native mixed text recovery takes a nested publication guard. The typed anchor/member helper records into the outer owner, the guard ends, and Task root/cwd/anchor identity is rechecked before output. Bytes, length and output view remain unchanged on denial. Remote RPC recovery never runs under the new guard; same-mount native recovery keeps its previous epoch path.

Null-snapshot text-only callers may already hold publication. A mixed anchor is refused with EXDEV under the Task path lock before taking its reference, preventing an unsupported capture from becoming the last closer under the caller's guard. Existing null-snapshot remote ENOTSUP precedence and exact owned-view output semantics remain. No mutex ownership is guessed, no new DAC requirement is added to name metadata, and private PROCFS authorization/revalidation remains before query and commit. No Task/credential/syscall/wire layout or production root-install path is added.

The32 native caller/cwd/policy profiles assemble test Task state from prepared contexts across both backend arrangements. They check current cwd text/length, ERANGE with unchanged output, held-publication/default-snapshot refusal with unchanged anchor count, raw PROCFS text, exact scope capture, private foreign-observer denial with unchanged generation/output, root/cwd mode000 metadata, rename/restoration and balanced cover references. Public queries also refuse removed/replaced cwd and stale mount attachment. A selftest-only hook withdraws the Task anchor after capture and before publication. The retained query prevents premature destruction, then returns EAGAIN without output changes and performs two actual final native File closes with path/publication/mount locks free. Both root-backend arrangements pass this deterministic withdrawal case. No pre-fix runtime baseline is claimed.

Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), all98 credential source definitions and seven other focused suites (including17 mount-lifetime definitions), manifest audit and cache-disabled scoped formatting/clang-tidy pass. The audit reports0 legacy raw-root checks; --require-complete still reports pending exec. Scoped whitespace checks pass; the whole-tree check continues to flag only four preexisting unrelated lines in tests/host/unit/wki_remote_vfs_source_test.py:4121-4124.

Full isolated KTEST passes54,065/0 at327.010seconds. New fixtures pass187.500/192.521seconds, poweroff follows327.011seconds and the launcher exits0. Authoritative host inspection at2026-09-14T17:36:53.357736+00:00 confirms both exact owned identities gone, no WOS VMs and normal/isolated disks free. No separate normal/multi-node VM fixture, concurrent stress, peer retry or packet-fault campaign ran in this increment.

Goal19 remains active,11enforced/10pending. Legacy null-snapshot recovery routes, shared scoped-walker ownership, exact selected-result boundary proof and serialized open still require integration. Canonical object-root Task installation, generic/post-pivot covers, renamed mount topology, negotiated owner-issued grants and authenticated receiver lifetime remain pending. Original remote cwd/name-replacement and readonly-policy defects are unresolved. The original goal command and appended --no-setup authorization remain unchanged. Evidence: build/goal19-cwd-recovery-ownership/validation.json, review.md, patch-contract.md, next-integration.md, serial-vm0.log, scoped.patch and before/.


## Scoped native walking and selected-object authorization (Goal19 9JG)

Explicit DirectoryView lookup, native dirfd/cwd component walking and serialized directory opens now share one outer covered-edge recorder. Initial scope admission captures exact root/cwd ancestry, native descent and upward walking validate exact covered objects, and final LookupHandle publication freezes the recorder after fallible preparation. Unsupported or missing-cover scoped transitions still return EXDEV without fallback; ordinary unscoped compatibility remains.

Shared single/pair consumers now validate the actual selected directory, or the retained parent of a regular/missing target, beneath the retained root. TMPFS and XFS helpers validate the native handle and select the retained node/inode under their metadata lock, then recover ancestry without nested locking, new File allocation or renewed ancestor DAC. Cross-mount consumers only borrow existing proof and validate each attachment. A complete cwd proof cannot admit an outside-root object on the root's own mount. Serialized directory-open preparation borrows lookup membership; references and final native closes remain outside publication.

Existing Snapshot, path/search/mount epochs, alias-output preservation, current cwd search, inherited/native policy separation, saved-ID and syscall/wire contracts remain. Canonical production Task-root installation and remote grants are not enabled.

The new32 profiles cover both native layouts, root/UID100 callers, root/child cwd placements, four readonly/nosuid combinations, exact descendant stat, regular and missing targets, rebase, public dirfd/cwd/upward walking, serialized opens, readonly creation denial without a file, hidden covers mode000, fresh caller search denial, stale lookup no-callback and balanced cover references. Existing complete/incomplete/absent/stale proof, alias replacement and unlocked final retirement cases remain. Supported cross-mount context lookup now asserts the selected inode instead of historical blanket EXDEV.

The first full suite finished54065passed/2failed. Both new fixtures passed; the outside-root negative fixture had paired different mounts and expected EPERM despite the existing pair API's earlier EXDEV. The fixture now pairs two selected objects on the root mount, preserving that error precedence and checking actual root containment. Initial failure logs and kernel are retained. An initial compile default-argument ambiguity and formatter wrap were fixed. One clang-tidy attempt crashed; its backtrace is retained, and static checks were rerun with sources held unchanged.

Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), all99 credential source definitions and seven other focused suites (including17 mount-lifetime definitions), manifest audit and cache-disabled scoped formatting/clang-tidy pass. The audit reports0 legacy raw-root checks; --require-complete still reports pending exec. Scoped whitespace passes; the whole-tree check retains only the same four preexisting unrelated lines in tests/host/unit/wki_remote_vfs_source_test.py:4121-4124.

The final full KTEST passes54067/0 at336.325seconds and powers off at336.326seconds; launcher exit0 is confirmed. Both corrected manually assembled-handle fixtures and both new scoped-walk fixtures pass. Authoritative host inspection at2026-09-14T18:18:35.697440+00:00 confirms all four exact initial/final process identities gone, no WOS VMs and normal/isolated disks free.

Goal19 remains active with11enforced/10pending. Legacy null-snapshot cwd text routes, canonical owned Task root/all absolute consumers, generic/post-pivot covers, renamed mount topology, negotiated owner-issued remote grants and authenticated receiver lifetime remain pending. Original remote cwd/name-replacement and readonly-policy defects remain unresolved. The original goal string and appended --no-setup authorization are preserved. No normal or multi-node VM fixture, peer retry, concurrent stress or packet-fault campaign ran in this increment. Evidence: build/goal19-scoped-native-walk/validation.json, review.md, patch-contract.md, next-integration.md, scoped.patch, initial-run/ and before/.


## Caller-authorized filesystem statistics (Goal19 9JH)

Process statvfs now captures one credential Snapshot and acquires a retained STAT/FOLLOW_FINAL lookup. Native selected-object and PROCFS metadata-generation validation precede filesystem queries; shared consumption checks task/root/cwd identity, namespace/mount/search epochs and exact DirectoryView membership. Output includes selected native plus explicitly inherited readonly/nosuid flags. Protected, missing, symlink and trailing-directory errors are preserved rather than collapsed by legacy text resolution. The no-Task early-boot resolver remains separate.

Remote paths reuse existing authenticated owner stat or retained-directory stat-at admission outside publication, followed by local request/table/lifetime validation. They still report their existing synthetic readonly filesystem statistics. No new wire message or remote grant is introduced. PROCFS preparation also runs outside publication. Lookup/proof/File references outlive guards, and both pathname and File-based statistics stage output so failures leave caller buffers unchanged. File queries retain original mount/policy across namespace changes. Approximate accounting semantics, saved-ID behavior and all syscall/wire layouts remain unchanged.

Four new KTEST fixtures cover TMPFS/XFS caller denial, warmed paths, intermediate search before dot-dot, mode000 final-file metadata, symlinks, missing/trailing paths, one Snapshot across a post-capture credential drop, PROCFS/DEVFS existence and failed File output.32 mixed-native profiles cover inherited policy, public cwd across mount crossings and rename, hidden ancestor metadata, and four consecutive permission-epoch races with zero statistics-backend calls and unchanged outputs. All existing view/proof lifetime cases remain.

Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), all100 credential source definitions and seven other focused suites, cache-disabled scoped formatting/clang-tidy, native fixture compilation and formatting pass. The initial KTEST package command exited1 before launching a VM because libguestfs could not access its newly partitioned /dev/sda1. QCOW metadata checks passed; no WOS processes or disk holders remained, and a verified retry packaged successfully. This was not a kernel test failure; the original log is preserved.

The full isolated suite passes54071/0 at354.746seconds and powers off at354.747seconds; launcher exit0 is verified. A fresh one-node normal VM boots with a GDB-verified matching kernel build ID. The real libc/syscall fixture passes44 root/UID100 checks across TMPFS and XFS, including unchanged denied outputs and retained cwd after rename. Fixture cleanup and sync pass; the normal launcher exits0. Final authoritative host inspection at2026-09-14T18:43:27.191351+00:00 confirms all four exact test-process identities gone, no WOS VMs and normal/isolated disks free.

Goal19 remains active with11enforced/10pending and0 legacy raw-root checks. The completion audit still reports pending exec. Canonical owned process root/all absolute consumers, other legacy text routes, generic/post-pivot covers, renamed mount topology and negotiated authenticated grants remain pending. Original remote cwd/name-replacement and readonly defects remain unresolved; native tests do not qualify new multi-node remote statvfs runtime. No pending peer retry, concurrent stress or packet-fault campaign ran. Original goal text and --no-setup authorization remain intact. Evidence: build/goal19-statvfs-authorization/validation.json, patch-contract.md, review.md, next-integration.md, live/, scoped.patch, packaging-first-failure.log and before/.


### 9JI — PROCFS canonical process-root consumers

PROCFS root links, ordinary FD-link root conversion and mount-list rendering now copy root text from the retained canonical filesystem owner under its path lock. A requested thread still remains the pinned PROCFS subject for original private admission and credential-generation revalidation. This removes stale thread-root reads and the unlocked ambient mount-root pointer. The same actual fixture exposed numeric owner_pid reselection in the shared VFS FD-table helper: fd_table_task_for now acquires its independent active Task reference from the retained credential owner. Unbound threads cannot fall back to private FD tables. Existing Task-state refusal, fd locks, RAII release, syscall/wire layouts, descriptor grants and output staging are preserved. Exact owned process roots and remote directory grants remain pending.

Both Normal Build WOS (RelWithDebInfo) and isolated diagnostic (Debug) builds, cache-disabled format/tidy and eight focused source suites pass; all101 credential source checks are invoked. The new deterministic PROCFS fixture covers192 process/thread root/FD readlink requests (including48 authorization denials),24 subject-exit refusals, zero/truncated output, original descriptor mode/position/reference counts, root replacement and canonical mount-list controls. Full corrected KTEST passes54072/0 and powers off. Both initial54071/1 runs are preserved. The first fixture incorrectly used ordinary /tmp as a distinct early-boot mount; the positive control now uses the actual /proc mount and preserves its required scoped/global difference. The second run reports36 allowed thread FD-link failures after that correction, exposing the numeric-owner production gap. The final fix keeps those cases and adds direct bound/unbound FD allocation, lookup/removal and exiting-owner refusal checks.

All eight exact owned launcher/VM identities are gone and normal/isolated disks are free at 2026-09-14T19:24:35.674672+00:00. A fresh single-node normal VM with a matching live build ID passes the existing15 exec-thread cases (image/PID/TCB/CLOEXEC/signal preservation and old-thread retirement) plus root and UID104 retired-main cases, each creating32 fresh threads while using the canonical process descriptors. Uploaded helpers are removed, the guest is synced and its launcher exits0. No multi-node/root-installation qualification, packet-fault campaign or peer mount-hierarchy retry was performed. The latter remains unattempted pending its previously requested approval. Broader exec/WKI and canonical-root gaps remain; manifest stays11 enforced/10 pending, zero legacy raw-root checks, and complete-gate still fails on exec. Evidence: build/goal19-procfs-canonical-root/validation.json; expanded ownership/absolute-consumer map: reconnaissance.md in that directory. The complete objective and --no-setup authorization remain intact.


### 9JJ — Complete filesystem context retains root ownership

PreparedFilesystemContext now keeps its root anchor inside the exposed Task::FilesystemContext alongside cwd. Complete copies and moves therefore preserve both exact objects and their policy. Canonical Task capture takes both anchors and both text caches under one path lock; fork/clone copy into empty private children, spawn moves the complete snapshot, and RemoteExecPlan drops both before releasing its actor. ACTIVE process exit detaches both together and releases them after unlocking; unpublished teardown also drops both while it can block. Threads continue to borrow their retained canonical owner.

Text-only WKI submission refuses an owned root before publishing output. Fresh receiver identity application refuses before umask/job/root effects, and temporary worker impersonation refuses before moving VFS rules or changing identity. No production root-installation API is enabled: null retains the existing global namespace view, including compatibility root prefixes after pivot. These guards are not an atomic root-installation protocol; future activation must serialize against temporary impersonation and migrate every absolute consumer. User syscall, UID/GID/saved-ID and wire layouts remain unchanged.

Actual TMPFS/XFS preparation fixtures now check complete copy/move identity, destruction of the preparation while copies retain both pins, readonly/nosuid policy, exact mount busy counts, recreation and balanced retirement across16 root/user/backend/policy profiles. They invoke the real WKI conversion adapters and verify refused conversion preserves output and task state, with a null-root positive control. Canonical snapshot replacement now performs two independent final File closes and checks both occur outside path/publication locks. Existing denial, alias, rename, removal and late-failure cases remain.

Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), cache-disabled formatting/clang-tidy and eight focused source suites pass; all102 credential source checks are invoked. The initial diagnostic compile failed because the direct WKI selftest call lacked its selftest-only header. The include was added after analysis completed, and builds, core analysis and source checks were rerun; the original failure log is retained. Final full KTEST passes54072/0, powers off and its launcher exits0. Both exact owned process identities are gone, no WOS VM remains and normal/isolated disks are free at 2026-09-14T19:53:43.343473+00:00.

This is ownership-carrier qualification, not exact-root enforcement or remote-grant qualification. No separate normal or multi-node VM fixture ran in this increment. Canonical owned-root activation/all absolute consumers, generic/post-pivot covers, renamed mount topology, negotiated authenticated grants and the original remote cwd/readonly defects remain pending. The specific peer mount-hierarchy retry remains unattempted pending its prior approval request. Goal19 stays active with11 enforced/10 pending and0 legacy raw-root checks; the completion gate still fails on exec. The original goal string and appended --no-setup authorization are preserved. Evidence: build/goal19-owned-root-context/validation.json, source.patch, changes.patch, review.md, patch-contract.md, reconnaissance.md and next-integration.md.


### 9JK — Lookup authorization includes canonical root identity

LookupAuthorizationContext now records the canonical owner's nonreused root-anchor identity beside its cwd identity. Native and compatibility acquisition capture it under the same path lock as their path context, and shared single/pair consumers reject stale root ownership before backend callbacks. Chdir's final publication check includes root identity. Native realpath copies root text and identity together, then verifies both before producing a visible pathname. LookupHandle already copies and resets the complete internal context. No additional anchor ownership, allocation, blocking call, syscall layout or wire field is introduced.

Two new TMPFS/XFS fixtures construct real prepared native root/cwd contexts and replace only the root anchor while keeping root/cwd text, cwd identity and namespace/mount generations unchanged. Across16 backend/root-or-user/process-or-thread/native-or-fallback combinations, stale single and mixed-age pair decisions return EAGAIN with no backend calls and unchanged output; fresh decisions succeed. Owned-to-null and null-to-owned transitions, unchanged anchor reference counts and move/reset are checked. Existing same-text cwd fixtures remain registered and passing. All fixture root swaps retire references after path unlock. No pre-fix runtime baseline is claimed.

Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), cache-disabled formatting/clang-tidy and eight focused source suites pass; all103 credential checks are invoked. Full KTEST passes54074/0 and powers off; launcher exit0 is confirmed. Both exact owned process identities are gone, no WOS VM remains and normal/isolated build disks are free at 2026-09-14T20:07:28.571935+00:00. Evidence: build/goal19-lookup-root-identity/validation.json, source.patch, changes.patch, patch-contract.md, review.md and serial-vm0.log.

Goal19 remains active with11 enforced/10 pending and0 legacy raw-root checks; the completion gate still reports pending exec. This closes stale-root admission without enabling production root installation. Exact-root absolute starts/symlink resets, direct cwd/PROCFS recovery, generic/post-pivot covers, installation serialization, negotiated remote grants and the original remote cwd/readonly failures remain pending. No separate normal/multi-node VM fixture or pending peer mount-hierarchy retry ran. The complete goal string and --no-setup authorization are preserved.


### 9JL — Absolute native lookup starts at the owned process root

Ordinary absolute Task lookup now captures the canonical owner's root anchor into outer walk storage, recovers its current native name as compatibility metadata, and binds the exact typed root object. Absolute symlink resets use that same anchor. Its root-only DirectoryView carries inherited readonly/nosuid policy through selected-object validation and File opens. A mount covering the root's pathname is refused by retained-anchor mount validation before a backend can pair the old inode with a different filesystem. Unsupported ordinary absolute owned-root traversal cannot fall back to the old root pathname or its caches. Explicit physical owner requests and null-root global/pivot compatibility remain unchanged.

Root-boundary admission for this absolute start is metadata-only. Component walking still checks each required directory search permission with the admitted Snapshot and search generations. Stat of root itself therefore succeeds with mode000 while unprivileged child lookup fails. The internal root_anchor_start marker requires a root-only view at consumption; cwd/dirfd views retain their existing directory search checks. Root ownership retires outside path/publication locks, including serialized open. User syscall and wire layouts are unchanged.

Two new TMPFS/XFS fixtures cover32 backend/credential/policy/process-or-thread combinations. They rename a real prepared root, install different data at its old name, and verify public stat/read/open still select the retained object through direct paths, absolute symlinks and dot-dot. They also verify serialized lookup, readonly creation denial, readonly/nosuid File policy, root metadata, child search refusal, unchanged denied output, canonical owner text preservation and balanced mount pins.16 transient root overmounts must return EAGAIN without changing output. Existing cwd/root identity and view/membership fixtures remain. No pre-fix runtime baseline is claimed.

Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), cache-disabled formatting/clang-tidy and eight focused source suites pass; all104 credential checks are invoked. Initial builds passed, then review added typed-root mount validation and the overmount fixture; final builds use that final source. Full KTEST passes54076/0 and powers off; launcher exit0 is confirmed. Both exact owned identities are gone and normal/isolated disks are free at 2026-09-14T20:25:25.398504+00:00. Evidence: build/goal19-owned-root-absolute-walk/validation.json, source.patch, changes.patch, patch-contract.md, reconnaissance.md, review.md and serial-vm0.log.

Goal19 remains active with11 enforced/10 pending and0 legacy raw-root checks; the completion gate still reports pending exec. No production root-install API is enabled. Rooted relative lookup/descriptor-view intersection, direct getcwd/realpath/chdir visible metadata, PROCFS roots, generic/post-pivot covers, renamed mount topology, installation serialization and negotiated remote grants remain pending. These new Task-root fixtures use same-mount root/cwd; existing mixed-native view fixtures do not substitute for new mixed Task-root qualification. The original remote cwd/readonly failures remain unresolved. No separate normal/multi-node fixture or pending peer retry ran. The complete objective and appended --no-setup authorization are preserved.


### 9JM — Relative native lookup intersects owned process and directory roots

Ordinary native lookup now captures the owned process root for relative requests as well as absolute paths. Cwd compatibility pairing is recorded before recovering the current root name, so rename no longer discards the retained cwd. Existing cwd/descriptor views are intersected with the process root under publication: names choose the proof direction, then exact native ancestry proves the root relation and directory membership. A request-local DirectoryView retains the narrower root and unions both sources' readonly/nosuid policy without changing the original descriptor view.

Ordinary native dirfds receive independent process-root membership and search admission before child lookup. Outside-root descriptors fail with EPERM; incomplete native descriptors fail with EBADF before mount/inode access. Remote and unsupported owned-root routes refuse without ordinary text fallback. Retained File/mount consistency is checked before native casts. Absolute symlinks reset to the effective retained boundary. The renamed internal root_identity_view marker describes metadata-only process-root boundary admission; actual traversed components and relative directories retain their search checks. Source root/cwd anchors and directory Files outlive publication, so replacing merged descriptions cannot perform a final native File close under that lock. Syscall/wire layouts and null-root/explicit physical-owner behavior remain unchanged.

Two new TMPFS/XFS fixtures add32 backend/caller/policy/process-or-thread combinations. They test cwd and ordinary, broader, narrower, outside and incomplete directory descriptors after root rename and old-name replacement. Public stat/open/read and serialized lookup preserve selected data and combined policy. Narrower descriptors refuse traversal beyond their boundary; broader ones are confined to the process root. Readonly creation, unchanged denied output, original descriptor-view identity and relative cwd access below a mode000 ancestor are checked. Exact root/cwd/base pin counts and successful unmount/removal prove teardown. The existing absolute controls remain.

Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), cache-disabled formatting/clang-tidy and eight focused source suites pass; all105 credential checks are invoked. The initial kernel passes54078/0 and powers off. Review then added explicit incomplete-descriptor validation and stricter fixture cleanup; its rebuilt final kernel separately passes54078/0 and powers off. Both launchers exit0. The initial run’s two recorded process identities are gone. The final launcher exited0 after guest poweroff; the final VM PID is recorded in its launcher log, but its start ticks were not captured before exit. An authoritative host process audit finds no WOS VM and no normal/isolated disk holders at 2026-09-14T21:12:43.935812+00:00. Initial kernel/source/run evidence is retained under initial-run. Final evidence: build/goal19-owned-root-relative-walk/validation.json, source.patch, changes.patch, patch-contract.md, reconnaissance.md, review.md and serial-vm0.log.

Goal19 remains active with11 enforced/10 pending and0 legacy raw-root checks; the completion gate still reports pending exec. No production root installation is enabled. Direct getcwd/realpath/chdir visible-root metadata, PROCFS roots, mixed Task-root qualification, generic/post-pivot covers, renamed mount topology, installation serialization and negotiated remote grants remain pending. Original remote cwd/readonly failures are unresolved. No separate normal/multi-node fixture or pending peer retry ran. The complete objective and appended --no-setup authorization remain intact.


### 9JN — Getcwd recovers names beneath the owned process root

Getcwd now captures the canonical owner's root anchor with cwd/root text and cwd anchor, and validates both retained typed objects under namespace publication. Exact native ancestry produces the process-visible cwd name directly, so renaming the root and replacing its old name cannot select a different directory or cause stale-prefix EXDEV. A narrower cwd view still reports its location within the process root. Name recovery remains metadata-only: it does not renew search permission on the root, ancestors or cwd.

A requested cwd view is intersected with the owned process root using native membership proof and retains the narrower root with the union of readonly/nosuid policy. Existing source views are unchanged. Root/cwd anchors, membership edges and candidate output storage outlive the publication guard; final output replacement occurs after unlock. Both root and cwd references and compatibility text are checked before staged output is published. Legacy null-Snapshot callers that may already hold publication refuse owned-root capture before retaining either anchor (existing mixed text-only EXDEV remains). This explicit caller migration remains necessary before production root installation; null-root and remote cwd behavior remain unchanged. Syscall/wire layouts are unchanged.

Two new TMPFS/XFS fixtures cover32 backend/root-or-user/policy/process-or-thread combinations after root rename and fake old-name replacement. Each exercises ordinary text recovery, short buffers, broader/equal/narrower source views, union policy, mode000 root/cwd metadata, unchanged denied text/length/view, root withdrawal during recovery and no-Snapshot refusal without temporary retention under publication. Existing mixed-native cwd fixtures additionally install actual owned process roots for text/view/short-buffer/mode000 cases across both mount directions and two directory depths, then restore their original null-root coverage. File reference counts and mount cleanup remain checked. No pre-fix runtime baseline is claimed.

Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), cache-disabled format/clang-tidy and eight source suites pass; all106 credential source checks are invoked. Full isolated KTEST passes54080/0, powers off and its launcher exits0. Both recorded exact process identities are gone and the authoritative host audit finds no WOS VM or normal/isolated disk holder at 2026-09-14T21:42:21.897046+00:00. Evidence: build/goal19-owned-root-getcwd/validation.json, changes.patch, source.patch, patch-contract.md, reconnaissance.md, review.md and serial-vm0.log.

Goal19 remains active with11 enforced/10 pending and0 documented legacy root checks; the completion gate still reports pending exec. Realpath/chdir/PROCFS root metadata, serialized caller migration, installation serialization, generic/post-pivot covers, renamed mount topology and negotiated remote grants remain pending. These tests qualify owned-root getcwd across native mounts, not all mixed-root path operations. Original remote cwd/readonly failures remain unresolved. No normal/multi-node fixture or pending peer retry ran. The original objective and --no-setup live-debug authorization remain intact.


### 9JO — Realpath and chdir format names beneath the retained process root

Native realpath and chdir now pair each admitted lookup with a request-local root snapshot. Root text and anchor are captured together under the canonical owner's path lock, then matched to the lookup's root hash and identity. The retained root's current native name is recovered under consumer publication; prefix conversion only formats the already-authorized selected object. A narrower descriptor/cwd view cannot substitute for the process-visible root. Public realpath reaches native acquisition before legacy text preflight when a process owns its root, and native decline cannot fall back through that branch.

Owned-root chdir stages its visible name before entering the final Task path-lock publication section. Existing root/cwd hashes, anchor identities and search-epoch checks remain before cwd replacement. Root snapshots and rejected/adopted cwd references retire outside path/publication locks, including retries. Null-root realpath retains strict prefix behavior; null-root chdir keeps its original compatibility conversion. Remote realpath RPCs still run outside publication. The getcwd API comment now explicitly describes owned-root Snapshot/context requirements. Syscall and wire layouts remain unchanged.

Two new TMPFS/XFS fixtures cover32 backend/root-or-user/policy/process-or-thread combinations after root rename and fake old-name replacement. They exercise absolute/relative/dot/symlink native realpath, root metadata without renewed search, missing paths, short-buffer unchanged output, chdir root/current/parent transitions with getcwd roundtrips, target-search denial, fchdir narrower views, readonly/nosuid inheritance, outside-root descriptor refusal and stale-root snapshot capture. Denied cwd/length/anchor changes are checked, original cwd restored and final File reference/mount cleanup preserved. Synthetic Task tests directly invoke native realpath and chdir/fchdir; the public realpath dispatch branch has source checks, not a separate current-task runtime fixture. No pre-fix runtime baseline is claimed.

Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), cache-disabled format/clang-tidy and eight source suites pass; all107 credential source checks are invoked. Full isolated KTEST passes54082/0, powers off and its launcher exits0. Both exact recorded process identities are gone and the authoritative host audit finds no WOS VM or normal/isolated disk holder at 2026-09-14T21:58:15.474470+00:00. Evidence: build/goal19-owned-root-visible-path/validation.json, changes.patch, source.patch, patch-contract.md, reconnaissance.md, review.md and serial-vm0.log.

Goal19 remains active with11 enforced/10 pending and0 documented legacy root checks; the completion gate still reports pending exec. PROCFS root handling, serialized caller migration, installation serialization, complete mixed-root operation qualification, generic/post-pivot covers, renamed mount topology and negotiated remote grants remain pending. Original remote cwd/readonly failures remain unresolved. No production root installation, normal/multi-node fixture or pending peer retry ran. The original objective and --no-setup live-debug authorization remain intact.


### 9JP — PROCFS root and scoped descriptor links retain subject confinement

PROCFS root links now use an explicit metadata-only VFS query. It captures canonical root text and ownership together, recovers the current native root name, and checks the captured context before producing physical root text or a retained root-only view. Renaming the root and replacing its old pathname cannot redirect the view or retain the old spelling. Legacy null-Snapshot root queries refuse owned-root capture before borrowing references; value-only root copying remains available to existing locked display/text callers.

Directory-descriptor links with existing retained views now intersect them with the subject's owned root before handoff. Current names choose the direction, then exact native ancestry proves the root relation and directory membership. The narrower boundary survives with the union of readonly/nosuid policy; the original descriptor view is unchanged. Metadata recovery does not renew root/cwd/ancestor search permission. Incoming native lookup still admits its own search. All root/source/candidate/membership ownership outlives publication, and output replacement happens after unlocking. PROCFS private-access authorization remains before work, with credential-generation revalidation before text/view/generation publication. Syscall and wire layouts remain unchanged.

Two new TMPFS/XFS fixtures cover32 backend/root-or-user/policy/process-or-thread combinations after root rename and fake old-name replacement. They verify root text/truncation, exact root-only view and scoped selected inode, cross-process private denial, mode000 metadata, wide/narrow/outside descriptor views, policy union, unchanged source views, and root withdrawal during root/view recovery with unchanged outputs. Existing mixed-native owned-root cases now also exercise root recovery and view intersection across both mount directions and directory depths. Exact File reference and mount cleanup checks remain. Synthetic Task tests invoke PROCFS link APIs and native scoped lookup; no separate public current-task/multi-node runtime claim is made.

Initial diagnostic compilation caught a test-helper declaration mismatch; review also corrected fixture caller IDs to use canonical process IDs and a distinct PID for private denial. These fixture corrections preceded runtime. Final Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), cache-disabled format/clang-tidy and eight source suites pass; all108 credential source checks are invoked. Full isolated KTEST passes54084/0, powers off and its launcher exits0. Both exact recorded process identities are gone and the authoritative host audit finds no WOS VM or normal/isolated disk holder at 2026-09-14T22:20:46.623577+00:00. Evidence: build/goal19-procfs-owned-root/validation.json, changes.patch, source.patch, patch-contract.md, reconnaissance.md, review.md and serial-vm0.log; initial build/tidy logs are retained separately.

Goal19 remains active with11 enforced/10 pending and0 documented legacy root checks; the completion gate still reports pending exec. Ordinary descriptors without retained views, generic FD text and mount-display root recovery, serialized caller migration, installation serialization, full mixed-root operation qualification, generic/post-pivot covers, renamed mount topology and negotiated remote grants remain pending. Original remote cwd/readonly failures remain unresolved. No production root installation, normal/multi-node fixture or pending peer retry ran. The complete original objective and --no-setup live-debug authorization remain intact.


### 9JQ — Ordinary native PROCFS directory descriptors retain exact identity

PROCFS directory links now obtain a subject-scoped view even when the original TMPFS/XFS File has no DirectoryView. The VFS helper captures the canonical owned root, proves exact native membership, and creates a separate metadata-only pin from an empty-path lookup of the retained File inode. It never reopens the saved pathname. Existing scoped Files use the prior root intersection; subjects without owned roots retain legacy text behavior. Outside, malformed and unsupported owned-root descriptors fail without text fallback.

Source File position, flags, policy and view are unchanged. Native mount flags remain per selected mount; inherited root policy becomes the new view policy. Metadata recovery does not renew search permission, while subsequent lookup admits search normally. Root consistency and namespace/mount/membership checks precede output replacement. Root, pin, candidate and cross-mount membership ownership retire outside publication/path locks. PROCFS private authorization and credential-generation revalidation still bracket capture/publication, including failures. No syscall or wire layout changes, and no production root installation.

Two new TMPFS/XFS selftests exercise32 backend/root-or-user/policy/process-or-thread combinations. Descriptors opened before root rename select the original inode after a fake old-path replacement. Cases cover inside/outside/malformed/unsupported Files, private denial, mode000 metadata versus search, root withdrawal with unchanged outputs, exact source state/reference stability, and use of the returned view after the original File closes. Mixed-native cases cover both mount directions and directory depths, matching inode/device across original File close and preserving reference/unmount cleanup.

Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), cache-disabled format/clang-tidy and eight source suites pass; all109 credential source checks are invoked. Full isolated KTEST passes54086/0, powers off and its launcher exits0. Both recorded process identities are gone; the authoritative host audit finds no WOS VM or normal/isolated disk holder at 2026-09-14T22:44:58.956449+00:00. Evidence: build/goal19-procfs-plain-directory/validation.json, changes.patch, source.patch, patch-contract.md, reconnaissance.md, review.md and serial-vm0.log. Build sessions were recovered and polled after context compaction, without duplicate builds.

Goal19 remains active with11 enforced/10 pending and0 documented legacy root checks; the completion gate still reports pending exec. Generic/remote descriptor text, mount-display root recovery, serialized caller migration, root installation serialization, broader mixed-root qualification, generic/post-pivot covers, renamed mount topology and negotiated remote grants remain pending. Original remote cwd/readonly failures remain unresolved. No normal/multi-node fixture or specifically rejected peer retry ran. The complete original objective and --no-setup live-debug authorization remain intact.


### 9JR — Text-only PROCFS directory links recover retained native names

Text-only PROCFS directory FD links under an owned root now recover the current name from the retained native directory inode and exact process root. A narrower descriptor view is validated but does not replace the process-visible root. Ordinary TMPFS/XFS and existing scoped Files share this metadata path; no saved pathname is reopened and no new File pin is allocated. Unsupported/outside/malformed owned-root descriptors refuse without saved-path fallback. Null-root descriptors explicitly retain legacy dispatch.

Canonical root text/ref are captured together and checked before output/length publication. Source-view and process-root membership run under namespace publication with outer reference/proof retirement. Metadata recovery does not renew search permission or change source position, flags, policy, view or File references. Legacy null-Snapshot callers refuse owned-root borrowing before acquiring references, including callers already holding publication. PROCFS private authorization and credential-generation revalidation remain around the operation; readlink truncation is preserved. No syscall/wire layout change or production root installation.

Two new TMPFS/XFS selftests add32 backend/root-or-user/policy/process-or-thread profiles. They query descriptors opened before root rename, then rename the directory itself and require the new text; full, short and zero-capacity readlink outputs are checked. Cases include narrower descriptor views with process-relative names, outside/malformed/unsupported descriptors, private denial, mode000 metadata, root withdrawal with untouched outputs, and null-Snapshot refusal under publication with unchanged references. Mixed-native cases verify both mount directions and depths, including internal short-buffer unchanged text/length. Existing exact File/mount cleanup and view source-close tests remain.

Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), cache-disabled format/clang-tidy and eight source suites pass; all110 credential source checks are invoked. Full isolated KTEST passes54088/0, powers off and its launcher exits0. Both recorded process identities are gone and the authoritative audit finds no WOS VM or normal/isolated disk holder at 2026-09-14T23:01:09.915555+00:00. Evidence: build/goal19-procfs-directory-text/validation.json, changes.patch, source.patch, patch-contract.md, reconnaissance.md, review.md and serial-vm0.log.

Goal19 remains active with11 enforced/10 pending and0 documented legacy root checks; the completion gate still reports pending exec. Generic regular-file/remote names, mount-display root recovery, serialized caller migration, root installation serialization, broader mixed-root operations, generic/post-pivot covers, renamed mount topology and negotiated remote grants remain pending. Original remote cwd/readonly failures are unresolved. No normal/multi-node fixture or specifically rejected peer retry ran. The original objective and --no-setup authorization remain intact.


### 9JS — Mount reads and seeks observe the current caller root

PROCFS mount reads now build a fresh staged listing for the explicit caller instead of using the descriptor's first-reader content cache. A request-local FilesystemRootMetadata owns only the canonical root anchor, recovers its current native name and captures namespace/mount generations. Snapshot iteration remains outside publication and content locks, including pivot waits; final validation checks root owner/text/identity and both generations before public output. Failed reads leave output and existing cached content unchanged. Root metadata is not a path grant and installs no root.

SEEK_END now measures the current caller's listing before taking the cursor/content lock; SET/CUR behavior remains, and failed seeks preserve the cursor. Production read/seek wrappers pass their current Task/PID and one credential Snapshot into the explicit mount APIs. PUBLIC_READ policy remains public; unbound filesystem identity fails ESRCH. No root/cwd ownership is stored in ProcFileData. Same mount row order, six fields, bounded4096-byte staging and outside-root physical visibility are preserved. In-root readonly/nosuid display includes inherited owned-root policy alongside native flags. No syscall or wire layout change.

Two new TMPFS/XFS fixtures cover32 backend/root-or-user/policy/process-or-thread profiles. A child mount installed after root rename must display /cwd for the owned reader and its physical name for another reader sharing the same File. A deliberately seeded old cache stays unchanged and is never served. Cases cover policy flags, full/offset/EOF/zero reads, END/CUR/SET and failed seeks, invalid buffers, unbound identity, mode000 metadata, root withdrawal during capture and after row generation, namespace invalidation, canonical owner matching and mount-table invalidation after unmount. Exact root/cwd references and parent mount File accounting are checked. Synthetic Task APIs run live in KTEST; production fops caller binding is source-checked.

Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), cache-disabled format/clang-tidy and eight source suites pass; all111 credential source checks are invoked. Full isolated KTEST passes54090/0, powers off and its launcher exits0. Both exact recorded process identities are gone, and the authoritative audit finds no WOS VM or normal/isolated disk holder at 2026-09-14T23:25:16.162183+00:00. Evidence: build/goal19-procfs-mount-root/validation.json, changes.patch, source.patch, patch-contract.md, reconnaissance.md, review.md and serial-vm0.log.

Goal19 remains active with11 enforced/10 pending and0 documented legacy root checks; the completion gate still reports pending exec. Existing child mounts renamed with their parent, hidden/generic/post-pivot attachments, generic regular-file/remote names, serialized caller migration, root installation serialization, broader mixed-root operations and negotiated remote grants remain pending. Original remote cwd/readonly failures remain unresolved. No normal/multi-node fixture or specifically rejected peer retry ran. The original objective and --no-setup authorization remain intact.


### 9JT — Owner directory PINs retain complete root/cwd views

Owner directory-reference PINs now carry an independent native DirectoryView with the exact retained export root and directory. Derived PINs reuse that view and inherited policy; PATH recovers the name relative to its retained root. Renaming/replacing the original export name no longer selects a new boundary for these two operations. Initial PINs capture the export boundary once. Ordinary owner OPEN sources receive a detached directory identity and complete view before PIN publication.

A new vfs_pin_directory_view_as helper uses one explicit Snapshot for scoped lookup, rebase and final search admission. The returned search-only File owns a different detached cwd File, avoiding File/view cycles and parent chains. Its effective readonly/nosuid flags remain separate from the view's inherited policy and native-only identity flags. No task filesystem or credentials are installed. Exact channel-generation/token checks, existing v1 wire layout and purpose-scoped cancellation remain unchanged; these are still client-chosen channel-local PIN tokens, not transferable owner-issued grants.

Ordinary OPEN sources do not require renewed ancestor search merely to retain the export boundary: the bare-pin API permits identity-only pinning solely for that exact boundary. Current cwd search is still required. Failed bare-pin attachments now drain their outer File owner after namespace publication unlock. Denied directory-reference operations leave response/name and source activity unchanged; successful refresh verifies the row still holds the same retained File. Final File/view release remains outside registry, path and publication locks.

The extended TMPFS/XFS owner fixture covers empty export-root wire paths, ordinary OPEN source closure, mode000 export-root metadata retention with admitted cwd search, stale channel/token cleanup, cwd and export-root rename/replacement, denied search without activity refresh, inherited readonly/nosuid and readonly create denial, detached acyclic identities, and exact three-native-File retirement accounting. The first runtime attempt was deliberately stopped after source review found a fixture reading its reservation ID after publication had cleared it; that incomplete run and exact cleanup are retained separately. The following completed run exposed an empty-path ordinary-OPEN conversion error (54089/1); the owner now uses dot, as required by the native pathname API. Its failed log and exact cleanup remain under empty-path-failure. The final corrected run is the verification evidence.

Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), cache-disabled format/clang-tidy, and eight source suites pass; all 112 credential source checks are invoked. Full corrected isolated KTEST passes 54090/0, powers off and its launcher exits 0. Both recorded process identities are gone and no WOS VM or normal/isolated disk holder remains at 2026-09-15T00:08:59.733984+00:00. Evidence: build/goal19-wki-directory-view/validation.json, changes.patch, source.patch, patch-contract.md, reconnaissance.md, review.md and serial-vm0.log.

Goal19 remains active: 11 enforced, 10 pending, 0 documented legacy root checks; completion still fails at pending exec. Other owner fd-relative operations retain historical export-text checks, so broader export-root rename support is not claimed. Negotiated owner-issued grants, independent receiver retention/adoption before TASK_ACCEPT, client readonly/nosuid transfer on return to owner, third-owner placement and transfer cleanup remain outstanding. Original remote cwd/readonly failures remain unresolved. No normal/multi-node fixture or previously rejected peer retry ran. The original objective and --no-setup authorization remain intact.


### 9JU — Owner-issued directory grants with independent adoption

Added separately negotiated VFS directory grant operation 0x0420 and feature1024. ISSUE creates a distinct, independently owned search-only PIN with an owner-issued non-reused transfer ID, exact owner resource incarnation, source and recipient boot identities, submission transaction and canonical mapped credential snapshot. ADOPT rechecks current search and the exact live source binding, retains independent native scope and claims only once. Remote adoption atomically publishes the destination PIN with the claim. Native adoption returns a complete view without installing Task state. Readonly/nosuid accumulate, including source client mount restrictions; derived client directory PINs retain inherited policy. Grant rows are bounded per consumer and serial allocation saturates.

All RX/deferred admission, authorization classification and exact response checks include the new operation. Existing directory v1 and TaskSubmit layouts remain unchanged. Both client operations reserve durable v1 cancellation before sending and never replay ISSUE/ADOPT. Failed or lost replies clean up by the locally reserved cancellation token. A successful claim followed by transport failure requires a fresh grant; it is not represented as an authorization denial. Native work, allocation and final reference release stay outside registry locks; server and VFS locks are not nested. No long-lived binding reference is stored on a grant row.

Verification: Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), cache-disabled format/clang-tidy and all eight host suites pass; all113 credential source checks are invoked. Full isolated KTEST --no-setup passes 54203/0, powers off and its launcher exits0. New production-helper TMPFS/XFS fixture covers exact identity/credential/recipient/transaction checks, current-search denial without claim/output/activity changes, original source closure and export rename/replacement, one-time native and remote adoption, policy survival after cancellation, cancellation between retention and publication, bounded grant capacity and serial exhaustion, and a writable positive control. Wire tests validate every identity byte, request/reply shape and authorization classification. The fixture's root-GID setup was corrected before VM execution; no failed runtime is hidden. Recorded launcher and VM identities are gone and all WOS disks are free at 2026-09-15T00:55:17.863759+00:00. Evidence: build/goal19-directory-transfer-grant/validation.json, source.patch, changes.patch, patch-contract.md, review.md and serial-vm0.log.

Goal19 remains active at11 enforced/10 pending/0 documented legacy root checks, with completion still failing at pending exec. This increment does not integrate grants into compute submission or qualify a live multi-node client round trip. Remaining work includes both encoders and fragment validators, authenticated compute source boot and submission matching, receiver preparation before TASK_ACCEPT, native-source issuance, third-owner root representation, cleanup/rollback and placement qualification. Original remote cwd/readonly failures remain unresolved. No normal cluster or previously rejected peer fixture retry was run. The original objective and --no-setup authorization remain intact.


### 9JV — Compute work retains its admitted peer identity

Compute submission now captures source boot, authentication generation/session identifier, policy and negotiated capabilities together under peer lifecycle admission. Normal submissions retain this value through deferred construction. Session/group origins and signal-home ownership use the admitted boot instead of querying a mutable peer after ELF fetch. Fixed deferred RX slots retain the same value; workers validate it before completion/cancel processing, mapped user/group signals, fragment handling and reply retry. Mapping consumes the captured policy and boot.

Partial submissions retain their first compute session and identity. Later fragments must match, and completed assembly queues that retained admission rather than calling the normal handler to recapture policy. Full admission checks include both identity and session key before queue entry and receiver publication. Existing running/completion cleanup keys retain their prior semantics and cannot independently satisfy the new admission predicate. Same channel/generation cannot silently adopt a replacement boot/auth identity. Existing channel retirement and payload cleanup stay intact; no wire/syscall/TaskSubmit layout or compute feature advertisement changes.

Validation: Normal Build WOS (RelWithDebInfo), isolated diagnostic Debug build and cache-disabled format/clang-tidy pass. Ten host suites pass; all114 credential checks are invoked. The additional exec source suite exposed a stale guard from the earlier vfs_openat wrapper refactor; it now follows the wrapper to open_at_for_task and verifies the same credential consumption. No VFS implementation changed. The initial diagnostic fixture used an invalid peer-state enum name, corrected before runtime. Full isolated KTEST --no-setup passes 54204/0, powers off, and its launcher exits0. The new production-helper fixture checks boot/auth-session/policy/capability/state replacement, immutable origin resolution, full admission versus cleanup keys, partial-fragment rejection without append, exact-identity continuation and retirement cleanup. Both recorded processes are gone and all WOS disks are free at 2026-09-15T01:17:44.057836+00:00. Evidence: build/goal19-compute-admitted-identity/validation.json, source.patch, changes.patch, patch-contract.md, review.md and serial-vm0.log.

Goal19 remains active:11 enforced/10 pending/0 documented legacy root checks; completion still fails at pending exec. This fixes an identity prerequisite for directory transfer, not the remaining remote-exec filesystem integration. Required work includes native-source grant issuance, both encoders and root/cwd wire validation, durable staging ownership across abandoned waits, receiver filesystem installation before TASK_ACCEPT, third-owner roots and live placement qualification. Original remote cwd/readonly failures remain unresolved. No normal cluster or rejected peer fixture retry ran. The original objective and --no-setup authorization remain intact.


### 9JW — Native owners issue retained directory grants

Native directory owners can now issue the existing transfer grant using an exact export resource and a complete retained DirectoryView. Issuance keeps one credential Snapshot, checks the backing mount and native root membership, narrows readonly/nosuid policy, and requires current cwd search before publication. A separate bounded local registry stores the native grant; the returned close-only kernel File owns cancellation and cannot read, write, enumerate, or enter a user FD table. Native grants share the non-reused owner ID sequence with remote grants.

Adoption validates the live native export incarnation and publication revision instead of requiring a remote source binding. Remote-source admission retains its existing binding checks. Native and remote recipients independently retain the directory scope before an atomic one-time claim. Closing the staging File cancels unclaimed adoption and drains native references after registry unlock; an already adopted destination remains independently owned. Existing wire layouts, syscall ABI, compute advertisement and Task filesystem state are unchanged.

Validation: Normal Build WOS (RelWithDebInfo), isolated Debug build, cache-disabled format/clang-tidy and ten host source suites pass; all115 credential checks are invoked. Full isolated KTEST --no-setup passes 54205/0 and powers off; its launcher exits0. The new TMPFS/XFS production-helper fixture covers native and remote recipients, resource withdrawal, current-search denial, replay rejection, policy retention, source view/staging closure, rename and replacement, cancellation between retention and claim, bounded capacity and a writable positive control. Both recorded process identities are gone and all WOS disks are free at 2026-09-15T01:44:29.539612+00:00. Evidence: build/goal19-native-directory-grant/validation.json, source.patch, changes.patch, patch-contract.md, review.md and serial-vm0.log.

Goal19 remains active with11 enforced/10 pending/0 documented legacy root checks; completion still fails at pending exec. Native issuance is now available, but compute must still connect durable staging ownership, both encoders and root/cwd wire validation, receiver preparation and filesystem installation before TASK_ACCEPT, and third-owner roots. Original remote cwd/readonly failures and live placement qualification remain unresolved. No normal cluster or previously rejected peer fixture retry ran. The original goal string and --no-setup authorization remain intact.


### 9JX — Abandoned grant RPCs retain cleanup ownership

Directory grant RPC preparation now registers its private File, context and proxy reference by executor PID before the feature-query and ISSUE/ADOPT waits. The existing task-exit path first quiesces proxy waiters, then removes the exact PID's preparations. Cleanup transfers the matching owned directory token to the existing deferred cancellation outbox, deletes private allocations and releases the preparation's proxy reference outside the registry lock. The outbox independently retains its proxy until acknowledged cancellation or confirmed exact binding retirement. No cancellation network wait or grant replay occurs during preparation cleanup.

Successful RPCs transfer the File, context, proxy reference and owned cancellation token together before caller output publication. Failure errno and staged outputs remain unchanged. Registration/removal require no registry allocation; current immutable Snapshot, exact channel/resource/grant checks, syscall/wire layouts and compute advertisement remain intact. This fixes the issuing RPC's abandoned-stack lifetime, without installing Task filesystem state.

Validation: Normal Build WOS (RelWithDebInfo), final isolated Debug build, cache-disabled format/clang-tidy and ten host source suites pass; all116 credential checks are invoked. The first Debug build caught a missing namespace qualifier in the new KTEST registration, corrected before runtime with the failed diagnostic retained. The first runtime exposed a fixture expectation error at final File close: confirmed retirement returns ENOTCONN while releasing ownership. That failed run is retained under close-errno-failure; the corrected fixture preserves the production errno and still requires exact reference/outbox cleanup. Full corrected isolated KTEST --no-setup passes 54206/0 and powers off; its launcher exits0. The production-helper fixture verifies cleanup before token reservation, nested preparations, exact PID isolation, independent outbox proxy references, repeated cleanup, failed proxy acquisition, successful ownership transfer, and ordinary File close after confirmed binding retirement. Both exact process identities are gone and all WOS disks are free at 2026-09-15T02:15:15.280279+00:00. Evidence: build/goal19-directory-rpc-lifetime/validation.json, source.patch, changes.patch, patch-contract.md, review.md and serial-vm0.log.

Goal19 remains active at11 enforced/10 pending/0 documented legacy root checks, with completion failing at pending exec. Compute still needs durable ownership of returned grant Files, both encoders and root/cwd wire validation, receiver filesystem installation before TASK_ACCEPT and third-owner roots. The abandoned-wait audit must also cover other directory/exec PIN constructors and synchronous final directory File cancellation. Original remote cwd/readonly failures and live placement qualification remain unresolved. No normal cluster or rejected peer hierarchy retry ran. The original goal string and --no-setup authorization remain intact.


### 9JY — Directory and executable PINs share durable preparation

The existing directory-PIN and executable-view constructors now share PinPreparation with grant RPCs. It records explicit directory/executable cancellation purpose and links private File/context ownership to executor cleanup before interruptible waits. Directory pinning releases its temporary selected-lane lifecycle reference once preparation owns an open reference, before the feature query and PIN request. Failure queues the known directory token without another network wait and preserves the original errno and staged outputs.

Executable cloning preserves its resource-support, allocation, content-ID and proxy-admission order. Its private File owns ExecReadView metadata before waiting. Normal failure still performs synchronous token cancellation while preparation remains linked, preserving cancellation-error precedence. Abandoned cleanup queues only the exact OWNED proxy/token/purpose; already pending cancellation retains its independent outbox ownership. Success transfers the complete File/context/proxy/token together. No wire/syscall layout, source authority, readonly/nosuid inheritance, published File close, compute feature or Task filesystem state changes.

Validation: Normal Build WOS (RelWithDebInfo), isolated Debug build, cache-disabled format/clang-tidy and ten host source suites pass; all117 credential checks are invoked. Full isolated KTEST --no-setup passes 54207/0 and powers off; its launcher exits0. The shared-purpose fixture verifies identical token values across directory and executable cancellation records, exact PID isolation, private metadata ownership, already-pending cancellation, wrong-purpose refusal and successful executable File handoff/close with preserved ENOTCONN after confirmed retirement. Existing directory preparation and native owner grant fixtures also pass. Both exact process identities are gone and all WOS disks are free at 2026-09-15T02:33:27.083336+00:00. Evidence: build/goal19-pin-rpc-lifetime/validation.json, source.patch, changes.patch, patch-contract.md, review.md and serial-vm0.log.

Goal19 remains active:11 enforced/10 pending/0 documented legacy root checks; completion still fails at pending exec. Published retained-File close needs abandoned-close ownership while preserving errno. Compute still needs durable staging, both encoders/validators, receiver filesystem installation before TASK_ACCEPT, third-owner roots and placement evidence. Surrounding generic VFS caller frames are not claimed covered by the PIN constructor fix. Original remote cwd/readonly failures remain unresolved. No normal cluster or rejected peer hierarchy retry ran. The original goal string and --no-setup authorization remain intact.


### 9JZ — Retained-pin close keeps outer File ownership through cancellation

Immutable directory/executable pin close now links its consumed File to executor cleanup before synchronous cancellation, without holding the per-file I/O mutex across that wait. Normal return preserves its existing errno and releases backend state before VFS finalization. If task exit abandons the close stack, cleanup quiesces proxy waiters, detaches the exact PID owner, queues only the matching OWNED proxy/token/purpose, then releases caches/context/proxy and the outer path/mount/File outside the registry lock. Already pending cancellation retains its independent outbox ownership. A shared VFS post-backend finalizer preserves the original metadata, error, path and mount-release ordering.

The specialized path requires immutable access, no create/truncate or pending writebehind, and no native DirectoryView ownership. General writable close retains flush/completion/error precedence. No userspace ABI, wire layout, credentials, readonly/nosuid policy, compute feature or Task filesystem publication changes.

Validation: Normal Build WOS (RelWithDebInfo), isolated Debug build, cache-disabled format/clang-tidy and ten host suites pass, with all118 credential checks invoked. Full isolated KTEST --no-setup passes 54208/0 and powers off; launcher exit0. The new production-helper fixture checks directory/executable same-value tokens, exact PID isolation, repeated cleanup, pending cancellation, heap path/prefetch/cache ownership, mount open-file and proxy accounting, writable/truncate/pending-write exclusions and actual VFS close with preserved ENOTCONN/EIO. The initial isolated build failed on a test-only nonexistent O_RDWR name; its corrected ABI access-mode value is included in the passing builds. Both exact process identities are gone and all WOS disks are free at 2026-09-15T03:03:31.055529+00:00. Evidence: build/goal19-pin-close-lifetime/validation.json, source.patch, changes.patch, patch-contract.md, review.md and serial-vm0.log.

Goal19 remains active:11 enforced/10 pending/0 documented legacy root checks; completion still fails at pending exec. Compute still needs durable grant staging/transaction reservation, both encoders/validators, receiver root/cwd installation before TASK_ACCEPT, third-owner roots and placement evidence. Generic writable close and other surrounding VFS frame ownership are not claimed covered. Original remote cwd/readonly failures remain unresolved. No normal cluster or rejected peer hierarchy retry ran. The exact original goal string and --no-setup authorization remain intact.


### 9KA — Compute submission owns filesystem preparation and transaction

Inline and VFS_REF submitters now register TaskFilesystemPreparation with their executor before capture and waits. It owns one credential value, complete supplied/current filesystem anchors, root text and selected cwd text. Ordinary cwd uses fixed storage; longer explicit cwd uses checked owned storage. Passing the filesystem owner's cwd buffer uses the captured copy; an alternate explicit cwd cannot borrow that owner's unrelated anchor. Later source text changes cannot change the serialized preparation. Legacy encoding still refuses owned roots.

Preparation reserves a distinct saturating 64-bit transaction rather than reusing the wrapping 32-bit Task ID. Two bounded grant staging slots transfer File ownership only after local shape/transaction/target/boot checks; peer authority remains the responsibility of negotiated grant admission. Cancellation detaches one File at a time while remaining entries stay executor-discoverable, then releases cwd/root anchors and text. Conditional VFS task cleanup quiesces the interrupted proxy before pin close. Final remote DirectoryAnchor release deletes its surrounding allocation before putting the remote File, allowing 9JZ's retained-File continuation to complete abandoned cancellation without stranding the anchor. Native pin/mount/view order and normal writable close are preserved.

Validation: Normal Build WOS (RelWithDebInfo), isolated Debug, cache-disabled format/clang-tidy and ten host suites pass, with all 119 credential checks invoked. Full isolated KTEST --no-setup passes 54209/0 and powers off; launcher exit0. The new fixture covers transaction exhaustion, captured long paths, distinct preparations, wrong target/transaction preservation, unrelated/repeated task cleanup and reentrant close of three staged Files. Existing native/supplied filesystem tests exercise anchor retention and legacy root refusal. Both exact process identities are gone and all WOS disks are free at 2026-09-15T03:27:36.791135+00:00. Evidence: build/goal19-compute-filesystem-preparation/validation.json, source.patch, changes.patch, patch-contract.md, review.md and serial-vm0.log.

Goal19 remains active:11 enforced/10 pending/0 documented legacy root checks; completion still fails at pending exec. Grant bind/adopt storage is preparation infrastructure, not yet production grant issuance. The negotiated filesystem extension, both encoder/validator paths, authenticated receiver root/cwd installation before TASK_ACCEPT and third-owner scope remain next. Original remote cwd/readonly failures remain unresolved. No filesystem feature was advertised. No normal cluster or rejected peer hierarchy retry ran. The exact original goal string and --no-setup authorization remain intact.


### 9KB — Compute filesystem grants have one bounded wire contract

TaskFilesystemContext adds an optional packed 112-byte tail after the complete routed identity. The existing 20-byte TaskSubmit header and all credential/resource/job/signal offsets remain unchanged. The extension carries version/flags/reserved fields, a nonzero 64-bit transaction and bounded root/cwd grants. Absent grants must be canonical zero values; absent root selects global namespace and absent cwd selects that root. Present grants must share the transaction and exact source/target boot tuple.

Both production encoders consume optional prepared wire state when sizing, setting flags and copying the tail. Staging finalization requires matching File ownership; adoption and cancellation invalidate obsolete readiness. Shared decoding keeps all prior identity/resource checks, rejects mismatched flag/length combinations and canonical global-root/root-cwd inconsistencies, and publishes the filesystem output only on success. A caller unable to consume filesystem state receives UNSUPPORTED rather than dropping it. First-fragment and full queue admission use the same prefix agreement. The deferred receiver binds grants to captured source/local target boots, then explicitly declines before IPC/image work until actual object installation is implemented.

Validation: Normal Build WOS (RelWithDebInfo), isolated Debug, cache-disabled format/clang-tidy and ten host suites pass, with all 120 credential checks invoked. Full isolated KTEST --no-setup passes 54210/0 and powers off; launcher exit0. The new codec fixture round-trips every root/cwd grant combination through inline, VFS_REF and RESOURCE_REF layouts, preserves full credentials and signal-home data, rejects truncated suffixes and malformed flags/transaction/boot tuples, and preserves filesystem output on failure. Existing preparation cleanup checks wire readiness and cancellation. Both exact processes are gone and all WOS disks are free at 2026-09-15T03:52:11.285473+00:00. Evidence: build/goal19-compute-filesystem-wire/validation.json, source.patch, changes.patch, patch-contract.md, review.md and serial-vm0.log.

Goal 19 remains active:11 enforced/10 pending/0 documented legacy root checks; completion still fails at pending exec. Production source grant issuance/finalization, complete native/remote receiver views and root/cwd installation before TASK_ACCEPT remain required. The new feature is not advertised, and normal submission does not yet finalize grant-bearing wire state. Original remote cwd/readonly failures remain unresolved. No normal cluster or rejected peer hierarchy retry ran. The original goal string and --no-setup authorization remain intact.


### 9KC — Return-owner cwd grants reach production task installation

Inline and VFS_REF submission now borrow the exact retained remote cwd from their captured filesystem and issue a real owner grant when returning execution to that owner. The separate process-return-owner-cwd-v1 resource negotiates this implemented path. One captured credential value, nonreusing submission transaction, target boot, authenticated peer identity, channel generation and resource observation survive the ISSUE wait and are revalidated before submitted-row creation. The grant is adopted into durable staging and the existing filesystem wire suffix is finalized. A receiver without this capability gets ENOTSUP for this retained return-owner path. Canonical global-root serialization accepts harmless source buffer bytes after the NUL terminator.

The receiver maps full credentials before native grant adoption, prepares a cwd anchor from the retained complete view, and preserves global process-root intent. Current names come from identity, never old display-path reopening. The inherited readonly/nosuid view remains attached to relative lookup. An executor-linked preparation owns incoming native views/context before admission waits; private state is cancelled on denial. Peer lifecycle writers are held only for snapshot/revalidation because even native XFS storage can wait on remote block I/O. Both anchors, paths and lengths are swapped under one path lock before executable/interpreter loading and installed on the unpublished task before TASK_ACCEPT. Legacy cwd text cannot overwrite them; the worker's saved filesystem and hostname are restored on completion or policy denial.

Validation: Normal Build WOS (RelWithDebInfo), isolated Debug, cache-disabled format/clang-tidy and eleven host source suites pass, including all 121 credential audit checks. Final isolated KTEST --no-setup reports 54212/0 and powers off, launcher exit0. Two new native fixtures cover TMPFS/XFS, root/user snapshots, all readonly/nosuid combinations, rename plus old-name replacement, global absolute root versus retained relative lookup, worker/new-task installation, malformed-policy and current-search denials, and repeated executor cleanup. Initial diagnostic compilation needed a missing test include. A superseded first runtime candidate passed 54212/0; the separately archived final run includes the later receiver ownership fix and cleanup case. Exact final processes are gone and WOS disks are free at 2026-09-15T04:33:01.036606+00:00. Evidence: build/goal19-compute-return-owner-cwd/validation.json, source.patch, changes.patch, patch-contract.md, reconnaissance.md, review.md and serial-vm0.log.

Goal19 remains active: 11 enforced/10 pending/0 documented legacy root checks; first pending domain is exec. General filesystem capability remains unadvertised. Native-source issuance, owned-root transfer, third-owner import/installation and live multi-node placement qualification remain required. This isolated validation does not prove the original remote cwd/readonly matrices now pass. No normal cluster, penetration testing or rejected peer hierarchy retry ran. The exact original goal and --no-setup authorization remain intact.


### 9KD — Live root mismatch no longer hides retained cwd

The unchanged ordinary TMPFS/XFS cwd rename/exec matrix ran on two fresh private WOS nodes with the 9KC kernel: ten controls passed and all eight remote cases after rename failed, reading replacement data `fake` rather than retained `seed`. A repeat under a bounded source breakpoint reproduced the same results. All ten captured submissions had root `/rootfs`, no owned root, and a matching retained remote cwd anchor. WOSDBG verified both live kernel build IDs; the source GDB trace also matched the build ID and detached. Both fixtures cleaned up. This corrects any inference that the previous native global-root fixtures qualified normal userspace remote execution.

The borrowed-cwd accessor now reports the retained remote object independently of root-transfer support. Compute rejects an unsupported non-global or owned root with ENOTSUP before owner lookup, peer lease, target binding, grant ISSUE, legacy capture or wire publication. Both submitters propagate that error. No root is rewritten to `/`, and no wire/capability contract is broadened. Legacy submissions without a retained remote cwd and the supported global-root return-owner path retain their existing behavior. This closes silent pathname fallback for the observed unsupported-root case; successful root-aware transfer remains required.

Validation: Normal Build WOS (RelWithDebInfo), isolated Debug build, cache-disabled format/clang-tidy and eleven host suites pass; all 122 credential checks are invoked. Full isolated KTEST --no-setup passes 54213/0, powers off, and its launcher exits 0. The new production-helper regression covers /rootfs and another legacy root, owned-root variants, repeated refusal, unchanged credentials/output/caller anchors, no staging or target binding, mismatched labels and one final File close. The unchanged live matrix is pre-change failure evidence, not a post-change passing claim. The private cluster and final KTEST VMs and their launchers are gone; all WOS disks are free at 2026-09-15T05:05:55.898935+00:00. Attempts to sync the disposable cluster images through two assumed utility paths returned ENOENT before their launcher was stopped; those overlays will not be reused.

Evidence: build/goal19-return-owner-cwd-live/validation.json, identity-matrix/results.json, traced-matrix/results.json, trace-capture.log, trace-summary.json, source.patch, changes.patch, patch-contract.md, review.md, next-root-map.md and serial-vm0.log. Goal19 remains active: 11 enforced/10 pending/0 documented legacy root checks, first pending exec. Root-aware namespace transfer, native-source/owned-root/third-owner grant installation, the global-root third-owner fallback, and original readonly/placement evidence remain pending. No penetration testing or rejected peer-hierarchy retry ran. The original goal string and --no-setup authorization remain intact.


### 9KE — Native preparation preserves independent root and cwd scopes

The native filesystem factory now accepts independent retained process-root and cwd views. The process root is the root object from the root view; the selected cwd stays the directory object from its own view. Current names choose only which typed membership proof to perform. Native identity proves the narrower common boundary and cwd membership under one publication epoch. Absolute lookup retains the process-root policy, while cwd lookup unions inherited restrictions from both views. This prevents promoting a narrow cwd export to the whole process root or discarding independent readonly/nosuid restrictions.

Both native anchors retain their own backend and current identity-derived names. One captured credential value and outer mount-membership owner survive preparation; namespace, mount and both search generations are checked before all-or-nothing output replacement. Failed preparation preserves prior output. Source/output aliasing and final retirement stay outside publication. The existing three-argument API delegates with one view in both roles and keeps the existing cwd-view object when neither policy nor boundary changes. The cwd-only accessor is not a complete independent-root context and must not be used alone to refresh one.

Validation: final Normal Build WOS (RelWithDebInfo), isolated Debug, cache-disabled format/clang-tidy and eleven host suites pass, including all123 credential checks. Final isolated KTEST --no-setup passes 54215/0, powers off, and launcher exits0. Two new TMPFS/XFS fixtures cover128 root/user, boundary-order and independent-policy profiles, actual absolute/relative lookup and dotdot boundary rejection versus process-root clamping, disjoint/empty/current-search refusal, rename/replacement, aliased output and busy accounting. Existing cross-mount fixtures add64 independent-root/cwd preparations with opposite root-backend arrangements, leaf/nested cwd, both nesting directions and complementary inherited policies. Their typed anchors and merged policy checks pass. An earlier preliminary runtime was stopped before completion when these mixed cases were added; it is archived as incomplete, not passing. The first complete runtime then reported54213/2: the new fixtures incorrectly expected dotdot to clamp at a narrower inherited boundary. Existing VFS instead returns EPERM there. The corrected expectation preserves this policy; production code did not change for the correction. That failed run is archived separately. The final run uses the matching final source/kernel hashes and fresh overlays.

The preliminary and final VMs and launchers are gone; all WOS disks are free at 2026-09-15T05:43:24.012230+00:00. Evidence: build/goal19-independent-root-cwd/validation.json, source.patch, changes.patch, patch-contract.md, review.md, next-receiver-map.md, first-run/status.json, failed-run/status.json and serial-vm0.log. No new multi-node fixture or rejected hierarchy retry ran.

Goal19 remains active:11 enforced/10 pending/0 documented legacy root checks; first pending exec. Native independent preparation is now available, but routing-aware root selection, negotiated root transfer, durable receiver ownership and worker/Task installation still need connection. Normal /rootfs retained-cwd submission keeps the9KD ENOTSUP guard. The original remote cwd/readonly cases and general native/owned-root/third-owner placement remain unqualified. The default interpretation preserves existing declared LOCAL/HOST routing; an optional preference question was asked and no different user choice was supplied. The exact original goal and --no-setup authorization remain intact.


### 9KF — Negotiated legacy root and return-owner cwd

A separately advertised rooted-return-owner capability carries a canonical non-global root path plus the existing cwd owner grant in the unchanged112-byte filesystem suffix. ROOT_PATH requires CWD_GRANT and excludes ROOT_GRANT. Both source encoders require that capability and revalidate its exact resource generation alongside full peer/session/boot/channel identity. Unsupported or unknown remote cwd owners and owned roots fail before pathname fallback. Decoder rejects malformed root components, missing termination, nonzero wire padding and inconsistent flags without replacing filesystem output.

The receiver maps full credentials before adopting cwd, binds the legacy physical root, and uses independent native root/cwd preparation to prove membership. Executor-linked preparation retains both native views across loader waits. Installed tasks keep legacy root-path/routing semantics, with the cwd identity and inherited readonly/nosuid view intact; the native root remains preparation-owned. Worker state restores and child inheritance uses the same filesystem. This mode does not promise root identity after a root rename.

Normal Build WOS (RelWithDebInfo), isolated Debug, cache-disabled format/clang-tidy and eleven host suites pass, including124 invoked credential checks. Full isolated KTEST --no-setup passes54215/0, powers off at429.167s, launcher exits0. Expanded wire and actual TMPFS/XFS worker/child fixtures pass.

The unchanged ordinary two-node matrix now has13 PASS/5 FAIL: all nine XFS cases pass and retain seed across remote rename, including root/UID100 and main/worker exec; four local TMPFS cases pass. Five remote TMPFS attempts are refused, including the before-rename control. Both backends report cleanup=1 and both uploaded images are removed. Two bounded traced repeats reproduce the same results. WOSDBG verifies both live kernel IDs864c236a4eaf08deb6499283d5950c94b2a69748 and closes its session. Hardware traces then verify successful cwd adoption/root binding but native preparation=-ENOTSUP on TMPFS. Five mount observations show /rootfs/tmp dev_id6 covered_directory=nullptr; boot pivot clears these parent references. The receiver currently exposes that refusal as EINVAL; preserving ENOTSUP is also pending. No pathname fallback or hierarchy-operation retry was used.

All private cluster and KTEST VMs/launchers are gone; the final audit at2026-09-15T06:16:11.662665+00:00 finds no WOS VM. Evidence: build/goal19-rooted-return-owner-cwd/validation.json, source.patch, review.md, next-pivot-map.md, identity-matrix/, traced-matrix/, cover-matrix/, trace-root.log and trace-cover.log. Goal19 stays active11 enforced/10 pending/0 documented legacy checks; first pending exec. Native parent-cover publication across boot pivot is the next source gap. The exact original goal and --no-setup authorization remain intact.


### 9KG — Boot pivot publishes native parent covers; ordinary cwd matrix passes

Pivot now prepares destination covered-directory identities while its odd mount epoch freezes the complete mount catalog. The VFS caller holds namespace publication across remap, the existing task-root update and epoch reopening. Parent selection uses the longest strict component prefix of the prospective mount paths. TMPFS/XFS backend lookup pins the covered inode directly in that selected parent, without traversing the overlaid child or reopening a retained cwd by display name. Normal VFS epoch-waiting lookup APIs are not called inside the odd epoch.

An outer PivotMountReferences owner retains candidate Files, native lookup handles and their parent MountRefs, including failed lookups. Final busy checks exempt only the transaction's exact private parent refs and singly-owned candidate Files; external refs still reject pivot. All allocations and native preparation precede path mutation. Commit publishes new paths and covers together; failed pivots preserve old edges. Retired/failed references drain after namespace unlock, and WKI export drain/rebuild stays outside that lock. Missing synthetic or non-native relationships remain unproven and scoped lookup still refuses them. Receiver ENOTSUP now maps to RESOURCE_UNSUPPORTED instead of RESOURCE_INVALID/EINVAL. Broader mount/task-root concurrency requirements remain part of the pending mount domain.

Final validation: Normal Build WOS (RelWithDebInfo), isolated Debug, cache-disabled format/clang-tidy and all11 host suites pass, including125 invoked credential checks. Full corrected isolated KTEST --no-setup passes54217/0, powers off at438.811s, and launcher exits0. Two new native fixtures verify covered inode selection below an overlay, private versus external busy references, and rejection preserving old mount paths/covers. Their first setup incorrectly called the public mount syscall without a current task; that complete run reported54215/2. Setup was corrected to explicit-task native PIN plus prepared-mount publication, with no production change for the correction. The failed run is archived separately.

The unchanged ordinary two-node cwd rename matrix now passes all18 cases: TMPFS and XFS, local and strict remote exec, root and UID100, main and worker. Every payload preserves all six IDs and reads seed from the retained directory after its original pathname is replaced with fake contents. Both backends report cleanup=1 and both uploaded images are removed. Both normal nodes boot through the changed pivot successfully. WOSDBG verifies local and remote kernel build ID4e8e1cd3f6ac33ec1d84d979828518b0f7e79b52 on both nodes and closes its session. No rejected peer hierarchy-operation retry or new packet campaign ran.

All private cluster and KTEST VMs/launchers are gone. Final process audit at2026-09-15T06:54:20.424663+00:00 finds no WOS VM; normal disks are free. Evidence: build/goal19-pivot-native-covers/validation.json, source.patch, changes.patch, review.md, patch-contract.md, identity-matrix/, failed-run/, ktest-serial.log and next-exec-map.md.

Goal19 remains active11 enforced/10 pending/0 documented legacy root checks; first pending exec. The original ordinary retained-cwd regression is resolved. General native-source/owned-root/third-owner transfer, regular-FD IPC export, original readonly/nosuid and nested-placement evidence, and all other pending authorization domains remain in scope. The exact original goal string and --no-setup authorization are unchanged.
