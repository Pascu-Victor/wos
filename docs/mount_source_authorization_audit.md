# Mount source authorization audit

Goal19 9GZ, 2026-09-13. This is a source audit of the worktree recorded in
`build/goal19-mount-source-audit/source-hashes.json`, not runtime qualification.
It follows 9GY's retained devfs mount-target publication work. The mount
manifest row remains pending. No kernel behavior changes in this audit.

## Requirement and flow

Goal19 requires one stable credential snapshot for a decision, consistent VFS
traversal/final-object authority, and no partial effects from authorization
denials. Mount target validation alone does not establish source identity.

The current public flow in `modules/kern/src/vfs/core.cpp` is:

1. `vfs_mount` captures its Task and credential Snapshot, calls central MOUNT,
   acquires a MOUNT_TARGET LookupHandle and consumes that target once.
2. A `wki://` source selects an observed VFS resource and uses a callback to
   prepare/publish against the retained target after remote attachment.
3. `PARTUUID=` selects a raw BlockDevice pointer from the global registry.
4. `/dev/` resolves the source with `resolve_mount_source_path`. That helper
   uses ambient task path/root helpers and calls `resolve_symlinks` without
   passing the admitted Task or Snapshot. It returns task-visible text.
5. That text selects the global registry by name. Only a name miss invokes
   `devfs_resolve_block_device`, which walks devfs again and may attach a WKI
   proxy. A successful registry name lookup skips this devfs walk.
6. The provider may be read during filesystem detection and initialization.
   `consume_mount_target` then revalidates the admitted target and publishes
   the prepared mount. No retained source LookupHandle reaches this step.

Steps 1 and 6 retain the original request context. They do not make step 4
use that same snapshot, nor bind steps 5 and 6 to one source object.

## Findings and their limits

| Surface | Current source evidence | Assessment |
| --- | --- | --- |
| Initial mount authority | `vfs_mount` calls `authorize_mount_administration(CREDENTIALS)` before target lookup, URI handling or provider lookup | Preserved; do not replace with a fresh ambient decision |
| Source traversal credentials | `resolve_mount_source_path` omits the explicit `task_policy` and `admitted_credentials` parameters supported by `resolve_symlinks` | Incomplete: source traversal can recapture a later credential snapshot |
| Source namespace binding | Source resolution returns text; `block_device_find_by_name(block_source + 5)` reselects globally and bypasses final devfs inspection on success | Incomplete: an admitted source binding is not retained; the final mount target check cannot prove it |
| WKI existing provider | `devfs_resolve_block_device` captures owner/resource/generation, obtains an observation token, then returns a registry name match without comparing that provider's identity | Incomplete: observation validation does not establish that the returned existing provider represents that observation |
| Devfs node snapshot | Resolver pins the node and takes only OptionalWkiLock before reading node type and its borrowed Device fields | WKI allocation/upsert snapshot is guarded; generic namespace and metadata mutation do not share this guard |
| Block registry publication | `block_device_register`, unregister, find, count/at, PARTUUID lookup and read-only propagation access shared SmallVec state without a common registry lock | Incomplete: independent peer lifecycle leases do not serialize different peers or registry readers |
| Device wrapper registry | `device.cpp` register/unregister/find/enumeration access a second unguarded SmallVec; block registration and the journal publisher both use it | Block registry work must account for this dependency rather than assuming `dev_register` is serialized |
| Mount initialization | `prepare_mount_filesystem` owns its path reservation and writable-owner lease; FAT/XFS use the chosen raw provider after preparatory I/O | Those leases protect mount/storage use domains, not a pathname-to-provider selection |

These are source-level findings. No concurrent execution or wrong-provider
mount was reproduced in this audit. The existing source gate checks WKI
pin/snapshot ordering, not these complete source-binding requirements.

## Provider storage: a narrower conclusion than the initial suspicion

Read `dev/block_device.cpp`, `dev/ahci.cpp`, `net/wki/dev_proxy.cpp`, and
`platform/init/init_registry.hpp` together:

- AHCI providers and partitions are registered before VFS/devfs initialization.
  Their current production paths do not unregister or reclaim them at runtime.
  Partition objects retain their parent pointer.
- WKI starts later. A published ProxyBlockState is permanent:
  `erase_proxy_exact_locked` refuses `ever_published` states. Detach and epoch
  teardown unregister its bdev and release transport/I/O resources but retain
  the state and embedded BlockDevice. Terminal states are removed from the
  routable index. This storage must not be described as freed on detach.
- The separate generic Device wrapper allocated by `block_device_register`
  *is* deleted on unregister. `DevFSNodeRef` does not own that wrapper.
  However, current WKI block registration does not itself publish that wrapper
  into generic devfs, and devfs's boot enumeration precedes WKI. WKI discovery
  publishes its own Device/context pair with WKI reference accounting.
  Consequently, the wrapper deletion alone does not prove a reachable
  production devfs use-after-free. Future generic publication/removal must
  preserve this ownership distinction.
- Diagnostic scratch-device registration in `chaos_workload.cpp` is a further
  API caller with static storage. It was only read for the caller inventory;
  no workload, injection or packet test was enabled or run.

The immediate repair therefore does not require inventing ownership for all
BlockDevice providers. It does require coherent registry publication and
retention of the selected provider identity. An eventual removable hardware
provider would need an explicit lifetime contract before registration.

## Locks, contexts and compatibility

`consume_lookup_locked` already accepts an explicit Task/Snapshot pair and
checks request context, mount retirement, global namespace generation, mount
generation/identity and dirfd provenance. Devfs `with_lookup_node` compares a
retained node under its matching local/PTS/nodes/WKI namespace guard. These
are the existing seams for source acquisition and consumption.

No devfs namespace/metadata lock may span block I/O, probing, WKI attachment,
or ACK waits. A source snapshot must leave these guards before such work.
The metadata pin is not a Device ownership pin. A coherent snapshot must not
read a borrowed wrapper merely because node metadata remains allocated.

Registry readers include VFS, swap, WKI attach/server/advertisement and boot
enumeration. WKI calls `block_device_set_read_only` while holding proxy locks;
server block lookup is reachable from attach handling. Thus a sleeping mutex
wrapped around every existing API is not justified. Registry mutation may
grow SmallVec storage and register a Device wrapper; logging and accounting
also occur. Define lock ordering and allocate outside any bounded IRQ guard
before applying a registry-wide lock. Preserve the allocation-free writer
lease contract and do not hold a registry lock across Device callbacks.

The boot-remotable registry prefix is also a compatibility surface: resource
advertisement uses its index and the server resolves that index. The current
incarnation helper relies on boot providers retaining those indices. Runtime
proxies have no remotable trait and are appended after that prefix, so removing
one does not shift a boot provider. Do not replace this with a claim that all
registry indices are stable, or change wire IDs as incidental cleanup.

No UID/GID, saved-ID, syscall numbers, wire layouts, peer authentication,
resource incarnation protocol, or no-regain policy needs changing for the
mount source correction. `PARTUUID=` remains a privileged direct registry
selector; it must not accidentally acquire unrelated pathname requirements.

## Implementation order and verification boundary

1. Define coherent registry publication and selection, including duplicate
   names and the Device-registry dependency. Keep current permanent-provider
   storage semantics. Check every reader/writer context before choosing the
   guard; do not add a blocking guard under existing proxy locks.
2. Introduce an explicit mount-source binding using the original Task/Snapshot
   and the actual source mount/backend. Keep path lookup separate from the
   intentional PARTUUID selector. A missing/non-devfs source must not silently
   fall through to a global same-name device. Specify errno before changing
   that behavior and qualify task roots, symlinks and forwarding rules.
3. Capture a local provider identity without dereferencing an unowned wrapper;
   for WKI, compare owner, resource and generation/incarnation when reusing an
   existing provider. Validate a retained source at the appropriate admission
   boundary before preparatory effects. Later transport failures are not
   authorization-denial rollback obligations.
4. Exercise actual source selection with deterministic kernel cases: selected
   source replaced/removed, credential/root context changed after admission,
   same-name registry replacement, stale WKI existing provider, and no provider
   callback or namespace mutation on authorization refusal. Verify registry
   growth/removal against concurrent selection with actual implementation
   coverage, not a token-only gate.
5. Run Normal Build WOS, isolated KTEST, relevant host policy/lookup tests and
   native permitted/denied source mounts on disposable disks. Then use ordinary
   two-node mount/detach operation to qualify WKI reuse. Use `--no-setup` for
   authorized live runs; no penetration testing or packet fault injection.

The complete mount-domain audit still includes automatic mounts, lifecycle
retirement, and forwarding admission. This source audit closes the previous
unknown about current published provider storage and identifies the required
source/registry work; it does not mark those remaining requirements achieved.
No build or runtime check is warranted for this documentation-only change.


## 9HA follow-up: Device registry publication

The Device-registry row above describes the 9GZ baseline. 9HA replaces only
`dev/device.cpp`'s unguarded vector with guarded registry entries. Allocation
precedes an IRQ-save guard; linked-list publication/query/removal share that
guard; reclamation and accounting follow unlock. Device pointers remain
borrowed, keys and duplicate/first-match behavior are preserved. Per-index
lookup becomes linear and registration allocates one entry even for a small
registry; the current external index consumer is devfs boot enumeration.

A host test compiling actual `device.cpp` reproduced the baseline vector
use-after-free during concurrent registration. The corrected test covers
9,216 registration/removal cycles, actual lookup, ordering/duplicates, callback
reentry and allocation refusal with unchanged state. It uses a nonrecursive
host mutex and ASan/UBSan; it does not itself verify hardware IRQ/preemption.
Build/runtime qualification is recorded in the Goal19 ledger and
`build/goal19-device-registry-publication`.

The block registry is still unguarded. Its publication, read-only propagation,
name uniqueness, and selection must be corrected before treating mount source
registry resolution as coherent. The mount-source Snapshot/path/provider and
WKI existing-provider identity gaps remain pending. The original 9GZ source
hash manifest remains historical evidence, not a hash claim about this edit.


## 9HB follow-up: block registry publication

The block-registry row above describes the 9GZ baseline. 9HB replaces both
unprotected vectors in `dev/block_device.cpp` with entries guarded by an IRQ-save
registry lock. Name/pointer reservations are hidden while the generic wrapper is
prepared, published only after success, and hidden again while retiring. The
key remains reserved until wrapper removal finishes. Failure rolls back the
exact entry even if another registration completed in the meantime.

Duplicate names and duplicate provider pointers now fail atomically with -1;
major/minor duplicates remain supported. The wrapper registry, allocator,
reclamation, logging, accounting and provider I/O are outside the block guard.
Read-only snapshots/overlap propagation use the same guard. Provider and parent
storage remain borrowed; boot provider index order is preserved. Individual
count/index queries are coherent, not a multi-call snapshot, and index lookup
is now linear. Partition creation frees an unpublished partition and returns
null when registration fails.

The new host target compiles actual block/device registry and GPT code, with a
hosted decimal adapter and external environment shims. The baseline concurrent
case reproduced an ASan double-free during vector growth. Seven corrected cases
cover pending/retiring reservations, exact OOM rollback, 3,840 concurrent cycles,
same-name competition, order/PARTUUID, duplicate numeric IDs, alias read-only and
I/O callback reentry, writer leases and partition registration refusal. These
results qualify registry behavior; real IRQ/preemption and runtime build results
are recorded separately in `build/goal19-block-registry-publication`.

The two registry container dependencies are addressed. Mount-source acquisition
still needs the original Task/Snapshot and retained backend/provider binding;
existing WKI providers must be compared against the selected resource identity.
Those incomplete requirements are not replaced by a successful registry test.


## 9HC follow-up: existing WKI block provider identity

The canonical devfs block resolver now checks an existing remote provider against
the selected live resource observation before returning it. The helper requires
a published, active, unfenced routable proxy with the exact owner node, resource
ID, observation generation and both incarnation fields, plus the same currently
registered BlockDevice pointer. Pending reset, cleanup, resume and detach states
fail closed. Legacy zero-valued incarnation tokens match only an exact zero pair;
they do not act as wildcards.

The helper follows proxy registry -> proxy state -> leaf block registry locking.
It performs no allocation, provider I/O, attach request, peer admission or remote
wait. The successful attach writer now sets ever_published under the proxy
registry lock used by readers. Existing I/O admission still revalidates resource
and peer liveness; this comparison does not hold a peer lifecycle lease across
subsequent use. Published provider storage and wire/resource IDs are unchanged.

A private kernel fixture exercises the actual helper and registry, including
identity mismatches, lifecycle flags, unpublished/fenced/inactive providers,
unregistration and legacy tokens. It issues no network requests. Source gates
also ensure canonical devfs reuse calls the helper after dropping the node pin.
Build/runtime evidence is recorded in build/goal19-wki-block-reuse.

This closes the existing-provider identity comparison in the canonical devfs
fallback. Core's earlier direct global-name selection, source traversal under
the original Task/Snapshot, retained source/backend binding and generic devfs
provider capture still need work. The full mount domain remains pending.


## 9HD follow-up: admitted mount-source capture

Public /dev source handling now uses MOUNT_SOURCE LookupHandle acquisition with
the original Task and admitted credential Snapshot. Both component and fallback
lookup paths retain the selected devfs node. Consumption checks the request,
root/cwd, namespace and mount generations, requires central MOUNT, and captures
the provider under the matching devfs namespace and metadata guards. Missing
sources return ENOENT, non-devfs/non-block sources ENOTBLK, stale bindings EAGAIN
and a directory requirement on a block source ENOTDIR. A routed REMOTE backend
cannot be consumed locally (EXDEV). PARTUUID and wki:// selectors are unchanged.

The old ambient source-string helper and core's global-name fallback are gone.
A followed source can resolve to a devfs mount at a different physical path;
stripping the retained mount prefix selects its actual backend. Source capture
is the admission boundary before probing/attachment. Namespace locks do not
span provider I/O or remote waits, and later I/O errors do not re-authorize the
request from ambient credentials.

Generic node publication now caches the exact BlockDevice associated with its
owned Device wrapper. Two allocation-free registry queries compare wrapper or
provider identity under the leaf registry guard. Source capture reads the
cached permanent provider without dereferencing a possibly retired wrapper.
Unregistering that provider and publishing another under the same name cannot
redirect the source node or a previously captured snapshot. The cached pointer
preserves current borrowed permanent-provider lifetime; it does not establish a
lifetime contract for future removable/reclaimable hardware providers.

The shared devfs resolver now uses the same guarded snapshot API, including its
swap caller. WKI capture copies owner/resource/generation/name while its owned
Device/context is protected, then observation/incarnation validation, exact
existing-provider comparison and optional attachment happen after unlock.
Published proxy storage and I/O admission remain unchanged.

The actual registry host tests now cover wrapper/provider identity across
publication, retirement and same-name replacement. The actual kernel source
case checks explicit credentials after a live drop, wrong task/root/Snapshot,
missing node despite a registry name, non-block/non-devfs kinds, trailing slash,
symlink identity, in-place node changes, unregistration and name replacement,
with no device reads on refusal. Native and full runtime evidence belong to
build/goal19-mount-source-binding and the Goal19 ledger.

This repairs the source selection gaps identified by 9GZ for public /dev mounts.
Ordinary multi-node source lifecycle qualification, automatic mount/device
retirement and forwarding/quiescence audit still govern full mount enforcement.


### Runtime follow-up: requested read-only mount policy remains incomplete

The final native source fixture passes22/24 checks. Both allowed credential
profiles select and read the boot device and unmount successfully, but requested
MS_RDONLY is absent from statvfs (flags2, nosuid only). Current mount.cpp derives
mount->read_only solely from block_device_is_read_only(device); fat32_init_device
and xfs_vfs_init_device likewise derive backend policy solely from that device.
Thus a requested read-only mount of a writable device is not enforced. The
fixture retains failing assertions; this evidence must not be reported as a
fully passing mount qualification. A correction needs requested policy before
backend initialization/recovery, matching writer-lease behavior and actual
backend no-write plus VFS denial/reporting tests. Device-wide readonly state
must remain independent of one mount's flags.

Native chroot setup is unavailable (ENOSYS), so no private-root native result is
claimed; the actual kernel case qualifies root-context mismatch. No multi-node
source lifecycle run occurred in9HD. All owned VMs/launchers are stopped; logs
and exact kernel/source hashes are under build/goal19-mount-source-binding.


## 9HE follow-up: requested read-only policy reaches block backends

Mount preparation now computes read-only from the requested MS_RDONLY bit OR
the device-wide policy before filesystem initialization. FAT and XFS initializer
APIs receive that policy and independently retain device-wide restrictions.
Writable mounts keep writer-lease arbitration; a read-only mount does not claim
a writer lease. The shared BlockDevice flag is never changed by mount options.

FAT sync/unmount skip device flush for read-only contexts as well as FAT-table
writes. XFS sync returns without inode/journal/device flushing for a read-only
context. XFS's existing dirty-readonly-log rejection remains before recovery;
requested read-only mode now reaches that decision rather than arriving after
initialization. The change does not turn recovery writes into permitted writes
for a read-only request. Statvfs/fstatvfs include retained mount read-only policy.
Init parses ro/rw and suid/nosuid in input order into generic flags, preserving
other backend tokens and transactional overflow behavior.

Actual kernel tests cover a readonly FAT mount on writable private RAM media,
including writer-lease separation, unpublished cancellation, publication, denied
write-open, sync/unmount and zero write/flush callbacks. A counted XFS wrapper
over an isolated scratch partition qualifies readonly initialization, denied
write-open, sync/unmount and no forwarded writes/flushes. Exact runtime results
and the formerly failing native source fixture are recorded in the Goal19
ledger and build/goal19-readonly-mount-policy.

The broader readonly mutation audit remains open. In particular, current core
consume_lookup_locked/consume_lookup_pair_locked validate context and backend
identity but do not gate namespace mutation on the retained mount's read_only;
tmpfs_mkdir_lookup and the tmpfs rename path receive no mount policy. A generic
readonly namespace guard must preserve mounting over a readonly directory
(MOUNT_TARGET changes the mount table), explicit request snapshots, paired
rename/link semantics, and errno ordering. Remote forwarding/automatic mount
lifecycle qualification also remains pending. Backend block tests alone do not
establish full mount-domain enforcement.

## 9HF follow-up: readonly tmpfs namespace mutation

The 22 tmpfs namespace callers in VFS core now pass their retained mount's
read_only into mkdir, symlink, unlink, rmdir, link and rename operations,
including direct resolved helpers and ambient/explicit owner-confined paths.
The backend checks policy under its existing tree mutex after binding,
credential and semantic validation, before allocation, child detachment,
link-count changes or timestamp updates. The direct mkdir path also receives
policy. Raw kernel construction APIs retain their writable default; the only
production callers of these higher-level mutation functions are VFS core.

This replaces the proposed blanket shared-consumer check: namespace_mutation
also describes mount-table publication, and readonly rejection before the
backend would change existing semantic errors and successful no-op renames.
Same-name/same-inode rename remains a no-op. An empty root has no detachable
parent membership, so rmdir preserves its existing ENOENT before readonly
refusal. EXDEV, stale binding/context,
missing-path, type and authorization ordering remain intact. FAT/XFS/synthetic
backend contracts and mount-over-directory publication are unchanged. A denied
tmpfs mutation returns EROFS before its consumer publishes cache changes.
No per-tree readonly flag is introduced: policy belongs to the retained mount.
No syscall/wire layout, credentials, lock order or ownership changes.

The mounted kernel fixture exercises root and UID1000 callers, absolute and
dirfd paths, explicit owner execution after a credential drop, direct helpers, and the legacy ambient wrappers' ESRCH result when boot KTEST
has no current task. It checks unchanged bindings, link counts, allocation
capacity, generations and timestamps, plus writable overmount publication.
Verification results are recorded in the Goal19 ledger and
build/goal19-tmpfs-readonly-namespace.

Full readonly enforcement still needs metadata/data and remote/automatic-mount
qualification. In particular, vfs_fchmodat's retained tmpfs/devfs branches and
the direct chmod helper currently reach backend chmod without the mount policy;
the retained File metadata API has a separate readonly check. Shared consumers
remain identity/context validation boundaries, not blanket filesystem-write
gates. This increment does not mark the mount or VFS mutation domain enforced,
and does not resolve the earlier9FZ remote handoff observation.

## 9HG follow-up: readonly pathname metadata in tmpfs and devfs

Pathname chmod and timestamp updates now pass retained mount policy into the
tmpfs/devfs lookup metadata functions, across public, resolved and confined
owner entrypoints. Fourteen production calls plus one existing core selftest
consumer carry the policy. Identity, stale-binding, directory-kind and caller
checks precede readonly refusal under the existing backend guard; EROFS returns
before metadata/output or cache publication. Both-OMIT timestamps preserve their
existing successful no-op. Same-mode chmod still represents a ctime mutation.
File metadata already checked its retained mount; those gates and XFS/FAT error
contracts remain unchanged. No per-device or global devfs-node readonly flag
is added, and no metadata IRQ lock gains allocation, I/O or blocking work.

The mounted kernel fixture covers root, owner and unrelated identities, public
absolute/dirfd chmod, explicit owner operations, original-Snapshot use after a
credential drop, direct helpers, descriptor controls, missing/invalid inputs,
metadata preservation and timestamp no-ops. Owner-relative devfs directory
recovery/pinning is unsupported and retains ENOTSUP; ordinary devfs dirfds and
absolute owner requests are exercised. The initial fixture incorrectly required
that unsupported pin API to succeed, and its completed failing result is
archived. The corrected fixture does not expand owner-directory support.

The normal WOS native fixture passes96 syscall checks on readonly tmpfs/devfs
mounts under root, UID/GID104+NNP and zero-authority-ceiling root. It covers
pathname and dirfd chmod/utimensat, descriptor chmod/futimens, readonly reporting,
all-OMIT no-ops, invalid/missing errors, unchanged metadata and credentials.
The live kernel build ID matched before/after; only init and declared services
remained. Uploaded binary, mounts and paths were cleaned, guest synced and both
normal VM/launcher host-verified absent. Final KTEST/build/tidy records belong
to the Goal19 ledger and build/goal19-readonly-path-metadata.

The next concrete readonly gap is client-side REMOTE mount policy. Public
symlink/mkdir/remove/rename and metadata consumers dispatch REMOTE operations
before the local backend gates. A readonly client mount can refer to a writable
owner mount, so the owner's local readonly policy alone cannot enforce the
client's requested restriction. Chown has the same early remote branch;
xattr has an existing pre-dispatch readonly check. Qualification must cover
client readonly policy, original Snapshot/channel binding, no mutation or
ambiguous replay, timestamp no-ops and existing unsupported/error contracts.
This is not a claim that every remote backend currently allows every mutation.
Automatic mount/device and forwarding lifecycle qualification remains pending.

A bounded source review finds no automatic read-atime writes in tmpfs/devfs:
assignments are initialization, alias snapshots or explicit metadata updates.
Public write/pwrite enforce retained description access; ftruncate additionally
checks retained readonly policy. These observations do not establish complete
remote/automatic lifecycle or other filesystem data-path coverage.

### Goal19 9HH — remote descriptor mount policy qualified in KTEST

The remote File metadata dispatcher now reads immutable `File.mount_owner->read_only` after File and timestamp validation, before proxy locking, feature queries, or request submission. `fchmod`, `fchown` (including unchanged-ID requests), and mutating `futimens` return EROFS even if the owner exports a writable filesystem. Both-OMIT remains on the existing owner-validation path. Invalid File/timestamp inputs keep EBADF/EINVAL precedence; allowed requests keep their original Snapshot, complete channel binding, feature negotiation and single submission. No UID/GID, wire, syscall, lock-order or lifetime changes.

Kernel regression coverage calls the actual remote APIs with private disconnected state and two Files sharing one context but different retained mount policies. It checks root, restricted root and anonymous snapshots, cache/request-state preservation, retiring retained mount policy, timestamp modes, and invalid input. This proves a client admission boundary; it does not substitute for a multi-node writable-owner test. The full caller map and patch contract are in `build/goal19-remote-readonly-descriptors/`.

Pathname scalar/dirfd mutation and CREATE_CLOSE/UNLINK/RENAME batch client policy remain pending. Owner-only rename no-op and errno facts must be handled without speculative mutation, invented local success or replay. Existing open/xattr/truncate mount gates are distinct. Automatic mount lifecycle and earlier 9FZ observation remain unresolved.

Normal Build WOS and isolated build both passed. Nine focused host/source checks and policy checks passed. The complete isolated suite passed **52,696 checks / 0 failures** at uptime171.059; the new remote descriptor case passed42.906, automatic poweroff171.060, launcher exit0. Formatting/tidy passed with514 diagnostics including one new braces-style warning; the isolated compile emitted four pre-existing designated-initializer warnings. The initial sandbox launch stopped during libguestfs local-socket bind, before VM boot; the host-access --no-setup retry passed. Host inspection confirms both launchers1517331/1561360 and VM1631530 are absent and no QEMU holds either normal or isolated WOS disks. Source/kernel hashes match the tested artifacts. No native or multi-node run in9HH. Evidence: `build/goal19-remote-readonly-descriptors/validation.json`. Full goal remains active; manifest10 enforced /11 pending.

### Goal19 9HI — remote client readonly pathname/batch policy

Fifteen VFS dispatch gates now enforce the exact retained remote mount's read-only policy across pathname/dirfd and direct namespace/metadata adapters plus mutation batches. Rejection precedes transport submission and cache changes; no new locks, allocations, wire fields, syscall ABI changes, credential snapshots or replay paths. Local input/dirfd/cross-mount validation keeps precedence. Readonly remote rename returns EROFS even for equal text because the client cannot prove an owner-side no-op; writable-client owner results and native backend contracts are unchanged. Both-OMIT timestamps still undergo owner validation; invalid timestamps remain EINVAL. Whole mutation batches return EROFS for every item after full client path preflight, while STAT_FOLLOW remains readable.

Normal Build WOS and isolated build passed. Nine focused host/source checks and policy checks passed. Initial host check8/9 identified the newly introduced test helper missing from the checked pathname inventory; the inventory and FAMILIES row were updated without weakening discovery, and a stale XFS/remote ownership description was corrected. Formatting/tidy passed (776 diagnostics, none on changed lines); native fixture and Python compilation passed. KTEST **52887 passed /0 failed** at169.726; newcase PASS36.121; automatic poweroff169.727; launcher0.

Native two-node qualification passed **93 checks (31 each for root, UID/GID104+NNP, zero-ceiling root)** plus an independent writable-owner control. A client requested MS_RDONLY on the peer /tmp export; pathname/dirfd/File and batch mutations were denied, BOTH-OMIT completed owner validation, and owner metadata and complete credentials remained unchanged. Both live kernel build IDs matched before and after; authenticated peers remained CONNECTED with policy4095. Only declared tasks remained: node0 init1, journald93, netd94/95, httpd96, dropbear97; node3 init1, journald89, netd90. Fixtures removed owner files/directories, local mount/target and uploaded binary. Both guests synced, normal launcher0. Host inspection confirms KTEST launcher1647915/VM1718250 and normal launcher1722441/VMs1722591,1722600 absent; normal/isolated disks free. Evidence: `build/goal19-remote-readonly-paths/validation.json`, scoped patch, build/host/policy logs, full serial, native log, before/after GDB captures and process receipts.

Previous turn classification: progress. Full goal remains active; manifest10 enforced/11 pending. Automatic peer/mount lifecycle, forwarding/imported-descriptor retention of client policy and broader domain qualification remain to audit; historical9FZ is unresolved. No indispensable external runtime blocker.

### Goal19 9HJ — manual remote unmount retires the exact proxy group

Successful manual VFS unmount now retires the registered WKI anchor and hidden
lanes. After retained lookups/open Files drain, VFS pins the exact registered
owner while its retiring row still has private_data; competing WKI unmount
therefore sees EBUSY until the pin protects removal. After row removal and
mount_lock release, the completion hook claims an unclaimed group, or leaves
already-claimed teardown to its existing owner. It consumes the extra pin on
every success/failure path. A completed VFS removal does not recurse into path
unmount. Busy denial leaves the original group and RDONLY/NOSUID policy intact;
old teardown cannot remove a same-path replacement. Publication still holding
its construction pin is covered even before mount_configured becomes true.
No syscall/wire/credential ABI, peer policy or lock-order changes. Production
pin/claim adds no allocation; channel/cache cleanup remains outside VFS and
proxy registry locks. Deferred detach retains its existing ACK/epoch lifetime;
umount does not promise synchronous completion of owner-side File cleanup.

Normal Build WOS and isolated diagnostic builds passed, as did nine focused
host/source checks, credential policy gates, native compilation and formatting.
Tidy completed with537 diagnostics, including three new braces-style warnings;
no warning-free claim. KTEST passed **52,888 checks /0 failures** at161.822,
automatic poweroff161.823 and launcher0. New manual-retirement case PASS37.986;
existing busy/replacement retry PASS37.977. The new case covers manual, already
claimed and in-publication retirement, two registered lanes, cancelled pin,
busy policy preservation, exact replacement survival and registry removal.

Two-node ordinary native validation passed **93 authorization checks**, a
writable-owner control, busy/readable mount retention and complete retirement
of four successive eight-lane mount groups at the same path. The initial
combined busy/read assertion failed: diagnostic rerun showed EBUSY,8 proxies
before/after, and read0. The setup writer had unflushed remote write-behind;
adding fsync(writer) before opening the independent reader produced read4 and
all checks passed. Kernel code and denial expectations were unchanged. Initial
and diagnostic logs are preserved. Corrected native source/binary hashes are
separate from the original build receipt.

Both running kernel build IDs matched before/after; authenticated peers stayed
CONNECTED with policy4095. Only declared tasks remained: node0 init1,
journald93,netd94/95,httpd96,dropbear97; node3 init1,journald89,netd91. Fixtures
removed files, mount targets and uploaded binaries. Both guests synced;
cluster launcher exited0. Host verification confirms KTEST1743803/1801059 and
cluster1802919/1803039/1803043 absent; normal and isolated disks are free.
Evidence: build/goal19-remote-manual-unmount/validation.json, before/after
copies, scoped patch, contracts, build/host/policy logs, complete KTEST serial,
native logs, both-node GDB captures and process receipts.

Source audit also confirms ordinary filesystem Files are rejected by remote
FD export planning with ENOTSUP before resource publication; they are not
reopened remotely with lost RDONLY/NOSUID flags. CLOEXEC Files are skipped and
strict placement fails while local fallback keeps original descriptions. This
is source evidence, not a new descriptor-transfer runtime qualification.

Previous goal turn classification: progress. Original fenced goal unchanged;
full goal remains active, manifest10 enforced/11 pending. Broader automatic
mount/resource and domain qualification plus historical9FZ remain unresolved.
No indispensable external runtime blocker.


### Retained root-export withdrawals (Goal19 9HM)

Root replacement reserves space for the worst-case old export set in a fixed
256-record outbox before closing admission or remapping mounts. Lack of space
returns the existing pre-pivot EBUSY result. Failed/cancelled preparation releases
its reservation. Successful rebuild queues the exact retired resource IDs and
owner incarnation tokens before the replacement export revision becomes visible.

The deferred resource publisher drains each peer's oldest withdrawal before
advertising replacements. A failed initial send leaves it unaccepted and queued.
A live reliable receipt is polled without resending; only its ACK releases that
peer's obligation. If the exact channel is retired, the same idempotent withdrawal
is retried with a new receipt. Other peers retain independent obligations. Offline
peers keep pending notifications, so prolonged unavailability can consume the
bounded outbox and cause a later pivot to return EBUSY before mutation.

Wire-shape encoding and tracked TX share a peer lifecycle lease. Tracked sends
borrow a gate only when the current task already owns it, and never release the
outer owner's gate. Queue operations allocate nothing and run under s_vfs_lock;
channel-status polling, lifecycle acquisition and TX happen after that mutex is
released. Owner binding retirement remains fail-closed; credentials, UID/GID
syscalls, wire layouts, peer mapping and client mount policy are unchanged.

Evidence and qualification results: build/goal19-withdrawal-outbox/validation.json.
Original failed sends and stale bindings remain in build/goal19-export-withdrawal.
This entry does not resolve historical9FZ or claim completion of the full goal.
