# Goal 19 completion audit

**Completed in9OF.** The [final nine-requirement verification](goal19_final_verification.md)
records current source, fresh and reused evidence, exact service-profile checks
and preserved independent limitations. Older stages below are historical.

The current exec authority checklist is [9LT](goal19_exec_authority_audit.md),
including production cutover refusal, receiver final admission/private cleanup,
and in-flight source retirement with a retained remote executable File. It supersedes stale exec blockers in earlier summaries;
those summaries remain historical evidence.

The current traversal qualification is
[9MD](goal19_traversal_qualification.md), covering admitted credentials, native
and mapped retained-context authority, supported/refused backends and automatic
mount lifetime. Traversal and [open/create](goal19_open_create_qualification.md)
and [namespace mutation](goal19_mutation_qualification.md) are enforced;
[Metadata](goal19_metadata_qualification.md) and [mount control](goal19_mount_enforcement.md)
are also enforced. [Signal authorization](goal19_signal_authorization_audit.md)
is enforced in9NY. [Network/hostname](goal19_net_control_retry.md) is enforced
in9OB. [WKI identity](goal19_wki_identity_audit.md) is enforced in9OC;
[WKI resource authorization](goal19_resource_enforcement.md) is enforced in9OE,
following the9OD block-rights fix. The separate stale-root publication failure
remains unfixed and is explicitly distinguished from the qualified owner-side
authorization and retirement contracts.

This is the requirement-level index for the immutable credential and
least-privilege goal, reviewed in **9HS** with the signal completion follow-up
**9HT**, ordered-denial follow-up **9HU**, and procfs enforcement audit **9HV**. The original contract is in
[`goals/19-immutable-credentials-least-privilege.md`](../goals/19-immutable-credentials-least-privilege.md).
The nine-requirement review is complete in9OF. The versioned operation manifest
has **21 enforced and 0 pending domains**; implementation and runtime evidence,
not inventory status alone, support the final verification.

## Requirements and evidence

| Contract requirement | Current implementation and evidence | Completion limit |
| --- | --- | --- |
| 1. Stable immutable credentials | `Snapshot` owns all six IDs, bounded supplementary groups, authority ceiling, service grants and restrictions. `CredentialStore` publishes a whole value under a short IRQ-safe lock. Readers retain their own copy; threads use the retained canonical process owner. The credential manifest domain is enforced. | Preserve these properties in every remaining consumer; a syntactic root-check count cannot establish consumer coverage. |
| 2. Atomic fork/clone/exec and ID/group transitions | Credential foundations and the exec authority domain are enforced. The final9LT checklist reconciles native/source/receiver publication, original and resulting identity, saved IDs, no-regain and retained failure ownership against9LQ/9LR/9LS and prior targeted controls. | This closes exec authorization/publication qualification, not every signal/VFS/WKI operation. The current domain checklists resolve VFS, signal and WKI resource authorization scope;9OF completes the final goal review. Historical9FZ/9LG progress observations remain unexplained and preserved; no causal fix is claimed. |
| 3. Versioned authority manifest | `tests/security/credential_authority_v1.json` schema 1 records all 21 required domains, decisions, sources and evidence. The audit rejects missing/duplicate domains and invalid source references. All domains have current enforcement decisions after9OE. | Domain checklists and their actual evidence remain necessary; an inventory status alone cannot prove implementation. No row was removed to satisfy the gate. |
| 4. Source enforcement gate | `scripts/test/credential_policy_audit.py` passes with zero documented legacy root checks;9OE also passes `--require-complete`. | 9LE extends the gate to complete integral-zero tokens, common typed-zero expressions, getters and alternative operators, with public-audit regressions and C++ equivalence checks. It remains a spelling-based guard and does not resolve arbitrary aliases/macros or prove that every operation calls central policy or that every denial is transactional. |
| 5. Consistent VFS and transactional denials | Shared lookup credentials, retained object identities, backend checks, mount policy and mapped VFS handlers exist. 9LV corrects ACCESS to use real UID/GID or effective IDs with `AT_EACCESS`, preserving one snapshot throughout ancestor and final checks. Native/mapped TMPFS/XFS pass 40 identity/control checks and 1,568 existing traversal checks. | Traversal is qualified in9MD. Open/create is qualified in9MK. Mutation is qualified in9MM. Metadata is qualified in9MN. Mount control is qualified in9MR. WKI identity and resource authorization are qualified in9OC/9OE. Inherited/duplicated remote descriptor policy is qualified in9HY; ordinary filesystem FD IPC import remains explicitly unsupported. Unsupported operations must be assessed by their actual refusal behavior; unsupported regular-file IPC export is not evidence of an enabled policy bypass. |
| 6. Fail-closed init identity setup | Init validates all service identities before launching services. Child setup applies NNP/sealed grants, groups, GID, UID, umask and ceiling, then verifies the result before exec. Every failed operation reports failure and exits. | Account/drop domain is enforced. Session admission additionally precedes identity setup; its current source uses `setsid` and an ACK handshake. |
| 7. Deterministic accounts and ownership | Rootfs ownership and init accounts are enforced domains. Init opens configuration with `O_NOFOLLOW`, requires root-owned regular files without group/world write, bounds input and resolves every declaration. Historical 9DX/9GP evidence includes incremental XFS ownership repair and six rejected policy/account boots. | Those historical runs were not repeated in 9HS. Ownership policy covers declared managed paths; it does not promise repair of arbitrary unmanaged files. |
| 8. Explicit WKI identity mapping | Cluster configuration defaults missing peer identity to `deny`. Compute and VFS map supplied snapshots through authenticated operation/identity policy. 9HS passes 18 root admission and 36 complete snapshot/no-regain cases on four current-build nodes. | Same-version mapping is qualified for these cases. 9LF qualifies new exec requests after ordinary receiver restarts with identity policy allow/deny/restore (36/36 cases, continuous source, authenticated generations1/2/3). 9LG adds accepted-running reboot retirement/parent-completion cases with exact mapped credentials and no-regain; its initial fresh-control interpreter-fetch timeout remains unresolved. 9KU/9LR/9LS qualify exec version refusal, final admission and committed-private source restart; 9OC completes the current identity operation audit with fresh18admission/36snapshot cases and206service-state/transition checks. 9OE reconciles issued resource grants and owner-side lifetime authorization. The stale-root EINVAL/EBUSY availability failure remains explicitly unfixed; raw transport authentication remains Goal25 scope. Peer authentication alone is insufficient. |
| 9. Service isolation | Declared identities and grants distinguish HTTPD, journal, network, key generation and SSH roles. SSH network workers discard IDs/groups/authority before `session_loop`; retained privileged brokers enter separate sessions. Service state and individual control denials have dedicated native fixtures. | 9OF adds210exact-profile denial/no-regain assertions for all five converted IDs, on both physical nodes and remote execution, plus current SSH worker/broker inspection. The privileged listener/broker retains its declared signing/authenticated-session responsibilities; UID104 processes network input after dropping authority. See final verification for evidence and scope. |

## Source and data-flow map

- **Credentials/process:** `modules/kern/src/platform/security/{credentials,credential_store,authorization}.hpp`,
  `platform/sched/{task,resource}.cpp`, and `syscalls_impl/process/{process,exec}.cpp`.
  A caller captures an owned snapshot, prepares a complete transition, then
  publishes through the canonical process store. Shared owner lifetime must
  survive worker exec/retirement. Credential and publication locks must not
  cross blocking drains or user-memory accesses.
- **VFS/WKI:** `vfs/{core,mount}.cpp`, backend implementations,
  `net/wki/{auth_policy,remote_compute,remote_vfs,dev_server,remote_ipc}.cpp`.
  A mapped request carries its snapshot and retained channel/object identity
  through authorization and commit. Native and wire compatibility, namespace
  identity, exact lifetime, and unchanged state on denial remain review surfaces.
- **Services/rootfs:** `modules/init/src/{service_manifest,service_identity,services}.cpp`,
  `service_identity.h`, `configs/wos-services.conf`, rootfs policy/build scripts,
  and `toolchain/src/dropbear/src/{svr-session,svr-kex-broker}.c`.
  Validate configuration/accounts, resolve all declarations, fork, admit the
  child's new session, drop and verify credentials, then exec. PID 1 keeps its
  own identity; failure never enters service code.

This audit changes no production locks, allocation assumptions, syscall ABI,
wire formats, service grants, or manifest enforcement status.

## 9HS verification record

Artifacts are under `build/goal19-completion-audit-9hs/`; final commands, statuses,
hashes and cleanup receipts are indexed by `validation.json`.

- Normal **Build WOS** passed using the current `.vscode/tasks.json` command
  fallback, `scripts/dev/build_wos.sh`; cache profile is `RelWithDebInfo`.
- Eight focused host suites pass. The initial documentation/source check failed
  because it required removed parent/child `setpgid` calls. It now checks the
  documented session creation/admission path. The existing actual-source session
  runtime test separately passes all 12 scenarios.
- The policy audit passes with zero legacy root checks. `--require-complete`
  still fails at pending `exec`, as required by the unchanged manifest.
- Four private nodes use the current normal kernel, with all four live build IDs
  verified before fixture execution. Receiver policies for source node 0 are:
  node 1 unmapped, node 2 same-domain nonroot, node 3 same-domain root. All peers
  authenticate. Additional explicit node-3 trust was configured for cleanup;
  it does not change any node-0 admission expectation. Nested execution toward
  node 1 nevertheless returned `EACCES`; this audit does not qualify that route.
- All 18 root admission cases and all 36 full snapshot/no-regain cases pass.
  The latter cover root/inert/service profiles, ordinary protocol fragmentation,
  and small versus 9 MiB padded ELF files. The test-local identity runner omits
  its obsolete root VFS-readiness probe on deliberately denied receivers; every
  original compute result assertion remains unchanged.
- Service runtime/state checks pass 188 assertions across nodes 0 and 3; the
  six-profile device fixture passes 24 assertions. The original full native
  credential run reports 64/66 subprocesses passing. A journal fixture race
  assumed immediate publication of the first userspace module device, although
  the kernel publisher runs asynchronously. The fixture now waits at most two
  seconds for the alias before applying the unchanged permission assertions.
  It also reports subprocess indices and failed wait status.
- The rebuilt suite again reports 64/66: journal passes, but signal case 5 times
  out with `SIGALRM`, and procfs case 29 fails because this audit initially
  uploaded the binary under a different basename. The procfs fixture explicitly
  requires `testprog`. Correcting the test-local upload name gives a passing
  focused procfs run (14 checks) and journal run (36 checks). The full native
  suite is **not qualified as passing**.
- A temporary userspace progress adapter identifies the signal wait after
  direct foreign-process denial succeeds, at group query/delivery. A bounded
  live saved-stack capture of child 649 places it in `wki_wait_for_op` through
  `wki_group_signal_as` and `with_local_signal_targets`; victim 648 waits on its
  fixture pipe. The 30-second parent deadline expires. No unauthorized delivery
  or root cause is established. Preserve `cluster/group-query-wait-node0.log`
  and the diagnostic source/results for the next mapped-signal investigation.
- Final Normal Build WOS and scoped formatter/clang-tidy pass (1,042 warning
  diagnostics, zero errors). Kernel hashes and the four 9HR changed kernel/test
  source hashes still match the previous validation record.
- All four live build IDs match again after testing. Nodes 0 and 3 sync; nested
  sync toward node 1 returns `EACCES`, and node 2 sync is not attempted. The
  disposable cluster is stopped without changing its trust policy. All four
  exact VM processes and their launcher are absent; no WOS VM or normal/isolated
  disk holder remains. The cleanup script's printed plural denial summary is
  imprecise; `sync-progress.json` records the sole attempted nested denial.
- No isolated KTEST was run in 9HS: there is no kernel change in this audit.
  The prior 9HR result of 52,960 passed/0 failed is historical evidence, not a
  new run. It does not explain the earlier 9FZ failure.

Only ordinary functional OS tests are authorized here. Live work uses
`--no-setup` on the existing topology. No packet-fault campaign or penetration
test is part of this audit.

## Next completion work

The original9IA retained-cwd rename regression is resolved by9KG, with all18
ordinary two-node cases passing.9KH additionally prevents an inherited native
cwd view from falling back to unrestricted text on another remote exec.
General native-source, owned-root and third-owner transfer remains pending.

9KI fixes a reproduced startup dependency: new automatic child mounts kept a
retired peer root permanently busy. The worker now defers those successors
without spending their retry budget until exact-owner retirement removes the
old row. Fresh matched-kernel baseline/candidate observations prove root recovery
and restore the two previously failing remote procfs diagnostic commands.
This does not qualify arbitrary manual-mount races.

9KJ fixes the first-hop alias load by using the captured visible export name for
absolute symlink admission while retaining physical confinement.9KK then traces
the onward refusal to fd0's held pipe proxy being classified as an unsupported
ordinary file before directory preparation. Fresh peer-bound pipe forwarding
and nonblocking relay writes now preserve that existing authority. The original
return-hop hostname, exact pipe contents, EOF and a third placement all pass on
matched live kernels. Evidence: `build/goal19-onward-exec-admission/validation.json`.
9KL brings mapped STAT/READLINK/REALPATH and descriptor-relative OPEN into
the same captured namespace, including intersection with narrower directory
views. Both ordinary metadata controls and all768 live REALPATH checks pass.
Evidence: `build/goal19-mapped-path-alias-parity/validation.json`.
9KM extends the captured namespace to retained directory PIN while preserving
relative lookup after root rename and intersecting narrower inode boundaries.
Both live cwd fixtures pass29/29, including root/user absolute alias chdir.
Evidence: `build/goal19-mapped-directory-pin/validation.json`.
9KN brings the seven mapped mutation operations and both rename operands into
the same captured namespace, preserving existing commit and denial contracts.
Both native backend matrices and all480 live chmod/mkdir checks pass.
Evidence: `build/goal19-mapped-mutation-namespace/validation.json`.
9KO subsequently qualifies phased XATTR namespace lifetime: stages own the
credential snapshot and both export names through commit; the current source
hash still matches that qualified implementation. See
`build/goal19-xattr-namespace-lifetime/validation.json`. Socket/PTY proxy control
contracts, transferable process-view contracts and general filesystem transfers
remain pending. Unsupported grant transfers and permitted legacy pathname
placement need distinct authorization assessments; general transfer support is
not established by a passing denial test.

Use the pending manifest domains to identify a concrete missing decision,
transactional denial, or indispensable qualification case. Record that case and
its applicable numbered requirement before changing code. Broader OS reliability
work is relevant only when it violates one of this goal's stated invariants.

The unresolved 9FZ observation is an admitted remote signal/completion failure;
later passing matrices do not establish its cause or fix. Retain the captured
evidence and exact outstanding question rather than treating it as either a
proven authorization bypass or a resolved defect.

### 9HT: the 9HS group-signal timeout is fixed

The configured-policy receive gate consumed and ACKed denied group requests,
but `send_policy_denial` had no group or user-signal response cases. Each caller
waited for a missing RPC reply, eventually exceeding the native test deadline.
The receiver now replies with the existing `EACCES` response for task signals,
legacy group signals, group route queries/delivery and receiver enumeration.
Replies retain request/task correlation, exact peer/channel/generation and the
current boot where required. All other metadata is zero. No target enumeration,
credential installation or signal effect occurs. Peer policy, wire layouts,
syscall errno and the existing best-effort denial-send behavior are unchanged.

Current 9HT evidence under `build/goal19-group-policy-denial/`:

- Normal Build WOS and isolated diagnostic build pass. Four focused host suites
  and scoped formatter/clang-tidy pass (12 warnings, zero errors).
- The new KTEST calls the production denial-construction seam with a capture
  sender and verifies all four complete reply shapes and invalid-input refusal.
  Full isolated KTEST passes **52,961/0** and powers off normally.
- All four private normal nodes' live build IDs match the saved ELF. The original
  native credential suite passes **66/66 subprocesses**, including signal case
  5 with its unchanged 30-second deadline and assertions. Device checks pass
  **24/24**; root mapping **18/18**; full snapshot/no-regain **36/36**; service
  runtime/state **188/188**.
- Final capture again matches all four live build IDs. Cleanup sync succeeds on
  nodes 0 and 3. The additional UID 100 sync toward node 2 fails: source logs
  report `VFS_REF` submission `EIO` at its 60-second wait; receiver logs report
  expiry before post and retirement of its unpublished process. A further
  helper route through node 3 toward node 1 returns status 5. These routes are
  not qualified. All seven exact KTEST/cluster VM and launcher processes are
  absent after cleanup, with no WOS VM or normal/isolated disk holder remaining.

This establishes a cause and fix for the 9HS timeout. It does not establish a
cause or fix for historical 9FZ, whose admitted migration/completion observation
remains distinct. Broader mapped lifecycle, compatibility and other pending
domain qualification remain required; enforcement stays 10 enforced/11 pending.

The initial source inventory in the goal and parts of the historical table in
`docs/credential_authorization.md` predate enforced credential, device, ptrace,
account and ownership domains. Treat those passages as historical findings;
the local source, current manifest and explicit qualification records govern
current behavior.


### 9HU: policy denial preserves the next permitted dispatch

An authenticated policy refusal advanced the reliable receive/ACK cursor but
left the dispatch cursor behind on ordered dynamic and IPC_DATA channels. The
next permitted request could wait for a dispatch turn that would never complete.
`try_consume_policy_denial_locked` now consumes both turns under the existing
channel lock. If an earlier dispatch has not finished, it leaves the frame
unconsumed for ordinary reliable retry. It never blocks in RX, invokes a denied
handler, or changes credentials, peer policy, grants, wire/syscall ABI or errno.
Fixed waiter storage drains under the lock and wakes after unlock.

The deterministic KTEST covers ordered/unordered channels, earlier-turn retry,
unchanged ACK/accounting on refusal to consume, waiter transfer, next-turn
readiness, duplicate refusal and sequence wraparound. Its initial version wrongly
required a current scheduled Task even though selftests run before scheduler
startup; that run reports 52,961 passed/1 failed. A private Task marker corrects
the fixture without weakening its assertions. Final Normal Build WOS and isolated
build pass, five host suites pass, scoped formatter/tidy passes, and full KTEST
passes **52,962/0**, followed by automatic poweroff.

On four final normal-kernel guests with matching live build IDs, the existing
18 identity, 36 full snapshot, 66 credential subprocess, 24 device and 188 service
checks pass. Direct UID 100 sync/uname on nodes 2 and 3 passes. Nested execution
through node 3 toward node 1 returns exact `ENOTSUP`, clarifying the previous
helper's opaque status 5. It remains an unsupported qualification route.

The earlier mapped-read timeout remains open. An old-kernel run failed at
snapshot case 30; a fresh replay passed all 36, then timed out during UID 100 sync
on node 2. A bounded 1.88-second observer pause captured OP_VFS_READ/cookie 224 on
source peer 1/channel 73/generation 1, with receiver receive/dispatch cursors both
223 and source VFS workers idle. The source counterpart channel state was not
captured in-flight. A subsequent uname passed. This evidence does not attribute
the timeout to the dispatch defect or prove it fixed. Later service readiness
loss and historical 9FZ also remain unexplained.

Final nodes 0, 2 and 3 synced; node 1 sync is unqualified. All 19 recorded
VM/launcher identities across this work are gone, and no WOS QEMU or normal/
isolated disk holder remains. Evidence and scoped rollback patch:
`build/goal19-mapped-exec-read/validation.json`. The manifest remains **10 enforced/
11 pending**, and the complete gate still rejects pending `exec`.

### 9HV: procfs authorization domain qualified

The procfs manifest domain is now enforced. This closes the procfs portion of
requirements 3, 5 and 9 for current interfaces; the other ten pending domains
and their lifecycle observations remain open. No production code changed in
this audit. The earlier public-generator and remote-process qualification
limits in the procfs decision are superseded by the following evidence.

| Enabled surface | Source contract and qualification |
| --- | --- |
| Open, metadata, access and links | VFS retains the selected ProcFile/Task and admitted caller Snapshot/PID. Private link results and staged metadata revalidate target credential generation before publication. FD-link open duplicates the exact File and preserves its grant/shared offset. Current native matrices pass 4,560 open, 2,244 metadata, 1,200 readlink and 860 realpath checks, with 16 identity/dumpability profiles per matrix and cleanup. The existing kernel cases cover invalidation between staging and publication, retained identity and untouched refused output. |
| Private read, directory enumeration and seek | `authorize_private_procfs` uses the retained active target and one canonical credential observation. Same-process or central trace policy applies before cached data, length, EOF, cursor or entry publication; target generation is checked again. The production fops tests cover all eight private node types, authority loss, dumpability changes, cache regeneration, retirement and reference balance. |
| Limits | Read and seek carry the admitted caller Snapshot/PID to the retained canonical process owner. Own-process or RESOURCE_LIMIT authority precedes accounting/cache/cursor access. Actual dispatcher tests cover process/thread aliases, absent or retired owners, cached/uncached SET/CUR/END, zero-count and EOF refusal with unchanged output and accounting. |
| Global diagnostics and controls | The exhaustive `diagnostic_access` switch defaults to DENY. KERNEL_DEBUG gates detailed process/allocation/address diagnostics and kernel controls; WKI_ADMIN gates WKI controls. Write authorization precedes parsing and mutation, including zero-count requests. Production fops tests cover uncached and cached denials and unchanged control state. No packet-fault control was enabled for this audit. |
| Public generators | Status/stat/statm and WKI placement generators consume the retained Task, without PID reselection. Credential fields use one canonical Snapshot; uncached retired targets return ESRCH while existing public caches may survive exit. Other public outputs are declared metadata, mount paths, topology and aggregate accounting. The public zone interface intentionally includes physical zone starts; it is not claimed to conceal the physical memory map. KCPUSTAT exposes counters/timings; KCPUSTATE is separately privileged. WKI auth output contains policy/session metadata and counters, not authentication key material. Source review includes every public generator and metadata output. |
| Remote behavior | Auto-export skips PROCFS; the physical owner walker returns ENOTSUP before parsing it, and confined lookup never substitutes worker identity. Ordinary procfs descriptions cannot use the IPC stdio fallback. Live root and UID 100 opens of `/wki/wos-3/proc/self/status` both return exact ENOTSUP, with successful local procfs and remote `/tmp` controls. Supported remotely placed processes use receiver-local procfs: the two-node service fixture verifies their retained subjects and canonical credential fields. This audit does not add remote procfs support. |
| Coredump disclosure | Capture checks dumpability; the asynchronous writer creates exclusive, no-follow, mode-0600 output and retains the exact File. The existing actual-VFS test proves no reuse/truncation of an existing inode. General exception and remote execution reliability remain outside this domain's completion claim. |

Current evidence is in `build/goal19-procfs-enforcement-audit/validation.json`.
Normal Build WOS passes in RelWithDebInfo. The normal kernel, diagnostic kernel
and testprog hashes still match the recorded 9HU binaries. The prior full KTEST
log was re-inspected: **52,962 passed, zero failed**, including the procfs,
retained-result, coredump and realpath cases above. This is reused evidence for
an unchanged binary, not a new KTEST run. Current live build-ID comparisons
match all four private nodes. Fresh native checks additionally pass 66 credential
subprocesses, 24 device checks and 188 service credential/ownership checks.

The first open fixture exited 2 during setup without reporting the failing
operation. After confirming remote-directory availability, its unchanged binary
passed on the same VMs; no exact errno or cause is inferred for the first exit.
A sandbox-only process audit incorrectly reported those VMs absent. Host-level
inspection corrected that observation and prevented a duplicate launch. After
validation, nodes 0 and 3 synced, the exact private launcher received SIGINT,
and host-level PID/start-time and disk-holder checks confirm all five owned
processes absent and no WOS VM or normal/isolated disk holder remaining.

### 9HW: current mapped-read failure narrowed; identity fixture prerequisite fixed

`service_wki_identity.py` previously required root-visible executable metadata
on every receiver before running its mapping assertions. The unmapped and
nonroot-only receivers deliberately reject that root identity for VFS too.
The fixture therefore waited until its readiness deadline despite authenticated,
connected peers. Readiness now requires payload visibility on the root-authorized
receiver, while retaining authentication/connection checks for all peers and all
18 original exact mapping/errno assertions. The corrected fixture passes on the
same four private VMs; no guest policy, deadline or expected denial changed.

The subsequent current-kernel snapshot matrix passes cases 1–7, then case 8
(`small inert ordinary wos-2 strict`) returns ETIMEDOUT after 59.992 seconds.
The later matrix cases and planned repetitions were not run. This is a current
failure after 9HU, so the ordered-denial repair cannot be treated as resolving
the earlier mapped-read observation.

A bounded observer pause of 2.035 seconds verifies every live kernel build ID
and captures both endpoints of the pending READ, filling the earlier evidence
gap. Receiver node 2 waits for owner 1/channel 45/generation 1/resource 4100,
operation 1025, cookie 68. Source node 0's counterpart has receive and dispatch
cursors 69, transmit/ACK cursors 67, empty retransmit/reorder state and no queued
or active VFS read work. The receiver has transmit/ACK 69 and receive/dispatch
67. The request was consumed, but there is no outstanding reliable response.
The next required observation is actual handler/reply production and the exact
reply-send return value; no allocator, authentication or TX-contention cause is
established by these cursors.

An attempted source-line GDB send observer is inconclusive because optimized
line mappings also stop on successful paths. Its 20 bounded records are not
proof of send failure. Eight ordinary UID 100 sync commands pass on the same
cluster, four each toward nodes 2 and 3. Later success does not close the failed
snapshot case. Source review also confirms binding-reference draining protects
server File lifetime during disconnected-peer GC and binding retirement; a raw
read-handler pointer alone is not a demonstrated lifetime defect.

Normal Build WOS and Python compilation pass. No kernel code or ABI changed,
and KTEST was not rerun for this host fixture change. Final live IDs still match;
peer syncs pass, but the final source SSH connection for local sync times out.
The exact launcher exits after SIGINT, and host-level checks prove all five owned
processes gone and no WOS VM or disk holder remains. Evidence is indexed by
`build/goal19-mapped-read-recheck/validation.json`. Goal status stays active,
**11 enforced/10 pending**, with the failed mapped completion explicitly open.

### 9HX — retain completed scalar READ replies across transient send refusal

A fresh pre-patch cluster passed the 18 identity assertions and five complete snapshot matrices (180 cases). Later instruction-level observation captured one exact scalar READ response with owner status 0, 8192 bytes, cookie 15, peer 3/channel 34/generation 2, and actual send return `WKI_ERR_NO_ROUTE` (-2). This refusal occurs before reliable sequence publication, and the handler discarded the completed stack response. The observed failure followed a debugger-induced peer reset: stopping the earlier observer with `disconnect` left source execution paused while peers continued. That disturbance is excluded from natural failure qualification; this record does not assign the same cause to every historical timeout. The final observer captured 400 replies with one failed send. Separate error-only observations on resource detach and an earlier signed-pointer observer error are not READ-reply evidence.

The scalar READ response now retries only the existing pre-publication NO_ROUTE/NO_MEM/NO_CREDITS failures, retaining the exact response and channel identity. The native read executes once. WKI_OK and terminal failures stop immediately; retries are paced outside VFS/channel locks and end on the existing bounded deadline, binding retirement, or channel generation loss. The deferred worker passes its retained binding's retiring atomic; no new authority, credential capture, allocation, wire field, errno, or syscall ABI is introduced. Other operation responses retain their existing behavior.

The new deterministic KTEST exercises the production loop for transient refusal followed by acceptance, accepted/terminal one-shot completion, retirement, deadline and generation changes during waiting, and optional one-shot callers. The existing source check now follows the wrapper while preserving bounded stack-response assertions. Normal Build WOS and isolated diagnostic build pass. Full isolated KTEST passes 52,963/0 and powers off at 161.987 seconds; the new case passes at 38.295 seconds. Five focused host tests and the credential policy audit pass. Formatting and clang-tidy pass with warnings; the initial cache-enabled tidy invocation could not write its cache, so the successful run used WOS_TIDY_CACHE=0. This does not represent a warning-free audit.

The final fresh four-node cluster uses private copies of the updated normal disks. All four live build IDs match before and after validation. The 18 identity checks and five complete snapshot matrices (180 cases) pass, as do 66 native credential checks, 24 device checks and 188 service identity/ownership checks. Direct UID100 sync/uname executions on nodes2/3 pass without observation pauses. Nested execution through node3 to node1 still returns ENOTSUP95; no fix is claimed. Final QMP checks confirm all guests running before source sync, which succeeds; node2/3 syncs also succeed. The exact launcher exits cleanly. Host audit confirms all 12 owned processes across both clusters and KTEST gone, no WOS VM, and no normal or isolated disk holder.

Evidence: `build/goal19-mapped-read-send-result/validation.json`, exact pre-patch instruction results, patch contract, scoped diff, source/binary hashes and live receipts. Goal remains active; 11 domains enforced and 10 pending. Historical 9FZ and the nested remote completion limitation remain open.

### 9HY — read-only mount policy survives inherited descriptor aliases

Requirement5 follow-up narrows the descriptor-transfer obligation using current source. Public mount/unmount capture one immutable caller snapshot before administration policy. Native fork, dup and dup2 retain the same File; the mount reservation remains attached until File destruction. Exec stabilization clones a backend reference and explicitly attaches the source mount reservation. Remote file metadata checks the retained client mount policy before its I/O lock or request submission.

Remote placement first plans the entire retained FD snapshot. The current exporter admits pipe/socket/PTY resources, refuses epoll, and refuses ordinary native/REMOTE file and directory descriptors with ENOTSUP (including ordinary stdio). Its explicit DEVFS stdio fallback is separate. The receiver's typed import validator cannot import a filesystem File through WkiIpcFdEntry. Thus absent filesystem mount flags in that wire entry are not an enabled mount-policy loss. The previous KTEST export-admission test covers ordinary TMPFS descriptors at0/1/2/3, CLOEXEC omission, over-capacity denial and unchanged references/registry. This source conclusion does not claim new regular-file IPC support.

The existing remote-readonly fixture now mounts with RDONLY|NOSUID. Each forked root, service104 and zero-ceiling profile duplicates its inherited File and directory, uses dup2 to replace a directory FD with the File alias, closes the originals, checks stable device/inode/content and retained flags, then runs every existing scalar/path/dirfd/batch denial assertion through the surviving aliases. All99profile checks pass (33/profile), with owner metadata/namespace unchanged and complete credential snapshots unchanged. The writable owner control passes, busy unmount retains the live proxy group, successful unmount retires all lanes, and three same-path replacement cycles retire exactly. Upload and fixture namespace cleanup complete.

Fresh four-node normal-kernel IDs match before and after. All18identity admission checks and direct UID100node2/3sync/uname pass. Nested3-to1 remainsENOTSUP95. Final source/peer syncs pass; the exact launcher and fourVMs are gone under host audit, and no normal/isolated build disk has a WOS QEMU holder. Normal Build WOS, fixture compilation, Python syntax, scoped clang-format and three host suites pass. The fixture retains two pre-existing pid_t printf warnings. No matching fixture clang-tidy compilation unit exists; do not describe this as a tidy audit. No kernel source changed and no new KTEST ran. Previous9HX52,963/0 evidence and its nosuid/export-admission cases were reinspected with unchanged normal and diagnostic kernel hashes.

Evidence limitation: statvfs_for_mount currently synthesizes ST_RDONLY for every REMOTE mount, including a writable client view. Consequently the readonly flag alone does not prove enforcement; this qualification relies on actual EROFS/no-state-change assertions and the writable-owner positive control. NOSUID is added from the retained actual mount flag. The synthetic readonly reporting discrepancy remains an identified limitation, not a fixed authorization defect.

Goal remains active,11enforced/10pending. Inherited/duplicated remote descriptor policy retention is qualified for these operations; unsupported filesystem FD import is no longer treated as an enabled transfer gap. Automatic attach/reconnect policy publication, remaining retained-root/cwd and mapped lifecycle obligations still require their own evidence. Evidence index: `build/goal19-readonly-descriptor-audit/validation.json`.

### 9HZ — validate automatic VFS publication paths

Automatic VFS paths previously formatted unvalidated names into `/wki/<hostname>/<export>`. Mount canonicalization could interpret dot or parent components outside the intended peer subtree. The receiver now validates the complete length-delimited VFS name before discovery mutation: dot/parent components, embedded NUL and overlong names are refused. The builder checks bounded owned hostname/export copies, requires one hostname component, checks output capacity and leaves output unchanged on failure. Every automatic queue/rearm/rebind caller checks success; reconnect validates before changing the resource generation. Root exports, ordinary nested/hidden names and repeated separators retain their behavior. Current exporters already transmit at most63name bytes. Non-VFS handling, explicit mount destinations, immutable credentials, peer policy, mount flags, exact channel/resource identity and wire/syscall ABI are unchanged. No new allocation, lock or wait is introduced.

Normal Build WOS and isolated diagnostic builds pass. Five focused host suites, scoped formatting and cache-disabled clang-tidy pass with warnings. Full isolated KTEST passes52,964/0 and powers off at161.522seconds; the new production-helper case passes at37.956seconds and covers valid paths, component/name bounds, embedded NUL, exact/short capacities and unchanged refusal output. This is deterministic kernel validation; no crafted network traffic or penetration testing was used.

A fresh four-node normal cluster passes18identity assertions and four direct UID100sync/uname commands. All four live kernel build IDs match before and after. Its remote-readonly fixture passes99profile checks and the writable control, but fails its proxy-count lifecycle checks with invalid snapshots; a subsequent netdiag read is262142bytes without the completeness marker. That full fixture run is not counted as passing. On the fixture's intended two-node topology, the unchanged fixture passes all99checks, busy-unmount preservation, complete lane retirement, three same-path replacement cycles and cleanup. Both live IDs match before and after. The four-node nested3-to1 command still returnsENOTSUP95; no fix is claimed.

Earlier baseline exploration did not demonstrate a restricted-mount reset: after explicit unmount, the nested canonical restricted remount was refused withEXDEV beneath a remote parent. Both ordinary owner-restart attempts stopped before resetting any VM: four-node diagnostics lacked completeness, and the two-node helper produced no ADMITTED line. These are incomplete lifecycle evidence, not reconnect successes. Source-node sync succeeds before each owned cluster shutdown; all18owned process identities across four clusters and KTEST are absent, and no WOS VM or normal/isolated disk holder remains.

Evidence: `build/goal19-auto-mount-policy/validation.json`, patch contract, scoped diff, source/binary hashes and separate failed/passing runtime receipts. Goal remains active with11enforced/10pending. Remaining automatic mount lifecycle, retained root/cwd, mapped concurrency and historical9FZ obligations are unchanged. The bounded netdiag completeness limitation and synthetic REMOTE ST_RDONLY reporting discrepancy remain open.

### 9IA — reproduce retained cwd identity loss at remote exec

Requirement5 and exec/VFS traversal follow-up identifies a current identity-selection defect. Local relative lookup and local exec preserve the DirectoryAnchorRef. try_process_remote_exec instead copies the canonical owner's historical cwd text into RemoteExecPlan; TASK_SUBMIT serializes it, and the receiver directly installs localized text without an anchor. The separate wos_proc_exec spawn path also supplies parent_task->cwd after copying canonical filesystem state into the new task. A current-name query alone would still race rename and is not the required retained-object transfer.

New service_exec_cwd_identity.cpp/.py fixtures exercise ordinary directory rename/name reuse on a two-node cluster, with identical images provisioned on both nodes and explicit LOCAL routing for receiver paths. Each backend has one successful remote control before rename and four successful local controls after rename. All four remote cases after rename fail on both TMPFS and XFS: root/UID100, main/worker exec read `fake` from the replacement instead of `seed` from the retained directory. All18payload six-ID checks pass; directory identity is the lost state. Total10controls pass and8remote cases fail. Both fixture namespaces and uploaded images are cleaned up. This is a reproduced defect, not a passing integration suite or a claimed credential-mapping bypass.

Early artifact probes encountered receiver image absence; provisioning both images removes that confounder. The first full harness accidentally reused its image name for the TMPFS directory and relied on default submitter routing for XFS. Those setup/routing failures are preserved separately and excluded from product qualification. The corrected identity-matrix-v2 controls all pass with unchanged content assertions.

Both live normal kernel IDs match before and after; the ELF SHA256 remains30a9c6435ce83f6404376e8507244d7f035998dc8cd0eb94e5fab6893b6ca295. Normal Build WOS, fixture cross-compilation, Python syntax and scoped clang-format pass. No fixture clang-tidy compilation unit matches. No kernel source changed and no new KTEST was run; previous9HZ52964/0 is historical evidence, not validation of cwd transfer. Source sync passes; launcher2654873 and VMs2655005/2654992 are absent under host PID/start-time audit, with no WOS VM or normal/isolated disk holder remaining.

The next required implementation is recorded in docs/remote_exec_cwd_identity.md: negotiate a retained directory transfer, preserve exact peer/resource/object lifetimes, mapped current authority, root confinement and source read-only/nosuid restrictions, and install the independent receiver anchor before publication. Native-source, return-to-owner and third-owner placement must be qualified independently; do not silently fall back to pathname selection or mark this obligation complete with only a fresh getcwd string. Cancellation and final release must stay outside path/publication/registry locks. Evidence index: build/goal19-retained-cwd-exec/validation.json. Goal remains active,11enforced/10pending; previous9HZ is classified as progress.

### 9IB — own exec filesystem context and fix older-worker spawn cwd

Task::capture_filesystem_context captures the canonical filesystem owner's root, cwd, lengths and retained DirectoryAnchorRef together under the existing path_metadata_lock. It stages the capture locally and replaces caller output only after unlocking; missing thread owners preserve output. RemoteExecPlan owns this context through preparation and releases its anchor outside guards before dropping the actor lifetime reference. Native spawn uses the same context for private-child inheritance, then passes new_task->cwd to placement. Previously it passed parent_task->cwd, which can remain an obsolete per-thread copy after another thread changes the process cwd. No Task instance layout, saved-ID/credential policy, mount flag, syscall or wire format changes.

The new deterministic VFS KTEST uses a thread with poisoned legacy path copies, captures the canonical owner, replaces its cwd, and confirms captured paths and independent references survive. Replacing the last captured output closes exactly once outside both path and namespace-publication locks. Both builds pass. Full isolated KTEST passes52,965/0, with the new case at36.911seconds and clean poweroff at160.711seconds. Three focused exec/VFS/scheduler source suites pass; the existing exec source gate now follows context capture before spawn actions and requires the private-child cwd at placement. Scoped format and cache-disabled clang-tidy pass with warnings; the fixture has no matching tidy compilation unit.

The new service_spawn_cwd.cpp/.py regression creates a worker before the main thread changes the shared cwd, then uses the kernel spawn ABI. On the unchanged9HZbaseline,12controls pass and4remote older-worker cases start at `/` instead of the prepared cwd (root/UID100 on TMPFS/XFS). On the updated kernel, the identical corrected fixture passes all16cases. The existing raw spawn OPEN fixture also passes all24cases, preserving ordered actions and authorization denials. The first harness attempt omitted the required SpawnOptions.size field and all calls returnedEINVAL; those results are preserved separately and excluded from cwd qualification.

Both baseline and updated live IDs match before/after checks. Updated normal SHA256 is c9012873b3b7a9299462c0d3dfded7a935cf3c167d457a864129fc979359a25a; diagnostic SHA256 is7c44ea67c1dd9a3e55e47997305c901bba74a0c412e9c910872f23cac3712765. Both fixture namespaces and uploads are cleaned up. Source sync succeeds before each cluster shutdown. All8owned launcher/VM identities across baseline, KTEST and final clusters are absent, with no WOS VM or normal/isolated disk holder.

The separate9IArename/remote-exec matrix is rerun unchanged on the updated kernel:10controls pass and8remote cases still select the replacement directory. Holding the source reference does not transfer it to the receiver. The negotiated directory-reference record, exact owner/binding admission, receiver anchor adoption, root confinement and source read-only/nosuid retention remain required by docs/remote_exec_cwd_identity.md. Native-source, return-to-owner and third-owner placement remain separate qualification obligations. Do not mark the original retained-directory defect or the full goal complete. Goal remains active,11enforced/10pending. Evidence: build/goal19-exec-filesystem-snapshot/validation.json. Previous9IA is classified as progress.

### 9IC — encode the captured exec root/cwd pair

RemoteExecPlan now passes its owned filesystem context through WkiRemoteSpawnSpec into both INLINE and VFS_REF submission. Both encoders select the captured cwd and root before sizing/encoding context; fill_task_identity_context accepts that root instead of rereading Task-local root during serialization. Existing direct/legacy callers keep their explicit cwd argument and copy root from the canonical filesystem owner under its path_metadata_lock. This fallback neither acquires nor releases a directory reference, so it adds no potentially blocking final close to legacy placement and leaves no new anchor ownership on an interruptible submission stack. In the fallback, missing canonical owners leave the selection output unchanged. Root NUL termination/zero padding, packed identity sizes, credential records and saved IDs/groups, signal/resource offsets, Task instance layout and syscall ABI are preserved. The supplied anchor remains owned by the existing exec plan; no anchor is transmitted yet.

The new production-helper KTEST, WkiRemoteCompute/SubmissionUsesCapturedFilesystemContext, binds an older worker with poisoned task-local root, captures the canonical filesystem state, changes the owner's root/cwd, then encodes and decodes both delivery formats with the supplied capture and legacy explicit-cwd paths. It checks the selected root/cwd, exact credential record, missing-owner failure/output preservation and zero-filled wire tail. Existing full-credential tests continue to cover saved IDs, supplementary groups and discarded authority. Normal Build WOS and the isolated diagnostic build pass. Final full KTEST passes52,966/0; the new case passes at43.481seconds and clean poweroff occurs at161.901seconds. An initial full run also passed52,966/0 before the nonblocking-fallback and padding review refinements; only the final run qualifies the final revision. Three host suites (WKI remote compute, exec and credential policy audit), the manifest source audit and scoped formatting/cache-disabled clang-tidy pass. Existing source gates were updated for the captured-root argument without removing credential/resource assertions.

Fresh private two-node disks use normal kernel SHA256 ac13e00254152d66807b8e4d83036dc6748edfcdac3bab079c611ef5f6065b5b; final diagnostic SHA256 is739c6cf9112cecbe4e64650d91623ba5e4e824e842308a98114c783c61b1eb39. Both live build IDs match before and after fixtures. The unchanged service_spawn_cwd fixture passes all16cases (TMPFS/XFS, root/UID100, main/older worker, local/remote). The unchanged service_exec_cwd_identity fixture still has10passing controls and8failing remote rename cases; both backend namespaces and uploaded images are cleaned up. The source sync succeeds before shutdown, both KTEST launchers and the live cluster exit0, and the host audit finds all7owned process identities absent, no WOS VM and no normal/isolated disk holder. No packet-fault or penetration-testing campaign was run.

This fixes mixed filesystem-state serialization and the direct encoder's obsolete worker-root read, not retained-directory transfer. The negotiated object record, exact owner/binding admission, independent receiver anchor before acceptance, client read-only/nosuid restrictions and root confinement across native-source/return-to-owner/third-owner placement remain required by docs/remote_exec_cwd_identity.md. The full goal remains active with11enforced/10pending manifest domains. The previous goal turn is classified as no implementation progress; shutdown was revalidated and the next available encoding fix was implemented here. Evidence: build/goal19-submit-filesystem-context/validation.json.

### 9ID — qualify cwd client-policy loss across remote exec

Added service_exec_cwd_policy.cpp/.py as an ordinary two-node regression. The source uses a canonical `/wki/<owner>` root mount, alternately writable and read-only/nosuid, with a separate writable view for seeding and observing the same owner file. The fixture records the peer mount table, removes child mounts before replacing the peer root, and attempts to restore recorded paths/flags afterward. Cases cover TMPFS/XFS, root/UID100, main/worker, and local/remote exec. Each case first checks source write admission, then checks all six IDs, empty supplementary groups, expected inode and seed content in the payload. An actual write/fsync and an independent no-cache owner read establish whether data changed. Synthetic REMOTE ST_RDONLY reporting is not used as enforcement evidence; nosuid/set-ID execution is not qualified by this test.

The canonical-matrix-v2 run has24passing controls and8failing remote read-only cases. All32pre-exec policy checks and all32payload identity/object checks pass. Every failing case starts with source EROFS, retains the same file and identity after remote exec, then opens it writable, writes edit, and changes the independently observed owner data. Local exec preserves readonly denial, while writable local/remote controls succeed. This establishes a separate client-view restriction loss without rename or pathname substitution: an owner-native directory reference alone will not satisfy the required transfer contract. No kernel fix is claimed. Kernel SHA256 remains ac13e00254152d66807b8e4d83036dc6748edfcdac3bab079c611ef5f6065b5b from9IC, and both live build IDs match before and after all attempts.

The whole fixture is not green: restoration of original peer mounts returnedEXDEV and cleanup=0. Serial output records automatic host-root publication while pathname-based target creation was restoring `/wki/wos-3/oldroot` and then the peer root. The final compiled fixture instead retains the native `/wki` parent, creates native targets via mkdirat/openat before mounting, and avoids pathname-based mkdir during restoration. That restoration change has not run: automatic approval review rejected the retry, and rejected reconsideration after read-only PID and QCOW backing-chain evidence proved both guests and every backing disk were private to this task. Explicit approval is needed for another mount-hierarchy retry in a fresh disposable cluster. Do not count the final fixture as runtime-validated or claim original mount restoration succeeded. Test-file cleanup was attempted but was not separately proven after the aggregate cleanup failure.

Earlier attempts are excluded from policy conclusions. The initial alias fixture hit a60-second idle timeout; the old harness closed its output channel, and both live task registries subsequently confirmed the fixture absent before its remaining paths were removed. The new driver preserves incremental output and keeps the same command channel through idle periods. A completed alias run had ENOENT/object=0 for remote cases and successful cleanup, so it did not qualify mount-policy loss. An initial mount-table parser rejected the first row before changing any mounts; explicit four-field parsing corrected it. No VM restart or packet-fault campaign occurred.

Normal Build WOS, final fixture compilation, Python syntax and scoped format checks pass. No new KTEST was run because kernel source is unchanged;9IC KTEST remains historical evidence only. The final source sync succeeds, launcher2835578 exits0, and VM2835689/2835685 plus their launcher are absent under host PID/start-time audit. No WOS VM or normal/isolated disk holder remains. Goal remains active,11enforced/10pending. Previous9IC is classified as progress. Required next implementation remains negotiated directory and client-view policy transfer/adoption, with exact owner/binding lifetime, current mapped credentials and root confinement across native-source/return-to-owner/third-owner placements. Evidence: build/goal19-exec-cwd-mount-policy/validation.json and approval-review.md.


## 9IE — retained File mount-policy prerequisite

Inline File policy now combines read-only/nosuid restrictions with its pinned mount by union before sharing. Common FD data/metadata mutations, fstatvfs and existing nosuid exec consumers use the effective policy; the new retained positional-write adapter preserves syscall validation and usercopy routing. No wire feature or directory transfer is enabled.

Normal and diagnostic builds, scoped format/tidy and five targeted source suites plus the manifest audit pass. Final KTEST: 53,078 passing checks, zero failures, automatic poweroff at 165.723 seconds. The new TMPFS/XFS case passes at 6.913 seconds and covers restricted native Files, shared descriptors, unchanged denied-operation state, statvfs/nosuid preparation and a writable control. Two excluded runs had corrected test setup errors (missing early current task; integer-width expectations), with complete logs. All six owned process identities are absent and normal/isolated disks are free.

Goal remains active, 11 enforced/10 pending. Remote cwd identity and client-view loss remain unresolved; cwd/lookup propagation, grant/adoption and direct backend-write policy/lifetime coverage are still prerequisites to receiver publication. No peer mount-restoration retry was run. Full evidence, limitations, hashes and scoped patch: `build/goal19-retained-file-mount-policy/validation.json`.

## 9IF — remote owner write policy/lifetime

Scalar/RDMA owner writes now validate exact channel identity, File access mode, offset and retained view policy before registry activity, then retain the File through unlocked common positional I/O. RDMA admission errors complete the tagged region and cached write receipt, including access-mode denial that previously skipped completion. No wire feature or directory adoption is enabled.

Normal/diagnostic builds, credential/manifest and remote VFS/dev-server source gates, and scoped format/tidy pass. Full KTEST: 53,079/0, new owner-write admission/retirement case at 42.989 seconds, automatic poweroff at 167.997 seconds. Both owned process identities are absent; disks are free. This qualifies the deterministic owner File/registry behavior and source routing, not live RoCE/multi-node writes. Evidence: `build/goal19-owner-write-policy/validation.json`.

Goal remains active, 11 enforced/10 pending. Native mount flags must remain distinct from inherited client restrictions scoped to a retained export/root boundary before cwd/lookup propagation. Mapping writeback, compute output replay, negotiated directory grants, confinement and receiver adoption remain unfinished; the original remote-exec defects are unresolved. No peer mount-hierarchy retry occurred.

## 9IG — writeback/replay File policy

Mapping writeback and fallback compute-output replay now use common retained positional writes. Every writeback chunk validates its whole signed-offset range before committing its first prefix; short-write/backend-error behavior remains. Replay retains stdout through descriptor retirement and preserves offset-zero/one-call behavior and child-exit ordering.

Normal/diagnostic builds, credential/manifest, usercopy, remote-compute and waitpid source gates, and scoped format/tidy pass. Full KTEST: 53,081/0; new writeback/replay tests pass at 41.132/48.260 seconds, automatic poweroff at 168.490 seconds. Both owned processes are absent and disks are free. Evidence: `build/goal19-writeback-replay-policy/validation.json`.

Goal remains active, 11 enforced/10 pending. This qualifies adapters, not complete mmap admission, multi-range sync preflight or multi-node replay. file_allocate still separates FD stat from later File acquisition; initial/protect-time shared-write authorization and publication need one retained File contract. Cwd/export-view propagation, directory grants, confinement and receiver adoption remain unfinished. No peer mount-hierarchy retry occurred.


## 9IH — mmap File admission and shared mprotect policy

mmap now retains one File before stat and uses it through policy admission, materialization and shared-range publication. Every mapping requires a read grant; shared writable mappings require write access and a writable effective view. Denial precedes reservation/MAP_FIXED replacement. mprotect preflights retained shared mappings before changing lazy metadata/PTEs. Shared non-regular files retain policy without gaining writeback; clone/split and FD reuse preserve the selected File. Final newly admitted File release follows publication unlock.

Normal/diagnostic builds, credential/VM/usercopy/manifest source checks and scoped format/tidy pass. Full isolated KTEST:53,083/0; new mapping case41.084seconds; automatic poweroff167.798seconds. It exercises48 mode/view/protection combinations, unchanged denied MAP_FIXED state, retained lazy faults, eager callback FD replacement, whole-range protection admission and cloned/split policy. Direct userspace mprotect and non-lazy regular cache publication are not runtime-qualified. Two previously omitted credential source-test calls were added and pass; all73 definitions now run. Earlier script-pass records alone did not execute those two tests.

Both owned processes are absent and repository disks are free. Goal remains active,11enforced/10pending. Multi-range writeback preflight, full mapping/service behavior, remote cwd/export-view transfer and receiver adoption remain outstanding. Existing fixed-replacement allocation/I/O rollback is unchanged. No peer mount-hierarchy retry occurred. Evidence: `build/goal19-mmap-admission/validation.json`.


## 9II — complete sync preflight and native mmap qualification

Mapping sync validates all retained File grants/view policy and requested offset intervals before any backend write. Rejected clean intervals remain excluded no-ops; later dirtying is left for another sync. Standalone msync serializes mapping identity through I/O using shared VM publication. Quiesced teardown syncs all ranges once before retiring rows. No new per-page heap allocation, retained source-page pins across I/O, syscall/wire change or dirty-bit clearing. Backend state/I/O failures can still leave committed prefixes; this is not a cross-file transaction.

Normal/diagnostic builds, credential/VM/usercopy/waitpid/manifest source checks and scoped format/tidy pass. Full KTEST: 53,085/0; new 43-scenario preflight/teardown case passes at 40.753 seconds, poweroff at 167.770 seconds. An additional fresh single-node normal VM passes all 72 ordinary userspace mmap/mprotect/msync cases on TMPFS/XFS as root and real/effective/saved UID/GID100. This qualifies native FD-mode admission, fixed-replacement denial, private/shared upgrades, retained File after FD reuse and correct writeback destination. Live build IDs match before/after; files/namespaces/upload clean up; filesystem sync succeeds. The first userspace run's non-root setup used unsupported setres calls and was excluded; corrected supported drops are verified before all final cases.

All four owned process identities are gone and disks are free. Goal remains active, 11 enforced / 10 pending. Native readonly mount configuration and WKI mapping behavior are separate from this userspace matrix; cwd/export-view propagation, grants, confinement and receiver adoption remain unfinished. No peer mount-hierarchy retry occurred. Evidence: `build/goal19-msync-preflight/validation.json`.


## 9IJ — selected lookup policy and native mount-root recovery

LookupHandle captures the final selected mount's readonly/nosuid flags; direct handle policy consumers use the snapshot, moves transfer it and reset clears it. Native mount restrictions are not inherited across unrelated mounts. The regression exposed a dirfd recovery defect after namespace invalidation: appending backend root / to /mount produced /mount/, so the following parent component stayed inside the mount. Root recovery now copies the exact mount path, matching cwd recovery; retained identity, epochs, locks and syscall/wire layouts remain unchanged.

Normal/diagnostic builds, all75 credential source tests, lookup/mount-lifetime/remote-VFS/manifest gates and scoped format/tidy pass. Final full KTEST:53,366/0, new native crossing test passes40.821seconds, automatic poweroff168.546seconds. It checks root/UID100, cwd/dirfd, readonly denials, parent/symlink/nested writable controls, actual writes, and created/removed membership across namespace changes. Earlier full runs53156/1 and53157/2 recorded the same recovery defect before correction. All six owned identities are gone and disks are free.

Goal remains active,11enforced/10pending. Inherited view/root boundaries, hybrid/fallback/cache integration, directory grants and receiver adoption remain unfinished; original remote cwd/view failures remain unresolved. No peer mount-hierarchy retry ran. Evidence: `build/goal19-lookup-mount-policy/validation.json` and scoped.patch.


## 9IK — open and exec replacement preserve admitted File policy

Hybrid open admission and fresh File publication now share the stable lookup's selected policy; reservation combines it with native and existing private File restrictions. Native, mapped remote, fresh procfs, owner open/pin and remote cwd preparation propagate that policy. Shared procfs FD reopen remains untouched. Executable replacement copies effective source policy before publishing its independent clone; both exec entrypoints already captured nosuid before replacement, so this gap alone did not establish a set-ID bypass. No syscall/wire/Task/File layout, lock or allocation change.

Normal/diagnostic builds, all76 credential source tests, lookup/mount-lifetime/remote-VFS/exec/manifest gates and scoped format/tidy pass. Full KTEST:53,764/0; TMPFS/XFS four-policy first/repeated replacement case passes21.444seconds; native crossing case45.588seconds; automatic poweroff173.697seconds. New runtime evidence covers16 cloned views, frozen metadata/data, actual metadata denial, production credential preparation, independent source lifetime and lease release. Remote routing and mountless source copying are source evidence; complete userspace exec is not a new runtime claim. Both owned processes are gone and disks are free.

Goal active,11enforced/10pending. Inherited directory/export boundaries, fallback/cache integration, grants and receiver adoption remain unfinished. No peer mount-hierarchy retry. Evidence: `build/goal19-open-policy-publication/validation.json` and scoped.patch.


## 9IL — propagate selected readonly policy into XFS retained mutations

All eleven XFS retained-lookup mutation APIs now join an explicit readonly argument with the native mount flag at the existing admission check. All 24 VFS mutation calls pass their selected LookupHandle policy. The four handle-based TMPFS rename/link calls and four XFS pair calls combine source and destination restrictions. Native resolved-path fallbacks retain their same-mount behavior. Default false preserves existing direct backend callers. Invalid mount and cross-mount errors retain precedence; XFS readonly denial still precedes lookup validation, authorization, no-ops, inode/metadata locking and transactions. No shared mount flag, credential, lock, allocation or syscall/wire/Task/File layout changed.

Credentials/XfsLookupViewDenialsPreserveWritableBackendAndOutputs passes at 50.155 seconds. On a real writable XFS backend it exercises all eleven mutation denials as root and UID100, unchanged sentinel outputs and namespace booleans, mutation/lookup sequences, retained lookup validity, inode metadata, parent membership/time, xattr and data. Same-object rename, omitted timestamps and missing targets retain native EROFS precedence; invalid destination retains EINVAL. Positive native chmod, data write and mkdir/rmdir succeed afterward. Cross-mount EXDEV ordering is source checked, not separately exercised by this fixture. UID100 retains GID0 in this backend-admission test; it is not a service identity fixture.

Full isolated KTEST passes 53,848 assertions with zero failures and powers off at 177.791 seconds. Normal Build WOS and diagnostic build confirmation pass, as does scoped formatting/cache-disabled clang-tidy. The new source gate checks all eleven declarations/admission branches and every VFS call. Final credential (77 definitions invoked), lookup-contract, remote-VFS, XFS metadata/locking/rollback and manifest results are recorded in validation.json. Original build completion tool handles were unavailable after context recovery, so incremental confirmation builds were run; the KTEST launcher exit status was not recovered. Serial completion/poweroff and a host PID/start-time audit independently confirm both owned processes gone and no WOS disk holder at 2026-09-14T08:49:49.547792+00:00.

Previous 9IK is progress. Goal remains active, 11 enforced / 10 pending. Inherited directory/export boundaries, fallback/cache admission, removed-cwd handling, negotiated owner grants and receiver adoption remain unfinished; original remote cwd identity/client-view failures are unresolved. No peer mount-hierarchy retry ran. Evidence: build/goal19-xfs-lookup-policy/validation.json, review.md, scoped.patch and before/ snapshots. Original goal command and --no-setup authorization note are preserved.


## 9IM — bind lookup validation to the retained cwd anchor identity

Lookup authorization now captures an immutable, never-reused local cwd-anchor serial under the canonical filesystem owner's path lock. Existing single/pair request validators and final chdir publication compare it alongside root/cwd hashes and the admitted Snapshot. Same-text anchor replacement can no longer pass those checks. The private factory uses relaxed atomic CAS for uniqueness, reserves zero for no anchor, and permanently refuses allocation after serial exhaustion rather than wrapping. All native/remote anchor construction uses that factory. The serial is local bookkeeping, never an owner-issued transferable grant. Lookup move/reset clears retired authorization values.

No new filesystem reference is retained by lookup authorization, and validation adds no allocation, lock, blocking or final-release path. Anchor refcount, mount pin, backend ownership and remote cancellation lifetimes remain unchanged. Task/File, credential, syscall and WKI wire layouts are unchanged; internal anchor and lookup authorization each gain a uint64_t. Existing bounded EAGAIN retries now cover same-directory anchor refresh too.

The new TMPFS and XFS regressions pass at 51.535 and 56.185 seconds. Their 16 deterministic combinations cover root/UID-GID100, owner/worker requests and component/fallback acquisition. Real change_directory_at replaces the cwd anchor while root/cwd hashes, native inode, mount and namespace stay unchanged. Old single/pair and mixed old/fresh pairs reject before their stat consumers run, preserve sentinel outputs and take no additional anchor reference; fresh/moved lookups consume successfully and reset clears identity. An isolated counter checks exhaustion/non-reuse. These are deterministic production-helper interleavings, not simultaneous thread stress or a new userspace syscall fixture. Existing fchdir and canonical filesystem-context final-release tests also pass, including remote callback release outside path/publication locks.

Normal Build WOS and isolated diagnostic build pass. All78 credential source tests, lookup-contract/mount-lifetime/remote-VFS/manifest gates and scoped format/cache-disabled clang-tidy pass. Full isolated KTEST passes 53,850 assertions with zero failures, powers off at182.639seconds, and launcher exits0. PID/start-time audit at2026-09-14T09:03:29.583773+00:00 confirms both owned processes gone, no WOS VM and no normal/isolated disk holder. Evidence: build/goal19-lookup-cwd-identity/validation.json, review.md, scoped.patch and before/ snapshots.

Previous9IL is progress. Goal remains active with11enforced/10pending domains. Inherited directory/export boundaries, view-aware fallback/cache admission, removed-cwd adoption, negotiated owner grants and receiver adoption remain unfinished; original remote cwd identity/client-view failures remain unresolved. No peer mount-hierarchy retry ran. The original goal command and --no-setup authorization note are unchanged.


## 9IN — recover native directory membership beneath a retained root

Added vfs_directory_path_beneath_directory and native TMPFS/XFS helpers. The query accepts caller-retained root/directory Files on one native mount and recovers a relative name by following authoritative parent membership to the exact root object. Root rename/name replacement cannot substitute a different boundary. A live outside directory returns EPERM; removed roots/directories return ENOENT; invalid, non-directory, cross-mount and short-output cases preserve output bytes and length. The query stages its result under existing namespace/backend locks and rechecks mount generation/retirement before copying. It grants no DAC authority and creates no new File ownership. Returned text is only a point-in-time observation; future operations must retain and revalidate the boundary and namespace.

TMPFS shares its existing path recovery loop with an explicit strict mode. XFS shares its authoritative '..'/indexed-parent-name recovery with an optional exact retained boundary inode; the boundary cache pointer, type and liveness are checked before walking. Existing unbounded recovery delegates with false/null and keeps its prior behavior. No credential, syscall/wire, Task/File layout or lock-order change. Cross-mount ancestry still needs covered-mount object ownership; this helper deliberately exposes only the same-mount primitive and does not activate an incomplete inherited view.

Credentials/RetainedDirectoryBoundarySurvivesRenameAndRejectsOutsideObjects passes at50.489seconds. It executes36 boundary queries across real TMPFS/XFS mounts with pathless search-only Files: root equality, root rename/name replacement, inside rename, move outside/back, removed child/root, and invalid/type/cross-mount/capacity errors. Error sentinels and File references are unchanged. Recovery still works after revoking root search permission, while the separate operation using an explicit UID/GID100 Snapshot returns EACCES. This qualifies kernel membership/admission helpers, not a new service process or multi-node transfer fixture. Existing native mount-policy, XFS view-denial and cwd-identity regressions also pass.

The initial KTEST launcher exited1 before boot because libguestfs could not access /dev/sda2 for mkfs.xfs immediately after adding the secondary partition; qemu-img metadata validation passed and no VM remained. The KTEST-only packaging branch now syncs/closes that appliance and reattaches before formatting, preserving the existing separate seeding appliance. The retry packages and boots successfully. Normal rootfs layout/format behavior is unchanged. The original failure log is retained as ktest-launch.log; final launch is ktest-launch-final.log.

Normal Build WOS passes, including a final run after the packaging repair. Diagnostic build, all79 credential source tests, lookup/mount/remote-VFS, XFS metadata/locking/rollback/inode, qcow/rootfs and manifest checks pass. Scoped format/cache-disabled clang-tidy and shell syntax validation pass. Full isolated KTEST passes54,024assertions/0failures, powers off188.111seconds, and final launcher exits0. Host PID/start-time audit at2026-09-14T09:21:41.470394+00:00 confirms both final owned processes absent, no WOS VM and no normal/isolated disk holder.

Previous9IM is progress. Goal remains active,11enforced/10pending. Owned view attachment/lifetime, cross-mount ancestry, cwd/dirfd/cache/fallback propagation, removed-cwd adoption, owner-issued transferable grants and receiver adoption remain unfinished. The original remote cwd identity/client-view failures remain unresolved. No peer mount-hierarchy retry ran. Evidence: build/goal19-directory-boundary/validation.json, review.md, scoped.patch and before/ snapshots. Original goal command and --no-setup authorization note are unchanged.


## 9IO — own native directory boundaries and inherited policy

Added DirectoryViewRef, an immutable reference-counted description retaining search-only root/cwd Files and explicit inherited readonly/nosuid policy. Copies share ownership; narrowing retains independent Files, verifies the new root lies beneath the previous root, and unions restrictions. It creates no parent-view ownership chain. Native mount flags remain selected per lookup; base preparation requires its trusted caller to supply already-admitted inherited policy. Borrowed Files do not represent the complete view authority.

Preparation captures one credential Snapshot and stages allocation outside namespace publication. Native TMPFS/XFS membership helpers optionally check current cwd search under their existing metadata lock. Existing identity-only recovery retains its previous behavior, including no new search requirement on retained cwd ancestors. Admission uses exact retained objects, rejects widening, outside/cross-mount/invalid pins, and checks mount generation/retirement. Failure preserves output and balances references. Output replacement and candidate destruction run after admission unlocks. No syscall, wire, Task or File layout changes; final release remains a caller-context obligation outside path, registry and publication locks.

New TMPFS/XFS KTEST regressions pass at 45.836 and 50.781 seconds. They cover all four base policy combinations on both native backends, policy union with source/output aliasing, UID/GID100 current-search denial, ancestor-independent cwd admission, unchanged error outputs/references, source-pin closure, rename/name replacement, and rejected data reads/writes. Actual final native closes check path/publication locks are free, including replacing the last output and root==cwd ownership. No new registry-lock, service-process or multi-node runtime test is claimed.

Normal Build WOS and isolated diagnostic build pass. All80 credential source tests, lookup/mount-lifetime/remote-VFS, XFS metadata/locking/rollback and manifest gates pass. Scoped formatting/cache-disabled clang-tidy passes with nonfatal existing style diagnostics. Full isolated KTEST passes54,026assertions/0failures, powers off193.736seconds, and launcher exits0. Host audit at2026-09-14T09:43:30.354535+00:00 confirms no WOS VM and no normal/isolated disk holder. The VM exited before its start ticks were recorded; no per-process lifetime audit is claimed for this run.

Previous9IN is progress. Goal remains active,11enforced/10pending. This description is not yet attached to File, DirectoryAnchor, LookupHandle or Task and is not a transferable wire grant. Complete propagation/current-consumption validation, cross-mount ancestry, removed-cwd adoption, owner-issued grants and receiver adoption remain required. Original remote cwd identity/client-view failures remain unresolved. No peer mount-hierarchy retry ran. Evidence: build/goal19-owned-directory-view/validation.json, review.md, scoped.patch and before/ snapshots. Original goal command and --no-setup authorization note are preserved.


## 9IP — release serialized lookup captures after namespace publication

The serialized open fallback previously declared its LookupHandle after the namespace publication guard, while the nested component walker owned cwd/dirfd captures. Descriptor withdrawal could make either the successful handle or an early-error capture run its final File close under publication. This includes a remote dirfd declined before backend dispatch. This is a source-derived defect; no separate baseline runtime is claimed.

LocalWalkReferences now holds the cwd/dirfd captures. Ordinary component walks use a local holder; the sole serialized caller supplies an empty outer holder declared with its LookupHandle before publication. Successful acquisition transfers the retained dirfd to the handle; errors keep it in the holder. Both release after unlock. Empty-holder admission prevents replacement cleanup under the guard. The complete native walk and consumption remain serialized, preserving the admitted Snapshot, context hashes/anchor identity, generations, backend selection, errno and remote-cwd refusal. No new allocation, lock, queue or File/Task/LookupHandle/credential/syscall/wire layout change.

TMPFS/XFS descriptor-retirement regressions pass at60.834/65.540seconds. Twelve deterministic interleavings cover successful stat, consumer EIO, missing intermediate target, UID/GID100 search EACCES, non-directory ENOTDIR and synthetic REMOTE backend decline EAGAIN. The tests withdraw the original descriptor and reuse its number after production retain or inside consumption. The original closes exactly once with namespace publication, path and FD locks free; replacement ownership remains one reference. Rejected acquisition invokes no consumer and preserves output sentinels. Native callbacks delegate actual backend close. Synthetic REMOTE tests callback placement without RPC/cancellation; native cwd holder retirement is source checked. No concurrent thread stress, new service-process or multi-node fixture is claimed.

Normal Build WOS (RelWithDebInfo) and isolated diagnostic build (Debug) pass. All81 credential source tests, lookup/mount-lifetime/remote-VFS and manifest gates pass; scoped formatting/cache-disabled clang-tidy passes with nonfatal existing diagnostics. Full isolated KTEST passes54,028assertions/0failures, powers off198.789seconds, and launcher exits0. Final host audit confirms both recorded launcher/VM PID-start identities gone, no WOS VM, and normal/isolated disks free. Evidence: build/goal19-serialized-lookup-retirement/validation.json, review.md, scoped.patch and before/ snapshots.

Previous9IO is progress. Goal remains active,11enforced/10pending; --require-complete still exits1 at pending exec. Owned directory-view propagation through File/cwd/lookup, current-boundary consumption checks, cross-mount ancestry, removed-cwd adoption and negotiated owner grants/receiver adoption remain unfinished. Original remote cwd identity/client-view failures remain unresolved. No peer mount-hierarchy retry ran. Original goal command and --no-setup authorization note are preserved.


## 9IQ — retain directory-view boundary and policy through lookup admission

Added vfs_acquire_directory_view_lookup for an explicitly admitted owner-relative native view. It retains the view and one Snapshot, validates current root/cwd membership and cwd search, recovers the root object's current physical mount key, and performs the existing owner-confined component walk under namespace publication. A scoped epoch rejects different mounts before backend binding/stat. Absolute symlinks keep existing owner physical-confinement semantics; absolute input names and text/cache fallback are refused. This is not Task-visible root/cwd installation. Output replacement happens after unlock and errors preserve prior output, including source/output aliasing.

LookupHandle now owns the complete DirectoryViewRef, moves/releases it with its other references, and unions inherited readonly/nosuid bits with selected native mount flags. Empty-view native handles keep their previous policy branch. Single and paired consumers revalidate retained view membership/search after existing context/mount/namespace/backend checks and before callbacks. Scoped handles must be retired outside path, registry and publication locks. No File/Task/credential/syscall/wire layout change; internal lookup state gains the view reference and the search-generation value described below.

Source review also found that TMPFS directory permission/ownership changes did not invalidate a path lookup that had already searched through an intermediate ancestor. Changed directory UID/GID/mode now advances a separate atomic search-authority generation under tmpfs_lock. Both VFS component and fallback acquisition capture it before search; request/ambient consumption and final chdir publication reject changed admission with EAGAIN. Direct backend membership handles retain their existing current-DAC contract. Regular-file metadata and unchanged directory authority do not advance it. XFS already invalidates relevant metadata transactions. The counter conservatively invalidates VFS admissions globally on TMPFS directory authority changes. The gap was identified in source; no separate unchanged-kernel runtime reproduction is claimed. Component-view runtime is qualified; fallback capture and final chdir comparison are source checked here.

New TMPFS/XFS regressions pass65.745/70.694seconds. They exercise16 backend/identity/policy combinations: four inherited policies with root and explicit UID/GID100 Snapshots, single/pair consumption, native readonly chmod denial with unchanged mode and writable/ownership controls, relative/permitted physical-absolute symlinks, parent traversal, outside and nested-mount denial preserving output/references, move/reset, current cwd search revocation and retained cwd below an unsearchable root. Intermediate search revocation rejects old single/pair requests without callbacks or sentinel changes and fresh acquisition returns EACCES. The final handle survives source-pin/view closure and root rename/name replacement, rejects its stale path, and reacquires through its own view while replacing itself. Actual last native closes observe path/publication locks free. Nosuid is carried-policy evidence; no new set-ID userspace exec, concurrent thread stress, service process or multi-node fixture is claimed.

Validation retained three earlier full runs. First:54,029/1, because the fixture incorrectly expected XFS readonly-first errno from TMPFS; TMPFS ownership-first EPERM is now preserved. Its brace-format diagnostic was corrected to the local logger's printf format. Second:54,028/2, because the test's raw chmod callback omitted the cache notification normal VFS consumers send before cached stat; that callback now follows the existing contract and mode assertions remain. Third:53,919/13, while the new view tests passed; using TMPFS namespace generation invalidated direct backend handles that intentionally perform fresh DAC checks. The implementation was refined to separate VFS search admission from backend membership. Those existing backend tests were not changed.

Final Normal Build WOS (RelWithDebInfo), diagnostic build (Debug), all83 credential source checks, lookup/mount/remote-VFS/TMPFS-link/manifest checks and scoped cache-disabled format/clang-tidy pass. An additional TMPFS-link source assertion was already stale before this stage: its expected unlink signature omitted mount->read_only; the exact assertion was updated, retaining all ordering checks. Full final isolated KTEST passes54,030assertions/0failures and powers off203.499seconds; launcher exits0. Final host audit confirms tracked launchers/VMs absent, no WOS VM and no normal/isolated disk holder. The second-run VM's start ticks were not captured; its launcher exit and host scan establish shutdown. Evidence: build/goal19-directory-view-lookup/validation.json, initial-run records, review.md, scoped.patch and before/ snapshots.

Previous9IP is progress. Goal remains active,11enforced/10pending; --require-complete still exits1 at pending exec. Full File/DirectoryAnchor/Task view publication, process-root semantics, cross-mount covered-directory ancestry, removed-cwd adoption, negotiated owner-issued grants and mapped receiver adoption remain unfinished. Original remote cwd identity/client-view failures are unresolved. No peer mount-hierarchy retry ran. Original goal command and --no-setup authorization note are preserved.


## 9IR — derive independent directory views from admitted lookup targets

Added vfs_rebase_directory_view for an explicitly scoped native lookup. It consumes the existing Task/Snapshot admission under namespace publication, pins the selected directory with current native search permission, then releases publication before narrowing the source view onto that detached pin. The second admission checks exact root membership and current target search again. Errors preserve the previous output; staged pins and replaced views retire after unlock. The result owns root/directory Files directly, without predecessor-view chains or File/view cycles. Native mount flags remain on the detached identity File and inherited readonly/nosuid restrictions remain in the immutable view. No File/Task/credential/syscall/wire layout change or process-wide view activation is introduced.

TMPFS/XFS regressions pass at 65.994/71.127 seconds. They exercise 256 repeated rebases across both backends, root and UID/GID100 Snapshots, and all four inherited-policy combinations. Mount File counts stay bounded after predecessor lookup release; selected native inode and search-only access are preserved. Non-directory, current-search and stale-admission errors preserve output and ownership. Deterministic post-pin rename outside the root and permission revocation reject the new view and close its temporary File after unlock. Source/output aliasing, external source-pin closure, root rename/name replacement and actual final native closes are covered. No new concurrent thread stress, service-process, multi-node or set-ID exec evidence is claimed.

The first full run completed with 54,031 passes and one TMPFS fixture failure. Its close observer changed File::fops, which native fstat uses to distinguish TMPFS from pipe/epoll objects. The fixture now exposes the native table during stat and restores the observer afterward; identity/lifetime assertions and production code are unchanged. Initial logs, source, patch and hashes are retained. Final Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), all 84 credential source checks, lookup/mount-lifetime/remote-VFS/TMPFS-link/manifest checks and scoped cache-disabled format/clang-tidy pass. Full corrected KTEST passes 54,032 assertions with zero failures, powers off at 209.792 seconds, and its launcher exits 0. Final host audit at 2026-09-14T11:06:37.237882+00:00 confirms recorded launchers/VMs gone and normal/isolated disks free.

Previous 9IQ is progress. Goal remains active, 11 enforced/10 pending; the complete gate still fails at pending exec. File/DirectoryAnchor/Task publication, process-root semantics, cross-mount ancestry, removed-cwd adoption and negotiated owner grants/receiver adoption remain required. The original remote cwd/client-view failures remain unresolved. No peer mount-hierarchy retry ran. Evidence: build/goal19-directory-view-rebase/validation.json, initial-run.json, review.md, scoped.patch and before/ snapshots. Original goal command and --no-setup authorization note are preserved.


## 9IS — separate directory identity retention from search admission

Directory read-open and child search require distinct admission: a directory with mode0444 can be opened and listed while child traversal denies execute/search. Attaching the previous always-search rebase directly to directory OPEN would add a permission requirement. vfs_rebase_directory_view now offers explicit IDENTITY_ONLY admission while retaining SEARCH as its default. Native TMPFS/XFS pins and final view admission omit only target search in identity mode. Parent/path search, directory type, stale Task/Snapshot/mount/backend checks, exact root membership, inherited restrictions and ownership remain mandatory. Invalid admission values reject before allocation. Existing base/narrow factories and native pin callers keep search by default; public OPEN behavior is unchanged.

No admission mode or permission grant is stored in DirectoryView. Every later view lookup and consumer still checks current directory search. The detached pin remains mode3/O_DIRECTORY, granting no data or enumeration access; OPEN and CHDIR must authorize their operations independently. Existing staged ownership, independent root/directory references and output preservation remain intact. Final release happens after publication unlock. No File/Task/credential/syscall/wire layout change or view publication is introduced.

Native TMPFS/XFS regressions pass at 66.380/71.401 seconds. Actual UID/GID100 read-open and public directory listing succeed at0444; default rebase and child search deny. Explicit identity retention preserves inode and readonly/nosuid policy but data and directory-entry reads through its pin return EBADF. Chmod grants and revokes later search, old lookup consumption returns EAGAIN without callbacks, root has an explicit positive control, and a new read-open at0000 denies despite successful identity retention. Parent search revocation remains enforced. Deterministic post-pin moves outside the root reject in both admission modes and close temporary Files with path/publication locks free. These are kernel fixtures, not new service-process, concurrent thread, multi-node or set-ID exec evidence.

Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), all85 credential source checks, lookup/mount-lifetime/remote-VFS/TMPFS-link/manifest gates and scoped cache-disabled format/clang-tidy pass. Full isolated KTEST passes54,034 assertions/0 failures, powers off at215.210 seconds, and launcher exits0. Final host audit at2026-09-14T11:23:29.445783+00:00 confirms exact tracked launcher/VM identities absent, no WOS VM and normal/isolated disks free. Evidence: build/goal19-directory-view-admission/validation.json, review.md, scoped.patch and before/ snapshots.

Previous9IR is progress. Full Goal19 remains active,11enforced/10pending; the complete gate still fails at pending exec. This is required admission groundwork for directory File/cwd publication, which remains unfinished alongside process-root semantics, cross-mount ancestry, removed-cwd adoption and negotiated owner grants/receiver adoption. Original remote cwd/client-view failures remain unresolved. No peer mount-hierarchy retry ran. Original goal command and --no-setup authorization note are unchanged.


## 9IT — preserve owned views through native directory File/cwd publication

Native directory Files and cwd anchors now retain a complete immutable DirectoryViewRef. OPEN stages an independent identity-only rebase, while CHDIR requires current search. Detached identity Files cannot themselves carry a view, avoiding cycles and predecessor chains. Later relative lookup retains the exact view, checks current search/membership, intersects the current Task/export root, and combines inherited readonly/nosuid with selected native mount flags. Empty-view native mount crossing remains unchanged. Scoped routing/discovery, synthetic fallback and other-mount binding cannot discard the view. Process absolute symlinks preserve logical-root semantics; physical owner requests retain physical confinement.

Task-aware kernel File opens share openat admission without allocating a temporary userspace FD. Early no-Task loading and the kernel mode/no-extra-umask convention remain. Required direct-output path storage is allocated before create/truncate effects. Optimistic and serialized opens own staged views/pins beyond publication guards; failed/replaced views close native Files after unlock. Existing native cwd backend handles and mount accounting are preserved. No UID/GID, saved-ID, credential, syscall or WKI wire layout change.

The first runtime run completed54,034/2: both new backend cases failed at the first relative open. Review found physical-root-prefix stripping applied to process-relative owned-view input; the condition now explicitly requires physical_path. Initial source, hashes, launch/serial/QEMU logs remain preserved. Earlier missing forward-declaration build failures and source-inventory/helper-location failures are also recorded. Assertions were retained; source tests now verify public-wrapper delegation and the shared helper.

Final Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), all87 credential source checks, seven additional VFS/TMPFS/remote-VFS source suites, manifest audit, and scoped cache-disabled format/clang-tidy pass. Full corrected KTEST passes54,036 assertions/0failures and automatically powers off at220.664 seconds; launcher exits0. New native TMPFS/XFS publication fixtures pass64.871/72.536 seconds across root/UID-GID100 and four inherited policies: read-directory versus search admission, relative/absolute-link paths, Task-root clamping, outside/cross-mount denials, creation/no partial state, actual serialized fallback, duplicate/removed/reused FDs, cwd/root rename and name replacement, current search revocation, direct File opens without FD allocation, bounded mount counts and actual last closes with path/publication locks free.

A fresh disposable single-node normal VM booted services. Live kernel build IDs match before and after the native filesystem/mapping fixture, cross-compiled in this increment. All72 cases pass across TMPFS/XFS as root and verified UID/GID100; fixture namespaces and uploaded image clean up, final sync succeeds and launcher exits0. These are ordinary native empty-view compatibility checks, not imported-view or multi-node qualification. Final host audit at2026-09-14T12:19:13.135018+00:00 confirms all six exact launcher/VM identities gone and normal/isolated disks free.

Previous9IS is progress. Goal19 stays active,11enforced/10pending; --require-complete still fails at pending exec. Intermediate PROCFS fd/cwd links still restart through text and need exact owned-view capture before imported views can be activated; the serialized component walker already declines PROCFS before reading it. Covered-mount ancestry, removed cwd, process-root installation, negotiated transferable owner grants, authenticated receiver adoption and the original remote cwd/client-readonly failures remain unfinished. No peer mount-hierarchy retry ran. Evidence: build/goal19-directory-view-publication/validation.json, review.md, remaining-consumer-map.md, scoped.patch and before/ snapshots. Original goal command and appended --no-setup authorization are preserved.


## 9IU — preserve directory views through PROCFS magic-link traversal

PROCFS component traversal now captures either an owned native directory view or compatibility text from one selected FD/cwd through procfs_readlink_view_as. It shares existing private access and target-credential admission. FD scope is copied from the exact retained File without inspecting its possibly stale pathname; optional cwd scope uses one canonical root/cwd/anchor capture and validates that context before output. This avoids checking one descriptor for a view and then reading a reused descriptor or newer cwd in a second text operation. Raw READLINK remains text-only; final FD OPEN still shares the original open description. Empty-view links preserve their compatibility behavior.

A captured scope binds the component walk directly to the retained native directory, intersects its boundary with the caller's root, and walks the remaining components without text restart/rerouting. LookupHandle owns the selected ProcFile metadata and target credential generation separately from its native backend handle. Acquisition and single/paired consumers revalidate that origin before publication or native effects. Target credential ABA invalidates commit even when the caller Snapshot equals its old value again; descriptor withdrawal/reuse after capture cannot redirect the request. Move/reset retains/releases the origin with the view outside publication. Existing serialized lookup declines PROCFS before reading it; no new PROCFS capture or final File/Task release occurs under its guard. No UID/GID, saved-ID, credential, syscall or WKI wire layout change.

Normal Build WOS (RelWithDebInfo), isolated diagnostic build (Debug), all89 credential source checks, seven additional VFS/TMPFS/remote-VFS source suites, manifest audit and scoped cache-disabled formatting/clang-tidy pass. Full corrected KTEST passes54,038 assertions/0failures and automatically powers off at230.700 seconds; launcher exits0. New TMPFS/XFS magic-link fixtures pass72.066/81.661 seconds across root/UID-GID100 and four inherited policies. They exercise fd/cwd traversal, readonly creation and unchanged denied state, raw readlink/nofollow and final shared FD reopen, native realpath, root rename/name replacement, current search revocation, outside/cross-mount refusal, cwd OPEN/CHDIR, exact FD withdrawal/reuse after capture, private target access, credential ABA rejection in single and both paired orders with zero callbacks, origin move/reset, bounded reference accounting and actual native closes outside path/publication locks.

The initial full run completed54,036/2, both at the new deterministic FD-reuse fixture. An earlier free descriptor slot prevented lowest-free allocation from necessarily reusing the requested number. The corrected fixture occupies that slot through withdrawal/reuse and restores the original description. Additional realpath, rename/name-replacement and cross-mount coverage was added. Production core outside the fixture is byte-identical between runtime attempts, verified in fixture-only-correction.json; initial source, kernel hashes and logs are retained. Source assertion updates preserve prior holder/epoch checks and account for the added origin and dynamically narrowed mount scope.

A fresh disposable single-node normal VM ran the existing service_procfs_stat fixture, cross-compiled in this increment. All2,244 native PROCFS metadata checks across16 observer profile/phase combinations pass, including private denial outputs, target dumpability changes and retained metadata. Live kernel build IDs match before/after, fixture cleanup and final filesystem sync succeed, and launcher exits0. This is native userspace compatibility evidence; imported-view and multi-node qualification remain separate. Host audit at2026-09-14T12:50:25.128793+00:00 confirms all six exact test launcher/VM identities absent and normal/isolated disks free.

Previous9IT is progress. Goal19 remains active,11enforced/10pending; --require-complete still fails at pending exec. Cross-mount covered-directory ancestry, removed cwd, process-root identity/installation, negotiated transferable owner grants, authenticated receiver adoption and exact cancellation/retirement remain pending. The original remote cwd/name-replacement and client-readonly failures remain unresolved. No peer mount-hierarchy retry ran. Evidence: build/goal19-procfs-directory-view/validation.json, review.md, scoped.patch, fixture-only-correction.json and before/ snapshots. Original goal command and appended --no-setup authorization are preserved.

### 9KI: automatic root-withdrawal recovery

9KI: the serialized automatic mount worker now defers exact-path and descendant successors while an anchored retired proxy still owns its mount row, preserving the live resource observation and retry budget. Existing exact-owner retirement, busy-reference checks, credentials and wire/syscall ABI remain unchanged. Fresh baseline reproduced old root4096/gen7 EBUSY with zero open/lifecycle refs and four live successor children beneath it; read-only matched-symbol inspection proved that dependency. Candidate removes4096, publishes root4100 and restores both ordinary node3 procfs cat commands that previously returnedENOENT. Both live candidate IDs match1a94e2a63089331bd67fdb1b9a6b6150163e60e5. Normal Build WOS52315/0, isolated Debug52385/0, scoped format/tidy70751+44229/0 and12 final host suites86244/0 pass. Initial isolated build89492 failed on a test-only missing device argument; corrected evidence retained. KTEST91841/0 passes54218/0 at439.551 and powers off439.552; new real parent/child regression passes286.656. All private VM/launcher identities are gone and no WOS VM remains at2026-09-15T07:55:18.141914+00:00. Evidence build/goal19-retired-root-admission/validation.json.

The automatic-worker predicate is an observation, not a publication lease for manual mount callers; broader mount concurrency remains pending. A separate nested on/uname command is refused on its first node3 VFS_REF load of /usr/bin/on with UNAUTHORIZED5 before task acceptance; no return hop was established. Source open versus metadata/read still needs diagnosis. General native-source/owned-root/third-owner transfer and all other pending authorization domains remain in scope. No excluded peer hierarchy cleanup or packet campaign ran. Goal stays active11 enforced/10 pending; completion still fails at exec.

### 9KJ: mapped open absolute-symlink namespace

9KJ: mapped OPEN_READ_AS/OPEN_AS now interprets absolute symlinks against the visible export name retained with the authenticated binding, then walks beneath the original physical export root. Native owner calls and relative named exports preserve their physical-link defaults. Prefix validation is bounded; raw link components retain ancestor search, parent confinement, exact target/credential admission and denied-output/mutation invariants. No wire/syscall or credential-publication change. Matched baseline trace proves /usr/bin/on -> /usr/bin/wkictl was rejected against physical /rootfs before owner read/type admission. Candidate receiver-local on and locally aliases now both run uname successfully and report wos-3; direct remote wkictl and cat controls still pass. Both live candidate kernel IDs match64327454f6009d16ecec4abec220eb7288eb1bb4 and WOSDBG closes its session. The original onward-home command advances from first-hop on/EACCESS to second-image uname/ENOTSUP, so nested exec remains unresolved. Two bounded receiver-side admission probes did not hit; both were removed/detached. The initial inference that a particular scoped-directory guard caused that ENOTSUP is unproven and withdrawn. Normal Build WOS54656/0, isolated Debug17964/0, scoped cache-disabled format/tidy52019/0 and12 final host suites pass (126 credential definitions all invoked). Initial source-test assertions for old expressions/arguments and a misnamed new OPEN case were corrected without further production changes. Full isolated KTEST34995/0 passes54734/0 at446.125 and powers off446.126; new TMPFS/XFS alias/confinement cases pass12.538/17.265. Private candidate launcher84599 exits0. All tracked VMs/launchers are gone and no WOS VM remains at2026-09-15T08:38:42.657578+00:00. Evidence build/goal19-remote-exec-alias-admission/validation.json.

This qualifies mapped open and image-source aliases, not end-to-end onward exec or all mapped path operations. Goal remains active11/10.

### 9KK: onward exec pipe-proxy forwarding

9KK: matched live trace localizes the onward uname ENOTSUP to descriptor-export admission: fd0 is a held g_proxy_pipe_read_fops endpoint with TMPFS tag, rejected as an unsupported ordinary file before directory preparation or task submission. IPC-local classification now admits existing read/write pipe proxies through the existing fresh peer-bound exporter, preserving File ownership, access flags, source channel/active checks, all-or-none publication, CLOEXEC, and acceptance-controlled pumps. Generic VFS native-pipe classification and native-only capacity growth remain unchanged. Relay writes use a private nonblocking File view and one bounded invocation, so upstream credit pressure cannot block the shared flush worker or mutate another owner's descriptor flags. No wire/syscall or credential-publication changes. Native admission tests cover both proxy directions on fd0..3, CLOEXEC and reference preservation; the real zero-credit proxy writer returnsEAGAIN with no sent bytes, waiter or source-flag change. Normal Build WOS19670/0, isolated Debug51158/0, cache-disabled format/tidy37120/0 and5 focused host suites90858/0 pass. Full KTEST3517/0 passes54734/0 at445.313, powers off445.314; expanded bounded-write/admission cases pass319.693/319.928. Both live candidate kernels and symbols match3be53a41c91052e6e3fc8327d4b711d72912361e, WOSDBG session closed. All4 ordinary controls pass: original on wos-3 on wos-0 uname returnswos-0, return-hop cat preserves2496 input bytes exactly, empty cat preservesEOF, and third placement returnswos-3. Candidate launcher65280 exits0. All recorded baseline/candidate/KTEST processes are gone; no WOS VM remains at2026-09-15T09:02:00.288370+00:00. Evidence build/goal19-onward-exec-admission/validation.json.

The enabled pipe denial is resolved; this does not qualify every descriptor type or finish the exec domain. Goal remains active11/10.


### 9KL: mapped path namespace parity

9KL: mapped STAT/READLINK/REALPATH and descriptor-relative OPEN now use the same captured visible export namespace as mapped OPEN. The confined lookup helper validates that namespace before walking. Absolute link text is translated against the original physical export root before checking any narrower retained directory view, preventing selection of a different same-named object inside that view. One stable credential Snapshot, raw ancestor/parent checks, native physical defaults, nofollow target bytes and unchanged denied outputs remain intact. No wire/syscall, credential publication or lock-order changes. Normal Build WOS35939/0, isolated Debug37208/0, cache-disabled format/tidy50685/0 and11 final host suites pass. A source assertion was corrected for formatter line wrapping without changing production code. Full isolated KTEST4949/0 passes55462/0 at442.403 and powers off442.404; expanded TMPFS/XFS cases pass12.150/16.879. Fresh matched baseline remote stat-follow failsEPERM and canonical readlink fails; candidate all5 file/exec controls pass and canonical output is /wki/wos-3/usr/bin/wkictl. Both live candidate kernel IDs and symbols matchf4cc659a01cc01cbf0597ddba426cc71af267a74; WOSDBG session closed. Existing REALPATH fixtures pass384/384 on each of TMPFS and XFS across ungrouped/supplementary-group profiles, with cleanup complete. The fixture's stale /rootfs absolute-target assumption was corrected to the publisher's visible namespace. Candidate launcher72588 exits0. All recorded baseline/candidate/KTEST processes are gone; no WOS VM remains at2026-09-15T09:29:39.803788+00:00. Evidence build/goal19-mapped-path-alias-parity/validation.json.

Goal19 remains active:11 enforced/10 pending/0 documented legacy root checks; first pending exec. PIN and mutation namespace parity, transferable process-view contracts, general native-source/owned-root/third-owner filesystem transfer and the other pending authorization domains remain unfinished. No excluded peer hierarchy retry or packet campaign ran. The exact goal command and appended --no-setup authorization are unchanged.


### 9KM: mapped directory PIN namespace

9KM: remote directory PIN now passes the authenticated binding's captured export path/name into the retained-view lookup. Bounded normalization occurs before namespace locking. Absolute link text maps through that physical export origin and is then checked against the current retained inode boundary; relative paths retain their exact root/cwd identities after rename. Native and relative named-export defaults remain physical. One stable Snapshot, current search checks, independent search-only File ownership, inherited readonly/nosuid, exact channel publication and unchanged denied response/name remain intact. No wire/syscall or lock-order change. Existing owner-reference tests add root/user visible-alias and invalid-prefix cases on TMPFS/XFS, plus narrower-view outside/inside links and policy preservation; existing rename/replacement, permission and cancellation cases still pass. Normal Build WOS64969/0, isolated Debug7999/0, cache-disabled format/tidy78277/0 plus final remote-VFS79470/0 and fixture check, and11 host suites72040/0 pass. Pre-runtime review corrected two fixture mistakes (leading slash in returned name and reservation ID consumed by publication); both builds were rerun. Full isolated KTEST7365/0 passes55462/0 at442.170 and powers off442.171; expanded directory-reference case passes295.028. Both live candidate kernels/symbols match222f02fe9ad7f67ed1f183ecae40ef717c16a475, WOSDBG closed. Ordinary cwd fixtures pass29/29 on each of TMPFS and XFS, including root/UID1002 absolute-alias chdir, canonical cwd, rename/name replacement, fork/exec inheritance, source descriptor closure and permission revocation; both cleanups complete. Candidate launcher56734 exits0. All recorded KTEST and cluster processes are gone; no WOS VM remains at2026-09-15T09:50:17.645207+00:00. Evidence build/goal19-mapped-directory-pin/validation.json.

Goal19 stays active:11 enforced/10 pending/0 documented legacy root checks; first pending exec. Mutation and phased-XATTR namespace parity, general native-source/owned-root/third-owner process filesystem transfer, original outstanding exec evidence and the other pending authority domains remain unfinished. The next call-flow map is build/goal19-mapped-directory-pin/next-path-map.md. No excluded peer hierarchy retry or packet campaign ran. The exact goal command and appended --no-setup authorization are preserved.


### 9KN: mapped mutation namespace parity

9KN: mapped CHMOD/CHOWN/UTIMENS, MKDIR/SYMLINK/REMOVE and both RENAME operands now pass the retained authenticated export's visible namespace into the existing confined lookup. Scalar and descriptor-relative requests share the same translation and retained-boundary intersection. One immutable Snapshot remains outside retries; raw ancestor/parent search, final-follow intent, literal created symlink targets, readonly/nosuid, exact consume/pair revalidation and native physical defaults remain intact. No wire/syscall, backend commit, lock-order or credential-publication change. TMPFS/XFS native matrices cover root/grouped user, scalar/retained-dirfd/readonly views, positive mutations, invalid/outside/ancestor denials, unchanged denied metadata and directory entries, literal target bytes, and independently mapped rename source/destination. Normal Build WOS30598/0, isolated Debug78849/0, cache-disabled format/tidy84819/0 and11 final host suites pass; the new source assertion's local variable typo was corrected and the remote VFS suite rerun. Full isolated KTEST9015/0 passes59044/0 at453.190 and powers off; new TMPFS/XFS mutation cases pass17.225/28.744. Both live kernels/symbols match24890945feb31b1c31f47bd961cedc814d9106f3 and WOSDBG closes. Existing live fixtures now use absolute owner aliases: chmod48/48 and mkdir192/192 on each of TMPFS and XFS, including retained directory rename, owner/group policy, setgid/umask, nofollow and unchanged refusal cases. All480 live checks pass and all fixture cleanups complete. Candidate launcher39199 exits0. All recorded KTEST/cluster processes are gone; no WOS VM remains at2026-09-15T10:11:58.943354+00:00. Evidence build/goal19-mapped-mutation-namespace/validation.json.

Goal19 stays active:11 enforced/10 pending/0 documented legacy root checks; first pending exec. Phased XATTR still needs captured namespace lifetime across BEGIN/commit. General native-source/owned-root/third-owner process filesystem transfer, original outstanding exec evidence and other pending domains remain unfinished. The next source map is build/goal19-mapped-mutation-namespace/next-xattr-map.md. No excluded peer hierarchy retry or packet campaign ran. Exact goal command and appended --no-setup authorization are unchanged.


### 9KO: staged XATTR namespace lifetime

Mapped XATTR stages now own bounded copies of both the physical export root and its visible name from BEGIN through commit. Exact BEGIN replay compares both values; failed capture leaves outputs and the prior stage untouched. Path operations pass the saved visible namespace to shared confined VFS lookup. FD operations retain their existing held-File authority. One immutable credential snapshot, raw ancestor search, readonly/nosuid restrictions, final-follow behavior, replay/cancellation semantics and unchanged denial state are preserved. No syscall or wire format changes.

Native tests exercise actual staged GET/LIST/SET/REMOVE on TMPFS and XFS, root/user credentials, scalar and retained-directory paths, readonly views, absolute aliases, malformed/outside prefixes, copied-buffer lifetime and unchanged denied output/metadata. Attempt1 recorded 59,043 passed and one fixture setup failure: early KTEST has no current Task. The shared private dispatch now takes explicit worker context; production passes the current task and the fixture supplies an independent Task. The failed log and shutdown receipt remain in ktest-attempt1/. The corrected full isolated KTEST passes 59,044/0 at guest461.107, powers off, and launcher83858 exits0. The expanded XATTR test passes at307.869.

Normal Build WOS50769/0 and isolated Debug90628/0 pass. All11 host suites passed before the correction; the affected remote-VFS suite passes again after updating its exact signature assertion. Cache-disabled format/tidy93279/0 and correction95186/0 pass. Both live kernels and symbols match b9c497b4f6d331fe69edb446fbf710c45bb99301, and WOSDBG closes. The existing mapped XATTR fixture passes244 regular plus50 cwd-regression checks on XFS, including absolute raw-ancestor aliases and64KiB values; cleanup completes. Candidate launcher66016 exits0. All owned KTEST and cluster processes are gone; no WOS VM remains at2026-09-15T10:46:45.179608+00:00. Evidence: build/goal19-xattr-namespace-lifetime/validation.json.

Goal19 remains active with11 enforced/10 pending domains and exec first pending. This qualifies staged XATTR namespace lifetime; general process root/cwd transfer, mapped concurrency and other pending domains remain open. Return next to the original9FZ UID100 worker completion evidence, with prompt bounded capture before alarm cleanup; next-exec-evidence.md maps the current fixture and source entrypoints. The historical9FZ statement that proxy-descriptor re-export is unsupported is stale for held pipe proxies, which9KK now admits; that does not establish the cause of9FZ. No excluded peer hierarchy retry or packet campaign ran. The original goal command and appended --no-setup authorization are unchanged.


### 9KP: exec observation capture

The first pending domain remains exec. The previous turn made verified progress on staged XATTR; this turn returned to the original9FZ UID100 worker B-A-B completion failure. The unchanged current native return-home fixture was cross-built and run once for UID100 main and worker execution on two fresh private nodes. Both live kernels and symbols matched b9c497b4f6d331fe69edb446fbf710c45bb99301 through WOSDBG; the read session closed before fixture execution.

A bounded host watcher armed at each ADMISSION record and would capture a stage after2 seconds without an observation, at most once per stage. Its GDB capture validates exact VM PID/start/overlay identity and live build ID, reads bounded task state, saved frame chains, peer/channel state and pending wait metadata, and restores the initial VM run state. WOSDBG's current catalog lacks typed scheduler-task saved-stack decoding, so this part uses the existing read-only GDB approach. Baseline and final captures succeeded on both nodes; final pending-WKI-wait lists were empty. No missing-observation capture fired, so the full failure-triggered path remains unexercised.

All six stage observations passed with UID100/100/100, correct process-group/receiver identity and exactly one delivered signal. Both source children completed normally with exit23 and were reaped; all six receivers were verified absent and fixture files removed. This is current compatibility evidence, not an explanation or repair of9FZ. Analysis of the original matching ELF found that the stalled receiver's preserved R12 points to the literal "wki_wait" at0xffffffff803c6f94. This suggests a WKI wait path but cannot identify the operation or establish the cause without the missing caller stack. The saved RIP remains the instruction immediately after HLT in kern_yield_impl. Current wait completion distinguishes PENDING/CLAIMED/DONE and retirement; pending signals may remain while a kernel wait owns resources. Do not infer an authorization defect from those bits alone.

No production or fixture source changed. The initial standalone compile omitted the repository libc++ include path and failed; adding the current compile-database include path produced the tested binary. Normal Build WOS and KTEST were not rerun because the kernel/source hashes match the previously verified9KO candidate. Candidate launcher25748 exits0; all three owned processes are gone and no WOS VM remains at2026-09-15T10:57:33.949958+00:00. Evidence: build/goal19-exec-observation-capture/validation.json, watch-events.json, return-home/results.json and historical-register-analysis.log.

Goal19 remains active,11 enforced/10 pending, with exec first pending. Next qualify the outstanding mapped exec/credential lifecycle obligations using the now-armed capture whenever return-home execution is part of a relevant scenario; a further identical passing run cannot explain9FZ. On a missing observation inspect the exact pending wait state, task perf_wait_callsite/saved frame and peer/channel identity before cleanup. General native-source/owned-root/third-owner filesystem transfer and the other requirement-level obligations remain open. The original goal string and --no-setup authorization remain unchanged; no excluded hierarchy retry or packet campaign ran.


### 9KQ: current remote exec identity gap

A new focused fixture reproduces a current exec authorization/compatibility defect. Direct remote exec branches before native Snapshot::prepare_exec; both WKI submitters encode the original snapshot, and receiver mapping/construction installs it unchanged. Remote exec consequently retains obsolete saved IDs and ignores source executable set-ID bits. Native spawn already prepares credentials before placement, so a blanket receiver-side transition would double-apply that path.

The fixture uses a local set-ID seed image to establish UID1002/1002/1000 and GID2002/2002/2000 through WOS's supported public calls. Each fresh child then executes an owned plain, set-UID, set-GID, both-bit or NNP payload locally and remotely. All five local cases pass: saved IDs become resulting effective IDs, expected set-ID changes apply, NNP suppresses those changes, and obsolete saved IDs cannot be restored. All five remote cases fail: UID1002/1002/1000 and GID2002/2002/2000 remain unchanged, both obsolete-ID setters succeed, and requested set-ID transitions are absent. These are normal OS credential regression checks on fixture-owned files/processes; no packet campaign or excluded hierarchy operation ran.

Both live baseline kernels and symbols match b9c497b4f6d331fe69edb446fbf710c45bb99301. WOSDBG closes before testing. The initial fixture attempted unsupported setres-style setup and stopped before exec; its child was reaped and its own two files were removed. The corrected full baseline records10 cases/5 failures, all ten source children normally reaped, all reported payload processes verified absent, and complete fixture cleanup. The baseline and setup failure are preserved separately. Candidate launcher73219 exits0; all owned processes are gone and no WOS VM remains at2026-09-15T11:12:22.983859+00:00.

Changes are the new C++/Python parity fixtures and goal/audit/manifest evidence. Fixture cross-build, Python syntax and cache-disabled format/tidy pass. No production source changed; Normal Build WOS and KTEST were not rerun for this baseline. Evidence: build/goal19-remote-exec-id-parity/validation.json, baseline-corrected/results.json, baseline-corrected.log and patch-contract-next.md.

Goal19 remains active,11 enforced/10 pending, with exec first pending. The next patch must preserve original read/grant authority while deriving and admitting a separate replacement Snapshot exactly once, including saved IDs, set-ID, NNP/nosuid/ptrace and authenticated peer mapping. Replacing the sole serialized snapshot early could change image-fetch permissions; silently applying a transition to all existing spawn messages would also be wrong. The exact wire/capability and cutover design requires the remaining audit recorded in patch-contract-next.md. This demonstrated current defect takes priority over repeating the passing9KP historical-stall reproduction. The original9FZ cause and broader remaining domains stay open. Original goal text and appended --no-setup authorization are unchanged.


### 9KR: remote exec credential transition qualified

Direct remote exec now derives replacement credentials from retained executable ownership/mode, mount nosuid and tracing state while retaining the original admitted snapshot for image/interpreter reads and filesystem/IPC preparation. The process owner rechecks credentials after sibling quiescence, then revalidates tracing and atomically publishes the prepared snapshot at cutover. A prepublication error withdraws the owned commit waiter, preserves exact errno and rolls back without sending COMMIT. Both resulting and original identities must pass authenticated receiver policy before image work; the receiver publishes the replacement on its private prepared Task before READY. Already prepared spawn submissions keep their existing identity semantics.

The wire extension is the advertised process-exec-handoff-v2 resource (0x45584302) with a validated16-byte executable-metadata tail after IPC entries. Both INLINE and VFS_REF encode it; the receiver strips it before existing context/IPC parsing. Original full-credential and filesystem identity layouts remain unchanged. A peer lacking the new resource cannot silently accept the old handoff interpretation. No syscall ABI change was made; saved IDs always refresh to final effective IDs, including NNP/nosuid/traced suppression, and groups/authority ceilings remain inherited.

Normal Build WOS (RelWithDebInfo) and isolated diagnostic builds pass. Final KTEST reports59044 passed/0 failed and powers off at458.759 seconds. Its full-credential native case includes32 set-ID/suppression profiles, malformed/truncated metadata, unchanged output on rejection, and independent root replacement mapping refusal. Credential/wire host tests, all36 authenticated-policy host cases, five focused source gates, Python syntax and cache-disabled format/tidy pass. The earlier KTEST launch was intentionally interrupted to include the final post-quiescence recheck; its partial log and verified shutdown are archived and are not claimed as a passing run.

Both live kernels and symbols match0a16616c7d43a1c79ee5b363484c2e0f709475b8; WOSDBG closes before fixtures run. The original unchanged ten-case parity matrix now passes10/10, correcting all five9KQ remote failures. Additional main and worker matrices pass14/14 each, including executables readable by the original caller but unreadable after the set-UID or set-GID transition. All38 source children are normally reaped, all38 reported payload processes are verified absent, and every fixture directory is removed. Both test VMs and launcher exit; no WOS VM remains at2026-09-15T11:49:56.883240+00:00. Evidence: build/goal19-remote-exec-credential-transition/validation.json and its baseline-parity, extended-main and extended-worker results.

Exec remains pending: benign loader observations expose the next concrete gap. All20 main/worker local/remote cases with differing real/effective IDs report AT_SECURE=0 and secure_getenv exposes the harmless WOS_EXEC_OBSERVATION marker. Source review confirms all three stack builders omit AT_SECURE while mlibc uses it to select secure loader/environment behavior. No library injection or packet experiment was performed. Implement coherent secure-exec auxv from the final admitted credentials with a fresh patch contract; see next-exec-evidence.md. Live forced-INLINE, mixed-version peers, tracing/suppression races and late rejection rollback remain additional qualification work. The original9FZ cause and broader filesystem/other-domain obligations remain unresolved. Goal19 remains active,11 enforced/10 pending, first pending exec; original goal text and appended --no-setup authorization are unchanged.


### 9KS: coherent identity and secure-exec auxv

Native exec, native spawn and WKI received images now emit AT_UID/AT_EUID/AT_GID/AT_EGID/AT_SECURE through one value-only helper. The final admitted Snapshot supplies the loader identity. Existing real/effective UID or GID differences and effective sealed service grants select secure loader behavior; NNP does not erase existing authority, while a dropped ceiling makes those grants inert. Ordinary equal-ID root remains ordinary. Native staged images retain the stack Snapshot and compare the freshly prepared commit Snapshot before any image/debug publication; a tracing-related mismatch returns EAGAIN. Original read authority, saved-ID rules, groups/ceilings, stack alignment, denial state and actor retirement are preserved.

The advertised process-exec-handoff-v3 resource (0x45584303) guarantees this loader contract for ordinary remote spawn as well as exec. Both low-level submitters capture the capability and actual resource channel generation under one PeerLifecycleLease, compare filesystem admission against that token, then use generation-bound send. No allocation or blocking was added under that lease. No syscall or credential/submit byte layout changed from9KR; mixed-version runtime qualification remains pending.

Normal Build WOS and isolated diagnostic builds pass. Final full KTEST reports59,044 passed/0 failed at459.592 seconds, powers off, and launcher82377 exits0. The earlier run was deliberately interrupted for the final capability/channel correction and remains archived without a passing claim. Credential/wire host tests, five focused source suites and cache-disabled format/tidy pass. One existing nosuid source assertion was updated for the additional stack preparation; its initial failure is retained.

Both live kernels and symbols match7eccbed085de495f0c3e66257dcc3a17790a93b9, and WOSDBG closes. The corrected main and worker matrices pass18/18 each. The initial main fixture incorrectly expected service mask3 instead of3|SERVICE_PRIVILEGES_LOCKED (259); all its auxv/marker observations matched, and that failed run remains preserved. The complete posix_spawn matrix passes16/18. All52 executed payloads report matching identity auxv and secure_getenv behavior, are normally reaped and verified absent. Both failed remote spawns returnEACCES for setuid_unreadable/setgid_unreadable; their local counterparts pass, and source seeds are normally reaped. A rejected spawn reports no receiver PID, so this does not independently prove private receiver cleanup. All fixture files/directories are removed. Launcher59567 exits0; every recorded owned process is gone and no WOS VM remains at2026-09-15T12:32:57.117395+00:00. Evidence: build/goal19-secure-exec-auxv/validation.json.

The missing secure-exec auxv demonstrated in9KR is repaired and qualified. Exec remains pending because ordinary remote spawn captures the already-transitioned child's Snapshot as its sole read/mapping identity, unlike direct remote exec's separate original/replacement admission. The current fixture demonstrates the resulting refusal when the original caller can read an image but the resulting owner/group cannot. Preserve both authorities with a fresh contract; do not bypass read or peer policy or weaken the failing assertion. Source flow and remaining validation are recorded in next-spawn-evidence.md. Live forced-INLINE, mixed-version/restart, tracing/late-rejection evidence, the original9FZ cause and other pending domains remain open. Goal19 stays active,11 enforced/10 pending, first pending exec. No penetration testing, injected library or excluded peer hierarchy fixture ran. Original goal command and appended --no-setup authorization are unchanged.


### 9KT: remote spawn preserves original image-read authority

The two9KS remote posix_spawn EACCES failures are fixed. Spawn now retains one original read Snapshot and executable transition record while keeping the prepared private child under its final credentials. Both submitters validate that the original Snapshot plus retained metadata produces the child's exact current Snapshot before filesystem preparation/submission. Receiver policy independently maps the original and resulting identities before image/interpreter work, retains original authority during preparation, and publishes the final Snapshot before the child becomes runnable. Original image-read permissions are preserved even when the resulting owner/group class cannot read the executable.

SPAWN_TRANSITION flag8 uses the existing16-byte credential-transition tail and is distinct from EXEC_HANDOFF flag1; combining them or omitting required routed/full identity is rejected. Only actual exec handoff sets the READY/COMMIT state or invokes cutover. The negotiated process-exec-handoff-v4 resource0x45584304 advertises this distinction, retaining9KS capability/channel-generation binding. No syscall or credential-record layout changes, new locks, or blocking under locks. Original/final mapping, saved IDs, NNP/nosuid/tracing semantics, groups/ceilings, secure auxv, inherited descriptors, process reservation and rejection cleanup remain intact. Borrowed source metadata is used only during synchronous preparation/serialization.

Normal Build WOS5730/0 and isolated diagnostic34326/0 pass. Credential/wire host tests69322/0, five focused source suites and cache-disabled format/tidy19781/0 pass. A source assertion still required the old implicit credential capture; it now checks explicit immutable override/default capture, and the failed log is retained. Native32 set-ID/suppression profiles additionally verify exact original/final identity, unchanged output on mismatch, both transition flags and conflicting/missing-home rejection. Existing malformed metadata and independent root-replacement mapping refusal remain covered. Full isolated KTEST15768/0 passes59,044/0 at466.874, powers off, and its VM/launcher are confirmed down. The expanded credential case passes328.319.

Both live kernels and symbols match5378d96da98052dd8255eca506d648aeaab74d4b; WOSDBG closes before testing. The unchanged9KS fixture now passes all54 cases:18 spawn,18 main exec and18 worker exec. This includes both formerly failing original-read cases, plain/set-ID/NNP profiles, active sealed grants and inert ceilings, exact identity auxv and harmless secure_getenv marker behavior. All54 reported payloads are normally reaped and verified absent; every fixture directory is removed. Launcher56987 exits0. All owned processes are gone and no WOS VM remains at2026-09-15T12:55:33.497157+00:00. Evidence: build/goal19-remote-spawn-read-authority/validation.json and review.md.

Goal19 remains active,11 enforced/10 pending, with exec first pending. This supersedes9KS's current remote-spawn refusal; its failed baseline remains historical evidence. It does not establish mixed-version/restart or every enabled lifecycle interleaving, explain original9FZ, or close the other pending authorization domains. Next review exact enabled routes and contract evidence; unsupported features alone are not an authorization bypass and do not justify expanding the goal into unrelated functionality. No penetration testing, injected library or excluded peer hierarchy operation ran. Original goal command and appended --no-setup authorization are preserved.


### 9KU: exec/spawn version refusal and fallback qualified

Added ordinary exec, worker-exec and posix_spawn compatibility fixtures for plain, sealed-service and inert-ceiling credentials. Final corrected-path results pass54/54:18 strict ENOTSUP refusals across v4-to-v3 and v3-to-v4,18 permitted local fallbacks, and18 remote executions on matched v4 nodes. Refusals preserve all IDs, empty groups, NNP, grants, ceiling, the original CLOEXEC descriptor and the spawn PID output. Successes report expected identity/auxv, close the child descriptor, append one owned marker, return23 and leave no extra child. All reported processes are reaped/verified absent and all fixture files removed.

Every phase verifies both live kernel IDs and matching symbols through WOSDBG, then closes the debugger session. v4 is5378d96da98052dd8255eca506d648aeaab74d4b; v3 is7eccbed085de495f0c3e66257dcc3a17790a93b9. The first matched-current control exposed a fixture path assumption: unqualified /tmp follows receiver-local routing. The runner now explicitly names /wki/wos-0's owned image; that failure and cleanup remain preserved, and both mixed-version matrices were rerun with the corrected path. Initial unqualified mixed-version passing runs are retained separately. An optional resource-diagnostic read exceeded the command helper limit, then failed its SFTP completeness assertion; no complete diagnostic snapshot is claimed.

No production source changed. The new fixture cross-build, Python syntax and format check pass; no clang-tidy compilation unit matched these standalone files. Normal Build WOS and KTEST were not rerun: production/kernel hashes match the qualified9KT build, with the preserved9KS kernel used only for comparison. All five phase launchers exit0 and every owned VM is down; no WOS VM remains at2026-09-15T13:19:35.232173+00:00. Evidence: build/goal19-exec-version-boundary/validation.json.

This qualifies v3/v4 refusal/fallback and matched-version success after cold topology replacement. It does not prove in-flight peer restart/channel rotation, all delivery modes, arbitrary historic versions, or original9FZ's cause. Goal19 remains active11 enforced/10 pending, first pending exec. No packet campaign, injected library or excluded peer hierarchy fixture ran. Original goal command and --no-setup authorization remain unchanged.

### 9KV: cutover retirement ownership and receiver cleanup progress

Added a native `ExecCutoverRetirementPreservesCredentialAndWaitOwnership` regression around the actual `wait_for_exec_submit`, peer-retirement and credential-publication functions. Six ordered scenarios cover timeout before preparation, retirement before cutover entry, callback refusal, retirement winning callback refusal, retirement after successful credential publication, and ordinary spawn bypass. Refusal preserves the original Snapshot and exact callback errno; committed cutover retains the final Snapshot and cannot resume the old identity. The test verifies callback count, stack-wait consumer/responder withdrawal, terminal status, private task publication state and reference reclamation. It uses an absent synthetic peer and sends no network packet. These are ordered native cases, not concurrent scheduler runs or a full invocation of the real exec owner cutover callback.

The adjacent source audit found a receiver cleanup defect: the final session/cancel/deadline refusal, monitor-allocation refusal and failed scheduler-publication branches retained the exclusive peer publication lease while destroying the private process. Final VFS puts can synchronously cancel retained executable/directory tokens or close writable remote files. RX admission rejects the response while that exclusive lease remains held, delaying cleanup until timeout and deferring owner-side retirement. All three branches now release the lease after the compute lock and any inserted private monitor have been retired, before blocking cleanup. Successful publication remains under the lease; the child remains owner-linked through teardown and replies remain bound to the admitted channel generation. No credential policy, syscall/wire layout or errno changes.

The branch-order regression fails on the pre-fix source and also detects omission of each release independently (3/3 mutation checks). Normal Build WOS, the isolated diagnostic build, four focused source gates and cache-disabled formatting/clang-tidy pass. The initial native-only KTEST run passes 59,045/0 and powers off; the final production-fix KTEST run also passes 59,045/0 and powers off (launcher35613 exit0). All owned WOS VMs are down at2026-09-15T13:52:01.326968+00:00. Evidence: `build/goal19-exec-cutover-retirement/validation.json`. The KTEST primitive lease tests complement the source proof; no live forced receiver-rejection timing result is claimed. Broader enabled lifecycle/in-flight restart qualification and the unexplained historical9FZ observation remain open. The operation manifest remains 11 enforced/10 pending with exec first; the full goal is not complete.

### 9KW: live cancellation route disproves the proposed cleanup fixture

The previous9KV turn made progress through its production fix and full native validation. This follow-up tested the proposed ordinary owned-child cancellation route on two rootless VMs. WOSDBG verified the current9KV kernel on both nodes (build ID `5baee873c9b4882ecc9360fcdbd0f3fc06a9fc21`) and closed its debug session. The new experimental launcher/controller compiled; formatting passed after correcting the initial formatting-only failure. No production source changed, and the normal kernel hash remains the qualified9KV hash. Normal Build WOS and KTEST were not rerun in9KW.

The one root/main case reached the post-COMMIT hold, but the parent per-PID `kill(SIGKILL)` did not return within the 20-second observation window. It returned0 after the controller released the hold. Current `wki_proxy_task_signal_as` explicitly waits while the submitted row has `exec_cutover && response_pending`, then sends a credential-bearing `TASK_SIGNAL_REQ`; this is not the early `TASK_CANCEL` route assumed by the9KV next-audit note. Consequently no queued-cancellation or private post-failure witness was established. Moreover both complete before/held snapshots reported zero owner executable views and zero active owner file grants, so the selected workload did not establish a retained VFS close dependency. This is an inconclusive cleanup qualification, not a passing regression or evidence that9KV failed.

The host error path also sent a second cancel without first draining the delayed response and misparsed it as the reap result. Preserve that failure rather than claim parent wait/reap status. Follow-up observation confirms source PID111 absent, no fixture processes displayed on either node, publication hold/stage/waiters all zero and no payload marker. Only the two owned helper/controller files and their directory were removed. The unsuitable exact experimental fixture was moved out of `tests/fixtures` into `build/goal19-receiver-cancel-cleanup/inconclusive-fixture/` with its limitations. Do not rerun it as a gate. A future receiver-refusal qualification requires an independent cancellation trigger plus an observed retained VFS grant; expiration before lease reacquisition is still insufficient.

All owned VMs and launcher are down at2026-09-15T14:09:34.752118+00:00; launcher58737 exits0. The authority manifest remains11 enforced/10 pending, exec first. No blocker is asserted: safe source/native alternatives and other original goal requirements remain available. Evidence: `build/goal19-receiver-cancel-cleanup/validation.json`.


### 9KX: interpreter execute admission before cache and load

PT_INTERP previously received read admission but no execute check in native replacement or shared spawn/receiver construction. The owned-file baseline on archived kernel5baee873c9b4882ecc9360fcdbd0f3fc06a9fc21 reproduces root execution of a mode0644 interpreter: spawn returned PID125 instead of -EACCES. The fixture reaped that child before reporting failure; both baseline live build IDs/symbols were verified through WOSDBG and its session closed.

Native exec now checks retained interpreter metadata against the original request Snapshot before allocation/load. Shared construction performs the same central execute authorization with the private image task's original admitted/mapped Snapshot before any cache lookup. Metadata acquisition must succeed; lack of a cache freshness timestamp still permits authorized uncached loading. Optional internal error outputs propagate the new -EACCES denial through spawn and both remote constructors; receivers use existing UNAUTHORIZED. Existing unrelated failure conventions, interpreter-independent set-ID derivation, private-child ownership and blocking cleanup order are preserved. No new lock, allocation, Task layout, syscall ABI, wire field or protocol version.

Normal Build WOS, isolated diagnostic build, source gates and cache-disabled formatting/clang-tidy pass. An exact source assertion failed after formatting split its function call; normalizing whitespace fixed the assertion without changing production. Final isolated KTEST passes59,045/0 at461.466, powers off and launcher91835 exits0. The normal kernel and both live symbol identities matchfb84f1db5ab876b5909bf71da087b80c4c6c3a23.

The local fixture passes42 spawn and16 native replacement attempts, including corrupt/recover, private read denial, root/user execute denial and executable controls. Denial preserves six IDs, groups, NNP, grants, ceiling, PID, executable identity and CLOEXEC; spawn also preserves sentinel pipe data. Both remote delivery modes pass10 cases on the buffer path and10 with the9MiB file-backed image. All new execute denials are EACCES. Two fixture assumptions were corrected with their failed logs preserved: readable plain exec resets dumpability to1, and legacy private-read refusal is EIO for buffer construction but ENOMEM for file-backed construction. Production preserves that existing distinction.

Both owned fixture directories are verified absent. An optional final receiver procfs hostname probe returned ENOTSUP; it supplies no final receiver hostname evidence. Final normal launcher68417 exits0, all exact owned processes are absent and no WOS VM remains at2026-09-15T14:48:42.831257+00:00. Evidence: build/goal19-interpreter-execute-policy/validation.json and review.md.

Goal19 stays active,11 enforced/10 pending, exec first. This closes the reproduced PT_INTERP execute-admission gap; it does not qualify every enabled lifecycle/restart interleaving, explain historical9FZ or rehabilitate9KW's inconclusive cancellation fixture. Only ordinary OS permission tests ran. The original goal command and appended --no-setup authorization remain unchanged.


### 9KY: remote exec refusals preserve caller state

The older credential/source runners still expected the pre9KR nonroot plain-exec dumpability value0. Current Snapshot::prepare_exec and its native model establish dumpability1 after readable plain exec without an effective-ID transition. Both runners now require that current value, and the shared C++ launcher's legacy-decoder comment is corrected. The source fixture documents end-to-end read denial accurately: rejection can occur during sender retained-read admission or receiver mapped reading, so it does not claim a receiver-only witness.

The shared launcher now retains all six IDs, bounded groups, NNP, service grants, authority ceiling, dumpability, PID and executable identity before exec. On refusal it requires the same values, both CLOEXEC pipe flags and exact sentinel data before returning the original errno. Successful exec output is unchanged. Python runners create fresh owned directories, register uploads before transfer and remove only those owned files/directories; a preexisting path is never overwritten.

Four nodes use the unchanged qualified9KX kernel fb84f1db5ab876b5909bf71da087b80c4c6c3a23, with all live build IDs and symbols verified and the WOSDBG session closed. Node1 rejects source identity, node2 admits same-domain nonroot identity and node3 admits root. The credential matrix passes24/24 across root/UID1000, ordinary/fragmented argv and small/9MiB ELF. The receiver-local source matrix passes12/12 across private mode0711 and readable0755 controls, both delivery modes and sizes. Together16 refusals return EACCES with caller state preserved and20 executions report the expected receiver identity. These are plain root/UID1000 profiles; this turn does not add nonzero supplementary-group, service-grant or concurrent lifecycle coverage.

Fixture cross-build, Python syntax and format check pass; no clang-tidy compilation unit matched these standalone paths. Normal Build WOS and KTEST were not rerun: production/kernel hashes match the qualified9KX artifacts. The copied debugger configuration initially rejected four targets because maxTargets was2, before opening a session; setting its explicit bound to4 allowed verification. Both /tmp and /wki/wos-3/tmp contain no fixture directories after cleanup. Launcher43071 exits0; all five exact owned process identities are absent and no WOS VM remains at2026-09-15T15:00:28.310786+00:00. Evidence: build/goal19-exec-denial-snapshot/validation.json.

Goal19 remains active,11 enforced/10 pending, exec first. Enabled lifecycle/in-flight retirement, general root/cwd transfer and other pending domains remain incomplete; historical9FZ is unexplained. The unsuitable9KW cancellation approach is not repeated. Only ordinary OS permission checks ran, and the original goal command plus --no-setup authorization are preserved.


### 9KZ: one-shot routing commits with the native image

The native exec path consumed one-shot routing immediately after placement returned LOCAL, before interpreter admission and image construction could fail. A new owned-file baseline reproduces the violation in all8 root/UID1000 main/worker explicit-local/absent-target fallback cases: EACCES preserves credentials and CLOEXEC pipe state but changes flags34/32 to2 and erases the fallback hostname. Each child exits and is reaped. The old helper KTEST only classified placement results and did not cover a later native refusal.

NativeExecImage now captures the preparing actor's one-shot selection and PreparedNativeExecImage retains it across worker handoff. LOCAL selection leaves routing unchanged; successful canonical image/credential publication applies the local pin under the existing path_metadata_lock. Worker selection reaches the canonical payload even when the old owner's policy is BALANCED. Existing remote-success and private-spawn behavior uses the factored pin helper. This adds only a private translation-unit preparation field: no Task/syscall/wire ABI, protocol, credential/set-ID policy, errno, lock or allocation change.

The source boundary regression fails before the patch and passes after. Normal Build WOS60404, diagnostic build14936, cache-disabled format/tidy56159, compute source and policy audit checks pass; tidy reports warnings. Final isolated KTEST77610 passes59,045/0 at463.085, powers off463.086 and exits0. Both final live kernels and symbols match8472cba4d784c772a93f9cd429749418e4e65782; WOSDBG closes before testing. The unchanged fixture now passes8/8: all8 EACCES denials preserve routing and caller state, and all8 subsequent native execs report local pinning with the one-shot flag consumed. All successful child payloads exit23 and are reaped; the owned fixture directory is verified absent. Final launcher33653 exits0; all owned processes are absent and no WOS VM remains at2026-09-15T15:27:15.973566+00:00.

Evidence: build/goal19-exec-one-shot-commit/validation.json, production.patch and review.md. Goal19 remains active,11 enforced/10 pending, exec first. Broader enabled lifecycle/retirement and retained-namespace obligations remain open. next-audit.md records a separate unqualified candidate: RemoteExecPlan temporarily binds candidate metadata into the old Task before commit while procfs reads those fields. Read the complete observer/locking flow and obtain evidence before claiming an inconsistent observation or bypass. Historical9FZ remains unexplained; no excluded hierarchy fixture or packet campaign ran. Original goal text and appended --no-setup authorization are unchanged.


### 9LA: refused remote exec exposes candidate procfs metadata

The9KZ follow-up candidate is now reproduced on the current kernel. RemoteExecPlan::bind temporarily substitutes candidate ELF fields, executable path and target routing into the canonical Task before remote preparation/commit. An authorized procfs EXE_LINK reader copies that path under path_metadata_lock and rechecks credential generation; the binding does not change that generation or exclude the reader. Restoration after refusal is too late to prevent an in-flight observer from seeing candidate state.

Added a bounded owned-parent/child fixture. A pipe handshake arms observation before32 exec attempts per child; an owned mode0644 PT_INTERP forces each attempt to return EACCES. The child checks its original credentials, PID, executable identity and CLOEXEC report descriptor after every refusal. The parent samples /proc/PID/exe, then checks completion and reaps the child. Both main and worker execution are covered for root and UID1000, on small and9MiB file-backed ELF images.

All8 cases reproduce the inconsistent path:20,236 candidate observations during256 EACCES refusals, with no unexpected path/read errors. Every child completes and is reaped; post-return state remains unchanged. This proves visibility of an uncommitted image during refused exec, not a privilege grant bypass. The fixture is a failing regression until production is fixed; its timing samples do not establish every interleaving or successful-cutover visibility.

Fixture cross-build, Python syntax and format check pass; no standalone clang-tidy unit matched. Both live kernels/symbols match the unchanged qualified9KZ build8472cba4d784c772a93f9cd429749418e4e65782, and WOSDBG closes before execution. No production change or Normal/KTEST rerun occurred. The owned fixture directory is absent. Launcher28697 exits0; all owned processes are absent and no WOS VM remains at2026-09-15T15:41:34.235277+00:00. Evidence: build/goal19-exec-candidate-visibility/validation.json and baseline/results.json.

The production repair remains pending. patch-contract-next.md maps explicit candidate context in WkiRemoteSpawnSpec, plan-owned buffer lifetime, canonical commit publication, legacy/private-spawn compatibility and a related rejected-exec remote_pid reset that must not be lost when removing binding restoration. Masking only procfs would leave candidate state in other Task consumers. Successful main/worker and both source delivery paths require requalification after that change. Goal19 stays active,11 enforced/10 pending, exec first; this current demonstrated inconsistency is the next implementation priority. No blocker, excluded hierarchy retry or packet campaign is asserted. The original goal command and --no-setup authorization remain unchanged.


### 9LB: keep remote exec candidates private until cutover

Removed RemoteExecPlan's temporary binding of candidate ELF/path/routing into the canonical old Task. WkiRemoteSpawnSpec now borrows a candidate view from the process-owned plan for synchronous selection and submission. The inline submitter copies candidate bytes before waiting; VFS_REF serializes the candidate path. Candidate storage remains plan-owned, while the old Task ELF retains its existing proxy-cleanup lifetime. Candidate path/routing and one-shot consumption publish only after successful credential publication at canonical cutover under the existing VM publication/path locks. Ordinary private spawn keeps its existing ownership behavior. No Task, syscall or packed wire layout changes.

Removing restoration also required preserving the old remote ID directly on exec-handoff rejection. Ordinary spawn still clears its unaccepted remote ID. The existing native handoff selftest exercises the actual rejection handler for exec/spawn with stale and admitted generations; the plan selftest checks old-image privacy, explicit accepted metadata publication and independent storage lifetime. This is native handler evidence for onward identity preservation, not a live onward-receiver lifecycle claim.

The unchanged9LA observer now passes all8 main/worker root/UID1000 small/file-backed cases: zero candidate observations across256 EACCES refusals, compared with20,236 in the baseline. Every child completes and is reaped; no unexpected paths or observer errors occur. Eight one-shot refusal/retry controls pass with caller/routing/CLOEXEC state preserved on denial and successful local pinning afterward. Five18-case saved-ID/set-ID/NNP/service/secure-auxv matrices pass90/90: small main/worker/spawn and9MiB main/worker, each comparing local and remote execution, with payload retirement checked and owned directories removed.

Normal Build WOS (RelWithDebInfo), isolated diagnostic build, scoped format/tidy and exec/WKI source checks pass. Full isolated KTEST passes59,045/0 at459.448s and powers off. Initial packaging failed on the sandbox's libguestfs socket bind restriction before any VM started; the authorized unsandboxed --no-setup retry completed. Both final live kernels and symbols match746e39fca46ff95dd74ef8aad0cbe1e141988e51; WOSDBG closes before fixtures. All7 owned fixture directories are absent. Launcher47239 exits0, and all exact owned processes are absent with no WOS VM remaining at2026-09-15T16:12:31.700694+00:00 (19:12:31 Europe/Bucharest, UTC+3). Evidence: build/goal19-private-exec-candidate/validation.json, observer-results.json, identity-summary.json, verified-symbols.json and down.json.

Goal19 remains active,11 enforced/10 pending, exec first. This closes the demonstrated refused-candidate visibility gap; it does not establish a single snapshot of all successful-cutover procfs fields or every exec/exit/restart interleaving. Existing enabled lifecycle/namespace obligations and historical9FZ cause remain open. No excluded hierarchy retry or packet campaign occurred. The exact goal command and --no-setup authorization remain unchanged.


### 9LC: onward refusal preserves original-parent completion

Requirement2 and the unchanged-denial invariant now have an ordinary live return-hop fixture. An owned parent on wos0 forks a root/UID1000 child and executes the helper on wos3. The receiver's main thread or worker attempts onward exec to wos0 against an owned interpreter. Mode0644 must return EACCES while preserving all six IDs, bounded groups, NNP, grants, ceiling, dumpability, PID, executable identity and both CLOEXEC pipe descriptors with exact held data. The old receiver then exits23, which the original parent must reap within a bounded wait. Mode0755 instead executes the final payload on wos0 and returns23 through both proxy hops. The runner separately checks every reported receiver/final PID is absent.

All16 cases pass: small and9MiB file-backed payloads, root/UID1000, main/worker, refusal/success control. All8 refusals preserve caller state and descriptors. All16 original children complete promptly and are reaped; all reported receiver/final payloads retire. The owned fixture directory is absent. This gives behavioral completion evidence for the9LB rejection-identity change, supplementing its native actual-handler test. Procfs remote_pid falls back to local PID for remote receivers, so its display alone would not prove that the internal remote ID survived; this fixture does not claim such direct internal observation.

The initial fixture used a /tmp source-helper path that current WKI receiver-local routing interpreted on node3. All4 first-hop attempts returned ENOENT and their original children were reaped; no onward case ran. The corrected fixture explicitly names /wki/wos-0/tmp/... for that source-owned helper. Failed small/results.json and initial-fixture.cpp remain under the artifact directory; this setup failure is not counted as a passing denial case.

Fixture/helper and owned-PT_INTERP payload cross-compilation, Python syntax and format check pass; no clang-tidy compilation unit matches the standalone fixture. Production and both kernel hashes remain identical to qualified9LB, so Normal Build WOS and isolated KTEST were not rerun in9LC. Both live normal kernel IDs and symbols match746e39fca46ff95dd74ef8aad0cbe1e141988e51, and WOSDBG closes before testing. Launcher4635 exits0; all exact owned processes are absent with no WOS VM remaining at2026-09-15T16:26:43.708322+00:00 (19:26:43 Europe/Bucharest, UTC+3). Evidence: build/goal19-onward-exec-refusal/validation.json, small-corrected/results.json, large/results.json, cleanup.json and down.json.

Goal19 remains active,11 enforced/10 pending, exec first. This qualifies normal onward refusal/completion and successful return-hop controls, not peer restart, cancellation at every cutover phase, general root/cwd transfer or the historical9FZ cause. No production ABI, credential policy, locks or protocol changed; no excluded hierarchy retry or packet campaign occurred. The original goal command and --no-setup authorization are unchanged.


### 9LD: current-kernel credential and service matrix

The previous9LC turn made verified progress on ordinary onward completion. This turn revalidated the broader existing userspace matrix after9LB, using four fresh private nodes and the unchanged qualified kernel746e39fca46ff95dd74ef8aad0cbe1e141988e51. Receiver policies for source0 remain deny on node1, same-domain nonroot on node2 and same-domain-root on node3; additional existing peer trust supports startup/inspection without changing these admission expectations. All four live kernel/symbol identities are verified and WOSDBG closes before execution.

All four matrix invocations pass: the native credential suite completes66/66 subprocesses, device checks pass24/24, authenticated root identity mapping passes18/18, and full Snapshot/no-regain mapping passes36/36 across root/inert/service profiles, ordinary/fragmented argv and small/9MiB images. Service/runtime ownership passes188 assertions across nodes0/3 (33+70 and33+52), including declared active service identities, irreversible ceiling/sealed-grant controls, account/config/state ownership and root SSH directory/key modes. These are the existing fixture assertions, not a claim that every possible service action or concurrent remote lifecycle has been exercised.

The four standalone fixtures were cross-built successfully. The full native testprog binary matches the prior qualified66-case build. Current production and both kernel hashes match9LB, so Normal Build WOS and isolated KTEST were not rerun in9LD. The remote-VFS source suite passes. Fixture files are absent from source and admitted receiver tmp directories. Launcher52412 exits0; all five exact owned process identities are absent and no WOS VM remains at2026-09-15T16:38:59.722087+00:00 (19:38:59 Europe/Bucharest, UTC+3). Evidence: build/goal19-service-reaudit/validation.json, matrix-results.json, verified-symbols.json, cleanup.json and down.json.

The completion overview incorrectly still listed phased XATTR namespace lifetime as pending. Source inspection confirms VfsXattrServerStage owns one Snapshot and bounded physical/visible export names through dispatch, and remote_vfs.cpp still has the exact9KO qualified hash. The current overview is corrected; historical stage records are preserved. Filesystem submission inspection separately confirms captured context ownership, exact target/channel/resource grant admission, explicit ENOTSUP for unsupported owned-root/view transfer, and permitted legacy pathname behavior for plain native anchors. This inspection does not prove arbitrary inode-preserving native/third-owner transfer or its unsupported denial matrix, and does not promote a domain.

Goal19 remains active,11 enforced/10 pending, exec first. The nine original requirements and all manifest domains remain intact. Remaining work must identify an enabled missing authority decision, unstable snapshot, partial denial or indispensable lifecycle qualification against that contract; passing this broad matrix does not explain historical9FZ or close remaining namespace/peer lifecycle cases. No kernel policy, ABI, wire layout, lock or service privilege changed; no excluded hierarchy retry or packet campaign ran. The exact original goal command and --no-setup authorization remain unchanged.


### 9LE: reject equivalent integral-zero root tests in the source gate

Measurable requirement4 had a reproduced source-gate gap. The current scanner missed common equivalent tests such as euid == 0b0, euid == static_cast<unsigned>(0), euid == uid_t{0}, euid not_eq 0, not euid and effective_uid() == 0. It also matched a zero prefix/suffix inside floating or nonzero numeric tokens. Baseline matcher output and the failing new public-audit regression are retained under build/goal19-root-check-literals/.

The gate now recognizes complete binary/hex/decimal integral-zero tokens with C++ digit separators and size suffixes, common explicit integral casts/constructions, false, zero-argument effective-UID getters and alternative not/not_eq operators. Token boundaries prevent matching just the zero part of a floating or nonzero literal. Comments/string/character handling, exact counted legacy exceptions, all21 required domains and --require-complete behavior are unchanged. This remains a spelling-based guard: it does not resolve arbitrary aliases/macros, fold general expressions or prove complete operation authorization.

The existing full credential-policy audit suite passes. A synthetic complete repository must reject each of15 equivalent expressions through audit(), while commented/string forms remain ignored and20 nonzero/floating-token boundary cases produce no integral-zero match. A host C++23 compiler separately validates the15 predicates for UID0 and UID1. Python compilation passes, and the actual current-tree gate passes with zero documented legacy checks. No newly exposed production root check was found. Normal Build WOS, KTEST and live VMs were not run: only Python enforcement/tests and ledger evidence changed; no kernel, syscall, wire, service identity or runtime policy changed.

The prior9LD turn was verified progress. Review of the historical9FZ evidence still finds no captured caller stack sufficient to justify a scheduler change; this gate repair does not explain or fix that incident. Goal19 remains active,11 enforced/10 pending, exec first; the complete gate still refuses completion. Original goal text and --no-setup authorization remain unchanged. Evidence: build/goal19-root-check-literals/validation.json and scoped.patch.


### 9LF: receiver restart applies changed identity policy

Requirement8 now has ordinary allow/deny/restore reconnection evidence on the unchanged9LB kernel. Source0 stays alive with the same PID/start ticks throughout. Receiver3 is replaced twice using fresh private overlays and its existing pairwise transport key: same-domain-root initially, deny after the first restart, then same-domain-root restored. Source-side authenticated session generation advances1 →2 →3. Each receiver has a distinct recorded process lifetime, and the source logs authenticated reconnection. Source auth-status policy remains its own configured trust of peer3; the receiver fw_cfg policy and execution results establish receiver-side denial.

All36 cases pass:12 per phase across root/inert/service profiles, ordinary/fragmented argv and small/9MiB images. The allow phases return exact Snapshot/no-regain success; every denied-phase attempt returns EACCES. Both live kernel/symbol identities match746e39fca46ff95dd74ef8aad0cbe1e141988e51 in each phase, with debugger sessions closed before execution. The existing runner gains an explicit --receiver-only allow|deny mode; normal36-case and mixed-capability behavior remain unchanged, and incompatible mode combinations are rejected.

The initial stop helper required /proc absence, but the launcher waits on source0 before reaping its exited receiver. Exact start ticks, zombie state, empty argv and no descriptors established that receiver exit had completed. The public launcher then correctly rejected a second topology. A stage-local helper therefore replaced only the exited receiver inside the existing guarded topology using the shared launch_one_vm/node layout API. It validated the original guard holder, exact source identity, old receiver exit and absence of other WOS VMs; retained the original pairwise key without logging it; and changed only the declared receiver identity bits. The production launcher and coordination guard are unchanged. These setup failures are preserved and are not counted as authorization test results.

All fixture files are absent on source and admitted receiver. The denied receiver helper exits0. Final restored-helper shutdown exits1 after a reentrant subprocess wait in its signal handler times out; its finally path completes cleanup. The independent exact-process audit confirms all seven recorded process identities are absent and no WOS VM remains at2026-09-15T17:09:32.739111+00:00 (20:09:32 Europe/Bucharest, UTC+3). The original launcher log reports Cluster stopped; its unified session exit code was unavailable after context recovery. Do not describe helper teardown as an entirely successful command. Evidence: build/goal19-peer-identity-restart/validation.json, matrix-results.json, phase-ready.json files, phase-policy.json files, cleanup.json and down.json.

Current production and normal/diagnostic kernel hashes still match qualified9LB. Python syntax/help, the credential-policy gate and its full host suite pass; the complete gate still fails at pending exec. Normal Build WOS and isolated KTEST were not rerun because this stage changes only the Python fixture and evidence. No kernel locks, syscall ABI, wire layout, service grants or runtime authorization implementation changed. This qualifies new exec requests after authenticated reconnect and a changed peer identity policy; it does not qualify accepted tasks in flight across reboot, arbitrary channel reset interleavings or the historical9FZ cause. Goal19 remains active,11 enforced/10 pending, with exact original goal text and --no-setup authorization preserved.


### 9LG: accepted running exec retires across receiver reboot

The prior9LF turn made verified progress on new requests after identity-policy changes. This stage addresses accepted running work under requirements2/8. New standalone service_running_exec_restart.cpp/.py fixtures fork an owned source child, install root/inert/service profiles and exec from the main or worker thread. The actual receiver checks all six IDs, all eight supplementary groups, NNP, authority ceiling and sealed service grants, then verifies privilege/ceiling/grant regain refusal. It writes a bounded readiness record through an inherited pipe. Only exact readiness permits a descriptor/PID/start/overlay-validated QMP reset of the disposable receiver. The source parent must reap its original child exactly once with the existing failure code255 and unchanged credentials. Fresh controls on the new authenticated boot return23 normally. Small and9MiB payloads cover inline and file-backed paths.

The initial matrix exits1 after17 passing results: nine accepted-restart cases and eight fresh controls. Its large-image inert main-thread fresh control never reaches readiness (source parent221/child222). The receiver later logs OP_VFS_OPEN_READ_AS1048 timing out after60s while opening /lib/ld.so; source logs VFS_REF submit failure EIO. The initial fixture's30s readiness deadline expired first, requested child termination and closed its SSH channel on missing readiness, losing final fixture output. Receiver prepublication cancellation and source child termination are therefore part of this failed observation. Both exact failed source PIDs are subsequently confirmed absent, and source compute diagnostics have no submitted tasks/pending completion. This is not an accepted-restart failure, nor proof of why interpreter fetching stalled. Preserve failure-serial-vm0/3.log, failure-inspection.json and the initial fixture sources; no kernel correction or causal claim follows from this timeout.

The fixture now allows90s readiness, beyond the existing60s kernel fetch timeout; reports exec errno and retains the final child result even without readiness. Explicit size/profile/worker filters permit bounded continuation without discarding prior evidence. With the same source VM, the corrected runner passes all8 large-image inert/service main/worker restart/control cases. Across both runs25 successful results cover all24 unique intended combinations (one restart case is repeated), with13 verified receiver boot changes and15 debugger sessions that verify both live kernel/symbol identities and close before functional work. Each captured post-reboot source diagnostic has zero submitted tasks and pending compute completions. This provides passing evidence for each combination but is not a clean all-pass default matrix: the initial fresh-control timeout remains unresolved, and the successful retry does not explain it.

Both fixture cross-builds, final formatting, Python syntax/help, the current remote-compute source suite, policy audit and full audit host suite pass. Initial formatting violations were corrected; no clang-tidy compilation unit matches the standalone fixture. Current production and normal/diagnostic kernel hashes still match qualified9LB746e39fca46ff95dd74ef8aad0cbe1e141988e51. Normal Build WOS and KTEST were not rerun because no kernel/runtime implementation changed. All owned helper files are removed. Launcher54213 exits0; exact launcher/VM processes are absent and no WOS VM remains at2026-09-15T17:29:53.555352+00:00 (20:29:53 Europe/Bucharest, UTC+3). Evidence: build/goal19-running-exec-restart/validation.json, matrix/results.json, remaining/results.json, cleanup.json and down.json.

Goal19 remains active,11 enforced/10 pending, exec first. Kernel locks, syscall ABI, wire layout, service grants and authorization implementation are unchanged. This qualifies the observed accepted-running parent-completion paths, not every prepublication/cutover/peer interleaving, the newly observed fetch timeout or historical9FZ. No packet campaign occurred. The exact original goal command and --no-setup authorization remain unchanged.


### 9LH: full reboot matrix with pending-fetch capture armed

The prior9LG turn made progress and retained a pre-readiness interpreter OPEN_READ_AS timeout. This turn traced the current requester, pre-ACK worker admission, exact retained binding and credential mapping, owner open/response, policy-denial response, and worker wake/block paths. Mapped-open and policy-denial branches construct correlated responses; scheduler event wake preserves wakeup_pending before park. Those source observations do not establish where the old request stalled or whether its response send succeeded, and do not justify a speculative kernel change.

A stage-local watcher now arms when an ordinary fixture readiness log opens and remains empty for5s. At most three captures can pause both exact owned VMs to inspect typed VFS queues/queued metadata, daemon/process saved stacks, WKI wait entries, peer policy and channel state. A shared host lock serializes capture with every debugger verification and receiver reset; readiness is checked again after locking. Each capture checks PID/start/overlay identity and the live kernel build-ID section before typed inspection, then restores prior QMP running states in finally. No authentication key bytes are logged. The adapted Python driver changes only import location and debugger/reset serialization; original fixture assertions, profiles, deadlines and ordinary reset behavior are preserved in driver-adapter.patch.

The smoke capture passes on both nodes in1.927s, enumerating96/90 tasks, all16 VFS worker shards per node and70 selected saved stacks per node without registry truncation, then resumes both VMs. The corrected9LG fixture subsequently passes its complete default24-case matrix:12 accepted-running reboot retirements and12 fresh controls across small/9MiB root/inert/service main/worker execution. Every parent preserves credentials, reaps the original child exactly once and observes failure255 after reboot or success23 for the control. Twelve changed receiver boot epochs and13 closed WOSDBG sessions with both live build IDs/symbols matching746e39fca46ff95dd74ef8aad0cbe1e141988e51 are verified. The watcher records zero delayed-readiness captures; no pending failure was observed. This is a clean current matrix result, not a cause or fix for the historical9LG timeout or9FZ observation.

No production or fixture source changed. Python syntax and current VFS/remote-compute source suites pass; policy audit and full audit host suite pass, while --require-complete still rejects pending exec. Production and normal/diagnostic kernel hashes remain identical to qualified9LB, so Normal Build WOS and isolated KTEST were not rerun. All owned helper files are absent. Fixture watcher78002 exits0 and launcher62382 exits0. All exact owned processes are absent and no WOS VM remains at2026-09-15T17:45:16.830107+00:00 (20:45:16 Europe/Bucharest, UTC+3). Evidence: build/goal19-open-read-timeout-capture/validation.json, matrix/results.json, smoke-capture.json, watch-events.json, driver-adapter.patch, cleanup.json and down.json.

Goal19 remains active,11 enforced/10 pending, exec first. The next timeout investigation needs a pending operation/channel/worker capture; another passing run alone cannot resolve its cause. Broader original requirement/manifest obligations remain unchanged. Kernel authorization, locks, syscall ABI, wire format and service grants are unchanged; no packet campaign occurred. The exact original goal command and --no-setup authorization remain intact.


### 9LI: reproduced queue-affinity denial changes an earlier queue

The prior9LH turn made verified progress on exec lifecycle qualification, leaving its intermittent fetch timeout unexplained. Current-source audit of the pending network-hostname domain found a concrete violation of the goal's unchanged-denial invariant. SET_DEV_CPU_AFFINITY captures and authorizes one NETWORK_ADMIN Snapshot correctly, but invokes set_queue_cpu sequentially for each requested pair. Virtio can reject a later out-of-range pair withEINVAL after an earlier worker and MSI-X placement changed. Even valid pairs can later returnEBUSY through napi_set_worker_cpu/pin_task_to_cpu, so queue-count prevalidation alone is insufficient.

New service_queue_affinity_denial.cpp/.py fixtures reproduce the problem on the owned four-CPU source VM's one-queue eth1. The corrected unprivileged child gets-EPERM and its root parent confirms unchanged worker state. The first root request returns-EINVAL while the exact worker25 moves fromCPU3 toCPU0: ATTEMPT index=0 result=-22 worker=25 before=3 after=0 unchanged=0. The fixture restores that same worker toCPU3 and records restored=1, then exits1 because the denial invariant failed. This is a real current-kernel regression, not a passing test or an authorization bypass. The initial fixture attempted to read protected netdiag after dropping privileges; it reported preserved=0 because observation was unavailable and stopped before the root case. That setup failure and initial source are retained separately; the corrected fixture moves observation to the authorized parent after reaping its child.

The candidate implementation needs one batch admission/commit spanning all requested worker placements and no fallible operation after the first effect. Existing single-worker rollback is not sufficient because rollback can also returnEBUSY. Reconnaissance maps the syscall/device control guard, immutable driver queue configuration, NAPI workers, scheduler owner/migration admission, ordered IRQ-safe runqueue locks, fixed-capacity heaps and MSI-X commit. implementation-recon.md records the proposed bounded strategy and required actual-code denial/success regressions; no production transaction fix has been implemented yet. This concrete gap takes priority over generic pending-domain wording.

Both fixture cross-builds, final formatting and Python syntax/help pass. No clang-tidy compilation unit matches the standalone fixture. Both live kernel IDs and symbols match qualified9LB746e39fca46ff95dd74ef8aad0cbe1e141988e51, and WOSDBG closes before execution. Production and normal/diagnostic kernel hashes are unchanged, so Normal Build WOS and isolated KTEST were not rerun. Both owned helper files are absent. Launcher11233 exits0, all exact owned processes are absent, and no WOS VM remains at2026-09-15T18:00:43.829777+00:00 (21:00:43 Europe/Bucharest, UTC+3). Evidence: build/goal19-queue-affinity-denial/validation.json, observed/stdout.txt, baseline/stdout.txt, implementation-recon.md, cleanup.json and down.json.

Goal19 remains active,11 enforced/10 pending. No kernel authorization, syscall ABI, wire format or service grant changed; the queue-affinity transaction defect remains open. The source policy audit and its host suite pass, but those checks do not cover this semantic denial failure; --require-complete still refuses pending exec. No packet campaign or host-interface change occurred. Exact original goal text and --no-setup authorization are preserved.


### 9LJ: queue affinity admits and commits the whole request

The current9LI regression is repaired through a whole-mask NetDeviceOps callback. SET_DEV_CPU_AFFINITY keeps its24-byte request, first-eight-bit mapping, one captured NETWORK_ADMIN Snapshot, supported empty-mask success and errno convention. Retained device control serializes the callback. Virtio validates all requested pairs/workers, then submits one bounded DAEMON-worker scheduler batch. All affected source/target queues are locked in ascending CPU order; owner, state, GC, duplicate, reservation, migration/frame, exact membership and aggregate capacity are checked before CPU/pin/heap/wait/vruntime changes. All cross-CPU members detach before publication, so admitted outgoing work contributes capacity. Wake/IPI and the existing void MSI-X commits run after runqueue unlock. No denial attempts fallible rollback. Existing single-worker APIs remain unchanged.

Actual scheduler-core native tests cover twelve later-request refusal causes with unchanged earlier worker CPU/pin/vruntime/deadline/membership/sums, successful runnable/waiting exchange and same-CPU current-worker admission. The actual driver mask helper verifies pair overflow, missing worker and scheduler refusal precede all MSI-X calls, with successful mapping and empty-mask controls. The first full run records59046passed/2failed: test workers incorrectly inherited heap_index0 from default Task construction rather than initializing detached state to-1. The production constructor and neighboring native fixtures explicitly use-1. That test-only initialization is corrected; production transaction logic is unchanged. The failing serial log/panic and exact process cleanup are retained. Both corrected native scheduler cases and the driver case pass in the fresh retry.

The initial normal/diagnostic links failed because scalar std::sort selected a libc++ external instantiation unavailable to the freestanding kernel. A bounded insertion sort over at most16 CPU IDs closes that build issue. Normal Build WOS (RelWithDebInfo), diagnostic Debug build and scoped formatting/tidy now pass; warnings remain in pre-existing code. Native fixture cross-builds, Python syntax, network usercopy source check, policy audit and audit suite pass. The complete gate still refuses pending exec. Final full KTEST passes59048/0 at468.042 and powers off468.043; the isolated launcher exits0 and exact-process audit finds no remaining WOS VMs. The final normal kernel buildID is de64351a41c656b1d5678746fcec02e893101590. Both live node IDs/symbols match and WOSDBG closes before functional execution.

The unchanged9LI live fixture now reports root-EINVAL with worker25 unchangedCPU3, plus preserved unprivileged-EPERM, and exits0. New service_queue_affinity_batch.cpp/.py passes24/24 cases across root/user/inert-root/journal/netd/cleared-netd. Empty masks, pair overflow, a later invalidCPU and successful two-worker movement have exact expected results and mappings. Startup uses descending placement; this mask ABI cannot restore arbitrary descending assignments. The fixture records startup, establishes explicit mask15 baseline and restores that baseline after every case; it does not claim restoration of boot placement. No live busy retries were needed; deterministic current/handoff/migration/preempt-busy atomicity is native-test evidence. Both owned helpers are independently absent. Launcher10099 exits0; all exact owned processes and all WOS VMs are absent at2026-09-15T18:39:11.917998+00:00 (21:39:11 Europe/Bucharest, UTC+3). Evidence is indexed in build/goal19-queue-affinity-transaction/validation.json, with scoped patch, initial failed logs, final KTEST serial log, verified-symbols.json, denial/stdout.txt, batch/stdout.txt, cleanup.json and down.json.

The user confirms remote exec must preserve declared VFS routing: explicit LOCAL / selects receiver root. Retained cwd/root authority work must respect that choice; it must not force all absolute paths through the source exact root. Current explicit_task_vfs_route_for_path/task_uses_receiver_local_path consult declared rules before receiver-local defaults. No remote exec code, wire ABI or service grants changed in this increment. Goal19 remains active with11enforced/10pending domains; historical9LG fetch timeout and9FZ completion cause remain open.


### 9LK: PTY completion loss and alias ordering reproduced

The previous9LJ turn made verified implementation/runtime progress. This stage returns to pending exec completion and adds explicit --pty and --pty-receipt modes to service_exec_output.cpp/.py. Each disposable SSH PTY uses raw terminal settings so exact binary output is not confused with line-discipline translation. The default PTY mode retains the stderr receipt on its distinct FD; a separate stdout-receipt control isolates one stream. Existing pipe/inherited-writer/explicit-close modes are unchanged. Session metadata now records owned helper paths and test mode.

The current qualified9LJ kernel has two concrete PTY failures. Initial immediate alias cases deliver exact total byte counts, but remote131072/1048576 receipts appear after byte65536 instead of the final stdout byte. Removing the receipt yields the exact complete payload, identifying reorder rather than payload corruption. Held-exit alias controls still reorder. The separate immediate same-stream remote131072 case exits0 after94208of131093expected bytes and no receipt, proving truncation independently of alias ordering. All six held-exit same-stream controls pass. A final-binary immediate alias run reproduces failures at every remote size:4096 receipt truncated (4112of4115),131072 complete but reordered, and1MiB truncated to856064of1048598. Local controls all pass. These failed runs are preserved; neither a delay nor moving the receipt to stdout is a production fix. All ten existing pipe byte/handshake controls pass on the same pair.

Source tracing finds that only IPC_PIPE enables application flow receipts and WkiIpcOutputCompletion retention. PTY uses accounted asynchronous proxy_pipe_write but receives no completion receipt; its expected-byte close fence is separate from TASK_COMPLETE. Each FD alias has its own export ID/backlog, and output flush shards use resource_id modulo4. The immediate PTY path considers only that resource's backlog. This allows the shared terminal's streams to progress independently; a shared endpoint ordering mechanism must also cover queued/in-flight/partial writes and immediate delivery. Existing pty_file_identity_key distinguishes the actual master/slave endpoints, but is currently used only to deduplicate input pumps. Socket output remains separately unqualified. next-implementation-map.md records the remaining admission/receipt/ordering/retirement reads and required native/live tests before a critical production patch. No kernel change or fix is claimed in9LK.

Both live IDs/symbols match qualified9LJde64351a41c656b1d5678746fcec02e893101590 and WOSDBG closes before execution. Production and normal/diagnostic kernel hashes remain unchanged. Fixture cross-builds, formatting and Python syntax/help pass; IPC source and credential policy suites pass. Normal Build WOS and isolated KTEST were not rerun because only fixtures/docs/evidence changed;9LJ59048/0 is matching historical evidence, not a current-stage run. All owned output helpers are independently absent. Launcher55030 exits0 and exact-process audit finds no WOS VMs at2026-09-15T18:54:04.903566+00:00 (21:54:04 Europe/Bucharest, UTC+3).

Evidence: build/goal19-pty-output-completion/validation.json, all-ordering-analysis.json, strict failed byte artifacts under immediate/,single-stream/,held-alias/,final-alias/, passing held-single/ and pipe-controls/, verified-symbols.json, cleanup.json, down.json and next-implementation-map.md. Goal19 remains active11enforced/10pending. Credential, syscall, wire and peer policy are unchanged. Declared VFS routing remains the user requirement; exact goal text and --no-setup authorization are preserved. No packet campaign or penetration test ran.


### 9LL: PTY delivery completion and alias ordering qualified

Writable PTY imports now use the existing application-delivery flow flag, credits and finite WkiIpcOutputCompletion targets. Close still detaches the File; completion retains only proxy state until receipts reach the frozen accepted-byte target or a terminal error/peer retirement occurs. Legacy imports without the flag retain their existing behavior. The wire layout, syscall ABI, descriptor grants and authenticated peer decisions are unchanged.

Active exports of the same retained PTY endpoint share a resource-derived ordering group; master and slave remain distinct. Reliable RX admission links a fixed-pool DATA token before worker publication. The token survives backlog ownership transfer, partial writes and failed receipt enqueue, and is unlinked before storage reuse on delivery or cancellation. Ordered DATA preserves the recyclable pool reserve, including one-byte payloads. Workers scan their bounded queue for eligible work and sleep only when every queued entry is blocked. IPC-to-pool lock order is preserved, with no added allocation, VFS operation or blocking under those locks. The legacy inline PTY shortcut cannot consume flow-controlled output without a receipt.

Native tests exercise shared/distinct/retired endpoint groups, small-chunk transfer, partial delivery, receipt retry, cancellation, reserve admission and independent ready-work selection. Actual imports cover current/legacy/read-only/nonblocking flow flags. The existing finite completion, close, refund, receipt, EPIPE and peer cleanup cases now run for both pipes and PTYs. Normal Build WOS (RelWithDebInfo), isolated Debug build, scoped formatting/tidy and IPC/compute source checks pass. Final full KTEST passes59050/0 at468.629 and powers off468.630. The first implementation also passed59050/0 and all34 live cases, but per-blocked-entry sleeps made immediate remote1MiB cases take about31seconds. Its complete evidence is retained under first-implementation/. The final bounded scan removes that delay without weakening ordering.

Both final live IDs and symbols match553318dec851aff9784c107ecf5f6cc20934e14c; WOSDBG closes before execution. All four six-case PTY matrices pass: immediate aliases, immediate same-stream, held aliases and held same-stream. All ten original pipe byte/inherited-writer/explicit-close controls also pass. Fixture code and exact byte/receipt expectations are unchanged from9LK. Adjacent artifact timestamps put final immediate remote1MiB cases at0.840s and0.518s respectively; these are workload observations, not a controlled throughput benchmark. All five final helpers are independently absent, launcher93353 exits0, and exact-process audit finds no WOS VMs at2026-09-15T19:40:48.458978+00:00 (22:40:48 EEST, Europe/Bucharest).

Evidence: build/goal19-pty-delivery-transaction/validation.json, implementation.patch, ownership-review.md, first-implementation/validation.json, ktest-serial-final.log, matrices.json, exact byte artifacts, verified-symbols.json, cleanup.json and down.json. Goal19 remains active at11enforced/10pending. Socket and optional RDMA completion, remaining exec lifecycle/compatibility work, and the historical9LG fetch timeout/9FZ completion cause remain pending. Declared VFS routing remains authoritative, including explicit LOCAL / selecting the receiver root. The exact fenced goal and --no-setup run/debug authorization are preserved.


### 9LM: inherited socket close retirement repaired; shutdown ordering remains pending

A new ordinary loopback TCP fixture compares local/remote exec, write/send, close/SHUT_WR, sizes4096/131072/1048576 and root/UID100. The endpoint is created before the child drops identity and inherited across exec. The final UID100 emitter checks real/effective/saved UID/GID100, empty supplementary groups and EPERM with unchanged IDs on setuid(0)/setgid(0), then uses the retained descriptor. SHUT_WR requires all bytes before EOF and a peer reply on the surviving read side. All comparisons retain exact bytes, EOF and child status; a diagnostic hold is optional.

The qualified9LL baseline passes both local smoke cases and remote SHUT_WR, but remote close waits until the parent's60-second alarm despite the remote emitter reporting4096 accepted bytes. A second close run reproduces it. WOSDBG verifies both baseline IDs/symbols and closes; bounded GDB snapshots independently match build IDs and restore both VMs. Source export28683 is inactive, has file=null and pipe_bytes_received4096, and has pump_running=false, yet pumpPID51 remains in socket_fops_read/tcp_wait with its retained File. The source parent waits for TCP EOF after the remote child has exited. This identifies the last-File/input-pump close cycle. The capture helper's PORT placeholder also changed its EXPORT labels; raw logs are preserved and close-capture-summary.json normalizes those labels. The placeholder replacement is now scoped to the GDB endpoint.

The production patch changes only the native socket branch in the pump read loop. It uses the existing private nonblocking File view, leaving the shared File flags and Socket::nonblock untouched. Existing native fops turn that view into MSG_DONTWAIT. EAGAIN returns to the pump's outside-lock retry so stop/retirement can release the last File. Other pump types, descriptor/consumer/channel authorization, credentials, syscall ABI and wire layout are unchanged. No socket shutdown is used to cancel a shared endpoint.

Normal Build WOS, isolated diagnostic build, formatting/tidy and IPC source checks pass. Full KTEST passes59050/0 at468.241 and powers off468.242, including the existing private-view source-state invariant. Both final IDs/symbols matcha6a1545a86427ac460a4f83e6fdf2cff5af431c5; WOSDBG closes before execution. The48-case matrix passes45/48, including all24close cases. All24UID100 executions reach the accepted marker after the no-regain checks. The three failures are remote SHUT_WR: root131072/write receives126976bytes with childexit0; UID100131072/send receives8192bytes, and UID1001048576/write receives897024bytes, with both children timing out on the reply. All report every byte accepted; the source logs EPIPE on one4096-byte chunk in each failing operation. All16held-shutdown controls pass. These failures remain strict evidence that control-channel shutdown can overtake queued application writes; the hold is not a fix. Socket application-delivery accounting, shared-alias shutdown fencing and remaining read/compatibility contracts stay pending.

All ten existing pipe output/inherited-writer/explicit-close controls pass. Both baseline collectors report SFTP removal, but the separate baseline cleanup attempt receives SSH connection refusal before shutdown; its cause is unestablished. Final independent cleanup succeeds for every recorded helper path, launcher93348 exits0, and all owned processes/WOS VMs are absent at2026-09-15T20:17:31.866567+00:00 (23:17:31 EEST, Europe/Bucharest).

Evidence: build/goal19-socket-output-contract/validation.json, baseline/validation.json, smoke/, close-capture/, socket-close-capture.json, close-capture-summary.json, implementation.patch, patch-contract.md, ktest-serial.log, matrix/results.json, matrix-summary.json, held-shutdown/, pipe-controls/, verified-symbols.json, cleanup.json and down.json. next-socket-map.md records the remaining contracts, including alias-aware finite shutdown fences, speculative read EOF and pending-delivery/nonblocking reads. Goal19 remains active11enforced/10pending; declared VFS routing, exact fenced goal and --no-setup authorization are preserved. The historical9LG fetch timeout and9FZ completion cause remain unresolved.


### 9LN: stream socket shutdown waits for the accepted delivery prefix

Remote SHUT_WR/SHUT_RDWR now captures the next transmit sequence on the original DATA channel and appends that uint32 boundary after the existing int32 how. The home first observes dispatch through that boundary on its original DATA allocation, then waits for earlier work tokens belonging to the same native stream socket endpoint and channel to retire through application delivery. Distinct exported aliases of the same retained Socket share an opaque endpoint group; resource type also participates in grouping. Later sequences, other endpoints and stale channel generations do not extend the captured prefix. Datagram exports keep group zero and are not converted to stream delivery semantics. SHUT_RD retains the legacy four-byte request and does not wait for output.

The existing PTY fixed-pool token registry is generalized to endpoint delivery. Stream DATA retains its token through queued, in-flight, partial and terminal backlog delivery. Socket CLOSE also receives an ordered token: without it, the ready-work scan could skip blocked DATA and retire its export too early. Shutdown fence retries go to the worker queue back, allowing preceding DATA on the same shard to progress; DATA retries retain their original front placement. The retry policy is copied before handler ownership transfer. RX admission stays bounded/nonallocating under the existing channel -> IPC try-lock -> pool try-lock order. Worker channel snapshots occur without the IPC lock; grant revalidation and token scanning occur under IPC -> pool. Native shutdown runs outside those locks with the existing File pin and cleanup lease.

Invalid how returns EINVAL without a callback. Missing, wrong-consumer, retired or File-less exports retain ENOENT; a retired original DATA channel on an otherwise issued grant returns EHOSTUNREACH. Credentials, retained descriptor authority, shared File flags, Socket::nonblock, syscall ABI and declared VFS routing are preserved. Old homes ignore the optional request tail, and old senders omit it: no mixed-version ordering guarantee is claimed.

Normal Build WOS, isolated diagnostic build, formatting/tidy, policy audit/unit tests and IPC source checks pass. Final KTEST passes59051/0 at470.632 and powers off470.633, including the new native fixed-prefix/wrapped-sequence/stale-generation/close-order token test. Two earlier partial KTEST runs were intentionally stopped after source review identified the CLOSE ordering and missing-export errno corrections; their logs and exact down evidence are preserved in superseded/ and pre-errno/. They are not final validation.

Both final live kernels and symbols match59b4e823e05c71bbc3b46e107f0518093d09db8a; the debugger session closes before functional tests. All48 original strict socket cases pass, including the three9LM remote SHUT_WR truncation/reply failures. All24 inherited-duplicate cases pass: write through one descriptor, SHUT_WR through a separately exported alias, then exact bytes and EOF. These alias cases do not claim receive routing across duplicate pumps; the original single-descriptor shutdown cases continue to require a reply on the surviving read side. All16 invalid-how/SHUT_RD controls pass. All44 UID100 socket executions reach the accepted marker after real/effective/saved UID/GID100, empty supplementary groups and EPERM-with-unchanged-IDs checks for setuid(0)/setgid(0). No diagnostic hold is used.

All10 existing pipe byte/inherited-writer/explicit-close controls and12 PTY alias/same-stream controls pass, for110 live cases total. All six uploaded helpers are independently absent; launcher session45347 exits0. The exact-process audit finds no owned processes or WOS VMs at2026-09-15T21:02:30.647742+00:00 (2026-09-16 00:02:30 EEST, Europe/Bucharest).

Evidence: build/goal19-socket-shutdown-fence/validation.json, implementation.patch, patch-contract.md, ownership-review.md, ktest-serial.log, matrices.json, previous-failures.json, exact case artifacts, verified-symbols.json, cleanup.json, built-hashes.json and down.json. Goal19 remains active11enforced/10pending. Socket application flow credits/root completion retention, proxy read EOF/concurrent/nonblocking/overflow contracts, alias receive routing, optional RDMA/legacy qualification and remaining exec lifecycle obligations stay pending. Native token tests qualify fixed-prefix exclusion; live concurrent-writer cancellation is not claimed. The historical9LG fetch timeout and9FZ completion cause remain unresolved. Declared routing remains authoritative, including explicit LOCAL / selecting the receiver root. The exact fenced goal and --no-setup run/debug authorization remain unchanged.


### 9LO: native stream sockets join finite remote output completion

Writable native SOCK_STREAM exports now advertise the existing application-flow bit; datagram/raw exports retain the legacy path. Import initializes stream socket credits before publication and retains the proxy in the existing finite output-completion owner. The socket writer delegates flagged streams to the established accounted message writer, which reserves/refunds credits, counts accepted bytes inside reliable enqueue, honors File O_NONBLOCK and handles partial/error/interrupted writes. Existing DATA_FLOW and WRITE_FLOW messages carry home delivery receipts. Receipts count bytes accepted by the home File's native write path; TCP peer acknowledgement/application consumption are outside this counter.

Completion retains ProxyIpcState rather than File. Its target freezes once after root exit, so later inherited writes cannot extend that target; closed-proxy receipt lookup remains until delivery or terminal error/peer retirement. Socket POLLOUT now reflects available credits and terminal errors, with existing receipt wakeups; errors also report POLLERR. Home native socket writes use the existing private nonblocking File view, returning EAGAIN to the outside-lock backlog retry instead of parking the shared worker. Shared File flags, Socket::nonblock, original resource/consumer/channel authority, DATA/CLOSE endpoint ordering and the9LN shutdown boundary remain intact. Credentials, saved IDs, syscall/wire layouts and declared VFS routing are unchanged. Old unflagged imports keep the legacy writer; no mixed-version completion guarantee is claimed.

Native tests extend actual receipt-after-close, refund-versus-delivery, frozen targets, terminal EPIPE and peer-retirement coverage from pipes/PTYs to sockets. Actual socket imports test current/legacy/read-only/nonblocking flags and completion references, plus zero-credit EAGAIN without accepted-byte changes and credit/error-aware POLLOUT. A home socket mock callback observes O_NONBLOCK while the original File flags remain unchanged. Normal Build WOS, isolated diagnostic build, format/tidy, policy audit/unit tests and IPC source checks pass. Final KTEST passes59052/0 at461.504 and powers off461.505.

Both live IDs and symbols matchd9c72f0ca1882201563f9c19611efddca7972c30; the debugger closes before execution. Four new fixture smoke cases pass, followed by all48 full lifetime cases across root/UID100, local/remote, write/send and sizes4096/131072/1048576. In inherited mode, the root exits with a child holding the socket and a release pipe. The source reaps the root before releasing that pipe; the child rechecks its real/effective/saved identity and no-regain state, writes a required late socket byte, then closes. This proves completion is finite and the inherited descriptor remains usable afterward. Explicit-close mode requires EOF while the root is still blocked on its release pipe, detecting accidental File retention by completion.

All88 previous strict socket cases also pass (48ordinary,24duplicate shutdown,16invalid-how/SHUT_RD), as do10pipe and12PTY controls. Total:162 live cases including the four smoke cases. The full socket matrices include68 UID100 root executions with real/effective/saved UID/GID100, empty supplementary groups and EPERM-with-unchanged-IDs on attempts to regain root;12 UID100 descendants repeat those checks after root reaping before their required late write. Exact byte/EOF/reply checks and zero diagnostic hold are preserved. All eight uploaded helpers are independently absent; launcher session13467 exits0. Exact-process audit finds no owned processes or WOS VMs at2026-09-15T21:34:50.745391+00:00 (2026-09-16 00:34:50 EEST, Europe/Bucharest).

Evidence: build/goal19-socket-completion/validation.json, implementation.patch, patch-contract.md, ownership-review.md, ktest-serial.log, matrices.json, lifecycle/ and lifecycle-smoke/ exact artifacts, prior socket/pipe/PTY controls, verified-symbols.json, cleanup.json, built-hashes.json and down.json. Goal19 remains active11enforced/10pending. Socket read EOF/concurrency/nonblocking/overflow, alias receive routing, post-shutdown/peer-error syscall parity, onward proxy socket export, legacy/optional RDMA qualification and remaining exec lifecycle/credential obligations stay pending. Historical9LG fetch timeout and9FZ completion cause remain unresolved. Declared routing remains authoritative, including explicit LOCAL / selecting receiver root. The exact fenced goal and --no-setup run/debug authorization are preserved.

### 9LP: serialized inherited stream reads and bounded reader wakeups

Flow-advertised native stream socket proxies now share the pipe/PTY reader and poll path. A single IPC->proxy locked snapshot consumes older ring bytes before pending overflow and EOF, serializes concurrent tail publication, and registers waits without losing DATA/CLOSE wakeups. Copies are bounded to one DATA chunk. Completed chunk/work and pending-record ownership is released outside both locks. A fixed32 PID waiter list replaces the single raw Task slot for shared stream readers; full registration retries outside locks, and DATA, EOF, poll replies, detach and peer cleanup safely wake registered Tasks after unlocking. File O_NONBLOCK returns EAGAIN on empty open streams; zero-count reads return0. Shared poll observes pending bytes and EOF. Legacy unflagged sockets retain their previous path; distinct exported aliases still have separate receive queues. The old pipe reader only had the stronger EOF recheck: nonblocking and concurrent-reader protection were missing there too.

The first unchanged local receive smoke failed at45.018s with child status14 before readiness. Source inspection found that native socket File read dispatched zero count into TCP's blocking receive loop, whereas the recv syscall already returned0. The native File adapter now returns0 after existing File/protocol validation, without invoking protocol receive. Positive reads and all wire/TCP state remain unchanged. A native mock checks zero-count callback exclusion, positive-count dispatch and existing invalid-File/missing-protocol errors. The original failed run and its matched3de93424f2d47b9be9c5cbcb1aa06c25dd08458b symbols and59054/0 KTEST remain in before-zero/; the unchanged smoke passes after the correction.

Final Normal Build WOS and isolated diagnostic builds, scoped format/tidy, fixture cross-compile and syntax, policy audit/unit and IPC source checks pass. Final full KTEST passes 59055/0. Both final live IDs and symbols match478e99f77a6589150c0ffd8f951479a3f9d51742; WOSDBG closes before execution. All242 final live cases pass:4 receive smoke,80 full receive cases,136 previous socket output/lifetime/shutdown cases and22 pipe/PTY controls. Single-reader input checks exact order for4KiB/128KiB/1MiB;4/40 readers sharing one File check all128 unique bytes exactly once and EOF for every reader. Root/UID100, local/remote, read/recv, blocking/nonblocking are covered; all UID100 reader threads check saved IDs, empty groups and inability to regain root. No receive-alias equivalence is claimed.

All9 final helpers and the initial failed-run helper are independently absent. The final normal launcher exits0 and its pair is down before the final isolated selftest. Final exact-process audit confirms all owned/WOS VMs down at 2026-09-15T22:24:00.501438+00:00. Evidence: build/goal19-stream-read-contract/validation.json, patch-contract.md, ownership-review.md, implementation.patch, matrices.json, exact case artifacts, ktest-serial.log, verified-symbols.json, cleanup.json and down audits. Immutable credential/grant and resource/consumer/channel authority, finite output completion without File retention, source flags, DATA/CLOSE/shutdown order and declared VFS routing remain intact; explicit LOCAL / still selects the receiver root. The exact fenced goal and --no-setup run/debug authorization are preserved.

Goal19 stays active11enforced/10pending. Next is a bounded reconciliation of exec's actual credential/publication/lifetime requirements against current source and accumulated evidence, as mapped in exec-authority-audit-next.md, before treating more socket protocol completeness as a mandatory authorization task. Historical9LG fetch timeout and9FZ completion causes remain unresolved; ordinary protocol limitations and authorization/lifetime obligations must be distinguished without erasing evidence.

## 9LQ exec authority review and production cutover test

[The bounded exec audit](goal19_exec_authority_audit.md) maps current source and
historical evidence to Goal19 requirements. The manifest decision now identifies
receiver candidate publication versus cancellation/session retirement/deadline as
the next concrete qualification, rather than retaining already-addressed
saved-ID, metadata, signal and output blockers. No domain is promoted: 11 remain
enforced and 10 pending. The earlier 9FZ completion observation and 9LG initial
fresh interpreter-fetch timeout remain unexplained.

The new KTEST invokes actual remote owner preparation/cutover for six refusal
states and one successful control. It verifies no partial credential, metadata,
routing or signal changes on refusal and checks admission/reference cleanup.
Normal Build WOS, isolated diagnostic build/package and scoped formatting/tidy
pass; the full isolated --no-setup run passes59,056/0. The exact launcher and VM
are down, with no WOS VM remaining at 2026-09-15T22:57:50.886729+00:00 (UTC). Earlier cluster
results are historical; no cluster run was repeated in9LQ. Production behavior,
ABI/wire formats, grants and declared VFS routing remain unchanged.

## 9LR receiver final admission and private cleanup

The receiver now uses post_admitted_remote_process immediately before calling
the scheduler for both ordinary spawn and prepared exec. The worker must retain
its noncopyable peer lease; under the compute lock the helper rechecks the exact
sealed session/channel generation, consumes only matching cancellation and
checks the absolute deadline. It releases the compute lock before scheduler
posting and retains the peer lease through that call. This also covers time
spent creating the private monitor after the earlier ordinary-spawn check.

The existing refusal branch is extracted into discard_unposted_remote_process.
It retires the matching unpublished monitor under the compute lock, then drops
the lock and peer lease before deleting output receipts, releasing the two extra
Task references, and invoking owner-linked task/VFS teardown. Successful
publication, its post-publication cancellation check, response routing and
OVERLOADED refusal are preserved. No credentials, service grants, filesystem
routing, syscall ABI or wire layout changed.

FinalPublicationAdmissionAndPrivateCleanup has ten scenarios: missing lease,
handoff not ready, retired session, changed admitted identity, reused channel,
exact cancellation, unrelated-task cancellation, expired deadline, posting
refusal and successful admission. It exercises the production gate and cleanup
with real private Task ownership, restricted immutable credentials, NPROC
reservation, two pipe descriptors, a callback File, a private output receipt and
service-accounting membership. The close callback checks that peer RX admission
and the compute lock are available, the private monitor is retired, credentials
are unchanged and the child remains in its owner's teardown slot. Afterwards
NPROC and pipe current usage return to baseline, receipt references return to
their private base reference, service membership reaches zero and a new NPROC
reservation succeeds.

The scheduler callback is substituted and never schedules the synthetic child.
Successful admission controls therefore use private cleanup after observing the
callback. This is ordered native publication/cleanup coverage, not actual
concurrent scheduler posting or a real remote-VFS acknowledgement test. The
separate existing live identity matrix is used for ordinary main/worker/spawn
publication behavior.

The initial diagnostic run reports 59,056 passed and one failed, then the
expected selftest-failure panic. The added lease check required a current
scheduler Task, but sched_init runs KTEST immediately after setup_queues and
before startScheduler establishes task context. The noncopyable lease retains
its exclusive claim independently of that context; its owns check now uses the
stored claim and node. No session/mapping/cancellation/deadline check was
removed. The failed run and kernels are preserved in before-lease-claim/.
Per-scenario failure diagnostics were also added.

The first live attempt's independently built fixture faulted in its first
local case before producing identity results. The compile command had omitted
the repository's userspace -fno-sanitize=safe-stack/-fno-stack-protector flags.
The failed ELF has an unsafe-stack TLS relocation; disassembly at main+0x40
matches the faulting write through a null unsafe-stack pointer. Rebuilding the
unchanged fixture with the normal module flags removes that relocation.
The failed binary, serial log and result remain in before-fixture-flags/;
the faulted PID was verified absent and its exact three files/directory removed.
No kernel rebuild or production change was made for this harness correction.

Final verification: Normal Build WOS (RelWithDebInfo), isolated diagnostic build,
both packages, scoped formatting/clang-tidy, exec/compute source gates and policy
audit/unit tests pass. Five source-gate guard-removal mutations are rejected.
Final isolated --no-setup KTEST: **59,057 passed, zero failed**; the new test
passes at 320.021, summary at 459.717, automatic poweroff at 459.718.
Normal build ID: 46f608cce52899aad75757b707ccc548d2d4ac24; both live node IDs were verified by WOSDBG
and the session closed. The unchanged extended identity fixture, rebuilt
against the current sysroot, passes **54/54** local/remote main/worker/spawn
cases with child retirement checks. All three helper directories are
independently absent and both nodes sync successfully. All owned VMs and
launchers are down; no WOS VM remains at 2026-09-15T23:32:49.338213+00:00.

This qualifies ordered final admission/private cleanup and ordinary live
publication. It does not establish forced in-flight peer retirement with a
retained remote VFS dependency or concurrent scheduler publication. Exec stays
pending; the complete gate still rejects exec and the manifest remains
11 enforced/10 pending. Historical9FZ/9LG unexplained observations remain
preserved. Evidence: build/goal19-receiver-publication/validation.json.

## 9LS in-flight source retirement with a retained remote executable File

The current 9LR kernel passes all **12 phases**: held successful control,
held source reboot, and fresh success after reconnect, for root and UID1002
from both main and worker threads. Two private rootless `--no-setup` VMs use
existing TAPs; independent receiver management survives the source reset.
Only the recorded source QEMU PID/start-time pair receives `system_reset`.
No production code, credential policy, syscall ABI, wire format or declared
VFS routing changes in this stage. Explicit LOCAL `/` retains receiver-root
semantics. Normal and isolated kernel hashes still match 9LR.

Each control and reset phase holds a committed but unpublished receiver
candidate at the existing COMMIT diagnostic gate. Its private monitor is
observed, the source's active file grants increase, and bounded WOSDBG reads
prove that `Task.exec_image_file` owns a positive-reference REMOTE File with
a valid remote FD and source-node owner. The separate executable-pin view
count is zero: the retained dependency is the actual File, not an EXEC_PIN
view. Eight witnesses read 86 bytes each through four closed pause/read
sessions (32 sessions total). No credential/key memory is inspected.

In all four reset phases the receiver observes a different source boot epoch
while the original candidate remains private and marked `discard_completion`.
After hold release, logs identify that candidate's committed, nonexpired
refusal (`committed=1 expired=0`), owner-linked `wki-submit-post` teardown and
post failure. The active row and receiver PID disappear, executable-view
counts return to baseline, and the payload marker remains absent. Each
following fresh request executes and its source parent reaps the expected
exit status while retaining its six IDs and empty supplementary groups.
Reset destroys the old source VM process state; this is not a claim that its
old parent reaped a child. Payload identity mapping is covered by the separate
9LR identity matrix, not by this marker-only payload.

All four helper directories are independently absent on both nodes. Full
netdiag snapshots have no active compute rows; publication holds and waiters
are zero; both nodes sync. Both final live kernel IDs match
`46f608cce52899aad75757b707ccc548d2d4ac24` and the verification session closes.
The owned launcher exits zero. Host verification at
2026-09-16T00:07:56.232746+00:00 confirms both owned VMs and launcher are absent
and no WOS VM remains. Normal/isolated builds and KTEST were not repeated:
there is no production change and their 9LR evidence remains historical.
Current exec/compute source checks and policy audit/unit checks pass; the
complete policy gate still correctly rejects pending exec.

Failed experimental setups remain archived. The first dual-LAN configuration
omitted node3's already-declared identity mapping in its earlier LAN entry;
its effective policy rejected the root VFS request. Repeating the same declared
mapping there enabled the intended test. The initial owner-view assertion was
replaced by the actual File witness above. A malformed debugger batch failed
before connection and was corrected. The first fresh phase found its source
fixture missing after reboot cleared `/tmp`; recreating those files passed
without rerunning or overwriting the successful control/reset evidence. An
independent SFTP procfs read returned empty content; ordinary bounded local
reads then provided the cleanup observations. None of these failed attempts
is counted as a passing phase or a production defect fixed by this stage.

This closes the previously unqualified forced in-flight retirement case with
a real retained remote VFS dependency. It does not measure an individual
remote-close acknowledgement, exercise concurrent scheduler posting, or prove
the historical 9FZ/9LG unexplained observations fixed. Exec remains pending
until the final requirement-level reconciliation; optional transport expansion
is not an automatic requirement. Manifest counts remain **11 enforced / 10
pending**. Evidence: `build/goal19-inflight-exec-retirement/validation.json`.

## 9LT exec authority domain enforced

The final source/evidence reconciliation in
[the exec authority audit](goal19_exec_authority_audit.md#9lt-final-exec-authorization-reconciliation)
qualifies exec's immutable identity, original file admission, saved-ID/set-ID,
private preparation, source cutover, receiver final admission and associated
failure lifetime. Only the exec manifest row moves to enforced: **12 enforced /
9 pending**, with **vfs-traversal** first pending. General VFS and WKI domains
remain open. Historical9FZ/9LG completion/fetch observations and9EC availability
failure remain unexplained; no passing retry is represented as their fix.

No production code, ABI, locks, credentials, service grants or declared VFS
routing changes. Explicit LOCAL `/` still selects the receiver root. Current
policy audit/unit checks, manifest-domain preservation, source/kernel continuity
and patch checks pass; the complete gate now rejects pending vfs-traversal.
No new build/KTEST/live run was needed for this metadata-only decision. The
59,057/0 KTEST and54 identity cases are9LR evidence; the12 in-flight phases are9LS
evidence. The original fenced goal and `--no-setup` authorization are preserved.
Evidence: `build/goal19-exec-final-reconciliation/validation.json`.


## 9LU/9LV traversal identity correction

The [current traversal audit](goal19_vfs_traversal_audit.md) reconciles stale
directory/cwd implementation notes and records a confirmed ACCESS identity gap.
The unchanged baseline failed 8 identity checks with 2 passing effective-open
controls. The corrected kernel selects real UID/GID or AT_EACCESS effective IDs
from one admitted Snapshot; groups, ceiling, restrictions, Task credentials,
mode compatibility and declared routing are preserved.

Normal/diagnostic builds, 63 host credential tests, scoped formatting/tidy and
source/policy checks pass. Final isolated KTEST is 59,757/0 with automatic poweroff
and launcher exit 0. All 40 native/mapped TMPFS/XFS identity checks and 1,568 existing
traversal checks pass with verified live kernels and fixture cleanup. All owned
VM processes are down. Initial compiler/cache/tracer failures and the complete
59,733/24 mode-validation regression run remain archived; the unrelated mode
change was removed without relaxing identity assertions.

Traversal remains pending. Automatic mount lifecycle, reversed attachment order
and supported retained-root/binding concurrency need further reconciliation.
Counts stay 12 enforced / 9 pending; the complete policy gate correctly rejects
vfs-traversal. Evidence: `build/goal19-access-identity/validation.json`.


## 9LX transactional remote mount publication

Remote mount construction now finishes fallible lane admission before VFS
publication. A failed private candidate retires only its own group; ordinary
resource withdrawal remains generation-wide. Successful publication is the
commit, and subsequent asynchronous retirement preserves that successful
result. Constructor pins and exact mount-release ownership remain intact;
no registry lock spans VFS publication or remote waits. Declared LOCAL `/`
routing, retained cwd authority, credential and wire formats are unchanged.

Normal Build WOS and the isolated diagnostic build pass. Scoped formatting/tidy
and source/policy checks pass. Full KTEST passes 59,758 assertions with zero
failures and automatic poweroff, including same-resource candidate isolation
and native-child preservation. All 1,568 established live traversal checks pass.
The mount-cover fixture passes 24 checks with owner data/mode unchanged and
complete cleanup. Its initial run and errno-only probe incorrectly expected
EXDEV at the already occupied child; both failed runs remain archived. The
final fixture separately requires occupied-child EBUSY and fresh-remote-target
EXDEV. No production change was needed for this fixture correction.

Initial and final live IDs match normal kernel
`44568576e16e80d8b04b103fd486eb5feb9a0471`; diagnostic kernel is
`20230474f24f3c2495e0529ffcaac6f54eee3e97`. Fixture inventories are restored,
nodes synced, debug sessions closed, and the cluster launcher exits zero.
Host receipt at 2026-09-16T01:46:39.400474+00:00 confirms all task-owned
cluster processes absent and no WOS VM. The diagnostic VM had already powered
off and its launcher exited zero. Runs use `--no-setup` and private overlays.

The baseline owner's auxiliary ENOENT cause remains unqualified; transactional
rollback is fixed independently of its cause. Broader retained-root,
automatic-mount and mapped concurrency qualification remains open. Goal stays
active, 12 enforced / 9 pending, with vfs-traversal first pending.
Evidence: `build/goal19-mount-publication/validation.json` and
[the publication audit](goal19_remote_mount_publication.md).


## 9LY ordinary export replacement baseline

An unchanged9LX two-node probe confirms that an old automatic export binding
can read through a removed/replaced native mount. Owner TMPFS marker OLD is
read through its exact peer mount; ordinary owner umount succeeds, the old peer
row remains and reads underlying UNDER. Mounting a new TMPFS at the same path
then yields NEW through that same row. Serial shows initial resource4104 and
its original eight attaches without a replacement export. This is stale
resource-binding evidence, not a demonstrated UID/DAC bypass.

Source maps the gap to public native unmount lacking the export transition
used by pivot, refresh adding names without removing changed backing mounts,
and worker pathname operations resolving the captured export text. The next
patch must close/drain admission around native unmount, reconcile or cancel
according to the actual commit result, and preserve unrelated bindings and busy
mounts. It must distinguish old binding use from a newly admitted broader route.
See [the source and verification plan](goal19_export_replacement.md).

The first observation attempt failed before the baseline because SFTP returned
empty /proc/mounts; it terminated and cleaned up. Corrected WOS cat observation
and the independent underlying-only probe are retained. All scratch files are
removed and both nodes synced. Stale automatic rows remained until shutdown.
Both initial/final live kernels and symbols match44568576e16e80d8b04b103fd486eb5feb9a0471;
no kernel code/build/KTEST changed in this baseline stage. Kernel hashes are
unchanged, debug sessions close, launcher exits0, and the host receipt at
2026-09-16T01:59:20.776753+00:00 confirms no WOS VM remains. All live work uses
--no-setup with private overlays. Goal remains active,12enforced/9pending;
vfs-traversal remains first pending.
Evidence: `build/goal19-export-replacement/validation.json`.


## 9LZ native unmount export retirement

Public native unmount now closes and drains existing WKI VFS admission before
exact backing-dev-ID removal. Success withdraws only exports of that backing;
failed/busy removal cancels the transition and preserves matching issued Files
and bindings. Unrelated custom aliases are not rediscovered through the caller
root. The target is copied before blocking, existing empty-target errno is
preserved, and declared LOCAL `/` routing remains unchanged. Cancellation retains
the existing revision advance, so pretransition snapshots require reacquisition.
No syscall/wire/credential layout changes or locks held across the drain.

Final Normal Build WOS and isolated diagnostic builds passed, as did formatting,
clang-tidy and focused remote-VFS/lookup source checks. On normal kernel
06b3301d14b88ab8b1936c6fe9492989607172d9, all20 new unmount assertions and
all24 existing mount assertions pass. The old exact peer row disappears and
replacement obtains resource4104 after old4103. All1568 traversal assertions
were executed:1567pass,1fails with ETIMEDOUT in retained-directory STAT_AT
(phase1,grouped1,path `.`,op2). The failure is preserved; only the six previously
unexecuted cwd cases were subsequently run. No green rerun replaces it.
WOSDBG reconstructs two client timeouts from both node logs but cannot prove
cross-node order or the missing reply's cause. Response send status is discarded
in the owner handler; that is an investigation lead, not a demonstrated cause.

All fixture files are removed, both nodes synced, initial/final live symbols
match, debugger sessions close and launcher82298 exits0. Host ownership audit
at2026-09-16T02:36:51.071635+00:00 confirms no WOS VM before isolated KTEST.
Final isolated diagnostic KTEST passes59759/0 at466.521s, including the native busy/cancel/commit/custom-alias regression; automatic poweroff and launcher exit0. Host audit at2026-09-16T02:57:27.660620+00:00 confirms all owned VMs down.
Earlier full-rebuild and pre-empty-errno candidates, their runs, a fixture
SFTPAttributes observation error, and the interrupted superseded KTEST remain
archived separately. No superseded run qualifies the final diagnostic kernel.

See [native unmount review](goal19_native_unmount_exports.md) and
`build/goal19-native-unmount/validation.json`. Goal remains active,12enforced/
9pending; vfs-traversal is first pending. This bounded lifetime fix does not
qualify broader traversal, transfer/rotation, mapped concurrency or timeout
behavior. All live runs use --no-setup and isolated artifacts.


## 9MA retained-directory timeout observation

The unchanged 9LZ normal kernel passed the same 20 unmount, 24 mount-cover and
1,568 traversal assertions on fresh private nodes. Ten additional sequential
retained-directory fixtures passed 2,240 assertions. The earlier timeout did not
recur and remains unresolved; no production timeout fix is claimed.

Initial perf recordings were saved, but structured conversion failed at the
legacy size limit, and their remote begin/end coverage was incomplete. Both
failures and raw data remain archived. A diagnostic-only userspace collector
then drained every 5 ms, retaining remote records after the drain. Its focused
traces pair 2,260 source and 5,100 owner remote VFS begin/end records. All 2,240
primary path requests plus ten opens match unique owner replies by channel,
cookie and status. This is bounded successful-run evidence, not proof of all
interleavings. WOSDBG loaded 71,790 derived remote records and both serial logs;
global cross-node clock order remains unproven.

Recording is disabled, scratch inventories restored and nodes synced. Initial
and final symbols match; debugger sessions close; launcher15174 exits0; the host
PID/start-time audit at2026-09-16T03:12:34.244795+00:00 confirms no WOS VM remains.
No production source or kernel change, Normal Build WOS or KTEST rerun was needed.
The diagnostic collector compiled successfully. All live work used --no-setup.

The current manifest decision now reflects implemented native-unmount retirement
from9LZ, replacing its stale statement that this transition still needed fixing.
Next reconcile supported backend/root/cwd membership and binding concurrency
against current source and native tests. Unsupported surfaces must be assessed
through their actual refusals. Goal remains active:12enforced/9pending, with
vfs-traversal first pending. See [the observation](goal19_directory_timeout_observation.md)
and `build/goal19-response-timeout/validation.json`.


## 9MB native retained-context reconciliation

Reviewed native TMPFS/XFS root/cwd ownership, covered-mount membership,
search/namespace epoch validation, staged publication and scoped unsupported
backend refusals. Declared LOCAL `/` routing is unchanged. This reconciles the
native-context portion of traversal; mapped binding concurrency and internal
automatic mount lifecycle remain open.

The isolated diagnostic build passes with no compilation work; eleven inspected
source hashes and both kernel hashes are unchanged. Twenty-nine native-context
PASS records are indexed from the final9LZ KTEST (59,759/0); KTEST was not rerun.
Lookup, mount lifetime, remote VFS and policy source checks pass now. The mount
check's stale expectation of the original unmount target was corrected to the
bounded copied target, preserving exact-mount/dev-ID requirements. Its initial
failure is archived. No production edit, Normal Build WOS or live run in9MB.

Goal remains active:12enforced/9pending, vfs-traversal first pending. Details:
[native context audit](goal19_native_context_audit.md); artifacts: `build/goal19-native-context-audit/validation.json`.


## 9MC mapped directory binding retirement

Traced authenticated policy capture, pre-ACK fixed request admission, exact
binding/File ownership and mapped native traversal. Added deterministic source
and destination retirement after native directory preparation, before final
adoption, on TMPFS and XFS. Each refusal preserves reply/name, unclaimed source
and activity, registry size and binding references; same-grant positive controls
pass. Production behavior, declared LOCAL `/` routing and ABI/wire layout are
unchanged.

Normal Build WOS and isolated diagnostic builds pass. Four focused source/policy
checks and scoped formatting/tidy pass; an initial read-only host cache failure
is archived, and the retry disables caching. New isolated KTEST passes59,759/0
at471.612s, including the expanded grant test, and powers off at471.613s.
Launcher50620 exits0. The host PID/start-time audit at
2026-09-16T03:40:53.851608+00:00 confirms all owned processes absent and no QEMU.
Normal kernel735092b8027f69db3f19154ec37ab1af21450e70; diagnostic kernel
0742beb2b60f4980f0cdf6bd86b7505e3a048545. No normal VM run in9MC.

This reconciles the reviewed mapped admission/ownership/claim boundaries;
internal automatic mount and mixed attachment lifecycle remain the next
traversal item. The earlier directory timeout remains unresolved. Goal stays
active:12enforced/9pending. See [binding retirement review](goal19_directory_binding_retirement.md) and
`build/goal19-directory-binding-retirement/validation.json`.


## 9MD traversal qualification

Qualified `vfs-traversal` after reconciling automatic mount admission/publication,
exact-owner cleanup, supported backend search policy and explicit scoped
refusals. The requirement-level checklist links current source to57 relevant
PASS records from9MC and prior native/mapped ACCESS, traversal, mixed-mount and
export replacement fixtures. One Snapshot, exact retained identity, staged
denials and declared LOCAL `/` routing are preserved. No production edit.

Current native/mapped source hashes and both9MC kernel hashes match. No new
build, packaging or VM run in9MD; the latest full isolated run remains59,759/0.
The earlier timeout and auxiliary attach ENOENT cause remain unexplained and
visible. Neither is silently relabeled a successful run or a fixed transport bug.
Separate VFS operation commit obligations and broader WKI domains remain open.

All21 authority domains remain: **13enforced/8pending**. Goal19 stays active;
`vfs-open-create` is now first pending. See [the qualification checklist](goal19_traversal_qualification.md) and
`build/goal19-traversal-qualification/validation.json`.


## 9ME process-wide creation mask

Ordinary pthread creation reproduced stale masks on TMPFS and XFS: a worker
created0666/0777 after its process selected0077. Task now stores one atomic mask
on the retained canonical owner. Fork/clone/spawn and WKI independently inherit
one value; VFS open/mkdir/batch sample once. SETUMASK serializes with exec
publication and refuses a replaced-image worker without changing the new mask.
Syscall numbering, bounded0777 mask, credential Snapshots, WKI layout and declared
VFS routing (including LOCAL /) remain unchanged.

Normal Build WOS and isolated builds, scoped formatting, six selected source
checks and credential policy/unit checks passed. Final two-current-node live
matrix passes all7 invocations: native/mapped TMPFS and XFS, metadata batches,
remote execution, worker fork/spawn inheritance and child independence; all
fixture cleanup and guest sync succeeded. Both boot chains were verified. Final
isolated KTEST:59,783passed/0failed at475.552s, automatic poweroff and
launcher exit0; new owner-lifetime/replaced-worker and WKI roundtrip cases pass.
Exact process audit confirms all VMs down.

Retain baseline failures, initial harness/format/source-discovery errors, the
superseded partial diagnostic run, launcher conflicts, and mixed-version peer
configuration failure. The latter reproduced the old mask on the old receiver;
it is not relabeled a passing two-current-node run. Final receipts:
`build/goal19-shared-umask/validation.json`. See [shared-mask review](goal19_shared_creation_mask.md).

All21 domains remain13enforced/8pending; Goal19 stays active. Next open/create
reconnaissance found scalar remote OPEN client File/context and owner registry
allocation after possible backend mutation. No allocation-failure runtime
experiment or fix for that ordering is claimed here; map/strategy is in
`build/goal19-shared-umask/open-create-next.md`. The original fenced goal and
--no-setup authorization are unchanged.


## 9MF scalar remote-open preparation

Scalar remote OPEN now reserves an invisible owner descriptor row before the
backend and allocates client File/context/long-request storage before RPC.
Required private ownership is task-linked and drained after fatal-wait quiescence;
success transfers File/context/proxy ownership together. Exact channel publication,
stable credential Snapshots, errno, wire layouts and declared VFS routing,
including explicit LOCAL /, are preserved. Optional prefetch remains optional.

Normal Build WOS, isolated diagnostic build, scoped formatting/tidy and selected
source/credential gates passed. Two private current-kernel nodes passed66scalar
OPEN checks plus360retained OPEN_AT checks across TMPFS/XFS, with cleanup and
sync. Final isolated KTEST passed59,784/0 at473.930s, including allocation-failure,
task-exit/idempotence/ownership-transfer and owner-reservation tests; automatic
poweroff473.931s and launcher0. Exact host process audit at
2026-09-16T04:55:29.555134+00:00 confirms all VMs down.

Initial formatting/source-assertion errors and protected /proc scan failure are
retained. No live memory-exhaustion experiment was performed. Transport ambiguity
after authorized mutation and historical timeouts remain visible. No domain is
promoted: all21 remain13enforced/8pending and Goal19 stays active. Next review
covers backend commit/error and remaining remote descriptor lifetimes; bounded
reconnaissance is in `build/goal19-remote-open-preparation/open-create-next.md`.
Receipts: `build/goal19-remote-open-preparation/validation.json`.
See [remote-open preparation review](goal19_remote_open_preparation.md).


## 9MG OPEN reply admission

Scalar mapped OPEN now validates exact outer framing and all three current
reply forms before copying output or publishing a File. Valid errno, channel
identity/cookie matching, credentials, routing and wire layouts are preserved.
One rejection condition also prevents directory-reference/grant EPROTO from
being overwritten by the reply status. In-memory actual-handler KTEST covers
success forms, incomplete/extra/capacity/field refusals, unchanged output and
stale matching; it sends no transport traffic.

Normal Build WOS, isolated build, scoped formatting/tidy and selected host gates
pass. Full isolated KTEST59,785/0 at477.440s, automatic poweroff477.441s and
launcher0. Both normal boot chains match the current kernel. Live scalar OPEN
passes66checks; XFS OPEN_AT passes180 and one TMPFS repeat passes180. Preserve
the initial TMPFS timeout: after setup/mutation, host read timed out and guest
FSTAT op1050 waited60s. Cleanup succeeded and inventories showed no fixture
process before continuation. The426successful checks do not erase that failed
invocation or establish its cause. Both guests synced, cluster launcher0;
exact host audit2026-09-16T05:19:59.791319+00:00 found all VMs down.

All21 domains remain13enforced/8pending; Goal19 stays active. Next source-confirmed
gap: ordinary OPEN/OPEN_AT can publish an owner FD before task-exit/timeout drops
the client reply identity; unmatched replies are ignored and connected-peer GC
keeps the FD. Scalar private-allocation cleanup does not provide durable owner
cancellation. Design must reserve cleanup before mutation, retain exact operation
identity and preserve close/retirement semantics. No runtime leak count claimed.
See [OPEN reply review](goal19_open_reply_admission.md),
`build/goal19-open-reply-admission/validation.json` and
`build/goal19-open-reply-admission/ordinary-open-lifetime.md`.


## 9MH OPEN_AT private ownership

OPEN_AT now tracks its caller-prepared unpublished File, context and proxy
reference across the RPC. Ordinary failure leaves File ownership with the
caller; success transfers to that exact File; actual fatal-task cleanup releases
its path/mount pin outside the registry lock. The retained directory's immutable
proxy/FD identity no longer needs an I/O mutex across the wait. One credential
Snapshot, errno order, retained authority, declared VFS routing and wire ABI
remain unchanged. Durable owner FD cancellation is still required separately.

Normal Build WOS, isolated diagnostic build, scoped formatting/tidy and selected
host gates pass. Full isolated KTEST59,786/0 at474.745s, new ownership testPASS,
automatic poweroff474.746s and launcher0. Two private current-kernel nodes pass
all426 live checks:66 scalar OPEN and180 OPEN_AT on each of TMPFS/XFS, with
cleanup/sync. Both boot chains verified; exact QMP shutdown and launcher0.
Host audit2026-09-16T05:49:12.085860+00:00 confirms all VMs down.

Initial formatting and stale cleanup-call assertion failures are retained;
final checks pass. Receipt-script probes initially assumed a domains key instead
of operations and included the fence language in a content comparison; corrected
before ledger writes, without changing status or the original fenced goal.
Prior9MG timeout opcode1050 was mislabeled PATH_AS: local wire.hpp identifies
FSTAT. The timeout remains unexplained and is not claimed fixed. No owner-RPC
failure campaign or complete syscall-stack cleanup claim is made.

All21 domains remain13enforced/8pending; Goal19 stays active. Next implementation
requires pre-reserved exact-token ordinary owner cleanup across OPEN/OPEN_AT,
unknown replies and final close, with FIFO ordering/duplicate handling proven.
See [OPEN_AT ownership review](goal19_openat_private_ownership.md),
`build/goal19-openat-private-ownership/validation.json` and
`build/goal19-openat-private-ownership/owner-cleanup-next.md`.


## 9MI ordinary OPEN owner lifetime

The receiver now accepts a versioned wrapper for existing scalar/read/OPEN_AT
handlers and idempotent token cancellation. An owner-issued nonreused binding ID
and owner boot epoch prevent delayed old requests from reaching a successor,
even when RX supplies its current local channel identity. QUERY echoes a client
challenge. Per-binding highest consumed token prevents duplicate/late OPEN after
cancellation; senders must assign tokens in operation-slot/send order. Descriptor
publication stores both token and binding ID; cancellation releases the extracted
File outside the lock, preserving independently retained users and close errno.
Credential mapping, inner backend authority, declared routing and legacy wire
forms remain unchanged. No new feature is advertised and clients remain legacy.

Normal Build WOS and final isolated build, host auth-policy suite, scoped
format/tidy and selected source/policy checks pass. Final KTEST59787/0 at
467.814s; owner namespace/repeated cancellation/retained reader/close-error/
successor/retirement tests pass, automatic poweroff467.815s and launcher0.
Exact host audit2026-09-16T06:23:46.250354+00:00 confirms all VMs down.
The superseded16-byte draft also passed59787/0 but cannot qualify the final
32-byte binding contract. Initial stale source-call assertion and mistaken
nonexistent test-path probe are retained; final applicable checks pass.

Goal19 remains active, all21 domains13enforced/8pending. Unknown-reply owner
cleanup is not yet fixed end to end: client query, ordered token assignment,
pre-reserved durable outbox, strict reply admission and final-close/task-exit
ownership must be connected before feature advertisement and live cutover.
No new multi-node wrapped RPC run or historical timeout fix is claimed.
See [ordinary owner lifetime](goal19_open_lifetime_owner.md),
`build/goal19-open-lifetime-owner/validation.json` and
`build/goal19-open-lifetime-owner/client-cutover-next.md`.


## 9MJ ordinary OPEN client lifetime

Scalar OPEN/OPEN_READ and retained-directory OPEN_AT now require the advertised
OPEN_LIFETIME feature, validate a challenged owner binding/boot epoch, and reserve
a separate bounded ordinary cancellation slot before mutation. Tokens are assigned
after the exact proxy operation gate and never wrap. Preparation transfers the
slot to the published File or queues it after failure/fatal exit. Reply admission
validates full inner forms before copying; invalid replies never select a numeric
FD for cleanup. Credential mapping, retained authority, declared VFS routing and
existing syscall/wire layouts remain unchanged.

Final ordinary close tracks File ownership across flush/cancel; writable close
waits, preserving flush errno, and readonly close queues durable cancellation.
Fatal close does not replay an ambiguous write and finishes local ownership with
ECANCELED. Shared RDMA slot guards are task-linked and drained after waiter
quiescence, before File/context release; abandoned guards cannot clear successors.
ACK/proven exact retirement or a never-tokenized request releases the outbox ref;
other outcomes retain it for bounded deferred retry. Existing pin capacity and
diagnostics are separate.

Normal Build WOS and isolated diagnostic build, host auth suite, source/policy
checks and scoped formatting/tidy pass. Full KTEST59788/0 at475.150s, new client
casePASS312.054s, strict wrapped reply casePASS312.078s, automatic poweroff475.151s
and launcher0. Two current-kernel private nodes pass426 existing scalar/OPEN_AT
checks on TMPFS/XFS, plus48 child-exit flush cycles with distinct per-child data
and2200 readonly open/close cycles (zero backpressure retries). Cleanup/sync,
private boot-chain verification, exact QMP shutdown and launcher0 pass.
Host audit2026-09-16T07:02:58.347168+00:00 confirms all three stage VMs absent and no QEMU.

Initial source checks referenced the old unwrapped envelope and removed numeric
FD cleanup helper; final checks require wrapper accounting and exact-token cleanup.
The first lifetime fixture reused constant data; its successful draft receipts are
preserved but final visibility evidence uses distinct payloads. No transport-loss
campaign or historical timeout fix is claimed. The original fenced goal and
explicit --no-setup authorization remain unchanged. All21 domains remain
13enforced/8pending; Goal19 stays active. Next is requirement-level open/create
qualification across the already migrated native and mapped backends.
See [ordinary client lifetime](goal19_open_lifetime_client.md),
`build/goal19-open-lifetime-client/validation.json` and
`build/goal19-open-lifetime-client/next-open-create-audit.md`.


## 9MK open/create qualification

The supported open/create authority domain is now enforced after a requirement-level
review of public/direct/spawn entrypoints, canonical Snapshot/mask, fixed descriptor
and mount/File publication, TMPFS/XFS/FAT32/DEVFS/PROCFS admission, and mapped
scalar/read/OPEN_AT policy and lifetime. FAT synthetic ownership, unsupported
mapped synthetic filesystems and inherited procfd grants are explicit contracts.
Declared VFS routing and the original goal remain unchanged. No production code,
locks, ABI, wire format or service authority changes in this audit.

The unchanged current-task9MJ kernels are qualified by successful Normal Build WOS,
isolated build, full59788/0 KTEST and426remote authorization cases, plus48distinct-
payload child-exit flushes and2200readonly cycles. All stage VMs are down. This
audit extracts19actual current KTEST PASS records, builds/runs central credential
and lookup host suites, and runs lookup/mount source and final policy gates.
Historical9ME mask and9BM unsupported-selector runs are explicitly distinguished
from current evidence; no extra VM or kernel build is claimed for this audit.

The old pending text preceded completed prerequisites and is replaced by the
supported-operation authority contract. Unauthorized requests refuse before
backend effects; accepted mutation failures do not acquire an invented rollback
promise. No packet campaign or historical timeout fix is claimed. All21 domains
remain present, now14enforced/7pending. Goal19 stays active and the complete gate
must next stop at vfs-mutation. See [open/create qualification](goal19_open_create_qualification.md) and
`build/goal19-open-create-qualification/validation.json`.

## 9ML linkat admitted identity

`vfs_linkat` now captures one Snapshot before both lookups and all retries and
uses it at pair publication, matching the established rename admission model.
Namespace/mount/directory-view identity, readonly checks, protected-hardlink errno
and declared VFS routing remain unchanged; remote hardlinks remain unsupported.
Actual TMPFS/XFS cases replace a private Task identity between lookups, invalidate
the namespace, and verify original admission plus changed-identity fresh calls.
Denied requests leave source metadata and destination state unchanged.

Normal Build WOS, isolated diagnostic build, formatting/tidy, policy/source checks
and three host models passed. Current full KTEST: 59790 passed, zero failed;
poweroff and launcher exit0. The exact KTEST VM was confirmed down at
2026-09-16T07:29:22.523464+00:00. No multi-node evidence is claimed for this stage.
All21 domains remain present:14enforced/7pending, first pending vfs-mutation.
Goal19 stays active. See [linkat admission](goal19_linkat_admission.md) and `build/goal19-linkat-admission/validation.json`.

## 9MM namespace mutation qualification

Supported mkdir, symlink, removal, hardlink, rename and namespace batch authority
is now enforced after review of admitted Snapshot/retry/publication, native
parent/sticky/protected-link policy, retained identity/readonly views, synthetic
refusals and mapped receiver commit. Declared VFS routing remains unchanged.
The9ML code and exact current kernels are unchanged; its successful two builds,
full59790/0 KTEST, host models and formatting/tidy are referenced explicitly.

A fresh private two-node cluster passed6918mapped TMPFS/XFS mutation checks and
99readonly profile checks plus writable/retirement controls. Cleanup and sync
completed; exact private boot chains were verified and both guests stopped,
confirmed down2026-09-16T07:34:29.253631+00:00. Current source/policy gates pass.
Earlier pending traversal/cwd/scalar-mapping text was stale; current source and
9MD/9ML evidence supersede it. Batch per-item semantics and unsupported backend
operations remain explicit; no packet campaign or accepted-I/O rollback promise.

All21 domains remain present:15enforced/6pending. Goal19 remains active, with
vfs-metadata first pending. See [mutation qualification](goal19_mutation_qualification.md) and
`build/goal19-mutation-qualification/validation.json`.

## 9MN metadata qualification

Supported pathname and retained-File chmod/chown/timestamps/xattrs are now
qualified, including one Snapshot through retries, native prepare/commit, exact
File selection, readonly policy and mapped owner identity. Truncate/data/mapping
operations retain admitted description grants. FAT fixed metadata and unsupported
backends preserve existing refusal/errno. Declared VFS routing is unchanged.
Earlier unmapped-chown/descriptor/readonly/cwd blockers were stale and are
superseded by current source and evidence. No production change in this audit.

The unchanged9ML current kernels retain their successful builds,full59790/0
KTEST,host/source and formatting/tidy receipts. Fresh private two-node guests
passed2128native/mapped metadata,xattr,truncate,readonly and mapping checks,
including the retained-cwd xattr regression. Cleanup,sync and private boot-chain
verification passed; exact guests were down2026-09-16T07:43:34.212664+00:00.
The current-task9MM remote-readonly profiles are explicitly reused. No historical
selector-disabled run is claimed as new,and no packet campaign was performed.

All21 domains remain:16enforced/5pending,first pending mount. Goal19 stays active.
See [metadata qualification](goal19_metadata_qualification.md) and `build/goal19-metadata-qualification/validation.json`.

## 9MO mount identity exhaustion

Mount publication now refuses EOVERFLOW after issuing the final nonzero 32-bit
identity, before table, reservation, covered-directory or generation changes.
The existing mount lock owns the counter; exact unmount/export retirement cannot
select a replacement through ID reuse. Credentials, ABI, ordinary errno and
declared VFS routing remain unchanged. The actual boundary test uses a private
cursor without rewinding production, proves unchanged repeated denial, then
ordinary publication and exact-ID retirement.

Normal Build WOS, isolated diagnostic build, format/tidy, source/policy checks
and three host models pass. Full KTEST:59791 passed/0 failed at478.307s,
automatic poweroff478.308, launcher0. Exact VM1854902 was absent, with no QEMU
remaining, at2026-09-16T08:03:29.935549+00:00. Initial command/source-test issues
are preserved separately. No billion-operation runtime reproduction is claimed.
All21 domains remain16enforced/5pending, first pending mount; Goal19 active.
See [mount identity](goal19_mount_identity.md) and
`build/goal19-mount-identity/validation.json`.

## 9MP mount qualification observation

Current-source review covers admitted mount source/target authority, publication,
exact unmount/export retirement, pivot, immutable flags and automatic lifetime.
Fresh private9MO-kernel guests pass230mount/readonly checks and cleanup. Local
nosuid exec passes71/72: one worker-with-observer case exits137. An unchanged
local repeat and independent remote matrix each pass72/72, but do not explain
the first failure. All scratch is cleaned, guests synced, boot chains verified;
exactPIDs1857545/1857565 absent and no QEMU at2026-09-16T08:10:22.232134+00:00.

Mount stays pending. Source review found a late internal SIGKILL retirement-wake
race after the initial exit-request check;9MQ addresses that gap without claiming
a causal trace for the live failure. Earlier mount prerequisite text is stale,
while9LZ directory timeout remains unexplained. All21domains16enforced/5pending;
Goal19 active. See [mount review](goal19_mount_qualification.md) and
`build/goal19-mount-qualification/validation.json`.

## 9MQ late SIGKILL retirement dispatch

Fatal SIGKILL dispatch now rechecks an already-published exit request after
signal selection, so internal exec sibling retirement cannot become group kill
through the gap after the first check. Shared consumption preserves thread-only
retirement and exact requested process status; genuine SIGKILL137/9, other
signals, teardown guards, credentials, ABI and declared routing remain unchanged.
No new production lock, allocation or wait. The private kernel test exercises
actual request/selector state; live fixtures cover ordinary teardown/completion.

Both builds, format/tidy, focused source/policy checks and5host models pass.
Final fullKTEST59792/0 at477.337s, poweroff477.338, launcher0. Initial test-only
orphan-thread credential-view failure is preserved; corrected casePASS244.384.
Fresh private guests pass734mount/readonly/nosuid checks with cleanup/sync/private
boot-chain verification. All owned VMs down; final cluster absent at2026-09-16T08:34:26.312671+00:00.
The9MP status137 has no causal runtime trace and is not retroactively explained.
All21domains16enforced/5pending; mount/signals require separate qualification.
See [signal retirement](goal19_signal_retirement.md) and
`build/goal19-signal-retirement/validation.json`; Goal19 stays active.

## 9MR mount authorization qualification

Supported mount, unmount and pivot authority is now enforced after reconciliation
of admitted credentials, source/provider and target binding, private publication,
immutable mount policy, exact native/remote retirement and automatic lifetime.
Declared VFS routing remains unchanged. The stale active pending text is replaced
by the current supported contract; historical evidence and failures are preserved.

This status-only review verifies exact9MQ source/kernel continuity and reuses its
two successful builds,59792/0KTEST,five host models,source/format gates and734live
mount/readonly/nosuid checks. Cleanup,sync,private boot chains and all owned VM
shutdown are proved. No extra production change, build or VM run is claimed.
The9MP worker137 and9LZ timeout have no newly claimed causal explanation.

All21domains remain:17enforced/4pending,first pending signals. Goal19 stays active.
See [mount enforcement](goal19_mount_enforcement.md) and
`build/goal19-mount-enforcement/validation.json`.


## 9MS terminal foreground provenance — 2026-09-16

Foreground selection now retains the admitted group node/boot origin through
PTY claim/set/reset, control-character snapshots and hangup. Both signal paths
use exact group/session/terminal attachment identity. The no-match proxy fallback
had a stale forwarded-TIOCSPGRP premise and is removed; both remote IPC ends
already reject that actorless session mutation. Numeric ioctl ABI, credential
policy, locks and declared VFS routing are preserved.

Normal Build WOS and isolated diagnostic build pass, as do formatting/tidy,
focused source checks and five rebuilt host models. Full KTEST passes
**59,812 / 0** at 479.341s and powers off at 479.342s. The new origin predicate and
actual claim/denial/reset cases pass. The initial source occurrence-count check
became stale after fallback removal; it now checks the retained entrypoints.
An initial CTest selector selected zero tests; the corrected selector runs all
five and rejects empty selections. Original receipts are retained.

Fresh private guests pass 112 root/UID104 terminal/session checks plus 20 local/remote
signal checks. Every fixture cleanup and guest sync completes; exact owned
processes 2030844/2030868 are absent at 2026-09-16T09:03:06.440903+00:00.
Diagnostic process 2028949 is also gone. No WOS VMs remain; unrelated host guests
were not stopped. Boot manifest/backing-chain verification identifies the tested
payloads; no live debugger build-ID observation is claimed.

See [the focused audit](goal19_terminal_foreground.md) and
`build/goal19-terminal-foreground/validation.json`. This source finding is not
claimed as a live distributed collision reproduction or a causal explanation
of historical timeouts/exit 137. Signals remains pending, including terminal proxy
cancellation/current receiver membership review. All 21 domains remain 17 enforced,
4 pending; Goal19 stays active.


## 9MT remote terminal transition observation — 2026-09-16

A bounded native/remote comparison proves a remaining signal-authority gap:
remote children that change group or create a new session still receive SIGINT
from the previous terminal. Four remote cases fail (root/UID104 × group/session),
while all six native cases and both remote inherited-group controls pass.
Every case first proves real terminal delivery, then reports current job identity
before the second event. All targets exit normally and all targets/controllers
are reaped. Fixture helper cleanup and guest sync complete. Exact owned VMs
2032930/2032971 are absent at 2026-09-16T09:12:10.289343+00:00; no WOS VMs remain.

Source analysis identifies source-proxy selection followed by trusted TASK_CANCEL,
whose receiver does not check current job membership. The earlier unpublished-wake
suspicion is ruled out by find_running_task's published filter. No production fix
is claimed. Receiver-owned terminal admission needs a versioned protocol that
preserves ordinary lifecycle cancellation and existing terminal grants; extending
legacy CANCEL or ACCEPT fields without negotiation would be unsafe compatibility.

The fixture compiles, formatting and Python compilation pass. Existing 9MS kernel
and source hashes match; no new kernel build or KTEST run is claimed. See
[the observation](goal19_remote_terminal_transition.md),
`build/goal19-remote-terminal-transition/validation.json` and its next-patch contract.
All 21 domains remain 17 enforced/4 pending; signals is the first pending domain.
Goal19 remains active, with a concrete reproduced defect as the next action.


## 9MU receiver-owned terminal admission — 2026-09-16

The four 9MT remote setpgid/setsid terminal failures are fixed. A distinct
versioned/advertised terminal operation carries full session/group origins to
receiver-owned admission of the exact accepted Task. Current membership is checked
under VM publication/local signal admission before process/worker effects, while
UID changes retain terminal object authority. Further routing preserves expected
origins with a bound. Refusals never signal the parked source frame or substitute
lifecycle cancellation. ACCEPT/CANCEL formats, ordinary signal policy and declared
VFS routing remain unchanged.

Normal Build WOS, isolated diagnostics, formatting/tidy, seven source checks and
four rebuilt host suites pass. Full KTEST passes 59,814/0 at 483.631s and powers
off normally. The expanded matrix passes all 28 input/hangup/group/session/UID-drop
cases, including the original four failures. Existing terminal/session and signal
fixtures pass another 132 checks. All 160 functional checks pass; every fixture
cleanup and guest sync completes. Exact processes 2092855/2092890 are absent at
2026-09-16T09:38:00.761463+00:00; diagnostic 2090632 is also gone. No WOS VMs remain.

Older peers without the new COMPUTE advertisement refuse terminal events; they
are not sent through the unsafe old fallback. This stage does not claim a live
multi-hop terminal transition. Initial stale source assertions and an intermediate
Python test-path typo are preserved in the logs and corrected without production
rollback. See [the focused audit](goal19_remote_terminal_admission.md) and
`build/goal19-remote-terminal-admission/validation.json`.
All 21 domains remain 17 enforced/4 pending; Goal19 stays active, first pending signals.


## Goal19 9MV: source terminal cutover admission review

9MV source review finds an unleased source-terminal cutover window in signal_terminal_process_group and revoke_terminal_attachment: the route helper may return nullopt, exec can then publish acceptance/drain zero admissions/capture pending state, and the source subsequently generates a local pending bit. This is a source-backed interleaving, not a live reproduction or causal explanation of historical failures. Exact function/source hashes and lock constraints are recorded in build/goal19-signal-producer-audit/inspection.json. Four existing source suites pass despite the gap. Leader detach already holds nonrecursive VM publication and session mutation, so directly calling the receiver helper is not a safe fix. No production edit, kernel build or new runtime result; preserve 9MU and implement source admission with actual-producer cutover tests next. Goal remains17 enforced/4pending, first pending signals. See docs/goal19_signal_producer_admission.md.

[Current flow, impact map and next patch contract](goal19_signal_producer_admission.md).


## Goal19 9MW: source terminal admission across remote exec

9MW closes the 9MV source-terminal remote-cutover window. Both scheduler terminal producers use a borrowed-Task helper that acquires existing PendingSignalTarget admission before route selection, rechecks current full session/group and input binding, and holds a local lease through generation/wake. Remote admission owns no local lease and cannot fall back to the parked frame. Revoked HUP/CONT retains its terminal grant; UID-independent grants, existing worker enumeration, credentials, syscall/wire ABI and declared VFS routing remain unchanged. No recursive VM publication lock, new queue or allocation. Actual process/worker interleaving test runs under an already-held publication guard and passes; final full KTEST59815/0, normal/diagnostic builds,format,4source suites,4host suites,policy gates and160live checks pass. Initial KTEST59814/1 failed before admission because the synthetic worker lacked owner_pid; corrected setup only, original kernel/logs and deliberate-failure-panic shutdown retained. All fixture cleanup/sync and owned VM shutdown verified; no WOS VMs remain at 2026-09-16T10:23:39.832941+00:00. Broader signals/WKI qualification remains pending; counts17/4 unchanged. Evidence: docs/goal19_terminal_source_admission.md and build/goal19-terminal-source-admission/validation.json.

[Implementation and verification](goal19_terminal_source_admission.md).


## Goal19 9MX: traced-child ordinary SIGKILL baseline

9MX reproduces ordinary SIGKILL failing to terminate a trace-stopped owned child on unchanged9MW kernel58f889e0. All8 ordinary-kill cases fail across two nodes,UID0/104 and user/syscall loops: call succeeds, no termination within2s, explicit detach/re-kill cleanup then reaps signal9. All16 ptrace-KILL and SIGCONT/pending-USR1/detach controls pass. All24targets/controllers reaped and helpers/SHM cleaned. Source has unconditional ptrace-stopped scheduler parking plus two ptrace syscall park loops; a syscall-issuing loop is not qualification of PTRACE_SYSCALL stops. No kernel fix/build/newKTEST claimed. Fixture compile/format/syntax pass; private payload hashes/backing chains match, sync and exact owned VM shutdown verified at2026-09-16T10:33:06.698035+00:00. No WOS VMs remain. Implement coherent fatal-stop admission preserving denied-operation state, catchable/CONT stops, event claims, safe return frames and thread-only retirement next. Goal17/4 unchanged, firstpending signals. See docs/goal19_ptrace_signal_baseline.md and build/goal19-ptrace-signal-baseline/validation.json.

[Observed outcomes and implementation contract](goal19_ptrace_signal_baseline.md).


## Goal19 9MY: SIGKILL releases ptrace stops

9MY fixes the 9MX ordinary-SIGKILL trace-stop failure. A shared atomic stop gate permits fatal execution through all three scheduler branches and both ptrace syscall-stop loops; asynchronous and post-park stop publication recheck fatal state. SIGKILL remains visible until exit cleanup, preserving the pending-to-exit interval and existing thread-only retirement disposition. Stop claims and exit events keep their existing ownership; SIGCONT/catchable signals, current authorization, syscall/wire ABI and declared VFS routing are preserved. Final Normal/isolated builds, full KTEST59823/0, scoped formatting, four source suites, four host suites and policy gates pass. The unchanged baseline now passes24/24 with no recovery detach, including all8 previous ordinary-kill failures. Explicit entry/exit/fatal-syscall/breakpoint/queued-stop/foreign-UID-denial cases pass24/24; existing ptrace suites pass384 checks and signal/terminal suites160. The first extended run had a fixture-only UID104 shmat setup failure; moved the credential drop after reattachment, retained failed receipts and verified all12 targets/controllers and helper cleanup. Preliminary interrupted KTEST and a detected stale-package launch are retained and do not count as final-kernel qualification. All final helpers/children cleaned, both guests synced, owned VMs2331712/2331730 gone at2026-09-16T11:13:36.453235+00:00. Goal remains17 enforced/4pending, first pending signals; nonterminal remote-exec producers and broader distributed/native-worker qualification remain open. Evidence: docs/goal19_ptrace_fatal_release.md and build/goal19-ptrace-fatal-release/validation.json.

[Implementation and verified evidence](../docs/goal19_ptrace_fatal_release.md).


## Goal19 9MZ: parent-death remote-exec baseline

9MZ reproduces a parent-death signal gap across remote exec on unchanged9MY kernelb52361d6. UID0/104 local replacements retain registration10 and observe one SIGUSR1 after their parent exits; both remote replacements report registration0 and observe no signal in1.5s. Six controls pass, two enabled-remote cases fail. Each target reports readiness before parent release; all8 direct parents reaped status0, all8 final target observations received, source/receiver procfs entries absent and both helpers removed. Procfs disappearance is not claimed as an observer wait receipt for the orphaned targets. Source TaskExecSignals v1 omits registration, and child_events parent exit directly generates on the retained source Task without local signal admission or accepted-route forwarding. SIGCHLD is another direct-source producer but is not qualified by this fixture. No production edit, new kernel build or newKTEST claimed. Fixture compile/format/syntax, three existing signal/WKI source suites and policy gates pass; private payload/backing and exact shutdown verified at2026-09-16T11:25:38.571120+00:00. Goal remains17 enforced/4pending, firstpending signals. Next implement synchronized child-owned registration plus negotiated parent-death event forwarding over exact accepted execution identity, preserving post-exec registration changes, cutover admission, lifecycle refs and declared VFS routing. Evidence: docs/goal19_parent_death_baseline.md and build/goal19-parent-death-baseline/validation.json.

[Observed results and implementation requirements](../docs/goal19_parent_death_baseline.md).

## Goal19 9NA: parent-death delivery across remote exec

9NA implements parent-death registration transfer and event forwarding across remote exec. Registration is atomic; local lifecycle delivery owns signal admission through generation, while accepted routes forward a dedicated event over the exact task/session/channel generation after final ACCEPT. The receiver uses its current registration, including disabled or changed values, with no source-proxy fallback. Exec signal record v2 appends registration and reserved padding; a new advertised compute resource gates submission, and the event requires COMPUTE plus IDENTITY_SAME_DOMAIN. Worker exec transfers the retained process owner registration, matching local exec. Credential/syscall semantics, task references, existing kill/terminal/cancel protocols and declared VFS routing are preserved. Normal and isolated builds, scoped formatting, four source suites, five host suites and policy gates pass. Full KTEST passes 59825 assertions with zero failures. The unchanged 9MZ baseline now passes 8/8, fixing both previous remote failures; 30 extended UID0/104 cases pass across local, remote and round-trip exec, including receiver disable/enable/change. All 38 direct parents reaped, final target reports received, 48 receiver/intermediate procfs absence probes passed, and both copies of each helper removed. Orphan procfs absence is not an observer wait receipt. Existing signal/terminal regressions pass 160 checks, ptrace suites 384 checks, and extended ptrace/signal cases 24/24. The initial diagnostic build caught a selftest-only use of nonexistent Task::acquire; corrected to try_acquire before final qualification. Diagnostic VM2400124 powered off after KTEST; both private guests2401535/2401574 synced and were verified absent at2026-09-16T11:57:52.392845+00:00. Goal remains17 enforced/4pending, first pending signals. SIGCHLD, broader native-worker/distributed qualification and live mixed-version/peer-restart behavior remain open. Evidence: docs/goal19_parent_death_delivery.md and build/goal19-parent-death-delivery/validation.json.

[Patch contract and observed results](../docs/goal19_parent_death_delivery.md).

## Goal19 9NB: child events after parent remote exec

9NB establishes the child-event baseline after parent remote exec on unchanged 9NA kernel0dc73554. At UID0/104, local parents with local or remote children pass all4 controls: pre-exit WNOHANG returns0, SIGCHLD is observed once and waitpid consumes the correct source child PID/status37. All4 migrated-parent cases fail: remote and round-trip replacement parents report ECHILD before and after the gated child exit and receive no SIGCHLD. All8 direct parents are reaped status0 and children report their exit path; both helper files are removed. Procfs entries disappear, but bounded live debugger inspection proves that this is insufficient cleanup evidence: all4 failed source parent/child pairs remain on node0 dead lists, each Task at refcount3; each parent retains its child list, unconsumed event and SIGCHLD bit65536, and each child retains the parent in ZOMBIE membership with status9472. Control families are absent from the same dead-list scan. Source corroborates three linked gaps: direct SIGCHLD writes bypass accepted execution routes, waitpid claims only receiver-local queues, and finalize_proxy_task omits child-event exit preparation. Its scheduler callout also requires a context-safe cleanup design rather than blindly adding a blocking helper. WOSDBG discovery requires a debug endpoint absent from this launch, so inspection used an exact-owned private UNIX GDB socket with both nodes briefly QMP-paused and resumed, reading only recorded Task lifecycle fields. A first listener-syntax error was retained and safely resumed; corrected inspection succeeded. Fixture compilation/format/syntax, four source suites and policy gates pass; no production edit, new kernel build or new KTEST claimed. Both guests synced and PIDs2404078/2404091 were verified absent at2026-09-16T12:12:12.984058+00:00, discarding the retained test-only guest state. Goal remains17 enforced/4pending, first pending signals. Next implement coherent distributed child-event ownership/claim/commit, notification and proxy retirement while preserving task lifetime, exact accepted-session authority, wait selectors, retry and declared VFS routing. Evidence: docs/goal19_child_event_baseline.md and build/goal19-child-event-baseline/validation.json.

[Source map and live observations](../docs/goal19_child_event_baseline.md).


## Goal19 9NC: deferred proxy child retirement

9NC repairs source-proxy child retirement. The existing ACTIVE-to-EXITING completion winner stabilizes output/status/rusage and resources, then transfers an additional Task lifetime reference to an allocation-free intrusive FIFO. Compute RX workers retire child/event/waiter/tracer ownership outside peer lifecycle and queue locks before releasing the process charge and publishing immutable exit/dead-list state. Duplicate finalization cannot enqueue twice. The unchanged 9NB baseline retains four local-parent passes and four migrated-parent ECHILD/SIGCHLD failures, but bounded matching-ELF GDB inspection now finds all per-CPU dead lists and deferred queues empty on both nodes: the eight retained Tasks are reclaimed. All16 live-orphan cases pass across UID0/104, remote/round-trip parent and local/remote child with parent-death registration enabled/disabled; direct parents are reaped before child release. Existing30 parent-death and568 signal/terminal/ptrace checks pass. Normal Build WOS, isolated diagnostic build, full freshly packaged KTEST59826/0, five host suites, four source suites, scoped formatting and fixture compilation pass. Initial WKI/wait source guards assumed the old monolithic finalizer; corrected guards verify cleanup, retirement and publication across the new helpers, with failed receipts retained. No production edit followed final kernel qualification. Both guests synced; exact private PIDs2464897/2464900 and diagnostic2463171 are gone, no WOS VMs at2026-09-16T12:42:36.104525+00:00. Credentials, syscall/wire ABI, wait status/rusage, peer policy and declared VFS routing are preserved. Live peer-failure retirement remains unqualified, and pre-enqueue output/FD cleanup keeps its existing contexts. Goal remains17 enforced/4pending, first pending signals; next implement the remaining distributed child-event authority, claim/commit and notification contract. Evidence: docs/goal19_proxy_child_retirement.md and build/goal19-proxy-child-retirement/validation.json.


## Goal19 9ND: group origin in child waits

9ND preserves node/boot group identity through wait selection, immutable child events, blocked waiter registrations and claims. waitpid(0) captures the caller's exact group; negative numeric aliases resolve only among owned child/tracer subjects and queued events, with local then inherited then unique foreign precedence and no consumption on ambiguity. Positive-PID/any-child selection, retry/WNOWAIT/single winner, rusage and encoded procfs diagnostics are preserved. Two actual-lifecycle KTESTs cover node/boot collisions, origin-only group changes, immutable snapshots, claim retry and reference release. The identical native fixture on previous9NC and current kernels proves the bug and fix at UID0/104: imported groups123/141 collide with receiver-local child groups123/141. Previously waitpid(0) wrongly reaped those local children and negative wait returned0; now zero wait returns0, negative wait reaps local exit41, and final own-group wait reaps foreign children108/126 with exit37. All children/direct parents are reaped, recorded process entries absent and both helpers removed on both boots. Normal/isolated builds, full freshly packaged KTEST59828/0, five host suites, five source suites, formatting and fixture compilation pass;568 existing signal/terminal/ptrace checks pass. Stale numeric-snapshot and pre9NC-finalizer source guards were corrected, with failed receipts retained; fixture PID-format warnings corrected before runtime. Both baseline and fixed guests synced and exact shutdown completed, diagnostic2524985 powered off; no WOS VMs at2026-09-16T13:06:58.907093+00:00. Credentials, syscall/wire ABI and declared VFS routing unchanged. Goal remains17 enforced/4pending, first pending signals: distributed wait authority/claims/commit and SIGCHLD forwarding remain required. Future remote alias resolution must merge origin candidates across owned queues before selecting once. Evidence: docs/goal19_wait_group_origin.md and build/goal19-wait-group-origin/validation.json.


## Goal19 9NE: canceled child claim lifetime

9NE makes child-event claim release and commit validate through the waiter's lock-protected current event link before accessing storage. Copied event pointers remain borrowed identity; queue removal clears the link before subject reclamation. Identity and cookie checks reject stale tokens without disturbing replacement claims, preserving retry, WNOWAIT, single-winner commit, rusage and reference cleanup without new allocation or stack-owned references. Two lifecycle KTESTs cover parent-exit/tracer-detach reclamation and same-event reissue. An extracted-function host regression distinguishes old invalid access from safe current behavior; it is not live kernel concurrency evidence. Normal Build WOS, isolated diagnostic build, full freshly packaged KTEST59830/0, five host suites, five source suites and scoped formatting pass. Current-kernel group-wait cases pass at UID0/104 with all children/direct parents reaped and helpers removed;568 existing signal/terminal/ptrace checks pass. Both private guests synced and exact PIDs2591900/2591860 exited; diagnostic2585347 is absent, with no WOS VMs at2026-09-16T16:57:02.151150+00:00. Credentials, syscall/wire ABI, errno and declared VFS routing are preserved. Goal remains17 enforced/4pending, first pending signals; distributed child-event authority/claims/commit, SIGCHLD forwarding and peer-failure qualification remain required. Evidence: docs/goal19_wait_claim_retirement.md and build/goal19-wait-claim-retirement/validation.json.


## Goal19 9NF: distributed child authority contract

9NF completes source reconnaissance for the remaining distributed child-authority protocol. Current RX workers hold an exclusive peer lifecycle lease and shard by source node, so a recursive blocking reverse wait cannot safely retain that worker/lease while awaiting a round-trip response. Each Task has one claim backlink; remote transactions therefore need independent claim records, session-bound deferred continuations and explicit fatal-task/peer-generation cleanup. Reverse ownership must bind exact committed exec-handoff rows rather than PID or UID, including receiver waits issued before source final-ACCEPT processing. Group candidates must normalize hop-local origins and merge before selection. Commit reply loss requires retained transaction results and a defined terminal policy; positive-PID collisions across execution namespaces also remain unresolved. Source review of retire_main_thread does not support the suspected immediate DEAD-owner failure, so no speculative native-owner fix was made. Fifteen source hashes and23 function excerpts are recorded; current wait/WKI source guards pass despite lacking the distributed contract. No production edits, kernel build, KTEST or new live execution claimed; no WOS VMs observed. Goal remains17 enforced/4pending, first pending signals. Next implement independent lifecycle claim ownership, then the full negotiated reverse-authority/claim/commit/notification path and owned-process qualification. Evidence: docs/goal19_distributed_child_contract.md and build/goal19-distributed-child-contract/inspection.json.


## Goal19 9NG: independent child-event claims

9NG introduces stable noncopyable ChildEventClaim records and routes both local waiters and independent nonblocking claims through one lifecycle-locked implementation. Events name claim records, local embedded records retain their waiter Task, and independent records carry no executor identity. Busy records cannot overwrite a claim; cancellation/removal clears backlinks before releasing storage, while identity/cookie checks preserve EFAULT retry, WNOWAIT, single-winner commit and accounting. Two actual-lifecycle KTESTs interleave independent/local claims, wrong-owner/record refusal, cancellation/reissue, duplicate commits, exact rusage and parent-exit/tracer-detach reclamation. Normal Build WOS, isolated diagnostic build, full freshly packaged KTEST59832/0, five host suites, five source suites and scoped formatting pass. Current-kernel UID0/104 group-wait cases pass with children/direct parents reaped and helpers removed;568 existing signal/terminal/ptrace checks pass. Production and KTEST source hashes match the builds. Diagnostic2669530 exited before cluster launch; both private guests2671384/2671379 synced and exited, with no WOS VMs at2026-09-16T17:25:53.013873+00:00. Internal Task layout changes, but credential/syscall/wire semantics and declared VFS routing are unchanged. External records require stable storage until explicit cancel/commit; no remote protocol or session cleanup is enabled yet. Goal remains17 enforced/4pending, first pending signals. Next implement negotiated reverse exec authority, deferred continuations, distributed selectors/claim/commit and SIGCHLD forwarding, retaining the9NF PID-collision/reply-loss requirements. Evidence: docs/goal19_independent_child_claims.md and build/goal19-independent-child-claims/validation.json.


## Goal19 9NH: reverse child-authority admission

9NH implements module-internal reverse child-authority admission over exact retained Tasks and authenticated exec sessions. Receiver lookup distinguishes ordinary spawn from published/committed exec; source lookup requires the accepted submitted row, exact task/receiver/channel/generation/identity and COMPUTE plus IDENTITY_SAME_DOMAIN policy with credential capability. Requests preceding final ACCEPT defer without a reference; ready results acquire exactly one retained owner reference. No event is claimed or consumed, and no PID fallback, new wire/syscall caller or protocol advertisement is enabled. The actual-row KTEST invokes the ACCEPT handler and checks pending windows, identity/policy mismatch, retirement and exact reference cleanup. Normal Build WOS, isolated diagnostic build, full freshly packaged KTEST59833/0, two host tests, WKI/wait source guards and scoped formatting pass. Source/kernel hashes match; diagnostic2738206 is absent with no matching WOS VMs at2026-09-16T17:55:16.604628+00:00. No new normal-cluster or end-to-end distributed wait checks are claimed. Source review additionally requires single-owner child CPU accounting across source commits and receiver completion deltas. Credential/syscall/wire semantics and declared VFS routing are preserved. Goal remains17 enforced/4pending, first pending signals; deferred transactions, distributed selectors/claim/commit, SIGCHLD delivery and peer-failure qualification remain required. Evidence: docs/goal19_child_authority_admission.md and build/goal19-child-authority-admission/validation.json.


## Goal19 9NI: child-event SIGCHLD delivery

9NI routes child-event SIGCHLD through bounded local signal admission or an allocation-free, Task-retaining deferred queue. Exec drains local admission before its pending snapshot; workers asynchronously wait for ACCEPT and recheck exact submitted Task/channel generation plus current authenticated policy and advertised support. A versioned eight-byte TASK_CHILD_SIGNAL (0x64), resource0x53494706 and all submission preflights preserve compatibility. RX grants only SIGCHLD to the exact published execution; further hops enqueue without recursively waiting under a peer lease. Coalescing, in-flight/requeue pins and task cleanup preserve lifetime without changing event claims, accounting, credentials, syscall ABI or declared VFS routing. Normal/isolated builds, formatting, five source suites, two host suites and full freshly packaged KTEST59835/0 pass. The unchanged UID0/104 fixture now receives SIGCHLD in all8 cases, fixing the four previous missing notifications; all4 migrated-parent ECHILD wait failures remain recorded as failures. Direct parents were reaped, children reported exit, helpers were removed; matching-ELF inspection found both nodes' dead lists and deferred queues empty. Existing568 signal/terminal/ptrace checks pass. Source/image hashes match. Diagnostic2806495 and private2809121/2809098 are down, no matching WOS VMs at2026-09-16T18:22:39.174622+00:00. Initial invalid KTEST build flags and stale worker-parking source guard were corrected with failed receipts retained. Goal remains17 enforced/4pending, first signals; distributed wait transactions/selectors/claim/commit, accounting, native-worker and peer-failure qualification remain open. Evidence: docs/goal19_child_signal_delivery.md and build/goal19-child-signal-delivery/validation.json.


## Goal19 9NJ: retained child-wait transactions

9NJ implements bounded source child-wait transactions with stable independent claims, exact accepted-exec/session admission and one retained canonical owner reference. Requests bind immutable arguments to64 per-session lanes and nonzero increasing sequences;128 fixed records cache value-only outcomes until ACK or retirement. Completion watermarks prevent acknowledged or pre-canceled BEGIN requests from claiming another event. Commit rechecks execution authority, charges child CPU once, preserves WNOWAIT and returns the same result on replay; release after commit cannot imply rollback. Task/proxy/session cleanup marks records, and workers cancel claims and release owners outside peer/compute locks before slot reuse. The documented lock refinement permits only bounded local claim/commit under an already acquired peer lifecycle admission, with no compute/channel/RX spinlock, usercopy, send or remote wait. Normal/isolated builds, scoped formatting, four source suites, two host suites and full freshly packaged KTEST59836/0 pass. Actual lifecycle tests cover ACCEPT deferral, immutable requests, authority changes, competing claims, commit replay, exact accounting, release/retry, WNOWAIT, pre-BEGIN cancellation, busy cleanup and session retirement; all fixture references restore. Source/image hashes match; diagnostic2877480 is down with no matching WOS VMs at2026-09-16T18:53:39.762323+00:00. No wire/syscall caller or new live protocol qualification is enabled. Goal remains17 enforced/4pending, first signals; the four migrated-parent wait failures still require aggregate discovery/selectors, client fatal-exit cleanup, acknowledgments, multi-hop continuations and distributed accounting. Evidence: docs/goal19_child_wait_transactions.md and build/goal19-child-wait-transactions/validation.json.


## 9NY: signal authorization enforcement

The current [signal checklist](goal19_signal_authorization_audit.md) reconciles
ordinary kill, mapped/group delivery, exec admission, terminal/ptrace/lifecycle
producers and owned-family waits. It replaces stale missing-path assertions with
current source and recorded evidence, while retaining unqualified runtime
combinations and unexplained historical failures. Exactly signals is promoted;
18 domains are enforced and network-hostname, wki-identity and wki-resources
remain pending. Fresh model/source/policy checks pass. Prior9NT builds/KTEST and
9NX runtime receipts are reused explicitly; no new kernel or VM run is claimed.


## 9NZ: current IPv6 qualification and mapped NIC failures

[9NZ](goal19_network_ipv6_authority.md) adds156 passing local/mapped IPv6 and
socket authority checks plus fresh60 IPv4,24 queue-affinity and two-node hostname
regressions. Existing mapped NIC fixtures first fail their UP-proxy precondition
on the overlapping two-LAN topology. On the required sparse topology OPEN
reaches the network-grant profile and returns113; CLOSE includes a root110 and
network-grant107/restoration failure, then LINK lacks an UP proxy. Final owner
LAN/proxy states are DOWN despite connected WKI peers. Failures and diagnostics
are retained; no policy cause or fix is claimed. Both private clusters are synced
and stopped, Normal Build WOS and focused checks pass. Network/hostname stays
pending; status remains18 enforced/3 pending. Exact preparation/send/owner-reply
boundary capture on OPEN alone is next.

## 9OA/9OB network control and enforcement

The [network/hostname audit](goal19_net_control_retry.md) reconciles the original
contract with current authorization, publication and lifetime boundaries. A live
OPEN failure was traced to connected-peer lifecycle exclusion before reliable
acceptance. Bounded retained-identity send admission now covers requests and
completed owner replies without repeating driver effects. Both builds and the
full isolated KTEST pass60035/0;150 live NIC checks pass with restoration and
unchanged owner snapshots. The chosen child-wait collision semantics pass26
checks; a separate physical-node cleanup observation resolves the fixture's
unsupported remote-procfs probe. All owned VMs are down.

Only network-hostname moves to enforced:19enforced/2pending. Current WKI identity
and resource audits remain necessary; historical progress/timeout observations
remain preserved and are not all assigned the newly captured cause.


## 9OD — issued block access authority

Current resource reconnaissance found that BLOCK attach selected WRITE instead
of the union of requested READ/WRITE; bindings then discarded read permission.
The patched classifier requires every requested bit, including both for legacy
mode zero. Published bindings retain immutable read access through moves.
Scalar/ring READ and ring BULK_READ reject write-only grants before device/data
effects; read-only FLUSH returns EROFS in both paths. Existing writer arbitration,
malformed-input precedence, exact channel/zone lifetime, wire/syscall ABI,
credential semantics, declared LOCAL routing and waitpid collision rules remain.

Normal Build WOS and isolated diagnostic builds pass. Four host suites, seven
WKI source suites, policy audit and scoped formatting/tidy pass. Initial format
exit123 is preserved and corrected before final builds. Fresh full KTEST passes
60036/0, including actual scalar callback denials/positive controls and retained
ring READ/BULK_READ/FLUSH denials with zero bytes and unchanged buffers. It powers
off at484.516s; exact owned diagnostic process absence is recorded.

Fresh patched normal-kernel two-node functional checks pass124IPC assertions,
108main-thread and108worker-thread resource assertions. Both root-path readiness
and strict remote execution pass. The earlier unchanged9OB-kernel baseline is
recorded separately and is not substituted for patch validation. Fixture files
are absent on both physical nodes; guests synced/stopped. Final host audit at
2026-09-17T02:19:26.345957UTC finds no WOSVM. No live block/RDMA reset, packet
alteration, guest-memory writes or penetration testing is claimed.

Source and kernel hashes plus all command receipts are in
`build/goal19-wki-resource-audit/validation.json`; contract and evidence are in
`docs/goal19_block_binding_authority.md`. The separate9OC stale remote-root
EINVAL/retirementEBUSY observation remains unfixed. Its source review identifies
resource invalidation and auto-mount publication on separate workers; blocking
mount work must not move into the protocol worker and existing busy-mount
protections must remain. Current source reconciliation of the remaining issued
resource grants and this publication/retirement failure comes next.

Status remains **20 enforced / 1 pending**, with wki-resources and the goal active.
Original fenced goal command hash remains unchanged. This is verified progress,
not an indispensable runtime blocker or full completion.


## 9OE — current resource authorization enforcement

Reconciled the remaining resource manifest row with current explicit mapped
identity, issued VFS modes/readonly views, per-channel serialization, exact
binding drains, IPC consumer/import/channel/cookie contracts, DEVFS/MOUNT source
admission, block rights and retained zone grants. The old accumulated decision
contained stale pending claims superseded by the current VFS/signal/network/
identity domain qualifications; its exact text is preserved under
`build/goal19-resource-enforcement/before/`. The current checklist is
`docs/goal19_resource_enforcement.md`.

Only wki-resources is promoted: **21 enforced / 0 pending domains**. The complete
manifest/source gate now passes with zero documented legacy root checks. This
is domain enforcement evidence; the full goal remains active until a final
requirement-by-requirement audit of the original nine-item contract.

No production changes, new kernel build or new KTEST run in9OE. Current source
and both kernel hashes match the recorded9OD build. Its successful Normal Build
WOS/isolated diagnostic builds and60036/0KTEST are explicitly reused. Fresh7host
suites,11source/policy suites and all66native credential subprocess groups pass,
with no native stderr. Fresh two-node current-kernel service checks pass206
identity/ownership/no-regain assertions,48device assertions, and504descriptor
truncate-grant assertions across local/remote TMPFS/XFS. Fixture helpers are
absent on both physical nodes; guests synced/stopped, and final host receipt at
2026-09-17T02:31:24.921011UTC records no WOSVM. Evidence and exact commands:
`build/goal19-resource-enforcement/validation.json`.

The9OC stale-root EINVAL/EBUSY remains unfixed. Source review and current actual
native tests independently qualify owner export drain/cancel/commit and exact
backing retirement; the failure concerns automatic mount recovery and is not
relabeled a passing readiness test. Primary ivshmem raw callbacks remain outside
an authenticated RDMA-record guarantee; Goal25 owns transport replay/epoch/rkey
revocation. No universal wire-ID nonreuse, raw transport isolation or live block/
RDMA reset claim is made. Zone operations still require configured peer grants
and retained device rights. No packet alteration, guest-memory writes or
penetration testing was performed.

Original goal string, UID/GID ABI, declared LOCAL root routing, and positive-PID
collision EOVERFLOW/no-reap semantics remain unchanged. This turn is progress;
there is no indispensable runtime blocker. Next perform the full nine-item goal
completion audit, particularly the named converted-service denial surfaces,
using current source and actual fixture coverage rather than inventory status.


## 9OF — completed: original nine-requirement verification

**Status: Completed.** `docs/goal19_final_verification.md` reviews all nine
original requirements against current source and actual evidence. All21authority
manifest domains are enforced; the complete gate passes with zero legacy root
exceptions. The original fenced goal command remains byte-for-byte unchanged,
including its verified hash. The authorized --no-setup live workflow is retained.

Added reusable ordinary functional service_role_denials.cpp/.py fixtures to
close exact-profile coverage for UIDs100–104, including keygen103 omitted from
the earlier device matrix. All210qualified assertions pass across source-local,
receiver-local and remote execution. A root:root0600 dummy file under/root/.ssh,
an owned unrelated root sibling, power preparation/control, DEBUG and kernel
status exercise actual denials; journal grant and root prerequisites are positive
controls. Root/grant/ceiling restoration refuses and complete profiles remain
unchanged. Targets are reaped and files/helpers removed on both physical nodes.
Actual SSH worker104 has NNP1/ceiling0/sealed-empty grants and empty groups; its
root broker has exact ancestry and a separate SID/PGID. No private key material
was read or printed. Keygen generation itself was not rerun on the existing-key
image; current parser/drop/state source and prior qualified boots remain evidence.

No production changes in9OF. Fixture compilation and scoped format/tidy pass;
seven init/rootfs/policy source suites and final complete gate pass. Initial host
fixture failures (wrong broker helper source extension, exclusive SFTP write mode,
and WOS timestamp-unit parsing) are preserved and corrected. The qualified role
run additionally asserts key-file ownership/mode before operations. Earlier
successful observations are not double-counted.

9OD Normal Build WOS and isolated diagnostic builds plus fullKTEST60036/0 are
explicitly reused with matching kernel hashes.9OE's7host/11source suites,
66native credential groups,206service identity/state/no-regain,48device and504
local/remote descriptor assertions remain current evidence. Both final guests
synced/stopped; host receipt2026-09-17T02:45:57.648822UTC records no WOSVM. Evidence,
source/artifact hashes and exact commands: build/goal19-final-audit/validation.json.

Preserved limitations:9OC stale-root EINVAL/EBUSY automatic recovery remains
unfixed; historical9FZ/9LG/9LZ progress failures remain unexplained. Current owner
backing drain/cancel/commit and exact retirement separately establish the required
authorization boundaries. Raw RDMA record authentication/replay/rkey revocation
remain Goal25 scope; primary ivshmem is not claimed cryptographically isolated.
No broad recovery, universal wire-ID nonreuse or every-interleaving guarantee is
claimed. These limits do not change the original credential/service-authorization
scope or suppress observed failures. No PAM/directory service/containers/full
Linux capability ABI were added. UID/GID ABI, declared LOCAL routing and chosen
EOVERFLOW/no-reap positive-PID collision behavior remain preserved.
