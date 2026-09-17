# Goal19 9OE: WKI resource authorization audit

This is the current authorization checklist for `wki-resources`. It reconciles
that manifest row's accumulated historical pending statements with the current
source and the completed VFS, process, network and identity audits. The resource
authorization domain is enforced. Full Goal19 completion still requires the
nine-requirement completion audit, including the converted-service requirement.

No production code changes in9OE. The current kernel is the9OD block-permission
fix. Its successful Normal Build WOS, isolated diagnostic build and freshly
packaged KTEST **60,036 passed / 0 failed** are reused explicitly. Fresh normal
VMs in9OE use that kernel and private overlays over the9OD rootfs image.

## Required authority and evidence

| Surface | Current decision and lifetime | Evidence |
| --- | --- | --- |
| Process entry to devices and mounts | DEVFS authorizes its retained selected node with the supplied Snapshot before invoking a device callback. Public mount captures one Snapshot and requires MOUNT before source resolution. `devfs_resolve_block_source` requires the original resource observation/incarnation and matches an existing provider against that observation. Discovery names alone confer no process privilege. | [Mount audit](goal19_mount_enforcement.md), [open audit](goal19_open_create_qualification.md); current `WkiDevProxyReuse/ExistingProviderMatchesSelectedObservation` and DEVFS native cases; fresh48device checks. |
| Peer resource permission | `auth_policy::required_permissions` classifies resource attachment and operations separately from identity trust. BLOCK requires all requested access bits, including both for legacy mode zero. VFS operations introducing caller authority require mapped records; network controls require REMOTE_NET plus mapped identity; IPC and zones require their declared peer permission. Explicit peer block/zone delegation is a configured resource grant, not an arbitrary process-root identity. | Current auth-policy host suite, [identity audit](goal19_wki_identity_audit.md), [block fix](goal19_block_binding_authority.md). |
| New VFS authority | Canonical request identity is mapped using the RX-admitted policy/capabilities before confined native lookup or mutation. Path/component, retained-directory and current-caller metadata operations use their explicit Snapshot. Legacy callerless mutation/open selectors refuse. OPEN prefetch additionally requires a readable requested access mode before opening or staging data. | [Traversal](goal19_traversal_qualification.md), [open/create](goal19_open_create_qualification.md), [mutation](goal19_mutation_qualification.md), [metadata](goal19_metadata_qualification.md) current checklists; complete current KTEST and focused source gates. |
| Existing VFS data grants | `find_remote_fd` requires an active, nonretiring row and exact channel allocation/generation. Scalar/RDMA/bulk reads check the File's admitted access mode before data. `retain_server_write_file` checks write mode, offset and retained readonly policy before pinning/touching; writes use that File after unlocking. FSTAT reads the issued descriptor, including write-only and post-drop descriptors. Current-caller ownership mutation is a distinct mapped operation. | Current native `PolicyAdmissionAndRetirementKeepOneFile`, File-lifetime/readonly tests; fresh504local/remote TMPFS/XFS truncate-grant assertions, including denied-state preservation and allowed controls. |
| VFS operation and cleanup lifetime | Pre-ACK fixed admission retains the exact binding, copied request and captured policy. Deterministic worker selection serializes each peer/channel's operations. Detach, export reconciliation and stale-peer GC drain binding references before releasing server Files; GC holds peer lifecycle admission and rechecks disconnect state. Asynchronous registry mutations retain their own File references. Channel-bound OPEN cancellation and directory/exec tokens prevent cleanup from choosing a replacement object. | Current dev-server deferred-storage/detach/attach-failure tests, remote-VFS open/cancel/File-lifetime tests and11fresh source suites. |
| IPC issued descriptors | `ipc_export_matches_consumer` checks immutable resource ID, issued consumer and expected type under `s_ipc_lock`; successful acquisition pins the selected File. Import validation binds every home node to the authenticated submitter and rejects invalid/duplicate FD/resource/type entries before private installation. Existing grants survive credential drops. Legacy epoll/futex requests lacking an issued target grant refuse with ENOTSUP. | Current IPC native actual-registry cases and host/source suites;9OD's fresh10command/124assertion IPC matrix on the identical current kernel, with receiver retirement and physical-node cleanup. |
| IPC publication and replies | Export preparation snapshots FDs, reserves nonreused resource IDs before publication and binds original RESOURCE/DATA allocations plus cleanup epoch. Final submit matches every export against the admitted channel pair. Pumps, EOF, queued closes and cleanup send on those captured identities. The bounded unanswered-control table retains accepted uncertain cookie identities across timeout and skips them on wrap; valid late replies release capacity. | Current export/submit/import, retained-channel, cookie-wrap and response-envelope KTESTs;9OD's current-kernel IPC matrix and9OE source review. |
| Block binding authority | Published bindings retain independent read permission and write restriction through moves. Scalar/ring READ and ring BULK_READ reject write-only grants; WRITE/BULK_WRITE and scalar/ring FLUSH enforce the write restriction before callbacks. Existing writer leases and readonly downgrade preserve exclusive writer policy. Ring geometry and completion checks precede data effects. |9OD actual deferred-handler/ring native tests and block-policy host cases, rerun in the complete current KTEST; fresh7host suites in9OE. No live block-device workload is claimed here. |
| Zone backing authority | `WkiZoneRef` retains the exact allocation/backing after peer, requested local rights and original management-channel checks. Device server/proxy rings use that grant through work and retirement. Deferred creation owns request/channel identity and obtains setup lifecycle admission. Reliable notifications retain the original management allocation. Read/write message handlers check peer/channel, direction and bounds before copying. | Current `WkiDevProxyZone/RetainedGrantGatesIoAndCleanup`, `WkiZone/LocalBackingAndBlockRingRetainExactGrant`, deferred setup/notification/timeout cases and source gates. These are native ordinary-operation tests. |
| Network resources and process control | Current mapped network control rechecks NETWORK_ADMIN before driver admission; ordinary process signals map the captured caller, while wait/terminal/parent control uses its narrower retained relationship grant. No ambient worker-root fallback is introduced by resource dispatch. | [Network audit](goal19_net_control_retry.md), [signal audit](goal19_signal_authorization_audit.md), current native tests, and the separate current identity audit. |

## Locks and context

RX admission is bounded and owns copied input plus exact identities before ACK.
Blocking VFS/device operations run in worker context. Registry locks protect
selection, publication and retirement; backing callbacks, File releases and
binding drains occur outside those locks. An admitted Snapshot is a value owned
by the operation. Existing descriptor authority is held by retained objects,
without recapturing the worker's credentials. The audit changes no syscall ABI,
wire layout, declared LOCAL routing or selected waitpid collision semantics.

## Preserved failures and boundaries

The9OC four-node root path returned EINVAL while strict remote execution worked.
Serial evidence showed successor descendants mounted before old-root retirement
returned EBUSY. Resource invalidation and automatic mount publication run on
separate workers; the existing retired-ancestor guard does not make those steps
atomic. That availability defect is **unfixed**. The successful two-node readiness
in9OD/9OE does not explain it.

The authority conclusion relies on the owner-side contract: native unmount
prepares/drains export admission, cancels unchanged on busy denial, or retires the
exact backing's bindings on commit; server operations use mapped credentials or
issued File grants. Proxy retirement makes old groups unavailable and retains
busy mount ownership rather than selecting a replacement through a pathname.
The current `NativeUnmountCancelPreservesAndCommitRetiresExactBacking` and
`ManualRetirementReleasesExactGroupAfterBusyDenial` tests qualify those boundaries.
No broader automatic-recovery or timeout-completeness claim is made. The earlier
9FZ/9LG/9LZ progress observations likewise remain preserved in their records.

Goal25 owns authenticated RDMA records, transport replay/epoch protection,
rkey revocation and raw-memory isolation. Secure WKI withholds RoCE publication,
and raw RoCE RX checks actual initialization. Primary ivshmem RDMA still exists:
its raw callbacks use BAR offsets and do not themselves authenticate a peer.
Zone read/write helpers have no external production callers; the current zone
creation caller is device-server block setup. Goal19 verifies their process
entrypoints, configured resource grants and retained access checks. This audit
**does not certify raw RDMA authentication**, universal wire-ID/cookie nonreuse,
or live fast-path session/revocation behavior. Those properties cannot be inferred
from the current native tests or from a null dedicated RDMA transport.

Unsupported remote PROCFS/device opens, legacy callerless metadata and unsupported
IPC resource types keep their explicit refusal contracts. Their former pending
implementation notes do not denote an enabled worker-root fallback. Historical
manifest decisions are retained in the stage's `before/` copy.

## Fresh evidence and provenance

`build/goal19-resource-enforcement/validation.json` records source/kernel hashes,
commands and VM ownership. Fresh results:

- Seven host suites and11source/policy suites pass.
- Native `testprog credentials`: all66subprocess groups pass, with no stderr.
- Service status/ownership/no-regain:206assertions on two physical nodes.
- Device authority:48assertions across the six fixture profiles on both nodes.
- Descriptor truncate grants:504assertions over local/remote TMPFS/XFS.
- Fixture helpers are absent on both nodes; guests synced and exact owned QEMU
  processes stopped. Final host receipt2026-09-17T02:31:24.921011UTC records no WOSVM.

No fresh kernel compilation or KTEST execution is claimed in9OE. The immediately
preceding9OD build/KTEST artifacts remain authoritative for unchanged code.
All execution was ordinary functional OS testing with `--no-setup`.
