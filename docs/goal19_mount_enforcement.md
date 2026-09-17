# Goal19 9MR: mount authorization qualification

The supported `mount` domain is now enforced. This completes the mount review
started in [9MP](goal19_mount_qualification.md), with the exact-identity correction
in [9MO](goal19_mount_identity.md) and retirement-dispatch correction in
[9MQ](goal19_signal_retirement.md). This stage changes the evidence/status record;
production code and runtime artifacts are unchanged.

| Requirement | Qualified contract |
| --- | --- |
| Public authority | Mount, unmount and pivot capture one immutable caller Snapshot and require central MOUNT authority before backend or namespace effects. Privilege replacement during preparation does not change an admitted decision. |
| Source selection | A DEVFS source binds its retained lookup, metadata and exact local provider or remote observation before probing/attachment. PARTUUID selection is explicitly privileged. Non-block, missing and non-directory cases preserve their refusal behavior. |
| Target selection | Publication consumes the admitted existing directory and physical path. Native targets retain the exact covered object; PROCFS, FAT and DEVFS use their supported metadata/namespace lifetime contracts. Missing targets are not created, and replacements cannot redirect an admitted mount. |
| Publication | Private backend preparation, bounded path reservation, pivot epoch and retiring-ancestor checks precede table publication. The existing mount lock assigns nonzero IDs without reuse within a boot; exhaustion refuses before publication. Denied publication preserves table generation and covered-object ownership. |
| Mount policy | Readonly and nosuid are immutable mount properties retained by Files and directory views. Native and remote mutation dispatch respects readonly; executable selection carries nosuid through main, worker and spawn transitions. Accepted I/O errors do not imply rollback of completed I/O. |
| Native unmount and pivot | Exact mount IDs, descendant checks and retained references protect teardown. Export admission drains outside namespace locks; busy failure preserves bindings, successful removal retires only matching backing exports. Pivot prepares paths/covers before atomic remapping and reconciles exports after unlocking. |
| Remote and automatic lifetime | Construction failure retires only its candidate group. Manual teardown retains the exact proxy owner through row removal. Bounded deferred cleanup preserves busy anchors; retired ancestors defer successors until old children drain. Resource generation and authenticated peer policy constrain automatic publication. |
| Routing and compatibility | Explicit LOCAL `/` continues to select the receiver root. Existing errno, supported backend scope, retained cwd/root authority and unsupported-transfer refusals remain. General WKI identity/control and signal qualification remain separate. |

Current evidence uses the unchanged 9MQ normal kernel
`284ff917a727b7cb0b6765632d713292748a2451` and diagnostic kernel
`edc54694667d7a49c6ff9d6679333dc49adae12e`. Their Normal Build WOS and isolated
build pass; full KTEST passes **59,792 / 0**, including the mount admission,
binding, policy, cover, pivot, identity exhaustion and retirement cases. Five
host models, focused source/policy checks and formatting/tidy also pass.

The fresh 9MQ private two-node run passes **734 functional checks**: 230 mount
and readonly checks, 144 local/remote nosuid cases, and 360 additional local
nosuid cases. Mount reporting and retirement controls also pass. All fixtures
clean up, guests sync, private boot chains match, and all owned VMs are down.
These are explicitly reused current-task receipts; no additional build or VM
run is claimed for this status change.

The initial 9MP worker status 137 and historical 9LZ directory timeout remain
recorded. The source-backed retirement race is corrected and tested, but no
causal runtime trace links it to that particular worker failure. Passing repeats
do not erase failed runs or establish that every scheduling interleaving has
been tested. Those observations do not establish a remaining mount-authority
defect; broader signal/WKI progress review remains open.

Earlier manifest text describing absent target binding, readonly forwarding and
cwd policy was stale. The current contract replaces that active pending text;
the historical evidence remains intact. All 21 domains remain present, now
**17 enforced / 4 pending**, with `signals` first pending. Goal19 remains active.

Source/kernel continuity, selected actual KTEST records, policy gates and the
single-domain status transition are indexed in
`build/goal19-mount-enforcement/validation.json`.
