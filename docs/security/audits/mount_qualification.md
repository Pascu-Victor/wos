# Goal19 9MP: mount qualification review

Mount remains pending. The current source review and fresh private two-node
fixtures support the contracts below, but a nosuid worker-exec failure requires
follow-up. No domain status is changed by this stage.

| Surface | Reviewed contract |
| --- | --- |
| Public mount | One admitted Snapshot authorizes MOUNT before lookup, probing or attachment. An existing retained target is revalidated at publication with the same Task, credentials and physical path. Missing targets are not created. |
| Source and target identity | DEVFS sources bind the admitted node and local provider or exact remote observation before probing. TMPFS/XFS targets retain native covered objects; PROCFS retains its metadata File; FAT reads the admitted context; DEVFS serializes selected-node validation with publication. PARTUUID remains an explicit privileged provider selector. |
| Private preparation | Pivot epoch, occupied-path reservations, retiring ancestors and backend initialization precede table publication. Readonly and nosuid flags are immutable mount policy. Writable block mounts retain their provider lease. Failure destroys private state outside locks. 9MO prevents exact mount ID reuse. |
| Unmount | Public MOUNT admission and copied target select an exact ID. Native exports drain before removal; busy refusal cancels retirement without replacing issued bindings. Commit withdraws only matching backing IDs. Remote groups hold a lifecycle pin through row removal and release Files outside registry locks. |
| Pivot | Bounded copied paths, prepared export drain, odd pivot epoch, preallocated paths/covers and table generation fence remapping. Exact native covers and collision/busy checks precede publication. Root updates and WKI rebasing preserve declared routing; releases and network reconciliation happen outside namespace publication. |
| Automatic mounts | Pending entries validate authenticated live resource generation and bounded paths. Retired ancestors defer successors without spending their retry budget. A bounded worker retries exact private-owner removal outside the registry lock; EBUSY preserves the anchor. Construction failure retires only its own group. |
| Inherited policy | Retained Files and directory views carry readonly/nosuid policy through ordinary operations and executable selection. Explicit LOCAL `/` continues to select the receiver root. Unsupported context transfers retain their refusal contract. |

The current 9MO kernels passed Normal Build WOS, the isolated build and the full
59,791/0 KTEST. Its mount admission, source/target replacement, nosuid retention,
descendant/cover, pivot, exact retirement and export cancellation cases passed.
These receipts are explicitly reused here, not represented as a second run.

Fresh two-node fixtures passed 230 checks across source (28), DEVFS target (20),
PROCFS target (20), descendants (19), mixed cover (24), native export unmount
(20) and readonly profiles (99), plus their lifecycle controls. The local nosuid
matrix passed 71 of 72 cases: `nosuid=0 nnp=1 bits=1 method=5` exited 137.
Cleanup completed. An unchanged local repeat passed 72/72; the independent
remote matrix also passed 72/72. Neither passing run explains the original
failure. All four mount-reporting rows in the final local/remote runs passed.

Review found an exit-dispatch ordering gap: an internal sibling-retirement
SIGKILL wake may arrive after the first exit-request check and enter group-fatal
dispatch. Stage 9MQ addresses this source-backed race. There is no causal trace
proving it produced the 9MP live failure, and no such claim is made.

All fixtures cleaned up, both guests synced, private boot backing chains were
verified, and the two exact guest processes stopped. Commands, failed initial
setup attempts, logs and shutdown evidence are in
`build/goal19-mount-qualification/validation.json`. The initial wrong disk UUID
was corrected to the manifest's partition UUID before the source fixture ran;
nosuid compilation required the init include path and libc++ linkage.

Historical pending text about missing source/target binding, readonly forwarding
and retained cwd predates the current source and completed prerequisites. It
must be reconciled at final qualification, while preserving historical failures.
The 9LZ directory timeout remains unexplained. Goal19 remains active with all
21 domains: 16 enforced and 5 pending.
