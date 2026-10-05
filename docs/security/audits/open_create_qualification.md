# Goal 19 open/create qualification — 9MK

The `vfs-open-create` authority domain is qualified for the supported local and
mapped filesystem operations. This audit changes its manifest status to enforced.
It changes no kernel code, wire format, syscall ABI, locks, declared VFS routing,
service identity, or permission grant. The remaining domains retain their own
qualification requirements.

The decision is based on current source, the complete diagnostic run and private
cluster runs performed for 9MJ in this task, and the central credential/lookup
host suites run for this audit. It is not based solely on the absence of raw UID
checks. The earlier pending decision predates the completed traversal, creation
mask, private storage, owner publication, response admission and cancellation
work. Those prerequisites now have specific source and runtime evidence.

## Requirement map

| Requirement | Current source and admission point | Qualifying evidence |
|---|---|---|
| One stable caller identity | `sys_vfs.cpp` copies OPEN/OPENAT input. `core.cpp::vfs_open` routes process callers through `open_at_for_task`, which captures `REQUEST_CREDENTIALS` once and supplies it through retained lookup, retries and backend consumption. Spawn OPEN invokes the same API with the unpublished child. Explicit mapped/native helpers copy the supplied Snapshot, without adopting worker credentials. | Current credential and lookup host suites; `Credentials/ExplicitOwnerOpenCreatesAndTruncatesOnlyWithAdmittedAuthority`, `ExplicitOwnerXfsOpenCompletesTruncationAfterAuthorization`, `VFS/OpenatAtFdcwdUsesSuppliedTask`; current remote profile checks. |
| Creation identity and mask | `open_at_for_task` samples the canonical process `creation_mask()` once for O_CREAT. `prepare_file_create` checks parent write/search before producing effective UID and effective/inherited GID, masks mode bits and handles setgid membership. TMPFS/XFS install that identity before publication. | Exhaustive host permission/mode/group matrix and `FileCreationUsesEffectiveIdentityAndSetgidParent`; current `CreationMaskSharesRetainedProcessOwner` and `ReplacedExecWorkerCannotChangeCreationMask`; remote create metadata checks. The seven 9ME live mask checks are separately identified historical evidence. |
| Refusal before native mutation | TMPFS validates retained identity and parent/final policy under `tmpfs_lock`, then allocates its File before dentry/truncation work. XFS validates under `XfsMetadataGuard`, calls central create/open policy before transaction/publication and prepares File storage. Final truncation acts on the admitted retained File, not a new caller/path lookup. | Current `TmpfsOpenDenialPrecedesCreationAndTruncation`, explicit-owner TMPFS/XFS cases, and host source order checks. Failed authorization preserves bytes, size, timestamps, namespace generation and reference counts as asserted by those cases. |
| FAT policy | FAT's sleeping mount lock covers lookup and its declared immutable root ownership/mode projection. Readonly media refuse mutations first; central parent/final permission checks precede creation or File publication. FAT does not claim native Unix ownership persistence. | Current `FatOpenDenialPrecedesDeviceWrites`: denied requests leave directory bytes unchanged and perform zero device writes; allowed read/create controls pass. Source checks also require wrapper allocation before FAT creation writes. |
| DEVFS selected-object authority | DEVFS selects metadata/device under the appropriate namespace and metadata locks, authorizes the captured Snapshot, retains the selected node, and invokes its callback after releasing local/WKI namespace locks. Failure releases exactly those references. Credential-aware callbacks receive the admitted identity. | Current DEVFS local, PTS and peer open tests; selected-device callback/reentry tests; `JournalOpenKeepsDevfsAdmissionSnapshot`; the enforced device/procfs domain retains its separate control-operation evidence. |
| PROCFS selected-task authority | `open_procfs_lookup_as` reserves output, prepares the retained selected object outside namespace publication, and revalidates the selected task's credential generation before publishing. Private FD reopen retains the exact existing File grant. PROCFS does not create or truncate a backing inode. | Current `ProcfsOpenNoFollowUsesSelectedMetadataWithoutWorkerIdentity`, `ProcfsOpenRetainedPrivateMetadataRevalidatesBeforePublication`, and `ProcfsOpenFdRetainsExactFileWithoutChangingItsGrant`. |
| Mount and descriptor lifetime | `FdReservation` reserves a bounded fixed-table slot before backend effects and publishes with its exact owner. Its executor list supports task-exit cancellation. `MountOpenFileReservation` pins the admitted mount before effects, intersects its policy and transfers that pin without allocation. Final File close releases the mount after the backend. | Current `VFS/OpenFdInstallFailureClosesFile`, retained readonly/mount-policy and directory-view cases; current lookup/mount source suites; current OPEN_AT descriptor-limit and retained-directory live checks. |
| Mapped peer identity and confined target | WKI's admitted peer policy maps the canonical record before owner File work. Scalar/read and OPEN_AT invoke explicit confined native APIs with the mapped Snapshot. Unsupported records/features/selectors refuse before mutation, and legacy callerless OPEN is refused. Mapped device/PROCFS opens remain unsupported; they do not borrow worker authority. | Current WKI auth-policy host suite, kernel mapping/descriptor-grant tests, explicit-owner native tests, and 426 current scalar/OPEN_AT checks over TMPFS/XFS. Unsupported selector evidence from 9BM remains historical, with refusal wiring still checked in source. |
| Remote preparation and unknown replies | 9MF–9MJ reserve private client/owner storage before mutation, bind cancellation to the exact owner boot/binding/token, assign tokens in operation-slot order, validate complete responses before copying and retain cleanup across task exit/final close. No mutating replay or numeric-FD cleanup follows an invalid reply. | Current owner/client/reply-handler KTESTs, current source and framing/auth suites, 48 distinct-payload exit-flush cycles and 2,200 readonly open/close cycles. |
| Declared routing and retained directory policy | OPEN uses the admitted lookup and directory view. Native/remote readonly policy remains with the retained object. Explicit LOCAL `/` continues to select the receiver root as requested; this audit does not replace routing with the source's root text. | Current native policy, mapped absolute-symlink namespace and retained-view KTESTs; retained-directory live OPEN_AT cases. Traversal was separately qualified in 9MD. |

## Verification provenance

The unchanged current normal kernel is
`308c35f4db6dd0d98d300e73e1a96c7cd3d1e1af`; the unchanged diagnostic kernel is
`588e42faa3515a325932e0af168575ddcb225fb2`. Normal Build WOS and the isolated
build were executed successfully in this task. The full isolated KTEST passed
59,788 checks with zero failures, powered off, and returned launcher status 0.
The private two-node cluster passed 426 existing remote-open cases, 48 child-exit
flush cycles with distinct data and 2,200 readonly open/close cycles; cleanup,
sync, exact boot-chain verification and shutdown all succeeded. These are the
actual 9MJ receipts, not newly repeated runs or results from an older binary.
All three VMs were confirmed absent at `2026-09-16T07:02:58.347168+00:00`.

This audit additionally builds and runs `credential_model_test` and
`vfs_lookup_model_test`, and runs the lookup/mount source suites and final policy
checks. It records the exact source hashes, selected current KTEST PASS lines,
referenced receipt hash and status transition in
`build/goal19-open-create-qualification/validation.json`.
No additional kernel build or VM run is needed for this evidence/manifest change.

## Scope of enforcement

Enforced means that the supported open/create/truncate admission paths use the
central stable-identity policy and refuse unauthorized operations before their
backend effects. It does not promise rollback of an authorized XFS transaction
that later encounters an I/O/cache error, or of a remote mutation whose response
is lost. Those are accepted-operation failure semantics; the client must not
replay an ambiguous mutation. Actual cancellation and reply-handler tests cover
private kernel states; no packet campaign was performed or required for this
status decision. Historical availability timeouts remain recorded, without an
unsupported claim that 9MJ fixed their cause.

FAT's declared synthetic ownership, unsupported mapped synthetic filesystems,
and inherited procfd grants are explicit existing contracts, not implicit worker
privilege. Full metadata/mutation/mount, signal, network/hostname and WKI control
qualification remains separate. The overall goal is still active.
