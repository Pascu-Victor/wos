# Goal19 final verification — 9OF

Goal19's original nine requirements are satisfied by the current implementation
and the evidence below. All21authority-manifest domains are enforced, and
`scripts/test/credential_policy_audit.py --require-complete` passes with zero
legacy root-check exceptions. The original goal command is unchanged.

This conclusion covers immutable process credentials, central authorization,
declared service identities and WKI process/resource mapping. The implementation
preserves existing UID/GID syscall numbers and saved-ID behavior, eight
supplementary groups, set-ID/no-new-privileges rules, errno and retained task/File
lifetimes. Declared VFS routing remains authoritative, including LOCAL `/` on a
receiver. Positive owned-child PID collisions return EOVERFLOW without reaping;
waitpid(-1) remains usable.

## Requirement-by-requirement evidence

| Original requirement | Current implementation | Evidence and conclusion |
| --- | --- | --- |
| 1. Stable immutable snapshot | `platform/security/credentials.hpp` owns all six IDs, bounded groups, authority ceiling, restrictions and sealed service grants. `CredentialStore` copies/publishes whole values under a short IRQ-safe lock; readers own their copies. Task threads use their retained canonical credential owner. Generation exhaustion refuses publication. | Current compiled credential model tests, whole-snapshot concurrency and kernel credential/lifetime tests;9OE's66native credential groups. Verified. |
| 2. Atomic lifecycle and identity changes | Fork/clone capture one parent Snapshot and initialize private children. ID/group transitions prepare complete values and publish with UID population admission. Exec validates admitted credentials at publication; set-ID replacement quiesces siblings, preserves saved IDs and respects nosuid/NNP/ceiling. Remote exec admits original and resulting identities before private publication. | [Exec checklist](exec_authority_audit.md), current KTEST cutover/credential/race cases,9OE native transitions/thread/exec/restriction groups; prior actual local/remote main/worker/spawn qualification remains explicitly recorded in the checklist. Verified. |
| 3. Versioned authority inventory | `tests/security/credential_authority_v1.json` schema1 retains all21required domains, decisions and source links: filesystems, SHM/IPC, signals, ptrace, priority, power, init, procfs/devfs, network, WKI, accounts and ownership. | Current manifest validated for required/duplicate domains and source existence; the final resource audit supersedes stale accumulated pending descriptions without deleting domains. Verified. |
| 4. Static enforcement gate | `credential_policy_audit.py` checks raw effective-root comparisons outside the central policy and documented test/boot scope, including complete integer-zero tokens and common typed forms. | Final complete gate and its regression suite pass, with zero documented legacy exceptions. This is a syntactic regression guard; individual operation audits establish actual authorization. Verified for the specified gate. |
| 5. Consistent filesystem policy and denied-state preservation | Native and mapped lookups carry one Snapshot and retained ancestor/object/mount authority. XFS/TMPFS implement mutable DAC; DEVFS gates selected callbacks, PROCFS uses retained task/current observation policy, FAT enforces its declared fixed ownership/readonly projection. Issued descriptor data rights and current-caller metadata authority remain distinct. Denial precedes backend mutation/publication. | Current [traversal](traversal_qualification.md), [open/create](open_create_qualification.md), [mutation](mutation_qualification.md), [metadata](metadata_qualification.md), [mount](mount_enforcement.md) and [resource](resource_enforcement.md) checklists; full current KTEST and9OE504local/remote truncate-grant checks plus native filesystem groups. Unsupported backend operations refuse explicitly. Verified. |
| 6. Fail-closed service drop | Init resolves every identity before launch. `apply_service_identity` applies NNP/sealed grants, supplementary groups, GID, UID, umask and ceiling, then verifies all fields. `services.cpp` reports/exits on any failed stage before exec. | Current init-supervisor compiled tests cover setup failure boundaries; fresh init source checks confirm ordering and nonreturning child failure.9OE observes actual journald/netd/httpd credentials, and9OF verifies exact role no-regain behavior. Verified. |
| 7. Deterministic accounts/ownership and invalid-policy refusal | Rootfs generation declares IDs100–104 deterministically and uses numeric managed-path ownership in full and incremental installs. Init bounds/parses root-owned configuration, rejects duplicate/missing identities and malformed authority/mask/group policy before service launch, and prepares declared private state. | Current rootfs/source/directory-policy and init-supervisor tests;9OE206service identity/state/no-regain assertions. Historical9GP's six rejected policy/account boots are retained, not described as new9OF boots. Current parser/drop code and tests retain those refusal contracts. Verified. |
| 8. Explicit WKI trust mapping | Canonical complete snapshots are mapped under configured operation and identity policy. Missing identity policy defaults to deny; nonroot trust rejects any root ID/group and active service authority. Deferred work owns copied policy/session/channel identities. Existing resources use issued grants, without worker-root substitution. | [Identity audit](wki_identity_audit.md):18admission and36complete-snapshot ordinary/fragmented cases; [resource audit](resource_enforcement.md), current native mapping/publication tests and9OD124IPC checks.9OF additionally runs exact role denials after ordinary remote execution. Verified within the declared process/resource boundary. |
| 9. Converted service isolation | HTTPD100, journald101, netd102, keygen103 and SSH worker104 have permanent nonroot IDs and declared bounded grants. The SSH worker drops/verifies before client-input processing and retains public host keys only. Its privileged broker holds signing/authenticated-session responsibilities in a separate SID/PGID and closes its network descriptors. | New9OF fixture:210checks over the five exact profiles on both physical nodes and remote execution. Root-only SSH-directory file access, unrelated owned-root ptrace, power preparation/control, DEBUG and kernel diagnostics refuse; journal grant positive control succeeds. Ceiling/grant/root restoration refuses, and profiles remain unchanged. Actual live SSH worker104 and root broker credentials/ancestry/session are independently observed.9OE observes actual long-running service profiles/state. Verified. |

The root Dropbear listener and isolated broker intentionally retain their
explicit privileged signing/authentication/session-launch role; the network
worker does not. Disabled testd is not a converted running service. Keygen103
may access its own declared host-key state, while root-owned SSH state remains
inaccessible. The9OF key fixture contains ordinary text in a root:root0600 file
under `/root/.ssh`; no private key material is read or printed. Key generation
itself was not rerun in9OF because the image already contains its configured key.

SHM's current handlers capture one Snapshot and call `may_access_ipc` or
`may_control_ipc` under the existing segment selection contract.9OE native groups9
and10 cover owner/group/foreign lookup/attach/control plus readonly mapping
upgrades and unchanged denied ranges. The manifest now links that evidence
explicitly rather than relying only on its short enforced decision.

## Final checks and provenance

- **Normal Build WOS and isolated diagnostic build:**9OD passed. No production
  code changed in9OE/9OF; current kernel hashes match those artifacts.
- **Isolated KTEST:**9OD's freshly packaged run passed60,036/0 and powered off.
  It is explicitly reused, not presented as a new9OF run.
- **9OE:**7host and11source/policy suites, all66native credential subprocess
  groups,206service identity/state/transition assertions,48device assertions,
  and504local/remote descriptor assertions passed.
- **9OF:** the new fixture compiled with the repository's WOS userspace flags;
  scoped formatting/tidy passed.210qualified role assertions and live SSH
  worker/broker inspection passed. Seven init/rootfs/policy source suites and
  the complete authority gate passed.
- Both final guests synchronized and stopped. Root-only test files and helpers
  were removed on both physical nodes. Final host receipt at
  **2026-09-17T02:45:57.648822UTC** records no WOSVM.

Artifacts: `build/goal19-final-audit/validation.json`, with original command hash,
current source/artifact hashes, exact commands, requirement evidence and guest
ownership. The new reusable fixtures are
`tests/fixtures/service_role_denials.cpp` and `.py`; their compiler commands are
saved in `role-build-final-result.json`. Earlier stages retain their raw logs.

Initial9OF host preparation failures are preserved: the broker helper's source
extension was corrected from `.cpp` to `.c`; exclusive SFTP creation needed `wx`
to allow writing; SSH status parsing needed WOS's `us` suffix. These occurred
before the respective assertions, and none required production changes. The
first successful role run remains separate from the final run that additionally
asserts fixture ownership/mode. Passing totals count only that qualified run.

## Remaining independent limitations

The9OC four-node stale-root EINVAL/EBUSY automatic-mount recovery failure remains
unfixed. Current owner-side admission drain/cancel/commit and exact binding
retirement are independently qualified; this audit does not claim universal
mount recovery or explain historical9FZ/9LG/9LZ progress timeouts. Authorization
denial is transactional; an admitted I/O failure does not promise rollback of
already completed I/O.

Goal25 separately owns authenticated raw RDMA records, replay/epoch/rkey
revocation and transport isolation. Primary ivshmem raw callbacks remain outside
that guarantee; secure WKI withholds RoCE publication. The old WKI instruction
note implying RoCE initialization is stale relative to current source. Goal19
qualifies process entrypoints, explicit peer grants and retained resource access,
without certifying raw transport security or universal wire-ID/cookie nonreuse.

These limitations are preserved in the requirement/domain evidence and do not
represent missing immutable-credential or service-authorization implementations.
No PAM, directory service, container system or general Linux capability ABI was
introduced. All live work used authorized ordinary functional OS tests with
`--no-setup`; no packet alteration or guest-memory writes were performed.
