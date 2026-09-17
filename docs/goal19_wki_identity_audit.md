# Goal19 9OC: WKI identity enforcement audit

The `wki-identity` domain is enforced for the immutable-credential contract.
Configured trust, complete credential transport, operation-specific mapping and
native authorization are separate requirements. Authentication alone supplies
none of the caller's local authority. The goal remains active at **20 enforced /
1 pending**; `wki-resources` still requires qualification.

This stage audits current source and runs ordinary functional checks. It changes
no kernel code, ABI, wire format, trust policy implementation or credential rule.
The private cluster uses the immediately preceding 9OB build and `--no-setup`.

## Current authorization path

| Surface | Required decision and ownership | Evidence |
| --- | --- | --- |
| Configuration | `configured_wki_peer_policies` validates all node/peer declarations before provisioning. Missing identity policy means `deny`; operation permissions do not imply identity trust. | Current configuration source; 29 cluster configuration tests; fresh four-node omitted/nonroot/root-inclusive policy matrix. |
| Canonical identity | The versioned 80-byte record owns six IDs, eight bounded supplementary groups, NNP, dumpability, service sealing, authority ceiling and grants. Decode validates framing, flags, reserved/unused fields and snapshot invariants before assigning output. | `credential_record.hpp`, credential model and wire host suites, current full-credential KTEST. |
| Mapping | Central `map_remote_identity` copies the entire snapshot only after trust admission. Nonroot trust rejects every root ID slot, supplementary group zero and active service grants. An irreversibly inert grant cannot become active during mapping. Operation adapters also require their specific compute/VFS/network permission and reject unknown policy bits. | `authorization.hpp`, `auth_policy.cpp`, host policy matrix, fresh 18 admission and 36 exact snapshot/no-regain cases. |
| Authenticated admission | RX retains peer lifecycle while admitting an exact channel allocation. Deferred compute work owns the complete session identity; VFS/network work owns copied policy, capabilities, request bytes and an exact retained binding before reliable acknowledgment. A worker cannot substitute its own root identity or adopt a replacement peer's policy. | `wki.cpp`, `dev_server.cpp`, `remote_compute.cpp`; `QueuedIdentityAndFragmentsKeepAdmittedSession` and source guards. |
| Remote exec | `handle_task_submit_work` validates context and maps original and resulting credentials before ELF fetch, process reservation or construction. Both image constructors receive the mapped snapshot. The private process installs that identity; final publication checks current sealed session/channel admission while holding peer lifecycle ownership. Failure destroys private state. | [Exec authority audit](goal19_exec_authority_audit.md); current `FullCredentialContextPreservesRestrictions`, `ExecHandoffRequiresPreparedIdentityAndCommit`, `FinalPublicationAdmissionAndPrivateCleanup` KTESTs; fresh small/9-MiB executables with ordinary/fragmented context. |
| Exec compatibility | Legacy six-ID transport is admitted only if decoding reproduces the entire original snapshot exactly. Unsupported groups/restrictions/grants fail `ENOTSUP`; optional placement stays local. Negotiated full records require agreement with duplicated legacy IDs. | Shared legacy decoder/admission, host model, current three legacy-exec KTESTs; historical 9KU version-refusal qualification. |
| VFS reads and metadata | OPEN/OPEN_READ, STAT/access, READLINK/REALPATH, directory-relative forms and metadata batches carry complete credentials. The owner maps read/open intent before calling explicit native consumers. Legacy callerless selectors and missing required versions are refused. | [Traversal](goal19_traversal_qualification.md), [open/create](goal19_open_create_qualification.md), [metadata](goal19_metadata_qualification.md), current remote-VFS/server source checks and KTEST. |
| VFS mutation | Create/truncate derive both read/write intent from flags. Namespace, ownership/mode/time, descriptor metadata and xattr BEGIN map the current caller before native authorization. Later staged work retains that admitted identity and File; existing data descriptors retain their issued grants. Ambiguous mutations are not replayed. | [Namespace mutation](goal19_mutation_qualification.md), [metadata](goal19_metadata_qualification.md), current policy/wire/server suites. |
| Directory transfer | Adoption binds source node/boot/submission and intended recipient, maps the complete supplied snapshot and claims the exact retained directory scope. It prepares private ownership; compute still controls publication. Declared VFS routing, including LOCAL `/`, remains authoritative. | `wki_remote_vfs_adopt_directory_grant_native`; current directory-grant/compute-filesystem KTESTs; traversal and exec qualifications. |
| Process control | Ordinary task/group signals map the supplied snapshot through captured compute policy, then authorize against a retained current target. Terminal, parent-death, SIGCHLD, cancellation and wait messages have narrower relationship/session grants; they cannot substitute an arbitrary caller identity. | [Signal audit](goal19_signal_authorization_audit.md), current control/session KTESTs. Positive owned-child PID collisions still return `EOVERFLOW` before either claim; wait-any remains usable. |
| NIC control | OPEN/CLOSE/LINK decode the complete record, map under captured REMOTE_NET identity policy, and call central network authorization before driver admission/effects. Bounded send retries preserve the admitted request. | [9OB network qualification](goal19_net_control_retry.md), current three native mapped-control KTESTs; no fresh NIC run in 9OC. |

IPC descriptors, device attachments and zone/backing grants are existing-resource
authority surfaces rather than numeric process-identity mapping. Their exact
owner, session, rights and retirement rules remain in `wki-resources`; this audit
does not treat authentication or an issued descriptor as permission to create a
new arbitrary local identity.

Policy operates on bounded value copies without allocation. Registry locks only
protect admission/retention and release before native filesystem, device or
scheduler work. Peer lifecycle ownership excludes policy/session replacement
during final compute publication; it is released across preparation and waits,
with subsequent revalidation. A supported old descriptor grant and a request for
new authority have distinct lifetime contracts.

## Fresh checks and preserved failures

Artifacts: `build/goal19-wki-identity-audit/validation.json` and command logs.

- Four guests, independent LAN access and authenticated WKI; receiver policies
  omit source identity, accept nonroot identity, or accept root-inclusive identity.
- 18 root admission cases and 36 complete snapshot/no-regain cases pass.
- Source and mapped receiver each pass 33 owned credential-transition checks
  and 70 service identity/state-ownership checks: 206 assertions total.
- 48 local device-authority assertions pass across source and receiver.
- Four freshly built host model/wire/policy suites pass, as do six focused WKI
  source suites, configuration tests, the policy audit and its unit tests.
- Fixture executables are absent on all four nodes. All guests sync and their
  exact private QEMU processes stop. Host verification at
  `2026-09-17T01:49:36.117799+00:00` finds no WOS VM.

No new Normal Build WOS or isolated KTEST run was needed for this documentation
and manifest audit. The immediately preceding 9OB builds passed, and its complete
isolated KTEST passed **60035/0**. This is explicitly reused evidence. Kernel SHA256:

```text
normal     b2d38ae690918ba5bf0a44f5bf550aa73579ec99e8276c01075283a504a91797
diagnostic 48a06cec6c07845d0916d896773da1950d8dbd8d928745206741f3fc78f0b32f
```

The original readiness check failed: source access to
`/wki/wos-3/usr/bin/uname` returned `EINVAL`. Direct receiver access and actual
strict remote execution succeeded. A stage-local driver changed only its
readiness check to strict remote `uname`; all 18 identity assertions are unchanged.
The failed readiness result and diagnostic commands are preserved. This is not a
passing remote-filesystem readiness result.

Source serial logs show successor descendants mounted before retirement of the
old root resource 4096 failed with `EBUSY`. The old root remains visible and the
successor root 4100 has not become the usable root mount. This narrows the resource
investigation; it is not a complete diagnosis or a production fix. The 9KI guard
checks an already-retired ancestor, so its historical successful run does not
establish correctness when successors precede retirement. Resource qualification
must cover that ordering. The observation shows failed access, not an observed
credential mapping bypass; it does not invalidate the passing compute matrix.

## Historical claims reconciled

The former manifest decision accumulated pending implementation claims now
superseded by the current traversal, mutation, metadata, exec and signal audits.
Its exact text is preserved in the stage's `before/` files; chronological evidence
is retained. Earlier interpreter/directory/worker progress failures remain
recorded and are not explained by subsequent successful tests. Atomic file
versioning and transport cryptography have separate contracts; this stage makes
no broader claim for them. Resource authority and its known mount-lifetime
failure remain open, so neither the complete gate nor the goal is marked done.
