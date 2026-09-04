# Goal 23: Reproducible, Offline, Attestable WOS Builds

## Planning metadata

- **Target horizon:** one to two continuous weeks with source-lock, toolchain/rootfs, deterministic-image, and provenance workstreams.
- **Primary surfaces:** root CMake, toolchain/bootstrap scripts, sysroots, ports, initramfs/disk packaging, manifests, KTEST isolation, and CI/test tooling.
- **Completion rule:** two clean offline builds from one lock must match for every scoped artifact or leave no unexplained classified delta.

## Copy-pastable goal command

```text
/goal Outcome: Make a pinned WOS source/toolchain set build without network access and produce reproducibly verifiable kernel, userspace, sysroot, initramfs, and normalized disk contents with machine-readable provenance and an SBOM. | Verification surface: schema tests, content-hash validation, a network-disabled build, two independent clean-root builds, artifact/semantic-image comparison reports, Normal Build WOS, KTEST isolation checks, and provenance/SBOM validation. | Constraints: Preserve normal developer Build WOS behavior, pinned submodule authority, KTEST's isolated roots, private-key secrecy, and semantic separation between qcow container metadata and filesystem payload; never substitute branch tips silently. | Boundaries: Add a source/toolchain lock, verified distfile store, offline mode, isolated build roots, deterministic generators, comparison tooling, provenance, SBOM, and CI gates; do not vendor entire upstream histories or redesign the kernel. | Iteration policy: Inventory every network/nondeterministic input, lock and verify sources first, establish offline builds, remove unexplained artifact deltas one class at a time, then emit and validate provenance. | Blocked stop condition: Stop only after the same indispensable external tool/build blocker repeats for three goal turns, all safe isolation/comparison alternatives are exhausted, and the exact unresolved delta or required input is recorded. | Runtime capability: I am able to run and debug WOS live without root access by appending `--no-setup` to the `wos-ktest` and `wos-cluster` scripts.
```

## Local source evidence

The build already has useful pieces: pinned gitlinks, SHA-256 checks for many ports, sorted/mtime-normalized initramfs generation, rootfs payload manifests, and a preseed verification script. However, `verify_preseeded_artifacts.sh` primarily checks presence, toolchain helpers retain clone/update fallbacks, and the repository has no single source lock, verified offline store, repository-wide SBOM/provenance, or two-clean-root reproducibility gate. Shared source-tree sysroot/build directories also complicate isolation.

## Measurable completion contract

1. A versioned lock enumerates every submodule, archive, patch set, generated tool, compiler/runtime input, expected digest, and license/source identity.
2. Fetch/populate is separate from build; offline mode performs no network access and rejects missing or mismatched content.
3. Every preseeded artifact is content-verified, not accepted solely because a path or stamp exists.
4. Clean builds can use isolated sysroot, port, tool, module, image, and output roots without mutating the normal tree or KTEST roots.
5. Timestamps, locale, path embedding, archive order, generated configs, build IDs, initramfs, and rootfs payloads are deterministic or explicitly normalized.
6. Comparison tooling distinguishes byte equality, ELF-semantic equality, filesystem-tree equality, and expected qcow allocation metadata.
7. Machine-readable provenance records lock digest, commands/options, tool hashes, artifact hashes, and environment fields without secrets or private-key content.
8. An SPDX- or CycloneDX-compatible SBOM covers shipped kernel, userspace, libraries, ports, licenses, and source identities.
9. Two independent network-disabled clean-root builds have no unexplained differences across the declared scope.

## Invariants, boundaries, and security

Normal `Build WOS` remains the default fast/safe profile. KTEST stays in `build-ktest/` and `ktest-data/`. Source locks must honor gitlink commits and fail closed on drift. Provenance must never copy SSH private keys, authorized-key contents, credentials, host secrets, or unbounded environment data.

## Parallel workstreams

The primary agent owns the lock schema, scope, and final comparison. Subagent A handles fetch/store/offline verification. Subagent B isolates toolchain/sysroot/port roots. Subagent C normalizes initramfs/rootfs/images and comparisons. Subagent D may build SBOM/provenance and validators on disjoint files.

## Verification, iteration, and rollback

Run script syntax/unit tests, CMake configuration tests, one Normal Build WOS, an isolated KTEST build-only check, a network-disabled build, and two-root comparison. Keep offline/attestation opt-in until the lock is complete, but do not retain silent unlocked fallback in strict mode. Use the `/goal` blocked condition exactly.

## Completion evidence

Completed on 2026-09-04.

- The final source lock digest is `7e61010ef33f8f74c7294cbb7bfc84bed55c631a5a6f4275d69169248db4023a`; schema, workspace, verified-store, preseed-content, and tamper tests pass.
- Two independent strict builds used separate `/tmp/wos-goal23-{a,b}/{build,state}` roots and ran all source-consuming configure/compile work inside a user and network namespace. Libguestfs image construction was completed rootlessly outside that namespace because its appliance cannot mount inside a user namespace; strict mode remained active and no source fetch path was available.
- Kernel, declared userspace artifacts, initramfs, and boot qcow2 are byte-identical. The sysroot canonical tree digest is `431532c71ea0ceaa01ddc1102f0206ae734ae34c225cc97f4c22d2f91601ded9`. The rootfs canonical tree digest is `b8407a5b82e98551a754aff9005a62b4a7b7ce2163991c8d6ec192ad06fcec72`, with zero unexplained differences and only three explicitly classified qcow allocation-size fields.
- Validated provenance covers 22 artifacts and records the final lock digest without workspace/store paths or secret material. The validated SPDX 2.3 SBOM contains 30 packages, 9,511 files, and 9,541 relationships.
- Normal `Build WOS` and isolated `bin/wos-ktest --build-only` completed. A rootless `bin/wos-ktest --config configs/node_ktest_rootless.json --no-build --no-setup` live run booted under KVM, published `ktest-data/live-debug.json`, and executed passing kernel selftests before bounded clean shutdown.
- All goal-owned regression, syntax, JSON, comparison, provenance, and SBOM checks pass. The repository-wide host suite passes 179/180 tests; the sole failure is the pre-existing, untouched `wosdbg_interface_source_test` contract mismatch for `DebugAnalysisService::invoke_tool`.
