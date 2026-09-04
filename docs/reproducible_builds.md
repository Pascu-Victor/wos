# Reproducible, offline, and attestable builds

The normal `Build WOS` workflow remains unchanged. The workflow below is an
opt-in release/CI profile that fails closed on source drift, missing source
objects, and unverified preseeded toolchain artifacts.

## Source authority

`configs/reproducibility/source-lock.v1.json` is the versioned authority for
pinned Git sources, release archives, patches, repository-owned build/image
inputs, source identities, licenses, and content digests. Validate it and
create a content-addressed store while network access is available:

```sh
python3 scripts/build/source_lock.py validate
python3 scripts/build/source_lock.py verify-workspace --workspace .
python3 scripts/build/source_lock.py populate --store /var/tmp/wos-source-store
python3 scripts/build/source_lock.py verify-store --store /var/tmp/wos-source-store
```

`populate` is the only source-lock command allowed to use the network. An
already initialized checkout and its cached archives can instead seed a store
without network access:

```sh
python3 scripts/build/source_lock.py seed-local \
  --store /var/tmp/wos-source-store --workspace .
```

Strict build helpers materialize only verified objects from that store. They
never fall back to a branch tip or URL.

## Isolated offline build

Each clean build gets a distinct state root. Seed immutable bootstrap outputs,
then create a portable content manifest. The manifest stores logical root
names and hashes rather than host paths.

```sh
mkdir -p /var/tmp/wos-a/state/sysroot /var/tmp/wos-a/state/busybox-install
cp -a toolchain/sysroot/. /var/tmp/wos-a/state/sysroot/
cp -a toolchain/busybox-install/. /var/tmp/wos-a/state/busybox-install/
python3 scripts/build/source_lock.py create-preseed-manifest \
  --manifest /var/tmp/wos-preseed.json \
  --root sysroot=/var/tmp/wos-a/state/sysroot \
  --root busybox-install=/var/tmp/wos-a/state/busybox-install
```

Configure with ambient toolchain flags cleared so they cannot reintroduce the
normal source-tree sysroot:

```sh
env -u CFLAGS -u CXXFLAGS -u LDFLAGS cmake -S . -B /var/tmp/wos-a/build -G Ninja \
  -DWOS_SOURCE_MODE=offline \
  -DWOS_SOURCE_STORE=/var/tmp/wos-source-store \
  -DWOS_PRESEEDED_ARTIFACT_MANIFEST=/var/tmp/wos-preseed.json \
  -DWOS_STATE_ROOT=/var/tmp/wos-a/state \
  -DWOS_REPRODUCIBLE_BUILD=ON \
  -DSOURCE_DATE_EPOCH=0 \
  -DWOS_ASSUME_BOOTSTRAPPED_TOOLCHAIN=ON \
  -DWOS_BUILD_CMAKE_FOR_HOST=OFF \
  -DWOS_BUILD_HOST_TOOLS=OFF
cmake --build /var/tmp/wos-a/build --target wos_full
```

Run the configure and build commands in the CI executor's network-disabled
sandbox as an independent enforcement layer. Repeat with `/var/tmp/wos-b`;
copy the same seeded content and reuse the portable preseed manifest.

Strict mode exports the source policy to every CMake custom command, enables
disconnected FetchContent behavior, maps workspace/build/state paths to stable
debug paths, normalizes rootfs input ordering and timestamps, excludes host
SSH keys and timezone state, and recreates images with fixed GPT, partition,
filesystem, and FAT identities.

## Comparison and attestations

Use `scripts/build/wos_artifacts.py compare` for byte, ELF-semantic, canonical
tree, or qcow2-aware comparison. A qcow2 result is complete only when both
read-only extracted filesystem trees are supplied. XFS may change qcow
allocation layout while preserving identical payloads; classify only the
observed allocation fields explicitly:

```sh
python3 scripts/build/wos_artifacts.py compare --kind qcow \
  --expect-qcow-allocation-delta \
  --expect-container-delta actual-size \
  --expect-container-delta 'children[0].info.actual-size' \
  --expect-container-delta 'children[0].info.virtual-size' \
  --left-tree /var/tmp/wos-a/rootfs-tree \
  --right-tree /var/tmp/wos-b/rootfs-tree \
  /var/tmp/wos-a/state/mountfs.qcow2 \
  /var/tmp/wos-b/state/mountfs.qcow2 \
  --output /var/tmp/wos-rootfs-comparison.json
python3 scripts/build/wos_artifacts.py validate comparison \
  /var/tmp/wos-rootfs-comparison.json
```

`wos_attest` depends on `wos_full` and source-lock validation. It emits a
path-redacted provenance statement and SPDX 2.3 SBOM, then validates both:

```sh
cmake --build /var/tmp/wos-a/build --target wos_attest
```

Outputs are written to `/var/tmp/wos-a/build/attestation/`. Only allowlisted
environment variables are recorded. Credentials, private keys, authorized-key
contents, and unbounded host environment data are excluded.

## Rootless live verification

After building and packaging, existing rootless VM topology can be reused
without setup privileges:

```sh
bin/wos-ktest --config configs/node_ktest_rootless.json \
  --no-build --no-package --no-setup
bin/wos-cluster --config configs/cluster_selfhost.json --launch --no-setup
```

Use the matching teardown/owner workflow for the topology that was originally
created. `--no-setup` intentionally does not create taps, bridges, or other
host networking state.
