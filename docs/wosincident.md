# WOS incident bundles

A `.wosincident` captures the bounded host-side evidence needed to replay a
KTEST or cluster failure without copying a VM disk or a build tree. The public
collector is `bin/wos-incident`.

The version 1 envelope may be either a directory or a deterministic, uncompressed
USTAR archive. Both forms contain `manifest.json` at their root and only regular
file members. The manifest uses `format: "wosincident"`, `version: 1`, a
content-derived `incidentId`, source revision/profile metadata, sanitized
topology, explicit clock quality, capture errors and truncations, and a sorted
member inventory with exact sizes and SHA-256 digests.

## Capture after a launch

KTEST and cluster launches can capture their newly produced log ranges and
matching allowlisted binaries when the VMs exit or the launch fails:

```sh
bin/wos-ktest --no-setup \
  --incident-output test-results/failure.wosincident

bin/wos-cluster --launch --no-setup \
  --incident-output test-results/cluster-failure.wosincident \
  --incident-archive
```

`--no-setup` lets an agent run and debug WOS live without acquiring root access,
provided the bridges, TAPs, and shared-memory topology were configured earlier.
For `wos-cluster`, `--no-setup` is valid only with `--launch`.

Incident capture runs from a `finally` path. A capture error is reported as a
warning and never replaces the launcher’s original exit status. The output path
must not already exist. Before launch, the scripts snapshot log device, inode,
size, and modification time; after launch, an unchanged log contributes only its
new byte range, while an unlinked and recreated log is captured from byte zero.

Use `--incident-coverage-manifest PATH` to import a coverage
`run-all-iteration.json`. The source manifest remains unchanged. The collector
stores a sanitized copy, materializes its allowlisted artifacts, and copies the
recorded external log byte ranges.

## Standalone capture and live coredumps

Capture current KTEST or cluster artifacts directly with:

```sh
bin/wos-incident capture --kind ktest \
  --config configs/node_ktest.json \
  --output test-results/ktest.wosincident

bin/wos-incident capture --kind cluster \
  --config configs/cluster.json \
  --output test-results/cluster.wosincident \
  --archive
```

Current kernel coredumps are written to guest tmpfs as
`/tmp/*_coredump.bin`; they are not present in qcow2 overlays after shutdown.
While a guest is still running, fetch an exact target through the existing SFTP
transport:

```sh
bin/wos-incident capture --kind cluster \
  --config configs/cluster.json \
  --live-coredump wos-0:/tmp/testprog_123_coredump.bin \
  --output test-results/live-failure.wosincident
```

Live targets must name one exact `/tmp/*_coredump.bin` path. Wildcards and other
guest paths are rejected. Fetches have a timeout and an OS-enforced file-size
limit.

## Collection boundary

The collector uses fixed member-count, per-file, total-byte, and USTAR-path
limits. It rejects symlinks, devices, FIFOs, duplicate/traversing member names,
sources outside allowed roots, and concurrent source mutation. Oversized text
logs retain a bounded tail and are marked truncated; binaries and coredumps are
never truncated.

Configuration and imported manifests are sanitized, and common credential lines
and private-key blocks are redacted from text logs. The collector never includes
qcow2 disks, overlays, SSH host/user keys, authorized keys, arbitrary guest
directories, environment dumps, or recursive build trees. It does not execute or
upload captured content.

Clock metadata is deliberately conservative. Serial timestamps are described as
node-local boot-monotonic domains and are not declared comparable across nodes
without a real synchronization anchor.

## Version 1 manifest

The root object has this additive, versioned shape:

```text
format: "wosincident"
version: 1
incidentId: "sha256:<canonical identity digest>"
createdUtc
source {kind, revision, dirty, profile, configMember}
capture {complete, errors, redactions, truncatedMembers}
clocks {quality, domains[]}
topology {nodes[]}
members[]
```

Every member records `path`, `kind`, `size`, `sha256`, `required`, `truncated`,
and basename-only `sourceName`. Evidence may additionally record numeric
`nodeId`, `buildId`, a bundle-relative `binary` association, and `clockDomain`.
The member array and archive entries are path-sorted. `incidentId` hashes
canonical JSON for the manifest excluding only `incidentId` and `createdUtc`,
so the same captured evidence has the same identity regardless of its envelope
or capture wall-clock time.

## Validate and replay offline

The four incident operations live in WOSDBG's one shared backend catalog, so
the GUI Analysis Tools dock, JSON CLI, and MCP expose identical schemas:

- `validate_incident(path)` snapshots and validates a directory or archive.
- `load_incident(path)` creates a stateful incident session and loads bounded
  evidence.
- `get_incident_inventory(incidentId)` returns a bounded member page and
  explicit load/degradation states.
- `summarize_incident(incidentId)` returns bounded correlations, coredump
  checks, declared and observed clock quality, and a deterministic
  `semanticDigest`.

Use a CLI batch so the loaded session is retained:

```json
{
  "calls": [
    {"id": "load", "tool": "load_incident",
     "arguments": {"path": "test-results/failure.wosincident"}},
    {"id": "summary", "tool": "summarize_incident",
     "arguments": {"incidentId": "$load.incidentId", "maxEvents": 200}},
    {"id": "inventory", "tool": "get_incident_inventory",
     "arguments": {"incidentId": "$load.incidentId", "count": 200}}
  ]
}
```

Run it with `tools/build/bin/wosdbg --batch batch.json`. Repeating the batch on
the same bundle should produce the same normalized summary and
`semanticDigest`. Random snapshot paths and `createdUtc` are not semantic
inputs. A summary never upgrades node-local timestamps into a cross-node total
order: `clockQuality` preserves the manifest declaration, while
`timeline.clockQuality`, `globalOrderAvailable`, and empty
`clockOrderedEvents` describe what the evidence actually supports.

Unsupported coredump versions, corrupt/truncated coredumps, corrupt chunks,
truncated logs, missing mappings or symbols, build-ID mismatches, partial
clocks, and capture errors have distinct machine-readable issue codes. Valid
coredump versions 1 through 3 keep their existing parser compatibility.

## Redaction and sharing checklist

Collection redacts common credential-bearing text lines and private-key blocks
and omits disk images, overlays, SSH material, environment dumps, and recursive
build trees. Redaction is intentionally conservative rather than a proof that
arbitrary application output contains no sensitive data. Before sharing:

1. Inspect `capture.redactions`, `capture.errors`, and `truncatedMembers`.
2. Review `manifest.json`, the sanitized config, and every text-log member for
   project-specific secrets, customer data, paths, or payloads.
3. Validate the final directory/archive with `wosdbg.validate_incident` after
   any permitted regeneration; do not hand-edit members because checksums and
   the incident identity would no longer match.
4. Share the `.wosincident` through the intended secure channel. The collector
   never uploads automatically.

WOSDBG treats received bundles as hostile. The input must be inside an
effective configured `allowedRoots` directory. It rejects path escape,
symlinks, hard links, devices, sparse/encrypted entries, duplicate/conflicting
paths, unsupported containers, excessive paths/member counts, oversized
members or expansion, checksum failures, and incident-ID mismatches. Bundle
content is never executed, and offline replay does not silently substitute a
host binary for a missing bundle-local ELF.

## Regression and parser fuzzing

The generated corpus and CLI semantic test cover v1-v3, directory/archive
parity, single/distributed incidents, partial clocks, corruption, truncation,
missing symbols/mappings, build-ID mismatch, hostile archives, response bounds,
interface parity, and repeated semantic equality:

```sh
ctest --test-dir build/tools/wosdbg -R wosdbg_incident_cli_semantic_test --output-on-failure
```

For bounded parser fuzzing, configure the host tools with
`-DWOSDBG_BUILD_FUZZERS=ON`, build `wosdbg_coredump_parser_fuzz`, and run it with
libFuzzer `-runs=N -max_len=1048576`. The harness asserts that every successful
parse is v1-v3 and that all materialized segment/ELF ranges remain inside the
owned input buffer.
