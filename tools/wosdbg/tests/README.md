# WOSDBG incident assurance fixtures

This directory contains a deterministic, generated `.wosincident` corpus and
an end-to-end JSON CLI semantic/security test. Nothing under this directory is
a checked-in generated bundle: tests create compact fixtures in a disposable
directory so their bytes cannot drift independently of the generator.

## Run

Generate and locally verify the corpus:

```sh
python3 tools/wosdbg/tests/generate_incident_fixtures.py \
  --output /tmp/wosdbg-incident-fixtures
```

Run all fixture workflows through a chosen WOSDBG executable:

```sh
python3 tools/wosdbg/tests/incident_cli_semantic_test.py \
  --wosdbg tools/build/bin/wosdbg \
  --require-incident-tools
```

Without `--require-incident-tools`, the test verifies generator determinism and
prints `SKIP` when the binary or incident catalog is not present. This permits
the fixtures to land before production support without concealing missing
coverage in an integration run. Useful narrower invocations are `--mode
collector`, `--mode security`, and `--case 'valid-single-*'`. An existing
corpus may be supplied with `--fixtures PATH`.

The semantic suite is registered as `wosdbg_incident_cli_semantic_test` in the
WOSDBG CTest tree, where `$<TARGET_FILE:wosdbg>` ensures it exercises the binary
built from the same source tree.

## Manifest contract exercised

Every loader fixture uses the version 1 root envelope:

```text
format, version, incidentId, createdUtc,
source {kind, revision, dirty, profile, configMember},
capture {complete, errors, redactions, truncatedMembers},
clocks {quality, domains}, topology {nodes}, members
```

`incidentId` is `sha256:` plus the canonical identity digest used by the
collector. Members are sorted by path and carry `path`, `kind`, `size`,
`sha256`, `required`, `truncated`, and basename-only `sourceName`; relevant
members add numeric `nodeId`, `buildId`, and/or `binary`. The compact corpus
uses these production member kinds:

- `config`
- `binary`
- `coredump`
- `serial-log`

The collector also supports `qemu-log`, `coverage-manifest`,
`coverage-artifact`, and `coverage-log`; those are covered by the collector's
focused host tests rather than duplicated here.

`cases.json` deliberately separates three groups:

- `loaderCases`: valid and degraded directory/USTAR bundles for coredump v1,
  v2, and v3; single- and two-node incidents; partial/non-comparable clocks;
  missing symbols/mappings; a build-ID mismatch; unsupported, corrupt, and
  truncated coredumps; a truncated log; checksum failure; and response bounds.
- `securityCases`: USTAR traversal, symbolic link, hard link, device, truncated
  archive, and oversized-member rejection.
- `collectorCases`: raw KTEST/cluster artifact layouts that are intentionally
  not valid loader inputs. Collector tests can consume these before or without
  a WOSDBG build.

Synthetic ELF64 images contain a deterministic GNU build ID and symbol table.
Synthetic coredumps use the local v1-v3 byte layouts; the unsupported v4 case
retains a v3-shaped body so rejection tests the version gate, not random bytes.

## Shared tool/result contract asserted

Catalog schemas must expose and require `path` for `wosdbg.load_incident` and
the independently usable `wosdbg.validate_incident`; they require `incidentId`
for `wosdbg.get_incident_inventory` and `wosdbg.summarize_incident`. Invoking
all four through one `--batch` workflow checks catalog/dispatcher parity and
batch references. Validation returns the manifest identity; load returns the
stable session identity consumed by inventory and summary.

Load, validation, inventory, and summary results share these stable fields:

```text
ok, valid, incidentId, formatVersion, inventory, issues, degraded
```

Summary additionally carries:

```text
clockQuality, evidence, timeline, semanticDigest
```

Issue codes are asserted exactly as lower-case `snake_case`. Required corrupt,
truncated, or unsupported coredumps make the incident invalid while retaining a
session for bounded inspection. Missing optional evidence and dishonest/inexact
clock information make a valid incident explicitly degraded.

The semantic test compares repeated summaries in one process and across fresh
processes, verifies exact `semanticDigest` equality, rejects snapshot path
leaks, checks numeric node identity survives inventory loading, verifies
coredump versions, checks the top-level declared clock-quality enum as well as
the timeline's observed/effective quality and preserved manifest declaration,
requires honest non-comparable clock behavior, and checks that a tight event
cap remains bounded, reports truncation, and represents both active log lanes.
