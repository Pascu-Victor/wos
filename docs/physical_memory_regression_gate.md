# Exact physical-memory regression gate

WOS physical-memory accounting is a closed balance sheet. A checkpoint passes
only when both equations are exact:

```text
managed_pages =
    free_pages
  + allocator_metadata_pages
  + zone_descriptor_pages
  + per_cpu_page_cache_reserve_pages
  + sum(named_runtime_owner_pages)

physical_address_pages =
    managed_pages
  + sum(named_firmware_and_boot_reserve_pages)
```

There is no tolerance. `identity_mismatch_pages` and
`untracked_unreclaimable_pages` must both be zero.

## Fast deterministic gate

The normal host-test surface includes:

- `physical_balance_test`: compiles the real buddy allocator with a host-only
  IRQ-lock adaptation and exercises allocation, ownership transfer,
  block-splitting, free, concurrent mutation/snapshot, and exact injected
  owner/free-counter faults.
- `wos_memory_balance_test`: exercises the checkpoint and soak validators with
  injected identity, descriptor, quiescence, reclaim, and overhead faults.
- `phys_source_test` and `memacc_source_test`: retain structural checks for the
  fixed-capacity global snapshot, procfs schema, owner transfers, and reclaim
  entrypoints.

Run the focused gate with:

```sh
scripts/test/run_tests.sh build
ctest --test-dir /tmp/wos-tests --output-on-failure \
  -R '^(physical_balance_test|phys_source_test|memacc_source_test|wos_memory_balance_test)$'
```

## Four-node acceptance soak

Build WOS first. The acceptance runner launches a fresh cluster from
`configs/cluster_selfhost_4_host32.json` by default; do not use
`--use-running-cluster` for clean-boot acceptance evidence.

Supply a controlled pre-accounting self-host `runs.tsv` collected with the same
host resources, build command, placement policy, repository mirror, and
distfiles policy. The runner compares medians and rejects accounting overhead
above 2%.

Example:

```sh
scripts/bench/run_wos_memory_balance_regression.py \
  --expected-commit "$(git rev-parse HEAD)" \
  --overhead-baseline-runs-tsv /path/to/control/runs.tsv \
  --jobs 36 \
  --mirror-file /tmp/wos-git-repos \
  --distdir /root/wos-distfiles
```

The fixed acceptance sequence is:

1. Exact quiescent clean-boot checkpoint on all four nodes.
2. Three complete distributed self-host builds on the same boot, with an exact
   quiescent checkpoint after each build.
3. Deletion of the submitter-owned guest self-host tree, `sync`, and another
   exact quiescent checkpoint.
4. Five minutes of quiescence and another exact checkpoint.
5. Controlled anonymous-memory pressure on VM0 and an exact in-pressure
   checkpoint.
6. Pressure release, a quiescent pre-reclaim checkpoint, explicit buffer,
   packet, XFS inode, and file-mapping cache reclaim on every node, a bounded
   deferred-free settling interval, and a final exact quiescent checkpoint.

The runner requires every `pressure_reclaim` physical-owner category to contain
pages during controlled pressure and to release pages by the post-reclaim
checkpoint. Subsystem-specific endpoint metrics are compared immediately
before and after explicit reclaim. The runner also verifies the 2% overhead
bound, unchanged boot IDs reported by the self-host harness, successful
distributed placement, disabled caller-provenance diagnostics, and zero
quiescent build/WKI active or pending counters.

## Evidence

Every checkpoint contains:

- the coherent `memacc raw all` output and derived summary;
- WKI, IPC, scheduler, and `meminfo` snapshots;
- `status.tsv`;
- canonical `snapshot.json`;
- SHA-256 digests for every raw input.

The soak root contains the copied cluster configuration, three complete
self-host result trees, pressure and reclaim logs, `progress.jsonl`, and
`manifest.json`. The final manifest inventories every evidence file by SHA-256
and records pass/fail classification. Failed runs remain on disk and must not be
relabeled as acceptance evidence.

The latest evidence predating this permanent runner is
`benchmarks/results/wos-memory-balance-final-packetdrain-abffc81e-20260730`.
That directory is locally ignored benchmark output, not a substitute for a new
run of the committed gate.
