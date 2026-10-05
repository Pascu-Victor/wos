# Bounded aggregate WKI diagnostics

The four-node stability captures reached the 262,142-byte text limit inside
`/proc/wki/netdiag`'s channel table. The channel snapshot also retained at most
256 entries from a fixed 1,024-slot allocation pool. Missing completion footers
and rows prevent those captures from qualifying full aggregate state.

Netdiag now allocates a bounded 4 MiB content buffer and copies channels through
the actual pool capacity. The pool remains 1,024 slots; other channel diagnostic
callers retain their 256-row bound. Existing row fields, order and terminal
`wki_netdiag_end complete=1` remain unchanged. Reaching the bounded wait snapshot
capacity emits a conservative truncation marker. Existing subsystem caps and
allocation-failure markers remain explicit. The footer establishes completion
of the retained text; consumers must also reject these markers and nonzero
truncation counts to qualify a full snapshot. Collection across subsystems is
sequential and does not establish one atomic system observation.

The conservative retained-row bound is about 2.65 MiB and 3,280 rows: every
integer is charged its maximum decimal width, paths and names their fixed
capacity, and each virtio row an 8 KiB bound. The channel bound shares the
constant used by the allocation pool. Future field or capacity changes need
the bound reviewed again.

Content remains generated once per open descriptor, cached under its existing
mutex, and freed on close. Diagnostic read authorization still precedes
allocation, cached data, zero-count reads and EOF. Content allocation failure
returns `ENOMEM` with a null cache and untouched caller output. The larger
buffer increases retained memory per open diagnostic descriptor from 256 KiB
to 4 MiB, plus existing snapshot staging. The existing kernel allocator can
back large allocations with individually allocated physical pages.

The chaos host runner admits up to 4 MiB and 4,096 rows for netdiag. Other
snapshot sources retain their 256 KiB/1,024-row limits; ordinary command,
stderr and WOSDBG telemetry limits remain unchanged. Raw snapshot files remain
available when the existing telemetry compactor reduces metadata records.

The host regression executes the actual channel snapshot, channel formatter,
procfs read/cache and close bodies with modeled allocator, locks and other
subsystem content. It checks all 1,024 slots, maximal values, short buffers,
partial reads, stable cached content, refused reads and allocation failure.
Seven semantic or capacity mutants fail. The consumer test retains a snapshot
larger than both former limits and rejects missing footers and cap markers.
This host evidence does not qualify guest allocation pressure, IRQ delivery,
other subsystem snapshot serialization or distributed fault recovery.

Normal and isolated diagnostic builds, required formatting, 27 focused host
checks, and the final collector regression passed. Fresh isolated KTEST passed
66,703 selftests with zero failures and powered off normally. The fresh normal
four-node qualification passed settled and final health, SMP clocks, idle before
and after work, 2,000 shell launches, 24 timer rounds, 340 interval checks,
12 verified 8 MiB transfers, 400 SSH commands and 40 reconnects. Eight launch
captures retained complete event streams without loss.

All 32 aggregate snapshots completed without reported caps or allocation
failure. Their size was 258,964–292,867 bytes, with 218–252 active channel rows.
The guest matrix therefore verifies the enlarged text buffer; the host test
provides the separate 1,024-slot channel-capacity coverage. Measured idle worker
CPU stayed below 0.027% of one guest core, and visible maintenance below 0.10%.
These task measurements do not establish complete global IRQ or host CPU cost.

Across 800 unprofiled native launches, node medians were true 1.30–1.37 ms wall
and 0.34–0.36 ms child user; whoami 1.37–1.43 ms wall and 0.37–0.39 ms user.
The private libc was unchanged. This diagnostic change does not establish a
causal speedup; the approximately 1 ms target remains open.

The first normal attempt stopped at the idle collector's old 256 KiB SSH
stdout bound, before idle sampling. Its earlier settled health already retained
complete larger snapshots. After preserving and closing that phase, the
original baseline was restored and passed settled four-node health before
diagnosis. A fresh retry used a private collector with a 4 MiB stdout bound
only for the exact netdiag command; ordinary stdout, stderr and deadlines
remained unchanged. That retry passed the matrix above. The original-baseline
archive also hit a quota failure and was recovered into workspace storage,
preserving the damaged
partial copy and original images. All owned phases are closed; default disks,
shared libraries and unrelated host VMs were preserved.

Evidence is under
`/tmp/wos-stability-resume/launch-cost-current/clock-recon/netdiag-completeness-v20`
and `netdiag-completeness-v20b`; the latter's `matrix-proof.json` binds the
individual gates and raw snapshots. Closed normal images are retained under
`build/goal49-napi-normal-archives/`. Initial and final source ledgers distinguish
the runtime-tested source from subsequent documentation status updates.
Forced guest allocation exhaustion, selective loss/reorder, two-node/multi-hop,
mixed leases, trusted remote execution and physical hardware are outside this
qualification. Goals 48, 49 and 50 remain open.
