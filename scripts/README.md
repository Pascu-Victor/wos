# WOS Scripts

Public commands live in `../bin` and are added to `PATH` by `.vscode/wos-env.sh`.
Use lowercase hyphenated command names such as `wos-cluster`, `wos-ssh`,
`wos-test`, and `wos-format`.

The implementation scripts are grouped by role:

- `build/`: build, image, initramfs, and rootfs staging helpers.
- `cluster/`: WOS and Linux cluster topology helpers.
- `test/`: host-side test and kernel coverage tools.
- `bench/`: benchmark orchestration, provisioning, and benchmark runners.
- `remote/`: WOS and Linux SSH/SFTP/SCP helpers.
- `debug/`: host-side debug artifact extraction.
- `dev/`: developer maintenance tools.
- `lib/`: reserved for shared script helpers.

Do not add new public scripts at the top level of this directory. Add the
implementation under the appropriate category and expose it with a `bin/`
symlink only if it is meant to be part of the user-facing command surface.

## WOS self-host build benchmark

`scripts/bench/run_wos_selfhost_build.sh` runs the clone/submodule/bootstrap/
configure/build flow either inside an already-launched WOS VM or on Linux. It
defaults to cloning `https://github.com/Pascu-Victor/wos.git`, building the
`wos_full` target with Qt/wosdbg and host-only disk-image packaging disabled,
and writing
`selfhost-report.tsv`. It also writes `selfhost-detail.tsv` for the current
run and appends the same detailed rows to `<workdir>-history.tsv` by default.
In WOS mode it also writes `selfhost-cache-deltas.tsv` with VFS, XFS dentry,
and buffer cache counter deltas around each timed step and nested phase. Those
detailed rows include root WOS repository fetch time, recursive submodule
update time, configure time, build time, mode, commit, target, job count, and
shallow/full-history mode. Use `--history-file <path>` to put the append-only
history somewhere else. The intent is to see whether self-hosting changes move
the whole clone/build process in the right direction instead of only improving
one local step.

WOS mode defaults to `/root/wos-selfhost-bench` so the checkout and build land
on the XFS rootfs rather than `/tmp` tmpfs. The default clone is shallow
because the benchmark needs current source and submodule contents to build, not
full Git history. The default job count is 32.

For patched iteration without paying GitHub download time, prefer a shallow
bare mirror over a copied source tree:

```sh
scripts/dev/git_mirror_for_wos.sh snapshot --worktree
scripts/dev/git_mirror_for_wos.sh sync-file-mirror wos-0 /tmp/wos-git-repos
scripts/bench/run_wos_selfhost_build.sh wos --host wos-0 --jobs 32 --mirror-file /tmp/wos-git-repos
```

This keeps the benchmark's source acquisition as a depth-1 Git clone inside WOS
while avoiding a full live-workspace copy. `snapshot --worktree` captures the
current non-ignored worktree in a temporary synthetic commit without changing
the real index. Do not copy the live workspace into the VM for debug runs.
`--source-cache <path>` can still point at an already-staged depth-1 checkout
that contains initialized shallow submodules, but that skips clone timing and
is not the acceptance path measurement. Exported source trees are refused so a
debug run does not silently become a full workspace copy.

Rootless WOS launches can reuse an existing topology with either:

```sh
bin/wos-cluster --launch --no-setup
bin/wos-ktest --no-setup
```

Then collect comparable reports:

```sh
scripts/bench/run_wos_selfhost_build.sh wos --host wos-0 --jobs 32
scripts/bench/run_wos_selfhost_build.sh linux --workdir /tmp/wos-selfhost-linux \
  --jobs 32 --skip-bootstrap --host-toolchain "$PWD/toolchain/host"
```

The explicit host-toolchain path gives a fresh Linux checkout the same
preinstalled-toolchain premise used by the native WOS configure. The benchmark
uses that toolchain's CMake and derives its populated target sysroot from
`toolchain/sysroot`; pass `--host-sysroot` to override it. This does not seed or
bypass source checkout work, and compiler ABI probes must complete successfully.
Immediately before the timed configure step, both modes run the selected CMake
and Ninja version commands. This keeps executable paging outside the project
configure interval without warming source-tree metadata or generated files.

When running the script from an existing WOS shell, use `wos-local`; this is
the same WOS benchmark payload without the outer SSH hop.

Pass `--full-history` to measure a full-history recursive clone instead.

Compare clone/build/total timing with:

```sh
scripts/bench/compare_wos_selfhost_reports.py \
  --wos /root/wos-selfhost-bench/selfhost-report.tsv \
  --linux /tmp/wos-selfhost-linux/selfhost-report.tsv \
  --json-output benchmarks/results/wos-selfhost-comparison.json
```

For a checkout/configure diagnostic comparison, include the detailed timing
reports. This profile compares the derived `clone_checkout` time from root
repository clone plus recursive submodule init/update, and `configure_wos`,
with a WOS/Linux ratio of 1.0. It is not the full-process acceptance check:

```sh
scripts/bench/compare_wos_selfhost_reports.py \
  --wos /root/wos-selfhost-bench/selfhost-report.tsv \
  --linux /tmp/wos-selfhost-linux/selfhost-report.tsv \
  --wos-detail /root/wos-selfhost-bench/selfhost-detail.tsv \
  --linux-detail /tmp/wos-selfhost-linux/selfhost-detail.tsv \
  --acceptance-profile checkout-configure \
  --json-output benchmarks/results/wos-selfhost-checkout-configure-diagnostic.json
```

### Repeated WOS self-host acceptance runs

`scripts/bench/run_wos_selfhost_repeatability.sh` is the host-side evidence
wrapper for repeated full builds. It runs 50 fresh self-host attempts by
default in one already-booted WOS VM. Every attempt uses the normal shallow
clone, runs `tools/bootstrap.sh`, configures a new `build-selfhost`, and builds
`wos_full`; it never enables `--resume-checkout`, `--source-cache`, or
`--skip-bootstrap`.

Launch the self-host VM once, then start the repeatability run:

```sh
bin/wos-cluster --config configs/cluster_selfhost.json --launch --no-setup
scripts/bench/run_wos_selfhost_repeatability.sh \
  --output-dir benchmarks/results/wos-selfhost-repeat-50
```

The wrapper times the complete host-side command, including the guest workdir
replacement that occurs before the runner's own timed phases. Before the next
attempt replaces that workdir, it archives the current reports, bootstrap
detail, submodule manifest, CMake cache, and command logs. It also saves the
exact new byte range from `serial-vm0.log`, pins the serial-reported boot ID,
records guest uptime and resource snapshots, rejects commit or submodule drift,
and detects a reboot or serial-log reset. A failed attempt does not stop later
attempts, but the final command returns nonzero unless all requested runs pass.

The result directory contains:

- `runs.tsv`: one machine-readable status row per attempt;
- `summary.tsv` and `summary.json`: aggregate pass/fail evidence;
- `runs/run-NNNN/`: console output, reports, guest snapshots, serial range,
  serial matches, CMake cache, and `command-logs.tar` for that attempt.

The required performance acceptance profile compares the complete outer
command `wall_ms` recorded in WOS repeat and Linux baseline `runs.tsv`
evidence. It enforces every WOS comparison at WOS/Linux <= 1.10:

```sh
scripts/bench/run_linux_selfhost_baseline.sh \
  --expected-commit <commit-from-the-WOS-batch> \
  --output-dir benchmarks/results/linux-selfhost-baseline
scripts/bench/compare_wos_selfhost_reports.py \
  --wos-runs benchmarks/results/wos-selfhost-repeat-50/runs.tsv \
  --linux-runs benchmarks/results/linux-selfhost-baseline/runs.tsv \
  --acceptance-profile full-process \
  --json-output benchmarks/results/wos-selfhost-full-process-acceptance.json
```

`run_linux_selfhost_baseline.sh` runs 50 fresh attempts by default. Like the
WOS wrapper, it delegates each attempt to `run_wos_selfhost_build.sh`, never
uses resume, a source cache, `--skip-bootstrap`, or a prebuilt host toolchain,
and records the outer wall clock around the runner invocation. It archives
per-run command logs, all timing reports, bootstrap details, the CMake cache,
root commit and recursive submodule manifest, plus before/after Linux host
snapshots. Host reboot, source drift, command failure, or missing evidence
rejects the affected batch while later attempts continue. Use the same job
count, repository/mirror policy, and expected commit for the WOS and Linux
batches.

Both modes configure the benchmark target with host-only disk-image packaging
disabled. The full sysroot and all kernel/userspace component artifacts are
still built and verified, while the timed target graph stays identical across
Linux and WOS.

Linux evidence may contain one accepted baseline row, used for every WOS run,
or the same number of rows as the WOS evidence for pairwise comparison. Every
input row must have successful `runner_status` and `evidence_status` fields and
`accepted=1`. The profile refuses `selfhost-report.tsv`: its `total` is an
inner phase sum and does not include the complete outer command, including
remote invocation and fresh-workdir preparation. The existing inner Linux
reports alone are therefore not sufficient acceptance evidence.

The default serial failure expression covers kernel OOM, XFS allocator/btree
errors, kernel panic, KASan, and UBSan diagnostics. Use
`--serial-fail-regex` only when a documented acceptance profile requires a
different expression. Use `--expected-commit` when the accepted root commit is
known in advance; otherwise the first observed commit becomes the batch
baseline. Direct GitHub cloning remains the default. `--mirror-file` is
available for iteration on a locally snapshotted tree, but mirror-backed runs
must be identified as such in reported evidence.

## Cross-OS host KVM tracing

`scripts/bench/run_cross_os_benchmark_suite.py` can optionally wrap each
benchmark step with host-side KVM tracing:

```sh
scripts/bench/run_cross_os_benchmark_suite.py \
  --num-vms 4 \
  --os wos \
  --host-kvm-trace both
```

The `perf` mode records:

```sh
perf record -a -g -e 'kvm:*'
```

The `trace-cmd` mode records:

```sh
trace-cmd record -b 20000 -e kvm
```

Trace artifacts are written under each suite's `host-kvm-tracing/` directory
and indexed by a distinct `host-kvm-tracing` manifest step. Missing tools,
permissions, or absent KVM tracepoints are recorded as trace metadata and do
not fail the benchmark step.

For non-sudo runs, either grant the host tools the required capabilities or
pass a prepared wrapper with `--host-kvm-perf-cmd` and
`--host-kvm-trace-cmd`. Shell or Python scripts are not good direct `setcap`
targets, so prefer a compiled wrapper or the tool binary itself. Preview or
apply the common capability setup with:

```sh
scripts/bench/setup_host_kvm_trace_caps.sh --dry-run
sudo scripts/bench/setup_host_kvm_trace_caps.sh
```

Some kernels also require a permissive `kernel.perf_event_paranoid` setting
or mounted `tracefs`/`debugfs` before KVM tracepoints can be recorded.

## Cross-OS showcase step

`scripts/bench/run_cross_os_benchmark_suite.py` runs a lightweight showcase
step before the heavier mandel/render benchmarks unless `--skip-showcase` is
passed. On WOS this calls `/root/run-wos-showcase` (also symlinked as
`/run-wos-showcase`), installed into the XFS rootfs from
`configs/rootfs/root/wos-showcase/`. The scripts demonstrate `wkictl` placement
wrappers, `forward +path -path`, VFS probes, and small benchmark commands. On
Linux the paired runner uses SSH/SFTP and a tiny TCP helper instead of WOS IPC.

Use `--showcase-scale quick|full|stress` to tune the workload size.

## Cross-OS host schedstat probe

The same suite can add a distinct host-side schedstat step that samples QEMU
KVM vCPU threads around a WOS command:

```sh
scripts/bench/run_cross_os_benchmark_suite.py \
  --num-vms 4 \
  --os wos \
  --schedstat-probe \
  --schedstat-auto-discover-qemu
```

The default payload is `/usr/bin/testprog perf --verbose` on the WOS launcher.
Use `--schedstat-command` for a different host command, `--schedstat-duration`
for a passive window, or repeat `--schedstat-qemu-pid vmN:PID` when automatic
QEMU discovery is ambiguous.

## Cross-OS WOS cpustat snapshots

`scripts/bench/run_cross_os_benchmark_suite.py` now also captures WOS
`/usr/bin/perf cpustat` output before and after each WOS benchmark step. The
suite runs the probe against the WOS benchmark hosts, stores the outputs under
the step's `perf-cpustat/before/` and `perf-cpustat/after/` directories, and
records the artifacts in that step's manifest entry under `wos_perf_cpustat`.

Each cpustat record includes a `phase` field (`before` or `after`). These
snapshots are diagnostic data and do not turn a completed WOS benchmark step
into a failure if `perf cpustat` is unavailable or times out.
