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
`/usr/bin/perf cpustat` output after each WOS benchmark step. The suite runs
the probe against the WOS benchmark hosts, stores the outputs under the step's
`perf-cpustat/` directory, and records the artifacts in that step's manifest
entry under `wos_perf_cpustat`.

These cpustat snapshots are diagnostic data and do not turn a completed WOS
benchmark step into a failure if `perf cpustat` is unavailable or times out.
