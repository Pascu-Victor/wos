# Test Script Agent Notes

`ktest_setup.py` implements the public `bin/wos-ktest` command. It builds,
packages, and launches an isolated single-node WOS VM for kernel selftests with
diagnostic CMake options enabled and the kernel command line set from
`configs/node.json`.

Preserve the isolation contract:

- Build directory: `build-ktest/`
- Generated data and disks: `ktest-data/`
- Target sysroot: `ktest-data/sysroot`
- mlibc, BusyBox, and Dropbear build/install roots under `ktest-data/`

Do not silently fall back to `build/`, `toolchain/sysroot`, `disk.qcow2`, or
`mountfs.qcow2`. `--reset-sysroot` should delete and re-seed only the isolated
KTEST sysroot from the current toolchain sysroot.

Default diagnostic build options include KCFI, KUBSan, KASan, KCOV, selftests,
network packet tracing, allocator diagnostics, memacc provenance, and
mandelbench debug instrumentation. Read `docs/kernel_debug_flags.md` before
adding more expensive flags.

Useful commands:

- `bin/wos-ktest --build-only --reset-sysroot`
- `bin/wos-ktest --build-only`
- `bin/wos-ktest --no-build --no-package`
- `bin/wos-ktest --teardown`

For local verification after editing this script, run:

- `python3 -m py_compile scripts/test/ktest_setup.py scripts/cluster/node_setup.py scripts/cluster/cluster_setup.py`
- `python3 -m json.tool configs/node.json`
- `bin/wos-ktest --help`

Runtime boot and selftest results are user-run evidence unless the user
explicitly asks the agent to launch the VM and the environment permits it.
