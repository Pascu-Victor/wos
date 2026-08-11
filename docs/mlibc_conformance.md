# WOS mlibc conformance baseline

WOS maintains a versioned target-runtime baseline for the mlibc ANSI, POSIX,
Linux, Linux-wrapper, and dynamic-loader test corpus. This is an integration
gate for the declared WOS compatibility profile, not a claim of complete POSIX
certification.

The source of truth is tests/conformance/mlibc/baseline-v1.json. Its inclusion
rule is every case registered by the selected mlibc Meson suites. The validator
compares that list with the current mlibc source declarations, compares active
suites with scripts/build/build_mlibc.sh, and checks missing sysdep-tag
prerequisites against WosSysdepTags. A newly registered case therefore fails
validation until it is reviewed and added.

The version 1 profile enables ANSI, POSIX, and RTLD tests. It declares 187
corpus cases: 159 runnable and 28 reviewed unsupported. Fourteen Linux and
Linux-wrapper cases remain visible but are unsupported because WOS deliberately
builds mlibc with linux_option disabled. Twelve POSIX scheduler, timer, signal,
and wait cases are unsupported because their required sysdep tags are absent.
Two GNU IFUNC cases are unsupported because Clang does not implement the IFUNC
attribute for the x86_64-pc-wos target. Unsupported entries require both a
technical rationale and an objective missing prerequisite; runtime failures are
never converted automatically into exclusions.

## Build and staging

Normal Build WOS builds the selected target corpus and stages it at
build/mlibc-conformance-stage. The rootfs recipe installs that immutable
payload at /usr/libexec/wos-mlibc-conformance. The isolated KTEST workflow uses
build-ktest/ and ktest-data/ instead and includes the same mlibc_conformance
target:

    bin/wos-ktest --build-only --reset-sysroot

The build report is
build-ktest/mlibc-conformance-stage/build-results.json. It identifies the
repository, mlibc revision, manifest digest, every staged artifact digest, and
each case's build or unsupported status.

## Live reproduction

When the bridge, TAP, and ivshmem topology already exists, WOS can be launched
and debugged without root access by appending --no-setup:

    bin/wos-ktest --kernel-cmdline="" --no-setup
    bin/wos-cluster --config configs/cluster_selfhost.json --launch --no-setup

An empty KTEST kernel command line keeps the VM available after boot instead of
running the boot-time selftest-and-poweroff path. Run the complete declared
baseline, including the port profile, from the host:

    python3 scripts/test/mlibc_conformance.py run \
      --target wos-ktest \
      --output-dir test-results/mlibc-conformance

Use --suite ansi, --suite posix, --suite rtld, or repeated --case ansi/abs
selectors for focused reproduction. Port smokes are included by default only
in an unfiltered full run; use --port-smokes or --no-port-smokes to override
that behavior.

The target dispatcher accepts only a fixed case identifier and invocation
index generated from the reviewed manifest. It does not evaluate manifest text
as shell input. Every invocation gets an isolated temporary directory, a
target-side timeout, a host transport timeout, and a combined output limit.

## Results and gate policy

Each run writes stable JSON to results.json and JUnit XML to results.xml. Both
formats record the manifest digest, repository and mlibc revisions and dirty
state, target, UTC timestamps, and durations. JSON also records captured
bounded output and summary counts. Status values are:

- pass
- assertion_failure
- crash
- timeout
- build_failure
- unsupported
- infrastructure_failure

Any status other than pass or a pre-reviewed unsupported entry makes the runner
exit nonzero. The full gate is green only when no unexpected status remains.

The compatibility smoke profile starts BusyBox, Dropbear, Bash, Python, CMake,
and Git through bounded, non-networked commands. These cases protect loader,
startup, parsing, basic I/O, and representative standard-library behavior
without mutating repositories or opening services.

## Host checks and maintenance

Run the focused host contract checks with:

    python3 tests/host/unit/mlibc_conformance_test.py
    cmake --build tests/build --target wos_abi_parity_test
    ctest --test-dir tests/build --output-on-failure -R 'mlibc_conformance|wos_abi_parity'

wos_abi_parity_test compares duplicated kernel/libc syscall selectors,
constants, enum storage, structure sizes and alignments, and field offsets. The
mlibc shared-memory sysdep also asserts that its public ipc_perm and shmid_ds
layouts match the wire structures the kernel writes.

When updating the corpus, change the manifest and its rationale intentionally,
run its malformed-input host tests, rebuild the smallest affected suite, and
then run the complete target baseline and port profile. Keep mlibc fork changes
as a reviewable submodule commit and update the superproject pointer
deliberately.
