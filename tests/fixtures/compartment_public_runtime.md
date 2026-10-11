# Native public compartment runtime probe

This is an actual WOS userspace probe for process selector 54 and
`/usr/bin/wos-compartment`. It runs two short, sequential private service trees:
one through the public spec, one through the launcher. Its assertions cover
private PID 1/parent 0, unchanged nonroot identity, Host path refusal, explicit
asset contents, denied networking, named shared memory and local fork/wait.
It also checks private self/local-PID nice reads, lowering its own priority,
refusal to raise it again, and refusal to use the Host's physical PID as a local
priority target. The Host requires its own nice value to remain unchanged after
each mode.
Successful construction alone does not pass: Host wait must report exit 0 and
the explicitly granted regular result File must contain the service's report.
This probe does not qualify the two-hour, concurrent-tree, hostile-escape or
1,000-lifecycle campaigns in goal 39.

Use a completed normal Build WOS and its current sysroot:

```sh
python3 tests/fixtures/build_compartment_public_runtime.py \
  --output build/compartment-public-runtime
readelf -l -d build/compartment-public-runtime
```

The probe needs `/lib/ld.so`, `libc.so` and `libjournal.so`. The Host explicitly
imports all three, the probe ELF and a 13-byte `private-root\n` marker. It grants
one writable regular File as private FD 9. That File intentionally remains a
Host capability; private path confinement does not revoke it. No stdin, stdout,
stderr, directories, devices or sockets enter the private descriptor table.
Both services inherit UID/GID 100, zero authority, NNP and the creator's limits.
Both invocation paths pass the Host's decimal physical PID as the explicit
`--service --host-pid NUM` arguments. The bounded parser accepts only decimal
IDs greater than 2 and no greater than `INT64_MAX`. This guessed identifier
grants no Host object capability: the probe creates only private PID 1 and child
PID 2. Priority selectors must report `ESRCH` for both read and mutation using
that absent local name. Local nice checks use actual libc error handling; the
Host-PID checks use raw 64-bit wrappers to avoid truncating the identifier.
The private initial nice-0 check records the current native default and does
not qualify creator nice or CPU placement inheritance.

Prepare only an inactive, agent-owned private rootfs COW overlay over the
current normal rootfs. Install the ELF at
`/usr/libexec/compartment-public-runtime` (0755), the marker at
`/srv/compartment-marker` (0644), and an empty
`/srv/compartment-results` directory owned by UID/GID 100 with mode 0700.
Do not use `/tmp` for the reports: init's required tmpfs is root-owned 0755 and
its contents do not survive VM shutdown. Append
`compartment_public_runtime_services.fragment` to that overlay's service
manifest, keeping the existing journald service. The fragment selects the
ordinary `httpd` account (UID/GID 100) and journal-backed stdout. Start each run
with an empty report directory; the probe refuses pre-existing result Files.

Launch the one-node configuration pointing exclusively at private boot/rootfs
images through the shared workflow:

```sh
bin/wos-cluster --launch --no-setup --config /absolute/path/private-cluster.json
```

Use a private reflink boot image with the normal kernel/initramfs and no
`--selftest` command line. Do not modify shared disks, switch KTEST to the normal
sysroot or bypass the launch guard. Isolated diagnostics use
`bin/wos-ktest --fast --build-only`, then `bin/wos-ktest --fast --no-build
--no-package --no-setup`; `--no-setup` applies only to launch.

The expected final receipt is `PASS public-service modes=2 failures=0`, with
both mode rows reporting exit status 0, 21 checks, zero failures, failed-mask 0
and `host-nice-preserved=1`. The original 14 checks remain; seven priority checks
are added. Private self and local PID 1 start at nice 0, self lowers to nice 5,
local PID 1 reads 5, an attempted increase to nice 4 returns `EACCES`, and both
Host-PID operations return `ESRCH`. The actual private report is seven
little-endian uint64 values: magic
`0x39335055424c4943`, PID, parent, UID, checks, failure count and failed-check
bitmask. The Host returns 42 on a mode failure, 40 on wrong initial identity or
local-placement refusal, 41 on asset-open refusal and 43 on invalid arguments.
The persistent `/srv/compartment-results/status` and `result-0`/`result-1` Files
are small, fsynced diagnostic receipts. Missing stdout is not proof of a boot
stall; inspect init's published wait status and these Files.

Before each case measure allocated disposable images and available space;
enforce a 20GiB budget and 40GiB free-space floor. Keep one active topology,
read-only shared bases and private COW/reflink copies. After success, failure or
timeout, stop only the exact-owned VM and prove PID/QMP/FD/inode/backing
references are gone. Read the small receipts from its stopped overlay, then
prune all disposable disks and obsolete per-run appliances. Preserve user VMs,
normal/KTEST build/sysroot/current artifacts and required immutable seeds.
