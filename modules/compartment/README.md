# wos-compartment

This experimental WOS launcher creates a fresh native mount/PID/named-IPC view
through the version-1 public spec. Its mapped input/refusal recovery and a short
nonroot guest launch/fork/wait probe are qualified. The full isolation campaign
is pending. See `docs/wos_service_compartments.md` for current evidence.

Normal Build WOS installs `/usr/bin/wos-compartment` and the standalone C++ header
`/usr/include/wos/compartment.hpp`. Use `wos-compartment --help` for the current
options and `wos-compartment --version` for the spec version.

Supply every executable, interpreter, library and data File explicitly:

```sh
wos-compartment \
  --asset /srv/service /app/service 0755 \
  --asset /lib/ld.so /lib/ld.so 0755 \
  --asset /usr/lib/libc.so /usr/lib/libc.so 0644 \
  --env LD_LIBRARY_PATH=/usr/lib \
  -- /app/service argument
```

This is invocation syntax, not a tested service fixture. Inspect your service's
ELF interpreter and needed libraries; additional dependencies need assets too.
The launcher itself currently uses `/lib/ld.so` and `libc.so`. Asset modes are
octal. Asset paths are canonical absolute paths, with at most sixteen components;
directory parents are created during import. The service executable path must
fit the current kernel's 255-byte observation bound. No automatic `/proc`, device
nodes, shell or library search environment is supplied.

Networking is denied by default. `--share-network` explicitly requests shared
Host networking. `--ceiling` and `--service-privileges` retain only admitted bits;
both default to zero. IDs and supplementary groups remain the creator's, set-ID
execution cannot raise authority, and dumpability defaults to disabled.
`--limit ID SOFT HARD` lowers one of sixteen inherited limits. `--root-bytes`
sets the imported-data budget, not a total filesystem or process-memory limit.

No ambient FDs are inherited. `--fd SOURCE_FD DEST_FD` grants an already-open
local regular File, preserving rights, shared offset and backing identity.
Directory, device, socket and WKI-backed storage descriptor grants are refused.
The launcher closes its own asset FDs; it does not close borrowed grant FDs.
The manifest names every grant and asset, policy and limit without printing
environment values. Explicit shared mappings require separate runtime
qualification; path isolation does not revoke a deliberately granted File.

Foreground mode waits for the physical Host PID with the full 64-bit WOS wait
interface and returns its exit status. `--detach` returns after publication and
uses ordinary Host orphan adoption. A launch refusal prints a kernel diagnostic.
At most 64 assets, 256 grants and 256 entries per vector are accepted. Argument
strings and pointers share a 2MiB budget; imported data is 4KiB–32MiB in pages.

Development validation uses the registered `compartment_launcher_test` Host
test and actual mapped `CompartmentPublicInput` KTEST cases. The Host test adapts
raw syscall boundaries while compiling the actual launcher; KTEST deliberately
refuses before scheduling its synthetic ELF. The actual guest probe, build helper
and service fragment are documented in
[compartment_public_runtime.md](../../tests/fixtures/compartment_public_runtime.md).
Both public-spec and CLI modes passed with private PID1/parent0, UID100,
14checks/0failures and successful Host waits. This short sequential test leaves
CPU-placement inheritance, hostile isolation, two-hour simultaneous service
trees,1,000full lifecycles and complete Host/init/libc regression unqualified.
Owned software qualification may use `bin/wos-cluster --no-setup` or isolated
`bin/wos-ktest --no-setup`, with one topology, a 20GiB disposable storage budget,
40GiB free-space floor and exact-owned inactive-image pruning after every run.
