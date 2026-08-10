# WOS init supervisor

WOS PID 1 uses a small, WOS-specific service model. It is not a systemd unit
format and it does not execute shell fragments. The checked-in default is
`configs/wos-services.conf`; image packaging installs the same file as
`/etc/wos-services.conf` in both the initramfs and the normal root filesystem.

This document separates three kinds of statements:

- **Manifest and ABI contract** describes validation performed directly by the
  parser or kernel control broker.
- **Supervisor invariant** describes behavior the PID 1 runtime and the pure
  model must preserve together.
- **Live evidence required** identifies behavior that cannot be established by
  host tests alone. No live boot or fault-campaign success is claimed here.

The append-only kernel/userspace structures are described separately in
[`init_control_abi.md`](init_control_abi.md).

## Version 1 manifest

The format is bounded, line-oriented printable ASCII. Blank lines and lines
whose first non-space character is `#` are ignored. The first content line must
be the `version` assignment with decimal value `1`; its canonical spelling is
`version=1`. A service begins with `[service NAME]`; following `key=value`
assignments belong to that service until the next header. Spaces at the
beginning and end of a line, key, or value are trimmed. There is no quoting,
escaping, variable expansion, command substitution, include, or continuation
syntax.

```text
version=1

[service example]
exec=/usr/bin/example
arg=/usr/bin/example
type=daemon
class=normal
priority=0
environment=init
readiness=immediate
readiness-timeout-ms=5000
restart=on-failure
backoff-initial-ms=250
backoff-max-ms=30000
restart-budget=5
restart-window-ms=60000
stable-run-ms=60000
stop-term-ms=2000
stop-kill-ms=1000
drain-timeout-ms=2000
stdio=inherit
enable=always
```

Every non-conditional scalar field in the table below is required exactly once.
Conditional fields follow the rules in the table; `arg`, `env`, and `depends`
are the only repeatable fields.

| Key | Cardinality | Accepted value or bound | Meaning |
| --- | --- | --- | --- |
| `exec` | 1 | Safe absolute path, at most 255 bytes | Executable passed to `execve`; no path search occurs. |
| `arg` | 1–16 | Nonempty printable value, at most 255 bytes | Ordered argument vector. The first `arg` must equal `exec`. |
| `env` | 0–16 | `KEY=VALUE`, at most 255 bytes | Extra environment entry. Keys must be unique and use C identifier spelling. |
| `depends` | 0–8 | Another manifest service name | Hard readiness dependency. Repetition, self-reference, unknown names, and cycles are rejected. |
| `type` | 1 | `daemon`, `oneshot` | Long-running process or completion unit. |
| `class` | 1 | `journal`, `network-provider`, `network-consumer`, `normal` | Ordering and validation class. |
| `priority` | 1 | `-20` through `19` | Nice value applied to the service process. |
| `environment` | 1 | `init`, `empty` | Start from PID 1's bounded base environment or an empty base before applying `env`. |
| `readiness` | 1 | `immediate`, `ipv4`, `exit-success` | Readiness adapter. |
| `readiness-interface` | Conditional | Interface name, at most 15 bytes | Required only for `readiness=ipv4`; forbidden otherwise. |
| `readiness-timeout-ms` | 1 | `1`–`600000` | Deadline for becoming ready. |
| `restart` | 1 | `never`, `on-failure`, `always` | Restart eligibility after a completed generation. |
| `backoff-initial-ms` | 1 | At least `1` and no greater than the maximum | First restart delay. |
| `backoff-max-ms` | 1 | `1`–`3600000`, and at least the initial delay | Cap for exponential restart delay. |
| `restart-budget` | 1 | `1`–`32` | Maximum automatic replacements admitted in one restart window. |
| `restart-window-ms` | 1 | `1`–`86400000` | Failure-accounting window. |
| `stable-run-ms` | 1 | `1`–`86400000` | Ready time required before failure/backoff accounting resets. |
| `stop-term-ms` | 1 | `1`–`60000` | Grace period after TERM. |
| `stop-kill-ms` | 1 | `1`–`60000` | Reap/quiesce period after KILL escalation. |
| `drain-timeout-ms` | 1 | `1`–`60000` | Final bound for owned output drainage. |
| `stdio` | 1 | `inherit`, `null`, `journal` | Child standard-stream policy. |
| `enable` | 1 | `always`, `disabled`, `path-exists`, `path-missing` | Initial enablement policy. |
| `enable-path` | Conditional | Safe absolute path, at most 255 bytes | Required for either path condition; forbidden otherwise. |

Global limits are 32 KiB per manifest, 512 bytes per physical line, 16 services,
and 31 bytes per service name. A service name starts with an ASCII
letter and then contains only letters, digits, `_`, or `-`. Safe paths are
absolute, have no empty, `.` or `..` segment, do not end in `/`, and contain
only letters, digits, `_`, `-`, `.`, or `+` within each segment.

Validation rejects duplicate names or environment keys, ambiguous fields,
unsafe paths, invalid numeric ranges, unknown dependencies, hard dependencies
from an enabled service to an explicitly disabled service, and dependency
cycles. A `oneshot` must use `exit-success` and cannot use `restart=always`; a
`daemon` cannot use `exit-success`.

Manifest acceptance is all-or-nothing. PID 1 must not initialize or start a
partial graph after any read, size, parse, validation, or model-initialization
error. It remains alive in failed-supervisor mode to reap adopted children,
drain or reject controls, poll shutdown, and publish failed status rather than
exiting or booting a partial service set.

Exactly one service has `class=journal`. It must be an always-enabled,
dependency-free daemon with immediate readiness, and it cannot send its own
stdio back through `stdio=journal`. Every other enabled service must depend on
the journal transitively. Every enabled network provider uses IPv4 readiness,
and every enabled network consumer transitively depends on an enabled network
provider. These rules make journal-last shutdown and network readiness gating
properties of the validated graph rather than manifest ordering conventions.

### Default graph

The default manifest preserves the existing WOS paths and priorities:

| Service | Type/class | Priority | Readiness | Restart | Enablement | Direct dependencies | Stdio |
| --- | --- | ---: | --- | --- | --- | --- | --- |
| `journald` | daemon/journal | 10 | immediate, 5 s | never | always | none | inherit |
| `dropbear-keygen` | oneshot/normal | 0 | successful exit, 30 s | on failure | path missing: Dropbear RSA key | journald | journal |
| `netd` | daemon/network provider | 0 | nonzero IPv4 on `eth0`, 180 s | on failure | always | journald | inherit |
| `httpd` | daemon/network consumer | 10 | immediate, 5 s | on failure | always | netd | inherit |
| `dropbear` | daemon/network consumer | -5 | immediate, 5 s | on failure | always | netd, dropbear-keygen | journal |
| `testd` | oneshot/normal | 0 | successful exit, 5 s | never | disabled | netd, httpd, dropbear | inherit |

The common defaults are 250 ms initial and 30 s maximum backoff, five automatic
replacements per 60 s window, a 60 s stable-run reset, 2 s TERM, 1 s KILL, and
a 2 s output drain deadline. `testd` remains explicitly disabled; its readiness
dependencies replace the former fixed ten-second settle delay if an operator
enables it in a deliberate image variant or starts it explicitly for the
current boot.

## Lifecycle and readiness

The pure supervisor model uses these lifecycle states:

| State | Meaning |
| --- | --- |
| `DISABLED` | Administratively disabled or not selected for this boot. No process is owned. |
| `WAITING` | Desired, but at least one dependency is not ready. |
| `STARTING` | A new generation is being spawned or awaiting its spawn/exec result. |
| `RUNNING` | Exec succeeded; readiness has not yet completed. |
| `READY` | The generation satisfies dependents. |
| `STOPPING` | TERM/KILL/output-quiesce work is in progress. |
| `EXITED` | A successful oneshot, clean non-restarted daemon, or intentionally stopped service has no live generation. |
| `FAILED` | Start, readiness, process, budget, or drain failure. An ordinary quiesced failure may be retried by valid control; an abandoned generation may not. |
| `BACKOFF` | A restart is scheduled for a bounded future deadline. |

The public control ABI represents `READY` as a running service with readiness
`ready`; state and readiness are separate fields in `servicectl` output.

Supervisor invariants:

- A service starts only after all hard dependencies are ready. Loss of a
  dependency withdraws readiness and stops an active dependent; after
  quiescence it returns to `WAITING` without consuming restart budget.
- `immediate` becomes ready after exec success. `ipv4` begins a bounded probe
  only after exec success and becomes ready only after the named interface has
  a nonzero IPv4 address. A later pending result or probe error withdraws IPv4
  readiness, restarts its readiness deadline, and stops active dependents;
  another successful probe may restore readiness. `exit-success` becomes
  complete and satisfies dependents only after the oneshot exits successfully
  and its generation is quiesced.
- Enablement conditions are evaluated once when the model is initialized,
  without running shell code. A false `path-missing` condition on a oneshot is
  a successful skip, so the conditional keygen is already complete when the
  key exists. Other false path conditions and `enable=disabled` enter
  `DISABLED`. Start/restart controls do not override a false `path-exists` or
  `path-missing` condition in version 1. `start` may explicitly enable an
  `enable=disabled` unit for the remainder of the boot; `restart` alone does
  not provide that initial override.
- Every spawn attempt creates a new nonzero generation. Runtime events and
  actions carry both service identity and generation; stale-generation events
  must not change current state or target a newer process.
- Readiness, restart, TERM, KILL, and drain progress use monotonic deadlines.
  Wall-clock adjustment cannot lengthen them, and PID 1 must keep reaping,
  polling shutdown, receiving controls, and publishing status while deadlines
  are pending.

### Restart and failure accounting

`restart=never` does not schedule an automatic replacement. `on-failure`
restarts only failed generations. `always` also restarts clean daemon exits;
it is invalid for oneshots.

Eligible automatic replacements enter capped exponential backoff beginning at
`backoff-initial-ms` and never exceeding `backoff-max-ms`. The restart window
limits admitted replacements; for `restart=always`, clean daemon exits count as
well as failures. A new window resets the budget count, while remaining ready
for `stable-run-ms` resets both failure and backoff tracking. Backoff expiry
permits a new generation only if the service is still desired, dependencies are
ready, and shutdown has not begun. This prevents a crash loop from becoming a
PID 1 busy loop or an unbounded log producer.

## PID 1 ownership and stdio contract

The model alone cannot prove the following runtime properties. They are
mandatory PID 1 integration invariants and require live evidence:

- PID 1 is the single reap owner for every supervisor child. Each service
  generation records the service PID and process group, and group signalling is
  fenced by the current generation so a reused PID cannot be signalled.
- Spawn success and `execve` success are distinct. A bounded close-on-exec
  result channel reports child setup/exec errno; EOF means exec succeeded, and
  PID 1 processes this channel before reaping each tick. Exit status 127 is not
  sufficient evidence by itself.
- WOS placement remains `LOCAL` for service children while PID 1 retains
  `LOCAL | NOINHERIT`. Fork/exec setup must not expose a runnable child with the
  wrong inherited placement policy; failure to restore PID 1's flags after
  bounded retries fails the supervisor closed.
- `stdio=inherit` keeps the inherited descriptors, `stdio=null` attaches the
  null device, and `stdio=journal` routes stdout/stderr through a bounded owned
  drain. PID 1 owns that nonblocking drain directly, reads at most 4 KiB per
  service per tick, and does not create an untracked drain child. Journald
  itself never uses the journal route.
- A generation is not quiesced merely because its leader exited. The runtime
  must establish leader reap, exec-result resolution, owned process-group
  disappearance, and output EOF or forced close. Replacement never passes that
  generation-fenced `QUIESCED` barrier early.
- No service wait is unbounded. TERM is followed by KILL at the configured
  deadline. If the KILL deadline expires, the model orders output closed and
  starts the drain deadline. Expiry records `FAILED` with an abandoned
  generation: it does not fabricate quiescence, clear PID/PGID, or permit a
  replacement. During shutdown only, this explicit failure counts as settled
  so an unkillable generation cannot hang the machine forever. A later genuine
  `QUIESCED` event clears the retained identity. PID 1 continues nonblocking
  `waitpid(..., WNOHANG)` ownership throughout.

## Control and status

`servicectl` has this WOS-specific command surface:

```sh
servicectl status
servicectl status netd
servicectl start SERVICE
servicectl stop SERVICE
servicectl restart SERVICE
```

`status` is public. It prints supervisor state, kernel-assigned snapshot
sequence, service state/readiness, PID, process group, generation, restart
count, last wait status, and last error. Targeted status also prints at most
four ordered transition records, oldest first. A read before PID 1 publishes
its first complete snapshot fails with `EAGAIN`.

Start, stop, and restart submission requires effective UID 0. The kernel
authenticates the calling task and overwrites sender PID/effective UID; the CLI
is not the authorization boundary. PID 1 alone receives requests and publishes
status. The fixed mailbox holds 16 requests, so a full mailbox returns
`EAGAIN`; all four operations are nonblocking.

The line `accepted request=...` means only that the kernel enqueued a valid
request. It does **not** mean PID 1 recognized the service, began the action, or
completed it. There is no synchronous request/reply acknowledgment in ABI
version 1. Record the request ID, then observe `servicectl status SERVICE`, its
generation, the snapshot sequence, and transition history. An unknown but
syntactically valid service name can therefore be accepted by the broker and
later rejected by PID 1 policy.

ABI version 1 has no reload action. Editing `/etc/wos-services.conf` does not
mutate an already initialized graph; package the intended manifest and reboot a
test image when validating configuration changes.

Model-level control semantics are deliberately narrow:

- `start` is valid when the boot-time condition was met. It also explicitly
  enables an `enable=disabled` service for the remainder of the boot. It clears
  restart accounting and moves an exited, failed, disabled, or backoff service
  back to dependency waiting. Neither start nor restart can recover a
  generation while it remains abandoned; after a genuine `QUIESCED` event
  clears abandonment, normal control recovery is available again.
- `stop` clears desired-running and pending restart state. A waiting/backoff
  service exits immediately; an owned generation follows TERM, KILL, close,
  drain, and quiesce handling.
- `restart` also clears restart accounting, but an active generation must cross
  the quiesce barrier before its replacement can enter dependency waiting.
  Generations never overlap by model permission.
- Start/restart controls are rejected after shutdown begins. A stop of an
  already disabled unit has no effect.

During shutdown, new start/restart work must not bypass shutdown ordering.
Child exits, probe results, and drain completion are fenced against the service
generation they refer to; controls change current desired state but cannot make
a stale child event authoritative.

## Shutdown ordering

Shutdown follows the validated dependency graph in reverse. Dependents stop
before their providers, which puts httpd and Dropbear before netd. Conditional
oneshots are already quiesced or are stopped within their own bounds.

The model emits a non-journal completion barrier only after every non-journal
generation is genuinely quiesced or has reached the explicit bounded
`FAILED`/abandoned outcome described above. PID 1 then prepares kernel workers
and syncs filesystems while journald is still available. The journal phase
begins explicitly after that barrier. Journald receives the same bounded
TERM/KILL/quiesce-or-abandon treatment; journald is last. A final userspace
sync and the requested reboot, poweroff, or halt follow. Automatic restarts and
dependency starts remain suppressed throughout shutdown.

Use the graceful command path for testing:

```sh
shutdown -p now
```

Do not use `shutdown -f` for supervisor validation; force mode bypasses PID 1's
service ordering.

## Operator diagnostics

Start with the status snapshot and focused WOS journal views:

```sh
servicectl status
servicectl status netd
journalctl -u init -n 200
journalctl -u init_net -n 200
journalctl -u init -f
```

Useful interpretations:

- `waiting` with `not-ready` points first to an unmet dependency.
- `starting` or `running` with `not-ready` points to exec/readiness progress;
  for netd, correlate with the `init_net` IPv4 diagnostic dump.
- `backoff` is a bounded scheduled restart, not by itself a hung PID 1. The ABI
  contains the monotonic deadline, although the version 1 CLI does not print
  that field; correlate configured policy, history, and changing snapshot
  sequence.
- Repeated generations with increasing restart count indicate a crash loop;
  the count must stop when the budget is exhausted.
- `stopping` must progress through TERM/KILL/drain bounds. A generation change
  while old output is still owned is a quiesce violation.
- `failed` with the old PID/PGID still present after a drain timeout is the
  bounded abandoned outcome. It must not restart. Preserve this as failure
  evidence rather than interpreting shutdown progress as successful quiescence.
- A changing status `sequence` confirms PID 1 is still publishing complete
  snapshots. A fixed sequence plus a live VM warrants checking the serial log
  and PID 1 state.

For KTEST, inspect `ktest-data/serial-vm0.log` and
`ktest-data/qemu-vm0.log`. Cluster serial/QEMU paths are derived from the node
spec and printed during launch; the default names are `serial-vmN.log` and
`qemu-vmN.log`. Preserve the manifest, relevant status snapshots, journal
records, request IDs, and serial interval for every fault run.

## Rootless validation and live launch

Host checks require neither a VM nor root:

```sh
python3 tests/host/unit/service_manifest_config_source_test.py
python3 tests/host/unit/init_control_source_test.py
python3 tests/host/unit/shutdown_source_test.py
WOS_TEST_BUILD_DIR=/tmp/wos-init-tests scripts/test/run_tests.sh build
ctest --test-dir /tmp/wos-init-tests \
  -R '^(init_supervisor_test|service_manifest_config_source_test|init_control_source_test|shutdown_source_test)$' \
  --output-on-failure
```

The source checks prove bounded grammar/config packaging and control-broker
contracts. `init_supervisor_test` exercises the parser, graph, generations,
fake-time lifecycle, restart, readiness, quiesce, control, and shutdown model.
They do not prove actual WOS fork/exec, process-group signalling, wait status,
pipe drainage, IPv4 ioctl behavior, or filesystem shutdown.

If the host bridge/TAP/shared-memory topology already exists and is accessible
to the current user, launch the isolated diagnostic VM without privileged setup:

```sh
bin/wos-ktest --no-setup
timeout 120s tail -n 200 -f ktest-data/serial-vm0.log
```

After a successful isolated build/package, a repeat launch can avoid rebuilding:

```sh
bin/wos-ktest --no-build --no-package --no-setup
```

For normal WOS build artifacts and an already prepared topology, the checked-in
single-node self-host profile keeps supervisor observation bounded:

```sh
bin/wos-cluster --config configs/cluster_selfhost.json --launch --no-setup
scripts/remote/wos_ssh.sh wos-0 servicectl status
scripts/remote/wos_ssh.sh wos-0 journalctl -u init -n 200
```

The same rootless launch path supports live debugging. These forms pause the
selected VM at startup with its GDB stub enabled; use the GDB endpoint printed
by the launcher:

```sh
bin/wos-ktest --debug-node --no-setup
bin/wos-cluster --config configs/cluster_selfhost.json --launch --debug-node 0 --no-setup
```

`--no-setup` does not create or repair topology and does not grant access to
pre-existing TAP or ivshmem objects. A missing/inaccessible topology is a host
precondition failure, not supervisor evidence. Rootless launch also does not
authorize teardown; coordinate cleanup with the topology owner.

For a self-contained evidence bundle, choose a new output path:

```sh
bin/wos-ktest --no-setup \
  --incident-output /tmp/init-supervisor-ktest.wosincident
bin/wos-cluster --config configs/cluster_selfhost.json --launch --no-setup \
  --incident-output /tmp/init-supervisor-cluster.wosincident
```

## Bounded fault campaign

Run manifest-changing cases only in an isolated disposable worktree/image. Use
short but valid deadlines for a fault image so the campaign is bounded; record
the exact manifest with the evidence. Restore the normal default before judging
a production boot. Never infer live success from the host-model column.

| Fault | Bounded injection and pass condition | Host/model evidence | Live evidence required |
| --- | --- | --- | --- |
| Missing executable | Give one disposable service a safe absolute `exec` path that does not exist. Use a small restart budget and backoff. Exec failure must be distinct from exit 127; generations back off and finish `failed` when the budget is exhausted, while PID 1 keeps publishing. | Parser accepts only a safe path; fake spawn/exec failure proves generation-fenced backoff and exhaustion. | Serial/journal exec error, bounded generation/restart sequence, continuing status sequence, no zombie or surviving process group. |
| Crash loop | Use a disposable daemon that exits nonzero immediately with `restart=on-failure`, a two- or three-attempt budget, and capped subsecond backoff. It must never spin continuously or overlap generations. | Fake clock proves exponential cap, window accounting, budget exhaustion, and stable-run reset. | Per-generation PID/PGID/status, observed delay between starts, final failed state, bounded journal volume, and no orphan/drain leak. |
| Delayed IPv4 | Hold DHCP/IPv4 assignment for less than the test readiness timeout, then release it. netd stays not-ready and consumers stay waiting; one readiness event releases them. | Probe pending/ready events prove dependency gating without wall-clock sleeps. | netd status before/after address assignment, consumers absent before readiness and present afterward; an `init_net` failure dump is expected only if the timeout fires. |
| Missing IPv4 | Keep the test interface unconfigured beyond a shortened valid readiness timeout. The generation times out, quiesces, and follows bounded restart policy; consumers never start. | Fake time proves readiness timeout, stop actions, backoff, and dependency cascade. | `init_net` timeout diagnostics plus TERM/KILL/drain evidence, no httpd/Dropbear generation, bounded retries, responsive PID 1. |
| Control/drain race | On a journal-routed daemon, submit restart, stop, and start while output is active. Each accepted request remains asynchronous; an old generation must quiesce before replacement. Stop observing after the configured TERM + KILL + drain bounds per generation. | Control events and stale-generation cases prove deterministic desired-state, `QUIESCED` fencing, and non-restartable abandonment. | Request IDs, snapshot sequences/history, process-group disappearance and pipe EOF/forced-close evidence in the normal case; if the drain deadline is deliberately exhausted, retained PID/PGID, failed state, no replacement, and continued PID 1 progress. |
| Shutdown during transition | Begin graceful `shutdown -p now` separately while a service is starting, in IPv4 readiness, in backoff, and stopping. No new generation may escape shutdown. Non-journal completion precedes sync/journal stop. | Fake-time shutdown cases prove reverse DAG, cancellation of restart work, TERM/KILL/drain deadlines, and quiesced-or-abandoned completion barriers. | Ordered journal/serial trace showing consumers before provider, normal group/drain quiescence or an explicit bounded abandoned failure, filesystem sync with journald alive, journald last, and final power action. |

For every row, a pass requires monotonic progress within configured deadlines,
continued PID 1 reaping/control/status activity, no duplicate live generation,
no stale PID/group signal, and no dependency launch before readiness. Normal
completion also requires no zombie or orphaned drain. A deliberately exhausted
shutdown/drain case instead requires the explicit non-restartable abandoned
failure with its identity retained for diagnosis. VM logs and status history
are required before marking the live portion complete.
