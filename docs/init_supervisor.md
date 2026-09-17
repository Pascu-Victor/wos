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
user=root
group=root
supplementary-groups=none
umask=0022
authority-ceiling=65535
no-new-privileges=no
service-privileges=none
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

Fields marked `1` in the table are required exactly once. Conditional and
optional fields follow their listed cardinality; `arg`, `env`, and `depends`
are the only repeatable fields.

| Key | Cardinality | Accepted value or bound | Meaning |
| --- | --- | --- | --- |
| `user` | 1 | Account name, at most 31 bytes | Real, effective, and saved UID resolved from `/etc/passwd`. |
| `group` | 1 | Group name, at most 31 bytes | Real, effective, and saved GID resolved from `/etc/group`. |
| `supplementary-groups` | 1 | `none` or at most 8 comma-separated group names | Exact replacement, with no inherited or implicit membership. |
| `umask` | 1 | Three or four octal digits, 0000–0777 | Child creation mask. |
| `authority-ceiling` | 1 | Decimal 0–65535 | Restricts the existing 16 authority domains and bounds explicit service grants. |
| `runtime-state` | 0 or 1 | `none` (default), `netd` | Prepare fresh owned resolver state; at most one network provider with NNP, grant2 and ceiling4096. |
| `private-state` | 0 or 1 | `none` (default), `journal`, `keygen` | Validate and prepare a reserved flat namespace before services start; each state requires its narrow service profile. |
| `service-privileges` | 1 | `none`, `journal-read`, `network-admin`, or both comma-separated | Fixed, sealed operation grants; duplicate or unknown names reject startup. |
| `no-new-privileges` | 1 | `yes`, `no` | Irreversibly suppress set-ID privilege gain on exec. |
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
| `limit-cpu` | 0–1 | `soft:hard`, seconds | Per-process CPU limit and persistent service CPU ceiling. |
| `limit-fsize` | 0–1 | `soft:hard`, bytes | File endpoint limit. |
| `limit-stack` | 0–1 | `soft:hard`, bytes | Automatic stack backing bound. |
| `limit-core` | 0–1 | `soft:hard`, bytes | Complete binary core size. |
| `limit-nproc` | 0–1 | `soft:hard`, tasks | Node real-UID population bound, including threads. |
| `limit-nofile` | 0–1 | `soft:hard`, at most `256:256` | Descriptor-number bound. |
| `limit-as` | 0–1 | `soft:hard`, bytes | Occupied user virtual address-space bound. |

Limit values are unsigned decimal integers or `infinity`; internal spaces,
negative values, overflow, duplicate fields and soft greater than hard are
rejected. Defaults are `256:256` for NOFILE and `infinity:infinity` for the
others. For example, `limit-nofile=32:64` and `limit-as=134217728:134217728`
allow 32 descriptors and 128 MiB of user virtual space. Zero is accepted, even
when it prevents the configured executable from starting.

PID 1 configures fixed kernel service slots once, then selects a slot around
fork. Kernel inheritance applies these limits before child publication and
exec; PID 1 keeps its own policy. Descendants inherit the process limits and
service affiliation. Ordinary soft/hard privilege rules still apply, while
the configured service CPU ceiling cannot be raised by a service process.
Each CPU accounting event also charges that persistent service record. CPU
exhaustion therefore survives exec, automatic or manual restart, stable-run
resets and restart-window rollover. An exhausted hard CPU ceiling refuses a
new generation with `EDQUOT`; resetting the service counter requires reboot.
These are service defaults and a CPU ceiling, without arbitrary process-group
quotas. See [resource governance](resource_governance.md) for each resource's
enforcement and observation semantics.

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
| `journald` | daemon/journal | 10 | immediate, 5 s | never | always | none | null |
| `dropbear-keygen` | oneshot/normal | 0 | successful exit, 30 s | on failure | path missing: Dropbear RSA key | journald | journal |
| `netd` | daemon/network provider | 0 | nonzero IPv4 on `eth0`, 180 s | on failure | always | journald | null |
| `httpd` | daemon/network consumer | 10 | immediate, 5 s | on failure | always | netd | null |
| `dropbear` | daemon/network consumer | -5 | immediate, 5 s | on failure | always | netd, dropbear-keygen | journal |
| `testd` | oneshot/normal | 0 | successful exit, 5 s | never | disabled | netd, httpd, dropbear | inherit |

The common defaults are 250 ms initial and 30 s maximum backoff, five automatic
replacements per 60 s window, a 60 s stable-run reset, 2 s TERM, 1 s KILL, and
a 2 s output drain deadline. `testd` remains explicitly disabled; its readiness
dependencies replace the former fixed ten-second settle delay if an operator
enables it in a deliberate image variant or starts it explicitly for the
current boot.

All services must declare the seven identity fields above. Version 1 manifests
without them fail closed. Before configuring resources or launching any service,
init opens the manifest and both account files with `O_NOFOLLOW` and checks that
they are regular, root-owned, and not group/world writable. Each account file is
limited to 16 KiB, 512 bytes per line, and 32 entries. Passwd/group fields and
numeric IDs must be valid; duplicate names/IDs, unknown members, missing primary
groups, and unresolved declared accounts reject startup. Account files end with
a newline. User and group names begin with an ASCII letter or underscore and
continue with letters, digits, underscores, or hyphens.

After fork, the child creates a new session and reports readiness over the
private result pipe. It waits for PID 1 to verify SID=PGID=child PID and send one
ACK byte before proceeding. PID 1 polls this handshake without blocking and
never races the child with parent-directed setpgid. The child then establishes
placement, priority and standard streams, applies NNP, installs and seals the
declared service privileges, supplementary groups, all three GIDs, all
three UIDs, umask, and the authority ceiling. It verifies the final values before
exec. WOS privileged `setgid`/`setuid` set all three IDs; init uses these
supported calls and checks the resulting saved IDs explicitly. Any failed step
reports an identity-stage error through the existing exec
pipe and exits without launching service code. PID 1 keeps its credentials.
Nonroot service declarations require NNP and a ceiling exactly matching the
service privileges: journal-read requires 16384, network-admin requires 4096,
both require 20480, and none requires zero. Root declarations with privileges
also require NNP and the matching ceiling bits. NNP services seal their grant
set, including an empty set, before dropping IDs. Legacy root services with NNP
`no` and privileges `none` skip installation. Initial nonzero installation
requires root INIT_CONTROL and the matching root authority; later requests can
only reduce the sealed set. Reducing the ceiling disables the matching grant.
Journal-read does not grant access to other kernel diagnostics.

The shipped HTTPD uses locked UID/GID 100 (`httpd`), no supplementary groups,
umask 0077, an empty environment, null standard streams, NNP, and zero authority.
Its health/info and permitted file reads remain available; `/api/mount` returns
403 before creating directories or launching a process, and listings omit the
mount form. Root-owned private files remain protected by filesystem permissions.
Journald uses locked UID/GID101, NNP, journal-read, ceiling16384 and private state
prepared by init as described below. Netd uses locked UID/GID102, NNP, sealed
network-admin, ceiling4096, umask0077, no supplementary groups, empty environment
and null streams. Key generation uses locked UID/GID103 (`keygen`), no
supplementary groups, umask0077, NNP, a sealed empty grant set, zero authority and
an empty environment. Dropbear and disabled testd still declare root/root,
umask0022, full authority and NNP disabled; their reductions remain pending.
Root and the four service accounts are generated deterministically; service
accounts use `/nonexistent` and `/bin/false`.

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
  generation records the service PID and process group. Before session admission,
  cleanup targets only the child, which cannot yet exec service descendants.
  After admission, group signalling is fenced by the current generation and
  verified process group.
- Spawn success and `execve` success are distinct. A bounded close-on-exec
  result channel first carries session readiness, then child setup/exec errno;
  EOF resolves exec success only after session admission. PID 1 processes this
  channel before reaping each tick. Missing, duplicate or invalid readiness fails
  startup. The ACK writer is nonblocking and retains a read endpoint until its
  one-byte write completes, preventing SIGPIPE if the child dies concurrently. Exit status 127 is not
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

For root with retained `RESOURCE_LIMIT` authority, targeted status also displays configured limits, current usage,
peaks, denial counts and reasons. CPU totals include every affiliated process
and previous generation. Other rows show the maximum current per-process
observation, lifetime maximum peak and summed denial counters. Those counters
survive process destruction and restart; NPROC observations retain their real-UID
meaning. Nonroot callers retain access to the original public status/history.
The resource snapshot uses an additive version-1 ABI, independent of the
unchanged init-control snapshot. A status/history read and its resource read
are individually coherent observations taken at separate times.

Start, stop, and restart submission requires effective UID 0 and retained
`INIT_CONTROL` authority. The kernel uses one immutable credential snapshot to
authorize the request and stamp its effective UID, and stamps the process PID; the CLI
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

The disposable `tests/fixtures/service_identity_services.fragment` runs
`testprog credentials service-policy` under the HTTPD identity, including a
root-owned set-ID copy at `/usr/bin/identity-setid-probe` (mode 06755).
Create that copy only in the disposable VM image before appending the fixture.
Both probes must report zero failures and successful one-shot readiness. They
check the final IDs, groups, umask, NNP and authority ceiling, restoration
denials, mount/init-control denial, private key protection, and NNP suppression
of set-ID exec. The fixture is not part of the normal service manifest.


Journald now declares `private-state=journal`, locked UID/GID101 (`journald`),
NNP, journal-read only, ceiling16384, umask0077, empty environment and null stdio.
The optional private-state selector also accepts keygen, described below; journal
requires the unique journal class and this narrow privilege profile. Images
stage the private directory as root-owned0700. Before configuring resources or
starting any service, trusted init walks root/var/log without symlinks or
writable parents, validates the retained journal directory and its complete
flat namespace, and transfers active/archive ownership to the declared account.
Only wos.journal and wos.journal.1 are permitted. Existing files must be regular,
0600, single-link and root:root or already owned by the service; the directory
must be0700 with one of those owners. Files transfer before the directory.
Denied preflight leaves ownership/content unchanged; operational failure rolls
back prior ownership changes and aborts startup (EIO if rollback also fails).
This is an exclusive startup operation, not a concurrent or crash-atomic chown.
Unknown entries, links, special files and foreign ownership fail startup.
Existing state contents are preserved, and the daemon creates new files only
after init has permanently dropped its credentials. The host staging tree is
never chowned. Dropbear and disabled testd await their own drops.


### Netd runtime resolver state

Netd declares `runtime-state=netd`. After all account resolution and before any
service launch, init validates root-owned, non-writable root/run/etc ancestors,
reads the bounded regular root-owned `/etc/netd-resolv.conf` seed without following
symlinks, and creates a fresh `/run/.netd-init` staging directory. It writes the
seed, assigns file mode0644 and directory mode0755 and UID/GID102, then publishes
`/run/netd` by rename. Preexisting staging/final paths reject startup; failures
remove only newly created state. This is exclusive early startup, not a general
concurrent migration facility. `/run` is tmpfs; supervisor restarts retain the
state, and each boot creates it anew. The default seed contains resolver options
only, matching the previous initial configuration.

The root-owned `/etc/resolv.conf` symlink points to `/run/netd/resolv.conf`.
Netd validates its owned directory and existing regular, single-link file,
creates its temporary file exclusively without following symlinks, checks writes,
sets mode0644, then renames through the retained directory descriptor. A stale
owned, single-link regular temporary file can be discarded after interruption;
symlinks, hardlinks, unexpected permissions and special files are rejected.
Unrelated users can read the published resolver but cannot replace it or write
in `/etc`. Network administration is the daemon's only delegated control grant.


Host-key generation declares `private-state=keygen`. Only one service may own
this state, and its profile must be a normal oneshot with NNP, no grants,
ceiling0, no supplementary groups, umask0077 and an empty environment. Init
resolves the locked keygen account before preparing `/etc/dropbear` through
retained nofollow descriptors. Root and `/etc` must be root:root directories
without group/world write access. The complete bounded key namespace may
contain only `dropbear_rsa_host_key` and its optional `.pub` file, each regular,
0600, single-link and owned by root:root or keygen. Unknown entries, links,
special files, unsafe permissions and foreign ownership abort startup.

Full and incremental images stage the key directory root:root0700 without
changing existing keys. Init transfers its allowed files before the directory.
A legacy root:root0755 directory is tightened only after complete preflight;
failed ownership migration restores earlier ownership and the directory mode,
returning EIO if rollback itself fails. An already keygen-owned directory must
be0700. No service starts before state preparation succeeds. This is an
exclusive startup transaction, not a concurrent or crash-atomic migration.

Real `dropbearkey` runs after irreversible identity restrictions and creates
private files within its owned directory. The existing-key enablement rule,
RSA arguments, readiness dependency and journal output remain unchanged. The
key generator receives no kernel authority; the login server still requires a
separate design for authenticated user switching and session authority.

Disposable validation uses `service_keygen_state.cpp` with init's actual state
adapter for preflight, legacy-mode migration, denied ownership rollback and
content preservation. `service_keygen_policy.c`, installed root:root06755 as a
temporary keygen executable, checks21 restrictions before executing the real
key generator. `service_keygen_boot.py` retains the original key and digest
inside the guest, stages missing-key generation, restores the production
manifest/key, and verifies unchanged contents across restart. None of these
fixtures is installed by the production manifest. Native validation passed15
state checks and21 identity/denial checks, generated a new RSA key, restored
SSH availability, and preserved the original key across the production reboot.


Session-isolation qualification: Normal Build WOS passed. The host runtime
harness executes the actual init admission/cleanup functions with real pipes
and controlled process/model boundaries; all 12 scenarios passed. All 42 host
supervisor/account/private-state tests also passed. In a disposable live VM,
journald, netd, HTTPD and Dropbear each had SID=PGID=their own PID. Four HTTPD
replacements and a stop/start cycle preserved those identities and returned PID
1 to its original descriptor set. The keygen oneshot was already satisfied and
testd was disabled in that run; their session startup was not separately
exercised. These service boundaries preserve the POSIX same-session SIGCONT
exception while denying the exception across isolated root/nonroot sessions.
