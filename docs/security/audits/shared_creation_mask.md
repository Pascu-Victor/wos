# Goal 19 process-wide creation mask — 9ME

Ordinary pthread file creation reproduced a stale-mask bug on both TMPFS and
XFS. After the main thread changed its mask from `0000` to `0077`, an existing
worker still created a `0666` file and a `0777` directory. The worker's later
`0027` change did not affect the main thread. Baseline receipts identify kernel
`06b3301d14b88ab8b1936c6fe9492989607172d9` explicitly.

The mask now belongs to the retained process owner. `Task::creation_mask()` and
`exchange_creation_mask()` use a bounded atomic value; threads no longer copy a
private mask. Relaxed atomic operations suffice because this value publishes no
other data. An exchange returns one exact preceding value. An unbound thread
reads `0777`, while its runtime setter returns `ESRCH` without mutation.

The runtime setter holds the existing VM publication guard and rejects a worker
whose process has replaced its image. The existing retirement helper queues that
worker's exit; `ECANCELED` leaves the new image's mask untouched. Trusted private
fork/spawn/receiver construction uses the atomic accessor directly. Each new
process independently inherits one value, and ordinary exec preserves it.

VFS open, mkdir and metadata-batch creation sample the canonical mask once
before dispatch/retry. The extra mask application in `vfs_open()` was removed;
`open_at_for_task()` applies it. Explicit owner helpers continue to consume the
already-masked mode. One admitted credential Snapshot still supplies the IDs and
authorization decision. No credential, syscall-number or WKI record layout was
changed. Declared VFS routing, including explicit LOCAL `/`, is preserved.

The changes are confined to the Task mask representation/thread constructor,
process mask syscalls and child construction, spawn, WKI identity transfer, VFS
mask consumers, and focused tests. The source audit's root-anchor-before-mask
check now names the accessor instead of the removed assignment; its ordering
requirement is unchanged.

## Verification

Normal `Build WOS` and isolated diagnostic builds passed. Scoped formatting and
clang-tidy checks passed with existing nonfatal warnings. Credential policy and
policy-unit checks passed, as did the selected exec, remote-compute, scheduler,
VFS lookup, open-flags and open-path-storage source checks.

The final normal kernel is `de45f6f702b0826c25af9530177ba412c364a967`.
Both private cluster boot overlay chains were checked against the same final
image. All seven live invocations passed:

| Execution and storage | Checks |
| --- | --- |
| Source-local TMPFS and XFS | Existing worker sees `0077`: file `0600`, directory `0700`; worker sets `0027`, parent file becomes `0640`; fork and spawn/exec inherit independently. |
| Source execution, routed receiver TMPFS and XFS | The same checks plus metadata-batch creation at `0600`. |
| Receiver execution, receiver TMPFS and XFS | The same thread, fork and spawn/exec checks after remote execution. |
| Remote exec inheritance | A source `0077` mask reaches the receiver image. |

Every fixture cleaned its own objects; both guests completed `sync` before
verified QMP shutdown. The test uses ordinary process and filesystem APIs and
the existing authorized SSH agent. Its source is
`tests/fixtures/service_shared_umask.cpp`; the optional `--batch` argument is for
a routed path supporting metadata batches. A unique path prefix is required.

The diagnostic tests exercise canonical owner/sibling sharing, bounded masks,
main-thread retirement, orphan refusal, and actual runtime setter refusal after
image replacement. The existing WKI filesystem-context test also checks mask
encoding for owner and worker, both transfer forms, receiver installation and
independent receiver mutation. The final isolated suite passed **59,783 assertions, zero failures** at
475.552s, then powered off; the launcher exited zero. The diagnostic kernel is
`bcdb9bd91476ef2ac838f8af966bcc45f1f0bbef`. Exact process checks confirm
all task VMs are down. Receipts are in
`build/goal19-shared-umask/validation.json`.

## Preserved failures and limits

The first single-node launch rejected a removed peer named by the copied
configuration. An initial SSH harness attempt used an unsupported Paramiko
context manager and failed before connecting. Initial formatting checks found
line-layout errors, and an unsuitable unittest-discovery invocation ran zero
tests; direct test-script invocations supplied the actual source checks.

The first fixed candidate passed native live checks. Its diagnostic run was
intentionally stopped after the exec-publication review required the final
guard; it is not a full-suite pass. KTEST's global launcher lock also refused two
overlapping launch attempts before the cluster had exited. Those launch failures
remain separate from kernel test results.

The first two-node run retained an old per-node boot override for node 3. Its
remote-thread failures reproduced the baseline bug; updated-source routed
creation passed. The failed receipt explicitly records the mixed versions.
Corrected fresh overlays produced the final passing matrix. An initial stop
attempt assumed PID/node launch order; its ownership assertion refused to send
any QMP command. Shutdown then used actual command lines and start ticks.

This change closes the demonstrated shared-mask gap. It does not by itself
qualify the whole open/create domain or erase earlier transport timeouts.
Goal 19 remains active with 13 enforced and 8 pending authority domains.
