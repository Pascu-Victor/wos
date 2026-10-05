# Goal 19 native unmount export transition

9LY demonstrated that ordinary owner unmount left an automatic peer export
resolving the same text through underlying and replacement filesystems. 9LZ
connects public native unmount to the existing export reconciliation protocol.

`vfs_umount` authorizes one captured Snapshot and copies the target before any
blocking drain. Under namespace publication it captures the selected mount's
unique dev ID and whether it is native, then releases the guard and reference.
For native mounts with WKI active it closes VFS admission and drains admitted
binding work before attempting exact-ID unmount. Successful removal erases only exports matching the captured backing mount
ID, retires unmatched owner bindings and queues exact incarnation
withdrawals before admission reopens. Busy or failed removal cancels the
transition and reconciles the unchanged export set. The actual unmount errno
is preserved. A competing transition can refuse before commit with EBUSY.

Cancellation uses the preexisting revision advance: snapshots captured before
the transition are stale and must be reacquired. Matching backing identities,
channels and issued Files survive. Pending grant preparation has its existing
revision validation; this change does not qualify every transfer/rotation case.
Remote proxy unmount stays on its exact-group teardown path and never drains
its own WKI worker. Shutdown keeps the existing service-stop/unmount path.

Successful unmount does not rediscover export names through the caller root.
Normal discovery can later admit a fresh backing mount under its proper name.

No registry, mount or namespace lock, nor a mount reference needed by teardown,
spans the drain. Existing target dev-ID matching prevents removing a same-path
replacement. No syscall/wire representation, credential policy, readonly/nosuid
policy or declared LOCAL `/` routing changes.

The native regression runs before WKI initialization with real native mounts
and the real export table. A held File causes EBUSY, cancel preserves export
incarnation/backing, successful removal retires its export while preserving an
unrelated custom alias, and a fresh mount receives a different backing and incarnation.
It checks closed/reopened admission and restores its temporary state. Source
guards connect the public syscall's authorization/copy/drain/commit/finish order.

The ordinary live fixture holds a remote File across busy unmount, verifies
its continued use and an unrelated export, then closes it and waits boundedly
for asynchronous close to drain. It requires disappearance of the old exact
peer row and a new peer mount identity after replacement. Broader `/tmp`
fallback after withdrawal is allowed; reading through it is not evidence of
an old binding surviving. The underlying owner marker must remain unchanged.

Final results are recorded in `build/goal19-native-unmount/validation.json`. Goal19 remains active; this bounded lifetime fix
does not qualify all traversal, mount or WKI operations.


## Final validation

Final isolated diagnostic KTEST passes59759/0 at466.521s, including the native busy/cancel/commit/custom-alias regression; automatic poweroff and launcher exit0. Host audit at2026-09-16T02:57:27.660620+00:00 confirms all owned VMs down.

Normal Build WOS (RelWithDebInfo) and isolated diagnostic (Debug) builds passed.
Formatting/clang-tidy and focused source checks passed. Normal build ID is
06b3301d14b88ab8b1936c6fe9492989607172d9; diagnostic build ID is
4e460327f63666a5389bf9d70421ff7feba84b9e. Packaged kernel hashes match.

All20 new live unmount checks and24 mount-cover checks pass. Old resource4103
is withdrawn; replacement is4104. The established traversal matrix has1567/1568
passing assertions: one retained-directory STAT_AT returned ETIMEDOUT rather
than success. Failure, both serial logs and WOSDBG reconstruction are retained
in matrix-timeout/. Only the six previously unexecuted cwd cases were run
after that failure, without replaying failed assertions. The missing reply's
cause remains unproven; owner send status is discarded, which needs separate
investigation. Legacy logs cannot establish a global cross-node order.

Both nodes' scratch inventories are restored and synced, live symbols match
before/after checks, debugger sessions close and the normal launcher exits0.
The exact owned cluster is confirmed down at2026-09-16T02:36:51.071635+00:00.
All live runs use --no-setup, private overlays and isolated diagnostic disks.

The first full-rediscovery candidate was superseded after source review found
caller-root/custom-alias instability. Its59759/0 run remains historical.
An exact-retirement candidate then passed19+24+1568 live checks, but was replaced
to preserve preexisting empty-target errno; its incomplete KTEST was deliberately
stopped. A fixture-only SFTPAttributes.st_dev failure was corrected using native
stat and retained with successful cleanup. These runs do not qualify final code. The next diagnostic run reported one
early-task test error: public unmount returned ESRCH before a current task
exists. That misplaced assertion was removed; ordinary live empty-target errno
coverage remains. The failure and full run are archived in early-task-test-failure/.
Only the diagnostic test changed; normal builds exclude test sources.
Goal remains active,12enforced/9pending, with vfs-traversal first pending.
