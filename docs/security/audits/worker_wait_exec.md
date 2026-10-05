# Goal19 9NV: retained wait commit across local exec

Previous turn: progress;9NU qualified whole-process exit of a native waiter.
This stage qualifies the surviving-owner path using ordinary local exec.

Problem: live evidence must establish that retiring an old-image waiter does
not abandon its requested commit or lose/double-charge the live owner's child CPU.
Expected files: termination fixture/driver, goal and evidence docs/index.
Files read: native exec preparation/publication/finish, conditional credential
quiescence, sibling retirement; resource ExecSafeWait; Task CPU fields;
child-query close/task retirement/COMMIT worker and live CPU import; existing
termination fixture and its exact-VM debugger driver.
Flow: source child exits after bounded CPU work; migrated parent starts waiter;
pause source at exact commit entry; main receives same-UID USR1 and locally execs
the already uploaded helper; worker retires, canonical owner survives; new image
publishes a local ready marker and waits; inspect detached COMMIT and owner;
resume source, verify exact imported CPU delta and record retirement; release
replacement to check ECHILD/no extra accounting and exit23; source parent reaps.
Invariants: existing SIGKILL/process-exit modes preserved, stable owner/worker
identities and credential checks, exact configured VM/ELF identity, read-only
debugger state except hardware breakpoint, no protocol mutation or new kernel
API. Only owned helpers/markers touched. Error paths resume both guests.
Locks/unsafe contexts: no production changes; query/Task reads while guest paused.
Allocation/blocking: host/userspace only; bounded marker polling and child work.
ABI/wire/syscall impact: none. Build/test: fixture build/format/syntax and relevant
source/policy checks; live UID0/104; exact receipt/accounting, process/queue cleanup
and owned shutdown. Kernel build/KTEST reused only if hashes match9NT.
Rollback: build/goal19-worker-wait-exec/before. Remaining scope includes hard CPU,
concurrent usercopy, peer restart/mixed versions and full pending-domain audit.

## Results

The final fixture passes UID0 and UID104. Both distinct old-image workers retire
while the source VM remains paused and the canonical owner runs its new local
image. Read-only query inspection finds COMMIT still active, detached=true,
retiring=false, owner exit=false, and a new owner pagemap while the worker retains
its old root. Imported CPU counters are unchanged before the source resumes.

After resumption, the continuation retires and the held replacement owner has
exactly the claimed child CPU delta: UID0 user123953/system23776 microseconds;
UID104 user121259/system24157 microseconds. Both baselines were zero. The new
image receives ECHILD from three subsequent waits, observes unchanged child CPU
ticks across those calls and exits23; both source controllers reap status5888.
Observed worker-exit intervals331.45/330.68ms include debugger/SSH overhead and
are not kernel latency measurements. Both helpers and both owned marker
directories with their six files are removed; all8 final process probes are absent.

Two earlier fixture attempts remain preserved. Attempt1 reached successful root
exec/retirement and exact CPU import, then Paramiko rejected writing an exclusive
file opened with mode x; wx supplies write access. Attempt2 passed the complete
root case but UID104 produced no ready marker. Read-only inspection established
root-owned0755 /tmp on both guests; the fixture wrongly required UID104 to create
its marker there. Final setup creates a private0700 marker directory owned by
each case's UID/GID. The missing-marker run did not capture that process's exit
status, so no specific kernel errno is claimed from it. Neither failure is counted
as full qualification. Original sources, logs and partial observations are retained.
Both failed-attempt helper-name process scans were empty; known receiver PIDs were
absent, and remaining helper/marker files were checked/removed. Final inspection
includes all known failed-attempt identities and finds every CPU dead list empty,
no active child-query/claim records and empty child-signal/proxy-exit queues on
both nodes. Unknown failed-attempt source PIDs are not invented.

Fixture build/format/syntax, waitpid/exec source guards and policy audit/unit
checks pass. An initial formatting refusal was corrected and preserved. Kernel
sources and normal/diagnostic artifacts match9NT; no new kernel build or KTEST is
claimed. Both guests synced/stopped and no WOS VM remained at
2026-09-16T23:56:55.713920+00:00. Evidence and stage patch:
build/goal19-worker-wait-exec/validation.json.

The live surviving-owner detached commit/accounting requirement is now qualified
for local main exec at UID0/104. Hard CPU expiry, concurrent usercopy, live peer
restart/mixed versions and the whole signal-domain audit remain separate work.
Goal19 stays17 enforced/4 pending. No production, credential, syscall/wire ABI,
peer policy or declared VFS routing changes were made.
