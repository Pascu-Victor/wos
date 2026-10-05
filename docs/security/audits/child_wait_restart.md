# Goal19 9NW: receiver restart during child-wait COMMIT

Previous turn: progress. 9NV qualifies a surviving owner's detached completion.
The current signal audit still requires live exact-session retirement evidence.

Problem: deterministic session tests do not establish live cleanup when an
accepted receiver disappears while holding a remote child-wait COMMIT.
Expected files: termination fixture/driver, goal and evidence documents/index.
Files read: waitpid claim/copy/commit loop; child-query session retirement,
upstream termination and continuation retirement; proxy finalization/deferred
exit and child-event exit preparation; ordinary receiver-restart fixture and
netdiag boot-epoch snapshots; current signal recipient authorization and tests.
Flow: source child exits -> parent migrates -> pause source at receiver commit
entry -> observe COMMIT -> QMP reset exact owned receiver -> resume source ->
observe new authenticated boot epoch and fresh mounts -> source parent reaps
normal255 failure completion -> inspect surviving source claims/tasks -> fresh
wait and credential-changing signal controls after reconnection.
Invariants: no packet mutation, no credential/policy weakening, exact VM process
identity before reset, no invented rollback of an uncertain commit, old receiver
PID absence is not inferred from numeric PID after reboot. Both guests resume on
error. Only private disks/VMs and owned process fixtures participate.
Locks/contexts: production unchanged; debugger reads on paused exact guest only.
Allocation/blocking: host/userspace; bounded reconnect/parent completion waits.
ABI/wire/syscall impact: none. Build/test: fixture compile/format/syntax,
source/policy checks, UID0/104 resets with boot-epoch witnesses, fresh wait and
signal authorization controls, final retained-task/queue inspection and shutdown.
Reuse unchanged9NT kernels only after hash verification; no new KTEST claim.
Rollback: build/goal19-child-wait-restart/before. This is receiver restart of an
outstanding child wait, not mixed-version signaling or every peer-loss boundary.

## Results

Both ordinary reset cases pass on their first run. UID0 receiver106 and UID104
receiver102 belong to distinct VM incarnations, each observed in COMMIT with its exact
query/Task before reset. Source peer boot epochs change2712109567 ->3293388347
->127057653 with authenticated CONNECTED state and fresh active root mounts.
The source VM and its process start identity stay unchanged. Both source
controllers reap normal255 (raw status65280), and all four source parent/child
probes are absent. No assertion treats an old receiver's numeric PID as an
identity after reboot. The reset ends that incarnation; numeric PIDs can be
reused. No claim is made that an uncertain source COMMIT was rolled back.

After both resets, all26 wait-semantics checks pass at UID0/104: positive PID
collision EOVERFLOW, wait-any, WNOWAIT, group selection, direct kernel EFAULT and
CPU observation. All12 local/remote signal identity cases pass after target UID
changes, including signal zero, allowed/denied USR1 and session-aware CONT with
unchanged denial effects. Every direct child is reaped. Final independent procfs
checks find all30 recorded physical task identities and all six helper paths
absent. Read-only inspection finds empty dead lists on all four CPUs of both
nodes, no active child-query/claim records, and empty child-signal/proxy-exit
queues. Both guests were synced and stopped; no WOS VM remained at
2026-09-17T00:05:49.605147+00:00.

All three fixture builds, format/syntax, waitpid/WKI source checks and policy
audit/unit checks pass. No kernel source changed; normal and isolated diagnostic
hashes match9NT. No new kernel build or KTEST is claimed. Evidence and stage patch:
build/goal19-child-wait-restart/validation.json.

This closes the live receiver-restart boundary for pending child-wait COMMIT
and confirms fresh same-version signal authorization after reconnection. It
does not cover restart of an in-flight user-signal RPC, source restart with a
surviving receiver, mixed-version peers, concurrent usercopy or hard CPU expiry.
Those limits remain explicit in the signal-domain audit. Credentials, peer
policy, syscall/wire ABI and declared VFS routing are unchanged. Goal19 remains
17 enforced/4 pending.
