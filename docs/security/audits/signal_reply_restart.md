# Goal19 9NX: receiver restart before user-signal reply

Previous turn: progress.9NW qualified receiver loss during child-wait COMMIT.
This stage targets the independent user-signal pending-request table.

Problem: live evidence must distinguish a receiver authorization decision from
a successfully returned reply when that receiver reboots before enqueue.
Expected files: new native fixture/driver, this document, goal and signal index.
Files read: kill and signal_local_as stable caller/current-target admission;
authorize_user_signal mapping and retained running-row selection; fixed deferred
reply ownership; wki_proxy_task_signal_as pending slot/send/wait/consumption;
fail_user_signals_for_peer and transport errno mapping; wire response layout;
SmallVec running-task layout and existing exact-VM/reset/epoch fixtures.
Flow: owned child executes remotely and changes to declared UID; source sender
uses ordinary kill(0 or USR1); hardware breakpoint at existing generation-bound
send observes TASK_SIGNAL_RESP before enqueue; verify response and exact target,
pause source, reset exact receiver, inspect matching source pending slot, resume;
source must receive a transport error (EHOSTDOWN on retirement, or ETIMEDOUT at
the RPC deadline) rather than the now-lost success/EPERM reply and
reap receiver failure255. Reconnect uses fresh authenticated boot epoch.
Invariants: original caller snapshot, current receiver credentials, exact task/
request/channel generation, denied signal does not enter target pending bits;
successful USR1 is blocked and observable before reboot. No packet or kernel
memory mutation, no new protocol/API, no invented rollback of a delivered signal.
Locks/contexts: no production changes; hardware breakpoint reads at send entry,
after authorization. QMP operates only on exact owned private VM identities.
Allocation/blocking: host/userspace only, bounded gates/reconnect/parent wait.
ABI/wire/syscall impact: none. Build/test: cross-build/format/syntax, current
source/policy gates, allowed/denied live cases, fresh signal controls, final
pending-table/task/queue inspection and exact owned shutdown. Reuse unchanged
9NT kernels with hashes; no new kernel build/KTEST unless source changes.
Rollback: build/goal19-signal-reply-restart/before. Group RPCs, source restart,
mixed versions, hard CPU expiry and concurrent usercopy remain separate scope.

## Results and corrected expectation

The initial root case observed a successful authorization and blocked USR1 bit
at the reply-send entry, reset the receiver before enqueue, and verified the
original source pending slot. It returned ETIMEDOUT and reaped receiver failure255.
The fixture incorrectly demanded EHOSTDOWN exclusively and reported failure.
That complete original result, source and binary are retained in attempt1.
Production has two legitimate terminal paths: the15-second operation deadline
maps to ETIMEDOUT; earlier peer retirement maps to EHOSTDOWN. No kernel fix is
claimed. The corrected fixture measures elapsed guest monotonic time and accepts
only these transport errors within20seconds; timeout additionally requires at
least14seconds. It does not accept the lost authorization result as completion.

The final four cases pass: root->root USR1, UID104->UID104 USR1,
UID104->UID200 USR1 and UID104->UID200 signal zero. At each hardware breakpoint,
the exact running target and response request/task ID match. Allowed replies
are0 with USR1 bit512 pending; denied replies are-EPERM with no pending bit.
The source snapshot confirms the same request/task ID has both response and
consumer waiters and the exact sender owner. Receiver reset occurs before the
send function executes. No response packet is modified or injected.

Even with the source paused until the replacement receiver's SSH service was
ready, all four native calls time out:15000,15000,15000 and15001ms, errno110.
Thus these runs qualify bounded timeout cleanup, not the faster EHOSTDOWN wakeup.
Every sender is reaped0 and every displaced child is reaped255/raw65280. The
authenticated peer boot epochs change3960804131->2786573193->2206318854
->2339746097->3711733547 with fresh active mounts. The initial failed-expectation
run had separately reset1046283384 to3960804131. Later kernel peer retirement
and fresh calls do not resurrect any completed pending slot.

After reconnect, all12 existing local/remote current-credential signal controls
pass. Independent procfs checks find all28 recorded source/fresh-control task
identities and all six helper paths absent, including the initial attempt.
Both nodes have empty per-CPU dead lists, zero owned user-signal slots, zero
child-query/claim records and empty child-signal/proxy-exit queues. Both guests
synced and stopped; no WOS VM remained at2026-09-17T00:22:02.252768+00:00.

Native fixture compilation, formatting, Python syntax, signal/WKI source guards
and policy audit/unit checks pass. Production source and kernel hashes match9NT;
no new kernel build/KTEST is claimed. Evidence and stage patch:
build/goal19-signal-reply-restart/validation.json. No credentials, authorization
rules, peer policy, syscall/wire ABI or declared VFS routing changed. Goal19
remains17 enforced/4 pending. Earlier-retirement EHOSTDOWN, group RPCs, source
restart, mixed versions, hard CPU expiry and concurrent usercopy remain separate
qualification boundaries.
