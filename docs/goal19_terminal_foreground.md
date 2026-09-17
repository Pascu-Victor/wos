# Goal19 9MS: retain terminal foreground group origin

Terminal foreground selection now retains both the numeric PGID and its node/boot
origin. `set_foreground` already resolved that origin but previously discarded it.
A later local group with the same number, or an imported group from another peer
or boot, could satisfy the old numeric-only signal predicate. This is a source
finding; this stage does not claim a live reproduction of that collision.

`PtyPair` stores the origin under its existing lock. First claim copies it from
the process job identity, successful TIOCSPGRP copies the resolved origin, and
revocation/allocation reset it. VINTR, VQUIT and VSUSP snapshot it with the PGID,
terminal allocation, local session and attachment generation. Delivery and
hangup use the same exact foreground predicate before forwarding or generating
pending signals. Retained task references and existing wake behavior remain.

The no-match proxy fallback is removed. Its comment assumed a forwarded
TIOCSPGRP could install a remote PGID. Current `remote_ipc.cpp` rejects that
session-mutating ioctl at both ends because its wire request has no authenticated
process/session actor. Broadening an unmatched foreground selection to every
proxy in the terminal session was therefore neither a valid resolution rule nor
a supported remote job-control contract. No selected member now means no signal.
The two unused broad terminal-delivery helpers are removed with the fallback.

The kernel test exercises the production recipient predicate against local and
foreign numeric collisions, changed peer/boot, foreign session, changed PGID,
reused terminal allocation and stale attachment generation. The actual PTY
claim/detach test now starts with a local session and an imported group, checks
origin retention through repeated claim and denied updates, and checks clearing
on detach. Private test Tasks remain outside the runnable registry. This does
not simulate a distributed migration or execute a real collision in userspace.

There are no new locks, allocations or blocking points, and no syscall or wire
layout changes. Numeric terminal ioctl payloads, credential policy, and declared
VFS routing are preserved. The signals domain remains pending until its remaining
operation/lifecycle review is complete.

The initial signal source audit counted all scheduler signal-producing functions,
including a removed fallback. It now checks atomic publication in each retained
entrypoint. The initial host-model CTest selector omitted the `_test` suffix and
selected zero tests; that receipt is retained and is not a passing test run. The
corrected command uses `--no-tests=error`, rebuilds the five selected models, and
runs all five successfully.

Verification artifacts, exact source/kernel hashes, command outcomes and owned
VM shutdown evidence are recorded under `build/goal19-terminal-foreground/`.


## Verification

Normal Build WOS and the isolated diagnostic build pass. Normal kernel build ID:
`5fcab79fad604e6e0c31f523bfec6256d67a5125`; diagnostic kernel build ID:
`fc44bb8180970e274cd2b6c30a6f259159b7ff63`. Formatting/tidy, six focused
source/policy checks and five rebuilt host models pass. Existing build warnings
remain. Full KTEST passes **59,812 / 0**, powers off normally, and its launcher
exits 0. The new predicate case passes at 245.475s and actual admission case at 245.482s.

Two fresh private normal-kernel guests pass 112 terminal/session checks as root and
UID104, plus 20 local/remote signal permission/group checks. Fixture cleanup and
sync complete. Exact private disk chains are verified, both guests acknowledge
QMP quit and their processes are absent at`2026-09-16T09:03:06.440903+00:00`.
The diagnostic VM is also down. Goal19 remains active at 17 enforced/4 pending;
this stage does not promote signals.
