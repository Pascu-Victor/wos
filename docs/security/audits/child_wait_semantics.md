# Goal19 9NS: live remote wait semantics

Previous turn: progress.9NR enabled the native remote-wait client, passed60033/0
KTEST, eight live child-event cases and12 accounting checkpoints. Goal remains
17 enforced/4 pending. This stage qualifies additional ordinary owned-family
semantics through the real query/claim transport.

## Reconnaissance and test contract

Read: current waitpid usercopy/client/local branches, native query admission,
query group/PID selection and accounting contract, existing child-event, group
origin and CPU fixtures; testing matrix and the prior9NR evidence.
Files: new standalone userspace fixture/host runner, this record and goal notes.
No kernel, credential, ABI or wire change is planned. Existing normal Build WOS
and private two-node --no-setup workflow will verify the current image. Isolated
KTEST need not repeat unless this work changes kernel code or reveals a concern.

Fixture flow: probe receiver PID, advance only owned source children within a
fixed bound, create a gated source child, exec its parent onto the receiver,
then fork/reap owned padding children until a receiver child has the source
child's numeric PID. Keep both children alive while checking EOVERFLOW and exact
own-group WNOHANG. Release both gates; observe the local child's distinct group
with WNOWAIT and reap it with wait-any. Observe the remaining source child via
exact own-group, numeric group and PID WNOWAIT; a rejected status pointer must
not consume it. Reap it with wait-any, verify CPU observation increases exactly
once and ECHILD follows. Use UID0 and UID104 with bounded deadlines and record
physical node/PID identities for post-test cleanup checks. Also rerun the existing
local/imported group-number collision fixture against the activated client.

Invariants: every child belongs to the fixture; PID advancement uses normal fork
and reap, not allocator mutation. No packet manipulation or external targets.
No test result is inferred from cleanup; failed semantic assertions remain failed.
Helpers and recorded task identities are checked after execution. Exact private
VM PID/backing-image ownership is verified before stopping them.
Rollback: new files and pre-stage goal copy under
build/goal19-child-wait-semantics/before. A discovered kernel defect requires its
own read/call-flow review and patch-contract addendum before editing.

## Results

Normal Build WOS passes. Both normal and diagnostic kernel hashes remain exactly
those qualified in9NR; no kernel/library change was made. Fixture compilation,
scoped format/clang-tidy, waitpid source guards and policy audit pass. The complete
gate still fails at pending signals. No new KTEST run is claimed:9NR's60033/0 is
prior evidence for the unchanged diagnostic kernel.

The first fixture attempt failed in userspace at address1, on receiver PIDs107
and134. Current mlibc options/posix/generic/sys-wait.cpp shows why: waitpid supplies
a local temporary to its sysdep, then assigns through the application's pointer
after the syscall. That test therefore consumed the child before faulting in libc;
it did not exercise kernel EFAULT. wait4 supplies the caller's pointer directly.
The corrected fixture uses wait4 for this check. It also reflects libc waitpid's
zero status temporary on WNOHANG, rather than assuming that caller status remains
unchanged. Both initial failures, original helper/source/hashes, serial faults and
commands are retained in build/goal19-child-wait-semantics/attempt1. All8 recorded
initial-attempt task identities and both helpers are absent. No kernel fix or
passing initial run is claimed.

The corrected fixture passes13 checks per identity,26 total at UID0 and UID104:

- Same numeric owned PID on wos-0/wos-3 returns EOVERFLOW both before exit and
  after observing a ready local event. Status remains unchanged on that error.
- Exact own-group WNOHANG finds the still-live source child and returns0.
- Numeric local-group WNOWAIT observes status10496, and waitpid(-1) reaps that
  local child. The still-owned source child with the same PID remains available.
- pid0, numeric own-group and positive-PID WNOWAIT each observe the source child's
  status9472 without adding its CPU usage. wait4 with a rejected status pointer
  returns EFAULT14 without consumption/accounting; waitpid(-1) still reaps it.
- Successful remote reap adds child CPU; subsequent ECHILD leaves it stable.
  This fixture does not compare the charged amount with returned rusage; exact
  receipt-amount accounting still relies on the prior9NR kernel test.

The actual colliding PID pairs are190/190 and217/217 on the two physical nodes.
The existing group-origin fixture also passes both UID0/104 cases, with local
and imported group numbers239 and257: pid0 selects exact imported provenance,
whereas a numeric negative group selects the local candidate. Both fixtures remove
their helpers and all16 final recorded process probes are absent.

The private normal VMs3594298/3594301 were synced and stopped after qualification.
No WOS VM remains at2026-09-16T23:06:22.765110+00:00. All hashes/results are indexed by
build/goal19-child-wait-semantics/validation.json.

These are functional owned-process tests through real transport. They do not
qualify a concurrent unmap during usercopy, native waiter death during commit,
or peer loss/reconnection. Those remain before the signal-domain completion audit.
The overall goal stays active at17 enforced/4 pending. ABI, declared VFS routing,
credentials and wire formats remain unchanged.
