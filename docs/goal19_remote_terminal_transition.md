# Goal19 9MT: remote terminal signals after a job transition

A live functional comparison reproduces a remaining terminal signal-authority
gap. When a remote child changes its process group or creates a new session,
its former source terminal still delivers SIGINT. Native children stop receiving
that event. This occurs both as root and after dropping to UID/GID104.

The new `service_terminal_transition` fixture uses only private PTYs and owned
children. Each case first proves actual SIGINT delivery. The child then keeps
its job identity, creates its own group, or creates its own session and reports
its current PID/SID/PGID. A second control character is written to the same
terminal. The observation is bounded to 300 further syscall/sleep boundaries;
it does not claim to cover every possible signal delay. Children install a
handler, so an unexpected signal is counted rather than terminating the test.

Results on the unchanged current 9MS normal kernel:

| Placement | Identity | Keep group/session | New group | New session |
| --- | --- | --- | --- | --- |
| Native | root | PASS: 2 events | PASS: 1 event | PASS: 1 event |
| Native | UID104 | PASS: 2 events | PASS: 1 event | PASS: 1 event |
| Remote | root | PASS: 2 events | FAIL: 2 events, expected 1 | FAIL: 2 events, expected 1 |
| Remote | UID104 | PASS: 2 events | FAIL: 2 events, expected 1 | FAIL: 2 events, expected 1 |

All 12 positive controls pass, every target exits normally and is reaped, and
all 12 controller children are reaped. The runner removes the helper only after
checking those lifetime receipts. Fixture cleanup, guest sync and exact private
VM shutdown complete. Processes 2032930/2032971 are absent at
`2026-09-16T09:12:10.289343+00:00`; no WOS VMs remain.

Current source explains the difference: the source scheduler matches its local
proxy's original job identity, then calls `wki_proxy_task_forward_signal`.
That sends trusted `TASK_CANCEL`. The receiver resolves the accepted session/task
but does not compare current session/group membership before generating signals.
The 9MS origin fix prevents source-side alias substitution; it cannot validate a
receiver's later job transition. Removing another fallback or applying ordinary
kill UID checks would not implement the required terminal semantics.

The cancellation review also ruled out an initially suspected unpublished-task
wake: `find_running_task` explicitly requires a published row. Before publication,
cancellation is recorded against the submit session. Monitor rows retain their
Task, and final GC requires the scheduler's sole reference. No live PID-reuse
defect was demonstrated by this stage.

The required next change is receiver-owned terminal membership admission,
separate from trusted lifecycle cancellation. Its capability/version behavior
must be explicit: old `TASK_CANCEL` readers truncate extensions, and currently
reserved ACCEPT bytes participate in existing user-signal negotiation.
`build/goal19-remote-terminal-transition/next-patch-contract.md` records the
impact map, compatibility constraints and test plan. No production fix is
claimed in this observation stage.

The fixture compiles, formatting checks and Python compilation pass. No new
kernel build or KTEST run was needed for this userspace-only comparison; current
source and kernel hashes were checked against 9MS. Normal build ID is
`5fcab79fad604e6e0c31f523bfec6256d67a5125`. Private boot manifest and backing chains
identify the payload; no debugger build-ID observation is claimed.

Full receipts are indexed by
`build/goal19-remote-terminal-transition/validation.json`. Signals stays pending;
Goal19 remains active with 17 enforced/4 pending domains.
