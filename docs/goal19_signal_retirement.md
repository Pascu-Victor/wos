# Goal19 9MQ: late retirement wake at fatal dispatch

Fatal SIGKILL dispatch now rechecks a published process-exit request before
choosing group exit. This closes a race between the initial safe-return request
check and selection of an internal sibling-retirement wake.

`publish_process_exit_request` stores the requested status, publishes the request
flag, then sets SIGKILL and wakes the task. Successful exec uses this mechanism
to retire old siblings. Previously a sibling could pass the early request check,
observe the newly published wake, and call `wos_proc_exit_signal(9)`, which
unconditionally requested group exit. That could terminate the replacement
process instead of retiring only the old thread.

The request consumer is shared by the early check and final signal selector.
After observing the wake, an acquire read of the request flag preserves its
published status. A thread request selects the existing thread-only exit;
a process request keeps its exact status and wait status. Without a request,
SIGKILL still reports 137/9. Other fatal signals retain their prior behavior.
Existing task-liveness and teardown guards remain. No new lock, allocation,
blocking point, signal ABI, credential rule or VFS routing change is introduced.

The deterministic kernel test calls the production request store/consumer
and final selector on a private Task. It publishes between an initial negative
check and signal dispatch, checks thread and process outcomes, genuine SIGKILL,
another fatal signal and both cleanup deferrals, and verifies unchanged
credentials. It inspects the selected disposition; it does not execute the
noreturn scheduler teardown on the private test Task. Live exec fixtures
separately exercise ordinary thread retirement and parent completion.

The initial 9MP local nosuid run had one worker-with-observer exit 137; its other
71 cases passed. An unchanged repeat and remote run each passed 72/72. Those
receipts are preserved. The source race is established, but no causal trace
proves it produced that particular failure; later passing runs are not such a
trace.

The signal source audit also contained a stale inline worker-transfer assertion.
It now follows `transfer_native_worker_signals`, checks admission drain before
mask/pending transfer, and preserves transfer-before-acknowledgement/retirement
ordering. The exec source guard check follows the shared request consumer.

Current commands, exact kernels, test results, original failures and owned-VM
shutdown evidence are indexed in
`build/goal19-signal-retirement/validation.json`. Mount and signals retain their
pending status until their separate domain reviews; Goal19 remains active.

## Current verification

Normal Build WOS and the isolated diagnostic build pass. The final normal build
ID is `284ff917a727b7cb0b6765632d713292748a2451`; the diagnostic build ID is
`edc54694667d7a49c6ff9d6679333dc49adae12e`. Formatting/tidy, focused signal,
exec/futex/policy source checks and five host models pass. Existing warnings
remain. Full corrected KTEST passes **59,792 / 0** at477.337s, powers off at
477.338s, and its launcher exits0.

The initial new test compared the private process's credentials while its
synthetic thread flag selected a missing canonical owner. That assertion failed
at243.969s; the partial run was deliberately stopped. Restoring the process view
before the unchanged-store assertion corrects the test without changing the
production fix. Its original kernel/logs and manual shutdown are retained under
`initial-test/`. The corrected production-selector case passes at244.384s.

Fresh private guests pass230mount/readonly checks and144local/remote nosuid exec
cases; five further bounded local matrices pass360cases. All734functional checks
pass, along with mount reporting and retirement controls. Fixture cleanup and
sync complete; exact private boot chains are verified and guests1969455/1969459
are absent, with no QEMU remaining, at`2026-09-16T08:34:26.312671+00:00`. Both diagnostic VMs are also down.
Passing these runs does not retroactively diagnose the preserved9MP failure.
