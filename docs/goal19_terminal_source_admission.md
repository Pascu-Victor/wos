# Goal19 9MW: source terminal signal admission

The scheduler's input and hangup producers now use
`process::signal_terminal_source_as`. Local terminal generation acquires the
same Task admission counted by remote exec's final drain, before deciding between
local generation and an accepted remote route. A local lease lasts through the
pending-bit update, job-control generation and scheduler wake. Exec therefore
cannot finish its final pending snapshot while an admitted source event is
still being generated.

The helper borrows the exact Task reference already retained by the scheduler.
`PendingSignalTarget` registers lease release with the caller's existing fatal
syscall cleanup as well as ordinary return. The change adds no task cohort,
allocation, queue or wait. A remote admission owns no local lease and uses the
existing terminal forwarder; a missing/retired route after that decision is a
refusal, never authority to signal the parked source frame.

The original terminal selection still checks allocation, attachment generation
and full foreground identity. Local admission rechecks full SID/PGID provenance
and, for input events, the exact terminal binding. Hangup already revoked the
binding, so it retains its selected HUP/CONT grant while rechecking session and
group. Changing UID does not revoke that terminal grant. Existing process/worker
selection and job-control generation are preserved.

The source helper does not acquire VM publication or session mutation. It reads
the admission's value snapshots under the existing Task locks, making it usable
from leader detach, which already owns the nonrecursive publication lock. It
does not read or alter an address-space pointer. Receiver-side 9MU admission,
credential policy, numeric syscall/ioctl ABI, message layouts, mixed-version
refusal and declared VFS routing are unchanged.

The new `SignalPending/SourceTerminalAdmissionDrainsBeforeExecCapture` selftest
calls the actual source helper for private process and worker Tasks. It publishes
an accepted route between admission and generation, verifies that the real drain
predicate remains false, refuses a new local lease, then verifies that the actual
exec signal snapshot includes the generated bit after release. The test also
covers eight identity/binding changes, UID 0/100/104/200, cleared-binding hangup,
invalid signals, accepted-route refusal, reference balance and lease drain. It
runs with VM publication already held. Hidden test Tasks never become runnable
and no packet is sent.

Source checks follow both scheduler producers into the new admission helper and
retain the old no-cancellation/no-local-fallback requirements. Existing host
models and unchanged functional fixtures provide separate regression evidence.
The exact before files and patch contract are under
`build/goal19-terminal-source-admission/`.

This fixes the remote-cutover window identified by the
[9MV source review](goal19_signal_producer_admission.md). It does not establish
that this window caused a historical timeout, or qualify all native worker image,
ptrace/orphaned-group, nonterminal producer, multi-hop or distributed lifecycle
interactions. Signals remains pending until its full operation audit is complete.

The initial selftest returned before admission: its synthetic worker lacked the
`owner_pid` required by `resource::bind_thread_owner`. The first full run reports
59,814 passes and one failure, then deliberately panics for a failed suite. Its
logs, ELF, manifest and exact QMP shutdown are retained with `initial-` names.
The test setup now supplies the required owner PID. No production change or
weakened assertion was made in response. An initial formatting check required
normal formatting; a later mistyped formatter path is also retained separately.

## Verification

Normal Build WOS and the isolated diagnostic build pass. The final normal kernel
build ID is `58f889e02b2602caa3ff1b80f1a4e17dc6e9e46e`; diagnostic ID is
`d218da9364d2ca64b7521fdae1a74f3f885b0f34`. Scoped formatting/tidy,
four source suites, four host-model suites and the policy audit/unit suite pass.
Existing build/tidy warnings remain. The corrected full KTEST run passes
**59,815 / 0** at 481.422s and powers off at 481.423s; its launcher exits 0.
The new actual-producer cutover test passes at 247.884s.

On fresh private two-node guests, all 28 unchanged terminal transition cases,
112 terminal/session checks and 20 local/remote signal checks pass: **160 live
checks**. Every transition target/controller is reaped normally, fixture cleanup
completes, and both guests sync before acknowledging QMP quit. Exact private
backing chains and boot manifests identify the tested payloads. No live debugger
build-ID reading is claimed.

Diagnostic process 2214416 is gone. Cluster processes 2215711/2215714 are absent
at `2026-09-16T10:23:39.832941+00:00`; the initial failed-run process 2153287 is also gone.
No WOS VMs remain. Exact receipts, source hashes and both the failed and corrected
runs are under `build/goal19-terminal-source-admission/validation.json` and the
stage's `initial-` artifacts.

Goal19 remains active at **17 enforced / 4 pending** domains. The completion gate
still identifies `signals`; this focused fix does not promote the whole domain.
