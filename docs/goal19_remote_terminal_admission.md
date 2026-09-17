# Goal19 9MU: receiver-owned terminal signal admission

Remote terminal events now carry the full expected session and process-group
identity to the receiver. This addresses the four 9MT cases in which a remote
process continued receiving SIGINT from its former terminal after setpgid or
setsid. The original failing observations remain preserved.

The source terminal still checks its exact allocation, attachment generation,
local session and selected group origin. Its accepted remote task route now uses
`TASK_TERMINAL_SIGNAL`, not trusted lifecycle `TASK_CANCEL`. The new 48-byte packed
message has a version, reserved field, accepted task ID, signal, SID/PGID,
node/boot origins for both identities, and a bounded forwarding count.

A separate COMPUTE resource advertisement gates the new operation. Existing
ACCEPT reserved bytes and CANCEL layouts are unchanged. An unsupported, retired
or failed remote route consumes the terminal event without touching the parked
source frame. Reliable channel ordering/deduplication and the existing bounded
compute queue own delivery. This is a one-way terminal event; it adds no user
RPC waiter or application-level resend of a completed decision.

The receiver uses the admitted peer/session/channel generation to retain the
exact accepted Task. It never substitutes a numeric PID lookup. A shared-VM
publication guard and local signal admission compare current session/group with
the expected full identities before generating any pending signal. Admission
remains owned through canonical process/worker delivery so exec cutover drains
earlier effects. Further remote routing preserves the expected identities and
decrements the bound. Refusal releases all references and leaves pending state
unchanged. Current UID changes do not revoke terminal object authority; applying
ordinary kill UID checks here would break legitimate terminal behavior.

Peer-loss, teardown and other lifecycle cancellation keep their existing path.
Existing UID/GID syscall ABI, signal policy, saved IDs, task retention rules and
declared VFS routing remain unchanged. The new operation requires COMPUTE and
same-domain identity policy. No packet campaigns or penetration testing are used.

Kernel tests exercise actual process admission with differing session/group
numbers and origins, UID 0/100/104/200, invalid signals, accepted-route refusal,
pending-state preservation, lease drain and reference balance. The WKI test calls
the actual classifier and accepted-row delivery, covering exact message shape,
version/reserved/origin/hop bounds, current group changes, stale channel generation
and report-only completion rows. Private test Tasks are not made runnable.

The live fixture extends the preserved 9MT matrix with hangup and a foreground
child dropping to UID 200. It retains readiness handshakes, bounded observation,
positive delivery controls and explicit target/controller reap receipts. Existing
terminal/session and local/remote user-signal fixtures provide compatibility checks.

The initial WKI source audit still looked for the pre-9MQ inline exit-request
guard and for the replaced lifecycle-forwarding helper. It now follows the shared
request consumer and explicitly checks terminal routing/admission/refusal. An
intermediate test-only missing Python path constant was corrected; original logs
are retained. These source-check failures did not require production rollback.

Exact commands, source/kernel hashes, runtime results and shutdown evidence are
indexed in `build/goal19-remote-terminal-admission/validation.json`. This focused
change does not by itself qualify the complete signals or WKI domains.


## Verification

Normal Build WOS and the isolated diagnostic build pass. Normal kernel build ID:
`e093c1ce532d2ebb881f26222eeac7ecfb4e1f99`; diagnostic build ID:
`d6fd38ed84e0a4961dc7bbda3ef0fe260fb33faf`. Formatting/tidy, seven focused source
checks and four rebuilt host-model suites pass. Existing build/tidy warnings
remain. Full KTEST passes **59,814 / 0** at 483.631s and powers off at 483.632s;
its launcher exits 0. Both new production-admission tests pass.

The expanded live matrix passes **28/28**: all four original remote failures are
fixed, group/session changes exclude subsequent input and hangup, and retained
foreground membership preserves delivery after a UID 200 drop. All targets and
controllers exit normally and are reaped. Existing terminal/session fixtures pass
112 checks across both guests; local/remote signal fixtures pass 20 more.
All **160 functional checks** and fixture cleanups pass.

The guests sync and acknowledge QMP quit. Exact private backing chains are
verified. Processes 2092855/2092890 are absent at
`2026-09-16T09:38:00.761463+00:00`; diagnostic process 2090632 is also gone.
No WOS VMs remain. Payload identity is verified by manifests/backing chains;
no live debugger build-ID reading is claimed.

Mixed-version limitation: an older peer without the advertised terminal operation
cannot receive these terminal events. The sender refuses them instead of using
unrestricted lifecycle cancellation or delivering to its parked frame. Legacy
user-signal and lifecycle wire formats are unchanged. Further remote routing has
source and kernel coverage here; this stage's live matrix uses two nodes and does
not claim a live multi-hop transition test. Signals remains pending until its
separate domain audit is complete; Goal19 remains active at 17/21 domains.
