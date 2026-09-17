# Goal19 9NQ: imported child CPU observations

Previous turn: progress. Query-bound claims passed60031/0 live KTEST; no WOS
VM remains. Goal remains17 enforced/4pending, first signals.

## Reconnaissance and patch contract

Problem: remote claim commit charges the real source queue. Adding its receipt
to receiver exportable child totals would charge it again during completion.
Remote exec also currently omits previously reaped-child observations entirely.
Expected files: Task child accounting, child_events APIs/event capture, WKI resource
model/submit/receive/completion, advertisement constants/driver, host/KTEST and
source checks, this document and goal progress.
Files read: task.hpp/.cpp, resource.cpp/.hpp, child_events.hpp/.cpp, waitpid.cpp,
exec.cpp (prepare/quiesce/cutover), resource_contract.hpp/model, remote_compute
submit/receive/completion and tests, wire/remotable, previous claim protocol,
workflow/kernel/WKI/critical/build/test instructions and current task definition.

Flow: quiesced exec owner -> visible child totals -> explicitly flagged resource
submission -> private decoded observation -> unpublished receiver imported totals.
Local reaps/completions update exportable counters; imported observations remain
separate. rusage/times/procfs and immutable exit events read their saturating sum.
Completion exports only local deltas; rejection exports no child CPU at all.

Invariants: one charge at the source; no imported totals exported back; snapshots
are observational; no rejection mutates the old process; spawn starts with zero
inherited child usage; native exec preserves existing Task counters; remote exec
and onward exec preserve visible totals; saturation never wraps. Credentials,
source authority, declared VFS routing, PID policy and syscall ABI stay unchanged.
Locks: child lifecycle lock serializes both kinds of child counters. Resource
snapshots release resource_lock before entering lifecycle. Completion reads stable
exit counters without adding lifecycle under compute lock. No new nested lock,
allocation, blocking, send or log in accounting helpers or raw RX.
Unsafe contexts: no userspace memory access; bounded counter reads/writes only.

ABI/wire impact: preserve the832-byte version2 resource layout. New negotiated
submission-only flag gives reaped fields the meaning of imported observations;
unflagged submissions still require zero, completions must never carry this flag.
Advertise process-child-cpu-v1 separately and refuse nonzero transfer without it.
Old decoders reject the unknown flag before process construction. Restore requires
an explicit observation output to prevent silently dropping transferred totals.

Build/test plan: host model rejection/immutability/compatibility; KTEST source to
receiver to onward receiver accounting and exact completion/exit aggregation,
WNOWAIT/observation separation/saturation; normal and isolated builds, scoped
format/source/policy checks, fresh live KTEST. Multi-node waitpid remains pending.
No user runtime input needed: --no-setup is explicitly authorized.
Rollback: exact pre-stage files under build/goal19-child-cpu-observations/before.


## Implemented accounting and wire behavior

Task retains its existing exportable child counters and two imported observation
counters, protected by the same lifecycle lock. Public child-usage observations
and immutable exit events use saturating sums. Completion retains its original
local-only fields, so imported values cannot return as new charges. Local exec
preserves Task state; spawn submission does not capture parent child observations.

The version2 resource layout is unchanged at832bytes. Flag0x20 is submission-only
and requires an explicit decoder output plus EXEC_HANDOFF. The output is published
only on successful full resource preparation. Restore never places imported child
values in CPU policy state. Completion/rejection reject the observation flag;
unflagged submission still rejects nonzero reaped fields. Source sends nonzero
exec observations only with process-child-cpu-v1 resource0x53494709 advertised.
Existing decoders reject the formerly unknown flag before construction. Both
INLINE and VFS_REF submissions apply the same rule after exec quiescence.

Host tests cover missing output, unflagged/malformed records, preserved outputs,
completion-only deltas and rejection rules. KTEST uses real child event queues,
source charges, a receiver, onward exec baseline, completion merges, visible
observations, immutable EXIT rusage, WNOWAIT, saturation and reference cleanup.
Its remote-reap observation is injected at the point reserved for the future
initiating client; it does not claim that remote waitpid is enabled.

A separate userspace probe reaps owned children and checks exact times() child
values across local exec or wos-0 -> wos-3 -> wos-0, at UID0 and UID104. It ran
on two isolated normal-kernel VMs after KTEST; no concurrent node-zero topology.

Normal/diagnostic builds, host resource/wire/auth-policy tests, scoped formatting,
WKI/waitpid source checks and policy audit/unit tests passed. Two source guards
needed the new snapshot argument and actual deferred worker entrypoint. The first
standalone fixture compile lacked the explicit libc++ include path; its first VM
attempt then exposed a missing PT_INTERP caused by an obsolete no-dynamic-linker
flag. The root CMake rules explicitly remove that flag. Corrected fixture uses
the current /lib/ld.so contract, and failed receipts/binary remain preserved under
the stage directory and fixture-attempt1. No kernel build or KTEST failure occurred.


## Verification and remaining work

Fresh live diagnostic KTEST passed60,032/0, including the new chained-accounting
case and all query/claim tests. Diagnostic VM3436716 powered off; no matching WOS
VM remained at2026-09-16T22:07:35.097251+00:00. Source and packaged image hashes match.

Two normal-kernel VMs3438862/3438900 passed12 userspace checkpoints: three stages
for local and two-hop remote exec at UID0 and UID104. Each stage verified exact
inherited times() child values before its own new local reap. All eight distinct
recorded process identities disappeared, both helper copies were removed, both
guests synced and their exact QEMU processes acknowledged quit. No WOS VMs remained
at2026-09-16T22:11:02.793687+00:00. The first fixture-only ENOEXEC was corrected without
changing either tested kernel. Evidence: build/goal19-child-cpu-observations/validation.json.

The imported-observation API is additive; its future remote-wait caller must own
exactly-once application of a committed parent EXIT receipt. WNOWAIT/tracer/stop
receipts must not add child usage. The actual initiating waitpid continuation,
native-thread/fatal cleanup and wake behavior remain unimplemented. The earlier
four migrated-parent wait failures have not been rerun or resolved by this stage.
Goal remains17 enforced/4pending, first signals; the complete audit still refuses
completion. No live peer-failure scenario was added.
