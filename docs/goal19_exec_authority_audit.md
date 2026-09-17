# Goal 19 exec authority audit — 9LT

The 9LR follow-up qualifies ordered receiver final admission and private
cleanup plus ordinary live main/worker/spawn behavior. The 9LS follow-up
qualifies forced in-flight source retirement while the committed private
receiver candidate retains a remote executable File.

The final 9LT reconciliation below marks exec enforced for its authorization
and publication/lifetime contract. The inventory is 12 enforced and 9 pending,
with VFS traversal next. Historical qualification limits and unresolved progress
observations remain recorded; the full Goal19 remains active.

## Scope and evidence boundaries

Goal 19 requires stable credential decisions, atomic publication, saved-ID and
no-regain behavior, unchanged state on refusal, and explicit authenticated WKI
mapping. It also requires the relevant task/session lifetimes to survive those
decisions. It does not require implementation of every optional socket, legacy
transport, or RDMA feature. Unsupported operations must be assessed by their
actual refusal behavior. An unexplained timeout is retained evidence, not by
itself proof of an authorization failure or proof of safe cleanup.

The local source is authoritative. `historical-source-continuity.json` verifies
that the pre-9LQ exec implementation and current remote_compute.cpp/.hpp match
the recorded 9LB source hashes. The only kernel edits in 9LQ add a selftest,
its conditional declaration, and registration; `production-unchanged.json`
checks that removing that selftest restores exec.cpp exactly. Earlier live
results below are historical and were not rerun in this stage.

## Requirement checklist

| Obligation | Current source and evidence | Remaining qualification |
| --- | --- | --- |
| One immutable identity per admission | `Snapshot` owns six IDs, bounded groups, ceiling, NNP, dumpability and sealed grants. `CredentialStore` publishes whole values. `resource_process.cpp` prepares and compares the admitted snapshot before publication. | Preserve these properties at each publication boundary; a source root-check count alone is insufficient. |
| Original file-access authority, resulting exec identity | Native and remote exec retain original credentials for file admission; `prepare_exec` derives the resulting snapshot. Receiver mapping independently admits original and resulting identities. Historical 9KR/9KS/9KT qualify saved IDs, set-ID suppression, secure auxv and remote spawn original-read behavior. 9KX qualifies interpreter execute admission before loading or shared-cache use. | No newly demonstrated transition defect in this audit. These checks do not establish receiver cancellation ordering. |
| Native publication | `commit_native_exec_image` prepares under the VM publication guard, revalidates credentials/tracing, and requires safe quiescence for effective-ID changes. Private preparation precedes image/credential publication; successful publication consumes one-shot routing. Historical 9KZ qualifies denial-and-retry behavior. | Preserve existing lock and lifetime invariants in subsequent changes. |
| Source remote-exec cutover | `prepare_remote_exec_owner` closes admission, prepares the replacement, quiesces contributors and captures signals. `cutover_remote_exec_owner` rechecks admission, quiescence, tracing and current credentials before publishing credentials and private metadata. Historical 9LB qualifies candidate metadata refusal; 9HQ/9HR and existing KTESTs cover signal handoff. New 9LQ calls the actual preparation/cutover functions for six refusals and a successful control. | 9LQ uses an unregistered owner with no siblings or in-flight signal leases; it does not replace existing concurrent signal/admission tests. |
| Coordinator terminal ownership | `wait_for_exec_submit` installs committed-wait ownership before the cutover callback. Callback refusal preserves rollback; after successful cutover, terminal failure retires the submitted image and cannot resume the old image through fallback. Existing 9KV KTEST invokes this coordinator with a substituted callback. | Coordinator and source-owner tests do not directly qualify every receiver publication refusal. |
| Receiver private candidate publication | `handle_task_submit_work` maps identities before image work. Publication obtains a peer lifecycle lease and rechecks the sealed session, cancellation and deadline. Prepared exec releases the lease while waiting for COMMIT, then reacquires and rechecks before scheduler publication. Failure cleanup releases the lease before blocking teardown (9KV). | 9LR qualifies the production final admission/cleanup functions with ten ordered native scenarios and real private ownership; scheduler posting is substituted. 9LS adds four actual committed-private source-reboot retirements with a retained remote File, no payload execution, exact receiver cleanup and fresh-success controls. These distinguish prepublication refusal from accepted-running retirement. The final 9LT reconciliation qualifies this exec authority boundary; concurrent scheduler posting and individual remote-close acknowledgements are not measured by these tests. |
| Session identity and downgrade | Queued compute work retains peer boot/auth generation/session/policy/capabilities. Submission admission binds the original resource channel and generation to that identity; a same-channel identity change cannot be silently adopted. Existing admitted-peer KTEST mutates these fields and checks refusal. Historical 9KU qualifies v3/v4 strict refusal, optional fallback and matched success; 9LF qualifies allow/deny/restore after ordinary restarts. | Predicate/fragment tests and accepted-running restart tests do not establish the entire private-candidate publication lifecycle. Do not infer an enabled same-boot rotation bypass from hypothetical peer transitions. |
| Root/cwd and routing | `capture_submit_filesystem`, `prepare_return_owner_cwd` and `prepare_received_root_path_cwd` retain supported cwd authority, reject unsupported owned-root/third-owner transfers and recheck admitted return-owner channels. Receiver preparation holds its native root view privately, then preserves declared routing. | Explicit LOCAL `/` continues to select the receiver's root, as requested. General unsupported root transfers are not silently converted to source-root semantics. Broader VFS lifecycle qualification remains in the VFS/WKI rows. |
| Completion and inherited streams | Historical 9LL–9LP qualify finite native stream output, shutdown/close ordering and shared-File read behavior. 9LP passes 242 final live controls, including 40 readers and root/UID100 cases, plus 59,055/0 KTEST. | Distinct exported aliases still have separate receive queues. Do not claim general socket parity. Assess remaining enabled behavior against authority and lifetime requirements rather than requiring every transport feature. |

The unsuitable historical 9KW live fixture used per-PID kill, which waited
during cutover and did not establish the intended early cancellation branch.
It also did not retain the intended VFS dependency. Its result remains
inconclusive; the unchanged fixture must not be counted as a passing cleanup
regression. The next-step source map and evidence requirements are recorded in
receiver-publication-next.md under this stage's artifact directory.

The libc fast spawn attribute mask contains signal mask, process group and
USEVFORK flags. RESETIDS takes the generic path, which applies ID reset before
file actions/exec and aborts on failure; it is not silently treated as a fast
kernel-spawn flag. No libc behavior is changed here.

## New production-cutover test

`RemoteExecRequest/ProductionCutoverRefusesStalePreparationWithoutPartialState`
calls the actual owner preparation/cutover functions. Its seven scenarios are:
missing admission, admission not quiesced, changed IDs, changed tracing,
released admission, changed NNP, and successful commit.

Each refusal must return EAGAIN while preserving the latest snapshot (including
the intervening change), executable path, routing target/flags, pending signals,
signal result, accepted signal route and commit state. The successful control
checks resulting real/effective/saved IDs, private metadata publication,
one-shot routing consumption and accepted signal route. All cases preserve
root/cwd and ELF state, avoid scheduler publication and release the plan's
reference/admission state. The synthetic owner PID is checked absent before
testing so the successful control cannot retire a real process group.

This does not simulate a complete receiver, active sibling drains or network
traffic. It closes a specific gap between existing plan/predicate tests and
the production source-owner cutover implementation.

## Verification

Artifacts: `build/goal19-exec-authority-audit/`, indexed by `validation.json`.

- Normal Build WOS and isolated diagnostic build pass. Normal build ID:
  `1e007f38fbb31f1da3825e7ee98916d698fd02d6`.
- Scoped formatting/clang-tidy, the remote compute source check and policy
  audit/unit tests pass. The complete gate correctly fails at pending exec.
- Isolated `wos-ktest --no-setup` passes **59,056 tests, zero failures**.
  The new test passes at 236.219; the summary is at 465.236 and automatic
  poweroff at 465.237. This is new runtime evidence from this stage.
- Host ownership verification at 2026-09-15T22:57:50.886729+00:00 (UTC) confirms the exact
  KTEST launcher/VM are absent and no WOS VM remains. No new cluster run or
  WOSDBG session is claimed.

The historical 9LG initial fresh 9 MiB interpreter-fetch timeout and 9FZ
completion observation remain unexplained. Later passing 9LH runs did not
trigger the delayed-fetch capture and do not prove either issue fixed. Retain
those artifacts and determine their connection to required authority/lifetime
invariants if a bounded reproduction supplies new evidence.

Production locks, allocation behavior, credential/service grants, syscall ABI,
wire formats and declared VFS routing are unchanged. The exact fenced goal
string and the user's live-debug `--no-setup` authorization are preserved.

The initial test build passed 59,056/0, but the host policy gate rejected its
direct effective-ID-to-zero expected-value comparison. The assertion now
compares the complete expected ID tuple, without changing the source gate or
expected IDs. The original build/logs are retained in before-tuple/. Normal and
isolated builds and the policy audit/unit suite were rerun successfully; final
runtime evidence is recorded separately in validation.json.

## 9LR receiver final admission and private cleanup

The receiver now uses post_admitted_remote_process immediately before calling
the scheduler for both ordinary spawn and prepared exec. The worker must retain
its noncopyable peer lease; under the compute lock the helper rechecks the exact
sealed session/channel generation, consumes only matching cancellation and
checks the absolute deadline. It releases the compute lock before scheduler
posting and retains the peer lease through that call. This also covers time
spent creating the private monitor after the earlier ordinary-spawn check.

The existing refusal branch is extracted into discard_unposted_remote_process.
It retires the matching unpublished monitor under the compute lock, then drops
the lock and peer lease before deleting output receipts, releasing the two extra
Task references, and invoking owner-linked task/VFS teardown. Successful
publication, its post-publication cancellation check, response routing and
OVERLOADED refusal are preserved. No credentials, service grants, filesystem
routing, syscall ABI or wire layout changed.

FinalPublicationAdmissionAndPrivateCleanup has ten scenarios: missing lease,
handoff not ready, retired session, changed admitted identity, reused channel,
exact cancellation, unrelated-task cancellation, expired deadline, posting
refusal and successful admission. It exercises the production gate and cleanup
with real private Task ownership, restricted immutable credentials, NPROC
reservation, two pipe descriptors, a callback File, a private output receipt and
service-accounting membership. The close callback checks that peer RX admission
and the compute lock are available, the private monitor is retired, credentials
are unchanged and the child remains in its owner's teardown slot. Afterwards
NPROC and pipe current usage return to baseline, receipt references return to
their private base reference, service membership reaches zero and a new NPROC
reservation succeeds.

The scheduler callback is substituted and never schedules the synthetic child.
Successful admission controls therefore use private cleanup after observing the
callback. This is ordered native publication/cleanup coverage, not actual
concurrent scheduler posting or a real remote-VFS acknowledgement test. The
separate existing live identity matrix is used for ordinary main/worker/spawn
publication behavior.

The initial diagnostic run reports 59,056 passed and one failed, then the
expected selftest-failure panic. The added lease check required a current
scheduler Task, but sched_init runs KTEST immediately after setup_queues and
before startScheduler establishes task context. The noncopyable lease retains
its exclusive claim independently of that context; its owns check now uses the
stored claim and node. No session/mapping/cancellation/deadline check was
removed. The failed run and kernels are preserved in before-lease-claim/.
Per-scenario failure diagnostics were also added.

The first live attempt's independently built fixture faulted in its first
local case before producing identity results. The compile command had omitted
the repository's userspace -fno-sanitize=safe-stack/-fno-stack-protector flags.
The failed ELF has an unsafe-stack TLS relocation; disassembly at main+0x40
matches the faulting write through a null unsafe-stack pointer. Rebuilding the
unchanged fixture with the normal module flags removes that relocation.
The failed binary, serial log and result remain in before-fixture-flags/;
the faulted PID was verified absent and its exact three files/directory removed.
No kernel rebuild or production change was made for this harness correction.

Final verification: Normal Build WOS (RelWithDebInfo), isolated diagnostic build,
both packages, scoped formatting/clang-tidy, exec/compute source gates and policy
audit/unit tests pass. Five source-gate guard-removal mutations are rejected.
Final isolated --no-setup KTEST: **59,057 passed, zero failed**; the new test
passes at 320.021, summary at 459.717, automatic poweroff at 459.718.
Normal build ID: 46f608cce52899aad75757b707ccc548d2d4ac24; both live node IDs were verified by WOSDBG
and the session closed. The unchanged extended identity fixture, rebuilt
against the current sysroot, passes **54/54** local/remote main/worker/spawn
cases with child retirement checks. All three helper directories are
independently absent and both nodes sync successfully. All owned VMs and
launchers are down; no WOS VM remains at 2026-09-15T23:32:49.338213+00:00.

This qualifies ordered final admission/private cleanup and ordinary live
publication. It does not establish forced in-flight peer retirement with a
retained remote VFS dependency or concurrent scheduler publication. Exec stays
pending; the complete gate still rejects exec and the manifest remains
11 enforced/10 pending. Historical9FZ/9LG unexplained observations remain
preserved. Evidence: build/goal19-receiver-publication/validation.json.

## 9LS in-flight source retirement with a retained remote executable File

The current 9LR kernel passes all **12 phases**: held successful control,
held source reboot, and fresh success after reconnect, for root and UID1002
from both main and worker threads. Two private rootless `--no-setup` VMs use
existing TAPs; independent receiver management survives the source reset.
Only the recorded source QEMU PID/start-time pair receives `system_reset`.
No production code, credential policy, syscall ABI, wire format or declared
VFS routing changes in this stage. Explicit LOCAL `/` retains receiver-root
semantics. Normal and isolated kernel hashes still match 9LR.

Each control and reset phase holds a committed but unpublished receiver
candidate at the existing COMMIT diagnostic gate. Its private monitor is
observed, the source's active file grants increase, and bounded WOSDBG reads
prove that `Task.exec_image_file` owns a positive-reference REMOTE File with
a valid remote FD and source-node owner. The separate executable-pin view
count is zero: the retained dependency is the actual File, not an EXEC_PIN
view. Eight witnesses read 86 bytes each through four closed pause/read
sessions (32 sessions total). No credential/key memory is inspected.

In all four reset phases the receiver observes a different source boot epoch
while the original candidate remains private and marked `discard_completion`.
After hold release, logs identify that candidate's committed, nonexpired
refusal (`committed=1 expired=0`), owner-linked `wki-submit-post` teardown and
post failure. The active row and receiver PID disappear, executable-view
counts return to baseline, and the payload marker remains absent. Each
following fresh request executes and its source parent reaps the expected
exit status while retaining its six IDs and empty supplementary groups.
Reset destroys the old source VM process state; this is not a claim that its
old parent reaped a child. Payload identity mapping is covered by the separate
9LR identity matrix, not by this marker-only payload.

All four helper directories are independently absent on both nodes. Full
netdiag snapshots have no active compute rows; publication holds and waiters
are zero; both nodes sync. Both final live kernel IDs match
`46f608cce52899aad75757b707ccc548d2d4ac24` and the verification session closes.
The owned launcher exits zero. Host verification at
2026-09-16T00:07:56.232746+00:00 confirms both owned VMs and launcher are absent
and no WOS VM remains. Normal/isolated builds and KTEST were not repeated:
there is no production change and their 9LR evidence remains historical.
Current exec/compute source checks and policy audit/unit checks pass; the
complete policy gate still correctly rejects pending exec.

Failed experimental setups remain archived. The first dual-LAN configuration
omitted node3's already-declared identity mapping in its earlier LAN entry;
its effective policy rejected the root VFS request. Repeating the same declared
mapping there enabled the intended test. The initial owner-view assertion was
replaced by the actual File witness above. A malformed debugger batch failed
before connection and was corrected. The first fresh phase found its source
fixture missing after reboot cleared `/tmp`; recreating those files passed
without rerunning or overwriting the successful control/reset evidence. An
independent SFTP procfs read returned empty content; ordinary bounded local
reads then provided the cleanup observations. None of these failed attempts
is counted as a passing phase or a production defect fixed by this stage.

This closes the previously unqualified forced in-flight retirement case with
a real retained remote VFS dependency. It does not measure an individual
remote-close acknowledgement, exercise concurrent scheduler posting, or prove
the historical 9FZ/9LG unexplained observations fixed. Exec remains pending
until the final requirement-level reconciliation; optional transport expansion
is not an automatic requirement. Manifest counts remain **11 enforced / 10
pending**. Evidence: `build/goal19-inflight-exec-retirement/validation.json`.

## 9LT final exec authorization reconciliation

The exec operation is now **enforced** for Goal19's credential, authorization,
publication and associated lifetime contract. The manifest contains **12 enforced
and 9 pending domains**; `vfs-traversal` is first pending. This is a bounded
completion decision for exec, not completion of the goal, the general WKI
resource lifecycle or every remote I/O feature. Earlier pending statements in
this document describe the evidence available at those stages.

| Obligation | Current source and qualification | Decision |
| --- | --- | --- |
| One immutable original identity | `Snapshot`, `CredentialStore`, `prepare_exec_credentials` and the retained request snapshot preserve all IDs, groups, ceiling, NNP and grants. Preparation compares the current owner with the admitted value. | Qualified by credential foundation and current native tests; exec consumers preserve the snapshot contract. |
| Saved-ID/set-ID/no-regain transition | `Snapshot::prepare_exec` saves resulting effective IDs, applies set-ID only when permitted and preserves restrictions. `exec_policy::identity_auxv` derives loader identity from the resulting snapshot. | 9KR–9KT repairs, 9LB identity/auxv cases and 9LR's current-build 54-case main/worker/spawn matrix qualify this transition. |
| Original executable/interpreter authority | Native admission checks execute permission with the captured original identity. Remote mapping separately admits original and replacement snapshots before image work; original mapped credentials govern file/interpreter loading. Retained File/content views protect admitted input. | 9KX interpreter denial controls, file-stability qualification and original-read identity controls cover this decision; publication does not substitute the new identity for original access. |
| Atomic native publication and refusal | `commit_native_exec_image` revalidates credentials/stack identity under existing publication locks, requires quiescence for authority changes, then commits private image ownership and one-shot routing. | Existing native concurrency, signal and denial tests plus 9KZ routing controls qualify unchanged state on refusal. |
| Source remote cutover | `prepare_remote_exec_owner` and `cutover_remote_exec_owner` retain admission and recheck current credentials/tracing; signals drain before terminal handoff. Candidate metadata remains private until cutover. | 9LQ exercises six actual cutover refusals and success; 9LB observes 256 private-candidate refusals; 9HQ/9HR cover worker signal admission. |
| Receiver publication and failure cleanup | Original/replacement mapping precedes construction. `post_admitted_remote_process` requires the retained peer lease, exact sealed session/channel, no matching cancellation and an unexpired deadline. Private cleanup drops lease/compute lock before final File teardown. | 9LR's ten native scenarios qualify the actual gate/cleanup; 9LS's four nonexpired in-flight retirements prove actual remote-File private cleanup and fresh recovery. |
| Terminal ownership after source cutover | Coordinator committed-wait ownership prevents fallback into the old image after irreversible cutover; receiver rejection/retirement has an owner. | 9KV coordinator tests, 9LQ owner tests, 9LR cleanup tests and 9LS retained-File retirement jointly cover this boundary. |
| Authenticated mapping and version refusal | Supplied IDs are not ambient worker authority. Sealed identity/channel generations remain admission inputs; missing mapping and incompatible strict versions refuse. | 9KU version controls, 9LF policy allow/deny/restore, 9LG/9LH accepted-running reboot controls and 9LS prepublication reboot cover the exec slice. General WKI identity/resource domains remain pending. |
| Root/cwd and declared routing | Capture retains supported cwd authority, owner-return transfer revalidates grants, and unsupported owned-root/third-owner transfers refuse before binding/grants. Receiver-root preparation remains private. | Current source and filesystem KTESTs preserve refusal and retained ownership; explicit LOCAL `/` selects receiver root as the user requested. General VFS traversal remains a separate pending domain. |
| Inherited state and completion lifetime | CLOEXEC, retained IPC File ownership, output receipts and submitted/receiver task references follow their existing commit/retirement owners. | Existing descriptor, finite stream and receiver cleanup tests support lifetime preservation. General socket/transport equivalence is outside this exec completion decision. |

The review found no remaining demonstrated missing exec authority decision,
mutable credential decision or partial denied publication after these fixes and
qualifications. It does not infer proof of every possible scheduling interleaving
from ordered tests. In particular, 9LS proves retained remote File ownership and
completed receiver teardown; it does not observe a particular remote-close ACK.

Historical **9FZ** remains an unexplained completion observation after a
multi-hop UID100 worker handoff: ready identity and signal query were correct,
completion was not observed, a later capture missed the caller stack, and the
exact tasks were subsequently confirmed absent. Historical **9LG** remains an
unexplained pre-readiness interpreter fetch timeout; failed source PIDs and
submitted/pending completion state were subsequently confirmed absent. Later
passing runs do not diagnose either event. Neither observation establishes a
missing identity decision or retained unauthorized task. Their general
signal/WKI progress investigation remains open in the existing pending domains;
if reproduced, capture the blocked caller, sealed session/channel and pending
operation before cleanup. The earlier 9EC fetch timeout also remains historical
availability evidence, not a newly claimed fix. Do not delete these records or
count failed runs as passing controls.

No production code or runtime artifact changes here. This stage verifies source
and kernel hash continuity, the current policy/manifest audit and its unit suite,
and that the complete gate advances to `vfs-traversal`. The full 59,057/0 KTEST
and 54-case identity matrix belong to 9LR; the 12-phase in-flight matrix belongs
to 9LS. No new build, KTEST or VM session is claimed for this metadata-only review.
The exact original goal string and `--no-setup` authorization are unchanged.

Evidence: `build/goal19-exec-final-reconciliation/validation.json` and
`contract.md`; current production/live evidence remains indexed by the preceding
9LQ, 9LR and 9LS validation records. The next implementation audit starts at VFS
traversal and must identify a concrete missing decision or qualification against
requirement5 before changing code.
