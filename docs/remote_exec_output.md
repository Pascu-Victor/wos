# Remote exec output completion

Goal 19 qualification found a reproducible output-loss gap (9ED, reproduced in
9EW before the fix). A remote process could report successful writes and exit
status 0 while its SSH client received
only a prefix of stdout or stderr. This is independent of procfs snapshot sizing.
The 9EW implementation below addresses message-flow-controlled output pipes.
The 9LL extension also qualifies PTY delivery completion and shared-endpoint
alias ordering; its final evidence is recorded below.

`tests/fixtures/service_exec_output.cpp` writes a deterministic byte pattern with
partial-write/EINTR handling, then writes an `EMITTED bytes=N` receipt to stderr.
`service_exec_output.py` runs it locally and through `on wos-3`, drains both
streams through EOF and exit status, and compares exact bytes and the receipt.
It preserves output bytes and hashes under the specified output directory.

The same final binary produced these immediate-exit results:

| Requested stdout bytes | Local bytes / receipt | Remote bytes / receipt | Remote status |
|---:|---|---|---:|
| 4,096 | 4,096 / complete | 4,096 / missing | 0 |
| 131,072 | 131,072 / complete | 0 / missing | 0 |
| 1,048,576 | 1,048,576 / complete | 1,048,576 / truncated | 0 |

With the optional diagnostic `--hold-ms 1000`, all six local/remote cases delivered
exact bytes and complete receipts. Removing the delay reproduced loss with the
same binary. An earlier immediate run also lost stdout tails: 61,440 of 131,072
bytes and 110,592 of 1,048,576 bytes arrived. The varying prefix length is part of
the failure, not an expected output limit. The delay is not a production fix.

## 9ED ordering and lifetime constraint

- `remote_ipc.cpp::proxy_pipe_write` reports bytes accepted by reliable IPC sends;
  the home-node deferred pipe worker can still have data to write locally.
- `proxy_pipe_close` sends the expected byte count on the IPC data channel and
  detaches its proxy. The export backlog's close fence waits for the expected
  data and its buffered chunks before dropping the real File.
- `remote_compute.cpp::wki_remote_compute_check_completions` waits for local task
  exit/accounting, then sends TASK_COMPLETE through the resource channel.
- The home-side `handle_task_complete` can finalize its proxy and publish a child
  exit while independent IPC delivery is still pending.
- Dropbear's `svr-chansession.c::sesscheckclose` enables flushing after child exit.
  `common-channel.c::check_close` then closes currently empty output pipes even
  without EOF. Its documented purpose is to avoid waiting for background children
  that inherited a pipe. Later WKI delivery can consequently lose its reader.

The source ordering and held/immediate control identify a completion-ordering
problem. No production fix is claimed. A fix must preserve the finite prefix of
successful writes before publishing the corresponding child exit. It must not
wait for every inherited writer to reach EOF: that changes background child and
shared-descriptor lifetime. It also must not wait while holding the exclusive peer
lifecycle lease needed by incoming IPC progress. Blanket sleeps, hiding the error
in the SSH collector, or treating all inherited pipes as part of root-task exit
would not satisfy the goal. A bounded delivery receipt or equivalent retained
per-completion write accounting needs a complete ownership/retirement contract
before editing the critical path.

## 9EW message-pipe completion

The current baseline reproduced the immediate-exit failure: the remote 1 MiB
case returned status 0 with only 192,512 bytes and no stderr receipt. All local
controls passed. Artifacts are under `build/goal19-exec-output-receipts/baseline`.

The implementation counts accepted application bytes at reliable channel enqueue,
under the channel lock. Counting after `wki_send` returned could describe a
different order when multiple writers returned out of order. Delivery advances
only on existing application-credit receipts after a complete chunk reaches the
home pipe; local credit refunds never advance that counter.

A completed chunk retains its DATA storage and credit in the existing backlog
until its receipt enters the reliable send queue. Failed attempts requeue it
without duplicating or losing credit. Terminal write errors are retained there
too. DATA for a retired export retains its deferred work until the EPIPE response
is enqueued or peer cleanup retires that work. This replaces the old bounded
receipt-send loop, whose exhaustion could otherwise strand the new completion
barrier after a temporary transport failure.

Each remote root monitor owns a bounded set of proxy-state references, with no
extra File references. Close still makes the endpoint inactive and sends close;
only receipt lookup remains available while a completion owner exists. After the
existing exit/accounting-ready point, the monitor freezes a finite accepted-byte
target and checks it on later timer scans. It holds no peer lifecycle lease while
waiting. Later inherited writers cannot extend the target. Reader failure or peer
retirement is terminal; the actual program exit status is preserved. Rejection,
scheduler-publication failure, completion, and peer cleanup release the retained
state through their existing ownership paths.

The native regression also has two stdin handshakes. An inherited writer keeps
stdout/stderr open until the collector observes root exit and closes stdin. An
explicit-close case waits for an input byte that the collector sends only after
output EOF. These test finite completion and close-before-exit behavior without
using a delay as a fix. The isolated helper test covers failed/successful receipt
enqueue, partial receipts, frozen targets, refunds, close, EPIPE, peer cleanup,
and exact proxy reclamation. Retry injection here is deterministic helper testing,
not a claim that live packet loss was injected.

Wire layout, credential policy, syscall ABI, and write errno conventions are
unchanged. This work covers message-flow-controlled pipes. PTY, optional RDMA,
and fallback output-capture lifetime remain separate audit work, as do the
intermittent executable-fetch timeout and remaining exec lifecycle cases.

Final 9EW validation passed Normal Build WOS, the isolated diagnostic build,
52,164 KTESTs with zero failures, six focused source/audit suites, four rebuilt
host models, and formatting. All ten live byte/handshake controls and all 66
native credential groups passed on the final kernel. Both guests were synced and
stopped. Final logs, hashes, the scoped patch, and the ownership review are under
`build/goal19-exec-output-receipts`; the exec domain remains pending for the
separate work listed above.

## 9EX fallback capture lifetime

The fallback stdout/stderr Files previously shared a raw capture buffer. Their
close callback did nothing, while root completion deleted the buffer even when a
forked child retained a File. Concurrent appends, length queries and completion
copies also accessed mutable capture state without synchronization.

Each successfully installed output File now owns a capture reference independently
of the root monitor. Final File close and each monitor retirement path release
only their own reference. Failed unpublished FD installation acquires no File
references and still rolls back every inserted slot. A per-capture IRQ-safe lock
serializes bounded appends and length reads. Writes reach it through the existing
VFS kernel bounce, so it performs no usercopy, allocation or blocking under that
lock. Root completion freezes the payload after exit/accounting readiness; later
writes preserve the existing successful-count/overflow convention without changing
the immutable payload. It retains the existing combined 4096-byte capacity and
never waits for inherited writers to close.

The actual kernel helper checks both ownership-release orders, inherited late
writes, exact final reclamation, overflow, freeze stability and insertion rollback.
The native fixture uses non-tty DEVFS `/dev/null` redirection to select fallback
capture, runs concurrent stdout/stderr writers, and forks a child released only
after remote root completion. That child then writes, closes both descriptors,
and reports through a separate file. Reaping is checked with native lstat on the
peer; an initial collector attempt using remote procfs metadata was unsupported.
The original run was verified and cleaned up explicitly, and the corrected fixture
passed on the same cluster with complete cleanup.

Normal Build WOS, the isolated build, all 52,165 KTESTs, six focused source/audit
suites, the policy audit, four rebuilt host models and formatting passed. The
corrected native capture fixture, all ten prior pipe byte/handshake controls and
all 66 native credential groups passed. Both guests were synced and stopped;
artifacts, hashes and the ownership review are in
`build/goal19-exec-capture-lifetime`.

No credential policy, authenticated peer policy, resource contract, syscall ABI or
wire layout changed. Socket writes also use asynchronous OP_PIPE_DATA enqueue,
so socket, PTY and optional RDMA completion remain unqualified. The older
intermittent executable-fetch timeout and remaining exec lifecycle work remain
pending; the authority manifest stays at 8 enforced / 13 pending.

## Other 9ED evidence

All four consumer restart/disconnect cases passed in one fresh cluster run.
Each retained the independent local lease, retired owner views 0 -> 1 -> 0,
terminated the old command with status 255, and read replacement bytes in a fresh
consumer. Same-boot disconnects preserved boot epochs. No fresh command remained
pending for the observer's three-second threshold, so the earlier intermittent
remote executable-fetch timeout was not reproduced and remains unresolved.

The standalone native fixture compiles, Python compilation and formatting pass,
and the authority audit passes with zero documented legacy root checks. The first
standalone compile needed the existing libc++ include path explicitly. There is
no matching clang-tidy compilation unit for this fixture. No production kernel
source changed, so Normal Build WOS and isolated KTEST were not rerun; current
normal/isolated hashes still match the 9EC artifacts. Prior KTEST results remain
historical evidence, not tests performed in 9ED.

Artifacts: `build/goal19-exec-fetch-observation`, including `run/results.json`,
`output-immediate-run`, `output-held-run`, and their logs. Both guests were synced,
the descriptor archived, validated QMP quit succeeded, and task PIDs 1037490 and
1037487 were confirmed absent. Goal 19 remains active at 6 enforced / 15 pending.


## 9EZ input EOF follow-up

The remote signal fixture exposed an independent input-pipe EOF race: a one-byte
write followed immediately by close sometimes reached a remote read as EOF.
The reader could load an empty ring before DATA and then observe CLOSE after
DATA. The speculative closure path now enters the existing locked ring and
pending-queue recheck before returning EOF. No output receipt or wire format
changed. The corrected signal matrix passed four 12-case runs with immediate
gate close retained; all 10 existing exact-output/inherited-writer/explicit-close
controls and fallback capture controls also passed. Evidence is under
`build/goal19-signal-owner/`. These runs do not qualify the remaining socket,
PTY or optional RDMA completion paths.


### 9LK: PTY completion loss and alias ordering reproduced

The previous9LJ turn made verified implementation/runtime progress. This stage returns to pending exec completion and adds explicit --pty and --pty-receipt modes to service_exec_output.cpp/.py. Each disposable SSH PTY uses raw terminal settings so exact binary output is not confused with line-discipline translation. The default PTY mode retains the stderr receipt on its distinct FD; a separate stdout-receipt control isolates one stream. Existing pipe/inherited-writer/explicit-close modes are unchanged. Session metadata now records owned helper paths and test mode.

The current qualified9LJ kernel has two concrete PTY failures. Initial immediate alias cases deliver exact total byte counts, but remote131072/1048576 receipts appear after byte65536 instead of the final stdout byte. Removing the receipt yields the exact complete payload, identifying reorder rather than payload corruption. Held-exit alias controls still reorder. The separate immediate same-stream remote131072 case exits0 after94208of131093expected bytes and no receipt, proving truncation independently of alias ordering. All six held-exit same-stream controls pass. A final-binary immediate alias run reproduces failures at every remote size:4096 receipt truncated (4112of4115),131072 complete but reordered, and1MiB truncated to856064of1048598. Local controls all pass. These failed runs are preserved; neither a delay nor moving the receipt to stdout is a production fix. All ten existing pipe byte/handshake controls pass on the same pair.

Source tracing finds that only IPC_PIPE enables application flow receipts and WkiIpcOutputCompletion retention. PTY uses accounted asynchronous proxy_pipe_write but receives no completion receipt; its expected-byte close fence is separate from TASK_COMPLETE. Each FD alias has its own export ID/backlog, and output flush shards use resource_id modulo4. The immediate PTY path considers only that resource's backlog. This allows the shared terminal's streams to progress independently; a shared endpoint ordering mechanism must also cover queued/in-flight/partial writes and immediate delivery. Existing pty_file_identity_key distinguishes the actual master/slave endpoints, but is currently used only to deduplicate input pumps. Socket output remains separately unqualified. next-implementation-map.md records the remaining admission/receipt/ordering/retirement reads and required native/live tests before a critical production patch. No kernel change or fix is claimed in9LK.

Both live IDs/symbols match qualified9LJde64351a41c656b1d5678746fcec02e893101590 and WOSDBG closes before execution. Production and normal/diagnostic kernel hashes remain unchanged. Fixture cross-builds, formatting and Python syntax/help pass; IPC source and credential policy suites pass. Normal Build WOS and isolated KTEST were not rerun because only fixtures/docs/evidence changed;9LJ59048/0 is matching historical evidence, not a current-stage run. All owned output helpers are independently absent. Launcher55030 exits0 and exact-process audit finds no WOS VMs at2026-09-15T18:54:04.903566+00:00 (21:54:04 Europe/Bucharest, UTC+3).

Evidence: build/goal19-pty-output-completion/validation.json, all-ordering-analysis.json, strict failed byte artifacts under immediate/,single-stream/,held-alias/,final-alias/, passing held-single/ and pipe-controls/, verified-symbols.json, cleanup.json, down.json and next-implementation-map.md. Goal19 remains active11enforced/10pending. Credential, syscall, wire and peer policy are unchanged. Declared VFS routing remains the user requirement; exact goal text and --no-setup authorization are preserved. No packet campaign or penetration test ran.


### 9LL: PTY delivery completion and alias ordering qualified

Writable PTY imports now use the existing application-delivery flow flag, credits and finite WkiIpcOutputCompletion targets. Close still detaches the File; completion retains only proxy state until receipts reach the frozen accepted-byte target or a terminal error/peer retirement occurs. Legacy imports without the flag retain their existing behavior. The wire layout, syscall ABI, descriptor grants and authenticated peer decisions are unchanged.

Active exports of the same retained PTY endpoint share a resource-derived ordering group; master and slave remain distinct. Reliable RX admission links a fixed-pool DATA token before worker publication. The token survives backlog ownership transfer, partial writes and failed receipt enqueue, and is unlinked before storage reuse on delivery or cancellation. Ordered DATA preserves the recyclable pool reserve, including one-byte payloads. Workers scan their bounded queue for eligible work and sleep only when every queued entry is blocked. IPC-to-pool lock order is preserved, with no added allocation, VFS operation or blocking under those locks. The legacy inline PTY shortcut cannot consume flow-controlled output without a receipt.

Native tests exercise shared/distinct/retired endpoint groups, small-chunk transfer, partial delivery, receipt retry, cancellation, reserve admission and independent ready-work selection. Actual imports cover current/legacy/read-only/nonblocking flow flags. The existing finite completion, close, refund, receipt, EPIPE and peer cleanup cases now run for both pipes and PTYs. Normal Build WOS (RelWithDebInfo), isolated Debug build, scoped formatting/tidy and IPC/compute source checks pass. Final full KTEST passes59050/0 at468.629 and powers off468.630. The first implementation also passed59050/0 and all34 live cases, but per-blocked-entry sleeps made immediate remote1MiB cases take about31seconds. Its complete evidence is retained under first-implementation/. The final bounded scan removes that delay without weakening ordering.

Both final live IDs and symbols match553318dec851aff9784c107ecf5f6cc20934e14c; WOSDBG closes before execution. All four six-case PTY matrices pass: immediate aliases, immediate same-stream, held aliases and held same-stream. All ten original pipe byte/inherited-writer/explicit-close controls also pass. Fixture code and exact byte/receipt expectations are unchanged from9LK. Adjacent artifact timestamps put final immediate remote1MiB cases at0.840s and0.518s respectively; these are workload observations, not a controlled throughput benchmark. All five final helpers are independently absent, launcher93353 exits0, and exact-process audit finds no WOS VMs at2026-09-15T19:40:48.458978+00:00 (22:40:48 EEST, Europe/Bucharest).

Evidence: build/goal19-pty-delivery-transaction/validation.json, implementation.patch, ownership-review.md, first-implementation/validation.json, ktest-serial-final.log, matrices.json, exact byte artifacts, verified-symbols.json, cleanup.json and down.json. Goal19 remains active at11enforced/10pending. Socket and optional RDMA completion, remaining exec lifecycle/compatibility work, and the historical9LG fetch timeout/9FZ completion cause remain pending. Declared VFS routing remains authoritative, including explicit LOCAL / selecting the receiver root. The exact fenced goal and --no-setup run/debug authorization are preserved.


### 9LM: inherited socket close retirement repaired; shutdown ordering remains pending

A new ordinary loopback TCP fixture compares local/remote exec, write/send, close/SHUT_WR, sizes4096/131072/1048576 and root/UID100. The endpoint is created before the child drops identity and inherited across exec. The final UID100 emitter checks real/effective/saved UID/GID100, empty supplementary groups and EPERM with unchanged IDs on setuid(0)/setgid(0), then uses the retained descriptor. SHUT_WR requires all bytes before EOF and a peer reply on the surviving read side. All comparisons retain exact bytes, EOF and child status; a diagnostic hold is optional.

The qualified9LL baseline passes both local smoke cases and remote SHUT_WR, but remote close waits until the parent's60-second alarm despite the remote emitter reporting4096 accepted bytes. A second close run reproduces it. WOSDBG verifies both baseline IDs/symbols and closes; bounded GDB snapshots independently match build IDs and restore both VMs. Source export28683 is inactive, has file=null and pipe_bytes_received4096, and has pump_running=false, yet pumpPID51 remains in socket_fops_read/tcp_wait with its retained File. The source parent waits for TCP EOF after the remote child has exited. This identifies the last-File/input-pump close cycle. The capture helper's PORT placeholder also changed its EXPORT labels; raw logs are preserved and close-capture-summary.json normalizes those labels. The placeholder replacement is now scoped to the GDB endpoint.

The production patch changes only the native socket branch in the pump read loop. It uses the existing private nonblocking File view, leaving the shared File flags and Socket::nonblock untouched. Existing native fops turn that view into MSG_DONTWAIT. EAGAIN returns to the pump's outside-lock retry so stop/retirement can release the last File. Other pump types, descriptor/consumer/channel authorization, credentials, syscall ABI and wire layout are unchanged. No socket shutdown is used to cancel a shared endpoint.

Normal Build WOS, isolated diagnostic build, formatting/tidy and IPC source checks pass. Full KTEST passes59050/0 at468.241 and powers off468.242, including the existing private-view source-state invariant. Both final IDs/symbols matcha6a1545a86427ac460a4f83e6fdf2cff5af431c5; WOSDBG closes before execution. The48-case matrix passes45/48, including all24close cases. All24UID100 executions reach the accepted marker after the no-regain checks. The three failures are remote SHUT_WR: root131072/write receives126976bytes with childexit0; UID100131072/send receives8192bytes, and UID1001048576/write receives897024bytes, with both children timing out on the reply. All report every byte accepted; the source logs EPIPE on one4096-byte chunk in each failing operation. All16held-shutdown controls pass. These failures remain strict evidence that control-channel shutdown can overtake queued application writes; the hold is not a fix. Socket application-delivery accounting, shared-alias shutdown fencing and remaining read/compatibility contracts stay pending.

All ten existing pipe output/inherited-writer/explicit-close controls pass. Both baseline collectors report SFTP removal, but the separate baseline cleanup attempt receives SSH connection refusal before shutdown; its cause is unestablished. Final independent cleanup succeeds for every recorded helper path, launcher93348 exits0, and all owned processes/WOS VMs are absent at2026-09-15T20:17:31.866567+00:00 (23:17:31 EEST, Europe/Bucharest).

Evidence: build/goal19-socket-output-contract/validation.json, baseline/validation.json, smoke/, close-capture/, socket-close-capture.json, close-capture-summary.json, implementation.patch, patch-contract.md, ktest-serial.log, matrix/results.json, matrix-summary.json, held-shutdown/, pipe-controls/, verified-symbols.json, cleanup.json and down.json. next-socket-map.md records the remaining contracts, including alias-aware finite shutdown fences, speculative read EOF and pending-delivery/nonblocking reads. Goal19 remains active11enforced/10pending; declared VFS routing, exact fenced goal and --no-setup authorization are preserved. The historical9LG fetch timeout and9FZ completion cause remain unresolved.


### 9LN: stream socket shutdown waits for the accepted delivery prefix

Remote SHUT_WR/SHUT_RDWR now captures the next transmit sequence on the original DATA channel and appends that uint32 boundary after the existing int32 how. The home first observes dispatch through that boundary on its original DATA allocation, then waits for earlier work tokens belonging to the same native stream socket endpoint and channel to retire through application delivery. Distinct exported aliases of the same retained Socket share an opaque endpoint group; resource type also participates in grouping. Later sequences, other endpoints and stale channel generations do not extend the captured prefix. Datagram exports keep group zero and are not converted to stream delivery semantics. SHUT_RD retains the legacy four-byte request and does not wait for output.

The existing PTY fixed-pool token registry is generalized to endpoint delivery. Stream DATA retains its token through queued, in-flight, partial and terminal backlog delivery. Socket CLOSE also receives an ordered token: without it, the ready-work scan could skip blocked DATA and retire its export too early. Shutdown fence retries go to the worker queue back, allowing preceding DATA on the same shard to progress; DATA retries retain their original front placement. The retry policy is copied before handler ownership transfer. RX admission stays bounded/nonallocating under the existing channel -> IPC try-lock -> pool try-lock order. Worker channel snapshots occur without the IPC lock; grant revalidation and token scanning occur under IPC -> pool. Native shutdown runs outside those locks with the existing File pin and cleanup lease.

Invalid how returns EINVAL without a callback. Missing, wrong-consumer, retired or File-less exports retain ENOENT; a retired original DATA channel on an otherwise issued grant returns EHOSTUNREACH. Credentials, retained descriptor authority, shared File flags, Socket::nonblock, syscall ABI and declared VFS routing are preserved. Old homes ignore the optional request tail, and old senders omit it: no mixed-version ordering guarantee is claimed.

Normal Build WOS, isolated diagnostic build, formatting/tidy, policy audit/unit tests and IPC source checks pass. Final KTEST passes59051/0 at470.632 and powers off470.633, including the new native fixed-prefix/wrapped-sequence/stale-generation/close-order token test. Two earlier partial KTEST runs were intentionally stopped after source review identified the CLOSE ordering and missing-export errno corrections; their logs and exact down evidence are preserved in superseded/ and pre-errno/. They are not final validation.

Both final live kernels and symbols match59b4e823e05c71bbc3b46e107f0518093d09db8a; the debugger session closes before functional tests. All48 original strict socket cases pass, including the three9LM remote SHUT_WR truncation/reply failures. All24 inherited-duplicate cases pass: write through one descriptor, SHUT_WR through a separately exported alias, then exact bytes and EOF. These alias cases do not claim receive routing across duplicate pumps; the original single-descriptor shutdown cases continue to require a reply on the surviving read side. All16 invalid-how/SHUT_RD controls pass. All44 UID100 socket executions reach the accepted marker after real/effective/saved UID/GID100, empty supplementary groups and EPERM-with-unchanged-IDs checks for setuid(0)/setgid(0). No diagnostic hold is used.

All10 existing pipe byte/inherited-writer/explicit-close controls and12 PTY alias/same-stream controls pass, for110 live cases total. All six uploaded helpers are independently absent; launcher session45347 exits0. The exact-process audit finds no owned processes or WOS VMs at2026-09-15T21:02:30.647742+00:00 (2026-09-16 00:02:30 EEST, Europe/Bucharest).

Evidence: build/goal19-socket-shutdown-fence/validation.json, implementation.patch, patch-contract.md, ownership-review.md, ktest-serial.log, matrices.json, previous-failures.json, exact case artifacts, verified-symbols.json, cleanup.json, built-hashes.json and down.json. Goal19 remains active11enforced/10pending. Socket application flow credits/root completion retention, proxy read EOF/concurrent/nonblocking/overflow contracts, alias receive routing, optional RDMA/legacy qualification and remaining exec lifecycle obligations stay pending. Native token tests qualify fixed-prefix exclusion; live concurrent-writer cancellation is not claimed. The historical9LG fetch timeout and9FZ completion cause remain unresolved. Declared routing remains authoritative, including explicit LOCAL / selecting the receiver root. The exact fenced goal and --no-setup run/debug authorization remain unchanged.


### 9LO: native stream sockets join finite remote output completion

Writable native SOCK_STREAM exports now advertise the existing application-flow bit; datagram/raw exports retain the legacy path. Import initializes stream socket credits before publication and retains the proxy in the existing finite output-completion owner. The socket writer delegates flagged streams to the established accounted message writer, which reserves/refunds credits, counts accepted bytes inside reliable enqueue, honors File O_NONBLOCK and handles partial/error/interrupted writes. Existing DATA_FLOW and WRITE_FLOW messages carry home delivery receipts. Receipts count bytes accepted by the home File's native write path; TCP peer acknowledgement/application consumption are outside this counter.

Completion retains ProxyIpcState rather than File. Its target freezes once after root exit, so later inherited writes cannot extend that target; closed-proxy receipt lookup remains until delivery or terminal error/peer retirement. Socket POLLOUT now reflects available credits and terminal errors, with existing receipt wakeups; errors also report POLLERR. Home native socket writes use the existing private nonblocking File view, returning EAGAIN to the outside-lock backlog retry instead of parking the shared worker. Shared File flags, Socket::nonblock, original resource/consumer/channel authority, DATA/CLOSE endpoint ordering and the9LN shutdown boundary remain intact. Credentials, saved IDs, syscall/wire layouts and declared VFS routing are unchanged. Old unflagged imports keep the legacy writer; no mixed-version completion guarantee is claimed.

Native tests extend actual receipt-after-close, refund-versus-delivery, frozen targets, terminal EPIPE and peer-retirement coverage from pipes/PTYs to sockets. Actual socket imports test current/legacy/read-only/nonblocking flags and completion references, plus zero-credit EAGAIN without accepted-byte changes and credit/error-aware POLLOUT. A home socket mock callback observes O_NONBLOCK while the original File flags remain unchanged. Normal Build WOS, isolated diagnostic build, format/tidy, policy audit/unit tests and IPC source checks pass. Final KTEST passes59052/0 at461.504 and powers off461.505.

Both live IDs and symbols matchd9c72f0ca1882201563f9c19611efddca7972c30; the debugger closes before execution. Four new fixture smoke cases pass, followed by all48 full lifetime cases across root/UID100, local/remote, write/send and sizes4096/131072/1048576. In inherited mode, the root exits with a child holding the socket and a release pipe. The source reaps the root before releasing that pipe; the child rechecks its real/effective/saved identity and no-regain state, writes a required late socket byte, then closes. This proves completion is finite and the inherited descriptor remains usable afterward. Explicit-close mode requires EOF while the root is still blocked on its release pipe, detecting accidental File retention by completion.

All88 previous strict socket cases also pass (48ordinary,24duplicate shutdown,16invalid-how/SHUT_RD), as do10pipe and12PTY controls. Total:162 live cases including the four smoke cases. The full socket matrices include68 UID100 root executions with real/effective/saved UID/GID100, empty supplementary groups and EPERM-with-unchanged-IDs on attempts to regain root;12 UID100 descendants repeat those checks after root reaping before their required late write. Exact byte/EOF/reply checks and zero diagnostic hold are preserved. All eight uploaded helpers are independently absent; launcher session13467 exits0. Exact-process audit finds no owned processes or WOS VMs at2026-09-15T21:34:50.745391+00:00 (2026-09-16 00:34:50 EEST, Europe/Bucharest).

Evidence: build/goal19-socket-completion/validation.json, implementation.patch, patch-contract.md, ownership-review.md, ktest-serial.log, matrices.json, lifecycle/ and lifecycle-smoke/ exact artifacts, prior socket/pipe/PTY controls, verified-symbols.json, cleanup.json, built-hashes.json and down.json. Goal19 remains active11enforced/10pending. Socket read EOF/concurrency/nonblocking/overflow, alias receive routing, post-shutdown/peer-error syscall parity, onward proxy socket export, legacy/optional RDMA qualification and remaining exec lifecycle/credential obligations stay pending. Historical9LG fetch timeout and9FZ completion cause remain unresolved. Declared routing remains authoritative, including explicit LOCAL / selecting receiver root. The exact fenced goal and --no-setup run/debug authorization are preserved.

### 9LP: serialized inherited stream reads and bounded reader wakeups

Flow-advertised native stream socket proxies now share the pipe/PTY reader and poll path. A single IPC->proxy locked snapshot consumes older ring bytes before pending overflow and EOF, serializes concurrent tail publication, and registers waits without losing DATA/CLOSE wakeups. Copies are bounded to one DATA chunk. Completed chunk/work and pending-record ownership is released outside both locks. A fixed32 PID waiter list replaces the single raw Task slot for shared stream readers; full registration retries outside locks, and DATA, EOF, poll replies, detach and peer cleanup safely wake registered Tasks after unlocking. File O_NONBLOCK returns EAGAIN on empty open streams; zero-count reads return0. Shared poll observes pending bytes and EOF. Legacy unflagged sockets retain their previous path; distinct exported aliases still have separate receive queues. The old pipe reader only had the stronger EOF recheck: nonblocking and concurrent-reader protection were missing there too.

The first unchanged local receive smoke failed at45.018s with child status14 before readiness. Source inspection found that native socket File read dispatched zero count into TCP's blocking receive loop, whereas the recv syscall already returned0. The native File adapter now returns0 after existing File/protocol validation, without invoking protocol receive. Positive reads and all wire/TCP state remain unchanged. A native mock checks zero-count callback exclusion, positive-count dispatch and existing invalid-File/missing-protocol errors. The original failed run and its matched3de93424f2d47b9be9c5cbcb1aa06c25dd08458b symbols and59054/0 KTEST remain in before-zero/; the unchanged smoke passes after the correction.

Final Normal Build WOS and isolated diagnostic builds, scoped format/tidy, fixture cross-compile and syntax, policy audit/unit and IPC source checks pass. Final full KTEST passes 59055/0. Both final live IDs and symbols match478e99f77a6589150c0ffd8f951479a3f9d51742; WOSDBG closes before execution. All242 final live cases pass:4 receive smoke,80 full receive cases,136 previous socket output/lifetime/shutdown cases and22 pipe/PTY controls. Single-reader input checks exact order for4KiB/128KiB/1MiB;4/40 readers sharing one File check all128 unique bytes exactly once and EOF for every reader. Root/UID100, local/remote, read/recv, blocking/nonblocking are covered; all UID100 reader threads check saved IDs, empty groups and inability to regain root. No receive-alias equivalence is claimed.

All9 final helpers and the initial failed-run helper are independently absent. The final normal launcher exits0 and its pair is down before the final isolated selftest. Final exact-process audit confirms all owned/WOS VMs down at 2026-09-15T22:24:00.501438+00:00. Evidence: build/goal19-stream-read-contract/validation.json, patch-contract.md, ownership-review.md, implementation.patch, matrices.json, exact case artifacts, ktest-serial.log, verified-symbols.json, cleanup.json and down audits. Immutable credential/grant and resource/consumer/channel authority, finite output completion without File retention, source flags, DATA/CLOSE/shutdown order and declared VFS routing remain intact; explicit LOCAL / still selects the receiver root. The exact fenced goal and --no-setup run/debug authorization are preserved.

Goal19 stays active11enforced/10pending. Next is a bounded reconciliation of exec's actual credential/publication/lifetime requirements against current source and accumulated evidence, as mapped in exec-authority-audit-next.md, before treating more socket protocol completeness as a mandatory authorization task. Historical9LG fetch timeout and9FZ completion causes remain unresolved; ordinary protocol limitations and authorization/lifetime obligations must be distinguished without erasing evidence.
