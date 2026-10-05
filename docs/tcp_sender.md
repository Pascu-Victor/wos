# TCP sender and recovery

`modules/kern/src/net/proto/tcp_sender.hpp` is the bounded, allocation-free
sender model. TCP output, receive ACK processing, and the timer integrate it
under `TcpCB::lock`. Both IP families use the same sender; endpoint, checksum,
path-MSS, and routing helpers preserve IPv4, mapped IPv4, and native IPv6 behavior.

## Policy and limits

`WOS_TCP_CONGESTION_CONTROL=ON` enables new-data congestion gating, NewReno
recovery, sender SACK hints, and zero-window persist. The CMake default is ON
after goal 29 qualification. An OFF build is a diagnostic control using
peer-window admission and the current safe oldest-head timer. It is not a
reproduction of the previous sender's whole-queue timeout behavior. Rollback
to the previous sender requires reverting the complete TCP patch (or using
the preserved original artifact), rather than mixing accounting states.
No socket syscall or TCP wire layout changes.

Flight is the unsigned difference `snd_nxt - snd_una`. Only cumulative ACKs
retire stored packets. Retransmissions and SACK hints never add new flight or
release receive/congestion credit. Data flight and `cwnd` are bounded at 1 MiB;
one terminal FIN byte may follow that budget. At most 256 retransmit entries
exist per connection, with one reserved for FIN. Small writes can therefore
exhaust the metadata budget before the byte budget and resume as ACKs retire
entries. The fixed SACK scoreboard holds 16 sorted, merged ranges.

The effective sender MSS is at least one byte and no larger than the peer's
offer, path payload capacity, or packet capacity. Native IPv6 PMTU updates
also reduce sender MSS for subsequent output and retransmission slicing.

## Congestion and loss transitions

The initial window follows the RFC 6928 IW10 bound:
`min(10*MSS, max(2*MSS, 14600))`, subject to the byte cap. ACKs of the handshake
do not grow this window. After an active SYN timeout, initial data credit is
conservatively one negotiated MSS. Successful handshake progress ends its
backoff epoch, allowing the first data timeout to compute a fresh threshold.
New data requires space in both the peer window and the congestion window.
Window updates use `snd_wl1` and `snd_wl2` freshness checks. Duplicate ACKs
require unchanged cumulative ACK, outstanding flight, and no payload/SYN/FIN/
RST. Either an unchanged advertised window or newly validated SACK coverage
supplies evidence, as permitted by [RFC 5681 section 2](https://www.rfc-editor.org/rfc/rfc5681.html#section-2).
Pure window updates without new SACK coverage do not count. This permits loss
detection when the receiver's OOO queue consumes window space while reporting
new SACK coverage.

Slow start adds at most one MSS per advancing ACK when congestion limited.
Congestion avoidance accumulates acknowledged-byte credits and adds one MSS
per window of progress. Application-limited and peer-window-limited transfers
do not grow the congestion window merely because ACKs arrived.

Three duplicate ACKs halve flight into `ssthresh`, with a two-MSS minimum,
record the recovery point, and request the first missing range. Additional
duplicate ACKs inflate credit within fixed byte/flight limits. A NewReno partial
ACK deflates by acknowledged bytes, adds one MSS if at least one MSS advanced,
and requests the next hole. A full recovery ACK exits to
`min(ssthresh, max(remaining_flight, MSS) + MSS)`. The recovery guard prevents
another reduction for the same epoch. This is conservative SACK-assisted
NewReno, rather than RFC 6675's complete `pipe` algorithm.

SACK options are parsed within the 60-byte TCP header bound. Hints are clipped
to cumulative flight, and overlapping/adjacent ranges merge without covering
gaps. On overflow the highest hint is discarded, leaving bytes eligible for
retransmission. Timer retransmissions are clipped to the hole, stored packet,
and current MSS. RTO clears all hints so receiver reneging cannot suppress the
cumulative head. Partial data ACKs also retire entries in FIN_WAIT_1, CLOSING,
and LAST_ACK, allowing recovery to reach the FIN.

## RTT, RTO, and persist

Karn's rule applies across the entire prefix covered by a cumulative ACK:
if any covered entry was retransmitted, none supplies an RTT sample. Samples
are bounded to 1–60000 ms. The existing WOS estimator retains its 200 ms
established RTO floor, 1000 ms initial RTO, and 60000 ms ceiling. This floor
is a WOS policy choice, not RFC 6298's recommended one-second floor.

RTO retries only the oldest unacknowledged range. Preparing a timeout retry
collapses `cwnd` to one MSS and doubles RTO; unavailable packets retry admission
after 10 ms without congestion/backoff changes. Eight prepared timeout retries
exhaust a head entry's timeout budget, even if later output admission fails.
A missing cumulative-head entry fails
the connection with ETIMEDOUT rather than retrying forever. Advancing ACKs
restart the remaining head's deadline and clear the backoff epoch.

A zero peer window suppresses RTO in ESTABLISHED/CLOSE_WAIT and arms persist
at 1 second, doubling up to 60 seconds. Probes retransmit one byte from the
oldest unacknowledged range. If a writer has data but no flight, TCP accepts
one byte as a bounded persist seed and returns that partial count; subsequent
writes wait or return EAGAIN through the existing socket mechanism. An empty
probe uses `snd_una - 1`. Window reopening wakes writers and cancels persist;
close/error cancels it as well. Allocation failure retries admission after
10 ms without increasing the probe interval.

A fresh zero-window ACK arms persist even if cumulative retirement leaves no
flight, so a writer waiting through poll does not depend on another send call.
Zero-window handshakes do the same after entering ESTABLISHED. Empty predecessor
probes receive current-window ACKs in ESTABLISHED, CLOSE_WAIT, and FIN_WAIT_1/2,
allowing half-closed streams to recover a lost reopening update.

The timer trace model treats time and packet admission as explicit inputs.
Its persist state is `(needed, deadline, interval, admitted_probes)`; zero
denotes an unarmed deadline, addition saturates, and all units are milliseconds.
This separates state transitions from allocation/output side effects. The
actual-source host harness replays these transitions with deterministic time
and successful, failed, or cancelled packet admission:

| Input event | Persist transition | Flight/congestion effect |
| --- | --- | --- |
| Accepted fresh zero window in a live stream, including handshake | Set needed; if unarmed, deadline = now + 1000 and interval = 1000 | None |
| Probe due and packet prepared | interval = min(2 × max(interval, 1000), 60000); deadline = now + interval | None |
| Probe due, spare unavailable | deadline = now + 10; retain interval | None |
| Prepared probe admitted in the same generation/window | Saturating increment of admitted_probes | No sequence/flight charge |
| Reopen before admission | Cancel the prepared probe; clear needed/deadline; interval = 1000 | Restore normal head RTO and wake writers |
| Close/error/generation change | Cancel pending/deferred probes | Preserve ordinary closing/terminal rules |

RTO uses `(head_deadline, rto, backoff_epoch, head_retry_budget)` with an
independent Karn marker per stored packet. Only prepared retries consume the
eight-attempt budget, collapse congestion state and double bounded RTO.
An unavailable spare advances the admission deadline by 10 ms without consuming
budget or backoff. Advancing cumulative ACKs retire the covered prefix, reset
the backoff epoch, and restart the remaining head deadline. RTT estimation is
disabled for the whole prefix if any covered Karn marker indicates a retry.

## Locks and ownership

TCB state and queue changes require `TcpCB::lock`. Timer membership owns a
TCB reference and publishes an atomic deadline. The timer snapshots at most
32 due references under its list lock, then drops that lock before taking
TCB locks. No opposite timer-list/TCB lock nesting is introduced.

Output allocates packet copies/metadata before taking the TCB lock and
publishes sequence/queue state before routing output. FIN commits its closing
state before output can produce an inline ACK. Output failures identify queue
entries by monotonic transmission IDs; rollback is allowed only for the latest
unacknowledged, unretried output in the same generation.

RX ACK spares use the existing packet pool before the TCB lock. Timer and
receive-window-update spares are allocated before the lock as well. Prepared
packets, copied endpoints, retained TCBs, and retained wake targets survive
ACK/close races. Emission and socket wakeups occur after releasing protocol
locks. Deferred actions check their connection generation and relevant state
before output/retry; an update racing after that check can leave one already
admitted packet in transit. CLOSED sockets retain their owning TCB until the
ordinary close operation; orphan FIN_WAIT_2 has bounded timer cleanup.

## Diagnostics and validation

`/proc/wki/netdiag` appends bounded `tcp_conn` rows containing `sender_enabled`,
`cwnd`, `ssthresh`, `flight`, duplicate ACKs, recovery state/reason, RTO,
`retransmit_packets`, `recovery_requests`, timeouts, SACK counts/drops, and
persist counters/deadline. `recovery_reason` values are 0 none, 1 fast
retransmit, 2 partial ACK, and 3 RTO. Recovery requests count model decisions;
retry/probe packet counters count successful IP-output admission, not remote
acknowledgment. Counters saturate rather than wrap. `perf tcp-report` displays
these connection rows and reports incomplete/truncated snapshots explicitly.
Native IPv6 endpoints use bracketed addresses with their scope ID; IPv4 and
mapped IPv4 retain the existing dotted address format.

The focused host targets cover sender transitions, existing TCP receive/FIN/
endpoint/deadline behavior, a stateful sender fuzzer, the impairment bridge,
and an ASan/UBSan harness extracting actual TCP output/ACK/timer/close bodies.
That harness substitutes clock, packet-pool, route/output, and wake seams;
it tests transition and ownership behavior rather than NIC execution. Seven
`TcpSender` KTEST cases execute the pure model inside the diagnostic VM.

Use Normal `Build WOS` for normal artifacts. Isolated selftests can run with:

```sh
bin/wos-ktest --fast --no-setup --config configs/node_ktest_rootless.json \
  --cmake-option=-DWOS_TCP_CONGESTION_CONTROL=ON
```

The private network runner builds two disposable node images, verifies the
packaged kernel hash, and launches through `bin/wos-cluster --no-setup`:

```sh
python3 scripts/bench/run_tcp_qualification.py --help
python3 scripts/bench/validate_tcp_qualification.py --help
```

Its userspace Ethernet bridge provides bounded loss/reorder/duplication/delay
and a shared rate limit without root setup. For matched clean-link comparisons,
`--clean-window-bytes 131072` optionally caps advertised peer credit equally
for both kernels. A fixed 128-flow table tracks both SYN window-scale offers;
ordinary ACK window words only decrease, and RFC 1624 incremental checksum
updates preserve the payload. SYN windows remain unscaled and zero windows
remain zero. Unknown/incomplete flows, malformed TCP, and table exhaustion fail
the receipt. This calibration is disabled by default; the impairment, persist,
and fairness scenarios use their ordinary peer windows.

Completion requires exact bytes
for identical IPv4/IPv6 impairments, repeated window reopening, packet balance,
and these gates fixed before measurement: median clean throughput
at least 95% of the preserved pre-goal kernel baseline, tiny-stream p95 latency
at most 110%, and Jain fairness at least 0.90 for two/four comparable competing
flows with at least 80% overlap. Each clean run has five 64 MiB bulk and twenty
64-byte tiny samples per family. The final predeclared campaign alternates
kernel order across five fresh matched pairs and pools all 25 bulk / 100 tiny
samples per kernel and family, requiring every run and excluding no sample.
VM performance evidence qualifies this fixture;
it does not establish physical NIC or Internet-path performance.

The preserved baseline is an existing pre-goal binary; its hash identifies the
artifact, but it does not establish an exact source revision. Qualification uses
the same benchmark binary, MTU, private rootfs base, bridge profile, sample counts,
and server timeout for both kernels. A current TCP-off build has the safe
oldest-head timer and therefore is not the original sender baseline.

Measured gates, artifact hashes, diagnostic receipts, and retained failures
are recorded in [tcp_sender_qualification.md](tcp_sender_qualification.md).
