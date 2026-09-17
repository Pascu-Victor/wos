# Goal 31 direct timeout consumer audit

This is a bounded source audit of the local tree, not a claim that every wait in
WOS has been converted or has a measured expiry bound. The process wait subset
below was subsequently migrated. Goal 31 contract 8 allows a common helper or a documented
reason for retaining a specialized timer; polling that can hang is listed as a
remaining gap, not described as a valid expiry guarantee.

## Entrypoints and existing safe arithmetic

| Subsystem / primary sites | Existing deadline and completion contract | Disposition |
| --- | --- | --- |
| `platform/sched/scheduler.hpp`: `saturating_deadline_us`, `kern_sleep_us_impl`, `preemptible_syscall_park_impl`; `scheduler.cpp`: wait timer queue and extraction | Relative daemon sleep saturates the absolute microsecond deadline; process park accepts an absolute deadline. Zero means event-only wait. Event/signal wake and timer extraction preserve scheduler ownership. | Common scheduling seam; do not independently allocate a second timeout callback for a task. |
| `syscalls_impl/futex/futex.cpp`: `relative_timeout_us`, `deadline_from_now_us`, `futex_wait` | User timespec validated, nanoseconds rounded upward to microseconds, multiplication checked, addition saturates; waiter cleanup remains tied to futex task state. | Already checked. Arithmetic can delegate to the common helper without changing futex cleanup or sentinel semantics. |
| `syscalls_impl/net/sys_net.cpp`: `poll_deadline_after_ms`, `begin_poll_timeout`, poll wait loop | Timeout conversion/addition checked; absolute deadline persists across wake/retry via `poll_wait_deadline_us`. | Already checked. Keep one deadline across spurious wakes; do not restart a relative timeout. |
| `net/wki/timer_math.hpp`, `wki.cpp`: `wki_wait_for_op`, wait timeout scan and retransmit deadlines | WKI saturating arithmetic, atomically claimed completion, stack waiter unlink before return; process wait may park, daemon may block, deferred/timer caller must drive transport progress. | Retain specialized completion/lifetime protocol; generic callback cancellation alone is insufficient. Shared arithmetic implementation can replace duplicate math independently. |
| `net/wki/dev_proxy.cpp`: block slot acquisition, operation/RDMA deadlines, fence timeout tick | Saturating WKI deadlines; proxy active/fenced checks, operation cookies and channel identity protect completion; fence-lift wait yields to reconnect/teardown work. | Specialized device and transport progress. Do not block while holding the per-proxy state lock or remove fence teardown as timeout owner. |
| `net/proto/tcp.hpp`: `tcp_deadline_after_ms`; `tcp_timer.cpp`: active TCB timer list and deferred retransmission | Checked millisecond addition; worker snapshots under `timer_lock`, releases before TCB protocol work; bounded deferred retransmission batch retains TCB ownership. | Keep protocol timer/cancellation and lock ordering; converting arithmetic does not justify changing TCP retry/ACK semantics. |
| `dev/virtio/virtio_net.cpp`: `future_deadline_ms`, control ACK wait | Local checked addition, device control completion and polling ownership. | Arithmetic already safe; specialize device completion until its reset/queue lifetime is integrated explicitly. |
| `platform/ntp/ntp.cpp`: receive timeout and retry daemon | Saturating monotonic receive deadline; scheduler sleeps between polls/retries. | Uses common arithmetic and scheduler parking; no wall-clock deadline dependence. |

Read sources included these entrypoints and their directly referenced helper
bodies, WKI `wki.hpp`/`wire.hpp`, clock arithmetic, xHCI request wait/timeout
ownership, AHCI engine/completion loops, NDP and IPv6 DAD timer transitions,
and process remote-wait/exit cadence. WKI wire v3 and authenticated trailer are
present locally; older navigation notes are not authoritative wire descriptions.

## Minimal arithmetic migration set

Implementation contract for the process wait subset: change only the three
deadline additions in `waitpid.cpp` and retired-thread park cadence in `exit.cpp`
to `sched::saturating_deadline_us`. Read the complete local/remote wait loop,
claim/copy/commit/close and interruption paths. Preserve all locks, owner pins,
retry constants, cancellation and wire semantics; add no blocking/allocation.
Existing arithmetic boundary tests plus process/WKI wait guards and kernel
build/selftests validate this mechanically equivalent change. Rollback is the
four callsites. These four additions are now migrated; the other items below
remain specialized as documented.

The following raw additions should delegate to
`platform/ktime/arithmetic.hpp::saturating_add` (or an existing unit-named wrapper)
while preserving all current wait and cancellation behavior:

- `dev/usb/xhci.cpp`: command, control and finite bulk deadlines (currently around
  lines 258, 1169 and 1265). Preserve `UINT64_MAX` idle bulk wait sentinel and
  request-table generation/timeout claim ownership. `wait_for_request` checks an
  absolute deadline and yields; it is not a general scheduler callback.
- `syscalls_impl/process/waitpid.cpp`: initial/retry remote admission deadline and
  `NOW + (handle == 0 ? 10000 : 1000)` polling cadence. Preserve remote claimed
  child-event commit and interruption behavior.
- `syscalls_impl/process/exit.cpp`: 10 ms `thread_exit` park cadence (around 495).
- `net/netif.cpp`: IPv6 DAD probe/retry deadlines (`now_ms + 1000` and `+100`).
- `net/proto/ndp.cpp`: reachable-state lifetime, delay-to-probe and probe retry
  deadlines (around 348, 387, 405, 453 and 470). Preserve fixed cache scan,
  `ndp_lock` state transitions, and dispatch of network work outside that lock.

These additions use short internal constants. With today's saturated
nanosecond clock, `get_us()` is at most `UINT64_MAX / 1000` and `get_ms()` at most
`UINT64_MAX / 1000000`, so these particular additions cannot overflow in a real
clock read. They remain useful consistency/fake-clock boundary migrations, not
an assertion of a currently reachable user-triggered overflow vulnerability.
Caller-controlled large futex/poll/WKI durations already have checked arithmetic.

Do not mechanically replace `routing_lease.hpp::begin_request` additions: it
checks that both deadlines fit exactly and rejects overflow before mutation.
`routing.cpp` discovery horizon also explicitly fails closed with zero on
addition overflow. A saturating deadline would change lease/security semantics.
Likewise, elapsed comparisons (`now - start >= timeout`) in shutdown/resource
coordination intentionally avoid constructing a future absolute deadline and
are valid for the selected nondecreasing local clock and a captured start.
They still need platform progress, and cannot promise expiry beyond the terminal
nanosecond saturation horizon.

## Driver polling bounds still needing separate work

AHCI polling is specialized because engine startup/recovery and command ownership
must remain valid before and after scheduler availability. This explains keeping
polling; it does not excuse missing elapsed-time bounds:

- `dev/ahci.cpp::stop_cmd` waits indefinitely for FR/CR to clear.
- `start_cmd` uses one million iterations; the normal command path uses fifty
  million completion iterations. These are CPU/load-dependent, not time bounds.
- `flush_disk` and `identify_disk` completion loops have no time deadline when
  CI remains set and no task-file error appears (around 637 and 803).
- `wait_for_completion_relax` yields only when scheduler run queues exist;
  otherwise it pauses. Port lock/slot/DMA ownership and error recovery must be
  audited before changing these waits; freeing an in-flight DMA buffer merely
  because a timeout elapsed would be unsafe.

A minimal follow-up requires an explicit AHCI recovery contract: monotonic
polling deadlines, a boot-safe progress path, verified controller stop/reset or
quarantine before releasing DMA storage, and fault-injected stuck-controller
checks. A generic timer callback alone cannot safely implement that contract.

## Verification and remaining evidence

This audit ran source searches and read the above call/helper paths. It did not
run a build, tests, device fault injection, or live timing measurements. Existing
host arithmetic/queue tests cover common helpers; each migrated subsystem still
needs its existing source/model checks and Normal Build WOS. Live device/peer
loss, spurious wake, and timer-load checks must establish actual expiry latency;
none is implied by this document.
