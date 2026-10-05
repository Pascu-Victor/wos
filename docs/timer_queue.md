# Timer queue ownership

`platform/ktime/timer_queue.hpp` provides an intrusive AVL timer queue with
deferred callback tokens and a synchronous mode for the scheduler's per-CPU
wait index. Owners provide nodes and retain their callback contexts.

Queue operations allocate nothing and invoke no callbacks, blocking or logging.
Arm, cancel and bounded extraction may run in IRQ context under the caller's
IRQ-safe lock. That lock covers every queue/node operation, including claim and
finish; callbacks run in task context outside it. The core takes no locks.

Operations cost O(log n), peek is O(1), and extraction is bounded by the caller's
output span. Tree recursion is bounded by AVL height. Deferred transport is
caller-owned and preallocated. Node and callback context lifetimes must remain
retained until every extracted token is consumed, including stale tokens;
cancellation alone does not establish reclamation safety.

## State and dispatch contract

Each node transitions `idle -> armed -> ready -> running -> idle/armed`. Extracted tokens contain the node's generation. Arm invalidates a previous ready token; cancellation of armed/ready state returns `cancelled`. Cancellation of running state returns `running` and prevents periodic rearm, but cannot stop the callback. A worker claims under the queue lock, invokes the captured callback outside it, then finishes under the same lock. A failed claim still requires releasing the token's owner lifetime reference. Consumers must not copy a token into multiple independently reclaiming work items.

Periodic deadlines preserve their original phase and skip missed periods arithmetically. Saturated deadlines are allowed once; a periodic timer disarms when no strictly future deadline is representable. Generations never wrap: arm fails permanently for a node whose generation reached UINT64_MAX. A deferred node remains associated with its original queue so stale tokens cannot race cross-queue locks; queue lifetime must enclose every node association and outstanding token. Synchronous queues cannot extract deferred tokens, and pop/cancel releases association under the owning lock so scheduler migration can reuse the node safely.

The scheduler now replaces timeout scanning with at most 16 synchronous due-root extractions per IRQ. The generic deferred interface still requires a task-context worker supplied by its consumer; it does not establish end-to-end expiry latency by itself.

## Scheduler integration

The scheduler's original wait-list detach was O(n); it now provides constant-time unlink with an exact list-owner check. Shared dead-list and GC cursor behavior remain intact. After process timer service integration, the wait-index key is solely `wake_at_us`; REAL interval timers belong to the process service. The index is synchronized at attach, detach and migration, and when `publish_reserved_task_wakeup_locked` clears a deadline while the outgoing reserved task still has wait-list membership. Shutdown also clears deadlines before detach. Job-control and signal continue paths clear deadline fields outside these transition helpers and are checked at their subsequent scheduler ownership boundary.

Deferred cancellation does not drain extracted tokens. Scheduler integration therefore uses a separate synchronous mode with no outstanding tokens, retaining existing migration lock ordering. This leaves the deferred association rule intact.

`get_run_queue_stats()` now exposes timer queue depth, extraction count, extraction budget exhaustion, maximum indexed deadline lateness in microseconds, and the IRQ extraction budget of 16. Existing `wait_list_scan_*` diagnostics retain their names for readers but count inspected due roots rather than unrelated wait-list entries.

The same per-CPU IRQ-safe RunQueue lock protects the wait list and deadline
index. Migration retains CPU-ordered lock acquisition. The exact intrusive
list-owner pointer is atomic; links are accessed only under the proven owner
lock. Cancellation removes the timed index before migration or reclamation.
Each Task embeds its node and links, and each RunQueue embeds its index.

## Validation coverage

Existing host coverage includes 10,000 simultaneous deadlines with extraction
batches of 16, 100,000 randomized mutations against an ordered reference,
stale-token cancellation/rearm, callback ownership under a caller mutex,
periodic phase, terminal deadline saturation and synchronous cross-queue
transfer. Kernel selftests cover bounded storms and scheduler integration.
Historical integrated runtime results are recorded in
[clock/timer validation](timekeeping_validation.md); they do not validate the
current source. Generation exhaustion is guarded without exercising a node
through UINT64_MAX generations.
