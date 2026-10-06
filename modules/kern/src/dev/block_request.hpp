#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <dev/block_request_model.hpp>
#include <span>

namespace ker::dev {
struct BlockDevice;
struct BlockRequestQueue;
struct BlockRequest;
using BlockCompletion = void (*)(BlockRequest&, void*);

struct BlockSegment {
    void* data{};
    size_t size{};
};

struct BlockQueueLimits {
    size_t depth{1};
    size_t max_transfer_bytes{BLOCK_MODEL_MAX_REQUEST_BYTES};
    size_t max_segments{BLOCK_MODEL_MAX_SG};
    size_t max_requests{BLOCK_MODEL_MAX_REQUESTS};
    size_t max_inflight_bytes{size_t{8} * 1024 * 1024};
};

// Caller-owned descriptor and kernel buffers stay live through wait AND
// release. Submitted fields are immutable until release. completed is the
// acquire/release publication of status, bytes, and callback retirement.
// Callbacks execute in task context; they cannot wait on this queue or free
// their descriptor/buffers. A rejected submission takes no ownership.
struct BlockRequest {
    BlockRequest() = default;
    BlockRequest(const BlockRequest&) = delete;
    auto operator=(const BlockRequest&) -> BlockRequest& = delete;

    BlockRequestDescription description{};
    std::array<BlockSegment, BLOCK_MODEL_MAX_SG> segments{};
    size_t segment_count{};
    BlockRequestToken token{};
    uint64_t deadline_us{};
    int status{};
    size_t bytes{};
    std::atomic<bool> completed{false};
    BlockCompletion callback{};
    void* callback_arg{};

    // Runtime ownership; consumers and drivers must not modify these fields.
    BlockRequestQueue* queue{};
    uint64_t submitted_us{};
    void* waiter{};
};

// submit=0 retains request/SG until complete. Negative return takes no
// ownership; -EAGAIN is transient device backpressure. service and cancel run
// in task/boot context outside the queue guard. cancel must eventually retire
// transport access before completion, including uncertain DMA on timeout.
struct BlockAsyncOps {
    int (*submit)(BlockDevice*, BlockRequest&){};
    void (*service)(BlockDevice*){};
    void (*cancel)(BlockDevice*, BlockRequestToken, int){};
    // Optional extra-wait poll only. true retains every transport owner and
    // makes no claim or state change; false requires a fresh full service.
    // Mandatory service passes and all required POST passes ignore this hook.
    bool (*poll_active)(BlockDevice*){};
};

// Configuration/provider storage has kernel lifetime. All partition aliases
// resolve to this physical-root queue. nullptr ops selects the bounded serial
// adapter for legacy callbacks. Configuration allocates no request storage.
auto block_queue_configure(BlockDevice*, const BlockQueueLimits&, const BlockAsyncOps* = nullptr) -> int;
void block_request_start_workers();
auto block_queue_available(const BlockDevice*) -> bool;
// Configured queues retain a synchronous bridge after worker retirement.
auto block_queue_configured(const BlockDevice*) -> bool;
auto block_queue_limits(const BlockDevice*) -> BlockQueueLimits;
auto block_request_submit(BlockDevice*, BlockRequest&, BlockRequestOperation, uint64_t lba, size_t count, std::span<const BlockSegment>,
                          uint64_t timeout_us = 30'000'000, BlockCompletion = nullptr, void* = nullptr) -> int;
// Callback-free admission without a worker wake. The task/boot caller must
// service/wait before other blocking, return or unwind, and retain/join ALL
// accepted batch entries even when a later submission fails. With no accepted
// entry, use bounded capacity service on -EAGAIN. Public shutdown admission
// remains closed; only the private synchronous bridge serves finalizer I/O.
auto block_request_submit_for_wait(BlockDevice*, BlockRequest&, BlockRequestOperation, uint64_t lba, size_t count,
                                   std::span<const BlockSegment>, uint64_t timeout_us = 30'000'000) -> int;
auto block_request_wait(BlockRequest&) -> int;
auto block_request_release(BlockRequest&) -> int;
auto block_request_cancel(BlockRequest&, int reason = -ECANCELED) -> int;

// IRQ-safe bounded claim only. Provider MUST have retired all DMA/remote/CPU
// accesses first. Generation rejection is not a DMA quiescence proof.
auto block_request_complete(BlockDevice*, BlockRequestToken, int status, size_t bytes) -> bool;
// Kernel-internal task-service-only claim; never call from IRQ context. A
// matching retained PRE/POST service owner gathers it immediately after the
// provider returns, so only that claim's worker wake can be omitted. Outside
// that scope this retains the generic worker wake. Transport must be retired
// first; descriptor callbacks and other waiter wakes remain runtime-owned.
auto block_request_complete_from_service(BlockDevice*, BlockRequestToken, int status, size_t bytes) -> bool;
void block_queue_wake(BlockDevice*);
void block_queue_service(BlockDevice*);
// Task-context backpressure wait; services progress before a bounded park.
void block_queue_wait_for_capacity(BlockDevice*);
void block_queue_close(BlockDevice*);
auto block_queue_drain(BlockDevice*) -> int;
auto block_queue_begin_reset(BlockDevice*, int reason = -EIO) -> int;
auto block_queue_finish_reset(BlockDevice*, bool reopen = true) -> int;

// Synchronous bridge stages user buffers in their caller's address space;
// asynchronous submissions accept retained kernel SG only.
auto block_request_sync(BlockDevice*, BlockRequestOperation, uint64_t lba, size_t count, void* buffer) -> int;

#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
// Cumulative observations from returned wait calls, not distinct parks or
// immutable completion causes. Before saturation, bursts equal the sum of
// completed through count_cap. Cap elapsed samples require a monotonic end;
// their sum includes descheduling and does not measure exclusive CPU time.
struct BlockWaitDiagnostics {
    uint64_t bursts{};
    uint64_t completed{};
    uint64_t no_context{};
    uint64_t mandatory_busy{};
    uint64_t disallowed{};
    uint64_t clock_regression{};
    uint64_t budget{};
    uint64_t deadline{};
    uint64_t extra_busy{};
    uint64_t count_cap{};
    uint64_t extra_passes{};
    uint64_t count_cap_under_budget{};
    uint64_t cap_elapsed_us{};
    uint64_t cap_elapsed_max_us{};
    uint64_t park_attempts{};
};
#endif

struct BlockQueueSnapshot {
    std::array<char, 256> name{};
    BlockQueueLimits limits{};
    BlockQueueModelStats stats{};
    uint64_t generation{};
    size_t owned_requests{};
    size_t owned_bytes{};
    size_t queued{};
    size_t issued{};
    size_t completing{};
    size_t queued_bytes{};
    size_t issued_bytes{};
    size_t completing_bytes{};
    uint64_t flushes{};
    uint64_t latency_us{};
    uint64_t max_latency_us{};
    std::array<uint64_t, 16> latency_histogram{};
    bool admission_open{};
    bool async_admission_open{};
    bool worker_present{};
    bool caller_driven{};
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
    BlockWaitDiagnostics wait_diagnostics{};
#endif
};
auto block_queue_snapshots(std::span<BlockQueueSnapshot>) -> size_t;
}  // namespace ker::dev
