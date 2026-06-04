#include "perf_events.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <platform/ktime/ktime.hpp>
#include <platform/smt/smt.hpp>
#include <string_view>

#include "platform/sys/spinlock.hpp"

namespace ker::mod::perf {

namespace {

// Recording is OFF by default - enabled explicitly via perf record / /proc/kperfctl.
// Stats counters (ctx_switches etc.) always increment regardless of this flag so
// /proc/kcpustat is always live and has zero overhead impact on the hot path.
std::atomic<bool> g_enabled{false};

// Event type mask: bitmask of PerfEventType/local scope values to record.
// Default: all types enabled. Only checked when g_enabled is true.
std::atomic<uint16_t> g_event_mask{PERF_MASK_ALL};

// Static ring buffers — dynamically allocated at init() time based on actual CPU count.
PerfCpuRing* g_rings = nullptr;
size_t g_num_cpus = 0;

// Sub-sampler: emit one SAMPLE per 10 timer ticks (~100 Hz at 1 kHz tick rate)
uint64_t* g_tick_count = nullptr;

// Per-subsystem aggregate statistics (always-on, lock-free via atomics).
// Total: ~13 subsystems * 48 bytes = ~624 bytes BSS.
std::array<PerfSubsystemStats, PERF_SUBSYSTEM_COUNT> g_subsys_stats{};
std::atomic<uint32_t> g_wki_trace_correlation{1};
std::atomic<uintptr_t> g_local_vmem_zero_page{0};

struct WkiPerfSummaryBucket {
    bool used = false;
    uint8_t scope = 0;
    uint8_t op = 0;
    uint16_t peer = 0;
    uint16_t channel = 0;
    uint64_t calls = 0;
    uint64_t errors = 0;
    uint64_t retries = 0;
    uint64_t bytes = 0;
    uint64_t latency_samples = 0;
    uint64_t total_latency_us = 0;
    uint32_t max_latency_us = 0;
    std::array<uint32_t, WKI_PERF_HIST_BUCKETS> latency_hist = {};
};

std::array<WkiPerfSummaryBucket, WKI_PERF_SUMMARY_BUCKETS> g_wki_summary{};
ker::mod::sys::Spinlock g_wki_summary_lock;

void perf_push_event(PerfCpuRing& ring, const PerfEvent& evt) {
    uint64_t const SLOT = ring.head & PERF_RING_MASK;
    ring.events[SLOT] = evt;
    ring.head++;
    ring.stats.ring_writes.fetch_add(1, std::memory_order_relaxed);
    if ((ring.head - ring.drain) > PERF_RING_ENTRIES) {
        ring.drain = ring.head - PERF_RING_ENTRIES;
    }
}

auto wki_scope_hash(uint8_t scope, uint8_t op, uint16_t peer, uint16_t channel) -> size_t {
    auto mixed = static_cast<uint32_t>(scope);
    mixed = (mixed * 131U) ^ static_cast<uint32_t>(op);
    mixed = (mixed * 65537U) ^ static_cast<uint32_t>(peer);
    mixed = (mixed * 65537U) ^ static_cast<uint32_t>(channel);
    return static_cast<size_t>(mixed % WKI_PERF_SUMMARY_BUCKETS);
}

auto wki_get_or_create_summary_bucket(uint8_t scope, uint8_t op, uint16_t peer, uint16_t channel) -> WkiPerfSummaryBucket* {
    size_t const SLOT = wki_scope_hash(scope, op, peer, channel);
    for (size_t probe = 0; probe < WKI_PERF_SUMMARY_BUCKETS; ++probe) {
        auto& bucket = g_wki_summary.at((SLOT + probe) % WKI_PERF_SUMMARY_BUCKETS);
        if (!bucket.used) {
            bucket.used = true;
            bucket.scope = scope;
            bucket.op = op;
            bucket.peer = peer;
            bucket.channel = channel;
            bucket.calls = 0;
            bucket.errors = 0;
            bucket.retries = 0;
            bucket.bytes = 0;
            bucket.latency_samples = 0;
            bucket.total_latency_us = 0;
            bucket.max_latency_us = 0;
            bucket.latency_hist.fill(0);
            return &bucket;
        }
        if (bucket.scope == scope && bucket.op == op && bucket.peer == peer && bucket.channel == channel) {
            return &bucket;
        }
    }
    return nullptr;
}

auto wki_hist_bucket(uint32_t latency_us) -> size_t {
    if (latency_us <= 1) {
        return 0;
    }

    size_t bucket = 0;
    uint32_t value = latency_us;
    while (value > 1 && bucket + 1 < WKI_PERF_HIST_BUCKETS) {
        value >>= 1U;
        bucket++;
    }
    return bucket;
}

auto wki_hist_bucket_value(size_t bucket) -> uint32_t {
    if (bucket == 0) {
        return 1;
    }
    if (bucket >= 31) {
        return 1U << 31U;
    }
    return 1U << static_cast<uint32_t>(bucket);
}

auto wki_hist_percentile(const std::array<uint32_t, WKI_PERF_HIST_BUCKETS>& hist, uint64_t total, uint32_t numer, uint32_t denom)
    -> uint32_t {
    if (total == 0 || denom == 0) {
        return 0;
    }

    uint64_t threshold = ((total * static_cast<uint64_t>(numer)) + (denom - 1U)) / static_cast<uint64_t>(denom);
    if (threshold == 0) {
        threshold = 1;
    }

    uint64_t cumulative = 0;
    for (size_t i = 0; i < hist.size(); ++i) {
        cumulative += hist.at(i);
        if (cumulative >= threshold) {
            return wki_hist_bucket_value(i);
        }
    }

    return wki_hist_bucket_value(hist.size() - 1);
}

auto wki_phase_flags(WkiPerfPhase phase) -> uint8_t {
    switch (phase) {
        case WkiPerfPhase::BEGIN:
            return PERF_FLAG_WKI_BEGIN;
        case WkiPerfPhase::END:
            return PERF_FLAG_WKI_END;
        case WkiPerfPhase::POINT:
        default:
            return PERF_FLAG_WKI_POINT;
    }
}

auto wki_is_launch_measurement_event(WkiPerfScope scope, uint8_t op) -> bool {
    if (scope != WkiPerfScope::REMOTE_COMPUTE) {
        return false;
    }

    switch (static_cast<WkiPerfComputeOp>(op)) {
        case WkiPerfComputeOp::SUBMIT_INLINE:
        case WkiPerfComputeOp::SUBMIT_VFS_REF:
        case WkiPerfComputeOp::COMPLETE_WAIT:
        case WkiPerfComputeOp::ACCEPT:
        case WkiPerfComputeOp::REJECT:
        case WkiPerfComputeOp::COMPLETE:
        case WkiPerfComputeOp::PROXY_READY:
        case WkiPerfComputeOp::DEFER_WAIT:
        case WkiPerfComputeOp::LOAD_ELF:
        case WkiPerfComputeOp::HANDLE_SUBMIT:
        case WkiPerfComputeOp::TASK_RUNTIME:
        case WkiPerfComputeOp::PROXY_READY_WAIT:
        case WkiPerfComputeOp::COMPLETE_HOLD:
            return true;
        default:
            return false;
    }
}

auto wki_should_record(uint16_t mask, WkiPerfScope scope, uint8_t op) -> bool {
    if ((mask & PERF_MASK_WKI) != 0U) {
        return true;
    }

    if (((mask & PERF_MASK_WKI_LAUNCH) != 0U) && wki_is_launch_measurement_event(scope, op)) {
        return true;
    }

    switch (scope) {
        case WkiPerfScope::LOCAL_PIPE:
            return (mask & PERF_MASK_LOCAL_PIPE) != 0U;
        case WkiPerfScope::LOCAL_PROC:
            return (mask & PERF_MASK_LOCAL_PROC) != 0U;
        case WkiPerfScope::LOCAL_VMEM:
            return (mask & PERF_MASK_LOCAL_VMEM) != 0U;
        case WkiPerfScope::LOCAL_LOADER:
            return (mask & PERF_MASK_LOCAL_LOADER) != 0U;
        case WkiPerfScope::LOCAL_XFS:
            return (mask & PERF_MASK_LOCAL_XFS) != 0U;
        case WkiPerfScope::LOCAL_IRQ:
            return (mask & PERF_MASK_LOCAL_IRQ) != 0U;
        default:
            return false;
    }
}

}  // namespace

const char* subsystem_name(PerfSubsystem s) {
    switch (s) {
        case PerfSubsystem::FD_TABLE:
            return "fd_table";
        case PerfSubsystem::FUTEX:
            return "futex";
        case PerfSubsystem::DEVICE_REG:
            return "device_reg";
        case PerfSubsystem::BLOCK_DEV:
            return "block_dev";
        case PerfSubsystem::PTY_POOL:
            return "pty_pool";
        case PerfSubsystem::MOUNT_TABLE:
            return "mount_table";
        case PerfSubsystem::PIPE_WAITQ:
            return "pipe_waitq";
        case PerfSubsystem::ACCEPT_Q:
            return "accept_q";
        case PerfSubsystem::NAPI_REG:
            return "napi_reg";
        case PerfSubsystem::VFS_RULES:
            return "vfs_rules";
        case PerfSubsystem::EXIT_WAITERS:
            return "exit_waiters";
        case PerfSubsystem::PTY_WAITERS:
            return "pty_waiters";
        default:
            return "unknown";
    }
}

const char* wki_scope_name(WkiPerfScope scope) {
    switch (scope) {
        case WkiPerfScope::TRANSPORT:
            return "transport";
        case WkiPerfScope::REMOTE_VFS:
            return "remote_vfs";
        case WkiPerfScope::REMOTE_VFS_SERVER:
            return "remote_vfs_srv";
        case WkiPerfScope::REMOTE_IPC:
            return "remote_ipc";
        case WkiPerfScope::LOCAL_PIPE:
            return "local_pipe";
        case WkiPerfScope::LOCAL_PROC:
            return "local_proc";
        case WkiPerfScope::LOCAL_VMEM:
            return "local_vmem";
        case WkiPerfScope::LOCAL_LOADER:
            return "local_loader";
        case WkiPerfScope::LOCAL_XFS:
            return "local_xfs";
        case WkiPerfScope::LOCAL_IRQ:
            return "local_irq";
        case WkiPerfScope::REMOTE_COMPUTE:
            return "remote_compute";
        case WkiPerfScope::EVENT_BUS:
            return "event_bus";
        default:
            return "unknown";
    }
}

const char* wki_phase_name(WkiPerfPhase phase) {
    switch (phase) {
        case WkiPerfPhase::BEGIN:
            return "begin";
        case WkiPerfPhase::END:
            return "end";
        case WkiPerfPhase::POINT:
            return "point";
        default:
            return "unknown";
    }
}

const char* wki_op_name(WkiPerfScope scope, uint8_t op) {
    switch (scope) {
        case WkiPerfScope::TRANSPORT:
            switch (static_cast<WkiPerfTransportOp>(op)) {
                case WkiPerfTransportOp::SEND:
                    return "send";
                case WkiPerfTransportOp::ACK_RTT:
                    return "ack_rtt";
                case WkiPerfTransportOp::RETRANSMIT:
                    return "retransmit";
                case WkiPerfTransportOp::FAST_RETRANSMIT:
                    return "fast_retransmit";
                case WkiPerfTransportOp::NO_CREDITS:
                    return "no_credits";
                case WkiPerfTransportOp::WAIT:
                    return "wait";
                case WkiPerfTransportOp::STALL:
                    return "stall";
                case WkiPerfTransportOp::RDMA_WRITE:
                    return "rdma_write";
                default:
                    return "unknown";
            }
        case WkiPerfScope::REMOTE_VFS:
            switch (static_cast<WkiPerfVfsOp>(op)) {
                case WkiPerfVfsOp::ATTACH_WAIT:
                    return "attach_wait";
                case WkiPerfVfsOp::PROXY_WAIT:
                    return "proxy_wait";
                case WkiPerfVfsOp::OPEN:
                    return "open";
                case WkiPerfVfsOp::STAT:
                    return "stat";
                case WkiPerfVfsOp::READ:
                    return "read";
                case WkiPerfVfsOp::READDIR:
                    return "readdir";
                case WkiPerfVfsOp::WRITE:
                    return "write";
                case WkiPerfVfsOp::SEEK:
                    return "seek";
                case WkiPerfVfsOp::TRUNCATE:
                    return "truncate";
                case WkiPerfVfsOp::READLINK:
                    return "readlink";
                case WkiPerfVfsOp::CLOSE:
                    return "close";
                case WkiPerfVfsOp::MKDIR:
                    return "mkdir";
                case WkiPerfVfsOp::UNLINK:
                    return "unlink";
                case WkiPerfVfsOp::RMDIR:
                    return "rmdir";
                case WkiPerfVfsOp::RENAME:
                    return "rename";
                case WkiPerfVfsOp::RETRY:
                    return "retry";
                default:
                    return "unknown";
            }
        case WkiPerfScope::REMOTE_VFS_SERVER:
            switch (static_cast<WkiPerfVfsServerOp>(op)) {
                case WkiPerfVfsServerOp::RX:
                    return "rx";
                case WkiPerfVfsServerOp::REPLY_SEND:
                    return "reply_send";
                case WkiPerfVfsServerOp::OPEN:
                    return "open";
                case WkiPerfVfsServerOp::STAT:
                    return "stat";
                case WkiPerfVfsServerOp::READ:
                    return "read";
                case WkiPerfVfsServerOp::READDIR:
                    return "readdir";
                case WkiPerfVfsServerOp::WRITE:
                    return "write";
                case WkiPerfVfsServerOp::SEEK:
                    return "seek";
                case WkiPerfVfsServerOp::TRUNCATE:
                    return "truncate";
                case WkiPerfVfsServerOp::READLINK:
                    return "readlink";
                case WkiPerfVfsServerOp::CLOSE:
                    return "close";
                case WkiPerfVfsServerOp::MKDIR:
                    return "mkdir";
                case WkiPerfVfsServerOp::UNLINK:
                    return "unlink";
                case WkiPerfVfsServerOp::RMDIR:
                    return "rmdir";
                case WkiPerfVfsServerOp::RENAME:
                    return "rename";
                default:
                    return "unknown";
            }
        case WkiPerfScope::REMOTE_IPC:
            switch (static_cast<WkiPerfIpcOp>(op)) {
                case WkiPerfIpcOp::PROXY_READ:
                    return "proxy_read";
                case WkiPerfIpcOp::PROXY_WRITE:
                    return "proxy_write";
                case WkiPerfIpcOp::PTY_IOCTL:
                    return "pty_ioctl";
                case WkiPerfIpcOp::SOCK_CTRL:
                    return "sock_ctrl";
                case WkiPerfIpcOp::PIPE_PUMP_READ:
                    return "pipe_pump_read";
                case WkiPerfIpcOp::PIPE_PUMP_SEND:
                    return "pipe_pump_send";
                case WkiPerfIpcOp::PIPE_DATA:
                    return "pipe_data";
                case WkiPerfIpcOp::DEV_OP_QUEUE:
                    return "dev_op_queue";
                case WkiPerfIpcOp::DEV_OP_HANDLE:
                    return "dev_op_handle";
                case WkiPerfIpcOp::WAKE_READER:
                    return "wake_reader";
                case WkiPerfIpcOp::POLL_WAKE:
                    return "poll_wake";
                case WkiPerfIpcOp::EPOLL_CTL:
                    return "epoll_ctl";
                default:
                    return "unknown";
            }
        case WkiPerfScope::LOCAL_PIPE:
            switch (static_cast<WkiPerfLocalPipeOp>(op)) {
                case WkiPerfLocalPipeOp::READ:
                    return "read";
                case WkiPerfLocalPipeOp::WRITE:
                    return "write";
                case WkiPerfLocalPipeOp::BLOCK_READ:
                    return "block_read";
                case WkiPerfLocalPipeOp::BLOCK_WRITE:
                    return "block_write";
                case WkiPerfLocalPipeOp::WAKE_READERS:
                    return "wake_readers";
                case WkiPerfLocalPipeOp::WAKE_WRITERS:
                    return "wake_writers";
                case WkiPerfLocalPipeOp::DIRECT_RESERVE:
                    return "direct_reserve";
                case WkiPerfLocalPipeOp::DIRECT_BEGIN:
                    return "direct_begin";
                case WkiPerfLocalPipeOp::DIRECT_READ:
                    return "direct_read";
                case WkiPerfLocalPipeOp::DIRECT_COMMIT:
                    return "direct_commit";
                default:
                    return "unknown";
            }
        case WkiPerfScope::LOCAL_PROC:
            switch (static_cast<WkiPerfLocalProcOp>(op)) {
                case WkiPerfLocalProcOp::FORK:
                    return "fork";
                case WkiPerfLocalProcOp::EXECVE:
                    return "execve";
                case WkiPerfLocalProcOp::ELF_READ:
                    return "elf_read";
                case WkiPerfLocalProcOp::FIRST_RUN:
                    return "first_run";
                case WkiPerfLocalProcOp::ARG_COPY:
                    return "arg_copy";
                case WkiPerfLocalProcOp::OPEN_ACCESS:
                    return "open_access";
                case WkiPerfLocalProcOp::REMOTE_SPAWN:
                    return "remote_spawn";
                case WkiPerfLocalProcOp::NEW_IMAGE:
                    return "new_image";
                case WkiPerfLocalProcOp::LOAD_ELF:
                    return "load_elf";
                case WkiPerfLocalProcOp::LOAD_INTERP:
                    return "load_interp";
                case WkiPerfLocalProcOp::STACK_SETUP:
                    return "stack_setup";
                case WkiPerfLocalProcOp::COMMIT:
                    return "commit";
                case WkiPerfLocalProcOp::DESTROY_OLD:
                    return "destroy_old";
                case WkiPerfLocalProcOp::WAITPID:
                    return "waitpid";
                case WkiPerfLocalProcOp::EXIT:
                    return "exit";
                default:
                    return "unknown";
            }
        case WkiPerfScope::LOCAL_VMEM:
            switch (static_cast<WkiPerfLocalVmemOp>(op)) {
                case WkiPerfLocalVmemOp::ANON_MMAP:
                    return "anon_mmap";
                case WkiPerfLocalVmemOp::FILE_MMAP:
                    return "file_mmap";
                case WkiPerfLocalVmemOp::ZERO_PAGE_MAP:
                    return "zero_page_map";
                case WkiPerfLocalVmemOp::FILE_CACHE_HIT:
                    return "file_cache_hit";
                case WkiPerfLocalVmemOp::FILE_CACHE_MISS:
                    return "file_cache_miss";
                case WkiPerfLocalVmemOp::FILE_CACHE_FILL:
                    return "file_cache_fill";
                case WkiPerfLocalVmemOp::FILE_CACHE_EVICT:
                    return "file_cache_evict";
                case WkiPerfLocalVmemOp::COW_ZERO:
                    return "cow_zero";
                case WkiPerfLocalVmemOp::COW_COPY:
                    return "cow_copy";
                case WkiPerfLocalVmemOp::COW_PROMOTE:
                    return "cow_promote";
                default:
                    return "unknown";
            }
        case WkiPerfScope::LOCAL_LOADER:
            switch (static_cast<WkiPerfLocalLoaderOp>(op)) {
                case WkiPerfLocalLoaderOp::PT_LOAD_MAIN:
                    return "pt_load_main";
                case WkiPerfLocalLoaderOp::PT_LOAD_INTERP:
                    return "pt_load_interp";
                case WkiPerfLocalLoaderOp::FINAL_PERMS_MAIN:
                    return "final_perms_main";
                case WkiPerfLocalLoaderOp::FINAL_PERMS_INTERP:
                    return "final_perms_interp";
                default:
                    return "unknown";
            }
        case WkiPerfScope::LOCAL_XFS:
            switch (static_cast<WkiPerfLocalXfsOp>(op)) {
                case WkiPerfLocalXfsOp::READ:
                    return "read";
                case WkiPerfLocalXfsOp::WRITE:
                    return "write";
                case WkiPerfLocalXfsOp::READ_BMAP:
                    return "read_bmap";
                case WkiPerfLocalXfsOp::READ_IO:
                    return "read_io";
                case WkiPerfLocalXfsOp::WRITE_BMAP:
                    return "write_bmap";
                case WkiPerfLocalXfsOp::WRITE_ALLOC:
                    return "write_alloc";
                case WkiPerfLocalXfsOp::WRITE_IO:
                    return "write_io";
                case WkiPerfLocalXfsOp::WRITE_ILOG:
                    return "write_ilog";
                case WkiPerfLocalXfsOp::WRITE_HOLE_ITER:
                    return "write_hole_iter";
                case WkiPerfLocalXfsOp::WRITE_MAP_ITER:
                    return "write_map_iter";
                case WkiPerfLocalXfsOp::DIRECT_READ:
                    return "direct_read";
                case WkiPerfLocalXfsOp::DIRECT_WRITE:
                    return "direct_write";
                case WkiPerfLocalXfsOp::BUFFERED_READ:
                    return "buffered_read";
                case WkiPerfLocalXfsOp::BUFFERED_WRITE:
                    return "buffered_write";
                case WkiPerfLocalXfsOp::BUF_READ_HIT:
                    return "buf_read_hit";
                case WkiPerfLocalXfsOp::BUF_READ_MISS:
                    return "buf_read_miss";
                case WkiPerfLocalXfsOp::BUF_GET_HIT:
                    return "buf_get_hit";
                case WkiPerfLocalXfsOp::BUF_GET_MISS:
                    return "buf_get_miss";
                case WkiPerfLocalXfsOp::BUF_DISK_READ:
                    return "buf_disk_read";
                case WkiPerfLocalXfsOp::BUF_DISK_WRITE:
                    return "buf_disk_write";
                case WkiPerfLocalXfsOp::BUF_DIRTY:
                    return "buf_dirty";
                case WkiPerfLocalXfsOp::BUF_DISCARD:
                    return "buf_discard";
                case WkiPerfLocalXfsOp::INODE_FETCH:
                    return "inode_fetch";
                case WkiPerfLocalXfsOp::INODE_CACHE_HIT:
                    return "inode_cache_hit";
                case WkiPerfLocalXfsOp::INODE_CACHE_MISS:
                    return "inode_cache_miss";
                case WkiPerfLocalXfsOp::INODE_UNAVAILABLE:
                    return "inode_unavailable";
                case WkiPerfLocalXfsOp::SYNC_BLOCKDEV:
                    return "sync_blockdev";
                case WkiPerfLocalXfsOp::BUF_FLUSH:
                    return "buf_flush";
                case WkiPerfLocalXfsOp::BUF_ALLOC:
                    return "buf_alloc";
                case WkiPerfLocalXfsOp::READ_COPY:
                    return "read_copy";
                case WkiPerfLocalXfsOp::READ_ZERO:
                    return "read_zero";
                case WkiPerfLocalXfsOp::READ_GAP:
                    return "read_gap";
                default:
                    return "unknown";
            }
        case WkiPerfScope::LOCAL_IRQ:
            switch (static_cast<WkiPerfLocalIrqOp>(op)) {
                case WkiPerfLocalIrqOp::HANDLER:
                    return "handler";
                default:
                    return "unknown";
            }
        case WkiPerfScope::REMOTE_COMPUTE:
            switch (static_cast<WkiPerfComputeOp>(op)) {
                case WkiPerfComputeOp::SUBMIT_INLINE:
                    return "submit_inline";
                case WkiPerfComputeOp::SUBMIT_VFS_REF:
                    return "submit_vfs_ref";
                case WkiPerfComputeOp::COMPLETE_WAIT:
                    return "complete_wait";
                case WkiPerfComputeOp::ACCEPT:
                    return "accept";
                case WkiPerfComputeOp::REJECT:
                    return "reject";
                case WkiPerfComputeOp::COMPLETE:
                    return "complete";
                case WkiPerfComputeOp::PROXY_READY:
                    return "proxy_ready";
                case WkiPerfComputeOp::DEFER_WAIT:
                    return "defer_wait";
                case WkiPerfComputeOp::LOAD_ELF:
                    return "load_elf";
                case WkiPerfComputeOp::HANDLE_SUBMIT:
                    return "handle_submit";
                case WkiPerfComputeOp::TASK_RUNTIME:
                    return "task_runtime";
                case WkiPerfComputeOp::PROXY_READY_WAIT:
                    return "proxy_ready_wait";
                case WkiPerfComputeOp::COMPLETE_HOLD:
                    return "complete_hold";
                default:
                    return "unknown";
            }
        case WkiPerfScope::EVENT_BUS:
            switch (static_cast<WkiPerfEventOp>(op)) {
                case WkiPerfEventOp::SUBSCRIBE:
                    return "subscribe";
                case WkiPerfEventOp::UNSUBSCRIBE:
                    return "unsubscribe";
                case WkiPerfEventOp::PUBLISH:
                    return "publish";
                case WkiPerfEventOp::ACK:
                    return "ack";
                case WkiPerfEventOp::RETRY:
                    return "retry";
                case WkiPerfEventOp::REPLAY:
                    return "replay";
                default:
                    return "unknown";
            }
        default:
            return "unknown";
    }
}

void init() {
    g_num_cpus = ker::mod::smt::get_core_count();
    if (g_num_cpus == 0) {
        g_num_cpus = 1;
    }
    g_rings = new PerfCpuRing[g_num_cpus]{};
    g_tick_count = new uint64_t[g_num_cpus]{};
    for (size_t i = 0; i < g_num_cpus; ++i) {
        g_rings[i].head = 0;
        g_rings[i].drain = 0;
        g_tick_count[i] = 0;
        g_rings[i].stats.reset();
    }
    g_enabled.store(false, std::memory_order_release);  // Off by default; enabled via perf record
    g_event_mask.store(PERF_MASK_ALL, std::memory_order_release);
    g_wki_trace_correlation.store(1, std::memory_order_release);
    for (auto& g_subsys_stat : g_subsys_stats) {
        g_subsys_stat.inserts.store(0, std::memory_order_relaxed);
        g_subsys_stat.removes.store(0, std::memory_order_relaxed);
        g_subsys_stat.resizes.store(0, std::memory_order_relaxed);
        g_subsys_stat.oom_failures.store(0, std::memory_order_relaxed);
        g_subsys_stat.peak_count.store(0, std::memory_order_relaxed);
        g_subsys_stat.current_count.store(0, std::memory_order_relaxed);
    }
    for (auto& bucket : g_wki_summary) {
        bucket.used = false;
        bucket.scope = 0;
        bucket.op = 0;
        bucket.peer = 0;
        bucket.channel = 0;
        bucket.calls = 0;
        bucket.errors = 0;
        bucket.retries = 0;
        bucket.bytes = 0;
        bucket.latency_samples = 0;
        bucket.total_latency_us = 0;
        bucket.max_latency_us = 0;
        bucket.latency_hist.fill(0);
    }
}

bool is_enabled() { return g_enabled.load(std::memory_order_acquire); }
void enable() { g_enabled.store(true, std::memory_order_release); }
void disable() { g_enabled.store(false, std::memory_order_release); }

size_t get_num_perf_cpus() { return g_num_cpus; }

void reset_rings() {
    // IRQ-safe reset of all ring head/drain pointers.
    // Stats counters are NOT cleared - they accumulate across sessions for /proc/kcpustat.
    for (size_t i = 0; i < g_num_cpus; ++i) {
        auto& ring = g_rings[i];
        auto saved = ring.lock.lock_irqsave();
        ring.head = 0;
        ring.drain = 0;
        ring.lock.unlock_irqrestore(saved);
    }

    auto saved = g_wki_summary_lock.lock_irqsave();
    for (auto& bucket : g_wki_summary) {
        bucket.used = false;
        bucket.scope = 0;
        bucket.op = 0;
        bucket.peer = 0;
        bucket.channel = 0;
        bucket.calls = 0;
        bucket.errors = 0;
        bucket.retries = 0;
        bucket.bytes = 0;
        bucket.latency_samples = 0;
        bucket.total_latency_us = 0;
        bucket.max_latency_us = 0;
        bucket.latency_hist.fill(0);
    }
    g_wki_trace_correlation.store(1, std::memory_order_release);
    g_wki_summary_lock.unlock_irqrestore(saved);
}

// ---------------------------------------------------------------------------
// record_sample
// ---------------------------------------------------------------------------
void record_sample(uint32_t cpu, uint64_t pid, uint64_t rip, bool user_mode, int64_t lag_v) {
    if (cpu >= g_num_cpus) {
        return;
    }

    // Sub-sample to ~100 Hz (tick count advances even when recording is off
    // so the phase is stable when recording is enabled mid-run).
    auto& ring = g_rings[cpu];
    ring.stats.samples.fetch_add(1, std::memory_order_relaxed);
    uint64_t const TICK = ++g_tick_count[cpu];
    if (!g_enabled.load(std::memory_order_acquire)) {
        ring.stats.fastpath_skips.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    auto mask = g_event_mask.load(std::memory_order_relaxed);
    if (((mask & PERF_MASK_SAMPLE) == 0) || ((TICK % 10) != 0)) {
        ring.stats.fastpath_skips.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    auto saved = ring.lock.lock_irqsave();
    PerfEvent evt{};
    evt.ts_ns = ker::mod::time::get_monotonic_ns();
    evt.pid = pid;
    evt.data = rip;
    evt.lag_v = lag_v;
    evt.cpu = static_cast<uint16_t>(cpu);
    evt.type = static_cast<uint8_t>(PerfEventType::SAMPLE);
    evt.flags = user_mode ? PERF_FLAG_USER_MODE : 0U;
    perf_push_event(ring, evt);
    ring.lock.unlock_irqrestore(saved);
}

// ---------------------------------------------------------------------------
// record_switch
// ---------------------------------------------------------------------------
void record_switch(uint32_t cpu, uint64_t prev_pid, uint64_t next_pid, uint8_t flags, int64_t lag_v, uint32_t run_us, uint64_t callsite) {
    if (cpu >= g_num_cpus) {
        return;
    }

    auto& ring = g_rings[cpu];
    auto& s = ring.stats;
    s.ctx_switches.fetch_add(1, std::memory_order_relaxed);
    if ((flags & PERF_FLAG_PREEMPT) != 0) {
        s.preemptions.fetch_add(1, std::memory_order_relaxed);
    }
    if ((flags & PERF_FLAG_YIELD) != 0) {
        s.yields.fetch_add(1, std::memory_order_relaxed);
    }
    if ((flags & PERF_FLAG_BLOCK) != 0) {
        s.sleeps.fetch_add(1, std::memory_order_relaxed);
    }

    if (!g_enabled.load(std::memory_order_acquire)) {
        s.fastpath_skips.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    auto mask = g_event_mask.load(std::memory_order_relaxed);
    if ((mask & PERF_MASK_SWITCH) == 0) {
        s.fastpath_skips.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    auto saved = ring.lock.lock_irqsave();
    PerfEvent evt{};
    evt.ts_ns = ker::mod::time::get_monotonic_ns();
    evt.pid = prev_pid;
    evt.data = next_pid;
    evt.callsite = callsite;
    evt.lag_v = lag_v;
    evt.cpu = static_cast<uint16_t>(cpu);
    evt.type = static_cast<uint8_t>(PerfEventType::SWITCH);
    evt.flags = flags;
    evt.aux = run_us;
    perf_push_event(ring, evt);
    ring.lock.unlock_irqrestore(saved);
}

// ---------------------------------------------------------------------------
// record_wake
// ---------------------------------------------------------------------------
void record_wake(uint32_t cpu, uint64_t pid, uint64_t wake_at_us, uint8_t flags, uint32_t sleep_us, uint64_t callsite,
                 const char* wait_channel) {
    if (cpu >= g_num_cpus) {
        return;
    }

    auto& ring = g_rings[cpu];
    ring.stats.wakes.fetch_add(1, std::memory_order_relaxed);
    if (!g_enabled.load(std::memory_order_acquire)) {
        ring.stats.fastpath_skips.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    auto mask = g_event_mask.load(std::memory_order_relaxed);
    if ((mask & PERF_MASK_WAKE) == 0) {
        ring.stats.fastpath_skips.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    auto saved = ring.lock.lock_irqsave();
    PerfEvent evt{};
    evt.ts_ns = ker::mod::time::get_monotonic_ns();
    evt.pid = pid;
    evt.data = wake_at_us;
    evt.callsite = callsite;
    evt.lag_v = static_cast<int64_t>(reinterpret_cast<uint64_t>(wait_channel));
    evt.cpu = static_cast<uint16_t>(cpu);
    evt.type = static_cast<uint8_t>(PerfEventType::WAKE);
    evt.flags = flags;
    evt.aux = sleep_us;
    perf_push_event(ring, evt);
    ring.lock.unlock_irqrestore(saved);
}

// ---------------------------------------------------------------------------
// record_sleep
// ---------------------------------------------------------------------------
void record_sleep(uint32_t cpu, uint64_t pid, uint64_t wake_at_us, uint8_t flags, uint32_t run_us, uint64_t callsite,
                  const char* wait_channel) {
    if (cpu >= g_num_cpus) {
        return;
    }

    auto& ring = g_rings[cpu];
    ring.stats.sleeps.fetch_add(1, std::memory_order_relaxed);
    if (!g_enabled.load(std::memory_order_acquire)) {
        ring.stats.fastpath_skips.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    auto mask = g_event_mask.load(std::memory_order_relaxed);
    if ((mask & PERF_MASK_SLEEP) == 0) {
        ring.stats.fastpath_skips.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    auto saved = ring.lock.lock_irqsave();
    PerfEvent evt{};
    evt.ts_ns = ker::mod::time::get_monotonic_ns();
    evt.pid = pid;
    evt.data = wake_at_us;
    evt.callsite = callsite;
    evt.lag_v = static_cast<int64_t>(reinterpret_cast<uint64_t>(wait_channel));
    evt.cpu = static_cast<uint16_t>(cpu);
    evt.type = static_cast<uint8_t>(PerfEventType::SLEEP);
    evt.flags = flags;
    evt.aux = run_us;
    perf_push_event(ring, evt);
    ring.lock.unlock_irqrestore(saved);
}

// ---------------------------------------------------------------------------
// drain_events - called from procfs read (process context, may sleep)
// ---------------------------------------------------------------------------
size_t drain_events(PerfEvent* dst, size_t max_events, uint32_t cpu_filter) {
    size_t total = 0;
    for (uint32_t c = 0; c < g_num_cpus && total < max_events; ++c) {
        if (cpu_filter < g_num_cpus && c != cpu_filter) {
            continue;
        }

        auto& ring = g_rings[c];
        auto saved = ring.lock.lock_irqsave();
        while (ring.drain < ring.head && total < max_events) {
            dst[total++] = ring.events[ring.drain & PERF_RING_MASK];
            ring.drain++;
        }
        ring.lock.unlock_irqrestore(saved);
    }
    return total;
}

// ---------------------------------------------------------------------------
// get_cpu_stats - non-destructive snapshot
// ---------------------------------------------------------------------------
PerfCpuStats get_cpu_stats(uint32_t cpu) {
    if (cpu >= g_num_cpus) {
        return {};
    }
    return g_rings[cpu].stats.snapshot();
}

// ---------------------------------------------------------------------------
// record_container_stat
// ---------------------------------------------------------------------------
void record_container_stat(uint32_t cpu, uint64_t pid, PerfSubsystem subsystem, uint32_t instance_id, uint8_t flags, int64_t element_count,
                           uint32_t capacity, uint64_t callsite) {
    // Always update subsystem atomic stats
    update_subsystem_stat(subsystem, flags, static_cast<uint64_t>(element_count >= 0 ? element_count : 0));

    if (cpu >= g_num_cpus) {
        return;
    }

    auto mask = g_event_mask.load(std::memory_order_relaxed);
    bool const DO_RECORD = g_enabled.load(std::memory_order_acquire) && ((mask & PERF_MASK_CONTAINER) != 0);

    if (!DO_RECORD) {
        return;
    }

    auto& ring = g_rings[cpu];
    auto saved = ring.lock.lock_irqsave();

    PerfEvent evt{};
    evt.ts_ns = ker::mod::time::get_monotonic_ns();
    evt.pid = pid;
    // Pack subsystem (upper 32b) + instance_id (lower 32b) into data
    evt.data = (static_cast<uint64_t>(static_cast<uint8_t>(subsystem)) << 32) | static_cast<uint64_t>(instance_id);
    evt.callsite = callsite;
    evt.lag_v = element_count;  // reuse: current element count
    evt.cpu = static_cast<uint16_t>(cpu);
    evt.type = static_cast<uint8_t>(PerfEventType::CONTAINER_STAT);
    evt.flags = flags;
    evt.aux = capacity;

    perf_push_event(ring, evt);

    ring.lock.unlock_irqrestore(saved);
}

bool is_wki_recording_enabled() {
    if (!g_enabled.load(std::memory_order_acquire)) {
        return false;
    }

    auto mask = g_event_mask.load(std::memory_order_relaxed);
    return (mask & (PERF_MASK_WKI | PERF_MASK_WKI_LAUNCH | PERF_MASK_LOCAL)) != 0U;
}

bool is_wki_scope_recording_enabled(WkiPerfScope scope, uint8_t op) {
    if (!g_enabled.load(std::memory_order_acquire)) {
        return false;
    }

    auto mask = g_event_mask.load(std::memory_order_relaxed);
    return wki_should_record(mask, scope, op);
}

bool is_local_xfs_recording_enabled() { return is_wki_scope_recording_enabled(WkiPerfScope::LOCAL_XFS); }

bool is_local_irq_recording_enabled() { return is_wki_scope_recording_enabled(WkiPerfScope::LOCAL_IRQ); }

void register_local_vmem_zero_page(const void* page) {
    g_local_vmem_zero_page.store(reinterpret_cast<uintptr_t>(page), std::memory_order_release);
}

bool is_local_vmem_zero_page(const void* page) {
    return page != nullptr && g_local_vmem_zero_page.load(std::memory_order_acquire) == reinterpret_cast<uintptr_t>(page);
}

uint64_t wki_pack_event_data(WkiPerfScope scope, uint8_t op, WkiPerfPhase phase, uint16_t peer, uint16_t channel) {
    return static_cast<uint64_t>(static_cast<uint8_t>(scope)) | (static_cast<uint64_t>(op) << 8U) |
           (static_cast<uint64_t>(static_cast<uint8_t>(phase)) << 16U) | (static_cast<uint64_t>(peer) << 24U) |
           (static_cast<uint64_t>(channel) << 40U);
}

void wki_unpack_event_data(uint64_t data, WkiPerfScope& scope, uint8_t& op, WkiPerfPhase& phase, uint16_t& peer, uint16_t& channel) {
    scope = static_cast<WkiPerfScope>(data & 0xFFU);
    op = static_cast<uint8_t>((data >> 8U) & 0xFFU);
    phase = static_cast<WkiPerfPhase>((data >> 16U) & 0xFFU);
    peer = static_cast<uint16_t>((data >> 24U) & 0xFFFFU);
    channel = static_cast<uint16_t>((data >> 40U) & 0xFFFFU);
}

uint64_t wki_pack_trace_state(uint32_t correlation, int32_t status) {
    return (static_cast<uint64_t>(correlation) << 32U) | static_cast<uint32_t>(status);
}

uint32_t wki_unpack_trace_correlation(int64_t packed) { return static_cast<uint32_t>(static_cast<uint64_t>(packed) >> 32U); }

int32_t wki_unpack_trace_status(int64_t packed) { return static_cast<int32_t>(static_cast<uint32_t>(packed)); }

uint32_t next_wki_trace_correlation() { return g_wki_trace_correlation.fetch_add(1, std::memory_order_relaxed); }

void record_wki_event(uint32_t cpu, uint64_t pid, WkiPerfScope scope, uint8_t op, WkiPerfPhase phase, uint16_t peer, uint16_t channel,
                      uint32_t correlation, int32_t status, uint32_t aux, uint64_t callsite) {
    if (cpu >= g_num_cpus || !g_enabled.load(std::memory_order_acquire)) {
        return;
    }

    auto mask = g_event_mask.load(std::memory_order_relaxed);
    if (!wki_should_record(mask, scope, op)) {
        return;
    }

    auto& ring = g_rings[cpu];
    auto saved = ring.lock.lock_irqsave();

    PerfEvent evt{};
    evt.ts_ns = ker::mod::time::get_monotonic_ns();
    evt.pid = pid;
    evt.data = wki_pack_event_data(scope, op, phase, peer, channel);
    evt.callsite = callsite;
    evt.lag_v = static_cast<int64_t>(wki_pack_trace_state(correlation, status));
    evt.cpu = static_cast<uint16_t>(cpu);
    evt.type = static_cast<uint8_t>(PerfEventType::WKI);
    evt.flags = wki_phase_flags(phase);
    evt.aux = aux;
    perf_push_event(ring, evt);

    ring.lock.unlock_irqrestore(saved);
}

void record_wki_summary(WkiPerfScope scope, uint8_t op, uint16_t peer, uint16_t channel, int32_t status, uint32_t latency_us,
                        bool has_latency, uint32_t retries, uint64_t bytes) {
    if (!g_enabled.load(std::memory_order_acquire)) {
        return;
    }

    auto mask = g_event_mask.load(std::memory_order_relaxed);
    if (!wki_should_record(mask, scope, op)) {
        return;
    }

    auto saved = g_wki_summary_lock.lock_irqsave();
    WkiPerfSummaryBucket* bucket = wki_get_or_create_summary_bucket(static_cast<uint8_t>(scope), op, peer, channel);
    if (bucket != nullptr) {
        bucket->calls++;
        if (status < 0) {
            bucket->errors++;
        }
        bucket->retries += retries;
        bucket->bytes += bytes;
        if (has_latency) {
            bucket->latency_samples++;
            bucket->total_latency_us += latency_us;
            bucket->max_latency_us = std::max(bucket->max_latency_us, latency_us);
            bucket->latency_hist.at(wki_hist_bucket(latency_us))++;
        }
    }
    g_wki_summary_lock.unlock_irqrestore(saved);
}

void record_local_xfs_summary(WkiPerfLocalXfsOp op, int32_t status, uint32_t latency_us, bool has_latency, uint64_t bytes) {
    record_wki_summary(WkiPerfScope::LOCAL_XFS, static_cast<uint8_t>(op), 0, 0, status, latency_us, has_latency, 0, bytes);
}

void record_local_irq_summary(WkiPerfLocalIrqOp op, uint16_t vector, uint16_t kind, int32_t status, uint32_t latency_us, bool has_latency) {
    record_wki_summary(WkiPerfScope::LOCAL_IRQ, static_cast<uint8_t>(op), vector, kind, status, latency_us, has_latency, 0, 0);
}

size_t get_wki_summary_snapshots(WkiPerfSummarySnapshot* dst, size_t max) {
    if (dst == nullptr || max == 0) {
        return 0;
    }

    auto saved = g_wki_summary_lock.lock_irqsave();
    size_t total = 0;
    for (const auto& bucket : g_wki_summary) {
        if (!bucket.used || total >= max) {
            continue;
        }

        dst[total++] = WkiPerfSummarySnapshot{
            .scope = bucket.scope,
            .op = bucket.op,
            .peer = bucket.peer,
            .channel = bucket.channel,
            .reserved = 0,
            .calls = bucket.calls,
            .errors = bucket.errors,
            .retries = bucket.retries,
            .bytes = bucket.bytes,
            .total_latency_us = bucket.total_latency_us,
            .latency_samples = bucket.latency_samples,
            .max_latency_us = bucket.max_latency_us,
            .p50_us = wki_hist_percentile(bucket.latency_hist, bucket.latency_samples, 50, 100),
            .p95_us = wki_hist_percentile(bucket.latency_hist, bucket.latency_samples, 95, 100),
            .p99_us = wki_hist_percentile(bucket.latency_hist, bucket.latency_samples, 99, 100),
            .p999_us = wki_hist_percentile(bucket.latency_hist, bucket.latency_samples, 999, 1000),
            .p9999_us = wki_hist_percentile(bucket.latency_hist, bucket.latency_samples, 9999, 10000),
            .p99999_us = wki_hist_percentile(bucket.latency_hist, bucket.latency_samples, 99999, 100000),
        };
    }
    g_wki_summary_lock.unlock_irqrestore(saved);
    return total;
}

// ---------------------------------------------------------------------------
// update_subsystem_stat - always-on atomic counters
// ---------------------------------------------------------------------------
void update_subsystem_stat(PerfSubsystem subsystem, uint8_t flags, uint64_t current_count) {
    auto idx = static_cast<size_t>(subsystem);
    if (idx == 0 || idx >= PERF_SUBSYSTEM_COUNT) {
        return;
    }

    auto& s = g_subsys_stats.at(idx);

    if ((flags & PERF_FLAG_CT_INSERT) != 0) {
        s.inserts.fetch_add(1, std::memory_order_relaxed);
    }
    if ((flags & PERF_FLAG_CT_REMOVE) != 0) {
        s.removes.fetch_add(1, std::memory_order_relaxed);
    }
    if ((flags & PERF_FLAG_CT_RESIZE) != 0) {
        s.resizes.fetch_add(1, std::memory_order_relaxed);
    }
    if ((flags & PERF_FLAG_CT_OOM) != 0) {
        s.oom_failures.fetch_add(1, std::memory_order_relaxed);
    }

    s.current_count.store(current_count, std::memory_order_relaxed);

    // Update peak (lock-free CAS loop)
    uint64_t old_peak = s.peak_count.load(std::memory_order_relaxed);
    while (current_count > old_peak) {
        if (s.peak_count.compare_exchange_weak(old_peak, current_count, std::memory_order_relaxed)) {
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// get_subsystem_stats - non-destructive snapshot
// ---------------------------------------------------------------------------
PerfSubsystemSnapshot get_subsystem_stats(PerfSubsystem subsystem) {
    auto idx = static_cast<size_t>(subsystem);
    if (idx == 0 || idx >= PERF_SUBSYSTEM_COUNT) {
        return {};
    }

    auto& s = g_subsys_stats.at(idx);
    return PerfSubsystemSnapshot{
        .inserts = s.inserts.load(std::memory_order_relaxed),
        .removes = s.removes.load(std::memory_order_relaxed),
        .resizes = s.resizes.load(std::memory_order_relaxed),
        .oom_failures = s.oom_failures.load(std::memory_order_relaxed),
        .peak_count = s.peak_count.load(std::memory_order_relaxed),
        .current_count = s.current_count.load(std::memory_order_relaxed),
    };
}

// ---------------------------------------------------------------------------
// Event mask control
// ---------------------------------------------------------------------------
void set_event_mask(uint16_t mask) { g_event_mask.store(mask, std::memory_order_release); }

uint16_t get_event_mask() { return g_event_mask.load(std::memory_order_acquire); }

// ---------------------------------------------------------------------------
// parse_event_mask - parse "switch,wake,container,wki,wki_launch" into bitmask
// ---------------------------------------------------------------------------
uint16_t parse_event_mask(const char* str, size_t len) {
    if (str == nullptr) {
        return 0;
    }
    std::string_view const INPUT{str, len};
    uint16_t mask = 0;
    size_t i = 0;

    while (i < len) {
        // Skip whitespace and commas
        while (i < len && (str[i] == ',' || str[i] == ' ' || str[i] == '\n')) {
            i++;
        }
        if (i >= len) {
            break;
        }

        // Find end of token
        size_t const START = i;
        while (i < len && str[i] != ',' && str[i] != ' ' && str[i] != '\n') {
            i++;
        }
        size_t const TOK_LEN = i - START;
        std::string_view const TOKEN = INPUT.substr(START, TOK_LEN);

        if (TOKEN == "sample") {
            mask |= PERF_MASK_SAMPLE;
        } else if (TOKEN == "switch") {
            mask |= PERF_MASK_SWITCH;
        } else if (TOKEN == "wake") {
            mask |= PERF_MASK_WAKE;
        } else if (TOKEN == "sleep") {
            mask |= PERF_MASK_SLEEP;
        } else if (TOKEN == "container") {
            mask |= PERF_MASK_CONTAINER;
        } else if (TOKEN == "wki") {
            mask |= PERF_MASK_WKI;
        } else if (TOKEN == "wki_launch") {
            mask |= PERF_MASK_WKI_LAUNCH;
        } else if (TOKEN == "local_pipe") {
            mask |= PERF_MASK_LOCAL_PIPE;
        } else if (TOKEN == "local_proc") {
            mask |= PERF_MASK_LOCAL_PROC;
        } else if (TOKEN == "local_vmem" || TOKEN == "vmem") {
            mask |= PERF_MASK_LOCAL_VMEM;
        } else if (TOKEN == "local_loader" || TOKEN == "loader") {
            mask |= PERF_MASK_LOCAL_LOADER;
        } else if (TOKEN == "local_xfs" || TOKEN == "xfs") {
            mask |= PERF_MASK_LOCAL_XFS;
        } else if (TOKEN == "local_irq" || TOKEN == "irq") {
            mask |= PERF_MASK_LOCAL_IRQ;
        } else if (TOKEN == "local") {
            mask |= PERF_MASK_LOCAL;
        } else if (TOKEN == "all") {
            mask = PERF_MASK_ALL;
        } else {
            return 0;  // unknown token
        }
    }
    return mask;
}

}  // namespace ker::mod::perf
