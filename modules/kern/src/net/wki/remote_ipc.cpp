#include "remote_ipc.hpp"

#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <dev/pty.hpp>
#include <iterator>
#include <net/socket.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/mm/mm.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/security/compartment_wki.hpp>
#include <platform/sys/spinlock.hpp>
#include <span>
#include <utility>
#include <vfs/epoll.hpp>
#include <vfs/fd_reservation.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/vfs.hpp>

#include "util/smallvec.hpp"

namespace ker::net::wki {

// =============================================================================
// File-scope globals (anonymous namespace)
// =============================================================================

namespace {

constexpr uint32_t WKI_LATENCY_DAEMON_SLICE_NS = 2'000'000;
constexpr int WKI_LATENCY_DAEMON_NICE = -5;

auto read_ipc_control_response(uint16_t channel, const uint8_t* payload, uint16_t len, DevOpRespPayload& response) -> bool {
    // Every current IPC control request uses RESOURCE. Data-plane traffic must
    // not claim a control waiter, even when endpoint/op/cookie values coincide.
    if (channel != WKI_CHAN_RESOURCE || payload == nullptr || len < sizeof(response)) {
        return false;
    }
    std::memcpy(&response, payload, sizeof(response));
    // Keep the addition wide: a uint16 data_len can exceed the complete frame.
    return response.data_len >= sizeof(uint32_t) && sizeof(response) + response.data_len <= len;
}

void promote_latency_sensitive_daemon(ker::mod::sched::task::Task* task) {
    if (task == nullptr || task->type != ker::mod::sched::task::TaskType::DAEMON) {
        return;
    }

    task->slice_ns = WKI_LATENCY_DAEMON_SLICE_NS;
    ker::mod::sched::set_task_nice(task, WKI_LATENCY_DAEMON_NICE);
}

auto perf_current_pid() -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr ? task->pid : 0;
}

auto perf_current_cpu() -> uint32_t {
    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr ? static_cast<uint32_t>(task->load_cpu()) : 0U;
}

auto current_task_has_deliverable_signal() -> bool {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return false;
    }
    return task->has_interrupting_signal_pending();
}

auto cmdline_has_token(const char* cmdline, const char* token) -> bool {
    if (cmdline == nullptr || token == nullptr || token[0] == '\0') {
        return false;
    }
    size_t const TOKEN_LEN = std::strlen(token);
    const char* cursor = cmdline;
    while (*cursor != '\0') {
        while (*cursor == ' ' || *cursor == '\t') {
            ++cursor;
        }
        const char* start = cursor;
        while (*cursor != '\0' && *cursor != ' ' && *cursor != '\t') {
            ++cursor;
        }
        if (std::cmp_equal(cursor - start, TOKEN_LEN) && std::strncmp(start, token, TOKEN_LEN) == 0) {
            return true;
        }
    }
    return false;
}

auto wait_diag_enabled() -> bool {
    static std::atomic<int> cached{-1};
    int value = cached.load(std::memory_order_acquire);
    if (value >= 0) {
        return value != 0;
    }
    int const ENABLED = cmdline_has_token(ker::init::get_kernel_cmdline(), "wki.wait_diag") ? 1 : 0;
    cached.store(ENABLED, std::memory_order_release);
    return ENABLED != 0;
}

auto pty_ioctl_cmd_name(unsigned long cmd) -> const char* {
    switch (cmd) {
        case ker::dev::pty::TIOCGPTN:
            return "TIOCGPTN";
        case ker::dev::pty::TIOCSPTLCK:
            return "TIOCSPTLCK";
        case ker::dev::pty::TIOCGWINSZ:
            return "TIOCGWINSZ";
        case ker::dev::pty::TIOCSWINSZ:
            return "TIOCSWINSZ";
        case ker::dev::pty::TIOCSCTTY:
            return "TIOCSCTTY";
        case ker::dev::pty::TIOCGPGRP:
            return "TIOCGPGRP";
        case ker::dev::pty::TIOCSPGRP:
            return "TIOCSPGRP";
        case ker::dev::pty::TIOCNOTTY:
            return "TIOCNOTTY";
        case ker::dev::pty::TCGETS:
            return "TCGETS";
        case ker::dev::pty::TCSETS:
            return "TCSETS";
        case ker::dev::pty::TCSETSW:
            return "TCSETSW";
        case ker::dev::pty::TCSETSF:
            return "TCSETSF";
        case ker::dev::pty::TCFLSH:
            return "TCFLSH";
        default:
            return "UNKNOWN";
    }
}

auto pty_session_mutation(unsigned long cmd) -> bool {
    return cmd == ker::dev::pty::TIOCSCTTY || cmd == ker::dev::pty::TIOCNOTTY || cmd == ker::dev::pty::TIOCSPGRP;
}

constexpr size_t PTY_IOCTL_ARG_SIZE = 128;  // max bytes we send/receive for ioctl arg

struct PtyIoctlMarshal {
    size_t arg_size = 0;
    bool pointer_arg = false;
    bool copy_in = false;
    bool copy_out = false;
};

auto pty_ioctl_marshalling(unsigned long cmd, unsigned long arg) -> PtyIoctlMarshal {
    auto pointer = [arg](size_t size, bool copy_in, bool copy_out) -> PtyIoctlMarshal {
        return {
            .arg_size = size,
            .pointer_arg = arg >= 8192,
            .copy_in = copy_in,
            .copy_out = copy_out,
        };
    };

    switch (cmd) {
        case ker::dev::pty::TIOCGPTN:
        case ker::dev::pty::TIOCSPTLCK:
            return pointer(sizeof(int), cmd == ker::dev::pty::TIOCSPTLCK, cmd == ker::dev::pty::TIOCGPTN);
        case ker::dev::pty::TIOCGWINSZ:
        case ker::dev::pty::TIOCSWINSZ:
            return pointer(sizeof(ker::dev::pty::Winsize), cmd == ker::dev::pty::TIOCSWINSZ, cmd == ker::dev::pty::TIOCGWINSZ);
        case ker::dev::pty::TIOCGPGRP:
        case ker::dev::pty::TIOCSPGRP:
            return pointer(sizeof(int64_t), cmd == ker::dev::pty::TIOCSPGRP, cmd == ker::dev::pty::TIOCGPGRP);
        case ker::dev::pty::TCGETS:
        case ker::dev::pty::TCSETS:
        case ker::dev::pty::TCSETSW:
        case ker::dev::pty::TCSETSF:
            return pointer(sizeof(ker::dev::pty::KTermios), cmd != ker::dev::pty::TCGETS, cmd == ker::dev::pty::TCGETS);
        default:
            return {};
    }
}

constexpr int WKI_SIGPIPE_NUM = 13;

void clear_current_daemon_sigpipe() {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || task->type != ker::mod::sched::task::TaskType::DAEMON) {
        return;
    }

    task->signal_clear_pending_mask(1ULL << (WKI_SIGPIPE_NUM - 1));
}

auto clamp_io_count(ssize_t result, size_t requested) -> ssize_t {
    if (result <= 0 || std::cmp_less_equal(result, requested)) {
        return result;
    }
    return static_cast<ssize_t>(requested);
}

void perf_record_ipc_event(uint8_t op, ker::mod::perf::WkiPerfPhase phase, uint16_t peer, uint16_t channel, uint32_t correlation,
                           int32_t status, uint32_t aux, uint64_t callsite) {
    if (!ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::REMOTE_IPC, op)) {
        return;
    }
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_IPC, op, phase, peer,
                                     channel, correlation, status, aux, callsite);
}

void perf_record_ipc_summary(uint8_t op, uint16_t peer, uint16_t channel, int32_t status, uint32_t latency_us, uint64_t bytes = 0,
                             uint32_t retries = 0) {
    if (!ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::REMOTE_IPC, op)) {
        return;
    }
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_IPC, op, peer, channel, status, latency_us, true, retries,
                                       bytes);
}

auto ipc_perf_enabled(ker::mod::perf::WkiPerfIpcOp op) -> bool {
    return ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::REMOTE_IPC, static_cast<uint8_t>(op));
}

class IpcPerfTrace {
   public:
    IpcPerfTrace(ker::mod::perf::WkiPerfIpcOp op, uint16_t peer, uint16_t channel, uint64_t callsite, uint32_t begin_aux = 0)
        : op_id(static_cast<uint8_t>(op)), peer_id(peer), channel_id(channel), trace_callsite(callsite) {
        if (!ipc_perf_enabled(op)) {
            return;
        }
        active = true;
        trace_correlation = ker::mod::perf::next_wki_trace_correlation();
        started_us = wki_now_us();
        perf_record_ipc_event(op_id, ker::mod::perf::WkiPerfPhase::BEGIN, peer_id, channel_id, trace_correlation, 0, begin_aux,
                              trace_callsite);
    }

    IpcPerfTrace(ker::mod::perf::WkiPerfIpcOp op, uint16_t peer, uint16_t channel, uint64_t callsite, uint32_t begin_aux,
                 uint32_t correlation)
        : op_id(static_cast<uint8_t>(op)), peer_id(peer), channel_id(channel), trace_callsite(callsite), trace_correlation(correlation) {
        if (!ipc_perf_enabled(op)) {
            return;
        }
        active = true;
        started_us = wki_now_us();
        perf_record_ipc_event(op_id, ker::mod::perf::WkiPerfPhase::BEGIN, peer_id, channel_id, trace_correlation, 0, begin_aux,
                              trace_callsite);
    }

    void finish(int32_t status, uint64_t bytes = 0, uint32_t retries = 0) const {
        if (!active) {
            return;
        }
        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - started_us);
        perf_record_ipc_event(op_id, ker::mod::perf::WkiPerfPhase::END, peer_id, channel_id, trace_correlation, status, ELAPSED_US,
                              trace_callsite);
        perf_record_ipc_summary(op_id, peer_id, channel_id, status, ELAPSED_US, bytes, retries);
    }

   private:
    uint8_t op_id = 0;
    uint16_t peer_id = WKI_NODE_INVALID;
    uint16_t channel_id = WKI_CHAN_RESOURCE;
    uint64_t trace_callsite = 0;
    uint32_t trace_correlation = 0;
    uint64_t started_us = 0;
    bool active = false;
};

void perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp op, uint16_t peer, uint16_t channel, uint32_t correlation, int32_t status,
                           uint32_t aux, uint64_t callsite) {
    if (!ipc_perf_enabled(op)) {
        return;
    }
    perf_record_ipc_event(static_cast<uint8_t>(op), ker::mod::perf::WkiPerfPhase::POINT, peer, channel, correlation, status, aux, callsite);
}

void wake_blocked_ipc_reader(ker::mod::sched::task::Task* reader) {
    if (reader == nullptr) {
        return;
    }

    ker::mod::sched::wake_task_from_event(reader);
}

constexpr int WKI_IPC_FD_ACCESS_MASK = 0x0003;
constexpr int WKI_IPC_FD_RDWR = 0x0002;
constexpr int WKI_IPC_O_NONBLOCK = 04000;
constexpr int WKI_IPC_FD_OPEN_FLAG_MASK = WKI_IPC_FD_ACCESS_MASK | WKI_IPC_O_NONBLOCK;
static_assert((WKI_IPC_FD_FLAG_PIPE_FLOW_CONTROL & WKI_IPC_FD_OPEN_FLAG_MASK) == 0);

auto ipc_fd_access_mode(int open_flags) -> uint16_t { return static_cast<uint16_t>(open_flags & WKI_IPC_FD_ACCESS_MASK); }

auto ipc_pipe_endpoint_requires_affinity(int open_flags) -> bool { return ipc_fd_access_mode(open_flags) == 0; }

auto ipc_fd_export_open_flags(int open_flags) -> uint16_t { return static_cast<uint16_t>(open_flags & WKI_IPC_FD_OPEN_FLAG_MASK); }

auto ipc_fd_import_open_flags(uint16_t flags) -> int { return static_cast<int>(flags & WKI_IPC_FD_OPEN_FLAG_MASK); }

constexpr auto ipc_proxy_can_receive_data(ResourceType res_type, uint16_t access_mode) -> bool {
    return res_type == ResourceType::IPC_SOCKET || res_type == ResourceType::IPC_PTY ||
           (res_type == ResourceType::IPC_PIPE && access_mode == 0);
}

bool g_ipc_initialized = false;
ker::mod::sys::Spinlock s_ipc_lock;

// Server-side (home node) exports
// Non-owning intrusive membership. Publication/erasure cannot allocate, and
// registry links are independent of the pump queue and retirement chain.
template <typename Entry>
class IpcRegistry {
   public:
    class Iterator {
       public:
        using value_type = Entry*;
        using difference_type = std::ptrdiff_t;
        using iterator_category = std::forward_iterator_tag;
        using iterator_concept = std::forward_iterator_tag;
        Iterator() = default;
        explicit Iterator(Entry* value) : current(value) {}
        auto operator*() const -> Entry* { return current; }
        auto operator++() -> Iterator& {
            current = current->registry_next;
            return *this;
        }
        auto operator++(int) -> Iterator {
            Iterator old = *this;
            ++*this;
            return old;
        }
        auto operator==(const Iterator&) const -> bool = default;

       private:
        Entry* current = nullptr;
    };
    IpcRegistry() = default;
    IpcRegistry(const IpcRegistry&) = delete;
    auto operator=(const IpcRegistry&) -> IpcRegistry& = delete;
    auto begin() const -> Iterator { return Iterator(head); }
    auto end() const -> Iterator { return Iterator(); }
    auto size() const -> size_t { return count; }
    void push_back(Entry* exp) {
        if (exp == nullptr) return;
        exp->registry_previous = tail;
        exp->registry_next = nullptr;
        if (tail != nullptr)
            tail->registry_next = exp;
        else
            head = exp;
        tail = exp;
        ++count;
    }
    auto erase(Iterator position) -> Iterator {
        auto* exp = *position;
        if (exp == nullptr) return end();
        auto* next = exp->registry_next;
        if (exp->registry_previous != nullptr)
            exp->registry_previous->registry_next = next;
        else
            head = next;
        if (next != nullptr)
            next->registry_previous = exp->registry_previous;
        else
            tail = exp->registry_previous;
        exp->registry_next = nullptr;
        exp->registry_previous = nullptr;
        --count;
        return Iterator(next);
    }

   private:
    Entry* head = nullptr;
    Entry* tail = nullptr;
    size_t count = 0;
};
using IpcExportRegistry = IpcRegistry<WkiIpcExport>;
IpcExportRegistry g_ipc_exports;
uint32_t g_next_ipc_resource_id = 0x7000;

// Client-side (remote node) proxies — tracked for cleanup
IpcRegistry<ProxyIpcState> g_ipc_proxies;

std::atomic<uint64_t> g_proxy_write_payload_bytes{0};
std::atomic<uint64_t> g_proxy_write_no_credit_waits{0};
std::atomic<uint64_t> g_proxy_write_block_us{0};
std::atomic<uint64_t> g_proxy_pipe_rdma_full_waits{0};
std::atomic<uint64_t> g_proxy_ring_full_waits{0};
std::atomic<uint64_t> g_proxy_ring_full_bytes{0};
std::atomic<uint64_t> g_pipe_payload_bytes{0};

#ifdef WOS_SELFTEST
std::atomic<int> g_ipc_export_selftest_frees{0};
std::atomic<int> g_ipc_proxy_selftest_frees{0};
std::atomic<int> g_ipc_pending_delivery_selftest_frees{0};
std::atomic<int> g_ipc_pending_proxy_close_selftest_frees{0};
std::atomic<int> g_ipc_dev_op_work_selftest_frees{0};
std::atomic<int> g_ipc_file_selftest_closes{0};
std::atomic<bool> g_ipc_selftest_force_attach_insert_failure{false};

struct IpcPipeWriteSelftestScript {
    std::array<ssize_t, 4> results{};
    std::array<const void*, 4> buffers{};
    std::array<size_t, 4> counts{};
    std::array<size_t, 4> offsets{};
    std::array<int, 4> open_flags{};
    size_t result_count = 0;
    size_t calls = 0;
};

auto ipc_selftest_scripted_write(ker::vfs::File* file, const void* buf, size_t count, size_t offset) -> ssize_t {
    auto* script = file != nullptr ? static_cast<IpcPipeWriteSelftestScript*>(file->private_data) : nullptr;
    if (script == nullptr) {
        return -EIO;
    }

    size_t const CALL = script->calls++;
    if (CALL >= script->result_count || CALL >= script->results.size()) {
        return -EIO;
    }
    script->buffers.at(CALL) = buf;
    script->counts.at(CALL) = count;
    script->offsets.at(CALL) = offset;
    script->open_flags.at(CALL) = file->open_flags;
    return script->results.at(CALL);
}

auto ipc_selftest_file_close(ker::vfs::File* /*file*/) -> int {
    g_ipc_file_selftest_closes.fetch_add(1, std::memory_order_relaxed);
    return 0;
}

std::atomic<int> g_ipc_accept_selftest_reads{0};

ker::vfs::FileOperations g_ipc_selftest_file_ops = {
    .vfs_open = nullptr,
    .vfs_close = ipc_selftest_file_close,
    .vfs_read = nullptr,
    .vfs_write = nullptr,
    .vfs_lseek = nullptr,
    .vfs_isatty = nullptr,
    .vfs_readdir = nullptr,
    .vfs_readlink = nullptr,
    .vfs_truncate = nullptr,
    .vfs_poll_check = nullptr,
    .vfs_poll_register_waiter = nullptr,
    .vfs_ioctl = nullptr,
};

ker::vfs::FileOperations g_ipc_pipe_write_selftest_ops = {
    .vfs_open = nullptr,
    .vfs_close = nullptr,
    .vfs_read = nullptr,
    .vfs_write = ipc_selftest_scripted_write,
    .vfs_lseek = nullptr,
    .vfs_isatty = nullptr,
    .vfs_readdir = nullptr,
    .vfs_readlink = nullptr,
    .vfs_truncate = nullptr,
    .vfs_poll_check = nullptr,
    .vfs_poll_register_waiter = nullptr,
    .vfs_ioctl = nullptr,
};
#endif

struct IpcDevOpWork;
void free_ipc_dev_op_work(IpcDevOpWork* work);

struct PendingPipeChunk {
    const uint8_t* data = nullptr;
    uint16_t len = 0;
    uint16_t offset = 0;
    uint16_t flow_credit = 0;
    IpcDevOpWork* dev_op_owner = nullptr;
};
static_assert(sizeof(PendingPipeChunk) == 24);

struct PendingPipeDelivery {
    uint16_t home_node = WKI_NODE_INVALID;
    uint32_t resource_id = 0;
    std::deque<PendingPipeChunk> chunks;
    IpcDevOpWork* close_owner = nullptr;
    uint32_t buffered_bytes = 0;
    bool write_closed = false;
};

std::deque<PendingPipeDelivery*> g_pending_pipe_deliveries;

struct ExportPipeWriteBacklog {
    ExportPipeWriteBacklog* registry_previous = nullptr;
    ExportPipeWriteBacklog* registry_next = nullptr;
    WkiIpcExport* exp = nullptr;
    uint32_t resource_id = 0;
    std::deque<PendingPipeChunk> chunks;
    uint32_t buffered_bytes = 0;
    int32_t write_error = 0;  // terminal receipt retained until reliable enqueue or peer retirement
    bool close_pending = false;
    bool close_has_expected_bytes = false;
    uint64_t close_expected_bytes = 0;
    bool queued = false;
    bool persistent_for_rdma = false;
};

IpcRegistry<ExportPipeWriteBacklog> g_export_pipe_write_backlogs;
constexpr size_t WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_COUNT = 4;
constexpr size_t WKI_IPC_EXPORT_PIPE_WRITE_BURST_CALLS = 3;
std::array<std::deque<ExportPipeWriteBacklog*>, WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_COUNT> g_export_pipe_write_flush_queues;
std::array<ker::mod::sched::task::Task*, WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_COUNT> g_export_pipe_write_flush_tasks = {};
std::array<std::atomic<bool>, WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_COUNT> g_export_pipe_write_flush_stopped = {true, true, true, true};
std::atomic<bool> g_ipc_consumer_stopping{false};
bool g_ipc_flush_workers_starting = false;  // Protected by s_ipc_lock.
std::atomic<bool> g_ipc_shutdown_drain_owner{false};

struct IpcShutdownDrainOwner {
    ~IpcShutdownDrainOwner() { g_ipc_shutdown_drain_owner.store(false, std::memory_order_release); }
};
constexpr std::array<const char*, WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_COUNT> WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_NAMES = {
    "wki_pipe_w0",
    "wki_pipe_w1",
    "wki_pipe_w2",
    "wki_pipe_w3",
};

struct IpcDevOpWork {
    WkiHeader hdr = {};
    WkiChannelIdentity channel_identity{};
    uint8_t* payload = nullptr;
    IpcDevOpWork* next = nullptr;
    IpcDevOpWork* delivery_previous = nullptr;
    IpcDevOpWork* delivery_next = nullptr;
    uint32_t endpoint_delivery_group = 0;
    uint16_t payload_len = 0;
    bool payload_coallocated = false;
    bool fixed_pool = false;
    bool payload_accounted = false;
    bool pipe_chunk_transferable = false;
    uint64_t cleanup_epoch = 0;
};

struct IpcDevOpStorage {
    IpcDevOpWork work{};
    std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> payload{};
};

static_assert(__builtin_offsetof(IpcDevOpStorage, work) == 0);
static_assert(__builtin_offsetof(IpcDevOpStorage, payload) == sizeof(IpcDevOpWork));

struct IpcDevOpQueue {
    IpcDevOpWork* head = nullptr;
    IpcDevOpWork* tail = nullptr;
    size_t count = 0;
};

constexpr size_t WKI_IPC_DEV_OP_WORKER_COUNT = 4;
constexpr size_t WKI_IPC_DEV_OP_MAX_PENDING = 256;
constexpr size_t WKI_IPC_DEV_OP_TRANSFER_RESERVE = WKI_IPC_DEV_OP_WORKER_COUNT * 2;
static_assert(WKI_IPC_DEV_OP_TRANSFER_RESERVE < WKI_IPC_DEV_OP_MAX_PENDING);
std::array<IpcDevOpQueue, WKI_IPC_DEV_OP_WORKER_COUNT> g_ipc_dev_op_queues{};
std::array<ker::mod::sched::task::Task*, WKI_IPC_DEV_OP_WORKER_COUNT> g_ipc_dev_op_worker_tasks = {};
std::array<IpcDevOpStorage, WKI_IPC_DEV_OP_MAX_PENDING> g_ipc_dev_op_work{};
IpcDevOpWork* g_ipc_dev_op_free = nullptr;
// RX admission order, retained through backlog delivery and its receipt.
// Guarded solely by s_ipc_dev_op_pool_lock; tokens own no File or Task.
IpcDevOpWork* g_endpoint_delivery_head = nullptr;
IpcDevOpWork* g_endpoint_delivery_tail = nullptr;
size_t g_ipc_dev_op_free_count = 0;
bool g_ipc_dev_op_pool_initialized = false;
ker::mod::sys::Spinlock s_ipc_dev_op_pool_lock;
constexpr size_t WKI_IPC_DEV_OP_CLEANUP_BATCH = 64;

struct IpcPeerCleanupEpoch {
    bool used = false;
    uint16_t node_id = WKI_NODE_INVALID;
    uint64_t epoch = 0;
    bool cleanup_active = false;
    uint32_t active_workers = 0;
};

std::array<IpcPeerCleanupEpoch, WKI_MAX_PEERS> g_ipc_peer_cleanup_epochs = {};

constexpr uint64_t WKI_IPC_PIPE_WRITE_RETRY_US = 1000;
constexpr uint64_t WKI_IPC_PIPE_READ_POLL_RECHECK_US = 1000;
constexpr uint32_t WKI_IPC_PIPE_EOF_MAX_SEND_ATTEMPTS = 128;
constexpr uint64_t WKI_IPC_DIAG_FIRST_US = 500'000;
constexpr uint64_t WKI_IPC_DIAG_INTERVAL_US = 1'000'000;
constexpr size_t WKI_IPC_MAX_POLL_WAKE_WAITERS = 32;
constexpr size_t WKI_IPC_PROXY_CLOSE_MSG_MAX = sizeof(DevOpReqPayload) + sizeof(uint32_t) + sizeof(uint64_t);
constexpr size_t WKI_IPC_PROXY_CLOSE_CLEANUP_BATCH = WKI_IPC_MAX_EXPORTS * 8;
constexpr size_t WKI_IPC_PIPE_DATA_HEADER_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
constexpr size_t WKI_IPC_PIPE_DATA_MAX_CHUNK = WKI_ETH_MAX_PAYLOAD - WKI_IPC_PIPE_DATA_HEADER_SIZE;
constexpr uint16_t WKI_IPC_DEV_OP_TRANSFER_MIN_DATA = 1;
constexpr size_t WKI_IPC_EXPORT_PIPE_CAPACITY = 4UL * 1024UL * 1024UL;
static_assert(WKI_IPC_PIPE_DATA_HEADER_SIZE < WKI_ETH_MAX_PAYLOAD);
static_assert(WKI_IPC_PIPE_DATA_MAX_CHUNK <= UINT16_MAX);
static_assert(WKI_IPC_PIPE_DATA_HEADER_SIZE + WKI_IPC_PIPE_DATA_MAX_CHUNK == WKI_ETH_MAX_PAYLOAD);
static_assert(WKI_ETH_MAX_PAYLOAD <= ker::mod::mm::KERNEL_STACK_SIZE / 16);
static_assert(WKI_IPC_DEV_OP_TRANSFER_MIN_DATA > 0);
static_assert(WKI_IPC_EXPORT_PIPE_WRITE_BURST_CALLS > 0);
static_assert(WKI_IPC_MESSAGE_PIPE_WINDOW >= WKI_IPC_PIPE_DATA_MAX_CHUNK);

auto ipc_dev_op_work_can_back_pipe_chunk(const IpcDevOpWork* work, const uint8_t* data, uint16_t len) -> bool {
    if (work == nullptr || data == nullptr || !work->payload_coallocated || !work->fixed_pool || !work->pipe_chunk_transferable ||
        len < WKI_IPC_DEV_OP_TRANSFER_MIN_DATA) {
        return false;
    }

    auto const PAYLOAD_BEGIN = reinterpret_cast<uintptr_t>(work->payload);
    auto const DATA_BEGIN = reinterpret_cast<uintptr_t>(data);
    if (DATA_BEGIN < PAYLOAD_BEGIN) {
        return false;
    }
    size_t const OFFSET = DATA_BEGIN - PAYLOAD_BEGIN;
    return OFFSET <= work->payload_len && len <= work->payload_len - OFFSET;
}

void release_pending_pipe_chunk(PendingPipeChunk& chunk) {
    auto* const OWNER = chunk.dev_op_owner;
    const auto* const DATA = chunk.data;
    chunk = {};
    if (OWNER != nullptr) {
        free_ipc_dev_op_work(OWNER);
        return;
    }
    delete[] DATA;
}

constexpr uint32_t WKI_IPC_PIPE_RDMA_CAPACITY = WKI_IPC_PIPE_REGION_SIZE;
constexpr uint32_t WKI_IPC_PIPE_RDMA_TOTAL_SIZE = WKI_IPC_PIPE_RDMA_CAPACITY;
constexpr uint64_t WKI_IPC_PIPE_RDMA_WATCH_US = 1000;
constexpr uint16_t IPC_SECURE_VERSION = 1;
constexpr uint16_t IPC_SECURE_QUERY = 1;
constexpr uint16_t IPC_SECURE_APPEND = 2;
struct IpcSecureIntent {
    uint16_t version = IPC_SECURE_VERSION;
    uint16_t kind = 0;
    uint32_t resource_id = 0;
    uint64_t lifetime = 0;
    uint64_t head = 0;
    uint64_t tail = 0;
    uint32_t credit = 0;
    std::array<uint8_t, 28> reserved{};
} __attribute__((packed));
static_assert(sizeof(IpcSecureIntent) == rdma_control::NATIVE_INTENT_BYTES);
struct IpcSecureSnapshot {
    uint16_t version = IPC_SECURE_VERSION;
    uint16_t reserved = 0;
    uint32_t resource_id = 0;
    uint64_t lifetime = 0;
    uint64_t head = 0;
    uint64_t tail = 0;
    uint32_t credit = 0;
    uint32_t flags = 0;
    int32_t status = 0;
    uint32_t reserved2 = 0;
} __attribute__((packed));
static_assert(sizeof(IpcSecureSnapshot) == 48);
WkiIpcExport* g_secure_retired_exports = nullptr;
uint64_t g_next_ipc_secure_lifetime = 1;
void service_secure_retired_exports();
auto ipc_peer_cleanup_epoch_locked(uint16_t node_id) -> uint64_t;
auto ipc_peer_cleanup_active_locked(uint16_t node_id) -> bool;
auto ipc_peer_cleanup_slot_locked(uint16_t node_id, bool create) -> IpcPeerCleanupEpoch*;
void release_ipc_dev_op_worker_locked(uint16_t node_id);
struct PendingProxyPipeClose {
    WkiChannelIdentity channel_identity{};
    uint16_t home_node = WKI_NODE_INVALID;
    uint16_t msg_size = 0;
    uint16_t op_id = 0;
    uint32_t resource_id = 0;
    uint32_t attempts = 0;
    std::array<uint8_t, WKI_IPC_PROXY_CLOSE_MSG_MAX> msg = {};
};

auto ipc_diag_due(uint64_t started_us, uint64_t& last_log_us) -> bool {
    if (!wait_diag_enabled() || started_us == 0) {
        return false;
    }

    uint64_t const NOW_US = wki_now_us();
    if (NOW_US < started_us || NOW_US - started_us < WKI_IPC_DIAG_FIRST_US) {
        return false;
    }
    if (last_log_us != 0 && NOW_US - last_log_us < WKI_IPC_DIAG_INTERVAL_US) {
        return false;
    }
    last_log_us = NOW_US;
    return true;
}

void log_ipc_send_retry_diag(const char* event, const ProxyIpcState* proxy, uint32_t resource_id, size_t requested, size_t progressed,
                             int ret, uint32_t attempts, uint64_t started_us) {
    if (!wait_diag_enabled()) {
        return;
    }

    auto* task = ker::mod::sched::get_current_task();
    uint64_t const PID = task != nullptr ? task->pid : 0;
    const char* const TASK_NAME = task != nullptr && task->name != nullptr ? task->name : "?";
    uint16_t const HOME = proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID;
    uint32_t const RES = proxy != nullptr ? proxy->resource_id : resource_id;
    uint64_t const WRITTEN = proxy != nullptr ? proxy->bytes_written.load(std::memory_order_acquire) : 0;
    uint32_t const ACTIVE = proxy != nullptr && proxy->active.load(std::memory_order_acquire) ? 1U : 0U;
    uint64_t const ELAPSED = started_us != 0 ? wki_now_us() - started_us : 0;

    ker::mod::dbg::logger<"wki">::warn(
        "%s peer=0x%04x res=%u requested=%llu progressed=%llu ret=%d attempts=%u elapsed_us=%llu active=%u bytes_written=%llu "
        "pid=%llu task=%s",
        event != nullptr ? event : "ipc_send_retry", HOME, RES, static_cast<unsigned long long>(requested),
        static_cast<unsigned long long>(progressed), ret, attempts, static_cast<unsigned long long>(ELAPSED), ACTIVE,
        static_cast<unsigned long long>(WRITTEN), static_cast<unsigned long long>(PID), TASK_NAME);
}

void log_pipe_pump_send_retry_diag(const char* event, uint16_t target, uint32_t resource_id, ssize_t bytes, int ret, uint32_t attempts,
                                   uint64_t started_us) {
    if (!wait_diag_enabled()) {
        return;
    }

    auto* task = ker::mod::sched::get_current_task();
    uint64_t const PID = task != nullptr ? task->pid : 0;
    const char* const TASK_NAME = task != nullptr && task->name != nullptr ? task->name : "?";
    uint64_t const ELAPSED = started_us != 0 ? wki_now_us() - started_us : 0;

    ker::mod::dbg::logger<"wki">::warn("%s peer=0x%04x res=%u bytes=%lld ret=%d attempts=%u elapsed_us=%llu pid=%llu task=%s",
                                       event != nullptr ? event : "pipe_pump_send_retry", target, resource_id,
                                       static_cast<long long>(bytes), ret, attempts, static_cast<unsigned long long>(ELAPSED),
                                       static_cast<unsigned long long>(PID), TASK_NAME);
}

std::deque<PendingProxyPipeClose*> g_pending_proxy_pipe_closes;
ker::mod::sched::task::Task* g_proxy_pipe_close_tx_task = nullptr;

auto register_poll_write_waiter(ker::vfs::File* file, bool* ready_now) -> bool;
void handle_ipc_dev_op_req_inline(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, IpcDevOpWork** work_owner,
                                  bool* retry_out = nullptr, WkiChannelIdentity response_channel = {});
auto stop_pipe_pump_locked(WkiIpcExport* exp) -> ker::mod::sched::task::Task*;
void wake_pipe_pump(ker::mod::sched::task::Task* task);
auto export_has_pipe_pump_slot_locked(const WkiIpcExport* exp) -> bool;
auto find_export_by_resource_id(uint32_t resource_id) -> WkiIpcExport*;
auto send_proxy_pipe_close(ProxyIpcState* proxy, const uint8_t* msg, uint16_t msg_size, uint32_t resource_id, uint16_t op_id) -> void;
auto enqueue_proxy_pipe_close_tx(const WkiChannelIdentity& identity, const uint8_t* msg, uint16_t msg_size, uint32_t resource_id,
                                 uint16_t op_id) -> bool;
auto export_pipe_rdma_front_locked(WkiIpcExport* exp, uint32_t* tail_out, uint16_t* len_out, uint8_t** data_out) -> bool;
auto queue_export_pipe_rdma_flush(uint16_t src_node, uint32_t resource_id) -> bool;
void ipc_release_file_ref(ker::vfs::File* file);
void wake_export_pipe_write_flush_worker(size_t shard);
auto is_proxy_pipe_file(const ker::vfs::File* file) -> bool;

auto should_proxy_tty_like_file(ker::vfs::File* file) -> bool {
    if (file == nullptr || file->fs_type != ker::vfs::FSType::DEVFS) {
        return false;
    }

    bool const IS_DEVFS_WRAPPER = file->fops == ker::vfs::devfs::get_devfs_fops();
    if (IS_DEVFS_WRAPPER && ker::dev::pty::pty_is_file(file)) {
        return true;
    }

    return file->fops != nullptr && file->fops->vfs_isatty != nullptr && file->fops->vfs_isatty(file);
}

auto ipc_pipe_send_retry_sleep_us(int ret, uint32_t attempt) -> uint64_t {
    uint64_t const BASE_US = (ret == WKI_ERR_NO_CREDITS) ? 1000 : 2000;
    uint32_t const SHIFT = attempt < 5 ? attempt : 5;
    uint64_t const SLEEP_US = BASE_US << SHIFT;
    uint64_t const CAP_US = (ret == WKI_ERR_NO_CREDITS) ? 20000 : 50000;
    return std::min(SLEEP_US, CAP_US);
}

void pause_for_ipc_send_retry(int ret, uint32_t attempt) { ker::mod::sched::kern_sleep_us(ipc_pipe_send_retry_sleep_us(ret, attempt)); }

auto ipc_pipe_nonblocking_send_status(int ret) -> ssize_t {
    switch (ret) {
        case WKI_ERR_NO_CREDITS:
        case WKI_ERR_BUSY:
        case WKI_ERR_TX_FAILED:
            return -EAGAIN;
        case WKI_ERR_NO_MEM:
            return -ENOMEM;
        default:
            return -EIO;
    }
}

auto current_task_can_block_for_ipc_close() -> bool {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return true;
    }

    return task->state.load(std::memory_order_acquire) == ker::mod::sched::task::TaskState::ACTIVE && !task->has_exited;
}

void free_pending_proxy_pipe_close(PendingProxyPipeClose* pending) {
    if (pending == nullptr) {
        return;
    }
#ifdef WOS_SELFTEST
    g_ipc_pending_proxy_close_selftest_frees.fetch_add(1, std::memory_order_relaxed);
#endif
    delete pending;
}

auto proxy_pipe_close_tx_terminal(int ret) -> bool {
    return ret == WKI_ERR_PEER_FENCED || ret == WKI_ERR_NO_ROUTE || ret == WKI_ERR_INVALID || ret == WKI_ERR_NOT_FOUND;
}

auto enqueue_proxy_pipe_close_tx(const WkiChannelIdentity& identity, const uint8_t* msg, uint16_t msg_size, uint32_t resource_id,
                                 uint16_t op_id) -> bool {
    if (identity.peer_node_id == WKI_NODE_INVALID || identity.channel_id != WKI_CHAN_IPC_DATA ||
        !wki_channel_generation_is_live(identity.channel, identity.peer_node_id, identity.channel_id, identity.generation) ||
        msg == nullptr || msg_size == 0 || msg_size > WKI_IPC_PROXY_CLOSE_MSG_MAX) {
        return false;
    }

    auto* pending = new (std::nothrow) PendingProxyPipeClose();
    if (pending == nullptr) {
        return false;
    }

    pending->channel_identity = identity;
    pending->home_node = identity.peer_node_id;
    pending->msg_size = msg_size;
    pending->op_id = op_id;
    pending->resource_id = resource_id;
    std::memcpy(pending->msg.data(), msg, msg_size);

    ker::mod::sched::task::Task* worker = nullptr;
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    g_pending_proxy_pipe_closes.push_back(pending);
    worker = g_proxy_pipe_close_tx_task;
    s_ipc_lock.unlock_irqrestore(IRQF);

    if (worker != nullptr) {
        ker::mod::sched::kern_wake(worker);
    }
    return true;
}

auto capture_ipc_channel(uint16_t node, uint16_t channel_id) -> WkiChannelIdentity {
    if (!g_wki.initialized || node == WKI_NODE_INVALID || node == WKI_NODE_BROADCAST) return {};
    auto* channel = wki_channel_get(node, channel_id);
    if (channel == nullptr) return {};
    WkiChannelIdentity identity{};
    channel->lock.lock();
    if (channel->active && channel->peer_node_id == node && channel->channel_id == channel_id && channel->generation != 0)
        identity = {channel, node, channel_id, channel->generation};
    channel->lock.unlock();
    return identity;
}

auto ipc_channel_identity_equal(const WkiChannelIdentity& a, const WkiChannelIdentity& b) -> bool {
    return a.channel == b.channel && a.peer_node_id == b.peer_node_id && a.channel_id == b.channel_id && a.generation == b.generation;
}

auto ipc_channels_live(const WkiIpcChannels& channels, uint16_t node) -> bool {
    return channels.control.peer_node_id == node && channels.control.channel_id == WKI_CHAN_RESOURCE &&
           channels.data.peer_node_id == node && channels.data.channel_id == WKI_CHAN_IPC_DATA &&
           wki_channel_generation_is_live(channels.control.channel, node, WKI_CHAN_RESOURCE, channels.control.generation) &&
           wki_channel_generation_is_live(channels.data.channel, node, WKI_CHAN_IPC_DATA, channels.data.generation);
}

auto capture_ipc_companion_channel(const WkiChannelIdentity& admitted, uint16_t admitted_id, uint16_t companion_id) -> WkiChannelIdentity {
    auto live = [&]() {
        return admitted.channel_id == admitted_id &&
               wki_channel_generation_is_live(admitted.channel, admitted.peer_node_id, admitted_id, admitted.generation);
    };
    if (!live()) return {};
    auto const companion = capture_ipc_channel(admitted.peer_node_id, companion_id);
    return live() ? companion : WkiChannelIdentity{};
}

auto send_pending_proxy_pipe_close(const PendingProxyPipeClose& pending) -> int {
    auto const& identity = pending.channel_identity;
    if (identity.peer_node_id != pending.home_node || identity.channel_id != WKI_CHAN_IPC_DATA ||
        !wki_channel_generation_is_live(identity.channel, identity.peer_node_id, identity.channel_id, identity.generation))
        return WKI_ERR_NOT_FOUND;
    return wki_send_on_channel_identity(identity, MsgType::DEV_OP_REQ, pending.msg.data(), pending.msg_size);
}

void schedule_pending_pipe_discard(const WkiChannelIdentity& DATA_CHANNEL, uint32_t resource_id) {
    uint16_t const target_node = DATA_CHANNEL.peer_node_id;
    if (target_node == WKI_NODE_INVALID || DATA_CHANNEL.channel_id != WKI_CHAN_IPC_DATA || resource_id == 0) {
        return;
    }

    std::array<uint8_t, sizeof(DevOpReqPayload) + sizeof(uint32_t)> msg{};
    auto* req = reinterpret_cast<DevOpReqPayload*>(msg.data());
    req->op_id = OP_PIPE_DISCARD_PENDING;
    req->data_len = sizeof(uint32_t);
    std::memcpy(msg.data() + sizeof(DevOpReqPayload), &resource_id, sizeof(resource_id));
    if (enqueue_proxy_pipe_close_tx(DATA_CHANNEL, msg.data(), static_cast<uint16_t>(msg.size()), resource_id, OP_PIPE_DISCARD_PENDING)) {
        return;
    }

    // Allocation failure in the asynchronous TX queue must not turn a
    // retired endpoint into permanent receiver state. Fall back to the same
    // bounded, allocation-free send loop used for terminal pipe closes.
    int ret = WKI_ERR_NO_MEM;
    for (uint32_t attempt = 0; attempt < WKI_IPC_PIPE_EOF_MAX_SEND_ATTEMPTS; ++attempt) {
        ret = wki_send_on_channel_identity(DATA_CHANNEL, MsgType::DEV_OP_REQ, msg.data(), static_cast<uint16_t>(msg.size()));
        if (ret == WKI_OK || proxy_pipe_close_tx_terminal(ret)) {
            return;
        }
        pause_for_ipc_send_retry(ret, attempt);
    }
    ker::mod::dbg::logger<"wki">::warn("IPC pending-discard send failed: target=0x%04x resource_id=%u ret=%d", target_node, resource_id,
                                       ret);
}

auto proxy_register_waiter_locked(ker::util::SmallVec<uint64_t, 2>& waiters, uint64_t pid) -> bool {
    for (auto waiter : waiters) {
        if (waiter == pid) {
            return true;
        }
    }
    return waiters.push_back(pid);
}

void proxy_collect_waiters_locked(ker::util::SmallVec<uint64_t, 2>& waiters, std::array<uint64_t, WKI_IPC_MAX_POLL_WAKE_WAITERS>& pending,
                                  size_t* pending_count) {
    size_t copied = 0;
    while (copied < pending.size() && !waiters.empty()) {
        pending.at(copied++) = waiters.at(0);
        static_cast<void>(waiters.remove_at(0));
    }
    *pending_count = copied;
}

void proxy_reschedule_waiters(const std::array<uint64_t, WKI_IPC_MAX_POLL_WAKE_WAITERS>& waiters, size_t waiter_count) {
    for (auto waiter_pid : std::span(waiters.data(), waiter_count)) {
        auto* waiter = ker::mod::sched::find_task_by_pid_safe(waiter_pid);
        if (waiter == nullptr) {
            continue;
        }

        ker::mod::sched::wake_task_from_event(waiter, ker::mod::sched::EventWakeDeferredSwitch::CANCEL);
        waiter->release();
    }
}

auto proxy_register_reader_locked(ProxyIpcState* proxy, uint64_t pid) -> bool {
    for (auto& slot : proxy->read_waiters) {
        if (slot == pid) return true;
    }
    for (auto& slot : proxy->read_waiters) {
        if (slot == 0) {
            slot = pid;
            return true;
        }
    }
    return false;
}

void proxy_remove_reader_locked(ProxyIpcState* proxy, uint64_t pid) {
    for (auto& slot : proxy->read_waiters) {
        if (slot == pid) slot = 0;
    }
}

void proxy_wake_stream_readers(ProxyIpcState* proxy) {
    static_assert(ProxyIpcState::MAX_READ_WAITERS == WKI_IPC_MAX_POLL_WAKE_WAITERS);
    std::array<uint64_t, WKI_IPC_MAX_POLL_WAKE_WAITERS> pending{};
    size_t count = 0;
    uint64_t const IRQF = proxy->lock.lock_irqsave();
    for (auto& slot : proxy->read_waiters) {
        if (slot != 0) {
            pending.at(count++) = slot;
            slot = 0;
        }
    }
    proxy->lock.unlock_irqrestore(IRQF);
    proxy_reschedule_waiters(pending, count);
}

void proxy_wake_message_write_waiters(ProxyIpcState* proxy) {
    if (proxy == nullptr) {
        return;
    }

    for (;;) {
        std::array<uint64_t, WKI_IPC_MAX_POLL_WAKE_WAITERS> pending = {};
        size_t pending_count = 0;
        uint64_t const IRQF = proxy->lock.lock_irqsave();
        proxy_collect_waiters_locked(proxy->message_write_waiters, pending, &pending_count);
        proxy->lock.unlock_irqrestore(IRQF);
        proxy_reschedule_waiters(pending, pending_count);
        if (pending_count < pending.size()) {
            return;
        }
    }
}

void proxy_grant_message_write_credits(ProxyIpcState* proxy, uint32_t credits) {
    if (proxy == nullptr || credits == 0) {
        return;
    }

    uint32_t current = proxy->message_write_credits.load(std::memory_order_acquire);
    for (;;) {
        uint32_t const ROOM = current < WKI_IPC_MESSAGE_PIPE_WINDOW ? WKI_IPC_MESSAGE_PIPE_WINDOW - current : 0;
        uint32_t const UPDATED = current + std::min(credits, ROOM);
        if (proxy->message_write_credits.compare_exchange_weak(current, UPDATED, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return;
        }
    }
}

auto proxy_reserve_message_write_credits(ProxyIpcState* proxy, size_t requested, bool nonblocking) -> ssize_t {
    if (proxy == nullptr || requested == 0) {
        return 0;
    }

    for (;;) {
        int32_t const WRITE_ERROR = proxy->message_write_error.load(std::memory_order_acquire);
        if (WRITE_ERROR != 0) {
            return WRITE_ERROR;
        }
        if (!proxy->active.load(std::memory_order_acquire)) {
            return -EBADF;
        }

        uint32_t credits = proxy->message_write_credits.load(std::memory_order_acquire);
        while (credits != 0) {
            uint32_t const RESERVED = static_cast<uint32_t>(std::min<size_t>(requested, credits));
            if (proxy->message_write_credits.compare_exchange_weak(credits, credits - RESERVED, std::memory_order_acq_rel,
                                                                   std::memory_order_acquire)) {
                return static_cast<ssize_t>(RESERVED);
            }
        }

        if (nonblocking) {
            return -EAGAIN;
        }
        if (current_task_has_deliverable_signal()) {
            return -EINTR;
        }

        auto* task = ker::mod::sched::get_current_task();
        if (task == nullptr) {
            return -ESRCH;
        }

        bool registered = false;
        uint64_t const IRQF = proxy->lock.lock_irqsave();
        bool const READY = proxy->message_write_error.load(std::memory_order_acquire) != 0 ||
                           !proxy->active.load(std::memory_order_acquire) ||
                           proxy->message_write_credits.load(std::memory_order_acquire) != 0;
        if (!READY) {
            registered = proxy_register_waiter_locked(proxy->message_write_waiters, task->pid);
            if (registered) {
                task->set_wait_channel("wki_proxy_pipe_credit");
            }
        }
        proxy->lock.unlock_irqrestore(IRQF);
        if (READY) {
            continue;
        }
        if (!registered) {
            ker::mod::sched::kern_sleep_us(WKI_IPC_PIPE_WRITE_RETRY_US);
            continue;
        }

        ker::mod::sched::preemptible_syscall_park("wki_proxy_pipe_credit");

        uint64_t const REMOVE_IRQF = proxy->lock.lock_irqsave();
        for (size_t i = 0; i < proxy->message_write_waiters.size(); ++i) {
            if (proxy->message_write_waiters.at(i) == task->pid) {
                static_cast<void>(proxy->message_write_waiters.remove_at(i));
                break;
            }
        }
        proxy->lock.unlock_irqrestore(REMOVE_IRQF);
    }
}

// ---- Proxy fops for pipe (message-based, local ring buffer) ----

void proxy_release(ProxyIpcState* proxy) {
    if (proxy == nullptr) {
        return;
    }
    if (proxy->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        delete[] proxy->ring_buf;
#ifdef WOS_SELFTEST
        g_ipc_proxy_selftest_frees.fetch_add(1, std::memory_order_relaxed);
#endif
        delete proxy;
    }
}

auto proxy_unregister_locked(ProxyIpcState* proxy) -> bool {
    for (auto it = g_ipc_proxies.begin(); it != g_ipc_proxies.end(); ++it) {
        if (*it == proxy) {
            g_ipc_proxies.erase(it);
            return true;
        }
    }
    return false;
}

auto find_proxy_by_endpoint_locked(uint16_t home_node, uint32_t resource_id, bool receipt = false) -> ProxyIpcState* {
    for (auto* proxy : g_ipc_proxies) {
        if (proxy != nullptr && (proxy->active.load(std::memory_order_acquire) || (receipt && proxy->completion_refs != 0)) &&
            proxy->home_node == home_node && proxy->resource_id == resource_id) {
            proxy->refcount.fetch_add(1, std::memory_order_acq_rel);
            return proxy;
        }
    }
    return nullptr;
}

auto proxy_endpoint_rejects_received_data_locked(uint16_t home_node, uint32_t resource_id) -> bool {
    for (const auto* proxy : g_ipc_proxies) {
        if (proxy != nullptr && proxy->active.load(std::memory_order_acquire) && proxy->home_node == home_node &&
            proxy->resource_id == resource_id) {
            return !proxy->can_receive_data;
        }
    }
    return false;
}

// s_ipc_lock protects the issued consumer/type and the pin or mutation that
// follows this decision. Descriptor grants do not use worker credentials.
auto ipc_export_matches_consumer(const WkiIpcExport* exp, uint32_t resource_id, uint16_t consumer_node,
                                 ResourceType expected_type = ResourceType::CUSTOM) -> bool {
    return resource_id != 0 && consumer_node != WKI_NODE_INVALID && consumer_node != WKI_NODE_BROADCAST && exp != nullptr && exp->active &&
           exp->resource_id == resource_id && exp->consumer_node == consumer_node &&
           (expected_type == ResourceType::CUSTOM || exp->res_type == expected_type);
}

auto ipc_acquire_export_file_locked(uint32_t resource_id, uint16_t consumer_node, ResourceType expected_type = ResourceType::CUSTOM)
    -> ker::vfs::File* {
    for (auto* exp : g_ipc_exports) {
        if (!ipc_export_matches_consumer(exp, resource_id, consumer_node, expected_type) || exp->file == nullptr) continue;
        exp->file->refcount.fetch_add(1, std::memory_order_acq_rel);
        return exp->file;
    }
    return nullptr;
}

auto export_has_write_backlog_locked(const WkiIpcExport* exp) -> bool {
    if (exp == nullptr) {
        return false;
    }
    return std::ranges::any_of(g_export_pipe_write_backlogs,
                               [exp](const auto* backlog) { return backlog != nullptr && backlog->exp == exp; });
}

auto export_can_retire_locked(const WkiIpcExport* exp) -> bool {
    if (exp == nullptr) {
        return true;
    }
    if ((exp->active && exp->file != nullptr) || exp->pipe_write_in_flight || exp->pipe_secure_reserved) {
        return false;
    }
    return !export_has_write_backlog_locked(exp) && !export_has_pipe_pump_slot_locked(exp);
}

auto compact_inactive_exports_locked() -> WkiIpcExport* {
    WkiIpcExport* retired = nullptr;
    for (auto it = g_ipc_exports.begin(); it != g_ipc_exports.end();) {
        auto* exp = *it;
        if (!export_can_retire_locked(exp)) {
            ++it;
            continue;
        }

        it = g_ipc_exports.erase(it);
        if (exp == nullptr) {
            continue;
        }

        exp->pump_next = retired;
        retired = exp;
    }
    return retired;
}

// Core unregister closes admission and returns BUSY while exact native/job
// pins remain. Keep context and backing intact until unregister0. Callers hold
// no IPC lock; task retirement retries without waiting under a gate.
auto release_export_pipe_rdma(WkiIpcExport& exp) -> bool {
    if (exp.pipe_secure_descriptor.rkey != 0) {
        if (wki_secure_rdma_unregister_region(exp.pipe_secure_descriptor) != 0) return false;
        exp.pipe_rdma_enabled = false;
        exp.pipe_secure_descriptor = {};
    }
    exp.pipe_rdma_rkey = 0;
    auto* region = std::exchange(exp.pipe_rdma_region, nullptr);
    if (region != nullptr) std::fill_n(region, exp.pipe_rdma_region_size, uint8_t{0});
    exp.pipe_rdma_region_size = 0;
    exp.pipe_rdma_capacity = 0;
    delete[] region;
    return true;
}

void free_retired_exports(WkiIpcExport* retired) {
    bool wake = false;
    while (retired != nullptr) {
        auto* next = retired->pump_next;
        retired->pump_next = nullptr;
        if (retired->pipe_secure_descriptor.rkey != 0) {
            // Context/backing stays reachable until task-context unregister0.
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            retired->active = false;
            retired->pump_next = g_secure_retired_exports;
            g_secure_retired_exports = retired;
            s_ipc_lock.unlock_irqrestore(IRQF);
            wake = true;
        } else {
            auto* file = retired->file;
            retired->file = nullptr;
            static_cast<void>(release_export_pipe_rdma(*retired));
            ipc_release_file_ref(file);
#ifdef WOS_SELFTEST
            g_ipc_export_selftest_frees.fetch_add(1, std::memory_order_relaxed);
#endif
            delete retired;
        }
        retired = next;
    }
    if (wake) wake_export_pipe_write_flush_worker(0);
}

void service_secure_retired_exports() {
    // One bounded detached batch; requeue BUSY without waiting or freeing.
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    auto* retired = std::exchange(g_secure_retired_exports, nullptr);
    s_ipc_lock.unlock_irqrestore(IRQF);
    while (retired != nullptr) {
        auto* next = retired->pump_next;
        retired->pump_next = nullptr;
        if (release_export_pipe_rdma(*retired)) {
            auto* file = retired->file;
            retired->file = nullptr;
            ipc_release_file_ref(file);
#ifdef WOS_SELFTEST
            g_ipc_export_selftest_frees.fetch_add(1, std::memory_order_relaxed);
#endif
            delete retired;
        } else {
            uint64_t const BUSY_IRQF = s_ipc_lock.lock_irqsave();
            retired->pump_next = g_secure_retired_exports;
            g_secure_retired_exports = retired;
            s_ipc_lock.unlock_irqrestore(BUSY_IRQF);
        }
        retired = next;
    }
}

void ipc_release_file_ref(ker::vfs::File* file) { ker::vfs::vfs_put_file(file); }

auto find_pending_pipe_delivery_locked(uint16_t home_node, uint32_t resource_id) -> PendingPipeDelivery* {
    for (auto* pending : g_pending_pipe_deliveries) {
        if (pending != nullptr && pending->home_node == home_node && pending->resource_id == resource_id) {
            return pending;
        }
    }
    return nullptr;
}

void erase_pending_pipe_delivery_locked(PendingPipeDelivery* pending) {
    if (pending == nullptr) {
        return;
    }

    for (auto it = g_pending_pipe_deliveries.begin(); it != g_pending_pipe_deliveries.end(); ++it) {
        if (*it == pending) {
            g_pending_pipe_deliveries.erase(it);
            return;
        }
    }
}

auto take_pending_pipe_delivery_locked(uint16_t home_node, uint32_t resource_id) -> PendingPipeDelivery* {
    auto* pending = find_pending_pipe_delivery_locked(home_node, resource_id);
    if (pending != nullptr) {
        erase_pending_pipe_delivery_locked(pending);
    }
    return pending;
}

void free_pending_pipe_delivery(PendingPipeDelivery* pending) {
    if (pending == nullptr) {
        return;
    }
    for (auto& chunk : pending->chunks) {
        release_pending_pipe_chunk(chunk);
    }
    free_ipc_dev_op_work(pending->close_owner);
    pending->close_owner = nullptr;
#ifdef WOS_SELFTEST
    g_ipc_pending_delivery_selftest_frees.fetch_add(1, std::memory_order_relaxed);
#endif
    delete pending;
}

auto discard_pending_pipe_delivery(uint16_t home_node, uint32_t resource_id) -> bool {
    PendingPipeDelivery* pending = nullptr;
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    pending = take_pending_pipe_delivery_locked(home_node, resource_id);
    s_ipc_lock.unlock_irqrestore(IRQF);
    free_pending_pipe_delivery(pending);
    return pending != nullptr;
}

auto find_export_pipe_write_backlog_locked(WkiIpcExport* exp) -> ExportPipeWriteBacklog* {
    for (auto* backlog : g_export_pipe_write_backlogs) {
        if (backlog != nullptr && backlog->exp == exp) {
            return backlog;
        }
    }
    return nullptr;
}

auto export_pipe_write_backlog_shard(const ExportPipeWriteBacklog* backlog) -> size_t {
    if (backlog == nullptr) {
        return 0;
    }
    return static_cast<size_t>(backlog->resource_id % WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_COUNT);
}

void wake_export_pipe_write_flush_worker(size_t shard) {
    if (shard >= g_export_pipe_write_flush_tasks.size()) {
        return;
    }
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    auto* task = g_export_pipe_write_flush_tasks.at(shard);
    if (task != nullptr && !task->try_acquire_lifetime_ref()) task = nullptr;
    s_ipc_lock.unlock_irqrestore(IRQF);
    if (task != nullptr) {
        ker::mod::sched::kern_wake(task);
        task->release();
    }
}

void wake_all_export_pipe_write_flush_workers() {
    for (size_t shard = 0; shard < g_export_pipe_write_flush_tasks.size(); ++shard) {
        wake_export_pipe_write_flush_worker(shard);
    }
}

auto export_pipe_write_flush_queue_depth_locked() -> size_t {
    size_t depth = 0;
    for (const auto& queue : g_export_pipe_write_flush_queues) {
        depth += queue.size();
    }
    return depth;
}

void erase_export_pipe_write_backlog_locked(ExportPipeWriteBacklog* backlog) {
    if (backlog == nullptr) {
        return;
    }

    for (auto it = g_export_pipe_write_backlogs.begin(); it != g_export_pipe_write_backlogs.end(); ++it) {
        if (*it == backlog) {
            g_export_pipe_write_backlogs.erase(it);
            break;
        }
    }

    for (auto& queue : g_export_pipe_write_flush_queues) {
        for (auto it = queue.begin(); it != queue.end();) {
            if (*it == backlog) {
                it = queue.erase(it);
                continue;
            }
            ++it;
        }
    }

    backlog->queued = false;
}

void free_export_pipe_write_backlog(ExportPipeWriteBacklog* backlog) {
    if (backlog == nullptr) {
        return;
    }

    for (auto& chunk : backlog->chunks) {
        release_pending_pipe_chunk(chunk);
    }
    delete backlog;
}

auto send_export_pipe_write_flow(const WkiChannelIdentity& control, uint32_t resource_id, uint32_t credit_bytes, int32_t status) -> bool {
    if (control.peer_node_id == WKI_NODE_INVALID || control.channel_id != WKI_CHAN_RESOURCE || (credit_bytes == 0 && status == 0)) {
        return false;
    }

    constexpr size_t MSG_SIZE = sizeof(DevOpReqPayload) + sizeof(IpcPipeWriteFlowPayload);
    std::array<uint8_t, MSG_SIZE> msg = {};
    auto* req = reinterpret_cast<DevOpReqPayload*>(msg.data());
    req->op_id = OP_PIPE_WRITE_FLOW;
    req->data_len = sizeof(IpcPipeWriteFlowPayload);

    IpcPipeWriteFlowPayload flow = {
        .resource_id = resource_id,
        .credit_bytes = credit_bytes,
        .status = status,
    };
    std::memcpy(msg.data() + sizeof(DevOpReqPayload), &flow, sizeof(flow));

    // The caller retains its backlog chunk/error or deferred DATA work until
    // this succeeds. Retry on a later worker visit without losing the receipt
    // or holding up every other pipe in the shard for a long retry loop.
    return wki_send_on_channel_identity(control, MsgType::DEV_OP_REQ, msg.data(), static_cast<uint16_t>(msg.size())) == WKI_OK;
}

auto complete_export_pipe_flow_receipt_locked(ExportPipeWriteBacklog* backlog, bool accepted) -> bool {
    if (!accepted) return false;
    if (backlog->write_error == 0) {
        release_pending_pipe_chunk(backlog->chunks.front());
        backlog->chunks.pop_front();
    }
    return true;
}

struct FailedExportPipeWriteRetirement {
    WkiIpcExport* retired_exports = nullptr;
    ker::vfs::File* export_file = nullptr;
    uint16_t consumer_node = WKI_NODE_INVALID;
    bool message_write_flow_control = false;
};

// s_ipc_lock must be held. The flush worker owns one temporary File reference
// in addition to the export's lifetime reference. Return the latter separately
// so both ownerships are released after dropping the spinlock.
auto retire_failed_export_pipe_write_locked(ExportPipeWriteBacklog* backlog, WkiIpcExport* exp, ker::vfs::File* acquired_file)
    -> FailedExportPipeWriteRetirement {
    FailedExportPipeWriteRetirement retirement;
    if (backlog == nullptr || exp == nullptr || exp->file != acquired_file) {
        return retirement;
    }

    retirement.export_file = exp->file;
    retirement.consumer_node = exp->consumer_node;
    retirement.message_write_flow_control = exp->message_write_flow_control;
    exp->file = nullptr;
    exp->active = false;
    erase_export_pipe_write_backlog_locked(backlog);
    retirement.retired_exports = compact_inactive_exports_locked();
    return retirement;
}

auto ensure_export_pipe_write_backlog_locked(WkiIpcExport* exp) -> ExportPipeWriteBacklog* {
    auto* backlog = find_export_pipe_write_backlog_locked(exp);
    if (backlog != nullptr) {
        return backlog;
    }

    backlog = new ExportPipeWriteBacklog();
    if (backlog == nullptr) {
        return nullptr;
    }
    backlog->exp = exp;
    backlog->resource_id = exp->resource_id;
    g_export_pipe_write_backlogs.push_back(backlog);
    return backlog;
}

void queue_export_pipe_write_flush_locked(ExportPipeWriteBacklog* backlog) {
    if (backlog == nullptr || backlog->queued) {
        return;
    }

    backlog->queued = true;
    g_export_pipe_write_flush_queues.at(export_pipe_write_backlog_shard(backlog)).push_back(backlog);
}

void note_export_pipe_data_received_locked(WkiIpcExport* exp, uint16_t len) {
    if (exp == nullptr || len == 0) {
        return;
    }

    exp->pipe_bytes_received += len;
    auto* backlog = find_export_pipe_write_backlog_locked(exp);
    if (backlog != nullptr && backlog->close_pending) {
        queue_export_pipe_write_flush_locked(backlog);
    }
}

void publish_export_pipe_write_chunk_locked(ExportPipeWriteBacklog* backlog, const uint8_t* data, uint16_t len,
                                            IpcDevOpWork* transfer_owner, IpcDevOpWork** work_owner, bool flow_controlled) {
    backlog->chunks.push_back(
        PendingPipeChunk{.data = data, .len = len, .flow_credit = flow_controlled ? len : uint16_t{0}, .dev_op_owner = transfer_owner});
    if (transfer_owner != nullptr) {
        // Publication is the ownership handoff. The flush worker may release
        // the work as soon as s_ipc_lock is dropped, so the deferred worker
        // must stop dereferencing its header and payload after this call.
        *work_owner = nullptr;
    }
}

auto queue_export_pipe_write_data(uint32_t resource_id, const uint8_t* data, uint16_t len, IpcDevOpWork** work_owner,
                                  bool flow_controlled = false) -> bool {
    if (data == nullptr || len == 0) {
        return true;
    }

    IpcDevOpWork* const TRANSFER_OWNER =
        ipc_dev_op_work_can_back_pipe_chunk(work_owner != nullptr ? *work_owner : nullptr, data, len) ? *work_owner : nullptr;
    uint8_t* copy = nullptr;
    if (TRANSFER_OWNER == nullptr) {
        copy = new (std::nothrow) uint8_t[len];
        if (copy == nullptr) {
            return false;
        }
        std::memcpy(copy, data, len);
    }

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    auto* exp = find_export_by_resource_id(resource_id);
    if (exp == nullptr || !exp->active || exp->file == nullptr || exp->pipe_secure_reserved ||
        len > UINT64_MAX - exp->pipe_bytes_received) {
        s_ipc_lock.unlock_irqrestore(IRQF);
        delete[] copy;
        return false;
    }

    auto* backlog = ensure_export_pipe_write_backlog_locked(exp);
    if (backlog == nullptr) {
        s_ipc_lock.unlock_irqrestore(IRQF);
        delete[] copy;
        return false;
    }

    publish_export_pipe_write_chunk_locked(backlog, TRANSFER_OWNER != nullptr ? data : copy, len, TRANSFER_OWNER, work_owner,
                                           flow_controlled);
    backlog->buffered_bytes += len;
    note_export_pipe_data_received_locked(exp, len);
    queue_export_pipe_write_flush_locked(backlog);
    size_t const FLUSH_SHARD = export_pipe_write_backlog_shard(backlog);
    s_ipc_lock.unlock_irqrestore(IRQF);

    wake_export_pipe_write_flush_worker(FLUSH_SHARD);
    return true;
}

void init_nonblocking_pipe_write_view(ker::vfs::File& view, const ker::vfs::File& source) {
    view.fd = source.fd;
    view.private_data = source.private_data;
    view.fops = source.fops;
    view.pos = source.pos;
    view.is_directory = source.is_directory;
    view.fs_type = source.fs_type;
    view.refcount.store(source.refcount.load(std::memory_order_acquire), std::memory_order_release);
    view.open_flags = source.open_flags | WKI_IPC_O_NONBLOCK;
    view.fd_flags = source.fd_flags;
    view.vfs_path = source.vfs_path;
    view.dir_fs_count = source.dir_fs_count;
    view.stream_cache_attachment = source.stream_cache_attachment;
    view.cache_notify_attachment = source.cache_notify_attachment;
    view.positional_read_depth.store(source.positional_read_depth.load(std::memory_order_acquire), std::memory_order_release);
}

auto export_pipe_write_bounded(ker::vfs::File* write_file, const uint8_t* data, uint16_t len, size_t max_calls) -> ssize_t {
    size_t written = 0;
    ssize_t result = 0;
    for (size_t call = 0; call < max_calls && written < len; ++call) {
        size_t const REMAINING = len - written;
        result = clamp_io_count(write_file->fops->vfs_write(write_file, data + written, REMAINING, static_cast<size_t>(write_file->pos)),
                                REMAINING);
        if (result <= 0) {
            break;
        }
        written += static_cast<size_t>(result);
    }
    return written != 0 ? static_cast<ssize_t>(written) : result;
}

auto export_pipe_write_nonblocking(ker::vfs::File* file, const uint8_t* data, uint16_t len, bool grow_capacity = true) -> ssize_t {
    if (len == 0) {
        return 0;
    }
    if (file == nullptr || data == nullptr || file->fops == nullptr || file->fops->vfs_write == nullptr) {
        return -EBADF;
    }

    bool const IS_PIPE = ker::vfs::vfs_is_pipe_file(file);
    bool const IS_PROXY_PIPE = is_proxy_pipe_file(file);
    ker::vfs::File pipe_write_view{};
    ker::vfs::File* write_file = file;
    if (IS_PIPE && grow_capacity) {
        // Only actual remote output may grow the source pipe. Submission
        // preparation and rejection must leave its capacity untouched. The
        // deferred caller owns File lifetime and holds no IPC/FD lock here;
        // optional allocation failure retains normal bounded-pipe behavior.
        static_cast<void>(ker::vfs::vfs_pipe_reserve_capacity(file, WKI_IPC_EXPORT_PIPE_CAPACITY));
    }
    if (IS_PIPE || IS_PROXY_PIPE || ker::vfs::vfs_is_socket_file(file)) {
        // A relay must not block its shared flush worker on upstream credits,
        // and changing the held File's flags would affect its other owners.
        init_nonblocking_pipe_write_view(pipe_write_view, *file);
        write_file = &pipe_write_view;
    }

    clear_current_daemon_sigpipe();
    bool const BURST = IS_PIPE && len <= WKI_IPC_PIPE_DATA_MAX_CHUNK;
    size_t const MAX_CALLS = BURST ? WKI_IPC_EXPORT_PIPE_WRITE_BURST_CALLS : 1;
    ssize_t const RET = export_pipe_write_bounded(write_file, data, len, MAX_CALLS);
    clear_current_daemon_sigpipe();

    return RET;
}

auto ipc_secure_same_request(const WkiSecureRdmaRequest& a, const WkiSecureRdmaRequest& b) -> bool {
    return std::memcmp(&a.grant, &b.grant, sizeof(a.grant)) == 0 && a.operation_id == b.operation_id &&
           a.receive_ticket == b.receive_ticket && a.offset == b.offset && a.length == b.length && a.peer_node == b.peer_node &&
           a.flags == b.flags && a.opcode == b.opcode && a.intent_length == b.intent_length && a.native_intent == b.native_intent;
}

auto ipc_secure_intent(const WkiSecureRdmaRequest& request, IpcSecureIntent& intent) -> bool {
    if (request.intent_length != sizeof(intent) || request.flags != 0 || request.operation_id == 0 || request.receive_ticket == 0)
        return false;
    std::memcpy(&intent, request.native_intent.data(), sizeof(intent));
    return intent.version == IPC_SECURE_VERSION && intent.resource_id != 0 && intent.lifetime != 0 &&
           std::ranges::all_of(intent.reserved, [](uint8_t value) { return value == 0; });
}

auto ipc_secure_credit_locked(const WkiIpcExport& exp) -> uint32_t {
    auto* backlog = find_export_pipe_write_backlog_locked(const_cast<WkiIpcExport*>(&exp));
    if (exp.pipe_write_in_flight ||
        (backlog != nullptr && (!backlog->chunks.empty() || backlog->close_pending || backlog->write_error != 0)) ||
        exp.pipe_bytes_received < exp.pipe_bytes_delivered || exp.pipe_bytes_received - exp.pipe_bytes_delivered > exp.pipe_rdma_capacity)
        return 0;
    return exp.pipe_rdma_capacity - static_cast<uint32_t>(exp.pipe_bytes_received - exp.pipe_bytes_delivered);
}

auto ipc_secure_arm(void* context, const WkiSecureRdmaRequest& request, uint64_t& generation) -> int {
    auto* exp = static_cast<WkiIpcExport*>(context);
    IpcSecureIntent intent{};
    if (exp == nullptr || !ipc_secure_intent(request, intent)) return -EINVAL;
    if (!ipc_channels_live(exp->channels, request.peer_node)) return -ESTALE;
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    int status = -ESTALE;
    if (!g_ipc_consumer_stopping.load(std::memory_order_acquire) && exp->active && exp->pipe_secure_activated && exp->pipe_rdma_enabled &&
        exp->file != nullptr && exp->res_type == ResourceType::IPC_PIPE && exp->consumer_node == request.peer_node &&
        exp->pipe_rdma_capacity != 0 && exp->pipe_secure_descriptor.service == static_cast<uint8_t>(rdma_protocol::Service::IPC) &&
        exp->pipe_secure_descriptor.binding_generation == exp->pipe_secure_lifetime &&
        !ipc_peer_cleanup_active_locked(exp->consumer_node) &&
        exp->pipe_secure_epoch == ipc_peer_cleanup_epoch_locked(exp->consumer_node) && intent.resource_id == exp->resource_id &&
        intent.lifetime == exp->pipe_secure_lifetime &&
        std::memcmp(&request.grant, &exp->pipe_secure_descriptor, sizeof(request.grant)) == 0) {
        status = -EAGAIN;
        if (!exp->pipe_secure_reserved && !exp->pipe_write_in_flight) {
            if (intent.kind == IPC_SECURE_QUERY && request.opcode == rdma_protocol::Opcode::READ && request.offset == 0 &&
                request.length == sizeof(IpcSecureSnapshot) && intent.head == 0 && intent.tail == 0 && intent.credit == 0)
                status = 0;
            else if (intent.kind == IPC_SECURE_APPEND && request.opcode == rdma_protocol::Opcode::WRITE && request.length != 0 &&
                     intent.head == exp->pipe_bytes_received && intent.tail == exp->pipe_bytes_delivered &&
                     intent.credit == ipc_secure_credit_locked(*exp) && intent.credit != 0 && request.length <= intent.credit &&
                     request.length <= UINT16_MAX && request.offset == intent.head % exp->pipe_rdma_capacity &&
                     request.length <= exp->pipe_rdma_capacity - request.offset && request.length <= UINT64_MAX - intent.head)
                status = 0;
            if (status == 0) {
                exp->pipe_secure_reserved = true;
                exp->pipe_secure_request = request;
                exp->pipe_secure_file = exp->file;
                exp->file->refcount.fetch_add(1, std::memory_order_acq_rel);
                generation = exp->pipe_secure_lifetime;
            }
        }
    }
    s_ipc_lock.unlock_irqrestore(IRQF);
    return status;
}

auto ipc_secure_effect(void* context, const WkiSecureRdmaRequest& request, std::span<uint8_t> backing, std::span<const uint8_t> input,
                       std::span<uint8_t> output) -> WkiSecureRdmaResult {
    auto* exp = static_cast<WkiIpcExport*>(context);
    IpcSecureIntent intent{};
    if (exp == nullptr || !ipc_secure_intent(request, intent)) return {-EINVAL, 0, WkiSecureRdmaOutcome::NO_EFFECT};
    if (!ipc_channels_live(exp->channels, request.peer_node)) return {-ESTALE, 0, WkiSecureRdmaOutcome::NO_EFFECT};
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    bool const EXACT = exp->pipe_secure_reserved && ipc_secure_same_request(request, exp->pipe_secure_request) &&
                       backing.data() == exp->pipe_rdma_region && backing.size() == exp->pipe_rdma_region_size;
    bool const LIVE = EXACT && exp->active && exp->pipe_secure_activated && exp->file == exp->pipe_secure_file &&
                      !ipc_peer_cleanup_active_locked(exp->consumer_node) &&
                      exp->pipe_secure_epoch == ipc_peer_cleanup_epoch_locked(exp->consumer_node);
    if (!LIVE) {
        s_ipc_lock.unlock_irqrestore(IRQF);
        return {-ESTALE, 0, WkiSecureRdmaOutcome::NO_EFFECT};
    }
    if (intent.kind == IPC_SECURE_QUERY) {
        if (!input.empty() || output.size() != sizeof(IpcSecureSnapshot)) {
            s_ipc_lock.unlock_irqrestore(IRQF);
            return {-EINVAL, 0, WkiSecureRdmaOutcome::NO_EFFECT};
        }
        IpcSecureSnapshot const SNAPSHOT{.resource_id = exp->resource_id,
                                         .lifetime = exp->pipe_secure_lifetime,
                                         .head = exp->pipe_bytes_received,
                                         .tail = exp->pipe_bytes_delivered,
                                         .credit = ipc_secure_credit_locked(*exp)};
        s_ipc_lock.unlock_irqrestore(IRQF);
        std::memcpy(output.data(), &SNAPSHOT, sizeof(SNAPSHOT));
        return {0, sizeof(SNAPSHOT), WkiSecureRdmaOutcome::EXACT};
    }
    auto* epoch = ipc_peer_cleanup_slot_locked(exp->consumer_node, false);
    if (intent.kind != IPC_SECURE_APPEND || input.size() != request.length || !output.empty() || epoch == nullptr ||
        epoch->active_workers == UINT32_MAX || exp->pipe_write_in_flight || intent.head != exp->pipe_bytes_received ||
        intent.tail != exp->pipe_bytes_delivered || intent.credit != ipc_secure_credit_locked(*exp) || request.length > intent.credit ||
        request.offset > backing.size() || request.length > backing.size() - request.offset) {
        s_ipc_lock.unlock_irqrestore(IRQF);
        return {-EAGAIN, 0, WkiSecureRdmaOutcome::NO_EFFECT};
    }
    ++epoch->active_workers;
    exp->pipe_secure_effect_claimed = true;
    exp->pipe_write_in_flight = true;
    auto* file = exp->pipe_secure_file;
    s_ipc_lock.unlock_irqrestore(IRQF);
    // Full authenticated coverage is immutable before any private/native effect.
    std::memcpy(backing.data() + request.offset, input.data(), input.size());
    // The common service owner must not allocate/reclaim while executing a
    // native effect. Ordinary shard writes retain their optional pipe growth.
    ssize_t const WRITTEN =
        export_pipe_write_nonblocking(file, backing.data() + request.offset, static_cast<uint16_t>(request.length), false);
    uint32_t const PREFIX = WRITTEN > 0 ? static_cast<uint32_t>(std::min<ssize_t>(WRITTEN, request.length)) : 0;
    uint64_t const DONE_IRQF = s_ipc_lock.lock_irqsave();
    // The claimed native File remains pinned through close. Publish its actual
    // prefix even if native admission retired while VFS ran; never resolve a replacement.
    exp->pipe_bytes_received += PREFIX;
    exp->pipe_bytes_delivered += PREFIX;
    exp->pipe_write_in_flight = false;
    exp->pipe_secure_effect_claimed = false;
    release_ipc_dev_op_worker_locked(exp->consumer_node);
    s_ipc_lock.unlock_irqrestore(DONE_IRQF);
    if (PREFIX != 0) return {0, PREFIX, WkiSecureRdmaOutcome::EXACT};
    if (WRITTEN == -EAGAIN || WRITTEN == -EPIPE || WRITTEN == -EINTR)
        return {static_cast<int32_t>(WRITTEN), 0, WkiSecureRdmaOutcome::NO_EFFECT};
    return {WRITTEN < 0 ? static_cast<int32_t>(WRITTEN) : -EIO, 0, WkiSecureRdmaOutcome::UNCERTAIN};
}

void ipc_secure_release(void* context, const WkiSecureRdmaRequest& request, const WkiSecureRdmaResult& /*result*/) {
    auto* exp = static_cast<WkiIpcExport*>(context);
    if (exp == nullptr) return;
    ker::vfs::File* file = nullptr;
    size_t shard = 0;
    bool wake = false;
    WkiIpcExport* retired = nullptr;
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    if (exp->pipe_secure_reserved && !exp->pipe_secure_effect_claimed && ipc_secure_same_request(request, exp->pipe_secure_request)) {
        file = std::exchange(exp->pipe_secure_file, nullptr);
        exp->pipe_secure_reserved = false;
        exp->pipe_secure_request = {};
        auto* backlog = find_export_pipe_write_backlog_locked(exp);
        if (backlog != nullptr) {
            queue_export_pipe_write_flush_locked(backlog);
            shard = export_pipe_write_backlog_shard(backlog);
            wake = true;
        }
        retired = compact_inactive_exports_locked();
    }
    s_ipc_lock.unlock_irqrestore(IRQF);
    ipc_release_file_ref(file);
    free_retired_exports(retired);
    if (wake) wake_export_pipe_write_flush_worker(shard);
}

constexpr WkiSecureRdmaHooks IPC_SECURE_HOOKS{ipc_secure_arm, ipc_secure_effect, ipc_secure_release};

auto mark_export_pipe_write_closed(uint32_t resource_id, uint16_t consumer_node, uint64_t expected_bytes = 0,
                                   bool has_expected_bytes = false, ResourceType expected_type = ResourceType::CUSTOM) -> bool {
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    auto* exp = find_export_by_resource_id(resource_id);
    if (!ipc_export_matches_consumer(exp, resource_id, consumer_node, expected_type) || exp->file == nullptr) {
        s_ipc_lock.unlock_irqrestore(IRQF);
        return false;
    }

    auto* backlog = ensure_export_pipe_write_backlog_locked(exp);
    if (backlog == nullptr) {
        s_ipc_lock.unlock_irqrestore(IRQF);
        return false;
    }

    backlog->close_pending = true;
    if (has_expected_bytes) {
        backlog->close_has_expected_bytes = true;
        backlog->close_expected_bytes = expected_bytes;
    }
    queue_export_pipe_write_flush_locked(backlog);
    size_t const FLUSH_SHARD = export_pipe_write_backlog_shard(backlog);
    s_ipc_lock.unlock_irqrestore(IRQF);

    wake_export_pipe_write_flush_worker(FLUSH_SHARD);
    return true;
}

// Called under s_ipc_lock after the sole shard worker returns from VFS or a
// receipt send. Both operations pin the exact exporter while outside the lock.
// Detachment may have removed backlog ownership while the worker pinned the
// exporter/ring. Finish that retirement before dropping the independent File
// reference; never resolve the old owner through a possibly detached backlog.
auto finish_export_pipe_write_locked(ExportPipeWriteBacklog* backlog, WkiIpcExport* owner, ker::vfs::File* file, WkiIpcExport** retired)
    -> WkiIpcExport* {
    owner->pipe_write_in_flight = false;
    auto* exp = backlog->exp;
    if (exp != owner || !exp->active || exp->file != file) {
        erase_export_pipe_write_backlog_locked(backlog);
        *retired = compact_inactive_exports_locked();
        return nullptr;
    }
    return exp;
}

void export_pipe_write_flush_thread_loop(size_t shard) {
    for (;;) {
        auto const HOLD = ker::mod::sched::hold_kernel_shutdown();
        if (g_ipc_consumer_stopping.load(std::memory_order_acquire) || ker::mod::sched::kernel_threads_shutdown_requested()) {
            g_ipc_consumer_stopping.store(true, std::memory_order_release);
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            auto* worker = std::exchange(g_export_pipe_write_flush_tasks.at(shard), nullptr);
            s_ipc_lock.unlock_irqrestore(IRQF);
            if (worker != nullptr) worker->release();
            g_export_pipe_write_flush_stopped.at(shard).store(true, std::memory_order_release);
            return;
        }
        if (shard == 0) service_secure_retired_exports();
        ExportPipeWriteBacklog* backlog = nullptr;
        uint64_t irqf = s_ipc_lock.lock_irqsave();
        auto& queue = g_export_pipe_write_flush_queues.at(shard);
        if (!queue.empty()) {
            backlog = queue.front();
            queue.pop_front();
            if (backlog != nullptr) {
                backlog->queued = false;
            }
        }
        s_ipc_lock.unlock_irqrestore(irqf);

        if (backlog == nullptr) {
            uint64_t const RETIRE_IRQF = s_ipc_lock.lock_irqsave();
            bool const RETIRING = shard == 0 && g_secure_retired_exports != nullptr;
            s_ipc_lock.unlock_irqrestore(RETIRE_IRQF);
            if (RETIRING)
                ker::mod::sched::kern_sleep_us(WKI_IPC_PIPE_RDMA_WATCH_US);
            else
                ker::mod::sched::kern_block();
            continue;
        }

        WkiIpcExport* exp = nullptr;
        ker::vfs::File* file = nullptr;
        uint32_t resource_id = 0;
        uint16_t remaining = 0;
        uint8_t const* data_ptr = nullptr;
        bool close_pending = false;
        bool close_ready = false;

        irqf = s_ipc_lock.lock_irqsave();
        exp = backlog->exp;
        if (exp == nullptr || !exp->active || exp->file == nullptr) {
            erase_export_pipe_write_backlog_locked(backlog);
            s_ipc_lock.unlock_irqrestore(irqf);
            free_export_pipe_write_backlog(backlog);
            continue;
        }

        if (exp->pipe_secure_reserved) {
            queue_export_pipe_write_flush_locked(backlog);
            s_ipc_lock.unlock_irqrestore(irqf);
            ker::mod::sched::kern_sleep_us(WKI_IPC_PIPE_WRITE_RETRY_US);
            continue;
        }

        file = exp->file;
        resource_id = exp->resource_id;
        close_pending = backlog->close_pending;
        close_ready = close_pending && (!backlog->close_has_expected_bytes || exp->pipe_bytes_received >= backlog->close_expected_bytes);

        bool const FLOW_RECEIPT =
            backlog->write_error != 0 || (!backlog->chunks.empty() && backlog->chunks.front().offset == backlog->chunks.front().len &&
                                          backlog->chunks.front().flow_credit != 0);
        if (FLOW_RECEIPT) {
            int32_t const ERROR = backlog->write_error;
            uint16_t const CREDIT = ERROR == 0 ? backlog->chunks.front().flow_credit : uint16_t{0};
            auto const CONTROL_CHANNEL = exp->channels.control;
            file->refcount.fetch_add(1, std::memory_order_acq_rel);
            exp->pipe_write_in_flight = true;
            WkiIpcExport* const RECEIPT_OWNER = exp;
            s_ipc_lock.unlock_irqrestore(irqf);

            bool const ACCEPTED = send_export_pipe_write_flow(CONTROL_CHANNEL, resource_id, CREDIT, ERROR);

            WkiIpcExport* retired = nullptr;
            irqf = s_ipc_lock.lock_irqsave();
            exp = finish_export_pipe_write_locked(backlog, RECEIPT_OWNER, file, &retired);
            if (exp == nullptr) {
                s_ipc_lock.unlock_irqrestore(irqf);
                free_export_pipe_write_backlog(backlog);
                free_retired_exports(retired);
                ipc_release_file_ref(file);
                continue;
            }
            if (!complete_export_pipe_flow_receipt_locked(backlog, ACCEPTED)) {
                queue_export_pipe_write_flush_locked(backlog);
                s_ipc_lock.unlock_irqrestore(irqf);
                ipc_release_file_ref(file);
                ker::mod::sched::kern_sleep_us(WKI_IPC_PIPE_WRITE_RETRY_US);
                continue;
            }
            if (ERROR != 0) {
                FailedExportPipeWriteRetirement const RETIREMENT = retire_failed_export_pipe_write_locked(backlog, exp, file);
                s_ipc_lock.unlock_irqrestore(irqf);
                ipc_release_file_ref(file);
                ipc_release_file_ref(RETIREMENT.export_file);
                free_export_pipe_write_backlog(backlog);
                free_retired_exports(RETIREMENT.retired_exports);
                continue;
            }
            bool const FREE_BACKLOG = !backlog->persistent_for_rdma && backlog->chunks.empty() && !backlog->close_pending;
            if (FREE_BACKLOG)
                erase_export_pipe_write_backlog_locked(backlog);
            else
                queue_export_pipe_write_flush_locked(backlog);
            s_ipc_lock.unlock_irqrestore(irqf);
            ipc_release_file_ref(file);
            if (FREE_BACKLOG) free_export_pipe_write_backlog(backlog);
            continue;
        }

        if (!backlog->chunks.empty()) {
            file->refcount.fetch_add(1, std::memory_order_acq_rel);
            auto& chunk = backlog->chunks.front();
            remaining = static_cast<uint16_t>(chunk.len - chunk.offset);
            data_ptr = chunk.data + chunk.offset;
        } else if (close_pending) {
            if (!close_ready) {
                s_ipc_lock.unlock_irqrestore(irqf);
                continue;
            }
            auto* pump_task = stop_pipe_pump_locked(exp);
            auto const DATA_CHANNEL = exp->channels.data;
            bool const DIRECT_DISCARD = pump_task == nullptr;
            exp->file = nullptr;
            exp->active = false;
            erase_export_pipe_write_backlog_locked(backlog);
            auto* retired_exports = compact_inactive_exports_locked();
            s_ipc_lock.unlock_irqrestore(irqf);
            wake_pipe_pump(pump_task);
            ipc_release_file_ref(file);
            free_export_pipe_write_backlog(backlog);
            free_retired_exports(retired_exports);
            if (DIRECT_DISCARD) {
                // A pump that is still assigned publishes this fence after
                // its last DATA/EOF send. If it already retired, publish the
                // same-channel fence here so an EOF cannot outlive close.
                schedule_pending_pipe_discard(DATA_CHANNEL, resource_id);
            }
            continue;
        } else {
            if (!backlog->persistent_for_rdma) {
                erase_export_pipe_write_backlog_locked(backlog);
            } else {
                queue_export_pipe_write_flush_locked(backlog);
            }
            s_ipc_lock.unlock_irqrestore(irqf);
            if (!backlog->persistent_for_rdma) {
                free_export_pipe_write_backlog(backlog);
            } else {
                ker::mod::sched::kern_sleep_us(WKI_IPC_PIPE_RDMA_WATCH_US);
            }
            continue;
        }
        exp->pipe_write_in_flight = true;
        WkiIpcExport* const WRITE_OWNER = exp;
        s_ipc_lock.unlock_irqrestore(irqf);

        ssize_t const WRITE_RET = export_pipe_write_nonblocking(file, data_ptr, remaining);

        WkiIpcExport* retired = nullptr;
        irqf = s_ipc_lock.lock_irqsave();
        exp = finish_export_pipe_write_locked(backlog, WRITE_OWNER, file, &retired);
        if (exp == nullptr) {
            s_ipc_lock.unlock_irqrestore(irqf);
            free_export_pipe_write_backlog(backlog);
            free_retired_exports(retired);
            ipc_release_file_ref(file);
            continue;
        }

        if (WRITE_RET > 0) {
            auto const ADVANCED = static_cast<uint16_t>(std::cmp_less(WRITE_RET, remaining) ? WRITE_RET : remaining);
            if (ADVANCED <= UINT64_MAX - exp->pipe_bytes_delivered) exp->pipe_bytes_delivered += ADVANCED;
            {
                auto& chunk = backlog->chunks.front();
                chunk.offset = static_cast<uint16_t>(chunk.offset + ADVANCED);
                backlog->buffered_bytes -= ADVANCED;
                if (chunk.offset == chunk.len && chunk.flow_credit == 0) {
                    release_pending_pipe_chunk(chunk);
                    backlog->chunks.pop_front();
                }
            }

            uint32_t next_tail = 0;
            uint16_t next_len = 0;
            uint8_t* next_data = nullptr;
            bool const HAS_RDMA = export_pipe_rdma_front_locked(exp, &next_tail, &next_len, &next_data);
            if (!backlog->chunks.empty() || HAS_RDMA || backlog->close_pending) {
                queue_export_pipe_write_flush_locked(backlog);
            } else if (!backlog->persistent_for_rdma) {
                erase_export_pipe_write_backlog_locked(backlog);
            }
            bool const FREE_BACKLOG = !backlog->persistent_for_rdma && backlog->chunks.empty() && !backlog->close_pending;
            s_ipc_lock.unlock_irqrestore(irqf);
            ipc_release_file_ref(file);
            if (FREE_BACKLOG) {
                free_export_pipe_write_backlog(backlog);
            }
            continue;
        }

        if (WRITE_RET == -EAGAIN || WRITE_RET == -EINTR) {
            queue_export_pipe_write_flush_locked(backlog);
            s_ipc_lock.unlock_irqrestore(irqf);
            if (WRITE_RET == -EINTR) {
                ipc_release_file_ref(file);
                ker::mod::sched::kern_yield();
                continue;
            }
            bool ready_now = false;
            bool const POLL_REGISTERED = register_poll_write_waiter(file, &ready_now);
            ipc_release_file_ref(file);
            if (POLL_REGISTERED && !ready_now) {
                ker::mod::sched::kern_block();
            } else if (!POLL_REGISTERED) {
                ker::mod::sched::kern_sleep_us(WKI_IPC_PIPE_WRITE_RETRY_US);
            } else {
                ker::mod::sched::kern_yield();
            }
            continue;
        }

        ker::mod::dbg::log("[WKI] IPC export pipe write failed: resource_id=%u ret=%ld remaining=%u", resource_id, WRITE_RET, remaining);
        if (exp->message_write_flow_control) {
            backlog->write_error = WRITE_RET != 0 ? static_cast<int32_t>(WRITE_RET) : -EIO;
            queue_export_pipe_write_flush_locked(backlog);
            s_ipc_lock.unlock_irqrestore(irqf);
            ipc_release_file_ref(file);
            continue;
        }
        FailedExportPipeWriteRetirement const RETIREMENT = retire_failed_export_pipe_write_locked(backlog, exp, file);
        s_ipc_lock.unlock_irqrestore(irqf);
        // Drop the worker's temporary reference and the distinct reference
        // formerly owned by the export. Releasing only one leaves a pipe end
        // alive with no fd/export owner and suppresses EOF indefinitely.
        ipc_release_file_ref(file);
        ipc_release_file_ref(RETIREMENT.export_file);
        free_export_pipe_write_backlog(backlog);
        free_retired_exports(RETIREMENT.retired_exports);
    }
}

[[noreturn]] void export_pipe_write_flush_thread_0() {
    export_pipe_write_flush_thread_loop(0);
    ker::mod::sched::exit_current_kernel_thread();
}

[[noreturn]] void export_pipe_write_flush_thread_1() {
    export_pipe_write_flush_thread_loop(1);
    ker::mod::sched::exit_current_kernel_thread();
}

[[noreturn]] void export_pipe_write_flush_thread_2() {
    export_pipe_write_flush_thread_loop(2);
    ker::mod::sched::exit_current_kernel_thread();
}

[[noreturn]] void export_pipe_write_flush_thread_3() {
    export_pipe_write_flush_thread_loop(3);
    ker::mod::sched::exit_current_kernel_thread();
}

auto queue_pending_pipe_data(uint16_t home_node, uint32_t resource_id, const uint8_t* data, uint16_t len,
                             IpcDevOpWork** work_owner = nullptr) -> bool {
    if (data == nullptr || len == 0) {
        return true;
    }

    IpcDevOpWork* const TRANSFER_OWNER =
        ipc_dev_op_work_can_back_pipe_chunk(work_owner != nullptr ? *work_owner : nullptr, data, len) ? *work_owner : nullptr;
    uint8_t* copy = nullptr;
    if (TRANSFER_OWNER == nullptr) {
        copy = new (std::nothrow) uint8_t[len];
        if (copy == nullptr) {
            return false;
        }
        std::memcpy(copy, data, len);
    }

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    if (proxy_endpoint_rejects_received_data_locked(home_node, resource_id)) {
        s_ipc_lock.unlock_irqrestore(IRQF);
        delete[] copy;
        return true;
    }
    auto* pending = find_pending_pipe_delivery_locked(home_node, resource_id);
    if (pending == nullptr) {
        pending = new PendingPipeDelivery();
        if (pending == nullptr) {
            s_ipc_lock.unlock_irqrestore(IRQF);
            delete[] copy;
            return false;
        }
        pending->home_node = home_node;
        pending->resource_id = resource_id;
        g_pending_pipe_deliveries.push_back(pending);
    }

    pending->chunks.push_back(
        PendingPipeChunk{.data = TRANSFER_OWNER != nullptr ? data : copy, .len = len, .dev_op_owner = TRANSFER_OWNER});
    if (TRANSFER_OWNER != nullptr) {
        *work_owner = nullptr;
    }
    pending->buffered_bytes += len;
    s_ipc_lock.unlock_irqrestore(IRQF);
    return true;
}

auto mark_pending_pipe_write_closed(uint16_t home_node, uint32_t resource_id, IpcDevOpWork** work_owner = nullptr) -> bool {
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    if (proxy_endpoint_rejects_received_data_locked(home_node, resource_id)) {
        s_ipc_lock.unlock_irqrestore(IRQF);
        return true;
    }
    auto* pending = find_pending_pipe_delivery_locked(home_node, resource_id);
    if (pending == nullptr) {
        pending = new (std::nothrow) PendingPipeDelivery();
        if (pending == nullptr) {
            s_ipc_lock.unlock_irqrestore(IRQF);
            return false;
        }
        pending->home_node = home_node;
        pending->resource_id = resource_id;
        g_pending_pipe_deliveries.push_back(pending);
    }

    if (!pending->write_closed && work_owner != nullptr && *work_owner != nullptr && (*work_owner)->fixed_pool) {
        pending->close_owner = *work_owner;
        *work_owner = nullptr;
    }
    pending->write_closed = true;
    s_ipc_lock.unlock_irqrestore(IRQF);
    return true;
}

auto consume_pending_pipe_closed(uint16_t home_node, uint32_t resource_id) -> bool {
    PendingPipeDelivery* closed_pending = nullptr;
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    auto* pending = find_pending_pipe_delivery_locked(home_node, resource_id);
    if (pending != nullptr && pending->write_closed && pending->chunks.empty()) {
        erase_pending_pipe_delivery_locked(pending);
        closed_pending = pending;
    }
    s_ipc_lock.unlock_irqrestore(IRQF);

    if (closed_pending == nullptr) {
        return false;
    }

    free_pending_pipe_delivery(closed_pending);
    return true;
}

void proxy_mark_pipe_closed(ProxyIpcState* proxy, uint32_t resource_id) {
    if (proxy == nullptr) {
        return;
    }

    (void)resource_id;

    auto* reader = static_cast<ker::mod::sched::task::Task*>(nullptr);
    uint64_t const PROXY_IRQF = proxy->lock.lock_irqsave();
    proxy->write_closed.store(1U, std::memory_order_release);
    reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
    proxy->lock.unlock_irqrestore(PROXY_IRQF);

#ifdef WKI_IPC_DEBUG
    ker::mod::dbg::log("[WKI] IPC proxy EOF received: resource_id=%u reader_pid=0x%lx", resource_id, reader != nullptr ? reader->pid : 0UL);
#endif

    wake_blocked_ipc_reader(reader);
    proxy_wake_stream_readers(proxy);

    wki_ipc_proxy_wake_poll_waiters(proxy);
}

auto mark_ipc_pty_write_closed_or_pending(uint16_t peer_node, uint32_t resource_id, uint64_t expected_bytes = 0,
                                          bool has_expected_bytes = false, IpcDevOpWork** work_owner = nullptr) -> bool {
    ProxyIpcState* proxy = nullptr;
    bool export_exists = false;
    bool denied = false;

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    if (peer_node != WKI_NODE_INVALID) {
        proxy = find_proxy_by_endpoint_locked(peer_node, resource_id);
    }
    if (proxy == nullptr) {
        auto* exp = find_export_by_resource_id(resource_id);
        export_exists = ipc_export_matches_consumer(exp, resource_id, peer_node, ResourceType::IPC_PTY) && exp->file != nullptr;
        denied = exp != nullptr && !export_exists;
    }
    s_ipc_lock.unlock_irqrestore(IRQF);

    if (proxy != nullptr && proxy->res_type != ResourceType::IPC_PTY) {
        proxy_release(proxy);
        return true;
    }
    if (denied) return true;  // Consumed refusal; no pending state or retry.
    if (proxy != nullptr) {
        proxy_mark_pipe_closed(proxy, resource_id);
        proxy_release(proxy);
        return true;
    }

    if (export_exists) {
        return mark_export_pipe_write_closed(resource_id, peer_node, expected_bytes, has_expected_bytes, ResourceType::IPC_PTY);
    }

    if (peer_node == WKI_NODE_INVALID) {
        return false;
    }
    return mark_pending_pipe_write_closed(peer_node, resource_id, work_owner);
}

void wake_proxy_reader(ProxyIpcState* proxy) {
    if (proxy == nullptr) {
        return;
    }

    auto* reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
    wake_blocked_ipc_reader(reader);
    proxy_wake_stream_readers(proxy);
}

auto proxy_pipe_read(ker::vfs::File* f, void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    auto* proxy = f != nullptr ? static_cast<ProxyIpcState*>(f->private_data) : nullptr;
    IpcPerfTrace trace(ker::mod::perf::WkiPerfIpcOp::PROXY_READ, proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_RESOURCE,
                       WOS_PERF_CALLSITE());
    auto finish = [&](ssize_t rc) -> ssize_t {
        trace.finish(rc >= 0 ? 0 : static_cast<int32_t>(rc), rc > 0 ? static_cast<uint64_t>(rc) : 0);
        return rc;
    };
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) return finish(-EBADF);
    if (count == 0) return finish(0);
    if (buf == nullptr) return finish(-EINVAL);
    if (proxy->ring_buf == nullptr || proxy->ring_capacity == 0) return finish(-EIO);
    // Bound the IRQ-disabled copy, including large userspace reads.
    count = std::min(count, WKI_IPC_PIPE_DATA_MAX_CHUNK);
    bool const NONBLOCKING = (f->open_flags & WKI_IPC_O_NONBLOCK) != 0;
    auto* task = ker::mod::sched::get_current_task();

    for (;;) {
        PendingPipeChunk chunk{};
        PendingPipeDelivery* retired_pending = nullptr;
        ssize_t result = -EAGAIN;
        bool registered = false;
        bool promoted_close = false;
        // This single snapshot orders ring consumption, overflow consumption,
        // EOF and wait registration. Concurrent readers cannot publish the
        // same tail, or consume pending bytes ahead of older ring bytes.
        uint64_t const PENDING_IRQF = s_ipc_lock.lock_irqsave();
        uint64_t const IRQF = proxy->lock.lock_irqsave();
        uint32_t const HEAD = proxy->ring_head.load(std::memory_order_acquire);
        uint32_t const TAIL = proxy->ring_tail.load(std::memory_order_relaxed);
        auto* pending = find_pending_pipe_delivery_locked(proxy->home_node, proxy->resource_id);
        if (!proxy->active.load(std::memory_order_acquire)) {
            result = -EBADF;
        } else if (HEAD != TAIL) {
            size_t const TO_READ = std::min<size_t>(count, HEAD - TAIL);
            size_t const RING_TAIL = TAIL % proxy->ring_capacity;
            size_t const FIRST = std::min<size_t>(TO_READ, proxy->ring_capacity - RING_TAIL);
            std::memcpy(buf, proxy->ring_buf + RING_TAIL, FIRST);
            std::memcpy(static_cast<uint8_t*>(buf) + FIRST, proxy->ring_buf, TO_READ - FIRST);
            proxy->ring_tail.store(TAIL + static_cast<uint32_t>(TO_READ), std::memory_order_release);
            result = static_cast<ssize_t>(TO_READ);
        } else if (pending != nullptr && !pending->chunks.empty()) {
            auto& front = pending->chunks.front();
            size_t const TO_READ = std::min<size_t>(count, front.len - front.offset);
            std::memcpy(buf, front.data + front.offset, TO_READ);
            front.offset = static_cast<uint16_t>(front.offset + TO_READ);
            pending->buffered_bytes -= static_cast<uint32_t>(TO_READ);
            if (front.offset == front.len) {
                chunk = front;
                pending->chunks.pop_front();
            }
            if (pending->chunks.empty() && !pending->write_closed) {
                erase_pending_pipe_delivery_locked(pending);
                retired_pending = pending;
            }
            result = static_cast<ssize_t>(TO_READ);
        } else if ((pending != nullptr && pending->write_closed) || proxy->write_closed.load(std::memory_order_acquire) != 0U) {
            if (pending != nullptr) {
                erase_pending_pipe_delivery_locked(pending);
                retired_pending = pending;
            }
            promoted_close = proxy->write_closed.exchange(1U, std::memory_order_acq_rel) == 0U;
            result = 0;
        } else if (!NONBLOCKING) {
            if (current_task_has_deliverable_signal()) {
                result = -EINTR;
            } else if (task == nullptr) {
                result = -ESRCH;
            } else {
                registered = proxy_register_reader_locked(proxy, task->pid);
                if (registered) task->set_wait_channel("wki_proxy_stream");
            }
        }
        proxy->lock.unlock_irqrestore(IRQF);
        s_ipc_lock.unlock_irqrestore(PENDING_IRQF);
        release_pending_pipe_chunk(chunk);
        free_pending_pipe_delivery(retired_pending);
        if (promoted_close) {
            wake_proxy_reader(proxy);
            wki_ipc_proxy_wake_poll_waiters(proxy);
        }
        if (result != -EAGAIN || NONBLOCKING) return finish(result);
        if (!registered) {
            ker::mod::sched::kern_sleep_us(WKI_IPC_PIPE_WRITE_RETRY_US);
            continue;
        }
        ker::mod::sched::preemptible_syscall_park("wki_proxy_stream");
        uint64_t const REMOVE_IRQF = proxy->lock.lock_irqsave();
        proxy_remove_reader_locked(proxy, task->pid);
        proxy->lock.unlock_irqrestore(REMOVE_IRQF);
        if (current_task_has_deliverable_signal()) return finish(-EINTR);
    }
}

auto acquire_proxy_pipe_rdma_writer(ProxyIpcState* proxy, bool nonblocking) -> bool {
    if (proxy == nullptr) {
        return false;
    }

    uint32_t spins = 0;
    while (proxy->active.load(std::memory_order_acquire)) {
        bool expected = false;
        if (proxy->pipe_rdma_writer_active.compare_exchange_weak(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return true;
        }
        if (nonblocking || current_task_has_deliverable_signal()) {
            return false;
        }
        if ((++spins & 0x3FU) == 0) {
            ker::mod::sched::kern_yield();
        } else {
            asm volatile("pause" ::: "memory");
        }
    }
    return false;
}

void release_proxy_pipe_rdma_writer(ProxyIpcState* proxy) {
    if (proxy != nullptr) {
        proxy->pipe_rdma_writer_active.store(false, std::memory_order_release);
    }
}

auto proxy_pipe_write_rdma(ProxyIpcState* proxy, const uint8_t* src, size_t count, uint64_t callsite, bool nonblocking) -> ssize_t {
    if (proxy == nullptr || src == nullptr || count == 0 || !proxy->pipe_rdma_enabled || proxy->pipe_rdma_rkey == 0 ||
        proxy->pipe_rdma_capacity == 0)
        return -EOPNOTSUPP;
    if (proxy->pipe_secure_uncertain.load(std::memory_order_acquire)) return -EIO;
    rdma_protocol::RegionDescriptor grant{};
    if (!wki_secure_rdma_remote_grant(proxy->data_channel, rdma_protocol::Service::IPC, proxy->pipe_rdma_rkey, grant,
                                      rdma_protocol::PERMIT_READ | rdma_protocol::PERMIT_WRITE) ||
        grant.size != proxy->pipe_rdma_capacity || grant.binding_generation == 0)
        return -EOPNOTSUPP;
    size_t sent = 0;
    while (sent < count && proxy->active.load(std::memory_order_acquire)) {
        IpcSecureIntent query{.kind = IPC_SECURE_QUERY, .resource_id = proxy->resource_id, .lifetime = grant.binding_generation};
        IpcSecureSnapshot snapshot{};
        auto const OBSERVED = wki_secure_rdma_read(proxy->data_channel, grant, 0, {reinterpret_cast<uint8_t*>(&snapshot), sizeof(snapshot)},
                                                   {reinterpret_cast<const uint8_t*>(&query), sizeof(query)});
        if (OBSERVED.outcome != WkiSecureRdmaOutcome::EXACT || OBSERVED.status != 0 || OBSERVED.bytes != sizeof(snapshot))
            return sent != 0 ? static_cast<ssize_t>(sent) : -EOPNOTSUPP;
        if (snapshot.version != IPC_SECURE_VERSION || snapshot.reserved != 0 || snapshot.reserved2 != 0 ||
            snapshot.resource_id != proxy->resource_id || snapshot.lifetime != grant.binding_generation || snapshot.credit > grant.size ||
            snapshot.head < snapshot.tail || snapshot.head - snapshot.tail > grant.size || snapshot.flags != 0)
            return sent != 0 ? static_cast<ssize_t>(sent) : -EIO;
        if (snapshot.status != 0) return sent != 0 ? static_cast<ssize_t>(sent) : snapshot.status < 0 ? snapshot.status : -EIO;
        if (snapshot.credit == 0) return sent != 0 ? static_cast<ssize_t>(sent) : -EOPNOTSUPP;
        uint64_t const OFFSET = snapshot.head % grant.size;
        uint32_t const LENGTH = static_cast<uint32_t>(std::min<size_t>({count - sent, snapshot.credit, grant.size - OFFSET, UINT16_MAX}));
        IpcSecureIntent intent{.kind = IPC_SECURE_APPEND,
                               .resource_id = proxy->resource_id,
                               .lifetime = grant.binding_generation,
                               .head = snapshot.head,
                               .tail = snapshot.tail,
                               .credit = snapshot.credit};
        auto const RESULT = wki_secure_rdma_write(proxy->data_channel, grant, OFFSET, {src + sent, LENGTH}, 0,
                                                  {reinterpret_cast<const uint8_t*>(&intent), sizeof(intent)});
        if (RESULT.outcome == WkiSecureRdmaOutcome::NO_EFFECT) {
            if (RESULT.status == -EPIPE) proxy->message_write_error.store(-EPIPE, std::memory_order_release);
            return sent != 0 ? static_cast<ssize_t>(sent) : RESULT.status == -EPIPE ? -EPIPE : -EOPNOTSUPP;
        }
        if (RESULT.outcome != WkiSecureRdmaOutcome::EXACT || RESULT.status != 0 || RESULT.bytes == 0 || RESULT.bytes > LENGTH) {
            proxy->pipe_secure_uncertain.store(true, std::memory_order_release);
            proxy->message_write_error.store(-EIO, std::memory_order_release);
            proxy_wake_message_write_waiters(proxy);
            wki_ipc_proxy_wake_poll_waiters(proxy);
            return sent != 0 ? static_cast<ssize_t>(sent) : -EIO;
        }
        proxy->pipe_rdma_head = snapshot.head + RESULT.bytes;
        proxy->pipe_rdma_tail_cache = proxy->pipe_rdma_head;
        proxy->bytes_written.fetch_add(RESULT.bytes, std::memory_order_acq_rel);
        // Only the authenticated native prefix receipt certifies delivery.
        proxy->bytes_delivered.fetch_add(RESULT.bytes, std::memory_order_release);
        sent += RESULT.bytes;
        g_proxy_write_payload_bytes.fetch_add(RESULT.bytes, std::memory_order_relaxed);
        perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::PROXY_WRITE, proxy->home_node, WKI_CHAN_IPC_DATA, 0, 0, RESULT.bytes, callsite);
        if (nonblocking || current_task_has_deliverable_signal()) break;
    }
    return sent != 0 ? static_cast<ssize_t>(sent) : -EIO;
}

struct ProxyPipeWriterLoan {
    ProxyIpcState* proxy = nullptr;
    ~ProxyPipeWriterLoan() { release_proxy_pipe_rdma_writer(proxy); }
};

auto proxy_pipe_write(ker::vfs::File* f, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    IpcPerfTrace trace(ker::mod::perf::WkiPerfIpcOp::PROXY_WRITE, proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_IPC_DATA,
                       WOS_PERF_CALLSITE());
    auto finish = [&](ssize_t rc, uint64_t bytes = 0) -> ssize_t {
        int32_t const STATUS = rc >= 0 ? 0 : static_cast<int32_t>(rc);
        trace.finish(STATUS, bytes);
        return rc;
    };
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
        return finish(-EBADF);
    }
    if (buf == nullptr && count != 0) {
        return finish(-EINVAL);
    }
    if (count == 0) {
        return finish(0);
    }

    bool const NONBLOCKING = (f->open_flags & WKI_IPC_O_NONBLOCK) != 0;
    ProxyPipeWriterLoan writer;
    if (proxy->pipe_rdma_enabled) {
        if (!acquire_proxy_pipe_rdma_writer(proxy, NONBLOCKING))
            return finish(!proxy->active.load(std::memory_order_acquire) ? -EIO : NONBLOCKING ? -EAGAIN : -EINTR);
        writer.proxy = proxy;
        auto const RDMA_RET = proxy_pipe_write_rdma(proxy, static_cast<const uint8_t*>(buf), count, WOS_PERF_CALLSITE(), NONBLOCKING);
        if (RDMA_RET != -EOPNOTSUPP) {
            return finish(RDMA_RET, RDMA_RET > 0 ? static_cast<uint64_t>(RDMA_RET) : 0);
        }
    }

    // Send data via wire message to home node
    // Message format: [DevOpReqPayload(4B)] [resource_id:u32] [data...]
    constexpr size_t HEADER_SIZE = WKI_IPC_PIPE_DATA_HEADER_SIZE;
    constexpr size_t MAX_CHUNK = WKI_IPC_PIPE_DATA_MAX_CHUNK;
    // Every byte in the transmitted prefix is assigned below; do not
    // pattern-initialize the unused bounded tail.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
    std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> msg __attribute__((uninitialized));

    size_t sent = 0;
    const auto* src = static_cast<const uint8_t*>(buf);
    auto* req = reinterpret_cast<DevOpReqPayload*>(msg.data());
    bool const MESSAGE_FLOW_CONTROL = proxy->message_write_flow_control;
    req->op_id = MESSAGE_FLOW_CONTROL ? OP_PIPE_DATA_FLOW : OP_PIPE_DATA;
    std::memcpy(msg.data() + sizeof(DevOpReqPayload), &proxy->resource_id, sizeof(uint32_t));

    uint64_t const SEND_STARTED_US = wki_now_us();
    uint64_t last_send_diag_us = 0;
    while (sent < count) {
        size_t to_send = std::min(count - sent, MAX_CHUNK);
        uint32_t reserved_credits = 0;
        if (MESSAGE_FLOW_CONTROL) {
            ssize_t const RESERVED = proxy_reserve_message_write_credits(proxy, to_send, NONBLOCKING);
            if (RESERVED <= 0) {
                return finish(sent != 0 ? static_cast<ssize_t>(sent) : RESERVED, sent);
            }
            reserved_credits = static_cast<uint32_t>(RESERVED);
            to_send = reserved_credits;
        }

        auto refund_reserved_credits = [&]() {
            if (reserved_credits == 0) {
                return;
            }
            proxy_grant_message_write_credits(proxy, reserved_credits);
            reserved_credits = 0;
            proxy_wake_message_write_waiters(proxy);
        };

        req->data_len = static_cast<uint16_t>(sizeof(uint32_t) + to_send);
        std::memcpy(msg.data() + HEADER_SIZE, src + sent, to_send);

        int ret = WKI_ERR_TX_FAILED;
        uint32_t attempts = 0;
        while (proxy->active.load(std::memory_order_acquire)) {
            int32_t const WRITE_ERROR = proxy->message_write_error.load(std::memory_order_acquire);
            if (WRITE_ERROR != 0) {
                refund_reserved_credits();
                return finish(sent != 0 ? static_cast<ssize_t>(sent) : static_cast<ssize_t>(WRITE_ERROR), sent);
            }
            ret = wki_send_accounted(proxy->data_channel, MsgType::DEV_OP_REQ, msg.data(), static_cast<uint16_t>(HEADER_SIZE + to_send),
                                     proxy->bytes_written, static_cast<uint16_t>(to_send));
            if (ret == WKI_OK) {
                break;
            }
            if (ret == WKI_ERR_NOT_FOUND) break;
            if (NONBLOCKING) {
                refund_reserved_credits();
                return finish(sent != 0 ? static_cast<ssize_t>(sent) : ipc_pipe_nonblocking_send_status(ret), sent);
            }
            if (current_task_has_deliverable_signal()) {
                refund_reserved_credits();
                return finish(sent != 0 ? static_cast<ssize_t>(sent) : static_cast<ssize_t>(-EINTR), sent);
            }
            uint32_t const ATTEMPT = attempts++;
            if (ret == WKI_ERR_NO_CREDITS) {
                g_proxy_write_no_credit_waits.fetch_add(1, std::memory_order_relaxed);
            }
            if (ret != WKI_ERR_NO_CREDITS || ATTEMPT != 0) {
                g_proxy_write_block_us.fetch_add(ipc_pipe_send_retry_sleep_us(ret, ATTEMPT), std::memory_order_relaxed);
            }
            if (ipc_diag_due(SEND_STARTED_US, last_send_diag_us)) {
                log_ipc_send_retry_diag("proxy_write_wait", proxy, proxy->resource_id, count, sent, ret, attempts, SEND_STARTED_US);
            }
            pause_for_ipc_send_retry(ret, ATTEMPT);
        }

        if (ret != WKI_OK) {
            refund_reserved_credits();
            return finish(sent != 0 ? static_cast<ssize_t>(sent) : static_cast<ssize_t>(-EIO), sent);
        }

        reserved_credits = 0;
        sent += to_send;
        g_proxy_write_payload_bytes.fetch_add(to_send, std::memory_order_relaxed);
    }

    return finish(static_cast<ssize_t>(sent), sent);
}

auto send_proxy_pipe_close(ProxyIpcState* proxy, const uint8_t* msg, uint16_t msg_size, uint32_t resource_id, uint16_t op_id) -> void {
    if (proxy == nullptr || msg == nullptr || msg_size == 0 || proxy->home_node == WKI_NODE_INVALID) {
        return;
    }

    bool const CAN_BLOCK = current_task_can_block_for_ipc_close();
    uint32_t const MAX_ATTEMPTS = CAN_BLOCK ? WKI_IPC_PIPE_EOF_MAX_SEND_ATTEMPTS : 1U;
    int ret = WKI_ERR_TX_FAILED;
    uint32_t attempts = 0;
    uint64_t const SEND_STARTED_US = wki_now_us();
    uint64_t last_send_diag_us = 0;
    while (attempts < MAX_ATTEMPTS) {
        ret = wki_send_on_channel_identity(proxy->data_channel, MsgType::DEV_OP_REQ, msg, msg_size);
        if (ret == WKI_OK || proxy_pipe_close_tx_terminal(ret)) {
            return;
        }
        ++attempts;
        if (!CAN_BLOCK || attempts >= MAX_ATTEMPTS) {
            break;
        }
        if (ipc_diag_due(SEND_STARTED_US, last_send_diag_us)) {
            log_ipc_send_retry_diag("proxy_close_wait", proxy, resource_id, msg_size, 0, ret, attempts, SEND_STARTED_US);
        }
        pause_for_ipc_send_retry(ret, attempts);
    }

    if (enqueue_proxy_pipe_close_tx(proxy->data_channel, msg, msg_size, resource_id, op_id)) {
        return;
    }

    ker::mod::dbg::log("[WKI] IPC proxy pipe close enqueue failed: resource_id=%u op=%u ret=%d attempts=%u can_block=%u", resource_id,
                       op_id, ret, attempts, CAN_BLOCK ? 1U : 0U);
}

auto proxy_pipe_close(ker::vfs::File* f) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr) {
        return 0;
    }

    // Send close to home node with resource_id
    uint16_t const OP = (f->fops != nullptr && f->fops->vfs_write == nullptr)
                            ? OP_PIPE_CLOSE_READ
                            : (proxy->message_write_flow_control ? OP_PIPE_CLOSE_WRITE_FLOW : OP_PIPE_CLOSE_WRITE);

    constexpr size_t HEADER_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    constexpr size_t MAX_CLOSE_SIZE = HEADER_SIZE + sizeof(uint64_t);
    std::array<uint8_t, MAX_CLOSE_SIZE> msg = {};
    auto* req = reinterpret_cast<DevOpReqPayload*>(msg.data());
    req->op_id = OP;
    req->data_len = sizeof(uint32_t);
    std::memcpy(msg.data() + sizeof(DevOpReqPayload), &proxy->resource_id, sizeof(uint32_t));
    uint16_t msg_size = HEADER_SIZE;
    if (OP == OP_PIPE_CLOSE_WRITE || OP == OP_PIPE_CLOSE_WRITE_FLOW) {
        uint64_t const EXPECTED_BYTES = proxy->bytes_written.load(std::memory_order_acquire);
        req->data_len = static_cast<uint16_t>(req->data_len + sizeof(EXPECTED_BYTES));
        std::memcpy(msg.data() + HEADER_SIZE, &EXPECTED_BYTES, sizeof(EXPECTED_BYTES));
        msg_size = static_cast<uint16_t>(msg_size + sizeof(EXPECTED_BYTES));
    }

    send_proxy_pipe_close(proxy, msg.data(), msg_size, proxy->resource_id, OP);

    wki_ipc_detach_proxy_file(f, proxy);
    return 0;
}

auto proxy_poll_has_read_side(ker::vfs::File* f, ProxyIpcState* proxy) -> bool {
    if (proxy != nullptr && (proxy->res_type == ResourceType::IPC_PTY || proxy->res_type == ResourceType::IPC_SOCKET)) {
        return true;
    }
    return f != nullptr && f->fops != nullptr && f->fops->vfs_write == nullptr;
}

auto proxy_poll_has_write_side(ker::vfs::File* f, ProxyIpcState* proxy) -> bool {
    if (proxy != nullptr && (proxy->res_type == ResourceType::IPC_PTY || proxy->res_type == ResourceType::IPC_SOCKET)) {
        return true;
    }
    return f == nullptr || f->fops == nullptr || f->fops->vfs_write != nullptr;
}

auto proxy_pipe_poll_check(ker::vfs::File* f, int events) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr) {
        return 0;
    }
    bool const HAS_READ_SIDE = proxy_poll_has_read_side(f, proxy);
    bool const HAS_WRITE_SIDE = proxy_poll_has_write_side(f, proxy);
    if (!proxy->active.load(std::memory_order_acquire)) {
        int ready = 0;
        if (HAS_READ_SIDE) {
            ready |= dev::pty::POLLHUP;
        }
        if (HAS_WRITE_SIDE) {
            ready |= dev::pty::POLLERR;
        }
        return ready;
    }

    int ready = 0;
    if (HAS_READ_SIDE) {
        PendingPipeDelivery* retired_pending = nullptr;
        uint64_t const PENDING_IRQF = s_ipc_lock.lock_irqsave();
        uint64_t const IRQF = proxy->lock.lock_irqsave();
        uint32_t const HEAD = proxy->ring_head.load(std::memory_order_acquire);
        uint32_t const TAIL = proxy->ring_tail.load(std::memory_order_acquire);
        uint32_t const AVAIL = HEAD - TAIL;
        auto* pending = find_pending_pipe_delivery_locked(proxy->home_node, proxy->resource_id);
        bool const HAS_PENDING_DATA = pending != nullptr && !pending->chunks.empty();
        bool const PROMOTE_CLOSE = pending != nullptr && pending->write_closed && !HAS_PENDING_DATA;
        if (PROMOTE_CLOSE) {
            proxy->write_closed.store(1U, std::memory_order_release);
            erase_pending_pipe_delivery_locked(pending);
            retired_pending = pending;
        }
        bool const WR_CLOSED = proxy->write_closed.load(std::memory_order_acquire) != 0U;
        proxy->lock.unlock_irqrestore(IRQF);
        s_ipc_lock.unlock_irqrestore(PENDING_IRQF);
        free_pending_pipe_delivery(retired_pending);
        if (PROMOTE_CLOSE) {
            wake_proxy_reader(proxy);
            wki_ipc_proxy_wake_poll_waiters(proxy);
        }

        // Read end
        if (((events & dev::pty::POLLIN) != 0) && (AVAIL > 0 || HAS_PENDING_DATA || WR_CLOSED)) {
            ready |= dev::pty::POLLIN;
        }
        if (WR_CLOSED && AVAIL == 0 && !HAS_PENDING_DATA) {
            ready |= dev::pty::POLLHUP;
        }
    }
    if (HAS_WRITE_SIDE) {
        int32_t const WRITE_ERROR = proxy->message_write_error.load(std::memory_order_acquire);
        if (WRITE_ERROR != 0) {
            ready |= dev::pty::POLLERR;
        }
        bool const HAS_CREDIT = !proxy->message_write_flow_control || proxy->message_write_credits.load(std::memory_order_acquire) != 0;
        if (((events & dev::pty::POLLOUT) != 0) && WRITE_ERROR == 0 && HAS_CREDIT) {
            ready |= dev::pty::POLLOUT;
        }
    }
    return ready;
}

auto proxy_pipe_poll_register_waiter(ker::vfs::File* f, uint64_t pid) -> bool {
    auto* proxy = (f != nullptr) ? static_cast<ProxyIpcState*>(f->private_data) : nullptr;
    return wki_ipc_proxy_register_poll_waiter(proxy, pid);
}

ker::vfs::FileOperations g_proxy_pipe_read_fops = {
    .vfs_open = nullptr,
    .vfs_close = proxy_pipe_close,
    .vfs_read = proxy_pipe_read,
    .vfs_write = nullptr,
    .vfs_lseek = nullptr,
    .vfs_isatty = nullptr,
    .vfs_readdir = nullptr,
    .vfs_readlink = nullptr,
    .vfs_truncate = nullptr,
    .vfs_poll_check = proxy_pipe_poll_check,
    .vfs_poll_register_waiter = proxy_pipe_poll_register_waiter,
    .vfs_ioctl = nullptr,
};

ker::vfs::FileOperations g_proxy_pipe_write_fops = {
    .vfs_open = nullptr,
    .vfs_close = proxy_pipe_close,
    .vfs_read = nullptr,
    .vfs_write = proxy_pipe_write,
    .vfs_lseek = nullptr,
    .vfs_isatty = nullptr,
    .vfs_readdir = nullptr,
    .vfs_readlink = nullptr,
    .vfs_truncate = nullptr,
    .vfs_poll_check = proxy_pipe_poll_check,
    .vfs_poll_register_waiter = proxy_pipe_poll_register_waiter,
    .vfs_ioctl = nullptr,
};

// ---- Server-side helpers ----

// Keep this classification local to IPC forwarding: general VFS pipe helpers
// also feed native pipe casts/capacity operations, which cannot accept a proxy.
auto is_proxy_pipe_file(const ker::vfs::File* file) -> bool {
    return file != nullptr && (file->fops == &g_proxy_pipe_read_fops || file->fops == &g_proxy_pipe_write_fops);
}

// ---- Proxy fops for epoll (IPC_EPOLL) ----
//
// The EpollInstance MUST be the first member of ProxyEpollFile so that
// static_cast<EpollInstance*>(file->private_data) works in kernel epoll_ctl /
// epoll_pwait (standard-layout guarantee: address of first member == address of
// the enclosing struct).

struct ProxyEpollFile {
    ker::vfs::EpollInstance inst{};  // MUST be first — addr(inst) == addr(ProxyEpollFile)
    uint32_t resource_id = 0;
    uint16_t home_node = WKI_NODE_INVALID;
};

static_assert(__builtin_offsetof(ProxyEpollFile, inst) == 0, "EpollInstance must be at offset 0 in ProxyEpollFile");

auto proxy_epoll_close(ker::vfs::File* f) -> int {
    auto* epf = reinterpret_cast<ProxyEpollFile*>(f->private_data);
    if (epf == nullptr) {
        return 0;
    }

    uint32_t resource_id = epf->resource_id;
    uint16_t const HOME_NODE = epf->home_node;
    f->private_data = nullptr;
    delete epf;

    ProxyIpcState* proxy = nullptr;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy = find_proxy_by_endpoint_locked(HOME_NODE, resource_id);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
        if (proxy != nullptr) {
            proxy_release(proxy);
        }
        return 0;
    }

    // Send OP_PIPE_CLOSE_READ to home node — the server handler tears down the export
    // (sets exp->active = false, releases file ref) for any resource type.
    constexpr size_t CLOSE_MSG_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    std::array<uint8_t, CLOSE_MSG_SIZE> close_msg = {};
    auto* req = reinterpret_cast<DevOpReqPayload*>(close_msg.data());
    req->op_id = OP_PIPE_CLOSE_READ;
    req->data_len = sizeof(uint32_t);
    std::memcpy(close_msg.data() + sizeof(DevOpReqPayload), &resource_id, sizeof(uint32_t));
    if (HOME_NODE != WKI_NODE_INVALID) {
        wki_ipc_send_control(proxy, close_msg.data(), static_cast<uint16_t>(close_msg.size()));
    }

    wki_ipc_detach_proxy_file(f, proxy);
    proxy_release(proxy);  // release find_proxy_by_endpoint_locked() lookup ref
    return 0;
}

ker::vfs::FileOperations g_proxy_epoll_fops = {
    .vfs_open = nullptr,
    .vfs_close = proxy_epoll_close,
    .vfs_read = nullptr,
    .vfs_write = nullptr,
    .vfs_lseek = nullptr,
    .vfs_isatty = nullptr,
    .vfs_readdir = nullptr,
    .vfs_readlink = nullptr,
    .vfs_truncate = nullptr,
    .vfs_poll_check = nullptr,
    .vfs_poll_register_waiter = nullptr,
    .vfs_ioctl = nullptr,
};

// ---- Proxy fops for PTY (IPC_PTY) ----
//
// Data plane: same ring-buffer / pump path as pipes.
// Control plane: ioctl forwarded to home node via OP_PTY_IOCTL.
// Close: OP_PTY_CLOSE fires and the export tears down.

auto proxy_pty_ioctl(ker::vfs::File* f, unsigned long cmd, unsigned long arg) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    uint32_t const CORRELATION = ker::mod::perf::next_wki_trace_correlation();
    uint64_t const STARTED_US = wki_now_us();
    auto finish = [&](int rc) -> int {
        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
        perf_record_ipc_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::PTY_IOCTL), ker::mod::perf::WkiPerfPhase::END,
                              proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_RESOURCE, CORRELATION, rc, ELAPSED_US,
                              CALLSITE);
        perf_record_ipc_summary(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::PTY_IOCTL),
                                proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_RESOURCE, rc, ELAPSED_US);
        return rc;
    };
    perf_record_ipc_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::PTY_IOCTL), ker::mod::perf::WkiPerfPhase::BEGIN,
                          proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_RESOURCE, CORRELATION, 0, 0, CALLSITE);
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
        return finish(-EBADF);
    }
    if (proxy->home_node == WKI_NODE_INVALID) {
        return finish(-EHOSTUNREACH);
    }
    // This wire version names an exported File, but not an authenticated
    // process/session actor. Never substitute a home worker's job identity.
    if (pty_session_mutation(cmd)) {
        return finish(-ENOTSUP);
    }
    PtyIoctlMarshal const MARSHAL = pty_ioctl_marshalling(cmd, arg);

    // Wire layout (after resource_id): [cookie:u16][cmd:u64][arg_val:u64][arg_in:128B]
    constexpr size_t EXTRA = sizeof(uint64_t) + sizeof(uint64_t) + PTY_IOCTL_ARG_SIZE;
    constexpr size_t MSG_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t) + WKI_IPC_OP_COOKIE_BYTES + EXTRA;
    std::array<uint8_t, MSG_SIZE> msg = {};

    auto* req = reinterpret_cast<DevOpReqPayload*>(msg.data());
    req->op_id = OP_PTY_IOCTL;
    req->data_len = static_cast<uint16_t>(sizeof(uint32_t) + WKI_IPC_OP_COOKIE_BYTES + EXTRA);
    std::memcpy(msg.data() + sizeof(DevOpReqPayload), &proxy->resource_id, sizeof(uint32_t));

    WkiWaitEntry wait = {};
    uint16_t op_cookie = 0;
    uint64_t irqf = proxy->lock.lock_irqsave();
    if (proxy->pending_wait_op != 0) {
        uint16_t const PENDING_OP = proxy->pending_wait_op;
        uint16_t const PENDING_COOKIE = proxy->pending_wait_cookie;
        proxy->lock.unlock_irqrestore(irqf);
        if (wait_diag_enabled()) {
            ker::mod::dbg::logger<"wki">::warn("pty_ioctl_busy peer=0x%04x res=%u cmd=%s(0x%lx) pending_op=0x%x pending_cookie=%u",
                                               static_cast<unsigned>(proxy->home_node), proxy->resource_id, pty_ioctl_cmd_name(cmd), cmd,
                                               static_cast<unsigned>(PENDING_OP), static_cast<unsigned>(PENDING_COOKIE));
        }
        return finish(-EBUSY);
    }
    op_cookie = wki_ipc_allocate_wait_cookie_locked(proxy, OP_PTY_IOCTL);
    if (op_cookie == 0) {
        proxy->lock.unlock_irqrestore(irqf);
        return finish(-EAGAIN);
    }
    wait.diag_name = "pty_ioctl";
    wait.diag_callsite = CALLSITE;
    wait.diag_arg0 = static_cast<uint64_t>(cmd);
    wait.diag_arg1 = static_cast<uint64_t>(arg);
    wait.diag_resource_id = proxy->resource_id;
    wait.diag_op_id = OP_PTY_IOCTL;
    wait.diag_peer = proxy->home_node;
    wait.diag_channel = WKI_CHAN_RESOURCE;
    wait.diag_cookie = op_cookie;
    proxy->pending_wait = &wait;
    proxy->pending_wait_op = OP_PTY_IOCTL;
    proxy->pending_wait_cookie = op_cookie;
    proxy->pending_wait_status = -ETIMEDOUT;
    proxy->pending_wait_resp_len = 0;
    proxy->lock.unlock_irqrestore(irqf);

    size_t cursor = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    std::memcpy(msg.data() + cursor, &op_cookie, WKI_IPC_OP_COOKIE_BYTES);
    cursor += WKI_IPC_OP_COOKIE_BYTES;
    auto cmd64 = static_cast<uint64_t>(cmd);
    auto arg64 = static_cast<uint64_t>(arg);
    std::memcpy(msg.data() + cursor, &cmd64, sizeof(uint64_t));
    cursor += sizeof(uint64_t);
    std::memcpy(msg.data() + cursor, &arg64, sizeof(uint64_t));
    cursor += sizeof(uint64_t);

    if (MARSHAL.pointer_arg && MARSHAL.copy_in && MARSHAL.arg_size > 0) {
        std::memcpy(msg.data() + cursor, reinterpret_cast<const void*>(arg), MARSHAL.arg_size);
    }

    if (wait_diag_enabled()) {
        ker::mod::dbg::logger<"wki">::info("pty_ioctl_send peer=0x%04x res=%u cookie=%u cmd=%s(0x%lx) arg=0x%lx size=%lu in=%u out=%u",
                                           static_cast<unsigned>(proxy->home_node), proxy->resource_id, static_cast<unsigned>(op_cookie),
                                           pty_ioctl_cmd_name(cmd), cmd, arg, static_cast<unsigned long>(MARSHAL.arg_size),
                                           MARSHAL.copy_in ? 1U : 0U, MARSHAL.copy_out ? 1U : 0U);
    }

    int const TX = wki_ipc_send_control(proxy, msg.data(), static_cast<uint16_t>(msg.size()));
    if (TX != WKI_OK) {
        if (wait_diag_enabled()) {
            ker::mod::dbg::logger<"wki">::warn("pty_ioctl_tx_failed peer=0x%04x res=%u cookie=%u cmd=%s(0x%lx) tx=%d",
                                               static_cast<unsigned>(proxy->home_node), proxy->resource_id,
                                               static_cast<unsigned>(op_cookie), pty_ioctl_cmd_name(cmd), cmd, TX);
        }
        wki_ipc_cancel_pending_wait(proxy, &wait, WKI_ERR_TX_FAILED, false);
        return finish(-EIO);
    }

    int const WAIT_RC = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    if (WAIT_RC != 0) {
        if (wait_diag_enabled()) {
            ker::mod::dbg::logger<"wki">::warn("pty_ioctl_wait_failed peer=0x%04x res=%u cookie=%u cmd=%s(0x%lx) wait_rc=%d",
                                               static_cast<unsigned>(proxy->home_node), proxy->resource_id,
                                               static_cast<unsigned>(op_cookie), pty_ioctl_cmd_name(cmd), cmd, WAIT_RC);
        }
        wki_ipc_cancel_pending_wait(proxy, &wait, -ETIMEDOUT);
        return finish(-ETIMEDOUT);
    }

    std::array<uint8_t, PTY_IOCTL_ARG_SIZE> arg_out = {};
    uint16_t resp_len = 0;
    int const STATUS =
        wki_ipc_consume_pending_wait_response(proxy, &wait, OP_PTY_IOCTL, op_cookie, arg_out.data(), PTY_IOCTL_ARG_SIZE, &resp_len);
    if (MARSHAL.pointer_arg && MARSHAL.copy_out && MARSHAL.arg_size > 0 && resp_len >= MARSHAL.arg_size) {
        std::memcpy(reinterpret_cast<void*>(arg), arg_out.data(), MARSHAL.arg_size);
    }
    if (wait_diag_enabled()) {
        ker::mod::dbg::logger<"wki">::info("pty_ioctl_done peer=0x%04x res=%u cookie=%u cmd=%s(0x%lx) status=%d resp_len=%u copied=%lu",
                                           static_cast<unsigned>(proxy->home_node), proxy->resource_id, static_cast<unsigned>(op_cookie),
                                           pty_ioctl_cmd_name(cmd), cmd, STATUS, static_cast<unsigned>(resp_len),
                                           static_cast<unsigned long>(MARSHAL.pointer_arg && MARSHAL.copy_out ? MARSHAL.arg_size : 0));
    }
    return finish(STATUS);
}

auto proxy_pty_isatty(ker::vfs::File* /*f*/) -> bool { return true; }

auto proxy_pty_close(ker::vfs::File* f) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr) {
        return 0;
    }

    constexpr size_t HEADER_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    constexpr size_t MSG_SIZE = HEADER_SIZE + sizeof(uint64_t);
    std::array<uint8_t, MSG_SIZE> msg = {};
    auto* req = reinterpret_cast<DevOpReqPayload*>(msg.data());
    req->op_id = OP_PTY_CLOSE;
    req->data_len = sizeof(uint32_t) + sizeof(uint64_t);
    std::memcpy(msg.data() + sizeof(DevOpReqPayload), &proxy->resource_id, sizeof(uint32_t));
    uint64_t const EXPECTED_BYTES = proxy->bytes_written.load(std::memory_order_acquire);
    std::memcpy(msg.data() + HEADER_SIZE, &EXPECTED_BYTES, sizeof(EXPECTED_BYTES));

    send_proxy_pipe_close(proxy, msg.data(), static_cast<uint16_t>(msg.size()), proxy->resource_id, OP_PTY_CLOSE);

    wki_ipc_detach_proxy_file(f, proxy);
    return 0;
}

ker::vfs::FileOperations g_proxy_pty_fops = {
    .vfs_open = nullptr,
    .vfs_close = proxy_pty_close,
    .vfs_read = proxy_pipe_read,    // reuse ring-buffer read
    .vfs_write = proxy_pipe_write,  // reuse OP_PIPE_DATA write
    .vfs_lseek = nullptr,
    .vfs_isatty = proxy_pty_isatty,
    .vfs_readdir = nullptr,
    .vfs_readlink = nullptr,
    .vfs_truncate = nullptr,
    .vfs_poll_check = proxy_pipe_poll_check,
    .vfs_poll_register_waiter = proxy_pipe_poll_register_waiter,
    .vfs_ioctl = proxy_pty_ioctl,
};

auto find_export_by_resource_id(uint32_t resource_id) -> WkiIpcExport* {
    for (auto* exp : g_ipc_exports) {
        if (exp->active && exp->resource_id == resource_id) {
            return exp;
        }
    }
    return nullptr;
}

// Zero marks permanent exhaustion. Reserve the entire private export plan so
// failure cannot publish a partial set or recycle any previously issued ID.
auto reserve_ipc_resource_ids(uint32_t& next, uint16_t count, uint32_t& first) -> bool {
    if (count == 0) {
        first = 0;
        return true;
    }
    if (next == 0 || static_cast<uint32_t>(count - 1) > UINT32_MAX - next) {
        return false;
    }
    first = next;
    uint32_t const LAST = next + static_cast<uint32_t>(count - 1);
    next = LAST == UINT32_MAX ? 0 : LAST + 1;
    return true;
}

auto export_pipe_rdma_front_locked(WkiIpcExport* /*exp*/, uint32_t* /*tail_out*/, uint16_t* /*len_out*/, uint8_t** /*data_out*/) -> bool {
    return false;
}

auto queue_export_pipe_rdma_flush(uint16_t src_node, uint32_t resource_id) -> bool {
    size_t shard = 0;
    bool wake = false;
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    auto* exp = find_export_by_resource_id(resource_id);
    if (exp != nullptr && exp->active && exp->consumer_node == src_node && exp->pipe_rdma_enabled) {
        auto* backlog = find_export_pipe_write_backlog_locked(exp);
        if (backlog != nullptr && backlog->persistent_for_rdma) {
            queue_export_pipe_write_flush_locked(backlog);
            shard = export_pipe_write_backlog_shard(backlog);
            wake = true;
        }
    }
    s_ipc_lock.unlock_irqrestore(IRQF);

    if (wake) {
        wake_export_pipe_write_flush_worker(shard);
    }
    return wake;
}

// Both output objects are owned by IpcFdSnapshot until final publication.
// Never put a private backlog in a global registry or queue across registration.
void prepare_export_pipe_rdma(WkiIpcExport& exp, ExportPipeWriteBacklog*& /*backlog*/, ker::vfs::File* file, int fail_at = -1) {
    if (g_ipc_consumer_stopping.load(std::memory_order_acquire)) return;
    // Relay/socket/PTY effects may recursively call WKI and stay message-only.
    if (fail_at == 0 || exp.res_type != ResourceType::IPC_PIPE || !ker::vfs::vfs_is_pipe_file(file) ||
        !wki_secure_rdma_available(exp.channels.data, rdma_protocol::Service::IPC))
        return;
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    uint64_t const LIFETIME = g_next_ipc_secure_lifetime;
    if (LIFETIME != 0 && LIFETIME != UINT64_MAX)
        ++g_next_ipc_secure_lifetime;
    else
        g_next_ipc_secure_lifetime = 0;
    exp.pipe_secure_epoch = ipc_peer_cleanup_epoch_locked(exp.consumer_node);
    s_ipc_lock.unlock_irqrestore(IRQF);
    if (LIFETIME == 0 || LIFETIME == UINT64_MAX) return;
    if (fail_at != 1) exp.pipe_rdma_region = new (std::nothrow) uint8_t[WKI_IPC_PIPE_RDMA_TOTAL_SIZE];
    if (exp.pipe_rdma_region == nullptr) return;
    exp.pipe_rdma_region_size = WKI_IPC_PIPE_RDMA_TOTAL_SIZE;
    exp.pipe_rdma_capacity = WKI_IPC_PIPE_RDMA_CAPACITY;
    exp.pipe_secure_lifetime = LIFETIME;
    std::fill_n(exp.pipe_rdma_region, exp.pipe_rdma_region_size, uint8_t{0});
    int const RESULT = wki_secure_rdma_register_region(
        exp.channels.data, rdma_protocol::Service::IPC, exp.pipe_rdma_region, exp.pipe_rdma_region_size, LIFETIME,
        rdma_protocol::PERMIT_READ | rdma_protocol::PERMIT_WRITE, IPC_SECURE_HOOKS, &exp, exp.pipe_secure_descriptor);
    if (RESULT == 0) {
        exp.pipe_rdma_enabled = true;
        exp.pipe_rdma_rkey = exp.pipe_secure_descriptor.rkey;
    } else
        static_cast<void>(release_export_pipe_rdma(exp));
}

auto proxy_ring_used_bytes(const ProxyIpcState* proxy) -> uint64_t {
    if (proxy == nullptr || proxy->ring_buf == nullptr || proxy->ring_capacity == 0) {
        return 0;
    }

    uint64_t const HEAD = proxy->ring_head.load(std::memory_order_acquire);
    uint64_t const TAIL = proxy->ring_tail.load(std::memory_order_acquire);
    uint64_t const USED = HEAD >= TAIL ? HEAD - TAIL : 0;
    return std::min<uint64_t>(USED, proxy->ring_capacity);
}

// =============================================================================
// Server-side pipe pump: reads from real pipe, sends data via wire messages
// =============================================================================

struct PipePumpArg {
    std::atomic<WkiIpcExport*> exp{nullptr};
    ker::mod::sched::task::Task* worker = nullptr;
    bool starting = false;  // Protected by s_ipc_lock.
    std::atomic<bool> stopped{true};
};

struct PipePumpRetirement {
    WkiChannelIdentity data_channel{};
    WkiIpcExport* retired_exports = nullptr;
    uint16_t target = WKI_NODE_INVALID;
    uint32_t resource_id = 0;
    bool discard_pending = false;
};

constexpr int MAX_PUMPS = 32;
std::array<PipePumpArg, MAX_PUMPS> g_pump_args;
WkiIpcExport* g_pipe_pump_queue_head = nullptr;
WkiIpcExport* g_pipe_pump_queue_tail = nullptr;

auto export_has_pipe_pump_slot_locked(const WkiIpcExport* exp) -> bool {
    if (exp == nullptr) {
        return false;
    }
    if (exp->pump_queued || exp->pump_running.load(std::memory_order_acquire) || exp->pump_task != nullptr) {
        return true;
    }
    return std::ranges::any_of(g_pump_args, [exp](const auto& pump_arg) { return pump_arg.exp.load(std::memory_order_acquire) == exp; });
}

void remove_pipe_pump_queue_locked(WkiIpcExport* exp) {
    if (exp == nullptr || !exp->pump_queued) {
        return;
    }

    WkiIpcExport* prev = nullptr;
    auto* cur = g_pipe_pump_queue_head;
    while (cur != nullptr) {
        auto* next = cur->pump_next;
        if (cur == exp) {
            if (prev != nullptr) {
                prev->pump_next = next;
            } else {
                g_pipe_pump_queue_head = next;
            }
            if (g_pipe_pump_queue_tail == exp) {
                g_pipe_pump_queue_tail = prev;
            }
            exp->pump_queued = false;
            exp->pump_next = nullptr;
            return;
        }
        prev = cur;
        cur = next;
    }

    exp->pump_queued = false;
    exp->pump_next = nullptr;
}

void enqueue_pipe_pump_locked(WkiIpcExport* exp) {
    if (exp == nullptr || exp->pump_queued) {
        return;
    }

    exp->pump_queued = true;
    exp->pump_next = nullptr;
    if (g_pipe_pump_queue_tail != nullptr) {
        g_pipe_pump_queue_tail->pump_next = exp;
    } else {
        g_pipe_pump_queue_head = exp;
    }
    g_pipe_pump_queue_tail = exp;
}

auto pop_pipe_pump_queue_locked() -> WkiIpcExport* {
    while (g_pipe_pump_queue_head != nullptr) {
        auto* exp = g_pipe_pump_queue_head;
        g_pipe_pump_queue_head = exp->pump_next;
        if (g_pipe_pump_queue_tail == exp) {
            g_pipe_pump_queue_tail = nullptr;
        }
        exp->pump_queued = false;
        exp->pump_next = nullptr;

        if (exp->active && exp->file != nullptr) {
            return exp;
        }
        exp->pump_running.store(false, std::memory_order_release);
        exp->pump_task = nullptr;
    }

    return nullptr;
}

auto assign_next_pipe_pump_locked(PipePumpArg& pump_arg) -> WkiIpcExport* {
    if (g_ipc_consumer_stopping.load(std::memory_order_acquire) || ker::mod::sched::kernel_threads_shutdown_requested() ||
        pump_arg.worker == nullptr || pump_arg.exp.load(std::memory_order_acquire) != nullptr) {
        return nullptr;
    }

    auto* next = pop_pipe_pump_queue_locked();
    if (next == nullptr) {
        return nullptr;
    }

    next->pump_running.store(true, std::memory_order_release);
    next->pump_task = pump_arg.worker;
    pump_arg.exp.store(next, std::memory_order_release);
    return next;
}

void release_pipe_pump_slot_locked(PipePumpArg& pump_arg, WkiIpcExport* exp) {
    if (exp != nullptr && pump_arg.exp.load(std::memory_order_acquire) == exp) {
        exp->pump_running.store(false, std::memory_order_release);
        exp->pump_task = nullptr;
        pump_arg.exp.store(nullptr, std::memory_order_release);
    }

    static_cast<void>(assign_next_pipe_pump_locked(pump_arg));
}

auto collect_pipe_pump_retirement_locked(PipePumpArg& pump_arg, WkiIpcExport* exp) -> PipePumpRetirement {
    PipePumpRetirement retirement{};
    if (exp != nullptr) {
        retirement.data_channel = exp->channels.data;
        retirement.target = exp->consumer_node;
        retirement.resource_id = exp->resource_id;
        retirement.discard_pending = !exp->active;
    }
    release_pipe_pump_slot_locked(pump_arg, exp);
    retirement.retired_exports = compact_inactive_exports_locked();
    return retirement;
}

void finish_pipe_pump_retirement(const PipePumpRetirement& retirement) {
    free_retired_exports(retirement.retired_exports);
    if (retirement.discard_pending) {
        // The pump slot is released only after its last possible DATA/EOF
        // send, so this same-channel marker is the receiver teardown fence.
        schedule_pending_pipe_discard(retirement.data_channel, retirement.resource_id);
    }
}

auto stop_pipe_pump_locked(WkiIpcExport* exp) -> ker::mod::sched::task::Task* {
    if (exp == nullptr) {
        return nullptr;
    }
    remove_pipe_pump_queue_locked(exp);
    exp->pump_running.store(false, std::memory_order_release);
    auto* task = exp->pump_task;
    return task != nullptr && task->try_acquire_lifetime_ref() ? task : nullptr;
}

void wake_pipe_pump(ker::mod::sched::task::Task* task) {
    if (task != nullptr) {
        ker::mod::sched::kern_wake(task);
        task->release();
    }
}

auto register_poll_read_waiter(ker::vfs::File* file, bool* ready_now) -> bool {
    constexpr int POLL_READ_EVENTS = 0x0001 | 0x0010;  // POLLIN | POLLHUP
    if (ready_now != nullptr) {
        *ready_now = false;
    }
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || file == nullptr || file->fops == nullptr || file->fops->vfs_poll_register_waiter == nullptr) {
        return false;
    }
    task->set_wait_channel("wki_pipe_pump_poll");
    if (!file->fops->vfs_poll_register_waiter(file, task->pid)) {
        return false;
    }
    if (ready_now != nullptr && file->fops->vfs_poll_check != nullptr) {
        *ready_now = (file->fops->vfs_poll_check(file, POLL_READ_EVENTS) & POLL_READ_EVENTS) != 0;
    }
    return true;
}

auto pipe_pump_read_ready(ker::vfs::File* file) -> bool {
    constexpr int POLL_READ_EVENTS = 0x0001 | 0x0010;  // POLLIN | POLLHUP
    if (file == nullptr || file->fops == nullptr || file->fops->vfs_poll_check == nullptr) {
        return true;
    }

    if ((file->fops->vfs_poll_check(file, POLL_READ_EVENTS) & POLL_READ_EVENTS) != 0) {
        return true;
    }

    bool ready_now = false;
    if (!register_poll_read_waiter(file, &ready_now)) {
        return true;
    }
    if (ready_now) {
        return true;
    }

    ker::mod::sched::kern_block();
    return false;
}

auto register_poll_write_waiter(ker::vfs::File* file, bool* ready_now) -> bool {
    constexpr int POLL_WRITE_EVENTS = dev::pty::POLLOUT | dev::pty::POLLERR;
    if (ready_now != nullptr) {
        *ready_now = false;
    }
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || file == nullptr || file->fops == nullptr || file->fops->vfs_poll_register_waiter == nullptr) {
        return false;
    }
    task->set_wait_channel("wki_pipe_write_poll");
    if (!file->fops->vfs_poll_register_waiter(file, task->pid)) {
        return false;
    }
    if (ready_now != nullptr && file->fops->vfs_poll_check != nullptr) {
        *ready_now = (file->fops->vfs_poll_check(file, POLL_WRITE_EVENTS) & POLL_WRITE_EVENTS) != 0;
    }
    return true;
}

template <int SLOT>
void pipe_pump_thread_fn() {
    auto* arg = &std::get<SLOT>(g_pump_args);
    // Each DATA/EOF send initializes its exact transmitted prefix. Keep the
    // unused bounded tail untouched while this worker reuses the frame.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
    std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> msg __attribute__((uninitialized));

    for (;;) {
        auto const HOLD = ker::mod::sched::hold_kernel_shutdown();
        if (g_ipc_consumer_stopping.load(std::memory_order_acquire) || ker::mod::sched::kernel_threads_shutdown_requested()) {
            g_ipc_consumer_stopping.store(true, std::memory_order_release);
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            auto* worker = std::exchange(arg->worker, nullptr);
            PipePumpRetirement const RETIREMENT = collect_pipe_pump_retirement_locked(*arg, arg->exp.load(std::memory_order_acquire));
            s_ipc_lock.unlock_irqrestore(IRQF);
            finish_pipe_pump_retirement(RETIREMENT);
            if (worker != nullptr) worker->release();
            arg->stopped.store(true, std::memory_order_release);
            return;
        }
        auto* exp = arg->exp.load(std::memory_order_acquire);
        if (exp == nullptr) {
            ker::mod::sched::kern_block();
            continue;
        }

        ker::vfs::File* file = nullptr;
        uint16_t target = WKI_NODE_INVALID;
        WkiChannelIdentity data_channel{};
        uint32_t resource_id = 0;
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            if (arg->exp.load(std::memory_order_acquire) != exp) {
                s_ipc_lock.unlock_irqrestore(IRQF);
                continue;
            }
            if (!exp->active || exp->file == nullptr) {
                PipePumpRetirement const RETIREMENT = collect_pipe_pump_retirement_locked(*arg, exp);
                s_ipc_lock.unlock_irqrestore(IRQF);
                finish_pipe_pump_retirement(RETIREMENT);
                continue;
            }
            file = exp->file;
            file->refcount.fetch_add(1, std::memory_order_acq_rel);
            target = exp->consumer_node;
            data_channel = exp->channels.data;
            resource_id = exp->resource_id;
            s_ipc_lock.unlock_irqrestore(IRQF);
        }

        constexpr size_t HEADER_SIZE = WKI_IPC_PIPE_DATA_HEADER_SIZE;
        constexpr size_t BUF_SIZE = WKI_IPC_PIPE_DATA_MAX_CHUNK;
#ifdef DEBUG_WKI_IPC
        ker::mod::dbg::log("[WKI] IPC pipe pump started: resource_id=%u target=0x%04x", resource_id, target);
#endif
        while (exp->active && exp->pump_running.load(std::memory_order_acquire) &&
               !g_ipc_consumer_stopping.load(std::memory_order_acquire) && !ker::mod::sched::kernel_threads_shutdown_requested()) {
            if (!pipe_pump_read_ready(file)) {
                continue;
            }
            if (!exp->active || !exp->pump_running.load(std::memory_order_acquire)) {
                break;
            }

            IpcPerfTrace read_trace(ker::mod::perf::WkiPerfIpcOp::PIPE_PUMP_READ, target, WKI_CHAN_RESOURCE, WOS_PERF_CALLSITE());
            ssize_t n = 0;
            if (file != nullptr && file->fops != nullptr && file->fops->vfs_read != nullptr) {
                if (ker::vfs::vfs_is_socket_file(file) || ker::vfs::vfs_is_pipe_file(file)) {
                    // A blocking read holds the last File after remote close,
                    // preventing TCP EOF until the peer closes first. Keep the
                    // shared endpoint flags intact and revisit pump retirement
                    // through the existing outside-lock EAGAIN retry instead.
                    ker::vfs::File view{};
                    init_nonblocking_pipe_write_view(view, *file);
                    n = clamp_io_count(view.fops->vfs_read(&view, msg.data() + HEADER_SIZE, BUF_SIZE, 0), BUF_SIZE);
                } else {
                    n = clamp_io_count(file->fops->vfs_read(file, msg.data() + HEADER_SIZE, BUF_SIZE, 0), BUF_SIZE);
                }
            } else {
                break;
            }
            read_trace.finish(n >= 0 ? 0 : static_cast<int32_t>(n), n > 0 ? static_cast<uint64_t>(n) : 0);

            if (n > 0) {
                auto* req = reinterpret_cast<DevOpReqPayload*>(msg.data());
                req->op_id = OP_PIPE_DATA;
                req->data_len = static_cast<uint16_t>(sizeof(uint32_t) + n);
                std::memcpy(msg.data() + sizeof(DevOpReqPayload), &resource_id, sizeof(uint32_t));

                IpcPerfTrace send_trace(ker::mod::perf::WkiPerfIpcOp::PIPE_PUMP_SEND, target, WKI_CHAN_IPC_DATA, WOS_PERF_CALLSITE(),
                                        static_cast<uint32_t>(n));
                int ret = WKI_ERR_TX_FAILED;
                uint32_t attempts = 0;
                uint64_t const SEND_STARTED_US = wki_now_us();
                uint64_t last_send_diag_us = 0;
                while (exp->active && exp->pump_running.load(std::memory_order_acquire) &&
                       !g_ipc_consumer_stopping.load(std::memory_order_acquire) && !ker::mod::sched::kernel_threads_shutdown_requested()) {
                    ret =
                        wki_send_on_channel_identity(data_channel, MsgType::DEV_OP_REQ, msg.data(), static_cast<uint16_t>(HEADER_SIZE + n));
                    if (ret == WKI_OK || ret == WKI_ERR_NOT_FOUND) {
                        break;
                    }
                    uint32_t const ATTEMPT = attempts++;
                    if (ipc_diag_due(SEND_STARTED_US, last_send_diag_us)) {
                        log_pipe_pump_send_retry_diag("pipe_pump_data_wait", target, resource_id, n, ret, attempts, SEND_STARTED_US);
                    }
                    pause_for_ipc_send_retry(ret, ATTEMPT);
                }
                // NOLINTNEXTLINE(readability-suspicious-call-argument): ret is status; attempts is retry count.
                send_trace.finish(ret, static_cast<uint64_t>(n), attempts);
                if (ret != WKI_OK) {
                    ker::mod::dbg::logger<"wki">::warn("IPC pipe pump DATA send failed: resource_id=%u ret=%d len=%ld", resource_id, ret,
                                                       n);
                    break;
                }
            } else if (n == 0) {
                auto* req = reinterpret_cast<DevOpReqPayload*>(msg.data());
                req->op_id = OP_PIPE_CLOSE_WRITE;
                req->data_len = sizeof(uint32_t);
                std::memcpy(msg.data() + sizeof(DevOpReqPayload), &resource_id, sizeof(uint32_t));
                int ret = WKI_ERR_TX_FAILED;
                uint32_t retries = 0;
                uint32_t attempts = 0;
                uint64_t const SEND_STARTED_US = wki_now_us();
                uint64_t last_send_diag_us = 0;
                while (exp->active && (exp->pump_running.load(std::memory_order_acquire) || attempts == 0) &&
                       attempts < WKI_IPC_PIPE_EOF_MAX_SEND_ATTEMPTS && !g_ipc_consumer_stopping.load(std::memory_order_acquire) &&
                       !ker::mod::sched::kernel_threads_shutdown_requested()) {
                    attempts++;
                    ret = wki_send_on_channel_identity(data_channel, MsgType::DEV_OP_REQ, msg.data(), static_cast<uint16_t>(HEADER_SIZE));
                    if (ret == WKI_OK || ret == WKI_ERR_NOT_FOUND) {
                        break;
                    }
                    retries++;
                    if (ipc_diag_due(SEND_STARTED_US, last_send_diag_us)) {
                        log_pipe_pump_send_retry_diag("pipe_pump_eof_wait", target, resource_id, 0, ret, attempts, SEND_STARTED_US);
                    }
                    pause_for_ipc_send_retry(ret, attempts);
                }
                if (ret != WKI_OK) {
                    if (attempts != 0 && exp->active) {
                        ker::mod::dbg::logger<"wki">::warn("IPC pipe pump EOF send failed: resource_id=%u ret=%d attempts=%u retries=%u",
                                                           resource_id, ret, attempts, retries);
                    }
                }
#ifdef WKI_IPC_DEBUG
                else {
                    ker::mod::dbg::log("[WKI] IPC pipe pump EOF: resource_id=%u retries=%u", resource_id, retries);
                }
#endif
                break;
            } else if (n == -EAGAIN) {
                bool ready_now = false;
                if (register_poll_read_waiter(file, &ready_now)) {
                    if (!ready_now) {
                        ker::mod::sched::kern_block();
                    }
                } else {
                    ker::mod::sched::kern_sleep_us(WKI_IPC_PIPE_READ_POLL_RECHECK_US);
                }
                continue;
            } else {
                ker::mod::dbg::logger<"wki">::warn("IPC pipe pump read error: resource_id=%u err=%ld", resource_id, n);
                break;
            }
        }

        ipc_release_file_ref(file);
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        PipePumpRetirement const RETIREMENT = collect_pipe_pump_retirement_locked(*arg, exp);
        s_ipc_lock.unlock_irqrestore(IRQF);
        finish_pipe_pump_retirement(RETIREMENT);
    }
}

template <int SLOT>
[[noreturn]] void pipe_pump_thread_entry() {
    pipe_pump_thread_fn<SLOT>();
    ker::mod::sched::exit_current_kernel_thread();
}

using PumpFn = void (*)();
constexpr std::array<PumpFn, MAX_PUMPS> G_PUMP_FNS = {
    pipe_pump_thread_entry<0>,  pipe_pump_thread_entry<1>,  pipe_pump_thread_entry<2>,  pipe_pump_thread_entry<3>,
    pipe_pump_thread_entry<4>,  pipe_pump_thread_entry<5>,  pipe_pump_thread_entry<6>,  pipe_pump_thread_entry<7>,
    pipe_pump_thread_entry<8>,  pipe_pump_thread_entry<9>,  pipe_pump_thread_entry<10>, pipe_pump_thread_entry<11>,
    pipe_pump_thread_entry<12>, pipe_pump_thread_entry<13>, pipe_pump_thread_entry<14>, pipe_pump_thread_entry<15>,
    pipe_pump_thread_entry<16>, pipe_pump_thread_entry<17>, pipe_pump_thread_entry<18>, pipe_pump_thread_entry<19>,
    pipe_pump_thread_entry<20>, pipe_pump_thread_entry<21>, pipe_pump_thread_entry<22>, pipe_pump_thread_entry<23>,
    pipe_pump_thread_entry<24>, pipe_pump_thread_entry<25>, pipe_pump_thread_entry<26>, pipe_pump_thread_entry<27>,
    pipe_pump_thread_entry<28>, pipe_pump_thread_entry<29>, pipe_pump_thread_entry<30>, pipe_pump_thread_entry<31>,
};

auto create_pipe_pump_worker(PipePumpArg& pump_arg, PumpFn pump_fn) -> ker::mod::sched::task::Task* {
    auto const HOLD = ker::mod::sched::hold_kernel_shutdown();
    uint64_t irqf = s_ipc_lock.lock_irqsave();
    if (g_ipc_consumer_stopping.load(std::memory_order_acquire) || ker::mod::sched::kernel_threads_shutdown_requested() ||
        pump_arg.starting || pump_fn == nullptr) {
        s_ipc_lock.unlock_irqrestore(irqf);
        return nullptr;
    }
    if (pump_arg.worker != nullptr) {
        auto* worker = pump_arg.worker->try_acquire_lifetime_ref() ? pump_arg.worker : nullptr;
        s_ipc_lock.unlock_irqrestore(irqf);
        return worker;
    }
    pump_arg.starting = true;
    s_ipc_lock.unlock_irqrestore(irqf);

    auto* task = ker::mod::sched::task::Task::create_kernel_thread("wki_pipe_pump", pump_fn);
    if (task == nullptr) {
        irqf = s_ipc_lock.lock_irqsave();
        pump_arg.starting = false;
        s_ipc_lock.unlock_irqrestore(irqf);
        ker::mod::dbg::log("[WKI] IPC pump: failed to create kernel thread");
        return nullptr;
    }

    if (!task->try_acquire_lifetime_ref()) {
        ker::mod::sched::task::destroy_unpublished_user_thread(task);
        irqf = s_ipc_lock.lock_irqsave();
        pump_arg.starting = false;
        s_ipc_lock.unlock_irqrestore(irqf);
        return nullptr;
    }
    promote_latency_sensitive_daemon(task);
    irqf = s_ipc_lock.lock_irqsave();
    bool const PUBLISH = !g_ipc_consumer_stopping.load(std::memory_order_acquire) && !ker::mod::sched::kernel_threads_shutdown_requested();
    if (PUBLISH) {
        pump_arg.worker = task;
        pump_arg.stopped.store(false, std::memory_order_release);
    }
    s_ipc_lock.unlock_irqrestore(irqf);
    bool const POSTED = PUBLISH && ker::mod::sched::post_task_balanced(task);
    irqf = s_ipc_lock.lock_irqsave();
    if (!POSTED) {
        pump_arg.worker = nullptr;
        pump_arg.stopped.store(true, std::memory_order_release);
    }
    pump_arg.starting = false;
    // Return a separate temporary ref; the new worker may already have stopped.
    auto* result = POSTED && pump_arg.worker != nullptr && pump_arg.worker->try_acquire_lifetime_ref() ? pump_arg.worker : nullptr;
    s_ipc_lock.unlock_irqrestore(irqf);
    if (!POSTED) {
        task->release();
        ker::mod::sched::task::destroy_unpublished_user_thread(task);
    }
    return result;
}

void init_pipe_pump_workers() {
    const auto* pump_fn_it = G_PUMP_FNS.begin();
    for (auto& pump_arg : g_pump_args) {
        auto* worker = create_pipe_pump_worker(pump_arg, *pump_fn_it);
        if (worker != nullptr) worker->release();
        ++pump_fn_it;
    }
}

// Caller holds s_ipc_lock. Claim exactly once and only for the accepting peer.
// The caller must reserve a queue/worker slot before dropping the lock.
auto claim_accepted_pipe_pump_locked(uint32_t resource_id, uint16_t consumer_node) -> WkiIpcExport* {
    auto* exp = find_export_by_resource_id(resource_id);
    if (exp == nullptr || !exp->active || exp->file == nullptr || exp->consumer_node != consumer_node || !exp->pump_pending_accept ||
        exp->pump_queued || exp->pump_running.load(std::memory_order_acquire))
        return nullptr;
    exp->pump_pending_accept = false;
    return exp;
}

void start_pipe_pump(uint32_t resource_id, uint16_t consumer_node) {
    PipePumpArg* slot_arg = nullptr;
    PumpFn pump_fn = nullptr;
    ker::mod::sched::task::Task* task = nullptr;
    const auto* pump_fn_it = G_PUMP_FNS.begin();
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        if (g_ipc_consumer_stopping.load(std::memory_order_acquire)) {
            s_ipc_lock.unlock_irqrestore(IRQF);
            return;
        }
        auto* exp = claim_accepted_pipe_pump_locked(resource_id, consumer_node);
        if (exp == nullptr) {
            s_ipc_lock.unlock_irqrestore(IRQF);
            return;
        }
        for (auto& pump_arg : g_pump_args) {
            if (pump_arg.exp.load(std::memory_order_acquire) == nullptr) {
                slot_arg = &pump_arg;
                pump_fn = *pump_fn_it;
                task = pump_arg.worker;
                if (task != nullptr && !task->try_acquire_lifetime_ref()) task = nullptr;
                exp->pump_running.store(true, std::memory_order_release);
                exp->pump_task = task;
                pump_arg.exp.store(exp, std::memory_order_release);
                break;
            }
            ++pump_fn_it;
        }
        if (slot_arg == nullptr || pump_fn == nullptr) {
            enqueue_pipe_pump_locked(exp);
            s_ipc_lock.unlock_irqrestore(IRQF);
            return;
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    if (task == nullptr) {
        task = create_pipe_pump_worker(*slot_arg, pump_fn);
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        // A new worker may already have retired its exporter. Read only the
        // current slot under lock; never dereference the old raw pointer here.
        auto* current = slot_arg->exp.load(std::memory_order_acquire);
        if (current != nullptr && current->resource_id == resource_id && current->consumer_node == consumer_node) {
            if (task == nullptr) {
                current->pump_task = nullptr;
                slot_arg->exp.store(nullptr, std::memory_order_release);
                current->pump_running.store(false, std::memory_order_release);
                if (current->active && current->file != nullptr) enqueue_pipe_pump_locked(current);
            } else if (current->active && current->pump_running.load(std::memory_order_acquire)) {
                current->pump_task = task;
            } else {
                release_pipe_pump_slot_locked(*slot_arg, current);
            }
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
        if (task == nullptr) return;
    }
    wake_pipe_pump(task);
}

void init_ipc_dev_op_work_pool_locked() {
    if (g_ipc_dev_op_pool_initialized) {
        return;
    }
    g_ipc_dev_op_free = nullptr;
    g_ipc_dev_op_free_count = 0;
    for (auto& storage : g_ipc_dev_op_work) {
        storage.work = {};
        storage.work.payload = storage.payload.data();
        storage.work.payload_coallocated = true;
        storage.work.fixed_pool = true;
        storage.work.next = g_ipc_dev_op_free;
        g_ipc_dev_op_free = &storage.work;
        ++g_ipc_dev_op_free_count;
    }
    g_ipc_dev_op_pool_initialized = true;
}

void init_ipc_dev_op_work_pool() {
    uint64_t const IRQF = s_ipc_dev_op_pool_lock.lock_irqsave();
    init_ipc_dev_op_work_pool_locked();
    s_ipc_dev_op_pool_lock.unlock_irqrestore(IRQF);
}

void append_endpoint_delivery_locked(IpcDevOpWork* work, uint32_t group) {
    if (group == 0) return;
    work->endpoint_delivery_group = group;
    work->delivery_previous = g_endpoint_delivery_tail;
    if (g_endpoint_delivery_tail != nullptr)
        g_endpoint_delivery_tail->delivery_next = work;
    else
        g_endpoint_delivery_head = work;
    g_endpoint_delivery_tail = work;
}

auto endpoint_delivery_ready_locked(const IpcDevOpWork* work) -> bool {
    if (work->endpoint_delivery_group == 0) return true;
    for (auto* earlier = work->delivery_previous; earlier != nullptr; earlier = earlier->delivery_previous) {
        if (earlier->endpoint_delivery_group == work->endpoint_delivery_group) return false;
    }
    return true;
}

// Called with the pool lock held after the original DATA channel has
// dispatched the captured boundary. Later writes cannot extend this prefix.
auto endpoint_delivery_prefix_pending_locked(uint32_t group, const WkiChannelIdentity& channel, uint32_t boundary) -> bool {
    if (group == 0) return false;
    for (auto* work = g_endpoint_delivery_head; work != nullptr; work = work->delivery_next) {
        if (work->endpoint_delivery_group == group && ipc_channel_identity_equal(work->channel_identity, channel) &&
            seq_before(work->hdr.seq_num, boundary))
            return true;
    }
    return false;
}

auto take_ipc_dev_op_work_locked(uint16_t payload_len, uint32_t delivery_group = 0) -> IpcDevOpWork* {
    if (payload_len > WKI_ETH_MAX_PAYLOAD) {
        return nullptr;
    }
    init_ipc_dev_op_work_pool_locked();
    // Ordered DATA must retain its work until delivery, including one-byte
    // chunks. Leave the recyclable tail available for controls and receipts.
    if (delivery_group != 0 && g_ipc_dev_op_free_count <= WKI_IPC_DEV_OP_TRANSFER_RESERVE) return nullptr;
    IpcDevOpWork* work = g_ipc_dev_op_free;
    if (work == nullptr || g_ipc_dev_op_free_count == 0) {
        return nullptr;
    }
    g_ipc_dev_op_free = work->next;
    --g_ipc_dev_op_free_count;
    auto* storage = reinterpret_cast<IpcDevOpStorage*>(work);
    *work = {};
    work->payload = storage->payload.data();
    work->payload_len = payload_len;
    work->payload_coallocated = true;
    work->fixed_pool = true;
    // A full pipe may retain work items as exact backlog owners. Preserve a
    // small recyclable tail so tiny flow-controlled writes cannot pin the
    // entire RX pool and prevent the reliable head/close behind them from
    // being admitted. Reserved items copy only in the task-context worker.
    work->pipe_chunk_transferable = g_ipc_dev_op_free_count >= WKI_IPC_DEV_OP_TRANSFER_RESERVE;
    append_endpoint_delivery_locked(work, delivery_group);
    return work;
}

#ifdef WOS_SELFTEST
auto alloc_ipc_dev_op_work(uint16_t payload_len) -> IpcDevOpWork* {
    uint64_t const IRQF = s_ipc_dev_op_pool_lock.lock_irqsave();
    IpcDevOpWork* const WORK = take_ipc_dev_op_work_locked(payload_len);
    s_ipc_dev_op_pool_lock.unlock_irqrestore(IRQF);
    return WORK;
}
#endif

void free_ipc_dev_op_work(IpcDevOpWork* work) {
    if (work == nullptr) {
        return;
    }

#ifdef WOS_SELFTEST
    g_ipc_dev_op_work_selftest_frees.fetch_add(1, std::memory_order_relaxed);
#endif

    if (!work->fixed_pool) {
        return;
    }
    auto* storage = reinterpret_cast<IpcDevOpStorage*>(work);
    uint64_t const IRQF = s_ipc_dev_op_pool_lock.lock_irqsave();
    if (work->endpoint_delivery_group != 0) {
        if (work->delivery_previous != nullptr)
            work->delivery_previous->delivery_next = work->delivery_next;
        else
            g_endpoint_delivery_head = work->delivery_next;
        if (work->delivery_next != nullptr)
            work->delivery_next->delivery_previous = work->delivery_previous;
        else
            g_endpoint_delivery_tail = work->delivery_previous;
    }
    *work = {};
    work->payload = storage->payload.data();
    work->payload_coallocated = true;
    work->fixed_pool = true;
    work->next = g_ipc_dev_op_free;
    g_ipc_dev_op_free = work;
    ++g_ipc_dev_op_free_count;
    s_ipc_dev_op_pool_lock.unlock_irqrestore(IRQF);
}

using IpcControlStatusReply = std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(uint32_t)>;

auto unsupported_ipc_control_reply(uint16_t op, uint32_t resource_id, uint16_t cookie) -> IpcControlStatusReply {
    DevOpRespPayload response{};
    response.op_id = op;
    response.status = -ENOTSUP;
    response.data_len = sizeof(resource_id);
    response.reserved = cookie;
    IpcControlStatusReply reply{};
    std::memcpy(reply.data(), &response, sizeof(response));
    std::memcpy(reply.data() + sizeof(response), &resource_id, sizeof(resource_id));
    return reply;
}

auto ipc_op_uses_response_cookie(uint16_t op_id) -> bool {
    bool const SOCKET_CONTROL = op_id >= OP_SOCK_ACCEPT && op_id <= OP_SOCK_SETSOCKOPT && op_id != OP_SOCK_CLOSE;
    return op_id == OP_EPOLL_CTL || op_id == OP_FUTEX_WAKE || op_id == OP_PTY_IOCTL || SOCKET_CONTROL;
}

#ifdef WOS_SELFTEST
auto ipc_dev_op_is_close(uint16_t op_id) -> bool {
    return op_id == OP_PIPE_CLOSE_READ || op_id == OP_PIPE_CLOSE_WRITE || op_id == OP_PIPE_CLOSE_WRITE_FLOW || op_id == OP_PTY_CLOSE ||
           op_id == OP_SOCK_CLOSE || op_id == OP_PIPE_DISCARD_PENDING;
}

auto ipc_dev_op_must_not_drop(uint16_t op_id) -> bool {
    // Pipe DATA has no per-packet response path; once WKI accepts it, dropping
    // the deferred work item silently corrupts the byte stream. The
    // flow-controlled variant is bounded by application credits.
    return op_id == OP_PIPE_DATA || op_id == OP_PIPE_DATA_FLOW || ipc_dev_op_is_close(op_id);
}

auto should_defer_ipc_dev_op(uint16_t op_id, uint32_t resource_id, uint16_t src_node, bool* drop_out) -> bool {
    if (drop_out != nullptr) {
        *drop_out = false;
    }
    if (op_id == OP_PIPE_DATA || op_id == OP_PIPE_DATA_FLOW) {
        bool const FLOW_CONTROLLED = op_id == OP_PIPE_DATA_FLOW;
        bool has_proxy = false;
        bool has_receiving_proxy = false;
        bool has_matching_export = false;
        ProxyIpcState* proxy = nullptr;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy = find_proxy_by_endpoint_locked(src_node, resource_id);
        if (proxy != nullptr) {
            has_proxy = true;
            has_receiving_proxy = proxy->can_receive_data;
            if (!has_receiving_proxy) {
                auto* exp = find_export_by_resource_id(resource_id);
                has_matching_export = exp != nullptr && exp->active && exp->file != nullptr && exp->consumer_node == src_node;
            }
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
        if (proxy != nullptr) {
            proxy_release(proxy);
        }

        if (!FLOW_CONTROLLED && has_proxy && !has_receiving_proxy && !has_matching_export) {
            if (drop_out != nullptr) {
                *drop_out = true;
            }
            return false;
        }

        // Proxy ring delivery is bounded and wakes local readers. Export-side
        // writes can enter arbitrary file/socket/PTY ops, so defer them. With
        // no proxy, preserve pre-attachment delivery through the worker queue.
        return FLOW_CONTROLLED || !has_receiving_proxy;
    }

    if (op_id == OP_PIPE_POLL_STATE || op_id == OP_FUTEX_WAKE || op_id == OP_PTY_IOCTL || ipc_dev_op_is_close(op_id)) {
        (void)src_node;
        return true;
    }

    if (op_id == OP_EPOLL_CTL) {
        (void)src_node;
        // Legacy epoll control only builds an unsupported-operation reply;
        // it must never enter the ambient task's FD namespace.
        return false;
    }

    if (op_id >= OP_SOCK_ACCEPT && op_id <= OP_SOCK_SETSOCKOPT) {
        (void)src_node;
        // Socket callbacks may allocate, sleep, or enter protocol locks.
        return true;
    }

    return false;
}
#endif

auto ipc_dev_op_worker_index(uint32_t resource_id) -> size_t { return resource_id % WKI_IPC_DEV_OP_WORKER_COUNT; }

auto ipc_peer_cleanup_slot_locked(uint16_t node_id, bool create) -> IpcPeerCleanupEpoch* {
    IpcPeerCleanupEpoch* free_slot = nullptr;
    for (auto& slot : g_ipc_peer_cleanup_epochs) {
        if (slot.used && slot.node_id == node_id) {
            return &slot;
        }
        if (!slot.used && free_slot == nullptr) {
            free_slot = &slot;
        }
    }

    if (!create || free_slot == nullptr) {
        return nullptr;
    }

    free_slot->used = true;
    free_slot->node_id = node_id;
    free_slot->epoch = 0;
    free_slot->cleanup_active = false;
    free_slot->active_workers = 0;
    return free_slot;
}

void bump_ipc_peer_cleanup_epoch_locked(IpcPeerCleanupEpoch* slot) {
    if (slot == nullptr) {
        return;
    }
    slot->epoch++;
    if (slot->epoch == 0) {
        slot->epoch = 1;
    }
}

auto ipc_peer_cleanup_epoch_locked(uint16_t node_id) -> uint64_t {
    auto* slot = ipc_peer_cleanup_slot_locked(node_id, false);
    return slot != nullptr ? slot->epoch : 0;
}

auto ipc_peer_cleanup_active_locked(uint16_t node_id) -> bool {
    auto* slot = ipc_peer_cleanup_slot_locked(node_id, false);
    return slot != nullptr && slot->cleanup_active;
}

auto ipc_peer_active_workers_locked(uint16_t node_id) -> uint32_t {
    auto* slot = ipc_peer_cleanup_slot_locked(node_id, false);
    return slot != nullptr ? slot->active_workers : 0;
}

auto ipc_dev_op_work_is_fenced_locked(const IpcDevOpWork* work) -> bool;

auto claim_ipc_dev_op_worker_locked(const IpcDevOpWork* work) -> bool {
    if (ipc_dev_op_work_is_fenced_locked(work)) {
        return false;
    }
    auto* slot = ipc_peer_cleanup_slot_locked(work->hdr.src_node, true);
    if (slot == nullptr || slot->active_workers == UINT32_MAX) {
        return false;
    }
    slot->active_workers++;
    return true;
}

void release_ipc_dev_op_worker_locked(uint16_t node_id) {
    auto* slot = ipc_peer_cleanup_slot_locked(node_id, false);
    if (slot != nullptr && slot->active_workers != 0) {
        slot->active_workers--;
    }
}

void begin_ipc_peer_dev_op_cleanup_locked(uint16_t node_id) {
    auto* slot = ipc_peer_cleanup_slot_locked(node_id, true);
    if (slot == nullptr) {
        return;
    }
    slot->cleanup_active = true;
    bump_ipc_peer_cleanup_epoch_locked(slot);
}

void end_ipc_peer_dev_op_cleanup_locked(uint16_t node_id) {
    auto* slot = ipc_peer_cleanup_slot_locked(node_id, true);
    if (slot == nullptr) {
        return;
    }
    slot->cleanup_active = false;
    bump_ipc_peer_cleanup_epoch_locked(slot);
}

auto ipc_dev_op_work_is_fenced_locked(const IpcDevOpWork* work) -> bool {
    if (work == nullptr) {
        return true;
    }
    uint16_t const SRC_NODE = work->hdr.src_node;
    return ipc_peer_cleanup_active_locked(SRC_NODE) || work->cleanup_epoch != ipc_peer_cleanup_epoch_locked(SRC_NODE);
}

void ipc_dev_op_queue_push_back_locked(IpcDevOpQueue& queue, IpcDevOpWork* work) {
    if (work == nullptr) {
        return;
    }
    work->next = nullptr;
    if (queue.tail != nullptr) {
        queue.tail->next = work;
    } else {
        queue.head = work;
    }
    queue.tail = work;
    queue.count++;
}

auto ipc_dev_op_queue_pop_front_locked(IpcDevOpQueue& queue) -> IpcDevOpWork* {
    IpcDevOpWork* work = queue.head;
    if (work == nullptr) {
        return nullptr;
    }
    queue.head = work->next;
    if (queue.head == nullptr) {
        queue.tail = nullptr;
    }
    work->next = nullptr;
    queue.count--;
    return work;
}

void ipc_dev_op_queue_push_front_locked(IpcDevOpQueue& queue, IpcDevOpWork* work) {
    if (work == nullptr) {
        return;
    }
    work->next = queue.head;
    queue.head = work;
    if (queue.tail == nullptr) {
        queue.tail = work;
    }
    queue.count++;
}

#ifdef WOS_SELFTEST
auto ipc_dev_op_queue_remove_locked(IpcDevOpQueue& queue, IpcDevOpWork* target) -> bool {
    IpcDevOpWork* previous = nullptr;
    for (IpcDevOpWork* work = queue.head; work != nullptr; work = work->next) {
        if (work != target) {
            previous = work;
            continue;
        }
        if (previous != nullptr) {
            previous->next = work->next;
        } else {
            queue.head = work->next;
        }
        if (queue.tail == work) {
            queue.tail = previous;
        }
        work->next = nullptr;
        queue.count--;
        return true;
    }
    return false;
}
#endif

auto take_ready_ipc_dev_op_work_locked(IpcDevOpQueue& queue, bool& ordered_wait) -> IpcDevOpWork* {
    // Inspect at most the fixed pool's queued work. A blocked alias must not
    // impose one sleep per chunk on an eligible predecessor or other terminal.
    size_t const BUDGET = queue.count;
    uint64_t const IRQF = s_ipc_dev_op_pool_lock.lock_irqsave();
    for (size_t i = 0; i < BUDGET; ++i) {
        auto* work = ipc_dev_op_queue_pop_front_locked(queue);
        if (ipc_dev_op_work_is_fenced_locked(work) || endpoint_delivery_ready_locked(work)) {
            s_ipc_dev_op_pool_lock.unlock_irqrestore(IRQF);
            ordered_wait = false;
            return work;
        }
        ipc_dev_op_queue_push_back_locked(queue, work);
    }
    s_ipc_dev_op_pool_lock.unlock_irqrestore(IRQF);
    ordered_wait = BUDGET != 0;
    return nullptr;
}

auto try_enqueue_ipc_dev_op_work_rx(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, uint32_t resource_id,
                                    WkiChannel* rx_channel, uint32_t rx_channel_generation) -> bool {
    if (hdr == nullptr || payload == nullptr || payload_len > WKI_ETH_MAX_PAYLOAD || rx_channel == nullptr || rx_channel_generation == 0 ||
        !s_ipc_lock.try_lock()) {
        return false;
    }
    IpcPeerCleanupEpoch* const PEER_EPOCH = ipc_peer_cleanup_slot_locked(hdr->src_node, true);
    if (PEER_EPOCH == nullptr || PEER_EPOCH->cleanup_active || !s_ipc_dev_op_pool_lock.try_lock()) {
        s_ipc_lock.unlock();
        return false;
    }

    size_t const WORKER_INDEX = ipc_dev_op_worker_index(resource_id);
    if (g_ipc_dev_op_worker_tasks.at(WORKER_INDEX) == nullptr) {
        s_ipc_dev_op_pool_lock.unlock();
        s_ipc_lock.unlock();
        return false;
    }

    uint32_t delivery_group = 0;
    DevOpReqPayload req{};
    if (payload_len >= sizeof(req)) {
        std::memcpy(&req, payload, sizeof(req));
        bool const ORDERED_DATA = (req.op_id == OP_PIPE_DATA || req.op_id == OP_PIPE_DATA_FLOW) && req.data_len > sizeof(uint32_t);
        bool const SOCKET_CLOSE = req.op_id == OP_SOCK_CLOSE && req.data_len >= sizeof(uint32_t);
        if ((ORDERED_DATA || SOCKET_CLOSE) && sizeof(req) + req.data_len <= payload_len) {
            auto* exp = find_export_by_resource_id(resource_id);
            if (ipc_export_matches_consumer(exp, resource_id, hdr->src_node) && exp->file != nullptr &&
                (!SOCKET_CLOSE || exp->res_type == ResourceType::IPC_SOCKET))
                delivery_group = exp->endpoint_delivery_group;
            if (req.op_id == OP_PIPE_DATA && delivery_group != 0) {
                // Opposite-direction resource IDs may collide. A readable
                // proxy consumes legacy DATA before the home-side exporter.
                for (const auto* proxy : g_ipc_proxies) {
                    if (proxy->active.load(std::memory_order_acquire) && proxy->home_node == hdr->src_node &&
                        proxy->resource_id == resource_id && proxy->can_receive_data) {
                        delivery_group = 0;
                        break;
                    }
                }
            }
        }
    }
    IpcDevOpWork* work = take_ipc_dev_op_work_locked(payload_len, delivery_group);
    if (work == nullptr) {
        s_ipc_dev_op_pool_lock.unlock();
        s_ipc_lock.unlock();
        return false;
    }

    work->hdr = *hdr;
    work->channel_identity = {
        .channel = rx_channel,
        .peer_node_id = hdr->src_node,
        .channel_id = hdr->channel_id,
        .generation = rx_channel_generation,
    };
    work->cleanup_epoch = PEER_EPOCH->epoch;
    std::memcpy(work->payload, payload, payload_len);
    auto& queue = g_ipc_dev_op_queues.at(WORKER_INDEX);
    ipc_dev_op_queue_push_back_locked(queue, work);

    s_ipc_dev_op_pool_lock.unlock();
    s_ipc_lock.unlock();
    return true;
}

[[noreturn]] void ipc_dev_op_worker_thread_fn(size_t worker_index) {
    for (;;) {
        IpcDevOpWork* work = nullptr;
        bool claimed = false;
        bool ordered_wait = false;
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            auto& queue = g_ipc_dev_op_queues.at(worker_index);
            work = take_ready_ipc_dev_op_work_locked(queue, ordered_wait);
            if (work != nullptr && !ipc_dev_op_work_is_fenced_locked(work)) {
                claimed = claim_ipc_dev_op_worker_locked(work);
            }
            s_ipc_lock.unlock_irqrestore(IRQF);
        }

        if (ordered_wait) {
            // Every queued work still has an earlier alias in flight. Revisit
            // after the backlog workers have had a chance to finish delivery.
            ker::mod::sched::kern_sleep_us(WKI_IPC_PIPE_WRITE_RETRY_US);
            continue;
        }
        if (work == nullptr) {
            ker::mod::sched::kern_block();
            continue;
        }

        uint16_t const SRC_NODE = work->hdr.src_node;
        if (!claimed || !wki_channel_generation_is_live(work->channel_identity.channel, work->channel_identity.peer_node_id,
                                                        work->channel_identity.channel_id, work->channel_identity.generation)) {
            if (claimed) {
                uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                release_ipc_dev_op_worker_locked(SRC_NODE);
                s_ipc_lock.unlock_irqrestore(IRQF);
            }
            free_ipc_dev_op_work(work);
            continue;
        }

        auto const CORRELATION = static_cast<uint32_t>(work->hdr.seq_num & UINT16_MAX);
        IpcPerfTrace trace(ker::mod::perf::WkiPerfIpcOp::DEV_OP_HANDLE, work->hdr.src_node, work->hdr.channel_id, WOS_PERF_CALLSITE(), 0,
                           CORRELATION);
        // The handler may publish the coallocated payload and clear work.
        // Keep all post-handler bookkeeping independent of the work object.
        DevOpReqPayload request{};
        if (work->payload_len >= sizeof(request)) std::memcpy(&request, work->payload, sizeof(request));
        bool const RETRY_AT_BACK = request.op_id == OP_SOCK_SHUTDOWN;
        bool retry = false;
        handle_ipc_dev_op_req_inline(&work->hdr, work->payload, work->payload_len, &work, &retry, work->channel_identity);
        bool requeued = false;
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            release_ipc_dev_op_worker_locked(SRC_NODE);
            if (retry && work != nullptr && !ipc_dev_op_work_is_fenced_locked(work)) {
                if (RETRY_AT_BACK)
                    ipc_dev_op_queue_push_back_locked(g_ipc_dev_op_queues.at(worker_index), work);
                else
                    ipc_dev_op_queue_push_front_locked(g_ipc_dev_op_queues.at(worker_index), work);
                requeued = true;
            }
            s_ipc_lock.unlock_irqrestore(IRQF);
        }
        trace.finish(requeued ? -EAGAIN : 0);
        if (requeued) {
            ker::mod::sched::kern_sleep_us(WKI_IPC_PIPE_WRITE_RETRY_US);
            continue;
        }
        free_ipc_dev_op_work(work);
    }
}

[[noreturn]] void ipc_dev_op_worker_thread_0() { ipc_dev_op_worker_thread_fn(0); }
[[noreturn]] void ipc_dev_op_worker_thread_1() { ipc_dev_op_worker_thread_fn(1); }
[[noreturn]] void ipc_dev_op_worker_thread_2() { ipc_dev_op_worker_thread_fn(2); }
[[noreturn]] void ipc_dev_op_worker_thread_3() { ipc_dev_op_worker_thread_fn(3); }

[[noreturn]] void proxy_pipe_close_tx_thread_fn() {
    for (;;) {
        PendingProxyPipeClose* pending = nullptr;
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            if (!g_pending_proxy_pipe_closes.empty()) {
                pending = g_pending_proxy_pipe_closes.front();
                g_pending_proxy_pipe_closes.pop_front();
            }
            s_ipc_lock.unlock_irqrestore(IRQF);
        }

        if (pending == nullptr) {
            ker::mod::sched::kern_block();
            continue;
        }

        int const RET = send_pending_proxy_pipe_close(*pending);
        if (RET == WKI_OK) {
            free_pending_proxy_pipe_close(pending);
            continue;
        }

        pending->attempts++;
        if (proxy_pipe_close_tx_terminal(RET)) {
            ker::mod::dbg::log("[WKI] IPC proxy pipe close dropped: resource_id=%u op=%u ret=%d attempts=%u", pending->resource_id,
                               pending->op_id, RET, pending->attempts);
            free_pending_proxy_pipe_close(pending);
            continue;
        }

        uint64_t const SLEEP_US = ipc_pipe_send_retry_sleep_us(RET, pending->attempts);
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            g_pending_proxy_pipe_closes.push_back(pending);
            s_ipc_lock.unlock_irqrestore(IRQF);
        }
        ker::mod::sched::kern_sleep_us(SLEEP_US);
    }
}

}  // namespace

void wki_ipc_send_proxy_close(ProxyIpcState* proxy, const uint8_t* msg, uint16_t msg_size, uint32_t resource_id, uint16_t op_id) {
    send_proxy_pipe_close(proxy, msg, msg_size, resource_id, op_id);
}

// =============================================================================
// Public API: Initialization
// =============================================================================

void wki_ipc_subsystem_init() {
    if (g_ipc_initialized || g_ipc_consumer_stopping.load(std::memory_order_acquire) ||
        ker::mod::sched::kernel_threads_shutdown_requested()) {
        return;
    }
    g_ipc_initialized = true;
    init_ipc_dev_op_work_pool();
    init_pipe_pump_workers();
    using WorkerEntry = void (*)();
    constexpr std::array<WorkerEntry, WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_COUNT> FLUSH_WORKER_ENTRIES = {
        export_pipe_write_flush_thread_0,
        export_pipe_write_flush_thread_1,
        export_pipe_write_flush_thread_2,
        export_pipe_write_flush_thread_3,
    };
    uint64_t const START_IRQF = s_ipc_lock.lock_irqsave();
    g_ipc_flush_workers_starting = true;
    s_ipc_lock.unlock_irqrestore(START_IRQF);
    for (size_t i = 0; i < FLUSH_WORKER_ENTRIES.size(); ++i) {
        if (g_ipc_consumer_stopping.load(std::memory_order_acquire) || ker::mod::sched::kernel_threads_shutdown_requested()) break;
        auto* worker =
            ker::mod::sched::task::Task::create_kernel_thread(WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_NAMES.at(i), FLUSH_WORKER_ENTRIES.at(i));
        if (worker != nullptr) {
            if (!worker->try_acquire_lifetime_ref()) {
                ker::mod::sched::task::destroy_unpublished_user_thread(worker);
                continue;
            }
            promote_latency_sensitive_daemon(worker);
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            bool const PUBLISH =
                !g_ipc_consumer_stopping.load(std::memory_order_acquire) && !ker::mod::sched::kernel_threads_shutdown_requested();
            if (PUBLISH) {
                g_export_pipe_write_flush_tasks.at(i) = worker;
                g_export_pipe_write_flush_stopped.at(i).store(false, std::memory_order_release);
            }
            s_ipc_lock.unlock_irqrestore(IRQF);
            if (!PUBLISH || !ker::mod::sched::post_task_balanced(worker)) {
                uint64_t const FAIL_IRQF = s_ipc_lock.lock_irqsave();
                g_export_pipe_write_flush_tasks.at(i) = nullptr;
                g_export_pipe_write_flush_stopped.at(i).store(true, std::memory_order_release);
                s_ipc_lock.unlock_irqrestore(FAIL_IRQF);
                worker->release();
                ker::mod::sched::task::destroy_unpublished_user_thread(worker);
            }
        } else {
            ker::mod::dbg::log("[WKI] IPC export pipe writer thread creation failed: shard=%u", static_cast<unsigned>(i));
        }
    }
    uint64_t const FINISH_IRQF = s_ipc_lock.lock_irqsave();
    g_ipc_flush_workers_starting = false;
    s_ipc_lock.unlock_irqrestore(FINISH_IRQF);
    if (g_ipc_consumer_stopping.load(std::memory_order_acquire) || ker::mod::sched::kernel_threads_shutdown_requested()) return;

    constexpr std::array<WorkerEntry, WKI_IPC_DEV_OP_WORKER_COUNT> DEV_OP_WORKER_ENTRIES = {
        ipc_dev_op_worker_thread_0,
        ipc_dev_op_worker_thread_1,
        ipc_dev_op_worker_thread_2,
        ipc_dev_op_worker_thread_3,
    };
    constexpr std::array<const char*, WKI_IPC_DEV_OP_WORKER_COUNT> DEV_OP_WORKER_NAMES = {
        "wki_ipc_dev0",
        "wki_ipc_dev1",
        "wki_ipc_dev2",
        "wki_ipc_dev3",
    };
    for (size_t i = 0; i < WKI_IPC_DEV_OP_WORKER_COUNT; ++i) {
        auto* worker = ker::mod::sched::task::Task::create_kernel_thread(DEV_OP_WORKER_NAMES.at(i), DEV_OP_WORKER_ENTRIES.at(i));
        g_ipc_dev_op_worker_tasks.at(i) = worker;
        if (worker != nullptr) {
            promote_latency_sensitive_daemon(worker);
            ker::mod::sched::post_task_balanced(worker);
        } else {
            ker::mod::dbg::log("[WKI] IPC dev-op worker thread creation failed: index=%u", static_cast<unsigned>(i));
        }
    }
    g_proxy_pipe_close_tx_task = ker::mod::sched::task::Task::create_kernel_thread("wki_ipc_close_tx", proxy_pipe_close_tx_thread_fn);
    if (g_proxy_pipe_close_tx_task != nullptr) {
        promote_latency_sensitive_daemon(g_proxy_pipe_close_tx_task);
        ker::mod::sched::post_task_balanced(g_proxy_pipe_close_tx_task);
    } else {
        ker::mod::dbg::log("[WKI] IPC close-tx worker thread creation failed");
    }
    ker::mod::dbg::log("[WKI] IPC proxy subsystem initialized");
}

auto wki_ipc_secure_shutdown_drain() -> bool {
    bool expected = false;
    if (!g_ipc_shutdown_drain_owner.compare_exchange_strong(expected, true, std::memory_order_acquire)) return false;
    IpcShutdownDrainOwner const OWNER;
    g_ipc_consumer_stopping.store(true, std::memory_order_release);
    uint64_t irqf = s_ipc_lock.lock_irqsave();
    bool stopped = !g_ipc_flush_workers_starting;
    for (size_t shard = 0; shard < g_export_pipe_write_flush_tasks.size(); ++shard)
        if (g_export_pipe_write_flush_tasks.at(shard) != nullptr ||
            !g_export_pipe_write_flush_stopped.at(shard).load(std::memory_order_acquire))
            stopped = false;
    for (auto const& arg : g_pump_args)
        if (arg.worker != nullptr || arg.starting || !arg.stopped.load(std::memory_order_acquire) ||
            arg.exp.load(std::memory_order_acquire) != nullptr)
            stopped = false;
    s_ipc_lock.unlock_irqrestore(irqf);
    if (!stopped) {
        wake_all_export_pipe_write_flush_workers();
        // Snapshot each retained pump publication separately; no wake under IPC.
        for (auto& arg : g_pump_args) {
            irqf = s_ipc_lock.lock_irqsave();
            auto* worker = arg.worker != nullptr && arg.worker->try_acquire_lifetime_ref() ? arg.worker : nullptr;
            s_ipc_lock.unlock_irqrestore(irqf);
            wake_pipe_pump(worker);
        }
        return false;
    }
    // All sole shard owners have retired; peer cleanup detached these queues.
    for (size_t count = 0; count < WKI_IPC_MAX_EXPORTS; ++count) {
        irqf = s_ipc_lock.lock_irqsave();
        ExportPipeWriteBacklog* backlog = nullptr;
        for (auto* candidate : g_export_pipe_write_backlogs)
            if (candidate != nullptr && (candidate->exp == nullptr || (!candidate->exp->active && !candidate->exp->pipe_write_in_flight &&
                                                                       !candidate->exp->pipe_secure_reserved))) {
                backlog = candidate;
                break;
            }
        if (backlog != nullptr) erase_export_pipe_write_backlog_locked(backlog);
        auto* retired = compact_inactive_exports_locked();
        s_ipc_lock.unlock_irqrestore(irqf);
        free_export_pipe_write_backlog(backlog);
        free_retired_exports(retired);
        if (backlog == nullptr) break;
    }
    service_secure_retired_exports();
    irqf = s_ipc_lock.lock_irqsave();
    bool drained = g_secure_retired_exports == nullptr && g_ipc_exports.size() == 0 && g_export_pipe_write_backlogs.size() == 0 &&
                   g_pipe_pump_queue_head == nullptr && g_pipe_pump_queue_tail == nullptr;
    for (auto* exp : g_ipc_exports)
        if (exp != nullptr && (exp->pipe_secure_descriptor.rkey != 0 || exp->pipe_secure_reserved || exp->pipe_secure_file != nullptr))
            drained = false;
    s_ipc_lock.unlock_irqrestore(irqf);
    return drained;
}

auto wki_ipc_admit_dev_op_rx(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                             uint32_t rx_channel_generation) -> WkiIpcDevOpRxAdmission {
    if (hdr == nullptr || payload == nullptr || rx_channel == nullptr || rx_channel_generation == 0 ||
        payload_len < sizeof(DevOpReqPayload)) {
        return WkiIpcDevOpRxAdmission::NOT_APPLICABLE;
    }

    DevOpReqPayload req = {};
    std::memcpy(&req, payload, sizeof(req));
    if (req.op_id < 0x0700 || req.op_id > 0x07FF) {
        return WkiIpcDevOpRxAdmission::NOT_APPLICABLE;
    }
    if (req.data_len < sizeof(uint32_t) || sizeof(DevOpReqPayload) + req.data_len > payload_len) {
        return WkiIpcDevOpRxAdmission::DISCARD;
    }

    // Legacy epoll control is a bounded ENOTSUP reply. It performs no FD
    // lookup, object mutation or allocation in the RX context.
    if (req.op_id == OP_EPOLL_CTL) {
        return WkiIpcDevOpRxAdmission::INLINE;
    }

    uint32_t resource_id = 0;
    std::memcpy(&resource_id, payload + sizeof(DevOpReqPayload), sizeof(resource_id));
    if (!try_enqueue_ipc_dev_op_work_rx(hdr, payload, payload_len, resource_id, rx_channel, rx_channel_generation)) {
        return WkiIpcDevOpRxAdmission::RETRY;
    }
    return WkiIpcDevOpRxAdmission::DEFERRED;
}

void wki_ipc_notify_deferred_dev_op(const uint8_t* payload, uint16_t payload_len) {
    if (payload == nullptr || payload_len < sizeof(DevOpReqPayload) + sizeof(uint32_t)) {
        return;
    }
    uint32_t resource_id = 0;
    std::memcpy(&resource_id, payload + sizeof(DevOpReqPayload), sizeof(resource_id));
    auto* worker = g_ipc_dev_op_worker_tasks.at(ipc_dev_op_worker_index(resource_id));
    if (worker != nullptr) {
        ker::mod::sched::wake_task_from_event(worker);
    }
}

void wki_ipc_cleanup_exported_fds(const WkiIpcFdEntry* map, uint16_t count, uint16_t consumer_node, const WkiIpcChannels& channels) {
    if (map == nullptr || count == 0 || consumer_node == WKI_NODE_INVALID) {
        return;
    }

    std::array<ker::mod::sched::task::Task*, WKI_IPC_MAX_EXPORTS> stopped_pumps = {};
    std::array<ker::vfs::File*, WKI_IPC_MAX_EXPORTS> released_files = {};
    struct Discard {
        uint32_t resource_id;
        WkiChannelIdentity channel;
    };
    std::array<Discard, WKI_IPC_MAX_EXPORTS> direct_discards = {};
    size_t stopped_pump_count = 0;
    size_t released_file_count = 0;
    size_t direct_discard_count = 0;
    bool wake_export_pipe_flush = false;
    WkiIpcExport* retired_exports = nullptr;

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    for (uint16_t i = 0; i < count; ++i) {
        auto const& entry = map[i];
        auto* exp = find_export_by_resource_id(entry.resource_id);
        if (exp == nullptr) {
            if (channels.data.peer_node_id == consumer_node && direct_discard_count < direct_discards.size()) {
                direct_discards.at(direct_discard_count++) = {entry.resource_id, channels.data};
            }
            continue;
        }
        if (exp->consumer_node != consumer_node ||
            (channels.control.channel != nullptr && !ipc_channel_identity_equal(exp->channels.control, channels.control)))
            continue;

        auto* pump_task = stop_pipe_pump_locked(exp);
        if (pump_task != nullptr && stopped_pump_count < stopped_pumps.size()) {
            stopped_pumps.at(stopped_pump_count++) = pump_task;
        } else if (direct_discard_count < direct_discards.size()) {
            // No pump can still emit IPC_DATA messages for this endpoint, so
            // the discard marker may be sent directly after dropping the lock.
            direct_discards.at(direct_discard_count++) = {entry.resource_id, exp->channels.data};
        }

        if (exp->file != nullptr && released_file_count < released_files.size()) {
            released_files.at(released_file_count++) = exp->file;
        }
        exp->file = nullptr;
        exp->active = false;

        for (auto* backlog : g_export_pipe_write_backlogs) {
            if (backlog != nullptr && backlog->exp == exp) {
                backlog->exp = nullptr;
                queue_export_pipe_write_flush_locked(backlog);
                wake_export_pipe_flush = true;
            }
        }
    }
    retired_exports = compact_inactive_exports_locked();
    s_ipc_lock.unlock_irqrestore(IRQF);

    for (auto* pump : std::span(stopped_pumps.data(), stopped_pump_count)) {
        wake_pipe_pump(pump);
    }

    if (wake_export_pipe_flush) {
        wake_all_export_pipe_write_flush_workers();
    }

    // Retired objects no longer own Files. Free them before a last File put
    // can park; single-entry submission cancellation then carries no abandoned
    // retired chain across that callback.
    free_retired_exports(retired_exports);
    for (auto* file : std::span(released_files.data(), released_file_count)) {
        ipc_release_file_ref(file);
    }
    for (auto const& discard : std::span(direct_discards.data(), direct_discard_count)) {
        schedule_pending_pipe_discard(discard.channel, discard.resource_id);
    }
}

void wki_ipc_get_perf_snapshot(WkiIpcPerfSnapshot& out) {
    WkiIpcPerfSnapshot snapshot{};

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();

    for (const auto* exp : g_ipc_exports) {
        if (exp == nullptr || !exp->active) {
            continue;
        }
        snapshot.exports++;
        if (exp->pump_running.load(std::memory_order_acquire)) {
            snapshot.pump_tasks++;
        }
    }

    for (auto* proxy : g_ipc_proxies) {
        if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
            continue;
        }
        snapshot.proxies++;
        uint64_t const PROXY_IRQF = proxy->lock.lock_irqsave();
        if (proxy->ring_buf != nullptr) {
            snapshot.proxy_ring_bytes += proxy->ring_capacity;
            snapshot.proxy_ring_used_bytes += proxy_ring_used_bytes(proxy);
        }
        if (proxy->blocked_reader.load(std::memory_order_acquire) != nullptr ||
            std::ranges::any_of(proxy->read_waiters, [](uint64_t pid) { return pid != 0; })) {
            snapshot.blocked_readers++;
        }
        snapshot.poll_waiters += proxy->poll_waiters.size();
        proxy->lock.unlock_irqrestore(PROXY_IRQF);
    }

    for (const auto* pending : g_pending_pipe_deliveries) {
        if (pending == nullptr) {
            continue;
        }
        snapshot.pending_deliveries++;
        snapshot.pending_bytes += pending->buffered_bytes;
        snapshot.pending_chunks += pending->chunks.size();
    }

    for (const auto* backlog : g_export_pipe_write_backlogs) {
        if (backlog == nullptr) {
            continue;
        }
        snapshot.export_backlogs++;
        snapshot.export_backlog_bytes += backlog->buffered_bytes;
        snapshot.export_backlog_chunks += backlog->chunks.size();
        if (backlog->close_pending) {
            snapshot.export_close_pending++;
            const auto* exp = backlog->exp;
            if (backlog->close_has_expected_bytes && exp != nullptr && exp->pipe_bytes_received < backlog->close_expected_bytes) {
                snapshot.export_close_waiting_for_bytes++;
            }
        }
    }

    snapshot.export_flush_queue = export_pipe_write_flush_queue_depth_locked();
    snapshot.proxy_close_queue = g_pending_proxy_pipe_closes.size();
    for (const auto* pending_close : g_pending_proxy_pipe_closes) {
        if (pending_close != nullptr) {
            snapshot.proxy_close_attempts += pending_close->attempts;
        }
    }
    for (const auto& queue : g_ipc_dev_op_queues) {
        snapshot.dev_op_queue += queue.count;
        for (const auto* work = queue.head; work != nullptr; work = work->next) {
            snapshot.dev_op_payload_bytes += work->payload_len;
        }
    }

    snapshot.proxy_write_payload_bytes = g_proxy_write_payload_bytes.load(std::memory_order_relaxed);
    snapshot.proxy_write_no_credit_waits = g_proxy_write_no_credit_waits.load(std::memory_order_relaxed);
    snapshot.proxy_write_block_us = g_proxy_write_block_us.load(std::memory_order_relaxed);
    snapshot.proxy_pipe_rdma_full_waits = g_proxy_pipe_rdma_full_waits.load(std::memory_order_relaxed);
    snapshot.proxy_ring_full_waits = g_proxy_ring_full_waits.load(std::memory_order_relaxed);
    snapshot.proxy_ring_full_bytes = g_proxy_ring_full_bytes.load(std::memory_order_relaxed);
    snapshot.pipe_payload_bytes = g_pipe_payload_bytes.load(std::memory_order_relaxed);

    snapshot.approx_alloc_bytes = (snapshot.exports * sizeof(WkiIpcExport)) + (snapshot.proxies * sizeof(ProxyIpcState)) +
                                  snapshot.proxy_ring_bytes + (snapshot.pending_deliveries * sizeof(PendingPipeDelivery)) +
                                  (snapshot.pending_chunks * sizeof(PendingPipeChunk)) + snapshot.pending_bytes +
                                  (snapshot.export_backlogs * sizeof(ExportPipeWriteBacklog)) +
                                  (snapshot.export_backlog_chunks * sizeof(PendingPipeChunk)) + snapshot.export_backlog_bytes +
                                  (snapshot.proxy_close_queue * sizeof(PendingProxyPipeClose)) +
                                  (snapshot.dev_op_queue * sizeof(IpcDevOpWork)) + snapshot.dev_op_payload_bytes;

    s_ipc_lock.unlock_irqrestore(IRQF);

    out = snapshot;
}

auto wki_ipc_diag_snapshot(WkiIpcDiagRow* rows, size_t capacity, WkiIpcDiagCounts* counts) -> size_t {
    WkiIpcDiagCounts local_counts{};
    size_t row_count = 0;

    auto append_row = [&](const WkiIpcDiagRow& row) {
        if (rows != nullptr && row_count < capacity) {
            rows[row_count] = row;  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            row_count++;
        } else {
            local_counts.truncated++;
        }
    };

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();

    for (const auto* exp : g_ipc_exports) {
        if (exp == nullptr) {
            continue;
        }

        local_counts.exports++;
        local_counts.active_exports += exp->active ? 1U : 0U;
        WkiIpcDiagRow row{};
        row.kind = WkiIpcDiagKind::EXPORT;
        row.resource_id = exp->resource_id;
        row.peer_node = exp->consumer_node;
        row.assigned_channel = exp->assigned_channel;
        row.res_type = static_cast<uint16_t>(exp->res_type);
        row.active = exp->active;
        row.has_file = exp->file != nullptr;
        row.open_flags = exp->file != nullptr ? exp->file->open_flags : 0;
        row.pump_running = exp->pump_running.load(std::memory_order_acquire);
        row.pump_queued = exp->pump_queued;
        row.has_pump_task = exp->pump_task != nullptr;
        row.pipe_bytes_received = exp->pipe_bytes_received;
        row.pipe_rdma_enabled = exp->pipe_rdma_enabled;
        row.cleanup_epoch = ipc_peer_cleanup_epoch_locked(exp->consumer_node);
        row.cleanup_active = ipc_peer_cleanup_active_locked(exp->consumer_node);
        append_row(row);
    }

    for (auto* proxy : g_ipc_proxies) {
        if (proxy == nullptr) {
            continue;
        }

        local_counts.proxies++;
        bool const ACTIVE = proxy->active.load(std::memory_order_acquire);
        local_counts.active_proxies += ACTIVE ? 1U : 0U;
        WkiIpcDiagRow row{};
        row.kind = WkiIpcDiagKind::PROXY;
        row.resource_id = proxy->resource_id;
        row.peer_node = proxy->home_node;
        row.assigned_channel = proxy->assigned_channel;
        row.res_type = static_cast<uint16_t>(proxy->res_type);
        row.active = ACTIVE;
        row.proxy_bytes_written = proxy->bytes_written.load(std::memory_order_acquire);
        row.refcount = proxy->refcount.load(std::memory_order_acquire);
        row.pipe_rdma_enabled = proxy->pipe_rdma_enabled;
        row.pipe_rdma_writer_active = proxy->pipe_rdma_writer_active.load(std::memory_order_acquire);
        row.cleanup_epoch = ipc_peer_cleanup_epoch_locked(proxy->home_node);
        row.cleanup_active = ipc_peer_cleanup_active_locked(proxy->home_node);

        uint64_t const PROXY_IRQF = proxy->lock.lock_irqsave();
        row.write_closed = proxy->write_closed.load(std::memory_order_acquire) != 0U;
        row.ring_capacity = proxy->ring_capacity;
        row.ring_used = proxy->ring_buf != nullptr ? proxy_ring_used_bytes(proxy) : 0;
        auto* reader = proxy->blocked_reader.load(std::memory_order_acquire);
        row.blocked_reader_pid = reader != nullptr ? reader->pid : 0;
        if (row.blocked_reader_pid == 0) {
            for (auto pid : proxy->read_waiters) {
                if (pid != 0) {
                    row.blocked_reader_pid = pid;
                    break;
                }
            }
        }
        row.poll_waiters = proxy->poll_waiters.size();
        row.has_pending_wait = proxy->pending_wait != nullptr;
        row.pending_wait_op = proxy->pending_wait_op;
        row.pending_wait_cookie = proxy->pending_wait_cookie;
        row.message_write_credits = proxy->message_write_credits.load(std::memory_order_acquire);
        row.message_write_error = proxy->message_write_error.load(std::memory_order_acquire);
        row.message_write_waiters = static_cast<uint32_t>(proxy->message_write_waiters.size());
        proxy->lock.unlock_irqrestore(PROXY_IRQF);

        append_row(row);
    }

    for (const auto* backlog : g_export_pipe_write_backlogs) {
        if (backlog == nullptr) {
            continue;
        }

        local_counts.export_backlogs++;
        WkiIpcDiagRow row{};
        row.kind = WkiIpcDiagKind::EXPORT_BACKLOG;
        row.resource_id = backlog->resource_id;
        row.backlog_bytes = backlog->buffered_bytes;
        row.backlog_chunks = backlog->chunks.size();
        row.close_pending = backlog->close_pending;
        row.close_has_expected_bytes = backlog->close_has_expected_bytes;
        row.close_expected_bytes = backlog->close_expected_bytes;
        if (backlog->exp != nullptr) {
            row.peer_node = backlog->exp->consumer_node;
            row.assigned_channel = backlog->exp->assigned_channel;
            row.res_type = static_cast<uint16_t>(backlog->exp->res_type);
            row.active = backlog->exp->active;
            row.has_file = backlog->exp->file != nullptr;
            row.pipe_bytes_received = backlog->exp->pipe_bytes_received;
        }
        append_row(row);
    }

    for (const auto* pending_close : g_pending_proxy_pipe_closes) {
        if (pending_close == nullptr) {
            continue;
        }

        local_counts.proxy_close_queue++;
        WkiIpcDiagRow row{};
        row.kind = WkiIpcDiagKind::PROXY_CLOSE;
        row.resource_id = pending_close->resource_id;
        row.peer_node = pending_close->home_node;
        row.op_id = pending_close->op_id;
        row.msg_size = pending_close->msg_size;
        row.attempts = pending_close->attempts;
        append_row(row);
    }

    for (const auto* pending : g_pending_pipe_deliveries) {
        if (pending == nullptr) {
            continue;
        }
        local_counts.pending_deliveries++;
        WkiIpcDiagRow row{};
        row.kind = WkiIpcDiagKind::PENDING_DELIVERY;
        row.peer_node = pending->home_node;
        row.resource_id = pending->resource_id;
        row.backlog_bytes = pending->buffered_bytes;
        row.backlog_chunks = pending->chunks.size();
        row.write_closed = pending->write_closed;
        row.cleanup_epoch = ipc_peer_cleanup_epoch_locked(pending->home_node);
        row.cleanup_active = ipc_peer_cleanup_active_locked(pending->home_node);
        append_row(row);
    }

    for (const auto& queue : g_ipc_dev_op_queues) {
        for (const auto* work = queue.head; work != nullptr; work = work->next) {
            local_counts.dev_op_work++;
            WkiIpcDiagRow row{};
            row.kind = WkiIpcDiagKind::DEV_OP_WORK;
            row.peer_node = work->hdr.src_node;
            row.assigned_channel = work->hdr.channel_id;
            row.msg_size = work->payload_len;
            row.sequence = work->hdr.seq_num;
            row.cleanup_epoch = work->cleanup_epoch;
            row.cleanup_active = ipc_peer_cleanup_active_locked(work->hdr.src_node);
            row.fenced = ipc_dev_op_work_is_fenced_locked(work);
            append_row(row);
        }
    }

    for (const auto& peer : g_ipc_peer_cleanup_epochs) {
        if (!peer.used) {
            continue;
        }
        local_counts.peer_cleanup_slots++;
        WkiIpcDiagRow row{};
        row.kind = WkiIpcDiagKind::PEER_CLEANUP;
        row.peer_node = peer.node_id;
        row.cleanup_epoch = peer.epoch;
        row.cleanup_active = peer.cleanup_active;
        append_row(row);
    }

    s_ipc_lock.unlock_irqrestore(IRQF);

    if (counts != nullptr) {
        *counts = local_counts;
    }
    return row_count;
}

auto wki_ipc_proxy_register_poll_waiter(ProxyIpcState* proxy, uint64_t pid) -> bool {
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
        return false;
    }

    uint64_t const IRQF = proxy->lock.lock_irqsave();
    bool const OK = proxy->active.load(std::memory_order_acquire) && proxy_register_waiter_locked(proxy->poll_waiters, pid);
    proxy->lock.unlock_irqrestore(IRQF);
    return OK;
}

void wki_ipc_proxy_wake_poll_waiters(ProxyIpcState* proxy) {
    if (proxy == nullptr) {
        return;
    }

    while (true) {
        std::array<uint64_t, WKI_IPC_MAX_POLL_WAKE_WAITERS> pending_waiters = {};
        size_t pending_waiter_count = 0;

        uint64_t const IRQF = proxy->lock.lock_irqsave();
        if (!proxy->poll_waiters.empty()) {
            proxy_collect_waiters_locked(proxy->poll_waiters, pending_waiters, &pending_waiter_count);
        }
        proxy->lock.unlock_irqrestore(IRQF);

        if (pending_waiter_count == 0) {
            return;
        }
        proxy_reschedule_waiters(pending_waiters, pending_waiter_count);
    }
}

// =============================================================================
// Public API: Export task fds for remote submission
// =============================================================================

WkiIpcSubmissionGuard::WkiIpcSubmissionGuard(ker::mod::sched::task::Task* source, const WkiIpcFdEntry* map, WkiIpcTaskFdHandoff* handoff,
                                             uint16_t& count, uint16_t consumer, const WkiIpcChannels& channels)
    : source(source), map(map), handoff(handoff), count(count), consumer(consumer), original_channels(channels) {
    arm();
}
WkiIpcSubmissionGuard::~WkiIpcSubmissionGuard() { cancel(); }

void WkiIpcSubmissionGuard::arm() {
    if (owns) return;
    owns = true;
    handoff_cursor = 0;
    export_cursor = 0;
    executor = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : source;
    if (executor == nullptr) executor = source;
    if (executor == nullptr) return;
    next = executor->pending_wki_submissions;
    if (next != nullptr) next->previous = this;
    executor->pending_wki_submissions = this;
}

void WkiIpcSubmissionGuard::disarm() {
    owns = false;
    if (executor != nullptr) {
        if (previous != nullptr)
            previous->next = next;
        else
            executor->pending_wki_submissions = next;
        if (next != nullptr) next->previous = previous;
    }
    executor = nullptr;
    previous = nullptr;
    next = nullptr;
}

void WkiIpcSubmissionGuard::cancel() {
    // Clear one ownership before any put/cleanup that may block. Remaining
    // entries stay reachable if fatal exit re-enters and abandons this frame.
    uint16_t const COUNT = std::min<uint16_t>(count, 16);
    while (owns && handoff != nullptr && handoff_cursor < COUNT) {
        auto& entry = handoff[handoff_cursor++];
        auto* file = entry.file;
        entry = {};
        if (file != nullptr) ipc_release_file_ref(file);
    }
    while (owns && map != nullptr && export_cursor < COUNT) {
        auto const entry = map[export_cursor++];
        wki_ipc_cleanup_exported_fds(&entry, 1, consumer, original_channels);
    }
    disarm();
}

void wki_ipc_cancel_submission_guards(ker::mod::sched::task::Task& executor) {
    while (executor.pending_wki_submissions != nullptr) executor.pending_wki_submissions->cancel();
}

class IpcFdSnapshot {
   public:
    explicit IpcFdSnapshot(ker::mod::sched::task::Task& source) {
        executor = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : &source;
        if (executor == nullptr) executor = &source;
        next = executor->pending_wki_fd_snapshots;
        if (next != nullptr) next->previous = this;
        executor->pending_wki_fd_snapshots = this;
        uint64_t const FLAGS = source.fd_table_lock.lock_irqsave();
        source.fd_table.for_each([&](uint64_t fd, void* value) {
            if (value == nullptr || source.get_fd_cloexec(static_cast<unsigned>(fd))) return;
            auto* file = static_cast<ker::vfs::File*>(value);
            file->refcount.fetch_add(1, std::memory_order_acq_rel);
            files[fd] = file;
        });
        source.fd_table_lock.unlock_irqrestore(FLAGS);
    }
    ~IpcFdSnapshot() { cancel(); }
    IpcFdSnapshot(const IpcFdSnapshot&) = delete;
    auto operator=(const IpcFdSnapshot&) -> IpcFdSnapshot& = delete;
    auto prepare_exports(uint16_t count, int fail_at = -1) -> int {
        if (count > prepared_exports.size()) return -EINVAL;
        for (unsigned i = 0; i < count; ++i) {
            if (static_cast<int>(i) == fail_at) return -ENOMEM;
            prepared_exports[i] = new (std::nothrow) WkiIpcExport();
            if (prepared_exports[i] == nullptr) return -ENOMEM;
        }
        return 0;
    }
    void cancel() {
        // These objects have never entered the registry or acquired File
        // ownership. Keep private preparation reachable from fatal cleanup.
        for (auto& slot : prepared_backlogs) {
            auto* backlog = slot;
            slot = nullptr;
            delete backlog;
        }
        for (auto& slot : prepared_exports) {
            auto* exp = slot;
            slot = nullptr;
            if (exp == nullptr) continue;
            exp->active = false;
            free_retired_exports(exp);
        }
        // Keep the node reachable while puts may block. A fatal exit can
        // re-enter cleanup and drain the remaining pins without double puts.
        for (auto& slot : files) {
            auto* file = slot;
            slot = nullptr;
            if (file != nullptr) ipc_release_file_ref(file);
        }
        if (executor == nullptr) return;
        if (previous != nullptr)
            previous->next = next;
        else
            executor->pending_wki_fd_snapshots = next;
        if (next != nullptr) next->previous = previous;
        executor = nullptr;
        previous = nullptr;
        next = nullptr;
    }
    std::array<ker::vfs::File*, ker::mod::sched::task::Task::FD_TABLE_SIZE> files{};
    std::array<WkiIpcExport*, 16> prepared_exports{};
    std::array<ExportPipeWriteBacklog*, 16> prepared_backlogs{};

   private:
    ker::mod::sched::task::Task* executor = nullptr;
    IpcFdSnapshot* previous = nullptr;
    IpcFdSnapshot* next = nullptr;
};

void wki_ipc_cancel_fd_snapshots(ker::mod::sched::task::Task& executor) {
    while (executor.pending_wki_fd_snapshots != nullptr) executor.pending_wki_fd_snapshots->cancel();
}

namespace {
constexpr uint16_t MAX_SUBMIT_IPC_FDS = 16;
struct IpcFdExportPlan {
    std::array<ResourceType, ker::mod::sched::task::Task::FD_TABLE_SIZE> types{};
    std::array<const void*, ker::mod::sched::task::Task::FD_TABLE_SIZE> endpoint_identities{};
    std::array<bool, ker::mod::sched::task::Task::FD_TABLE_SIZE> pump_on_accept{};
    uint16_t count = 0;
    uint8_t stdio_mask = 0;
};

auto endpoint_export_delivery_group_locked(const WkiIpcExport& candidate) -> uint32_t {
    if (candidate.endpoint_identity == nullptr) return 0;
    for (const auto* existing : g_ipc_exports) {
        if (existing->active && existing->res_type == candidate.res_type && existing->endpoint_identity == candidate.endpoint_identity &&
            existing->endpoint_delivery_group != 0)
            return existing->endpoint_delivery_group;
    }
    return candidate.resource_id;
}

auto plan_ipc_fd_export(const IpcFdSnapshot& snapshot, IpcFdExportPlan& plan) -> int {
    IpcFdExportPlan candidate;
    candidate.types.fill(ResourceType::CUSTOM);
    std::array<const void*, MAX_SUBMIT_IPC_FDS> pty_identities{};
    size_t pty_identity_count = 0;
    for (unsigned fd = 0; fd < snapshot.files.size(); ++fd) {
        auto* file = snapshot.files[fd];
        if (file == nullptr) continue;
        if (fd < 3) candidate.stdio_mask |= static_cast<uint8_t>(1U << fd);
        ResourceType type = ResourceType::CUSTOM;
        if (ker::vfs::vfs_is_pipe_file(file) || is_proxy_pipe_file(file)) {
            // Export the held endpoint through this node with a fresh resource
            // binding. Preserve its original owner/channel checks in proxy I/O.
            type = ResourceType::IPC_PIPE;
        } else if (ker::vfs::vfs_is_socket_file(file)) {
            type = ResourceType::IPC_SOCKET;
            constexpr uint8_t SOCK_STREAM_TYPE = 1;
            auto* socket = static_cast<const ker::net::Socket*>(file->private_data);
            if (socket != nullptr && socket->type == SOCK_STREAM_TYPE) candidate.endpoint_identities[fd] = socket;
        } else if (ker::vfs::vfs_is_epoll_file(file)) {
            // An epoll interest list retains local File pointers and callbacks.
            return -ENOTSUP;
        } else if (should_proxy_tty_like_file(file)) {
            type = ResourceType::IPC_PTY;
        } else {
            // The legacy null/capture fallback cannot preserve ordinary file
            // I/O or enforce a subsequent FSIZE reduction on regular output.
            if (fd >= 3 || file->fs_type != ker::vfs::FSType::DEVFS) return -ENOTSUP;
            continue;
        }
        if (candidate.count == MAX_SUBMIT_IPC_FDS) return -ENOTSUP;
        candidate.types[fd] = type;
        candidate.pump_on_accept[fd] =
            type == ResourceType::IPC_SOCKET || (type == ResourceType::IPC_PIPE && ipc_fd_access_mode(file->open_flags) == 0);
        if (type == ResourceType::IPC_PTY) {
            const void* identity = file->fops == ker::vfs::devfs::get_devfs_fops() ? ker::dev::pty::pty_file_identity_key(file) : nullptr;
            if (identity == nullptr) identity = file;
            candidate.endpoint_identities[fd] = identity;
            bool seen = false;
            for (size_t i = 0; i < pty_identity_count; ++i) seen |= pty_identities[i] == identity;
            // The snapshot is ordered by FD; choose the first (lowest) alias.
            if (!seen) {
                pty_identities[pty_identity_count++] = identity;
                candidate.pump_on_accept[fd] = true;
            }
        }
        ++candidate.count;
    }
    plan = candidate;
    return 0;
}
}  // namespace

auto wki_ipc_read_stream_proxy(ker::vfs::File* file, void* buffer, size_t count) -> ssize_t {
    return proxy_pipe_read(file, buffer, count, 0);
}

auto wki_ipc_poll_stream_proxy(ker::vfs::File* file, int events) -> int { return proxy_pipe_poll_check(file, events); }

auto wki_ipc_write_stream_proxy(ker::vfs::File* file, const void* buffer, size_t count) -> ssize_t {
    return proxy_pipe_write(file, buffer, count, 0);
}

auto wki_ipc_capture_data_sequence(const WkiChannelIdentity& identity, bool transmit, uint32_t& sequence) -> bool {
    auto* channel = identity.channel;
    if (channel == nullptr || identity.generation == 0 || identity.channel_id != WKI_CHAN_IPC_DATA) return false;
    channel->lock.lock();
    bool const LIVE = channel->active && channel->peer_node_id == identity.peer_node_id && channel->channel_id == identity.channel_id &&
                      channel->generation == identity.generation;
    if (LIVE) sequence = transmit ? channel->tx_seq : channel->rx_dispatch_seq;
    channel->lock.unlock();
    return LIVE;
}

auto wki_ipc_capture_export_channels(uint16_t node) -> WkiIpcChannels {
    auto const control = capture_ipc_channel(node, WKI_CHAN_RESOURCE);
    return {control, wki_ipc_capture_data_channel(control)};
}

auto wki_ipc_exports_match_submit(const WkiIpcFdEntry* map, uint16_t count, uint16_t node, const WkiIpcChannels& channels,
                                  const WkiChannelIdentity& submit) -> bool {
    if (count == 0) return true;
    if (map == nullptr || count > MAX_SUBMIT_IPC_FDS || !ipc_channels_live(channels, node) ||
        !ipc_channel_identity_equal(channels.control, submit))
        return false;
    bool valid = true;
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    valid = !ipc_peer_cleanup_active_locked(node);
    for (uint16_t i = 0; valid && i < count; ++i) {
        auto* exp = find_export_by_resource_id(map[i].resource_id);
        valid = exp != nullptr && exp->active && exp->consumer_node == node &&
                ipc_channel_identity_equal(exp->channels.control, channels.control) &&
                ipc_channel_identity_equal(exp->channels.data, channels.data);
    }
    s_ipc_lock.unlock_irqrestore(IRQF);
    return valid;
}

auto wki_ipc_export_task_fds(ker::mod::sched::task::Task* task, uint16_t target_node, const WkiIpcChannels& channels,
                             WkiIpcFdEntry* map_out, uint16_t* count_out, WkiIpcTaskFdHandoff* handoff_out, uint8_t* stdio_mask_out)
    -> int {
    if (count_out != nullptr) *count_out = 0;
    if (stdio_mask_out != nullptr) *stdio_mask_out = 0;
    if (task == nullptr || map_out == nullptr || count_out == nullptr) {
        return -EINVAL;
    }

    int const SCOPE_ADMISSION = ker::mod::security::compartment::remote_namespace_admission(task);
    if (SCOPE_ADMISSION != 0) return SCOPE_ADMISSION;

    uint16_t count = 0;
    IpcFdSnapshot snapshot(*task);
    IpcFdExportPlan plan;
    int const ADMISSION = plan_ipc_fd_export(snapshot, plan);
    if (ADMISSION != 0) return ADMISSION;
    uint64_t const EPOCH_IRQF = s_ipc_lock.lock_irqsave();
    auto* export_epoch = plan.count != 0 ? ipc_peer_cleanup_slot_locked(target_node, true) : nullptr;
    uint64_t const EXPORT_EPOCH = export_epoch != nullptr ? export_epoch->epoch : 0;
    bool const CLEANING = plan.count != 0 && (export_epoch == nullptr || export_epoch->cleanup_active);
    s_ipc_lock.unlock_irqrestore(EPOCH_IRQF);
    if (plan.count != 0 && (CLEANING || !ipc_channels_live(channels, target_node))) return -EHOSTUNREACH;
    int const PREPARATION = snapshot.prepare_exports(plan.count);
    if (PREPARATION != 0) return PREPARATION;
    std::array<WkiIpcFdEntry, MAX_SUBMIT_IPC_FDS> entries{};
    // IDs may be consumed by an abandoned preparation, but no endpoint or
    // handoff File ownership is visible until every entry is ready.
    uint64_t irqf = s_ipc_lock.lock_irqsave();
    uint32_t first_resource_id = 0;
    bool const IDS_RESERVED = reserve_ipc_resource_ids(g_next_ipc_resource_id, plan.count, first_resource_id);
    s_ipc_lock.unlock_irqrestore(irqf);
    if (!IDS_RESERVED) return -EOVERFLOW;
    for (uint16_t i = 0; i < plan.count; ++i) snapshot.prepared_exports[i]->resource_id = first_resource_id + i;
    for (uint64_t fd_key = 0; fd_key < snapshot.files.size(); ++fd_key) {
        auto* file = snapshot.files[fd_key];
        if (file == nullptr) continue;
        ResourceType const res_type = plan.types[fd_key];
        if (res_type == ResourceType::CUSTOM) continue;
        uint16_t const ACCESS_MODE = ipc_fd_access_mode(file->open_flags);
        auto* exp = snapshot.prepared_exports[count];
        exp->pump_pending_accept = plan.pump_on_accept[fd_key];
        exp->res_type = res_type;
        exp->endpoint_identity = plan.endpoint_identities[fd_key];
        exp->consumer_node = target_node;
        exp->channels = channels;
        exp->assigned_channel = WKI_CHAN_RESOURCE;
        auto& entry = entries[count];
        entry.local_fd = static_cast<uint16_t>(fd_key);
        entry.res_type = static_cast<uint16_t>(res_type);
        entry.resource_id = exp->resource_id;
        entry.home_node = g_wki.my_node_id;
        entry.reserved1 = ipc_fd_export_open_flags(file->open_flags);
        if (res_type == ResourceType::IPC_PIPE && ACCESS_MODE != 0) {
            prepare_export_pipe_rdma(*exp, snapshot.prepared_backlogs[count], file);
            entry.rdma_rkey = exp->pipe_rdma_rkey;
            entry.rdma_offset = exp->pipe_rdma_capacity;
            exp->message_write_flow_control = true;
            if (exp->message_write_flow_control) entry.reserved1 |= WKI_IPC_FD_FLAG_PIPE_FLOW_CONTROL;
        }
        if ((res_type == ResourceType::IPC_PTY || (res_type == ResourceType::IPC_SOCKET && exp->endpoint_identity != nullptr)) &&
            ACCESS_MODE != 0) {
            exp->message_write_flow_control = true;
            entry.reserved1 |= WKI_IPC_FD_FLAG_PIPE_FLOW_CONTROL;
        }
        ++count;
    }

    // All publication is bounded and nonallocating. No private/global exporter
    // pointer is dereferenced after the registry lock is released.
    irqf = s_ipc_lock.lock_irqsave();
    if (count != 0 && (ipc_peer_cleanup_active_locked(target_node) || ipc_peer_cleanup_epoch_locked(target_node) != EXPORT_EPOCH)) {
        s_ipc_lock.unlock_irqrestore(irqf);
        return -EHOSTUNREACH;
    }
    for (uint16_t i = 0; i < count; ++i) {
        auto* exp = snapshot.prepared_exports[i];
        snapshot.prepared_exports[i] = nullptr;
        auto* file = snapshot.files[entries[i].local_fd];
        exp->file = file;
        exp->endpoint_delivery_group = endpoint_export_delivery_group_locked(*exp);
        exp->active = true;
        file->refcount.fetch_add(1, std::memory_order_relaxed);
        if (handoff_out != nullptr) {
            file->refcount.fetch_add(1, std::memory_order_acq_rel);
            handoff_out[i] = WkiIpcTaskFdHandoff{.local_fd = entries[i].local_fd, .file = file};
        }
        g_ipc_exports.push_back(exp);
        auto* backlog = snapshot.prepared_backlogs[i];
        snapshot.prepared_backlogs[i] = nullptr;
        if (backlog != nullptr) g_export_pipe_write_backlogs.push_back(backlog);
        map_out[i] = entries[i];
    }
    if (stdio_mask_out != nullptr) *stdio_mask_out = plan.stdio_mask;
    *count_out = count;
    s_ipc_lock.unlock_irqrestore(irqf);

    return 0;
}

void wki_ipc_activate_task_fds(const WkiIpcFdEntry* map, uint16_t count, uint16_t consumer_node) {
    if (map == nullptr || count > MAX_SUBMIT_IPC_FDS || consumer_node == WKI_NODE_INVALID) return;
    for (const auto& entry : std::span(map, count)) {
        bool wake = false;
        size_t shard = 0;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        auto* exp = find_export_by_resource_id(entry.resource_id);
        if (exp != nullptr && exp->active && exp->consumer_node == consumer_node && exp->pipe_rdma_enabled) {
            exp->pipe_secure_activated = true;
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
        if (wake) wake_export_pipe_write_flush_worker(shard);
        start_pipe_pump(entry.resource_id, consumer_node);
    }
}

void wki_ipc_release_task_fd_handoff(WkiIpcTaskFdHandoff* handoff, uint16_t count) {
    if (handoff == nullptr) {
        return;
    }

    for (uint16_t i = 0; i < count; ++i) {
        auto* file = handoff[i].file;
        handoff[i] = {};
        ipc_release_file_ref(file);
    }
}

auto wki_ipc_commit_task_fd_handoff(ker::mod::sched::task::Task* task, WkiIpcTaskFdHandoff* handoff, uint16_t count) -> uint16_t {
    if (task == nullptr || handoff == nullptr) {
        wki_ipc_release_task_fd_handoff(handoff, count);
        return 0;
    }

    uint16_t committed = 0;
    for (uint16_t i = 0; i < count; ++i) {
        auto const entry = handoff[i];
        handoff[i] = {};
        ker::vfs::File* removed = nullptr;
        if (entry.file != nullptr && entry.local_fd < ker::mod::sched::task::Task::FD_TABLE_SIZE) {
            uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
            if (task->fd_table.lookup(entry.local_fd) == entry.file) {
                removed = static_cast<ker::vfs::File*>(task->fd_table.remove(entry.local_fd));
                task->clear_fd_cloexec(entry.local_fd);
                ker::vfs::fd_usage_changed_locked(*task);
            }
            task->fd_table_lock.unlock_irqrestore(IRQF);
        }

        if (removed != nullptr) {
            ++committed;
            // Drop the fd-table ownership only after publishing its removal.
            ipc_release_file_ref(removed);
        }
        // Drop the temporary identity pin.  The matching export either still
        // owns the endpoint or an early remote close already retired it.
        ipc_release_file_ref(entry.file);
    }
    return committed;
}

auto wki_ipc_find_pipe_affinity_node(const ker::mod::sched::task::Task* task, uint16_t* node_out) -> bool {
    if (task == nullptr || node_out == nullptr) {
        return false;
    }

    uint16_t candidate_node = WKI_NODE_INVALID;
    bool found = false;
    bool conflict = false;

    task->fd_table.for_each([&](uint64_t, void* val) {
        if (val == nullptr || conflict) {
            return;
        }

        const auto* file = static_cast<const ker::vfs::File*>(val);
        if (!ker::vfs::vfs_is_pipe_file(file) || file->private_data == nullptr || !ipc_pipe_endpoint_requires_affinity(file->open_flags)) {
            return;
        }

        uint16_t matched_node = WKI_NODE_INVALID;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        for (const auto* exp : g_ipc_exports) {
            if (exp == nullptr || !exp->active || exp->res_type != ResourceType::IPC_PIPE || exp->file == nullptr) {
                continue;
            }
            if (exp->file->private_data != file->private_data || exp->consumer_node == WKI_NODE_INVALID) {
                continue;
            }
            matched_node = exp->consumer_node;
            break;
        }
        s_ipc_lock.unlock_irqrestore(IRQF);

        if (matched_node == WKI_NODE_INVALID) {
            return;
        }
        if (!found) {
            candidate_node = matched_node;
            found = true;
            return;
        }
        if (candidate_node != matched_node) {
            conflict = true;
        }
    });

    if (!found || conflict) {
        return false;
    }

    auto* peer = wki_peer_find(candidate_node);
    if (peer == nullptr || peer->state != PeerState::CONNECTED) {
        return false;
    }

    *node_out = candidate_node;
    return true;
}

// =============================================================================
// Public API: Attach proxy fds on remote task
// =============================================================================

namespace {

struct PreparedIpcImport {
    uint16_t fd = 0;
    ker::vfs::File* file = nullptr;
    ProxyIpcState* proxy = nullptr;
    ker::vfs::File* existing = nullptr;
};

auto install_ipc_proxy_files(ker::mod::sched::task::Task& task, std::span<PreparedIpcImport> imports) -> bool {
    uint64_t const IRQF = task.fd_table_lock.lock_irqsave();
    for (auto const& entry : imports) {
        if (task.fd_table.reserved(entry.fd)) {
            task.fd_table_lock.unlock_irqrestore(IRQF);
            return false;
        }
    }
    for (size_t i = 0; i < imports.size(); ++i) {
        auto& entry = imports[i];
        entry.existing = static_cast<ker::vfs::File*>(task.fd_table.lookup(entry.fd));
        bool injected_failure = false;
#ifdef WOS_SELFTEST
        // Fail at the final entry so multi-FD tests exercise actual rollback.
        injected_failure = g_ipc_selftest_force_attach_insert_failure.load(std::memory_order_relaxed) && i + 1 == imports.size();
#endif
        if (injected_failure || !task.fd_table.insert(entry.fd, entry.file)) {
            for (size_t j = 0; j < i; ++j) {
                // Validated fixed-table indices, no reservations and same lock.
                static_cast<void>(task.fd_table.insert(imports[j].fd, imports[j].existing));
            }
            task.fd_table_lock.unlock_irqrestore(IRQF);
            return false;
        }
    }
    for (auto const& entry : imports) task.clear_fd_cloexec(entry.fd);
    ker::vfs::fd_usage_changed_locked(task);
    task.fd_table_lock.unlock_irqrestore(IRQF);
    return true;
}

void free_uninstalled_ipc_proxy_file(ker::vfs::File* proxy_file, ProxyIpcState* proxy) {
    if (proxy_file != nullptr) {
        if (proxy_file->fops == &g_proxy_epoll_fops && proxy_file->private_data != nullptr && proxy_file->private_data != proxy) {
            delete static_cast<ProxyEpollFile*>(proxy_file->private_data);
        }
        proxy_file->private_data = nullptr;
        delete proxy_file;
    }

    if (proxy != nullptr) {
        proxy->active.store(false, std::memory_order_release);
        proxy_release(proxy);
    }
}

}  // namespace

auto wki_ipc_validate_task_fds(const WkiIpcFdEntry* map, uint16_t count, uint16_t source_node) -> int {
    if (count > MAX_SUBMIT_IPC_FDS || (count != 0 && map == nullptr)) return -EINVAL;
    if (count == 0) return 0;
    if (source_node == WKI_NODE_INVALID || source_node == WKI_NODE_BROADCAST) return -EACCES;
    for (size_t i = 0; i < count; ++i) {
        auto const& entry = map[i];
        // Every current export is issued by the submitting node, including
        // separate descriptor aliases. A peer cannot nominate another home's
        // already-issued grant to this receiver.
        if (entry.home_node != source_node) return -EACCES;
        auto const type = static_cast<ResourceType>(entry.res_type);
        if (entry.resource_id == 0 || entry.local_fd >= ker::mod::sched::task::Task::FD_TABLE_SIZE ||
            (type != ResourceType::IPC_PIPE && type != ResourceType::IPC_SOCKET && type != ResourceType::IPC_EPOLL &&
             type != ResourceType::IPC_PTY))
            return -EINVAL;
        for (size_t j = 0; j < i; ++j)
            if (map[j].local_fd == entry.local_fd || map[j].resource_id == entry.resource_id) return -EINVAL;
    }
    return 0;
}

auto wki_ipc_capture_data_channel(const WkiChannelIdentity& control) -> WkiChannelIdentity {
    auto control_live = [&]() {
        return control.channel_id == WKI_CHAN_RESOURCE &&
               wki_channel_generation_is_live(control.channel, control.peer_node_id, WKI_CHAN_RESOURCE, control.generation);
    };
    if (!control_live()) return {};
    auto const data = capture_ipc_channel(control.peer_node_id, WKI_CHAN_IPC_DATA);
    // A connected epoch change retires RESOURCE as well as DATA. The second
    // check prevents pairing a retired submit with the replacement DATA slot.
    return control_live() ? data : WkiChannelIdentity{};
}

auto wki_ipc_attach_task_fds(ker::mod::sched::task::Task* task, const WkiIpcFdEntry* map, uint16_t count, uint16_t source_node,
                             const WkiChannelIdentity& control_channel, const WkiChannelIdentity& data_channel,
                             WkiIpcOutputCompletion* completion) -> bool {
    constexpr size_t MAX_IMPORTS = MAX_SUBMIT_IPC_FDS;
    if (task == nullptr || wki_ipc_validate_task_fds(map, count, source_node) != 0) return false;
    if (count == 0) return true;
    if (control_channel.peer_node_id != source_node || control_channel.channel_id != WKI_CHAN_RESOURCE ||
        !wki_channel_generation_is_live(control_channel.channel, source_node, WKI_CHAN_RESOURCE, control_channel.generation))
        return false;
    if (data_channel.peer_node_id != source_node || data_channel.channel_id != WKI_CHAN_IPC_DATA ||
        !wki_channel_generation_is_live(data_channel.channel, source_node, WKI_CHAN_IPC_DATA, data_channel.generation))
        return false;
    std::array<PreparedIpcImport, MAX_IMPORTS> prepared{};
    size_t prepared_count = 0;
    auto abandon = [&]() {
        for (size_t i = 0; i < prepared_count; ++i) free_uninstalled_ipc_proxy_file(prepared[i].file, prepared[i].proxy);
        return false;
    };

    for (const auto& entry : std::span(map, count)) {
        auto& staged = prepared[prepared_count++];
        staged.fd = entry.local_fd;

        auto const RES_TYPE = static_cast<ResourceType>(entry.res_type);
        int const OPEN_FLAGS = ipc_fd_import_open_flags(entry.reserved1);
        uint16_t const ACCESS_MODE = ipc_fd_access_mode(OPEN_FLAGS);

        auto* proxy = new (std::nothrow) ProxyIpcState();
        staged.proxy = proxy;
        if (proxy == nullptr) return abandon();
        proxy->active = true;
        proxy->res_type = RES_TYPE;
        proxy->home_node = entry.home_node;
        proxy->resource_id = entry.resource_id;
        proxy->assigned_channel = WKI_CHAN_RESOURCE;
        proxy->control_channel = control_channel;
        proxy->data_channel = data_channel;
        proxy->can_receive_data = ipc_proxy_can_receive_data(RES_TYPE, ACCESS_MODE);
        proxy->message_write_error.store(0, std::memory_order_relaxed);

        // Allocate receive storage only for endpoints whose fops can consume it.
        if (proxy->can_receive_data) {
            constexpr uint32_t PIPE_RING_CAPACITY = WKI_PROXY_PIPE_RING_SIZE;
            auto* rb = new (std::nothrow) uint8_t[PIPE_RING_CAPACITY];
            if (rb != nullptr) {
                // RX copies every readable byte before publishing ring_head, so
                // free ring capacity does not need eager initialization.
                proxy->ring_buf = rb;
                proxy->ring_capacity = PIPE_RING_CAPACITY;
            } else {
                return abandon();
            }
            proxy->ring_head.store(0, std::memory_order_relaxed);
            proxy->ring_tail.store(0, std::memory_order_relaxed);
            proxy->write_closed.store(0U, std::memory_order_relaxed);
            proxy->blocked_reader.store(nullptr, std::memory_order_relaxed);
        }

        // Replace the fd's file with proxy file
        auto* proxy_file = new (std::nothrow) ker::vfs::File();
        staged.file = proxy_file;
        if (proxy_file == nullptr) return abandon();
        proxy_file->fd = static_cast<int>(entry.local_fd);
        proxy_file->private_data = proxy;
        proxy_file->pos = 0;
        proxy_file->is_directory = false;
        proxy_file->fs_type = ker::vfs::FSType::TMPFS;
        proxy_file->refcount = 1;
        proxy_file->fd_flags = 0;
        proxy_file->vfs_path = nullptr;
        proxy_file->dir_fs_count = 0;
        proxy_file->open_flags = OPEN_FLAGS;

        if (proxy->res_type == ResourceType::IPC_PIPE) {
            if (ACCESS_MODE != 0 && entry.rdma_rkey != 0 && entry.rdma_offset == WKI_IPC_PIPE_RDMA_CAPACITY) {
                proxy->pipe_rdma_enabled = true;
                proxy->pipe_rdma_rkey = entry.rdma_rkey;
                proxy->pipe_rdma_capacity = static_cast<uint32_t>(entry.rdma_offset);
            }
            proxy->message_write_flow_control = ACCESS_MODE != 0 && (entry.reserved1 & WKI_IPC_FD_FLAG_PIPE_FLOW_CONTROL) != 0;
            proxy->message_write_credits.store(proxy->message_write_flow_control ? WKI_IPC_MESSAGE_PIPE_WINDOW : 0,
                                               std::memory_order_relaxed);
            if (ACCESS_MODE == 0) {
                proxy_file->fops = &g_proxy_pipe_read_fops;
            } else {
                proxy_file->fops = &g_proxy_pipe_write_fops;
            }
        } else if (proxy->res_type == ResourceType::IPC_SOCKET) {
            // The owner advertises this only for retained native stream sockets.
            proxy->message_write_flow_control = ACCESS_MODE != 0 && (entry.reserved1 & WKI_IPC_FD_FLAG_PIPE_FLOW_CONTROL) != 0;
            proxy->message_write_credits.store(proxy->message_write_flow_control ? WKI_IPC_MESSAGE_PIPE_WINDOW : 0,
                                               std::memory_order_relaxed);
            proxy_file->fops = &g_proxy_socket_fops;
            proxy_file->fs_type = ker::vfs::FSType::TMPFS;  // not SOCKET to avoid bad cast via fd_to_socket
        } else if (proxy->res_type == ResourceType::IPC_EPOLL) {
            // Allocate a local EpollInstance so kernel epoll_ctl / epoll_pwait work
            // transparently on this fd.  EpollInstance MUST be at offset 0 in
            // ProxyEpollFile so the cast in epoll_ctl is valid.
            auto* epf = new (std::nothrow) ProxyEpollFile();
            if (epf == nullptr) return abandon();
            epf->inst.count = 0;
            epf->inst.interests.fill({});
            epf->resource_id = proxy->resource_id;
            epf->home_node = proxy->home_node;
            proxy_file->private_data = epf;  // epoll_ctl casts this to EpollInstance*
            proxy_file->fops = &g_proxy_epoll_fops;
        } else if (proxy->res_type == ResourceType::IPC_PTY) {
            proxy->message_write_flow_control = ACCESS_MODE != 0 && (entry.reserved1 & WKI_IPC_FD_FLAG_PIPE_FLOW_CONTROL) != 0;
            proxy->message_write_credits.store(proxy->message_write_flow_control ? WKI_IPC_MESSAGE_PIPE_WINDOW : 0,
                                               std::memory_order_relaxed);
            // PTY proxy: ring-buffer data plane + ioctl forwarding
            // private_data is already set to proxy above
            proxy_file->fops = &g_proxy_pty_fops;
            proxy_file->open_flags = (OPEN_FLAGS & ~WKI_IPC_FD_ACCESS_MASK) | WKI_IPC_FD_RDWR;
        } else {
            proxy_file->fops = &g_proxy_pipe_read_fops;
        }
    }

    // All imported numbers were inherited at the sender. A later NOFILE
    // lowering cannot revoke them; the fixed capacity and reservations still
    // constrain this all-or-none publication.
    if (!install_ipc_proxy_files(*task, std::span(prepared.data(), prepared_count))) return abandon();

    // The unpublished Task now owns every File. Register all proxies before
    // releasing replaced Files, whose close operations can block.
    for (size_t i = 0; i < prepared_count; ++i) {
        auto const& entry = map[i];
        auto* proxy = prepared[i].proxy;
        bool pending_write_closed = false;
        PendingPipeDelivery* dropped_pending = nullptr;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy->refcount.fetch_add(1, std::memory_order_acq_rel);
        g_ipc_proxies.push_back(proxy);
        if (completion != nullptr) completion->retain_locked(proxy);
        if (proxy->can_receive_data && proxy->ring_buf != nullptr) {
            auto* pending = find_pending_pipe_delivery_locked(entry.home_node, entry.resource_id);
            pending_write_closed = pending != nullptr && pending->write_closed;
            if (pending != nullptr && pending->write_closed && pending->chunks.empty()) {
                erase_pending_pipe_delivery_locked(pending);
                free_pending_pipe_delivery(pending);
            }
        } else if (!proxy->can_receive_data) {
            // A write-only endpoint cannot consume DATA that raced ahead of
            // TASK_SUBMIT attachment. Do not leave unreachable pending bytes.
            dropped_pending = take_pending_pipe_delivery_locked(entry.home_node, entry.resource_id);
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
        free_pending_pipe_delivery(dropped_pending);

        if (pending_write_closed) {
            proxy_mark_pipe_closed(proxy, entry.resource_id);
        }
#ifdef WKI_IPC_DEBUG
        ker::mod::dbg::log("[WKI] IPC proxy attached fd=%u type=%u res_id=%u home=0x%04x", entry.local_fd, entry.res_type,
                           entry.resource_id, entry.home_node);
#endif
    }
    for (size_t i = 0; i < prepared_count; ++i) {
        if (prepared[i].existing != nullptr) ker::vfs::vfs_put_file(prepared[i].existing);
    }
    return true;
}

// =============================================================================
// Doorbell RX handler — called from ISR when IPC doorbell arrives
// =============================================================================

auto wki_ipc_doorbell_rx(uint16_t /*src_node*/, uint32_t /*doorbell_value*/) -> bool { return false; }

auto wki_ipc_epoll_ctl_forward(ker::vfs::File* epfile, int op, int fd, uint32_t events, uint64_t data) -> int {
    if (epfile == nullptr) {
        return -EINVAL;
    }

    auto* epf = reinterpret_cast<ProxyEpollFile*>(epfile->private_data);
    if (epf == nullptr || epfile->fops != &g_proxy_epoll_fops) {
        return -EOPNOTSUPP;
    }

    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    uint32_t const CORRELATION = ker::mod::perf::next_wki_trace_correlation();
    uint64_t const STARTED_US = wki_now_us();
    uint16_t const HOME_NODE = epf->home_node;
    auto finish = [&](int rc, uint64_t bytes = 0) -> int {
        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
        perf_record_ipc_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::EPOLL_CTL), ker::mod::perf::WkiPerfPhase::END, HOME_NODE,
                              WKI_CHAN_RESOURCE, CORRELATION, rc, ELAPSED_US, CALLSITE);
        perf_record_ipc_summary(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::EPOLL_CTL), HOME_NODE, WKI_CHAN_RESOURCE, rc, ELAPSED_US,
                                bytes);
        return rc;
    };
    perf_record_ipc_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::EPOLL_CTL), ker::mod::perf::WkiPerfPhase::BEGIN, HOME_NODE,
                          WKI_CHAN_RESOURCE, CORRELATION, 0, 0, CALLSITE);

    ProxyIpcState* proxy = nullptr;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy = find_proxy_by_endpoint_locked(HOME_NODE, epf->resource_id);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    if (proxy == nullptr) {
        return finish(-EBADF);
    }

    auto release_proxy = [&]() {
        if (proxy != nullptr) {
            proxy_release(proxy);
            proxy = nullptr;
        }
    };

    if (!proxy->active.load(std::memory_order_acquire)) {
        release_proxy();
        return finish(-EBADF);
    }
    if (proxy->home_node == WKI_NODE_INVALID) {
        release_proxy();
        return finish(-EHOSTUNREACH);
    }

    struct __attribute__((packed)) EpollCtlWire {
        int32_t op;
        int32_t fd;
        uint32_t events;
        uint64_t data;
    } ctl = {
        .op = static_cast<int32_t>(op),
        .fd = static_cast<int32_t>(fd),
        .events = events,
        .data = data,
    };

    constexpr size_t RID_SIZE = sizeof(uint32_t);
    constexpr size_t EXTRA_SIZE = sizeof(EpollCtlWire);
    constexpr size_t MSG_SIZE = sizeof(DevOpReqPayload) + RID_SIZE + WKI_IPC_OP_COOKIE_BYTES + EXTRA_SIZE;
    std::array<uint8_t, MSG_SIZE> msg = {};

    auto* req = reinterpret_cast<DevOpReqPayload*>(msg.data());
    req->op_id = OP_EPOLL_CTL;
    req->data_len = static_cast<uint16_t>(RID_SIZE + WKI_IPC_OP_COOKIE_BYTES + EXTRA_SIZE);
    std::memcpy(msg.data() + sizeof(DevOpReqPayload), &proxy->resource_id, RID_SIZE);

    WkiWaitEntry wait = {};
    uint16_t op_cookie = 0;
    uint64_t irqf = proxy->lock.lock_irqsave();
    if (proxy->pending_wait_op != 0) {
        proxy->lock.unlock_irqrestore(irqf);
        release_proxy();
        return finish(-EBUSY);
    }
    op_cookie = wki_ipc_allocate_wait_cookie_locked(proxy, OP_EPOLL_CTL);
    if (op_cookie == 0) {
        proxy->lock.unlock_irqrestore(irqf);
        release_proxy();
        return finish(-EAGAIN);
    }
    proxy->pending_wait = &wait;
    proxy->pending_wait_op = OP_EPOLL_CTL;
    proxy->pending_wait_cookie = op_cookie;
    proxy->pending_wait_status = -ETIMEDOUT;
    proxy->pending_wait_resp_len = 0;
    proxy->lock.unlock_irqrestore(irqf);

    std::memcpy(msg.data() + sizeof(DevOpReqPayload) + RID_SIZE, &op_cookie, WKI_IPC_OP_COOKIE_BYTES);
    std::memcpy(msg.data() + sizeof(DevOpReqPayload) + RID_SIZE + WKI_IPC_OP_COOKIE_BYTES, &ctl, EXTRA_SIZE);

    int const TX = wki_ipc_send_control(proxy, msg.data(), static_cast<uint16_t>(msg.size()));
    if (TX != WKI_OK) {
        wki_ipc_cancel_pending_wait(proxy, &wait, WKI_ERR_TX_FAILED, false);
        release_proxy();
        return finish(-EIO);
    }

    int const WAIT_RC = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    if (WAIT_RC != 0) {
        wki_ipc_cancel_pending_wait(proxy, &wait, -ETIMEDOUT);
        release_proxy();
        return finish(-ETIMEDOUT);
    }

    int const STATUS = wki_ipc_consume_pending_wait_response(proxy, &wait, OP_EPOLL_CTL, op_cookie, nullptr, 0, nullptr);
    release_proxy();
    return finish(STATUS, msg.size());
}

// =============================================================================
// DEV_OP response handler for IPC control ops
// =============================================================================

auto wki_ipc_send_control(ProxyIpcState* proxy, const void* payload, uint16_t len) -> int {
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) return WKI_ERR_NOT_FOUND;
    auto const& identity = proxy->control_channel;
    if (identity.peer_node_id != proxy->home_node || identity.channel_id != WKI_CHAN_RESOURCE ||
        !wki_channel_generation_is_live(identity.channel, identity.peer_node_id, identity.channel_id, identity.generation))
        return WKI_ERR_NOT_FOUND;
    // Revalidate the same immutable identity at reliable commit too: reset
    // can race this preliminary check, but must never select a replacement.
    return wki_send_on_channel_identity(identity, MsgType::DEV_OP_REQ, payload, len);
}

namespace {

auto claim_proxy_pending_wait_locked(ProxyIpcState* proxy, int status) -> WkiWaitEntry* {
    if (proxy == nullptr || proxy->pending_wait == nullptr) {
        return nullptr;
    }
    auto* wait = proxy->pending_wait;
    if (!wki_claim_op(wait)) {
        return nullptr;
    }
    proxy->pending_wait = nullptr;
    proxy->pending_wait_op = 0;
    proxy->pending_wait_cookie = 0;
    proxy->pending_wait_status = status;
    proxy->pending_wait_resp_len = 0;
    return wait;
}

}  // namespace

void wki_ipc_cancel_pending_wait(ProxyIpcState* proxy, WkiWaitEntry* wait, int result, bool request_sent) {
    if (wait == nullptr) {
        return;
    }

    bool claimed = false;
    if (proxy != nullptr) {
        uint64_t const IRQF = proxy->lock.lock_irqsave();
        if (proxy->pending_wait == wait) {
            if (!request_sent) {
                wki_ipc_retire_wait_cookie_locked(proxy, proxy->pending_wait_op, proxy->pending_wait_cookie);
            }
            proxy->pending_wait = nullptr;
            proxy->pending_wait_op = 0;
            proxy->pending_wait_cookie = 0;
            proxy->pending_wait_status = result;
            proxy->pending_wait_resp_len = 0;
        }
        claimed = wki_claim_op(wait);
        proxy->lock.unlock_irqrestore(IRQF);
    } else {
        claimed = wki_claim_op(wait);
    }

    if (claimed) {
        wki_finish_claimed_op(wait, result);
        return;
    }

    while (wait->state.load(std::memory_order_acquire) != static_cast<uint8_t>(WkiWaitEntry::DONE)) {
        ker::mod::sched::kern_yield();
    }
}

auto wki_ipc_consume_pending_wait_response(ProxyIpcState* proxy, WkiWaitEntry* wait, uint16_t expected_op, uint16_t expected_cookie,
                                           void* resp_buf, uint16_t resp_max, uint16_t* resp_len_out) -> int {
    if (proxy == nullptr || wait == nullptr) {
        if (resp_len_out != nullptr) {
            *resp_len_out = 0;
        }
        return -EINVAL;
    }

    uint64_t const IRQF = proxy->lock.lock_irqsave();
    bool const RESPONSE_SLOT_MATCHES =
        proxy->pending_wait == nullptr && proxy->pending_wait_op == expected_op && proxy->pending_wait_cookie == expected_cookie;
    bool const SLOT_WAS_CANCELLED = proxy->pending_wait == nullptr && proxy->pending_wait_op == 0 && proxy->pending_wait_cookie == 0;
    int status = proxy->pending_wait_status;
    uint16_t resp_len = proxy->pending_wait_resp_len;
    if (!RESPONSE_SLOT_MATCHES && !SLOT_WAS_CANCELLED) {
        status = -EIO;
        resp_len = 0;
    }

    if (resp_buf != nullptr && resp_max > 0 && resp_len > 0) {
        uint16_t const COPY_LEN = resp_len < resp_max ? resp_len : resp_max;
        std::memcpy(resp_buf, static_cast<const void*>(proxy->pending_wait_resp), COPY_LEN);
    }
    if (resp_len_out != nullptr) {
        *resp_len_out = resp_len;
    }
    if (RESPONSE_SLOT_MATCHES || SLOT_WAS_CANCELLED) {
        proxy->pending_wait = nullptr;
        proxy->pending_wait_op = 0;
        proxy->pending_wait_cookie = 0;
        proxy->pending_wait_resp_len = 0;
    }
    proxy->lock.unlock_irqrestore(IRQF);
    return status;
}

void wki_ipc_handle_dev_op_resp(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len) {
    DevOpRespPayload resp = {};
    if (!read_ipc_control_response(channel, payload, len, resp)) {
        return;
    }
    uint16_t const OP_ID = resp.op_id;

    // Handle pipe close acknowledgement
    if (OP_ID == OP_PIPE_CLOSE_READ || OP_ID == OP_PIPE_CLOSE_WRITE || OP_ID == OP_PIPE_CLOSE_WRITE_FLOW) {
        // Close acknowledged — no further action needed
        return;
    }

    if (OP_ID == OP_PIPE_POLL_STATE) {
        // Home node responded to a poll-state query with readiness flags.
        // Wake any blocked reader so it can re-check availability.
        if (resp.data_len < 2 * sizeof(uint32_t)) {
            return;
        }
        uint32_t poll_resource_id = 0;
        std::memcpy(&poll_resource_id, payload + sizeof(DevOpRespPayload), sizeof(uint32_t));
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        auto* proxy = find_proxy_by_endpoint_locked(src_node, poll_resource_id);
        s_ipc_lock.unlock_irqrestore(IRQF);
        if (proxy != nullptr) {
            auto* reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
            wake_blocked_ipc_reader(reader);
            proxy_wake_stream_readers(proxy);
            wki_ipc_proxy_wake_poll_waiters(proxy);
            proxy_release(proxy);
        }
        return;
    }

    if (OP_ID == OP_EPOLL_CTL || OP_ID == OP_FUTEX_WAKE || OP_ID == OP_PTY_IOCTL) {
        // Control-op completion path: payload carries [resource_id] first in data.
        wki_ipc_socket_handle_dev_op_resp(src_node, channel, payload, len);
        return;
    }

    if (OP_ID >= OP_SOCK_ACCEPT && OP_ID <= OP_SOCK_SETSOCKOPT) {
        wki_ipc_socket_handle_dev_op_resp(src_node, channel, payload, len);
    }
}

#ifdef WOS_SELFTEST
struct IpcSelftestChannels {
    WkiChannel control{};
    WkiChannel data{};
    WkiIpcChannels identity{};
    explicit IpcSelftestChannels(uint16_t node) {
        control.active = true;
        control.peer_node_id = node;
        control.channel_id = WKI_CHAN_RESOURCE;
        control.generation = 1;
        data.active = true;
        data.peer_node_id = node;
        data.channel_id = WKI_CHAN_IPC_DATA;
        data.generation = 1;
        identity = {{&control, node, WKI_CHAN_RESOURCE, 1}, {&data, node, WKI_CHAN_IPC_DATA, 1}};
    }
};

auto wki_ipc_selftest_import_provenance_preserves_state() -> int {
    if (g_wki.initialized) return -EBUSY;
    constexpr uint16_t SOURCE = 0x7C01;
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = SOURCE;
    channel.channel_id = WKI_CHAN_RESOURCE;
    channel.generation = 1;
    WkiChannelIdentity const CONTROL{&channel, SOURCE, WKI_CHAN_RESOURCE, 1};
    WkiChannel data_channel{};
    data_channel.active = true;
    data_channel.peer_node_id = SOURCE;
    data_channel.channel_id = WKI_CHAN_IPC_DATA;
    data_channel.generation = 1;
    WkiChannelIdentity const DATA{&data_channel, SOURCE, WKI_CHAN_IPC_DATA, 1};
    constexpr uint16_t FD = 5;
    constexpr unsigned NOFILE = static_cast<unsigned>(ker::abi::resource::Id::NOFILE);
    ker::mod::sched::task::Task task{};
    auto* file = new (std::nothrow) ker::vfs::File{};
    if (file == nullptr) return -ENOMEM;
    file->fd = FD;
    file->fops = &g_ipc_selftest_file_ops;
    file->refcount.store(1);
    if (!task.fd_table.insert(FD, file)) {
        delete file;
        return -ENOMEM;
    }
    task.set_fd_cloexec(FD);
    task.resources.commit_extent(NOFILE, 1);
    std::array<WkiIpcFdEntry, 2> entries{};
    for (size_t i = 0; i < entries.size(); ++i) {
        entries[i].local_fd = FD + i;
        entries[i].res_type = static_cast<uint16_t>(ResourceType::IPC_PIPE);
        entries[i].resource_id = 0x7C010001 + i;
        entries[i].home_node = SOURCE;
        entries[i].reserved1 = 1;
    }
    auto const VALID = entries;
    size_t proxy_count = 0;
    size_t pending_count = 0;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy_count = g_ipc_proxies.size();
        pending_count = g_pending_pipe_deliveries.size();
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    int const FREES = g_ipc_proxy_selftest_frees.load(std::memory_order_acquire);
    auto intact = [&]() {
        return task.fd_table.lookup(FD) == file && task.fd_table.lookup(FD + 1) == nullptr && task.get_fd_cloexec(FD) &&
               file->refcount.load(std::memory_order_acquire) == 1 && task.resources.entries[NOFILE].current == 1 &&
               task.resources.entries[NOFILE].peak == 1 && task.resources.entries[NOFILE].denied == 0;
    };
    auto refused = [&](uint16_t source, int expected) {
        bool const REJECTED = wki_ipc_validate_task_fds(entries.data(), entries.size(), source) == expected &&
                              !wki_ipc_attach_task_fds(&task, entries.data(), entries.size(), source, CONTROL, DATA);
        return REJECTED && intact();
    };
    bool good = wki_ipc_validate_task_fds(entries.data(), entries.size(), SOURCE) == 0;
    entries[1].home_node = SOURCE + 1;
    good &= refused(SOURCE, -EACCES);
    entries = VALID;
    good &= refused(WKI_NODE_INVALID, -EACCES) && refused(WKI_NODE_BROADCAST, -EACCES);
    entries[1].resource_id = 0;
    good &= refused(SOURCE, -EINVAL);
    entries = VALID;
    entries[1].resource_id = entries[0].resource_id;
    good &= refused(SOURCE, -EINVAL);
    entries = VALID;
    entries[1].local_fd = FD;
    good &= refused(SOURCE, -EINVAL);
    entries = VALID;
    entries[1].local_fd = task.FD_TABLE_SIZE;
    good &= refused(SOURCE, -EINVAL);
    entries = VALID;
    entries[1].res_type = static_cast<uint16_t>(ResourceType::IPC_FUTEX);
    good &= refused(SOURCE, -EINVAL);
    good &= wki_ipc_validate_task_fds(nullptr, 1, SOURCE) == -EINVAL && wki_ipc_validate_task_fds(nullptr, 17, SOURCE) == -EINVAL &&
            wki_ipc_validate_task_fds(nullptr, 0, SOURCE) == 0;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        good &= g_ipc_proxies.size() == proxy_count && g_pending_pipe_deliveries.size() == pending_count;
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    good &= g_ipc_proxy_selftest_frees.load(std::memory_order_acquire) == FREES;
    auto* removed = static_cast<ker::vfs::File*>(task.fd_table.remove(FD));
    if (removed != nullptr) ker::vfs::vfs_put_file(removed);
    return good ? 0 : -EIO;
}

auto wki_ipc_selftest_resource_id_exhaustion() -> int {
    uint32_t next = UINT32_MAX - 1;
    uint32_t first = 0x1234;
    bool good = !reserve_ipc_resource_ids(next, 3, first) && next == UINT32_MAX - 1 && first == 0x1234;
    good &= reserve_ipc_resource_ids(next, 2, first) && first == UINT32_MAX - 1 && next == 0;
    first = 0x5678;
    good &= !reserve_ipc_resource_ids(next, 1, first) && next == 0 && first == 0x5678;
    good &= reserve_ipc_resource_ids(next, 0, first) && next == 0 && first == 0;
    next = UINT32_MAX;
    good &= reserve_ipc_resource_ids(next, 1, first) && first == UINT32_MAX && next == 0;
    next = 0x7000;
    good &= reserve_ipc_resource_ids(next, MAX_SUBMIT_IPC_FDS, first) && first == 0x7000 && next == 0x7010;
    good &= reserve_ipc_resource_ids(next, 1, first) && first == 0x7010 && next == 0x7011;
    return good ? 0 : -EIO;
}

auto wki_ipc_selftest_unsupported_control_completion() -> int {
    if (g_wki.initialized) return -EBUSY;
    constexpr uint16_t HOME = 0x7A51;
    constexpr uint16_t COOKIE = 0x1234;
    constexpr uint32_t RESOURCE = 0x7A510001;
    bool good = true;
    for (uint16_t op : {OP_EPOLL_CTL, OP_FUTEX_WAKE}) {
        auto* proxy = new (std::nothrow) ProxyIpcState{};
        if (proxy == nullptr) return -ENOMEM;
        WkiWaitEntry wait{};
        proxy->active.store(true, std::memory_order_release);
        proxy->resource_id = RESOURCE;
        proxy->home_node = HOME;
        proxy->refcount.store(1, std::memory_order_release);
        proxy->pending_wait = &wait;
        proxy->pending_wait_op = op;
        proxy->pending_wait_cookie = COOKIE;
        proxy->pending_wait_status = -ETIMEDOUT;
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            g_ipc_proxies.push_back(proxy);
            s_ipc_lock.unlock_irqrestore(IRQF);
        }
        auto deliver = [&](uint16_t node, uint16_t cookie) {
            auto const REPLY = unsupported_ipc_control_reply(op, RESOURCE, cookie);
            wki_ipc_handle_dev_op_resp(node, WKI_CHAN_RESOURCE, REPLY.data(), static_cast<uint16_t>(REPLY.size()));
        };
        deliver(HOME + 1, COOKIE);
        deliver(HOME, COOKIE - 1);
        good &= wait.state.load(std::memory_order_acquire) == WkiWaitEntry::PENDING && proxy->pending_wait == &wait &&
                proxy->pending_wait_status == -ETIMEDOUT && proxy->refcount.load(std::memory_order_acquire) == 1;
        deliver(HOME, COOKIE);
        good &= wait.state.load(std::memory_order_acquire) == WkiWaitEntry::DONE && proxy->pending_wait == nullptr;
        good &= wki_ipc_consume_pending_wait_response(proxy, &wait, op, COOKIE, nullptr, 0, nullptr) == -ENOTSUP;
        good &= proxy->pending_wait_op == 0 && proxy->pending_wait_cookie == 0 && proxy->refcount.load(std::memory_order_acquire) == 1;
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            proxy_unregister_locked(proxy);
            s_ipc_lock.unlock_irqrestore(IRQF);
        }
        proxy->pending_wait = nullptr;
        proxy_release(proxy);
    }
    return good ? 0 : -EIO;
}

auto wki_ipc_selftest_poll_state_response_refs() -> int {
    constexpr uint32_t RESOURCE_ID = 0x5A5A;

    auto* proxy = new ProxyIpcState{};
    if (proxy == nullptr) {
        return -ENOMEM;
    }
    proxy->active.store(true, std::memory_order_release);
    proxy->resource_id = RESOURCE_ID;
    proxy->refcount.store(1, std::memory_order_release);

    {
        uint64_t const PROXY_IRQF = proxy->lock.lock_irqsave();
        if (!proxy_register_waiter_locked(proxy->poll_waiters, 0x600D)) {
            proxy->lock.unlock_irqrestore(PROXY_IRQF);
            proxy_release(proxy);
            return -ENOMEM;
        }
        proxy->lock.unlock_irqrestore(PROXY_IRQF);

        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        g_ipc_proxies.push_back(proxy);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    DevOpRespPayload resp = {};
    resp.op_id = OP_PIPE_POLL_STATE;
    resp.status = 0;
    resp.data_len = sizeof(uint32_t) * 2;

    uint32_t ready_flags = 0;
    std::array<uint8_t, sizeof(DevOpRespPayload) + (sizeof(uint32_t) * 2)> payload = {};
    std::memcpy(payload.data(), &resp, sizeof(resp));
    std::memcpy(payload.data() + sizeof(DevOpRespPayload), &RESOURCE_ID, sizeof(uint32_t));
    std::memcpy(payload.data() + sizeof(DevOpRespPayload) + sizeof(uint32_t), &ready_flags, sizeof(uint32_t));

    bool rejected_preserves_waiters = true;
    auto waiters_unchanged = [&]() {
        uint64_t const IRQF = proxy->lock.lock_irqsave();
        bool const UNCHANGED =
            proxy->poll_waiters.size() == 1 && proxy->poll_waiters[0] == 0x600D && proxy->refcount.load(std::memory_order_acquire) == 1;
        proxy->lock.unlock_irqrestore(IRQF);
        return UNCHANGED;
    };
    wki_ipc_handle_dev_op_resp(WKI_NODE_INVALID, WKI_CHAN_IPC_DATA, payload.data(), static_cast<uint16_t>(payload.size()));
    rejected_preserves_waiters &= waiters_unchanged();
    for (uint16_t declared : {uint16_t{sizeof(uint32_t)}, uint16_t{UINT16_MAX}}) {
        resp.data_len = declared;
        std::memcpy(payload.data(), &resp, sizeof(resp));
        wki_ipc_handle_dev_op_resp(WKI_NODE_INVALID, WKI_CHAN_RESOURCE, payload.data(), static_cast<uint16_t>(payload.size()));
        rejected_preserves_waiters &= waiters_unchanged();
    }
    resp.data_len = sizeof(uint32_t) * 2;
    std::memcpy(payload.data(), &resp, sizeof(resp));
    wki_ipc_handle_dev_op_resp(WKI_NODE_INVALID, WKI_CHAN_RESOURCE, payload.data(), static_cast<uint16_t>(payload.size()));

    int const REFS_AFTER_RESPONSE = proxy->refcount.load(std::memory_order_acquire);
    bool poll_waiters_drained = false;
    {
        uint64_t const IRQF = proxy->lock.lock_irqsave();
        poll_waiters_drained = proxy->poll_waiters.empty();
        proxy->lock.unlock_irqrestore(IRQF);
    }
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy_unregister_locked(proxy);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    proxy->active.store(false, std::memory_order_release);
    for (int i = 0; i < REFS_AFTER_RESPONSE; ++i) {
        proxy_release(proxy);
    }
    return rejected_preserves_waiters && poll_waiters_drained ? REFS_AFTER_RESPONSE : -EIO;
}

auto wki_ipc_selftest_export_consumer_authority() -> int {
    if (g_wki.initialized) return -EBUSY;
    constexpr uint16_t CONSUMER = 0x7A61;
    constexpr uint16_t OTHER = 0x7A62;
    constexpr uint32_t RESOURCE = 0x7A610001;
    ker::vfs::File file{};
    file.refcount.store(1);
    WkiIpcExport exp{};
    exp.active = true;
    exp.resource_id = RESOURCE;
    exp.consumer_node = CONSUMER;
    exp.res_type = ResourceType::IPC_PTY;
    exp.file = &file;
    uint64_t flags = s_ipc_lock.lock_irqsave();
    if (find_export_by_resource_id(RESOURCE) != nullptr) {
        s_ipc_lock.unlock_irqrestore(flags);
        return -EBUSY;
    }
    g_ipc_exports.push_back(&exp);
    size_t const PENDING_BEFORE = g_pending_pipe_deliveries.size();
    bool good = true;
    for (auto peer : {OTHER, WKI_NODE_INVALID, WKI_NODE_BROADCAST}) {
        good &= ipc_acquire_export_file_locked(RESOURCE, peer) == nullptr;
    }
    good &= ipc_acquire_export_file_locked(RESOURCE + 1, CONSUMER) == nullptr;
    good &= ipc_acquire_export_file_locked(RESOURCE, CONSUMER, ResourceType::IPC_SOCKET) == nullptr;
    good &= ipc_acquire_export_file_locked(RESOURCE, CONSUMER, ResourceType::IPC_EPOLL) == nullptr;
    good &= file.refcount.load() == 1;
    auto* retained = ipc_acquire_export_file_locked(RESOURCE, CONSUMER, ResourceType::IPC_PTY);
    good &= retained == &file && file.refcount.load() == 2;
    s_ipc_lock.unlock_irqrestore(flags);

    // Refused close requests must not create a backlog, pending close, or
    // retire the row. Exercise the production close paths without any send.
    good &= !mark_export_pipe_write_closed(RESOURCE, OTHER);
    good &= !mark_export_pipe_write_closed(RESOURCE, CONSUMER, 0, false, ResourceType::IPC_SOCKET);
    good &= mark_ipc_pty_write_closed_or_pending(OTHER, RESOURCE);
    std::array<uint8_t, sizeof(DevOpReqPayload) + sizeof(uint32_t)> payload{};
    DevOpReqPayload request{.op_id = OP_PIPE_CLOSE_READ, .data_len = sizeof(uint32_t)};
    std::memcpy(payload.data(), &request, sizeof(request));
    std::memcpy(payload.data() + sizeof(request), &RESOURCE, sizeof(RESOURCE));
    WkiHeader header{};
    header.src_node = OTHER;
    header.channel_id = WKI_CHAN_RESOURCE;
    handle_ipc_dev_op_req_inline(&header, payload.data(), static_cast<uint16_t>(payload.size()), nullptr);

    flags = s_ipc_lock.lock_irqsave();
    good &= exp.active && exp.file == &file && exp.consumer_node == CONSUMER && file.refcount.load() == 2;
    good &= find_export_pipe_write_backlog_locked(&exp) == nullptr && g_pending_pipe_deliveries.size() == PENDING_BEFORE;
    exp.active = false;
    good &= ipc_acquire_export_file_locked(RESOURCE, CONSUMER) == nullptr;
    exp.file = nullptr;
    for (auto it = g_ipc_exports.begin(); it != g_ipc_exports.end(); ++it) {
        if (*it == &exp) {
            g_ipc_exports.erase(it);
            break;
        }
    }
    s_ipc_lock.unlock_irqrestore(flags);
    // A valid pin survives removal of its export and releases exactly once.
    if (retained != nullptr) ipc_release_file_ref(retained);
    good &= file.refcount.load() == 1;
    return good ? 0 : -EIO;
}

auto wki_ipc_selftest_export_compaction_frees() -> int {
    constexpr uint32_t RESOURCE_ID = 0x6A6A;

    auto* file = new (std::nothrow) ker::vfs::File{};
    auto* exp = new (std::nothrow) WkiIpcExport{};
    if (file == nullptr || exp == nullptr) {
        delete file;
        delete exp;
        return -ENOMEM;
    }
    auto* backlog = new (std::nothrow) ExportPipeWriteBacklog{};
    if (backlog == nullptr) {
        delete file;
        delete exp;
        return -ENOMEM;
    }

    file->fops = &g_ipc_selftest_file_ops;
    file->refcount.store(1, std::memory_order_release);
    exp->active = false;
    exp->resource_id = RESOURCE_ID;
    exp->file = file;
    backlog->exp = exp;
    backlog->resource_id = RESOURCE_ID;

    int const FREES_BEFORE = g_ipc_export_selftest_frees.load(std::memory_order_acquire);
    int const FILE_CLOSES_BEFORE = g_ipc_file_selftest_closes.load(std::memory_order_acquire);
    WkiIpcExport* first_retired = nullptr;
    WkiIpcExport* second_retired = nullptr;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        g_ipc_exports.push_back(exp);
        g_export_pipe_write_backlogs.push_back(backlog);

        first_retired = compact_inactive_exports_locked();
        erase_export_pipe_write_backlog_locked(backlog);
        second_retired = compact_inactive_exports_locked();
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    free_export_pipe_write_backlog(backlog);
    free_retired_exports(first_retired);
    free_retired_exports(second_retired);

    int const FREES_AFTER = g_ipc_export_selftest_frees.load(std::memory_order_acquire);
    int const FILE_CLOSES_AFTER = g_ipc_file_selftest_closes.load(std::memory_order_acquire);
    if (first_retired != nullptr) {
        return -EFAULT;
    }
    if (FILE_CLOSES_AFTER - FILE_CLOSES_BEFORE != 1) {
        return -EIO;
    }
    return FREES_AFTER - FREES_BEFORE;
}

auto wki_ipc_selftest_rdma_write_survives_detachment() -> int {
    constexpr uint16_t PEER = 0x7B26;
    constexpr std::array<uint8_t, 4> BYTES{0x91, 0x27, 0x35, 0xA4};
    WkiIpcExport exp{};
    struct Script {
        WkiIpcExport* exp;
        std::array<uint8_t, 4> output{};
        unsigned writes = 0;
    } script{.exp = &exp};
    ker::vfs::FileOperations operations{};
    operations.vfs_write = [](ker::vfs::File* file, const void* data, size_t size, size_t) -> ssize_t {
        auto& state = *static_cast<Script*>(file->private_data);
        ++state.writes;
        if (size != state.output.size() || !state.exp->pipe_secure_effect_claimed) return -EINVAL;
        std::memcpy(state.output.data(), data, size);
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        state.exp->active = false;
        state.exp->file = nullptr;
        s_ipc_lock.unlock_irqrestore(IRQF);
        return static_cast<ssize_t>(size);
    };
    ker::vfs::File file{};
    file.refcount.store(1);
    file.fops = &operations;
    file.private_data = &script;
    // The hook test needs a bounded private window, not a registered 64 KiB slot.
    std::array<uint8_t, 64> backing{};
    exp.active = true;
    exp.resource_id = 0x7B26;
    exp.consumer_node = PEER;
    exp.res_type = ResourceType::IPC_PIPE;
    exp.file = &file;
    exp.pipe_rdma_enabled = true;
    exp.pipe_secure_activated = true;
    exp.pipe_secure_lifetime = 1;
    exp.pipe_rdma_capacity = backing.size();
    exp.pipe_rdma_region_size = backing.size();
    exp.pipe_rdma_region = backing.data();
    exp.pipe_secure_descriptor.binding_generation = 1;
    exp.pipe_secure_descriptor.service = static_cast<uint8_t>(rdma_protocol::Service::IPC);
    WkiChannel control{}, data{};
    control.active = true;
    control.peer_node_id = PEER;
    control.channel_id = WKI_CHAN_RESOURCE;
    control.generation = 1;
    data.active = true;
    data.peer_node_id = PEER;
    data.channel_id = WKI_CHAN_IPC_DATA;
    data.generation = 1;
    exp.channels = {{&control, PEER, WKI_CHAN_RESOURCE, 1}, {&data, PEER, WKI_CHAN_IPC_DATA, 1}};
    uint64_t flags = s_ipc_lock.lock_irqsave();
    auto* epoch = ipc_peer_cleanup_slot_locked(PEER, true);
    if (epoch == nullptr || epoch->cleanup_active) {
        s_ipc_lock.unlock_irqrestore(flags);
        return -EIO;
    }
    exp.pipe_secure_epoch = epoch->epoch;
    s_ipc_lock.unlock_irqrestore(flags);
    IpcSecureIntent intent{.kind = IPC_SECURE_APPEND, .resource_id = exp.resource_id, .lifetime = 1, .credit = backing.size()};
    WkiSecureRdmaRequest request{};
    request.grant = exp.pipe_secure_descriptor;
    request.operation_id = 1;
    request.receive_ticket = 2;
    request.peer_node = PEER;
    request.opcode = rdma_protocol::Opcode::WRITE;
    request.length = BYTES.size();
    request.intent_length = sizeof(intent);
    std::memcpy(request.native_intent.data(), &intent, sizeof(intent));
    uint64_t generation = 0;
    if (ipc_secure_arm(&exp, request, generation) != 0) return -EIO;
    auto const RESULT = ipc_secure_effect(&exp, request, backing, BYTES, {});
    bool const PINNED = file.refcount.load() == 2 && !exp.active && exp.pipe_secure_reserved;
    ipc_secure_release(&exp, request, RESULT);
    return PINNED && RESULT.status == 0 && RESULT.outcome == WkiSecureRdmaOutcome::EXACT && RESULT.bytes == BYTES.size() &&
                   exp.pipe_bytes_received == BYTES.size() && exp.pipe_bytes_delivered == BYTES.size() && file.refcount.load() == 1 &&
                   script.writes == 1 && script.output == BYTES && !exp.pipe_secure_reserved
               ? 0
               : -EIO;
}

auto wki_ipc_selftest_accepted_handoff_cancellation() -> int {
    ker::mod::sched::task::Task task{};
    struct Script {
        ker::mod::sched::task::Task* executor;
        unsigned closes = 0;
    } script{.executor = &task};
    ker::vfs::FileOperations operations{};
    operations.vfs_close = [](ker::vfs::File* file) -> int {
        auto& state = *static_cast<Script*>(file->private_data);
        ++state.closes;
        wki_ipc_cancel_submission_guards(*state.executor);
        return 0;
    };
    std::array<WkiIpcTaskFdHandoff, 2> handoff{};
    uint16_t count = 0;
    WkiIpcSubmissionGuard accepted(&task, nullptr, handoff.data(), count, WKI_NODE_INVALID);
    for (uint16_t i = 0; i < handoff.size(); ++i) {
        auto* file = new (std::nothrow) ker::vfs::File{};
        if (file == nullptr) {
            accepted.cancel();
            for (uint16_t j = 0; j < i; ++j) {
                auto* existing = static_cast<ker::vfs::File*>(task.fd_table.remove(j));
                ipc_release_file_ref(existing);
            }
            return -ENOMEM;
        }
        file->fops = &operations;
        file->private_data = &script;
        file->refcount.store(2, std::memory_order_release);
        static_cast<void>(task.fd_table.insert(i, file));
        task.set_fd_cloexec(i);
        handoff[i] = {.local_fd = i, .file = file};
        ++count;
    }
    ker::vfs::fd_usage_changed_locked(task);
    // The first last-put simulates fatal cleanup before the next commit. Its
    // callback returns only in this test, exposing any duplicate/lost reference.
    uint16_t const COMMITTED = wki_ipc_commit_task_fd_handoff(&task, handoff.data(), count);
    auto* remaining = static_cast<ker::vfs::File*>(task.fd_table.lookup(1));
    bool const OK = COMMITTED == 1 && script.closes == 1 && task.fd_table.lookup(0) == nullptr && !task.get_fd_cloexec(0) &&
                    remaining != nullptr && remaining->refcount.load() == 1 && task.get_fd_cloexec(1) && handoff[0].file == nullptr &&
                    handoff[1].file == nullptr && task.pending_wki_submissions == nullptr;
    if (remaining != nullptr) {
        task.fd_table.remove(1);
        ipc_release_file_ref(remaining);
    }
    return OK && script.closes == 2 ? 0 : -EIO;
}

auto wki_ipc_selftest_pipe_growth_waits_for_write() -> int {
    constexpr uint16_t PEER = 0x7B25;
    IpcSelftestChannels CHANNELS(PEER);
    ker::mod::sched::task::Task task{};
    std::array<int, 2> descriptors{-1, -1};
    ker::vfs::LocalPipePerfSnapshot initial{};
    ker::vfs::vfs_get_local_pipe_perf_snapshot(initial);
    int const CREATE = ker::vfs::vfs_selftest_create_pipe(task, descriptors.data());
    if (CREATE != 0) return CREATE;
    auto* reader = static_cast<ker::vfs::File*>(task.fd_table.lookup(descriptors[0]));
    auto* writer = static_cast<ker::vfs::File*>(task.fd_table.lookup(descriptors[1]));
    writer->refcount.fetch_add(1, std::memory_order_acq_rel);
    task.set_fd_cloexec(static_cast<unsigned>(descriptors[0]));
    constexpr std::array<uint8_t, 3> PREFIX{0x51, 0x52, 0x53};
    constexpr std::array<uint8_t, 2> SUFFIX{0x61, 0x62};
    bool ok = writer->fops->vfs_write(writer, PREFIX.data(), PREFIX.size(), 0) == static_cast<ssize_t>(PREFIX.size());
    ker::vfs::LocalPipePerfSnapshot before{};
    ker::vfs::vfs_get_local_pipe_perf_snapshot(before);
    std::array<WkiIpcFdEntry, 16> map{};
    std::array<WkiIpcTaskFdHandoff, 16> handoff{};
    uint16_t count = 0;
    uint8_t mask = 0;
    WkiIpcSubmissionGuard guard(&task, map.data(), handoff.data(), count, PEER, CHANNELS.identity);
    ok &= wki_ipc_export_task_fds(&task, PEER, CHANNELS.identity, map.data(), &count, handoff.data(), &mask) == 0 && count == 1;
    ker::vfs::LocalPipePerfSnapshot prepared{};
    ker::vfs::vfs_get_local_pipe_perf_snapshot(prepared);
    ok &= prepared.capacity_bytes == before.capacity_bytes && prepared.buffered_bytes == before.buffered_bytes;
    guard.cancel();
    ker::vfs::LocalPipePerfSnapshot rejected{};
    ker::vfs::vfs_get_local_pipe_perf_snapshot(rejected);
    ok &= rejected.capacity_bytes == before.capacity_bytes && rejected.buffered_bytes == before.buffered_bytes &&
          task.fd_table.lookup(descriptors[1]) == writer && writer->refcount.load() == 2;
    count = 0;
    guard.arm();
    ok &= wki_ipc_export_task_fds(&task, PEER, CHANNELS.identity, map.data(), &count, handoff.data(), &mask) == 0 && count == 1;
    ok &= wki_ipc_commit_task_fd_handoff(&task, handoff.data(), count) == 1;
    wki_ipc_activate_task_fds(map.data(), count, PEER);
    ok &= export_pipe_write_nonblocking(writer, SUFFIX.data(), 0) == 0 && export_pipe_write_nonblocking(writer, nullptr, 1) == -EBADF;
    ker::vfs::LocalPipePerfSnapshot no_write{};
    ker::vfs::vfs_get_local_pipe_perf_snapshot(no_write);
    ok &= no_write.capacity_bytes == before.capacity_bytes;
    ok &= export_pipe_write_nonblocking(writer, SUFFIX.data(), SUFFIX.size()) == static_cast<ssize_t>(SUFFIX.size());
    ker::vfs::LocalPipePerfSnapshot grown{};
    ker::vfs::vfs_get_local_pipe_perf_snapshot(grown);
    ok &= grown.capacity_bytes == initial.capacity_bytes + WKI_IPC_EXPORT_PIPE_CAPACITY;
    ok &= export_pipe_write_nonblocking(writer, SUFFIX.data(), SUFFIX.size()) == static_cast<ssize_t>(SUFFIX.size());
    ker::vfs::LocalPipePerfSnapshot repeated{};
    ker::vfs::vfs_get_local_pipe_perf_snapshot(repeated);
    ok &= repeated.capacity_bytes == grown.capacity_bytes;
    std::array<uint8_t, 7> data{};
    ok &= reader->fops->vfs_read(reader, data.data(), data.size(), 0) == static_cast<ssize_t>(data.size());
    ok &= data == std::array<uint8_t, 7>{0x51, 0x52, 0x53, 0x61, 0x62, 0x61, 0x62};
    guard.cancel();
    for (int descriptor : descriptors) {
        auto* file = static_cast<ker::vfs::File*>(task.fd_table.remove(descriptor));
        ipc_release_file_ref(file);
    }
    ipc_release_file_ref(writer);
    wki_ipc_cleanup_for_peer(PEER);
    ker::vfs::LocalPipePerfSnapshot final{};
    ker::vfs::vfs_get_local_pipe_perf_snapshot(final);
    ok &= final.active_pipes == initial.active_pipes && final.capacity_bytes == initial.capacity_bytes &&
          final.buffered_bytes == initial.buffered_bytes;
    return ok ? 0 : -EIO;
}

auto wki_ipc_selftest_submission_guard_cancellation() -> int {
    constexpr uint16_t PEER = 0x7B24;
    IpcSelftestChannels CHANNELS(PEER);
    bool ok = true;
    for (uint16_t number : {uint16_t{1}, uint16_t{16}}) {
        ker::mod::sched::task::Task task{};
        auto* file = new (std::nothrow) ker::vfs::File{};
        if (file == nullptr) return -ENOMEM;
        file->fs_type = ker::vfs::FSType::SOCKET;
        file->fops = &g_ipc_selftest_file_ops;
        file->refcount.store(number, std::memory_order_release);
        for (uint16_t i = 0; i < number; ++i) static_cast<void>(task.fd_table.insert(i, file));
        std::array<WkiIpcFdEntry, 16> map{};
        std::array<WkiIpcTaskFdHandoff, 16> handoff{};
        uint16_t count = 0;
        uint8_t mask = 0;
        uint64_t flags = s_ipc_lock.lock_irqsave();
        size_t const BEFORE = g_ipc_exports.size();
        s_ipc_lock.unlock_irqrestore(flags);
        {
            WkiIpcSubmissionGuard outer(&task, map.data(), handoff.data(), count, PEER, CHANNELS.identity);
            ok &= task.pending_wki_submissions == &outer;
            // Empty/pre-publication cancellation, followed by retry re-arm.
            wki_ipc_cancel_submission_guards(task);
            ok &= task.pending_wki_submissions == nullptr;
            outer.arm();
            ok &=
                wki_ipc_export_task_fds(&task, PEER, CHANNELS.identity, map.data(), &count, handoff.data(), &mask) == 0 && count == number;
            ok &= file->refcount.load(std::memory_order_acquire) == 3 * number;
            {
                // The direct submitter takes ownership before disarming its
                // caller. Fatal cleanup must find the inner guard alone.
                WkiIpcSubmissionGuard inner(&task, map.data(), handoff.data(), count, PEER, CHANNELS.identity);
                outer.disarm();
                ok &= task.pending_wki_submissions == &inner;
                wki_ipc_cancel_submission_guards(task);
                ok &= task.pending_wki_submissions == nullptr;
            }
            ok &= file->refcount.load(std::memory_order_acquire) == number;
            for (uint16_t i = 0; i < number; ++i) ok &= handoff[i].file == nullptr && task.fd_table.lookup(i) == file;
            // A disarmed outer destructor cannot reclaim the same identities.
        }
        flags = s_ipc_lock.lock_irqsave();
        ok &= g_ipc_exports.size() == BEFORE;
        s_ipc_lock.unlock_irqrestore(flags);
        ok &= file->refcount.load(std::memory_order_acquire) == number;
        for (uint16_t i = 0; i < number; ++i) {
            task.fd_table.remove(i);
            ipc_release_file_ref(file);
        }
        wki_ipc_cleanup_for_peer(PEER);
    }
    return ok ? 0 : -EIO;
}

auto wki_ipc_selftest_submission_guard_reentrant_put() -> int {
    ker::mod::sched::task::Task task{};
    struct Script {
        ker::mod::sched::task::Task* executor;
        unsigned closes = 0;
    } script{.executor = &task};
    ker::vfs::FileOperations operations{};
    operations.vfs_close = [](ker::vfs::File* file) -> int {
        auto& state = *static_cast<Script*>(file->private_data);
        ++state.closes;
        wki_ipc_cancel_submission_guards(*state.executor);
        return 0;
    };
    std::array<WkiIpcTaskFdHandoff, 2> handoff{};
    uint16_t count = 0;
    {
        WkiIpcSubmissionGuard guard(&task, nullptr, handoff.data(), count, WKI_NODE_INVALID);
        for (auto& entry : handoff) {
            auto* file = new (std::nothrow) ker::vfs::File{};
            if (file == nullptr) return -ENOMEM;
            file->fops = &operations;
            file->private_data = &script;
            file->refcount.store(1, std::memory_order_release);
            entry.file = file;
            ++count;
        }
        guard.cancel();
        if (task.pending_wki_submissions != nullptr || handoff[0].file != nullptr || handoff[1].file != nullptr) return -EIO;
    }
    return script.closes == 2 ? 0 : -EIO;
}

auto wki_ipc_selftest_private_rdma_preparation_unwinds() -> int {
    // No authenticated IPC session is present in this isolated boot fixture.
    // Private preparation must leave no raw provider registration or storage.
    ker::mod::sched::task::Task task{};
    for (int phase : {0, 1}) {
        IpcFdSnapshot snapshot(task);
        if (snapshot.prepare_exports(1) != 0) return -ENOMEM;
        auto* exp = snapshot.prepared_exports[0];
        exp->resource_id = 0x7D22;
        exp->res_type = ResourceType::IPC_PIPE;
        prepare_export_pipe_rdma(*exp, snapshot.prepared_backlogs[0], nullptr, phase);
        if (exp->pipe_rdma_enabled || exp->pipe_rdma_region != nullptr || exp->pipe_rdma_rkey != 0 ||
            snapshot.prepared_backlogs[0] != nullptr)
            return -EIO;
        wki_ipc_cancel_fd_snapshots(task);
        if (task.pending_wki_fd_snapshots != nullptr || snapshot.prepared_exports[0] != nullptr || snapshot.prepared_backlogs[0] != nullptr)
            return -EIO;
    }
    return 0;
}

auto wki_ipc_selftest_private_export_allocation_unwinds() -> int {
    ker::mod::sched::task::Task task{};
    auto* file = new (std::nothrow) ker::vfs::File{};
    if (file == nullptr) return -ENOMEM;
    file->refcount.store(1, std::memory_order_release);
    file->fops = &g_ipc_selftest_file_ops;
    static_cast<void>(task.fd_table.insert(0, file));
    uint64_t flags = s_ipc_lock.lock_irqsave();
    size_t const BEFORE = g_ipc_exports.size();
    s_ipc_lock.unlock_irqrestore(flags);
    bool ok = true;
    for (int fail_at : {0, 1, 15, -1}) {
        int const FREES_BEFORE = g_ipc_export_selftest_frees.load(std::memory_order_acquire);
        unsigned const ALLOCATED = fail_at < 0 ? 16U : static_cast<unsigned>(fail_at);
        {
            IpcFdSnapshot snapshot(task);
            ok &= snapshot.prepare_exports(16, fail_at) == (fail_at < 0 ? 0 : -ENOMEM);
            unsigned prepared = 0;
            for (auto* exp : snapshot.prepared_exports) {
                if (exp != nullptr) {
                    ++prepared;
                    ok &= exp->file == nullptr && exp->registry_previous == nullptr && exp->registry_next == nullptr;
                }
            }
            ok &= prepared == ALLOCATED && file->refcount.load(std::memory_order_acquire) == 2;
            wki_ipc_cancel_fd_snapshots(task);
            ok &= task.pending_wki_fd_snapshots == nullptr && file->refcount.load(std::memory_order_acquire) == 1;
            for (auto* exp : snapshot.prepared_exports) ok &= exp == nullptr;
        }
        ok &= g_ipc_export_selftest_frees.load(std::memory_order_acquire) - FREES_BEFORE == static_cast<int>(ALLOCATED);
    }
    flags = s_ipc_lock.lock_irqsave();
    ok &= g_ipc_exports.size() == BEFORE;
    s_ipc_lock.unlock_irqrestore(flags);
    ok &= task.fd_table.lookup(0) == file;
    task.fd_table.remove(0);
    ipc_release_file_ref(file);
    return ok ? 0 : -EIO;
}

auto wki_ipc_selftest_intrusive_export_registry() -> int {
    // More than the cleanup batch size must remain a valid population.
    std::array<WkiIpcExport, WKI_IPC_MAX_EXPORTS + 3> entries{};
    IpcExportRegistry registry;
    bool ok = registry.begin() == registry.end() && registry.size() == 0;
    for (size_t i = 0; i < entries.size(); ++i) {
        entries[i].resource_id = static_cast<uint32_t>(i + 1);
        registry.push_back(&entries[i]);
    }
    ok &= registry.size() == entries.size();
    size_t index = 0;
    for (auto* exp : registry) ok &= exp == &entries[index++];
    ok &= index == entries.size();
    ok &= std::ranges::any_of(registry, [](const auto* exp) { return exp->resource_id == WKI_IPC_MAX_EXPORTS + 3; });
    // Unlink a middle node, the head, then the tail; all links must clear.
    auto next = registry.erase(IpcExportRegistry::Iterator(&entries[1]));
    ok &= *next == &entries[2] && entries[0].registry_next == &entries[2] && entries[2].registry_previous == &entries[0];
    registry.erase(registry.begin());
    ok &= *registry.begin() == &entries[2] && entries[2].registry_previous == nullptr;
    next = registry.erase(IpcExportRegistry::Iterator(&entries.back()));
    ok &= next == registry.end() && entries[entries.size() - 2].registry_next == nullptr;
    ok &= entries[0].registry_next == nullptr && entries[1].registry_previous == nullptr && entries[1].registry_next == nullptr &&
          entries.back().registry_previous == nullptr && registry.size() == entries.size() - 3;
    // Erase while iterating, then reuse a retired object without touching its
    // independent pump/retirement link.
    for (auto it = registry.begin(); it != registry.end();) it = registry.erase(it);
    ok &= registry.size() == 0 && registry.begin() == registry.end();
    entries[0].pump_next = &entries[1];
    registry.push_back(&entries[0]);
    ok &= registry.size() == 1 && *registry.begin() == &entries[0];
    registry.erase(registry.begin());
    ok &= registry.begin() == registry.end() && entries[0].pump_next == &entries[1];
    return ok ? 0 : -EIO;
}

auto wki_ipc_selftest_pumps_wait_for_acceptance() -> int {
    constexpr uint16_t PEER = 0x7B22;
    IpcSelftestChannels CHANNELS(PEER);
    ker::mod::sched::task::Task task{};
    auto* file = new (std::nothrow) ker::vfs::File{};
    if (file == nullptr) return -ENOMEM;
    auto ops = g_ipc_selftest_file_ops;
    ops.vfs_read = [](ker::vfs::File*, void*, size_t, size_t) -> ssize_t {
        g_ipc_accept_selftest_reads.fetch_add(1, std::memory_order_acq_rel);
        return 0;
    };
    file->fops = &ops;
    file->fs_type = ker::vfs::FSType::SOCKET;
    file->refcount.store(1, std::memory_order_release);
    static_cast<void>(task.fd_table.insert(0, file));
    int const READS_BEFORE = g_ipc_accept_selftest_reads.load(std::memory_order_acquire);
    std::array<WkiIpcFdEntry, MAX_SUBMIT_IPC_FDS> map{};
    uint16_t count = 0;
    uint8_t mask = 0;
    bool ok = wki_ipc_export_task_fds(&task, PEER, CHANNELS.identity, map.data(), &count, nullptr, &mask) == 0;
    ok &= count == 1 && mask == 1;
    if (count != 1) {
        task.fd_table.remove(0);
        ipc_release_file_ref(file);
        return -EIO;
    }
    wki_ipc_activate_task_fds(map.data(), count, PEER + 1);
    uint64_t flags = s_ipc_lock.lock_irqsave();
    auto* exp = find_export_by_resource_id(map[0].resource_id);
    ok &= exp != nullptr && exp->pump_pending_accept && !exp->pump_queued && !exp->pump_running.load(std::memory_order_acquire) &&
          exp->pump_task == nullptr && !export_has_pipe_pump_slot_locked(exp);
    s_ipc_lock.unlock_irqrestore(flags);
    // Rejected submission retires an unaccepted exporter without a read or
    // loss of the original FD's sole ownership reference.
    wki_ipc_cleanup_exported_fds(map.data(), count, PEER, CHANNELS.identity);
    flags = s_ipc_lock.lock_irqsave();
    ok &= find_export_by_resource_id(map[0].resource_id) == nullptr;
    s_ipc_lock.unlock_irqrestore(flags);
    ok &= g_ipc_accept_selftest_reads.load(std::memory_order_acquire) == READS_BEFORE && task.fd_table.lookup(0) == file &&
          file->refcount.load(std::memory_order_acquire) == 1;
    // Claim itself is peer-scoped and idempotent. Keep the exporter under the
    // lock here so no test worker consumes its deliberately synthetic socket.
    count = 0;
    ok &= wki_ipc_export_task_fds(&task, PEER, CHANNELS.identity, map.data(), &count, nullptr, &mask) == 0 && count == 1;
    flags = s_ipc_lock.lock_irqsave();
    exp = find_export_by_resource_id(map[0].resource_id);
    ok &= claim_accepted_pipe_pump_locked(map[0].resource_id, PEER + 1) == nullptr;
    ok &= exp != nullptr && claim_accepted_pipe_pump_locked(map[0].resource_id, PEER) == exp && !exp->pump_pending_accept;
    ok &= claim_accepted_pipe_pump_locked(map[0].resource_id, PEER) == nullptr;
    s_ipc_lock.unlock_irqrestore(flags);
    wki_ipc_cleanup_exported_fds(map.data(), count, PEER, CHANNELS.identity);
    // Lowest FD selects the sole pump for aliased PTY File identity.
    ops.vfs_isatty = [](ker::vfs::File*) -> bool { return true; };
    file->fs_type = ker::vfs::FSType::DEVFS;
    file->refcount.fetch_add(1, std::memory_order_acq_rel);
    static_cast<void>(task.fd_table.insert(2, file));
    {
        IpcFdSnapshot snapshot(task);
        IpcFdExportPlan plan;
        ok &= plan_ipc_fd_export(snapshot, plan) == 0 && plan.count == 2 && plan.pump_on_accept[0] && !plan.pump_on_accept[2];
    }
    task.fd_table.remove(2);
    ipc_release_file_ref(file);
    task.fd_table.remove(0);
    ipc_release_file_ref(file);
    return ok ? 0 : -EIO;
}

auto wki_ipc_selftest_export_admission_is_atomic() -> int {
    ker::mod::sched::task::Task task{};
    auto* files = new (std::nothrow) std::array<ker::vfs::File, 17>{};
    if (files == nullptr) return -ENOMEM;
    for (auto& file : *files) {
        file.fs_type = ker::vfs::FSType::SOCKET;
        file.refcount.store(1, std::memory_order_release);
    }
    bool ok = true;
    for (unsigned count : {0U, 1U, 16U, 17U}) {
        for (unsigned i = 0; i < count; ++i) static_cast<void>(task.fd_table.insert(i + 3, &(*files)[i]));
        {
            IpcFdSnapshot snapshot(task);
            IpcFdExportPlan plan;
            int const RESULT = plan_ipc_fd_export(snapshot, plan);
            ok &= count <= MAX_SUBMIT_IPC_FDS ? RESULT == 0 && plan.count == count : RESULT == -ENOTSUP;
        }
        for (unsigned i = 0; i < count; ++i) {
            ok &= (*files)[i].refcount.load(std::memory_order_acquire) == 1;
            task.fd_table.remove(i + 3);
        }
    }
    for (unsigned i = 0; i < files->size(); ++i) static_cast<void>(task.fd_table.insert(i + 3, &(*files)[i]));
    constexpr unsigned NOFILE = static_cast<unsigned>(ker::abi::resource::Id::NOFILE);
    task.resources.commit_extent(NOFILE, files->size());
    ok &= task.resources.set(NOFILE, {0, 0}, true, task.FD_TABLE_SIZE) == 0;
    std::array<WkiIpcFdEntry, MAX_SUBMIT_IPC_FDS> map{};
    std::array<WkiIpcTaskFdHandoff, MAX_SUBMIT_IPC_FDS> handoff{};
    for (auto& entry : map) entry.resource_id = 0xBAD22;
    for (auto& entry : handoff) entry.local_fd = UINT16_MAX;
    uint16_t count = 99;
    uint8_t mask = UINT8_MAX;
    uint64_t flags = s_ipc_lock.lock_irqsave();
    size_t const EXPORTS_BEFORE = g_ipc_exports.size();
    s_ipc_lock.unlock_irqrestore(flags);
    ok &= wki_ipc_export_task_fds(&task, 0x7222, {}, map.data(), &count, handoff.data(), &mask) == -ENOTSUP;
    ok &= count == 0 && mask == 0 && task.fd_table.size() == 17;
    ok &= task.resources.entries[NOFILE].current == 17 && task.resources.entries[NOFILE].peak == 17 &&
          task.resources.entries[NOFILE].denied == 0;
    flags = s_ipc_lock.lock_irqsave();
    ok &= g_ipc_exports.size() == EXPORTS_BEFORE;
    s_ipc_lock.unlock_irqrestore(flags);
    for (const auto& entry : map) ok &= entry.resource_id == 0xBAD22;
    for (const auto& entry : handoff) ok &= entry.local_fd == UINT16_MAX && entry.file == nullptr;
    for (unsigned i = 0; i < files->size(); ++i) {
        ok &= task.fd_table.lookup(i + 3) == &(*files)[i] && (*files)[i].refcount.load(std::memory_order_acquire) == 1;
        task.fd_table.remove(i + 3);
    }
    // Regular stdio cannot be captured safely even with initial FSIZE infinity:
    // the receiver may later lower its limit. Non-stdio files cannot disappear.
    (*files)[0].fs_type = ker::vfs::FSType::TMPFS;
    for (unsigned fd : {0U, 1U, 2U, 3U}) {
        static_cast<void>(task.fd_table.insert(fd, &(*files)[0]));
        {
            IpcFdSnapshot snapshot(task);
            IpcFdExportPlan plan;
            ok &= plan_ipc_fd_export(snapshot, plan) == -ENOTSUP;
        }
        task.set_fd_cloexec(fd);
        {
            IpcFdSnapshot snapshot(task);
            IpcFdExportPlan plan;
            ok &= plan_ipc_fd_export(snapshot, plan) == 0 && plan.count == 0 && plan.stdio_mask == 0;
        }
        task.clear_fd_cloexec(fd);
        task.fd_table.remove(fd);
    }
    // Already-held pipe authority can be relayed without native pipe casts.
    // Descriptor selection, access direction and CLOEXEC remain unchanged.
    for (bool read_end : {true, false}) {
        auto& file = (*files)[0];
        file.fops = read_end ? &g_proxy_pipe_read_fops : &g_proxy_pipe_write_fops;
        file.open_flags = (read_end ? 0 : 1) | WKI_IPC_O_NONBLOCK;
        for (unsigned fd : {0U, 1U, 2U, 3U}) {
            static_cast<void>(task.fd_table.insert(fd, &file));
            {
                IpcFdSnapshot snapshot(task);
                IpcFdExportPlan plan;
                ok &= plan_ipc_fd_export(snapshot, plan) == 0 && plan.count == 1 && plan.types[fd] == ResourceType::IPC_PIPE &&
                      plan.pump_on_accept[fd] == read_end && plan.stdio_mask == (fd < 3 ? 1U << fd : 0U);
            }
            task.set_fd_cloexec(fd);
            {
                IpcFdSnapshot snapshot(task);
                IpcFdExportPlan plan;
                ok &= plan_ipc_fd_export(snapshot, plan) == 0 && plan.count == 0 && plan.stdio_mask == 0;
            }
            task.clear_fd_cloexec(fd);
            task.fd_table.remove(fd);
            ok &= file.refcount.load(std::memory_order_acquire) == 1 && file.open_flags == ((read_end ? 0 : 1) | WKI_IPC_O_NONBLOCK);
        }
    }
    // A proxy epoll also retains local callback state and cannot be dropped.
    (*files)[0].fops = &g_proxy_epoll_fops;
    static_cast<void>(task.fd_table.insert(3, &(*files)[0]));
    {
        IpcFdSnapshot snapshot(task);
        IpcFdExportPlan plan;
        ok &= plan_ipc_fd_export(snapshot, plan) == -ENOTSUP;
    }
    task.fd_table.remove(3);
    delete files;
    return ok ? 0 : -EIO;
}

auto wki_ipc_selftest_fd_snapshot_lifetime_and_cloexec() -> int {
    ker::mod::sched::task::Task task{};
    std::array<ker::vfs::File*, 3> files{};
    for (auto& file : files) {
        file = new (std::nothrow) ker::vfs::File{};
        if (file == nullptr) {
            for (auto* allocated : files) delete allocated;
            return -ENOMEM;
        }
        file->fops = &g_ipc_selftest_file_ops;
        file->fs_type = ker::vfs::FSType::DEVFS;
        file->refcount.store(1, std::memory_order_release);
    }
    static_cast<void>(task.fd_table.insert(0, files[0]));
    static_cast<void>(task.fd_table.insert(1, files[1]));
    task.set_fd_cloexec(1);
    int const BEFORE = g_ipc_file_selftest_closes.load(std::memory_order_acquire);
    std::array<WkiIpcFdEntry, 16> map{};
    uint16_t count = 99;
    uint8_t mask = UINT8_MAX;
    // These are non-IPC Files: export owns no resources, but must still
    // preserve stdin and filter CLOEXEC stdout in its submission metadata.
    int const RESULT = wki_ipc_export_task_fds(&task, WKI_NODE_INVALID, {}, map.data(), &count, nullptr, &mask);
    bool ok = RESULT == 0 && count == 0 && mask == 1;
    {
        IpcFdSnapshot snapshot(task);
        ok &= snapshot.files[0] == files[0] && snapshot.files[1] == nullptr;
        ok &= files[0]->refcount.load(std::memory_order_acquire) == 2;
        ok &= files[1]->refcount.load(std::memory_order_acquire) == 1;
        static_cast<void>(task.fd_table.insert(0, files[2]));
        ipc_release_file_ref(files[0]);
        ok &= snapshot.files[0] == files[0] && files[0]->refcount.load(std::memory_order_acquire) == 1;
        wki_ipc_cancel_fd_snapshots(task);
        ok &= task.pending_wki_fd_snapshots == nullptr && snapshot.files[0] == nullptr;
        ok &= g_ipc_file_selftest_closes.load(std::memory_order_acquire) - BEFORE == 1;
    }
    ipc_release_file_ref(static_cast<ker::vfs::File*>(task.fd_table.remove(0)));
    ipc_release_file_ref(static_cast<ker::vfs::File*>(task.fd_table.remove(1)));
    ok &= g_ipc_file_selftest_closes.load(std::memory_order_acquire) - BEFORE == 3;
    return ok ? 0 : -EIO;
}

auto wki_ipc_selftest_task_fd_handoff_transfers_exact_owner() -> int {
    constexpr uint16_t FD = 9;
    int const CLOSES_BEFORE = g_ipc_file_selftest_closes.load(std::memory_order_acquire);

    ker::mod::sched::task::Task task{};
    auto* exact = new (std::nothrow) ker::vfs::File{};
    if (exact == nullptr) {
        return -ENOMEM;
    }
    exact->fops = &g_ipc_selftest_file_ops;
    // One fd-table owner plus the temporary handoff identity pin.
    exact->refcount.store(2, std::memory_order_release);
    if (!task.fd_table.insert(FD, exact)) {
        delete exact;
        return -ENOMEM;
    }
    task.set_fd_cloexec(FD);
    constexpr unsigned NOFILE = static_cast<unsigned>(ker::abi::resource::Id::NOFILE);
    task.resources.commit_extent(NOFILE, 1);

    WkiIpcTaskFdHandoff exact_handoff{.local_fd = FD, .file = exact};
    uint16_t const EXACT_COMMITTED = wki_ipc_commit_task_fd_handoff(&task, &exact_handoff, 1);
    bool const EXACT_REMOVED = task.fd_table.lookup(FD) == nullptr && !task.get_fd_cloexec(FD) && exact_handoff.file == nullptr &&
                               task.resources.entries[NOFILE].current == 0 && task.resources.entries[NOFILE].peak == 1;

    auto* pinned = new (std::nothrow) ker::vfs::File{};
    auto* replacement = new (std::nothrow) ker::vfs::File{};
    if (pinned == nullptr || replacement == nullptr) {
        delete pinned;
        delete replacement;
        return -ENOMEM;
    }
    pinned->fops = &g_ipc_selftest_file_ops;
    pinned->refcount.store(1, std::memory_order_release);
    replacement->fops = &g_ipc_selftest_file_ops;
    replacement->refcount.store(1, std::memory_order_release);
    if (!task.fd_table.insert(FD, replacement)) {
        delete pinned;
        delete replacement;
        return -ENOMEM;
    }

    WkiIpcTaskFdHandoff stale_handoff{.local_fd = FD, .file = pinned};
    uint16_t const STALE_COMMITTED = wki_ipc_commit_task_fd_handoff(&task, &stale_handoff, 1);
    bool const REPLACEMENT_PRESERVED = task.fd_table.lookup(FD) == replacement && stale_handoff.file == nullptr;
    auto* removed_replacement = static_cast<ker::vfs::File*>(task.fd_table.remove(FD));
    ipc_release_file_ref(removed_replacement);

    auto* retained = new (std::nothrow) ker::vfs::File{};
    if (retained == nullptr) {
        return -ENOMEM;
    }
    retained->fops = &g_ipc_selftest_file_ops;
    retained->refcount.store(2, std::memory_order_release);
    if (!task.fd_table.insert(FD, retained)) {
        delete retained;
        return -ENOMEM;
    }
    WkiIpcTaskFdHandoff abandoned_handoff{.local_fd = FD, .file = retained};
    wki_ipc_release_task_fd_handoff(&abandoned_handoff, 1);
    bool const ABANDON_PRESERVED = task.fd_table.lookup(FD) == retained && abandoned_handoff.file == nullptr;
    auto* removed_retained = static_cast<ker::vfs::File*>(task.fd_table.remove(FD));
    ipc_release_file_ref(removed_retained);

    int const CLOSES = g_ipc_file_selftest_closes.load(std::memory_order_acquire) - CLOSES_BEFORE;
    return EXACT_COMMITTED == 1 && EXACT_REMOVED && STALE_COMMITTED == 0 && REPLACEMENT_PRESERVED && ABANDON_PRESERVED && CLOSES == 4
               ? 0
               : -EIO;
}

auto wki_ipc_selftest_failed_export_write_releases_all_file_refs() -> int {
    constexpr uint32_t RESOURCE_ID = 0x6A6B;

    auto* file = new (std::nothrow) ker::vfs::File{};
    auto* exp = new (std::nothrow) WkiIpcExport{};
    auto* backlog = new (std::nothrow) ExportPipeWriteBacklog{};
    if (file == nullptr || exp == nullptr || backlog == nullptr) {
        delete file;
        delete exp;
        delete backlog;
        return -ENOMEM;
    }

    file->fops = &g_ipc_selftest_file_ops;
    // One reference belongs to the export and one models the flush worker's
    // acquired reference at the point where its write returns a fatal error.
    file->refcount.store(2, std::memory_order_release);
    exp->active = true;
    exp->resource_id = RESOURCE_ID;
    exp->file = file;
    backlog->exp = exp;
    backlog->resource_id = RESOURCE_ID;

    int const EXPORT_FREES_BEFORE = g_ipc_export_selftest_frees.load(std::memory_order_acquire);
    int const FILE_CLOSES_BEFORE = g_ipc_file_selftest_closes.load(std::memory_order_acquire);
    FailedExportPipeWriteRetirement retirement;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        g_ipc_exports.push_back(exp);
        g_export_pipe_write_backlogs.push_back(backlog);
        retirement = retire_failed_export_pipe_write_locked(backlog, exp, file);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    ipc_release_file_ref(file);
    ipc_release_file_ref(retirement.export_file);
    free_export_pipe_write_backlog(backlog);
    free_retired_exports(retirement.retired_exports);

    int const EXPORT_FREES = g_ipc_export_selftest_frees.load(std::memory_order_acquire) - EXPORT_FREES_BEFORE;
    int const FILE_CLOSES = g_ipc_file_selftest_closes.load(std::memory_order_acquire) - FILE_CLOSES_BEFORE;
    return EXPORT_FREES == 1 && FILE_CLOSES == 1 ? 0 : -EIO;
}

auto wki_ipc_selftest_message_pipe_flow_updates_proxy() -> int {
    constexpr uint16_t HOME_NODE = 0x6A6C;
    constexpr uint32_t RESOURCE_ID = 0x6A6D;
    constexpr uint32_t INITIAL_CREDITS = WKI_IPC_MESSAGE_PIPE_WINDOW - 32;

    auto* proxy = new (std::nothrow) ProxyIpcState{};
    if (proxy == nullptr) {
        return -ENOMEM;
    }
    proxy->active.store(true, std::memory_order_release);
    proxy->home_node = HOME_NODE;
    proxy->resource_id = RESOURCE_ID;
    proxy->message_write_flow_control = true;
    proxy->message_write_credits.store(INITIAL_CREDITS, std::memory_order_release);
    proxy->refcount.store(1, std::memory_order_release);

    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        g_ipc_proxies.push_back(proxy);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    auto notify = [&](uint32_t credits, int32_t status) {
        constexpr size_t MSG_SIZE = sizeof(DevOpReqPayload) + sizeof(IpcPipeWriteFlowPayload);
        std::array<uint8_t, MSG_SIZE> msg = {};
        auto* req = reinterpret_cast<DevOpReqPayload*>(msg.data());
        req->op_id = OP_PIPE_WRITE_FLOW;
        req->data_len = sizeof(IpcPipeWriteFlowPayload);
        IpcPipeWriteFlowPayload flow = {
            .resource_id = RESOURCE_ID,
            .credit_bytes = credits,
            .status = status,
        };
        std::memcpy(msg.data() + sizeof(DevOpReqPayload), &flow, sizeof(flow));
        WkiHeader hdr = {};
        hdr.src_node = HOME_NODE;
        hdr.channel_id = WKI_CHAN_RESOURCE;
        handle_ipc_dev_op_req_inline(&hdr, msg.data(), static_cast<uint16_t>(msg.size()), nullptr);
    };

    notify(64, 0);
    bool const CREDIT_CAPPED = proxy->message_write_credits.load(std::memory_order_acquire) == WKI_IPC_MESSAGE_PIPE_WINDOW;
    notify(0, -EPIPE);
    bool const ERROR_PUBLISHED = proxy->message_write_error.load(std::memory_order_acquire) == -EPIPE &&
                                 proxy->message_write_credits.load(std::memory_order_acquire) == 0;

    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        static_cast<void>(proxy_unregister_locked(proxy));
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    proxy->active.store(false, std::memory_order_release);
    proxy_release(proxy);

    return CREDIT_CAPPED && ERROR_PUBLISHED && ipc_dev_op_must_not_drop(OP_PIPE_DATA_FLOW) ? 0 : -EIO;
}

auto wki_ipc_selftest_output_completion_survives_close() -> int {
    constexpr uint16_t HOME_NODE = 0x6A6E;
    constexpr uint32_t RESOURCE_ID = 0x6A6F;
    int const FREES_BEFORE = g_ipc_proxy_selftest_frees.load(std::memory_order_acquire);
    bool valid = true;
    // Private backlog: failed reliable enqueue retains the exact credit and
    // chunk. Success consumes it once; terminal errors remain for retirement.
    ExportPipeWriteBacklog receipt;
    receipt.chunks.push_back(PendingPipeChunk{.len = 16, .offset = 16, .flow_credit = 16});
    for (unsigned attempt = 0; attempt < 3; ++attempt) {
        valid = !complete_export_pipe_flow_receipt_locked(&receipt, false) && receipt.chunks.size() == 1 &&
                receipt.chunks.front().flow_credit == 16 && valid;
    }
    valid = complete_export_pipe_flow_receipt_locked(&receipt, true) && receipt.chunks.empty() && valid;
    receipt.write_error = -EPIPE;
    valid = !complete_export_pipe_flow_receipt_locked(&receipt, false) && receipt.write_error == -EPIPE && valid;
    valid = complete_export_pipe_flow_receipt_locked(&receipt, true) && receipt.write_error == -EPIPE && valid;
    for (unsigned variant = 0; variant < 9; ++variant) {
        unsigned const mode = variant % 3;
        auto* proxy = new (std::nothrow) ProxyIpcState{};
        if (proxy == nullptr) return -ENOMEM;
        proxy->res_type = variant < 3 ? ResourceType::IPC_PIPE : (variant < 6 ? ResourceType::IPC_PTY : ResourceType::IPC_SOCKET);
        proxy->active.store(true, std::memory_order_release);
        proxy->home_node = HOME_NODE;
        proxy->resource_id = RESOURCE_ID;
        proxy->message_write_flow_control = true;
        proxy->bytes_written.store(16, std::memory_order_release);
        proxy->refcount.store(2, std::memory_order_release);  // File and registry
        ker::vfs::File file{};
        file.private_data = proxy;
        WkiIpcOutputCompletion completion;
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            g_ipc_proxies.push_back(proxy);
            completion.retain_locked(proxy);
            s_ipc_lock.unlock_irqrestore(IRQF);
        }
        valid = !completion.ready() && valid;                           // freezes16
        proxy->bytes_written.fetch_add(16, std::memory_order_release);  // later inherited writer
        wki_ipc_detach_proxy_file(&file, proxy);
        valid = file.private_data == nullptr && !completion.ready() && valid;
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            auto* ordinary = find_proxy_by_endpoint_locked(HOME_NODE, RESOURCE_ID);
            valid = ordinary == nullptr && valid;
            s_ipc_lock.unlock_irqrestore(IRQF);
            proxy_release(ordinary);
        }
        auto notify = [&](uint32_t bytes, int32_t status) {
            std::array<uint8_t, sizeof(DevOpReqPayload) + sizeof(IpcPipeWriteFlowPayload)> message{};
            DevOpReqPayload req{.op_id = OP_PIPE_WRITE_FLOW, .data_len = sizeof(IpcPipeWriteFlowPayload)};
            IpcPipeWriteFlowPayload flow{.resource_id = RESOURCE_ID, .credit_bytes = bytes, .status = status};
            std::memcpy(message.data(), &req, sizeof(req));
            std::memcpy(message.data() + sizeof(req), &flow, sizeof(flow));
            WkiHeader hdr{};
            hdr.src_node = HOME_NODE;
            hdr.channel_id = WKI_CHAN_RESOURCE;
            handle_ipc_dev_op_req_inline(&hdr, message.data(), static_cast<uint16_t>(message.size()), nullptr);
        };
        if (mode == 0) {
            // Refunds restore send capacity but are never delivery receipts.
            proxy_grant_message_write_credits(proxy, 16);
            valid = !completion.ready() && valid;
            notify(8, 0);
            valid = !completion.ready() && valid;
            notify(8, 0);
            valid = completion.ready() && proxy->bytes_delivered.load(std::memory_order_acquire) == 16 && valid;
        } else if (mode == 1) {
            notify(0, -EPIPE);
            valid = completion.ready() && valid;
        } else {
            wki_ipc_cleanup_for_peer(HOME_NODE);
            valid = completion.ready() && proxy->message_write_error.load(std::memory_order_acquire) == -EIO && valid;
        }
    }
    return valid && g_ipc_proxy_selftest_frees.load(std::memory_order_acquire) - FREES_BEFORE == 9 ? 0 : -EIO;
}

auto wki_ipc_selftest_pty_delivery_order() -> int {
    constexpr uint32_t GROUP = 0x7C100;
    bool valid = true;
    WkiIpcExport first_export{};
    WkiIpcExport alias_export{};
    first_export.active = true;
    first_export.endpoint_identity = &first_export;
    first_export.resource_id = GROUP;
    alias_export.endpoint_identity = first_export.endpoint_identity;
    alias_export.resource_id = GROUP + 1;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        first_export.endpoint_delivery_group = endpoint_export_delivery_group_locked(first_export);
        g_ipc_exports.push_back(&first_export);
        valid = first_export.endpoint_delivery_group == GROUP && endpoint_export_delivery_group_locked(alias_export) == GROUP;
        alias_export.endpoint_identity = &alias_export;  // other endpoint, including the master
        valid = endpoint_export_delivery_group_locked(alias_export) == GROUP + 1 && valid;
        alias_export.endpoint_identity = first_export.endpoint_identity;
        first_export.active = false;
        valid = endpoint_export_delivery_group_locked(alias_export) == GROUP + 1 && valid;
        g_ipc_exports.erase(IpcExportRegistry::Iterator(&first_export));
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    IpcDevOpWork* first = nullptr;
    IpcDevOpWork* alias = nullptr;
    IpcDevOpWork* other = nullptr;
    {
        uint64_t const IRQF = s_ipc_dev_op_pool_lock.lock_irqsave();
        first = take_ipc_dev_op_work_locked(2, GROUP);
        alias = take_ipc_dev_op_work_locked(1, GROUP);
        other = take_ipc_dev_op_work_locked(1, GROUP + 1);
        s_ipc_dev_op_pool_lock.unlock_irqrestore(IRQF);
    }
    if (first == nullptr || alias == nullptr || other == nullptr) {
        free_ipc_dev_op_work(first);
        free_ipc_dev_op_work(alias);
        free_ipc_dev_op_work(other);
        return -ENOMEM;
    }
    auto ready = [](const IpcDevOpWork* work) {
        uint64_t const IRQF = s_ipc_dev_op_pool_lock.lock_irqsave();
        bool const result = endpoint_delivery_ready_locked(work);
        s_ipc_dev_op_pool_lock.unlock_irqrestore(IRQF);
        return result;
    };
    valid = ready(first) && !ready(alias) && ready(other) && valid;
    valid = ipc_dev_op_work_can_back_pipe_chunk(first, first->payload, 2) &&
            ipc_dev_op_work_can_back_pipe_chunk(alias, alias->payload, 1) && valid;
    ExportPipeWriteBacklog backlog;
    publish_export_pipe_write_chunk_locked(&backlog, first->payload, 2, first, &first, true);
    valid = first == nullptr && !ready(alias) && valid;
    {
        IpcDevOpQueue queue;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        alias->hdr.src_node = 0x7C12;
        other->hdr.src_node = 0x7C12;
        alias->cleanup_epoch = ipc_peer_cleanup_epoch_locked(0x7C12);
        other->cleanup_epoch = alias->cleanup_epoch;
        ipc_dev_op_queue_push_back_locked(queue, alias);
        ipc_dev_op_queue_push_back_locked(queue, other);
        bool ordered_wait = true;
        valid = take_ready_ipc_dev_op_work_locked(queue, ordered_wait) == other && !ordered_wait && queue.count == 1 && valid;
        valid = take_ready_ipc_dev_op_work_locked(queue, ordered_wait) == nullptr && ordered_wait && queue.head == alias && valid;
        valid = ipc_dev_op_queue_pop_front_locked(queue) == alias && queue.count == 0 && valid;
        valid = take_ready_ipc_dev_op_work_locked(queue, ordered_wait) == nullptr && !ordered_wait && valid;
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    backlog.chunks.front().offset = 1;  // partial VFS write retains the token
    valid = !ready(alias) && ready(other) && valid;
    backlog.chunks.front().offset = 2;
    valid = !complete_export_pipe_flow_receipt_locked(&backlog, false) && !ready(alias) && valid;
    valid = complete_export_pipe_flow_receipt_locked(&backlog, true) && backlog.chunks.empty() && ready(alias) && valid;
    // Cancellation uses the same unlink path as successful delivery. Removing
    // the independent endpoint cannot strand either end of the order list.
    free_ipc_dev_op_work(other);
    free_ipc_dev_op_work(alias);

    // Exercise the actual allocator at its reserve boundary without publishing
    // any of these private works or altering the global free count by hand.
    std::array<IpcDevOpWork*, WKI_IPC_DEV_OP_MAX_PENDING> held{};
    size_t count = 0;
    IpcDevOpWork* control = nullptr;
    {
        uint64_t const IRQF = s_ipc_dev_op_pool_lock.lock_irqsave();
        while (count < held.size()) {
            auto* work = take_ipc_dev_op_work_locked(1, GROUP);
            if (work == nullptr) break;
            held[count++] = work;
            valid = work->pipe_chunk_transferable && valid;
        }
        valid = g_ipc_dev_op_free_count == WKI_IPC_DEV_OP_TRANSFER_RESERVE && valid;
        control = take_ipc_dev_op_work_locked(1);
        valid = control != nullptr && !control->pipe_chunk_transferable && valid;
        s_ipc_dev_op_pool_lock.unlock_irqrestore(IRQF);
    }
    free_ipc_dev_op_work(control);
    for (auto* work : std::span(held.data(), count)) free_ipc_dev_op_work(work);
    return valid ? 0 : -EIO;
}

auto wki_ipc_selftest_socket_delivery_fence() -> int {
    constexpr uint32_t GROUP = 0x7C140;
    constexpr uint16_t NODE = 0x7C14;
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = NODE;
    channel.channel_id = WKI_CHAN_IPC_DATA;
    channel.generation = 3;
    channel.tx_seq = 1;
    channel.rx_dispatch_seq = UINT32_MAX;
    WkiChannelIdentity identity{&channel, NODE, WKI_CHAN_IPC_DATA, 3};
    uint32_t snapshot = 99;
    bool valid = wki_ipc_capture_data_sequence(identity, true, snapshot) && snapshot == 1;
    valid = wki_ipc_capture_data_sequence(identity, false, snapshot) && snapshot == UINT32_MAX && valid;
    auto stale = identity;
    ++stale.generation;
    valid = !wki_ipc_capture_data_sequence(stale, true, snapshot) && snapshot == UINT32_MAX && valid;
    std::array<IpcDevOpWork*, 4> work{};
    {
        uint64_t const IRQF = s_ipc_dev_op_pool_lock.lock_irqsave();
        for (size_t i = 0; i < work.size(); ++i) {
            work[i] = take_ipc_dev_op_work_locked(1, i == 3 ? GROUP + 1 : GROUP);
            if (work[i] != nullptr) {
                work[i]->channel_identity = i == 2 ? stale : identity;
                work[i]->hdr.seq_num = i == 1 ? 1 : UINT32_MAX;
            }
        }
        s_ipc_dev_op_pool_lock.unlock_irqrestore(IRQF);
    }
    for (auto* item : work) {
        if (item == nullptr) {
            for (auto* allocated : work) free_ipc_dev_op_work(allocated);
            return -ENOMEM;
        }
    }
    auto pending = [&]() {
        uint64_t const IRQF = s_ipc_dev_op_pool_lock.lock_irqsave();
        bool const RESULT = endpoint_delivery_prefix_pending_locked(GROUP, identity, 1);
        s_ipc_dev_op_pool_lock.unlock_irqrestore(IRQF);
        return RESULT;
    };
    valid = pending() && valid;
    IpcDevOpWork* close = nullptr;
    {
        uint64_t const IRQF = s_ipc_dev_op_pool_lock.lock_irqsave();
        close = take_ipc_dev_op_work_locked(1, GROUP);
        valid = close != nullptr && !endpoint_delivery_ready_locked(close) && valid;
        s_ipc_dev_op_pool_lock.unlock_irqrestore(IRQF);
    }
    ExportPipeWriteBacklog backlog;
    publish_export_pipe_write_chunk_locked(&backlog, work[0]->payload, 1, work[0], &work[0], false);
    valid = work[0] == nullptr && pending() && valid;
    // Delivery ownership persists through the backlog; successful retirement
    // ends this fixed prefix despite a later write, a stale generation and an
    // independent endpoint remaining in the same finite pool.
    backlog.chunks.front().offset = 1;
    release_pending_pipe_chunk(backlog.chunks.front());
    backlog.chunks.pop_front();
    valid = !pending() && valid;
    for (auto* item : work) free_ipc_dev_op_work(item);
    if (close != nullptr) {
        uint64_t const IRQF = s_ipc_dev_op_pool_lock.lock_irqsave();
        valid = endpoint_delivery_ready_locked(close) && valid;
        s_ipc_dev_op_pool_lock.unlock_irqrestore(IRQF);
    }
    free_ipc_dev_op_work(close);
    channel.active = false;
    valid = !wki_ipc_capture_data_sequence(identity, false, snapshot) && valid;
    return valid ? 0 : -EIO;
}

static auto selftest_message_flow_import(ResourceType type) -> int {
    constexpr uint16_t NODE = 0x7C11;
    WkiChannel control{};
    control.active = true;
    control.peer_node_id = NODE;
    control.channel_id = WKI_CHAN_RESOURCE;
    control.generation = 1;
    WkiChannel data{};
    data.active = true;
    data.peer_node_id = NODE;
    data.channel_id = WKI_CHAN_IPC_DATA;
    data.generation = 1;
    auto* task = new (std::nothrow) ker::mod::sched::task::Task{};
    if (task == nullptr) return -ENOMEM;
    std::array<WkiIpcFdEntry, 4> entries{};
    for (uint16_t i = 0; i < entries.size(); ++i) {
        auto& entry = entries[i];
        entry.local_fd = i + 5;
        entry.res_type = static_cast<uint16_t>(type);
        entry.resource_id = 0x7C110 + i;
        entry.home_node = NODE;
        // Writable current peer, writable legacy peer, read-only current peer,
        // and nonblocking read/write current peer, all using the actual import.
        entry.reserved1 = i == 2 ? 0 : WKI_IPC_FD_RDWR;
        if (i != 1) entry.reserved1 |= WKI_IPC_FD_FLAG_PIPE_FLOW_CONTROL;
        if (i == 3) entry.reserved1 |= WKI_IPC_O_NONBLOCK;
    }
    WkiIpcOutputCompletion completion;
    bool valid = wki_ipc_attach_task_fds(task, entries.data(), entries.size(), NODE, {&control, NODE, WKI_CHAN_RESOURCE, 1},
                                         {&data, NODE, WKI_CHAN_IPC_DATA, 1}, &completion);
    for (uint16_t i = 0; i < entries.size(); ++i) {
        auto* file = static_cast<ker::vfs::File*>(task->fd_table.remove(entries[i].local_fd));
        if (file == nullptr) {
            valid = false;
            continue;
        }
        auto* proxy = static_cast<ProxyIpcState*>(file->private_data);
        bool const flow = i == 0 || i == 3;
        valid = proxy != nullptr && proxy->message_write_flow_control == flow &&
                proxy->message_write_credits.load(std::memory_order_acquire) == (flow ? WKI_IPC_MESSAGE_PIPE_WINDOW : 0) &&
                proxy->completion_refs == (flow ? 1U : 0U) &&
                file->fops == (type == ResourceType::IPC_SOCKET ? &g_proxy_socket_fops : &g_proxy_pty_fops) &&
                ((file->open_flags & WKI_IPC_O_NONBLOCK) != 0) == (i == 3) && valid;
        if (proxy != nullptr && type == ResourceType::IPC_SOCKET && i == 3) {
            proxy->message_write_credits.store(0, std::memory_order_release);
            valid = file->fops->vfs_write(file, "x", 1, 0) == -EAGAIN && proxy->bytes_written.load() == 0 &&
                    (file->fops->vfs_poll_check(file, 0x0004) & 0x0004) == 0 && valid;
            proxy_grant_message_write_credits(proxy, 1);
            valid = (file->fops->vfs_poll_check(file, 0x0004) & 0x0004) != 0 && valid;
            proxy->message_write_error.store(-EPIPE, std::memory_order_release);
            int const READY = file->fops->vfs_poll_check(file, 0x0004);
            valid = (READY & 0x0004) == 0 && (READY & 0x0008) != 0 && file->fops->vfs_write(file, "x", 1, 0) == -EPIPE &&
                    proxy->bytes_written.load() == 0 && valid;
        }
        wki_ipc_detach_proxy_file(file, proxy);
        ker::vfs::vfs_put_file(file);
    }
    valid = completion.ready() && valid;
    delete task;
    return valid ? 0 : -EIO;
}

auto wki_ipc_selftest_pty_flow_import() -> int { return selftest_message_flow_import(ResourceType::IPC_PTY); }

auto wki_ipc_selftest_socket_flow_import() -> int { return selftest_message_flow_import(ResourceType::IPC_SOCKET); }

auto wki_ipc_selftest_cleanup_for_peer_drains_over_capacity() -> int {
    constexpr uint16_t NODE_ID = 0x6B6B;
    constexpr uint32_t RESOURCE_BASE = 0x760000;
    constexpr size_t EXPORT_COUNT = WKI_IPC_MAX_EXPORTS + 3;
    constexpr size_t PROXY_COUNT = WKI_IPC_MAX_PROXIES + 3;
    constexpr size_t PENDING_COUNT = WKI_IPC_MAX_EXPORTS + 3;
    constexpr size_t PROXY_CLOSE_COUNT = WKI_IPC_PROXY_CLOSE_CLEANUP_BATCH + 3;

    wki_ipc_cleanup_for_peer(NODE_ID);

    int const EXPORT_FREES_BEFORE = g_ipc_export_selftest_frees.load(std::memory_order_acquire);
    int const PROXY_FREES_BEFORE = g_ipc_proxy_selftest_frees.load(std::memory_order_acquire);
    int const PENDING_FREES_BEFORE = g_ipc_pending_delivery_selftest_frees.load(std::memory_order_acquire);
    int const PROXY_CLOSE_FREES_BEFORE = g_ipc_pending_proxy_close_selftest_frees.load(std::memory_order_acquire);
    int const FILE_CLOSES_BEFORE = g_ipc_file_selftest_closes.load(std::memory_order_acquire);

    bool allocation_ok = true;
    for (size_t i = 0; i < EXPORT_COUNT; ++i) {
        auto* file = new (std::nothrow) ker::vfs::File{};
        auto* exp = new (std::nothrow) WkiIpcExport{};
        if (file == nullptr || exp == nullptr) {
            delete file;
            delete exp;
            allocation_ok = false;
            break;
        }

        file->fops = &g_ipc_selftest_file_ops;
        file->refcount.store(1, std::memory_order_release);
        exp->active = true;
        exp->consumer_node = NODE_ID;
        exp->resource_id = static_cast<uint32_t>(RESOURCE_BASE + i);
        exp->res_type = ResourceType::IPC_PIPE;
        exp->file = file;

        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        g_ipc_exports.push_back(exp);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    for (size_t i = 0; allocation_ok && i < PROXY_COUNT; ++i) {
        auto* proxy = new (std::nothrow) ProxyIpcState{};
        if (proxy == nullptr) {
            allocation_ok = false;
            break;
        }

        proxy->active.store(true, std::memory_order_release);
        proxy->home_node = NODE_ID;
        proxy->resource_id = static_cast<uint32_t>(RESOURCE_BASE + EXPORT_COUNT + i);
        proxy->refcount.store(1, std::memory_order_release);

        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        g_ipc_proxies.push_back(proxy);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    for (size_t i = 0; allocation_ok && i < PENDING_COUNT; ++i) {
        auto* pending = new (std::nothrow) PendingPipeDelivery{};
        if (pending == nullptr) {
            allocation_ok = false;
            break;
        }

        pending->home_node = NODE_ID;
        pending->resource_id = static_cast<uint32_t>(RESOURCE_BASE + EXPORT_COUNT + PROXY_COUNT + i);

        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        g_pending_pipe_deliveries.push_back(pending);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    for (size_t i = 0; allocation_ok && i < PROXY_CLOSE_COUNT; ++i) {
        auto* pending = new (std::nothrow) PendingProxyPipeClose{};
        if (pending == nullptr) {
            allocation_ok = false;
            break;
        }

        pending->home_node = NODE_ID;
        pending->resource_id = static_cast<uint32_t>(RESOURCE_BASE + EXPORT_COUNT + PROXY_COUNT + PENDING_COUNT + i);

        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        g_pending_proxy_pipe_closes.push_back(pending);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    wki_ipc_cleanup_for_peer(NODE_ID);
    if (!allocation_ok) {
        return -ENOMEM;
    }

    size_t leftovers = 0;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        for (auto const* exp : g_ipc_exports) {
            if (exp != nullptr && exp->consumer_node == NODE_ID) {
                leftovers++;
            }
        }
        for (auto const* proxy : g_ipc_proxies) {
            if (proxy != nullptr && proxy->home_node == NODE_ID) {
                leftovers++;
            }
        }
        for (auto const* pending : g_pending_pipe_deliveries) {
            if (pending != nullptr && pending->home_node == NODE_ID) {
                leftovers++;
            }
        }
        for (auto const* pending : g_pending_proxy_pipe_closes) {
            if (pending != nullptr && pending->home_node == NODE_ID) {
                leftovers++;
            }
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    int const EXPORT_FREES = g_ipc_export_selftest_frees.load(std::memory_order_acquire) - EXPORT_FREES_BEFORE;
    int const PROXY_FREES = g_ipc_proxy_selftest_frees.load(std::memory_order_acquire) - PROXY_FREES_BEFORE;
    int const PENDING_FREES = g_ipc_pending_delivery_selftest_frees.load(std::memory_order_acquire) - PENDING_FREES_BEFORE;
    int const PROXY_CLOSE_FREES = g_ipc_pending_proxy_close_selftest_frees.load(std::memory_order_acquire) - PROXY_CLOSE_FREES_BEFORE;
    int const FILE_CLOSES = g_ipc_file_selftest_closes.load(std::memory_order_acquire) - FILE_CLOSES_BEFORE;

    if (leftovers != 0) {
        return -EFAULT;
    }
    if (EXPORT_FREES < static_cast<int>(EXPORT_COUNT) || PROXY_FREES != static_cast<int>(PROXY_COUNT) ||
        PENDING_FREES != static_cast<int>(PENDING_COUNT) || PROXY_CLOSE_FREES != static_cast<int>(PROXY_CLOSE_COUNT) ||
        FILE_CLOSES != static_cast<int>(EXPORT_COUNT)) {
        return -EIO;
    }

    return 0;
}

auto wki_ipc_selftest_cleanup_for_peer_drains_deferred_dev_ops() -> int {
    constexpr uint16_t NODE_ID = 0x6C6C;
    constexpr uint16_t OTHER_NODE_ID = 0x6D6D;
    constexpr size_t DEV_OP_COUNT = WKI_IPC_DEV_OP_CLEANUP_BATCH + 3;
    constexpr uint16_t PAYLOAD_LEN = sizeof(DevOpReqPayload);

    wki_ipc_cleanup_for_peer(NODE_ID);
    wki_ipc_cleanup_for_peer(OTHER_NODE_ID);

    std::array<IpcDevOpWork*, DEV_OP_COUNT> works = {};
    for (size_t i = 0; i < works.size(); ++i) {
        auto* work = alloc_ipc_dev_op_work(PAYLOAD_LEN);
        if (work == nullptr) {
            for (auto* allocated : works) {
                free_ipc_dev_op_work(allocated);
            }
            return -ENOMEM;
        }
        work->hdr.src_node = NODE_ID;
        work->hdr.seq_num = static_cast<uint32_t>(0xA000U + i);
        works.at(i) = work;
    }

    auto* survivor = alloc_ipc_dev_op_work(PAYLOAD_LEN);
    if (survivor == nullptr) {
        for (auto* work : works) {
            free_ipc_dev_op_work(work);
        }
        return -ENOMEM;
    }
    survivor->hdr.src_node = OTHER_NODE_ID;
    survivor->hdr.seq_num = 0xB000U;

    int const FREES_BEFORE = g_ipc_dev_op_work_selftest_frees.load(std::memory_order_acquire);
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        for (size_t i = 0; i < works.size(); ++i) {
            auto* work = works.at(i);
            work->cleanup_epoch = ipc_peer_cleanup_epoch_locked(NODE_ID);
            ipc_dev_op_queue_push_back_locked(g_ipc_dev_op_queues.at(i % g_ipc_dev_op_queues.size()), work);
        }
        survivor->cleanup_epoch = ipc_peer_cleanup_epoch_locked(OTHER_NODE_ID);
        ipc_dev_op_queue_push_back_locked(g_ipc_dev_op_queues.at(0), survivor);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    wki_ipc_cleanup_for_peer(NODE_ID);

    size_t targeted_left = 0;
    size_t survivor_left = 0;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        for (auto& queue : g_ipc_dev_op_queues) {
            for (auto* work = queue.head; work != nullptr; work = work->next) {
                if (work == survivor) {
                    survivor_left++;
                } else if (work->hdr.src_node == NODE_ID) {
                    targeted_left++;
                }
            }
            static_cast<void>(ipc_dev_op_queue_remove_locked(queue, survivor));
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    free_ipc_dev_op_work(survivor);

    IpcDevOpWork popped = {};
    popped.hdr.src_node = NODE_ID;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        popped.cleanup_epoch = ipc_peer_cleanup_epoch_locked(NODE_ID);
        begin_ipc_peer_dev_op_cleanup_locked(NODE_ID);
        bool const ACTIVE_FENCED = ipc_dev_op_work_is_fenced_locked(&popped);
        end_ipc_peer_dev_op_cleanup_locked(NODE_ID);
        bool const EPOCH_FENCED = ipc_dev_op_work_is_fenced_locked(&popped);
        s_ipc_lock.unlock_irqrestore(IRQF);

        if (!ACTIVE_FENCED || !EPOCH_FENCED) {
            return -EIO;
        }
    }

    int const FREES_AFTER = g_ipc_dev_op_work_selftest_frees.load(std::memory_order_acquire);
    int const FREED_DEV_OPS = FREES_AFTER - FREES_BEFORE;
    if (targeted_left != 0 || survivor_left != 1) {
        return -EFAULT;
    }
    if (FREED_DEV_OPS < static_cast<int>(DEV_OP_COUNT + 1)) {
        return -EIO;
    }

    return 0;
}

auto wki_ipc_selftest_large_dev_op_work_coallocates_payload() -> int {
    constexpr uint16_t PAYLOAD_LEN = WKI_ETH_MAX_PAYLOAD;
    auto* work = alloc_ipc_dev_op_work(PAYLOAD_LEN);
    if (work == nullptr) {
        return -ENOMEM;
    }

    auto* const STORAGE = reinterpret_cast<IpcDevOpStorage*>(work);
    bool const VALID =
        work->fixed_pool && work->payload_coallocated && work->payload == STORAGE->payload.data() && work->payload_len == PAYLOAD_LEN;
    if (VALID) {
        work->payload[0] = 0xA5;
        work->payload[work->payload_len - 1] = 0x5A;
    }
    free_ipc_dev_op_work(work);
    return VALID ? 0 : -EIO;
}

auto wki_ipc_selftest_large_dev_op_work_backs_pipe_chunk() -> int {
    constexpr uint16_t FULL_PAYLOAD_LEN = static_cast<uint16_t>(WKI_IPC_PIPE_DATA_HEADER_SIZE + WKI_IPC_PIPE_DATA_MAX_CHUNK);
    auto* work = alloc_ipc_dev_op_work(FULL_PAYLOAD_LEN);
    if (work == nullptr) {
        free_ipc_dev_op_work(work);
        return -ENOMEM;
    }

    auto* const DATA = work->payload + WKI_IPC_PIPE_DATA_HEADER_SIZE;
    size_t const OVERRUN_OFFSET = work->payload_len - WKI_IPC_DEV_OP_TRANSFER_MIN_DATA + 1;
    bool const ELIGIBILITY_VALID =
        ipc_dev_op_work_can_back_pipe_chunk(work, DATA, static_cast<uint16_t>(WKI_IPC_PIPE_DATA_MAX_CHUNK)) &&
        ipc_dev_op_work_can_back_pipe_chunk(work, DATA, 8192) &&
        ipc_dev_op_work_can_back_pipe_chunk(work, DATA, WKI_IPC_DEV_OP_TRANSFER_MIN_DATA) &&
        !ipc_dev_op_work_can_back_pipe_chunk(work, DATA, 0) &&
        !ipc_dev_op_work_can_back_pipe_chunk(work, work->payload + OVERRUN_OFFSET, WKI_IPC_DEV_OP_TRANSFER_MIN_DATA);

    auto* const ORIGINAL_WORK = work;
    bool const FAILURE_RETAINS_OWNER =
        !queue_export_pipe_write_data(UINT32_MAX, DATA, static_cast<uint16_t>(WKI_IPC_PIPE_DATA_MAX_CHUNK), &work) && work == ORIGINAL_WORK;

    ExportPipeWriteBacklog backlog{};
    publish_export_pipe_write_chunk_locked(&backlog, DATA, static_cast<uint16_t>(WKI_IPC_PIPE_DATA_MAX_CHUNK), work, &work, false);
    bool const PUBLICATION_VALID = work == nullptr && backlog.chunks.size() == 1 && backlog.chunks.front().data == DATA &&
                                   backlog.chunks.front().len == WKI_IPC_PIPE_DATA_MAX_CHUNK &&
                                   backlog.chunks.front().dev_op_owner == ORIGINAL_WORK;

    int const FREES_BEFORE = g_ipc_dev_op_work_selftest_frees.load(std::memory_order_acquire);
    auto& chunk = backlog.chunks.front();
    release_pending_pipe_chunk(chunk);
    bool const CHUNK_RESET = chunk.data == nullptr && chunk.len == 0 && chunk.offset == 0 && chunk.dev_op_owner == nullptr;
    backlog.chunks.pop_front();
    int const FREES_AFTER = g_ipc_dev_op_work_selftest_frees.load(std::memory_order_acquire);

    bool const RELEASE_VALID = work == nullptr && FREES_AFTER - FREES_BEFORE == 1 && CHUNK_RESET && backlog.chunks.empty();
    return ELIGIBILITY_VALID && FAILURE_RETAINS_OWNER && PUBLICATION_VALID && RELEASE_VALID ? 0 : -EIO;
}

auto wki_ipc_selftest_dev_op_work_pool_is_bounded() -> int {
    std::array<IpcDevOpWork*, WKI_IPC_DEV_OP_MAX_PENDING> allocated{};
    size_t transferable = 0;
    IpcDevOpWork* reserved_work = nullptr;
    for (auto& work : allocated) {
        work = alloc_ipc_dev_op_work(WKI_ETH_MAX_PAYLOAD);
        if (work == nullptr || !work->fixed_pool || !work->payload_coallocated || work->payload == nullptr) {
            for (auto* item : allocated) {
                free_ipc_dev_op_work(item);
            }
            return -ENOMEM;
        }
        if (work->pipe_chunk_transferable) {
            ++transferable;
        } else {
            reserved_work = work;
        }
    }

    bool const EXHAUSTION_IS_BACKPRESSURE = alloc_ipc_dev_op_work(sizeof(DevOpReqPayload)) == nullptr;
    bool const TRANSFER_RESERVE_VALID = transferable == WKI_IPC_DEV_OP_MAX_PENDING - WKI_IPC_DEV_OP_TRANSFER_RESERVE &&
                                        reserved_work != nullptr &&
                                        !ipc_dev_op_work_can_back_pipe_chunk(reserved_work, reserved_work->payload, 1);
    for (auto* work : allocated) {
        free_ipc_dev_op_work(work);
    }

    IpcDevOpWork* recycled = alloc_ipc_dev_op_work(sizeof(DevOpReqPayload));
    bool const RECYCLED = recycled != nullptr && recycled->fixed_pool && recycled->payload != nullptr;
    free_ipc_dev_op_work(recycled);

    bool const MUST_NOT_DROP_CLASSES = ipc_dev_op_must_not_drop(OP_PIPE_DATA) && ipc_dev_op_must_not_drop(OP_PIPE_DATA_FLOW) &&
                                       ipc_dev_op_must_not_drop(OP_PIPE_CLOSE_WRITE) && ipc_dev_op_must_not_drop(OP_SOCK_CLOSE);
    bool const RESPONSE_COOKIE_CLASSES = !ipc_op_uses_response_cookie(OP_SOCK_CLOSE) && ipc_op_uses_response_cookie(OP_SOCK_ACCEPT) &&
                                         ipc_op_uses_response_cookie(OP_SOCK_SHUTDOWN) &&
                                         ipc_op_uses_response_cookie(OP_SOCK_GETPEERNAME) &&
                                         ipc_op_uses_response_cookie(OP_SOCK_GETSOCKOPT) && ipc_op_uses_response_cookie(OP_SOCK_SETSOCKOPT);
    return EXHAUSTION_IS_BACKPRESSURE && TRANSFER_RESERVE_VALID && RECYCLED && MUST_NOT_DROP_CLASSES && RESPONSE_COOKIE_CLASSES ? 0 : -EIO;
}

auto wki_ipc_selftest_stream_read_contract() -> int {
    constexpr uint16_t HOME = 0x7A39;
    constexpr uint32_t RESOURCE = 0x7A390001;
    for (auto type : {ResourceType::IPC_PIPE, ResourceType::IPC_PTY, ResourceType::IPC_SOCKET}) {
        ProxyIpcState proxy{};
        proxy.active.store(true, std::memory_order_release);
        proxy.res_type = type;
        proxy.home_node = HOME;
        proxy.resource_id = RESOURCE;
        proxy.message_write_flow_control = true;
        std::array<uint8_t, 8> ring{};
        proxy.ring_buf = ring.data();
        proxy.ring_capacity = ring.size();
        ker::vfs::File file{};
        file.private_data = &proxy;
        file.open_flags = WKI_IPC_O_NONBLOCK;
        file.fops = type == ResourceType::IPC_SOCKET ? &g_proxy_socket_fops
                    : type == ResourceType::IPC_PTY  ? &g_proxy_pty_fops
                                                     : &g_proxy_pipe_read_fops;
        auto read = [&](void* buffer, size_t count) { return file.fops->vfs_read(&file, buffer, count, 0); };
        auto cleanup = [&] {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            auto* pending = take_pending_pipe_delivery_locked(HOME, RESOURCE);
            s_ipc_lock.unlock_irqrestore(IRQF);
            free_pending_pipe_delivery(pending);
        };
        cleanup();
        std::array<uint8_t, 16> output{};
        bool ok = read(nullptr, 0) == 0 && read(output.data(), output.size()) == -EAGAIN && read(nullptr, 1) == -EINVAL;
        // Wrapped ring and a partially consumed overflow chunk precede EOF.
        ring[6] = 1;
        ring[7] = 2;
        ring[0] = 3;
        ring[1] = 4;
        proxy.ring_tail.store(6, std::memory_order_release);
        proxy.ring_head.store(10, std::memory_order_release);
        std::array<uint8_t, 4> const OVERFLOW{5, 6, 7, 8};
        if (!queue_pending_pipe_data(HOME, RESOURCE, OVERFLOW.data(), OVERFLOW.size()) || !mark_pending_pipe_write_closed(HOME, RESOURCE)) {
            cleanup();
            return -ENOMEM;
        }
        ok = ok && read(output.data(), output.size()) == 4 && output[0] == 1 && output[3] == 4;
        int const READY = file.fops->vfs_poll_check(&file, dev::pty::POLLIN);
        ok = ok && (READY & dev::pty::POLLIN) != 0 && (READY & dev::pty::POLLHUP) == 0;
        ok = ok && read(output.data(), 2) == 2 && output[0] == 5 && output[1] == 6;
        ok = ok && read(output.data(), output.size()) == 2 && output[0] == 7 && output[1] == 8;
        ok = ok && read(output.data(), output.size()) == 0 && read(nullptr, 0) == 0;
        int const EOF_READY = file.fops->vfs_poll_check(&file, dev::pty::POLLIN);
        ok = ok && (EOF_READY & (dev::pty::POLLIN | dev::pty::POLLHUP)) == (dev::pty::POLLIN | dev::pty::POLLHUP);
        proxy.active.store(false, std::memory_order_release);
        ok = ok && read(output.data(), 1) == -EBADF;
        cleanup();
        if (!ok) return -EIO;
    }
    return 0;
}

auto wki_ipc_selftest_stream_reader_waiters_are_bounded() -> int {
    ProxyIpcState proxy{};
    uint64_t const IRQF = proxy.lock.lock_irqsave();
    bool ok = true;
    for (size_t i = 0; i < proxy.read_waiters.size(); ++i) ok = proxy_register_reader_locked(&proxy, UINT64_MAX - i) && ok;
    ok = !proxy_register_reader_locked(&proxy, UINT64_MAX - ProxyIpcState::MAX_READ_WAITERS) &&
         proxy_register_reader_locked(&proxy, UINT64_MAX) && ok;
    proxy_remove_reader_locked(&proxy, UINT64_MAX - 1);
    ok = proxy_register_reader_locked(&proxy, UINT64_MAX - ProxyIpcState::MAX_READ_WAITERS) && ok;
    proxy.lock.unlock_irqrestore(IRQF);
    proxy_wake_stream_readers(&proxy);
    return ok && std::ranges::all_of(proxy.read_waiters, [](uint64_t pid) { return pid == 0; }) ? 0 : -EIO;
}

auto wki_ipc_selftest_poll_wake_drains_over_capacity() -> int {
    ProxyIpcState proxy{};
    constexpr size_t WAITER_COUNT = WKI_IPC_MAX_POLL_WAKE_WAITERS + 5;

    {
        uint64_t const IRQF = proxy.lock.lock_irqsave();
        for (size_t i = 0; i < WAITER_COUNT; ++i) {
            if (!proxy_register_waiter_locked(proxy.poll_waiters, static_cast<uint64_t>(0x9000 + i))) {
                proxy.lock.unlock_irqrestore(IRQF);
                return -ENOMEM;
            }
        }
        proxy.lock.unlock_irqrestore(IRQF);
    }

    wki_ipc_proxy_wake_poll_waiters(&proxy);

    uint64_t const IRQF = proxy.lock.lock_irqsave();
    size_t const REMAINING = proxy.poll_waiters.size();
    proxy.lock.unlock_irqrestore(IRQF);
    return REMAINING == 0 ? 0 : -EIO;
}

auto wki_ipc_selftest_inactive_proxy_poll_is_terminal() -> int {
    ProxyIpcState proxy{};
    proxy.active.store(false, std::memory_order_release);

    ker::vfs::File read_file{};
    read_file.private_data = &proxy;
    read_file.fops = &g_proxy_pipe_read_fops;

    ker::vfs::File write_file{};
    write_file.private_data = &proxy;
    write_file.fops = &g_proxy_pipe_write_fops;

    int const READ_READY = proxy_pipe_poll_check(&read_file, dev::pty::POLLIN);
    int const WRITE_READY = proxy_pipe_poll_check(&write_file, dev::pty::POLLOUT);
    if ((READ_READY & dev::pty::POLLHUP) == 0) {
        return -EIO;
    }
    if ((WRITE_READY & dev::pty::POLLERR) == 0) {
        return -EIO;
    }
    return 0;
}

auto wki_ipc_selftest_pty_proxy_poll_is_bidirectional() -> int {
    ProxyIpcState proxy{};
    proxy.active.store(true, std::memory_order_release);
    proxy.res_type = ResourceType::IPC_PTY;

    std::array<uint8_t, 8> ring{};
    proxy.ring_buf = ring.data();
    proxy.ring_capacity = static_cast<uint32_t>(ring.size());
    proxy.ring_head.store(1, std::memory_order_release);
    proxy.ring_tail.store(0, std::memory_order_release);

    ker::vfs::File pty_file{};
    pty_file.private_data = &proxy;
    pty_file.fops = &g_proxy_pty_fops;

    int const DATA_READY = proxy_pipe_poll_check(&pty_file, dev::pty::POLLIN | dev::pty::POLLOUT);
    if ((DATA_READY & dev::pty::POLLIN) == 0 || (DATA_READY & dev::pty::POLLOUT) == 0) {
        return -EIO;
    }

    proxy.ring_head.store(0, std::memory_order_release);
    proxy.ring_tail.store(0, std::memory_order_release);
    proxy.write_closed.store(1, std::memory_order_release);

    int const EOF_READY = proxy_pipe_poll_check(&pty_file, dev::pty::POLLIN);
    if ((EOF_READY & dev::pty::POLLIN) == 0 || (EOF_READY & dev::pty::POLLHUP) == 0) {
        return -EIO;
    }

    proxy.active.store(false, std::memory_order_release);

    int const INACTIVE_READY = proxy_pipe_poll_check(&pty_file, dev::pty::POLLIN | dev::pty::POLLOUT);
    if ((INACTIVE_READY & dev::pty::POLLHUP) == 0 || (INACTIVE_READY & dev::pty::POLLERR) == 0) {
        return -EIO;
    }

    return 0;
}

auto wki_ipc_selftest_pty_close_without_export_queues_pending() -> int {
    constexpr uint16_t HOME_NODE = 0x7A31;
    constexpr uint32_t RESOURCE_ID = 0x7A310001U;

    auto clear_pending = [&] {
        PendingPipeDelivery* pending = nullptr;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        pending = take_pending_pipe_delivery_locked(HOME_NODE, RESOURCE_ID);
        s_ipc_lock.unlock_irqrestore(IRQF);
        free_pending_pipe_delivery(pending);
    };

    clear_pending();
    if (!mark_ipc_pty_write_closed_or_pending(HOME_NODE, RESOURCE_ID, 0, true)) {
        clear_pending();
        return -EIO;
    }

    bool const CLOSED = consume_pending_pipe_closed(HOME_NODE, RESOURCE_ID);
    clear_pending();
    return CLOSED ? 0 : -EIO;
}

auto wki_ipc_selftest_pending_close_promotes_on_poll() -> int {
    constexpr uint16_t HOME_NODE = 0x7A32;
    constexpr uint32_t RESOURCE_ID = 0x7A320001U;

    auto clear_pending = [&] {
        PendingPipeDelivery* pending = nullptr;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        pending = take_pending_pipe_delivery_locked(HOME_NODE, RESOURCE_ID);
        s_ipc_lock.unlock_irqrestore(IRQF);
        free_pending_pipe_delivery(pending);
    };

    clear_pending();
    if (!mark_pending_pipe_write_closed(HOME_NODE, RESOURCE_ID)) {
        clear_pending();
        return -ENOMEM;
    }

    ProxyIpcState proxy{};
    proxy.active.store(true, std::memory_order_release);
    proxy.res_type = ResourceType::IPC_PIPE;
    proxy.home_node = HOME_NODE;
    proxy.resource_id = RESOURCE_ID;

    std::array<uint8_t, 8> ring{};
    proxy.ring_buf = ring.data();
    proxy.ring_capacity = static_cast<uint32_t>(ring.size());

    ker::vfs::File read_file{};
    read_file.private_data = &proxy;
    read_file.fops = &g_proxy_pipe_read_fops;

    int const READY = proxy_pipe_poll_check(&read_file, dev::pty::POLLIN);
    bool const CLOSED = proxy.write_closed.load(std::memory_order_acquire) != 0U;
    bool pending_left = false;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        pending_left = find_pending_pipe_delivery_locked(HOME_NODE, RESOURCE_ID) != nullptr;
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    clear_pending();
    if ((READY & dev::pty::POLLHUP) == 0 || (READY & dev::pty::POLLIN) == 0 || !CLOSED || pending_left) {
        return -EIO;
    }
    return 0;
}

auto wki_ipc_selftest_discard_retires_unattached_delivery() -> int {
    constexpr uint16_t HOME_NODE = 0x7A33;
    constexpr uint32_t RESOURCE_ID = 0x7A330001U;

    static_cast<void>(discard_pending_pipe_delivery(HOME_NODE, RESOURCE_ID));
    auto* pending = new (std::nothrow) PendingPipeDelivery{};
    if (pending == nullptr) {
        return -ENOMEM;
    }
    pending->home_node = HOME_NODE;
    pending->resource_id = RESOURCE_ID;
    pending->write_closed = true;

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    g_pending_pipe_deliveries.push_back(pending);
    s_ipc_lock.unlock_irqrestore(IRQF);

    if (!discard_pending_pipe_delivery(HOME_NODE, RESOURCE_ID)) {
        return -EIO;
    }

    uint64_t const VERIFY_IRQF = s_ipc_lock.lock_irqsave();
    bool const REMAINS = find_pending_pipe_delivery_locked(HOME_NODE, RESOURCE_ID) != nullptr;
    s_ipc_lock.unlock_irqrestore(VERIFY_IRQF);
    return REMAINS ? -EIO : 0;
}

auto wki_ipc_selftest_stopped_pump_publishes_discard_fence() -> int {
    constexpr uint16_t TARGET = 0x7A34;
    constexpr uint32_t RESOURCE_ID = 0x7A340001U;

    auto* exp = new (std::nothrow) WkiIpcExport{};
    if (exp == nullptr) {
        return -ENOMEM;
    }

    PipePumpArg pump_arg{};
    PipePumpRetirement retirement{};
    int const FREES_BEFORE = g_ipc_export_selftest_frees.load(std::memory_order_acquire);
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    bool const BUSY = g_pipe_pump_queue_head != nullptr ||
                      std::ranges::any_of(g_ipc_exports, [](const auto* candidate) { return export_can_retire_locked(candidate); });
    if (BUSY) {
        s_ipc_lock.unlock_irqrestore(IRQF);
        delete exp;
        return -EBUSY;
    }

    exp->active = false;
    exp->consumer_node = TARGET;
    exp->resource_id = RESOURCE_ID;
    exp->pump_running.store(true, std::memory_order_release);
    g_ipc_exports.push_back(exp);
    pump_arg.exp.store(exp, std::memory_order_release);

    retirement = collect_pipe_pump_retirement_locked(pump_arg, exp);
    bool const VALID = retirement.retired_exports == exp && retirement.target == TARGET && retirement.resource_id == RESOURCE_ID &&
                       retirement.discard_pending && pump_arg.exp.load(std::memory_order_acquire) == nullptr &&
                       !exp->pump_running.load(std::memory_order_acquire);
    s_ipc_lock.unlock_irqrestore(IRQF);

    // Do not invoke the network-facing finisher for the synthetic node. The
    // production source invariant separately requires both pump exits to use
    // the tested retirement record and publish its discard fence.
    free_retired_exports(retirement.retired_exports);
    int const FREES = g_ipc_export_selftest_frees.load(std::memory_order_acquire) - FREES_BEFORE;
    return VALID && FREES == 1 ? 0 : -EIO;
}

auto wki_ipc_selftest_epoll_close_releases_lookup_ref() -> int {
    constexpr uint32_t RESOURCE_ID = 0x7E7E;

    auto* proxy = new (std::nothrow) ProxyIpcState{};
    auto* file = new (std::nothrow) ker::vfs::File{};
    auto* epf = new (std::nothrow) ProxyEpollFile{};
    if (proxy == nullptr || file == nullptr || epf == nullptr) {
        delete proxy;
        delete file;
        delete epf;
        return -ENOMEM;
    }

    proxy->active.store(true, std::memory_order_release);
    proxy->resource_id = RESOURCE_ID;
    proxy->home_node = WKI_NODE_INVALID;
    proxy->refcount.store(2, std::memory_order_release);  // file owner + registry

    epf->resource_id = RESOURCE_ID;
    epf->home_node = WKI_NODE_INVALID;
    file->private_data = epf;
    file->fops = &g_proxy_epoll_fops;
    file->refcount.store(1, std::memory_order_release);

    int const FREES_BEFORE = g_ipc_proxy_selftest_frees.load(std::memory_order_acquire);
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        g_ipc_proxies.push_back(proxy);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    int const CLOSE_RET = proxy_epoll_close(file);
    delete file;

    int const FREES_AFTER = g_ipc_proxy_selftest_frees.load(std::memory_order_acquire);
    bool found = false;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        for (auto const* existing : g_ipc_proxies) {
            if (existing == proxy) {
                found = true;
                break;
            }
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    if (CLOSE_RET != 0 || found) {
        return -EIO;
    }
    return (FREES_AFTER - FREES_BEFORE) == 1 ? 0 : -EIO;
}

auto wki_ipc_selftest_nonblocking_pipe_write_view_preserves_source_flags() -> int {
    int private_marker = 0;
    int stream_marker = 0;
    int notify_marker = 0;
    constexpr int ORIGINAL_FLAGS = 0x0101;
    constexpr int FD = 17;
    constexpr off_t POS = 23;

    ker::vfs::File source{};
    source.fd = FD;
    source.private_data = &private_marker;
    source.fops = &g_ipc_selftest_file_ops;
    source.pos = POS;
    source.is_directory = false;
    source.fs_type = ker::vfs::FSType::DEVFS;
    source.refcount.store(7, std::memory_order_release);
    source.open_flags = ORIGINAL_FLAGS;
    source.fd_flags = ker::vfs::FD_CLOEXEC;
    source.vfs_path = "/selftest/ipc-pipe";
    source.dir_fs_count = 123;
    source.stream_cache_attachment = &stream_marker;
    source.cache_notify_attachment = &notify_marker;
    source.positional_read_depth.store(5, std::memory_order_release);

    ker::vfs::File view{};
    init_nonblocking_pipe_write_view(view, source);

    if (source.open_flags != ORIGINAL_FLAGS || source.refcount.load(std::memory_order_acquire) != 7 ||
        source.positional_read_depth.load(std::memory_order_acquire) != 5) {
        return -EIO;
    }

    bool const VIEW_OK =
        view.fd == FD && view.private_data == source.private_data && view.fops == source.fops && view.pos == POS &&
        view.fs_type == source.fs_type && view.refcount.load(std::memory_order_acquire) == 7 &&
        view.open_flags == (ORIGINAL_FLAGS | WKI_IPC_O_NONBLOCK) && view.fd_flags == source.fd_flags && view.vfs_path == source.vfs_path &&
        view.dir_fs_count == source.dir_fs_count && view.stream_cache_attachment == source.stream_cache_attachment &&
        view.cache_notify_attachment == source.cache_notify_attachment && view.positional_read_depth.load(std::memory_order_acquire) == 5;
    return VIEW_OK ? 0 : -EIO;
}

auto wki_ipc_selftest_export_pipe_write_burst_is_bounded() -> int {
    std::array<uint8_t, WKI_IPC_PIPE_DATA_MAX_CHUNK> data{};
    IpcPipeWriteSelftestScript script{};
    ker::vfs::File file{};
    file.private_data = &script;
    file.fops = &g_ipc_pipe_write_selftest_ops;
    file.pos = 23;

    auto reset_script = [&](std::array<ssize_t, 4> results, size_t result_count) {
        script = {};
        script.results = results;
        script.result_count = result_count;
    };

    constexpr ssize_t FINAL_FULL_CHUNK = static_cast<ssize_t>(WKI_IPC_PIPE_DATA_MAX_CHUNK) - 8192;
    static_assert(FINAL_FULL_CHUNK > 0);
    reset_script({4096, 4096, FINAL_FULL_CHUNK, -EIO}, 3);
    ssize_t const FULL_RET =
        export_pipe_write_bounded(&file, data.data(), static_cast<uint16_t>(data.size()), WKI_IPC_EXPORT_PIPE_WRITE_BURST_CALLS);
    bool const FULL_VALID = FULL_RET == static_cast<ssize_t>(data.size()) && script.calls == 3 && script.buffers.at(0) == data.data() &&
                            script.buffers.at(1) == data.data() + 4096 && script.buffers.at(2) == data.data() + 8192 &&
                            script.counts.at(0) == data.size() && script.counts.at(1) == data.size() - 4096 &&
                            script.counts.at(2) == static_cast<size_t>(FINAL_FULL_CHUNK) && script.offsets.at(0) == 23 &&
                            script.offsets.at(1) == 23 && script.offsets.at(2) == 23;

    reset_script({4096, 904, -EAGAIN, -EIO}, 3);
    ssize_t const PARTIAL_RET =
        export_pipe_write_bounded(&file, data.data(), static_cast<uint16_t>(data.size()), WKI_IPC_EXPORT_PIPE_WRITE_BURST_CALLS);
    bool const PARTIAL_VALID = PARTIAL_RET == 5000 && script.calls == 3 && script.buffers.at(1) == data.data() + 4096 &&
                               script.buffers.at(2) == data.data() + 5000 && script.counts.at(1) == data.size() - 4096 &&
                               script.counts.at(2) == data.size() - 5000;

    reset_script({4096, 0, -EIO, -EIO}, 2);
    ssize_t const ZERO_RET =
        export_pipe_write_bounded(&file, data.data(), static_cast<uint16_t>(data.size()), WKI_IPC_EXPORT_PIPE_WRITE_BURST_CALLS);
    bool const ZERO_VALID = ZERO_RET == 4096 && script.calls == 2;

    reset_script({0, -EIO, -EIO, -EIO}, 1);
    ssize_t const FIRST_ZERO_RET = export_pipe_write_bounded(&file, data.data(), 10, WKI_IPC_EXPORT_PIPE_WRITE_BURST_CALLS);
    bool const FIRST_ZERO_VALID = FIRST_ZERO_RET == 0 && script.calls == 1;

    reset_script({20, -EIO, -EIO, -EIO}, 1);
    ssize_t const CLAMPED_RET = export_pipe_write_bounded(&file, data.data(), 10, WKI_IPC_EXPORT_PIPE_WRITE_BURST_CALLS);
    bool const CLAMP_VALID = CLAMPED_RET == 10 && script.calls == 1;

    reset_script({1, 1, 1, 1}, 4);
    ssize_t const CAPPED_RET = export_pipe_write_bounded(&file, data.data(), 10, WKI_IPC_EXPORT_PIPE_WRITE_BURST_CALLS);
    bool const CAPPED_VALID = CAPPED_RET == 3 && script.calls == WKI_IPC_EXPORT_PIPE_WRITE_BURST_CALLS;

    reset_script({-EPIPE, -EIO, -EIO, -EIO}, 1);
    ssize_t const ERROR_RET = export_pipe_write_bounded(&file, data.data(), 10, WKI_IPC_EXPORT_PIPE_WRITE_BURST_CALLS);
    bool const ERROR_VALID = ERROR_RET == -EPIPE && script.calls == 1;

    reset_script({1, 1, -EIO, -EIO}, 2);
    ssize_t const NONPIPE_RET = export_pipe_write_nonblocking(&file, data.data(), 10);
    bool const NONPIPE_VALID = NONPIPE_RET == 1 && script.calls == 1;

    reset_script({-EAGAIN, -EIO, -EIO, -EIO}, 1);
    file.fs_type = ker::vfs::FSType::SOCKET;
    file.open_flags = WKI_IPC_FD_RDWR;
    ssize_t const SOCKET_RET = export_pipe_write_nonblocking(&file, data.data(), 10);
    bool const SOCKET_VALID = SOCKET_RET == -EAGAIN && script.calls == 1 &&
                              script.open_flags[0] == (WKI_IPC_FD_RDWR | WKI_IPC_O_NONBLOCK) && file.open_flags == WKI_IPC_FD_RDWR;

    // Exercise the real proxy writer with no upstream credit: a relay flush
    // must return EAGAIN, preserve the original blocking File, and send nothing.
    ProxyIpcState proxy{};
    proxy.active.store(true, std::memory_order_release);
    proxy.res_type = ResourceType::IPC_PIPE;
    proxy.message_write_flow_control = true;
    ker::vfs::File relay{};
    relay.fops = &g_proxy_pipe_write_fops;
    relay.private_data = &proxy;
    relay.fs_type = ker::vfs::FSType::TMPFS;
    relay.open_flags = 1;
    relay.refcount.store(3, std::memory_order_release);
    ssize_t const RELAY_RET = export_pipe_write_nonblocking(&relay, data.data(), 10);
    bool const RELAY_VALID = RELAY_RET == -EAGAIN && relay.open_flags == 1 && relay.refcount.load(std::memory_order_acquire) == 3 &&
                             proxy.message_write_credits.load(std::memory_order_acquire) == 0 &&
                             proxy.bytes_written.load(std::memory_order_acquire) == 0 && proxy.message_write_waiters.empty();

    return FULL_VALID && PARTIAL_VALID && ZERO_VALID && FIRST_ZERO_VALID && CLAMP_VALID && CAPPED_VALID && ERROR_VALID && NONPIPE_VALID &&
                   RELAY_VALID && SOCKET_VALID
               ? 0
               : -EIO;
}

auto wki_ipc_selftest_pipe_fd_flags_preserve_nonblocking_access_mode() -> int {
    constexpr int READ_NONBLOCK = WKI_IPC_O_NONBLOCK;
    constexpr int WRITE_NONBLOCK = 1 | WKI_IPC_O_NONBLOCK;
    constexpr int UNKNOWN_LOW_FLAG = 0100;
    constexpr int UNEXPORTED_HIGH_FLAG = 02000000;

    if (ipc_fd_access_mode(READ_NONBLOCK) != 0) {
        return -EIO;
    }
    if (ipc_fd_access_mode(WRITE_NONBLOCK) != 1) {
        return -EIO;
    }

    uint16_t const EXPORTED_READ = ipc_fd_export_open_flags(READ_NONBLOCK);
    uint16_t const EXPORTED_WRITE = ipc_fd_export_open_flags(WRITE_NONBLOCK | UNKNOWN_LOW_FLAG | UNEXPORTED_HIGH_FLAG);
    if (ipc_fd_import_open_flags(EXPORTED_READ) != READ_NONBLOCK) {
        return -EIO;
    }
    if (ipc_fd_import_open_flags(EXPORTED_WRITE) != WRITE_NONBLOCK) {
        return -EIO;
    }
    if ((EXPORTED_WRITE & UNKNOWN_LOW_FLAG) != 0 || (EXPORTED_WRITE & UNEXPORTED_HIGH_FLAG) != 0) {
        return -EIO;
    }
    if (ipc_pipe_nonblocking_send_status(WKI_ERR_NO_CREDITS) != -EAGAIN || ipc_pipe_nonblocking_send_status(WKI_ERR_BUSY) != -EAGAIN ||
        ipc_pipe_nonblocking_send_status(WKI_ERR_TX_FAILED) != -EAGAIN || ipc_pipe_nonblocking_send_status(WKI_ERR_NO_MEM) != -ENOMEM ||
        ipc_pipe_nonblocking_send_status(WKI_ERR_NO_ROUTE) != -EIO) {
        return -EIO;
    }

    return 0;
}

auto wki_ipc_selftest_pipe_affinity_uses_read_endpoints_only() -> int {
    constexpr int READ_NONBLOCK = WKI_IPC_O_NONBLOCK;
    constexpr int WRITE_ONLY = 1;
    constexpr int WRITE_NONBLOCK = WRITE_ONLY | WKI_IPC_O_NONBLOCK;

    return ipc_pipe_endpoint_requires_affinity(READ_NONBLOCK) && !ipc_pipe_endpoint_requires_affinity(WRITE_ONLY) &&
                   !ipc_pipe_endpoint_requires_affinity(WRITE_NONBLOCK)
               ? 0
               : -EIO;
}

auto wki_ipc_selftest_write_only_pipe_omits_receive_ring() -> int {
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = 0x7D01;
    channel.channel_id = WKI_CHAN_RESOURCE;
    channel.generation = 1;
    WkiChannelIdentity const CONTROL{&channel, 0x7D01, WKI_CHAN_RESOURCE, 1};
    WkiChannel data_channel{};
    data_channel.active = true;
    data_channel.peer_node_id = 0x7D01;
    data_channel.channel_id = WKI_CHAN_IPC_DATA;
    data_channel.generation = 1;
    WkiChannelIdentity const DATA{&data_channel, 0x7D01, WKI_CHAN_IPC_DATA, 1};
    constexpr uint16_t READ_FD = 5;
    constexpr uint16_t WRITE_FD = 6;
    constexpr uint32_t READ_RESOURCE_ID = 0x7D70;
    constexpr uint32_t WRITE_RESOURCE_ID = 0x7D71;

    if (!ipc_proxy_can_receive_data(ResourceType::IPC_PIPE, 0) || ipc_proxy_can_receive_data(ResourceType::IPC_PIPE, 1) ||
        ipc_proxy_can_receive_data(ResourceType::IPC_PIPE, 2) || ipc_proxy_can_receive_data(ResourceType::IPC_PIPE, 3) ||
        !ipc_proxy_can_receive_data(ResourceType::IPC_SOCKET, 0) || !ipc_proxy_can_receive_data(ResourceType::IPC_PTY, 0) ||
        ipc_proxy_can_receive_data(ResourceType::IPC_EPOLL, 0)) {
        return -EIO;
    }

    ker::mod::sched::task::Task task{};
    std::array<WkiIpcFdEntry, 2> entries = {};
    entries.at(0).local_fd = READ_FD;
    entries.at(0).res_type = static_cast<uint16_t>(ResourceType::IPC_PIPE);
    entries.at(0).resource_id = READ_RESOURCE_ID;
    entries.at(0).home_node = 0x7D01;
    entries.at(0).reserved1 = ipc_fd_export_open_flags(WKI_IPC_O_NONBLOCK);
    entries.at(1).local_fd = WRITE_FD;
    entries.at(1).res_type = static_cast<uint16_t>(ResourceType::IPC_PIPE);
    entries.at(1).resource_id = WRITE_RESOURCE_ID;
    entries.at(1).home_node = 0x7D01;
    entries.at(1).reserved1 = ipc_fd_export_open_flags(1 | WKI_IPC_O_NONBLOCK);

    int const PROXY_FREES_BEFORE = g_ipc_proxy_selftest_frees.load(std::memory_order_acquire);
    constexpr unsigned NOFILE = static_cast<unsigned>(ker::abi::resource::Id::NOFILE);
    bool const LOWERED = task.resources.set(NOFILE, {0, 256}, true, 256) == 0;
    bool const IMPORTED = wki_ipc_attach_task_fds(&task, entries.data(), static_cast<uint16_t>(entries.size()), 0x7D01, CONTROL, DATA);

    auto* read_file = static_cast<ker::vfs::File*>(task.fd_table.lookup(READ_FD));
    auto* write_file = static_cast<ker::vfs::File*>(task.fd_table.lookup(WRITE_FD));
    auto* read_proxy = read_file != nullptr ? static_cast<ProxyIpcState*>(read_file->private_data) : nullptr;
    auto* write_proxy = write_file != nullptr ? static_cast<ProxyIpcState*>(write_file->private_data) : nullptr;

    bool read_drop = true;
    bool write_drop = false;
    bool const READ_DEFERRED = should_defer_ipc_dev_op(OP_PIPE_DATA, READ_RESOURCE_ID, 0x7D01, &read_drop);
    bool const WRITE_DEFERRED = should_defer_ipc_dev_op(OP_PIPE_DATA, WRITE_RESOURCE_ID, 0x7D01, &write_drop);

    constexpr uint8_t INVALID_DATA = 0xA5;
    bool const PENDING_DATA_REJECTED = queue_pending_pipe_data(0x7D01, WRITE_RESOURCE_ID, &INVALID_DATA, sizeof(INVALID_DATA));
    bool const PENDING_CLOSE_REJECTED = mark_pending_pipe_write_closed(0x7D01, WRITE_RESOURCE_ID);
    bool pending_left = false;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        pending_left = find_pending_pipe_delivery_locked(0x7D01, WRITE_RESOURCE_ID) != nullptr;
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    bool const ATTACH_OK = LOWERED && IMPORTED && task.resources.entries[NOFILE].current == 2 && task.resources.entries[NOFILE].peak == 2 &&
                           task.resources.entries[NOFILE].denied == 0 && read_file != nullptr && write_file != nullptr &&
                           read_proxy != nullptr && write_proxy != nullptr && read_file->fops == &g_proxy_pipe_read_fops &&
                           write_file->fops == &g_proxy_pipe_write_fops && read_file->open_flags == WKI_IPC_O_NONBLOCK &&
                           write_file->open_flags == (1 | WKI_IPC_O_NONBLOCK) && read_proxy->can_receive_data &&
                           read_proxy->ring_buf != nullptr && read_proxy->ring_capacity == WKI_PROXY_PIPE_RING_SIZE &&
                           !write_proxy->can_receive_data && write_proxy->ring_buf == nullptr && write_proxy->ring_capacity == 0 &&
                           read_proxy->control_channel.channel == &channel && read_proxy->control_channel.generation == 1 &&
                           write_proxy->control_channel.channel == &channel && write_proxy->control_channel.generation == 1 &&
                           read_proxy->data_channel.channel == &data_channel && write_proxy->data_channel.channel == &data_channel &&
                           read_proxy->data_channel.generation == 1 && write_proxy->data_channel.generation == 1 && !READ_DEFERRED &&
                           !read_drop && !WRITE_DEFERRED && write_drop && PENDING_DATA_REJECTED && PENDING_CLOSE_REJECTED && !pending_left;

    auto detach = [&](uint16_t fd) {
        auto* removed = static_cast<ker::vfs::File*>(task.fd_table.remove(fd));
        if (removed == nullptr) {
            return;
        }
        auto* proxy = static_cast<ProxyIpcState*>(removed->private_data);
        wki_ipc_detach_proxy_file(removed, proxy);
        ker::vfs::vfs_put_file(removed);
    };
    detach(READ_FD);
    detach(WRITE_FD);

    int const PROXY_FREES_AFTER = g_ipc_proxy_selftest_frees.load(std::memory_order_acquire);
    return ATTACH_OK && PROXY_FREES_AFTER - PROXY_FREES_BEFORE == 2 ? 0 : -EIO;
}

auto wki_ipc_selftest_attach_insert_failure_preserves_existing_fd() -> int {
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = 0x7D01;
    channel.channel_id = WKI_CHAN_RESOURCE;
    channel.generation = 1;
    WkiChannelIdentity const CONTROL{&channel, 0x7D01, WKI_CHAN_RESOURCE, 1};
    WkiChannel data_channel{};
    data_channel.active = true;
    data_channel.peer_node_id = 0x7D01;
    data_channel.channel_id = WKI_CHAN_IPC_DATA;
    data_channel.generation = 1;
    WkiChannelIdentity const DATA{&data_channel, 0x7D01, WKI_CHAN_IPC_DATA, 1};
    constexpr uint16_t FD = 5;
    constexpr uint32_t RESOURCE_ID = 0x7D7D;

    ker::mod::sched::task::Task task{};
    auto* existing = new (std::nothrow) ker::vfs::File{};
    if (existing == nullptr) {
        return -ENOMEM;
    }
    existing->fd = FD;
    existing->fops = &g_ipc_selftest_file_ops;
    existing->refcount.store(1, std::memory_order_release);
    if (!task.fd_table.insert(FD, existing)) {
        delete existing;
        return -ENOMEM;
    }

    auto* second = new (std::nothrow) ker::vfs::File{};
    if (second == nullptr) {
        static_cast<void>(task.fd_table.remove(FD));
        ker::vfs::vfs_put_file(existing);
        return -ENOMEM;
    }
    second->fd = FD + 1;
    second->fops = &g_ipc_selftest_file_ops;
    second->refcount.store(1, std::memory_order_release);
    if (!task.fd_table.insert(FD + 1, second)) {
        delete second;
        static_cast<void>(task.fd_table.remove(FD));
        ker::vfs::vfs_put_file(existing);
        return -ENOMEM;
    }
    task.set_fd_cloexec(FD);
    task.set_fd_cloexec(FD + 1);
    constexpr unsigned NOFILE = static_cast<unsigned>(ker::abi::resource::Id::NOFILE);
    task.resources.commit_extent(NOFILE, 2);
    WkiIpcFdEntry entry = {};
    entry.local_fd = FD;
    entry.res_type = static_cast<uint16_t>(ResourceType::IPC_EPOLL);
    entry.resource_id = RESOURCE_ID;
    entry.home_node = 0x7D01;

    std::array<WkiIpcFdEntry, 2> entries{entry, entry};
    bool const DUPLICATE_REJECTED = !wki_ipc_attach_task_fds(&task, entries.data(), entries.size(), 0x7D01, CONTROL, DATA);
    entries[1].local_fd = FD + 1;
    entries[1].resource_id = RESOURCE_ID + 1;
    int const PROXY_FREES_BEFORE = g_ipc_proxy_selftest_frees.load(std::memory_order_acquire);
    std::array<WkiChannelIdentity, 4> denied{CONTROL, CONTROL, CONTROL, CONTROL};
    denied[0] = {};
    denied[1].generation = 2;
    denied[2].peer_node_id++;
    denied[3].channel_id++;
    bool channel_rejected = true;
    for (auto const& identity : denied) {
        channel_rejected &= !wki_ipc_attach_task_fds(&task, entries.data(), entries.size(), 0x7D01, identity, DATA) &&
                            task.fd_table.lookup(FD) == existing && task.fd_table.lookup(FD + 1) == second &&
                            existing->refcount.load() == 1 && second->refcount.load() == 1 &&
                            g_ipc_proxy_selftest_frees.load() == PROXY_FREES_BEFORE;
    }
    denied = {DATA, DATA, DATA, DATA};
    denied[0] = {};
    denied[1].generation = 2;
    denied[2].peer_node_id++;
    denied[3].channel_id--;
    for (auto const& identity : denied) {
        channel_rejected &= !wki_ipc_attach_task_fds(&task, entries.data(), entries.size(), 0x7D01, CONTROL, identity) &&
                            task.fd_table.lookup(FD) == existing && task.fd_table.lookup(FD + 1) == second &&
                            existing->refcount.load() == 1 && second->refcount.load() == 1 &&
                            g_ipc_proxy_selftest_frees.load() == PROXY_FREES_BEFORE;
    }
    g_ipc_selftest_force_attach_insert_failure.store(true, std::memory_order_release);
    bool const REJECTED = !wki_ipc_attach_task_fds(&task, entries.data(), entries.size(), 0x7D01, CONTROL, DATA);
    g_ipc_selftest_force_attach_insert_failure.store(false, std::memory_order_release);

    auto* after = static_cast<ker::vfs::File*>(task.fd_table.lookup(FD));
    bool found_proxy = false;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        for (auto const* proxy : g_ipc_proxies) {
            if (proxy != nullptr && (proxy->resource_id == RESOURCE_ID || proxy->resource_id == RESOURCE_ID + 1)) {
                found_proxy = true;
                break;
            }
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    int const PROXY_FREES_AFTER = g_ipc_proxy_selftest_frees.load(std::memory_order_acquire);

    bool const INTACT = task.fd_table.lookup(FD + 1) == second && task.get_fd_cloexec(FD) && task.get_fd_cloexec(FD + 1) &&
                        task.resources.entries[NOFILE].current == 2 && task.resources.entries[NOFILE].peak == 2;
    auto* removed_second = static_cast<ker::vfs::File*>(task.fd_table.remove(FD + 1));
    if (removed_second == second) ker::vfs::vfs_put_file(second);
    auto* removed = static_cast<ker::vfs::File*>(task.fd_table.remove(FD));
    if (removed == existing) {
        ker::vfs::vfs_put_file(existing);
    }

    if (!REJECTED || !DUPLICATE_REJECTED || !channel_rejected || !INTACT || after != existing || found_proxy ||
        PROXY_FREES_AFTER - PROXY_FREES_BEFORE != 2) {
        return -EIO;
    }
    return 0;
}

auto wki_ipc_selftest_dev_op_response_cookie_fences_stale_completion() -> int {
    constexpr uint32_t RESOURCE_ID = 0x7F7F;
    constexpr uint16_t CURRENT_COOKIE = 0x42;
    constexpr uint16_t STALE_COOKIE = 0x41;
    constexpr int16_t STATUS = -EIO;

    auto* proxy = new (std::nothrow) ProxyIpcState{};
    if (proxy == nullptr) {
        return -ENOMEM;
    }

    WkiWaitEntry wait = {};
    proxy->active.store(true, std::memory_order_release);
    proxy->resource_id = RESOURCE_ID;
    proxy->refcount.store(1, std::memory_order_release);
    proxy->pending_wait = &wait;
    proxy->pending_wait_op = OP_SOCK_SHUTDOWN;
    proxy->pending_wait_cookie = CURRENT_COOKIE;
    proxy->pending_wait_status = -ETIMEDOUT;
    proxy->pending_wait_resp_len = 0;

    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        g_ipc_proxies.push_back(proxy);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    auto send_response = [&](uint16_t cookie) {
        DevOpRespPayload resp = {};
        resp.op_id = OP_SOCK_SHUTDOWN;
        resp.status = STATUS;
        resp.data_len = sizeof(uint32_t);
        resp.reserved = cookie;

        std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(uint32_t)> payload = {};
        std::memcpy(payload.data(), &resp, sizeof(resp));
        std::memcpy(payload.data() + sizeof(DevOpRespPayload), &RESOURCE_ID, sizeof(uint32_t));
        wki_ipc_socket_handle_dev_op_resp(WKI_NODE_INVALID, WKI_CHAN_RESOURCE, payload.data(), static_cast<uint16_t>(payload.size()));
    };

    send_response(STALE_COOKIE);
    uint8_t const STATE_AFTER_STALE = wait.state.load(std::memory_order_acquire);
    if (STATE_AFTER_STALE != static_cast<uint8_t>(WkiWaitEntry::PENDING) || proxy->pending_wait != &wait ||
        proxy->pending_wait_cookie != CURRENT_COOKIE || proxy->pending_wait_status != -ETIMEDOUT) {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy_unregister_locked(proxy);
        s_ipc_lock.unlock_irqrestore(IRQF);
        proxy_release(proxy);
        return -EIO;
    }

    send_response(CURRENT_COOKIE);
    uint8_t const STATE_AFTER_CURRENT = wait.state.load(std::memory_order_acquire);
    int result = 0;
    if (STATE_AFTER_CURRENT != static_cast<uint8_t>(WkiWaitEntry::DONE) || wait.result != 0 || proxy->pending_wait != nullptr ||
        proxy->pending_wait_op != OP_SOCK_SHUTDOWN || proxy->pending_wait_cookie != CURRENT_COOKIE ||
        proxy->pending_wait_status != STATUS) {
        result = -EIO;
    }
    int const CONSUMED_STATUS = wki_ipc_consume_pending_wait_response(proxy, &wait, OP_SOCK_SHUTDOWN, CURRENT_COOKIE, nullptr, 0, nullptr);
    if (CONSUMED_STATUS != STATUS || proxy->pending_wait_op != 0 || proxy->pending_wait_cookie != 0) {
        result = -EIO;
    }

    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy_unregister_locked(proxy);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    proxy_release(proxy);
    return result;
}

auto wki_ipc_selftest_dev_op_response_keeps_slot_busy_until_consumed() -> int {
    constexpr uint32_t RESOURCE_ID = 0x7D7D;
    constexpr uint16_t COOKIE_A = 0x51;
    constexpr uint16_t COOKIE_B = 0x52;
    constexpr int16_t STATUS = -ENOTCONN;

    auto* proxy = new (std::nothrow) ProxyIpcState{};
    if (proxy == nullptr) {
        return -ENOMEM;
    }

    WkiWaitEntry wait_a = {};
    WkiWaitEntry wait_b = {};
    proxy->active.store(true, std::memory_order_release);
    proxy->resource_id = RESOURCE_ID;
    proxy->refcount.store(1, std::memory_order_release);
    proxy->pending_wait = &wait_a;
    proxy->pending_wait_op = OP_SOCK_SHUTDOWN;
    proxy->pending_wait_cookie = COOKIE_A;
    proxy->pending_wait_status = -ETIMEDOUT;
    proxy->pending_wait_resp_len = 0;

    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        g_ipc_proxies.push_back(proxy);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    DevOpRespPayload resp = {};
    resp.op_id = OP_SOCK_SHUTDOWN;
    resp.status = STATUS;
    resp.data_len = sizeof(uint32_t);
    resp.reserved = COOKIE_A;

    std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(uint32_t)> payload = {};
    std::memcpy(payload.data(), &resp, sizeof(resp));
    std::memcpy(payload.data() + sizeof(DevOpRespPayload), &RESOURCE_ID, sizeof(uint32_t));
    wki_ipc_socket_handle_dev_op_resp(WKI_NODE_INVALID, WKI_CHAN_RESOURCE, payload.data(), static_cast<uint16_t>(payload.size()));

    uint8_t const STATE_AFTER_RESPONSE = wait_a.state.load(std::memory_order_acquire);
    bool second_wait_was_busy = false;
    {
        uint64_t const IRQF = proxy->lock.lock_irqsave();
        second_wait_was_busy = proxy->pending_wait_op != 0;
        if (!second_wait_was_busy) {
            proxy->pending_wait = &wait_b;
            proxy->pending_wait_op = OP_SOCK_SHUTDOWN;
            proxy->pending_wait_cookie = COOKIE_B;
        }
        proxy->lock.unlock_irqrestore(IRQF);
    }

    int const CONSUMED_STATUS = wki_ipc_consume_pending_wait_response(proxy, &wait_a, OP_SOCK_SHUTDOWN, COOKIE_A, nullptr, 0, nullptr);

    bool second_wait_can_register_after_consume = false;
    {
        uint64_t const IRQF = proxy->lock.lock_irqsave();
        second_wait_can_register_after_consume = proxy->pending_wait_op == 0;
        if (second_wait_can_register_after_consume) {
            proxy->pending_wait = &wait_b;
            proxy->pending_wait_op = OP_SOCK_SHUTDOWN;
            proxy->pending_wait_cookie = COOKIE_B;
            proxy->pending_wait_status = -ETIMEDOUT;
            proxy->pending_wait_resp_len = 0;
        }
        proxy->pending_wait = nullptr;
        proxy->pending_wait_op = 0;
        proxy->pending_wait_cookie = 0;
        proxy->pending_wait_resp_len = 0;
        proxy->lock.unlock_irqrestore(IRQF);
    }

    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy_unregister_locked(proxy);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    proxy_release(proxy);

    if (STATE_AFTER_RESPONSE != static_cast<uint8_t>(WkiWaitEntry::DONE) || wait_a.result != 0 || !second_wait_was_busy ||
        CONSUMED_STATUS != STATUS || !second_wait_can_register_after_consume) {
        return -EIO;
    }
    return 0;
}

auto wki_ipc_selftest_export_channel_retirement() -> int {
    if (g_wki.initialized) return -EBUSY;
    constexpr uint16_t PEER = 0x7AA1;
    constexpr uint16_t FD = 5;
    IpcSelftestChannels storage(PEER);
    auto const CHANNELS = storage.identity;
    ker::mod::sched::task::Task task{};
    auto* file = new (std::nothrow) ker::vfs::File{};
    if (file == nullptr) return -ENOMEM;
    file->fs_type = ker::vfs::FSType::SOCKET;
    file->fops = &g_ipc_selftest_file_ops;
    file->refcount.store(1);
    if (!task.fd_table.insert(FD, file)) {
        delete file;
        return -ENOMEM;
    }
    std::array<WkiIpcFdEntry, MAX_SUBMIT_IPC_FDS> map{};
    std::array<WkiIpcTaskFdHandoff, MAX_SUBMIT_IPC_FDS> handoff{};
    uint16_t count = 0;
    uint8_t mask = 0;
    uint64_t irqf = s_ipc_lock.lock_irqsave();
    size_t const EXPORTS = g_ipc_exports.size();
    uint32_t const NEXT_ID = g_next_ipc_resource_id;
    s_ipc_lock.unlock_irqrestore(irqf);
    std::array<WkiIpcChannels, 5> denied{CHANNELS, CHANNELS, CHANNELS, CHANNELS, CHANNELS};
    denied[0] = {};
    denied[1].control.generation++;
    denied[2].data.generation++;
    denied[3].data.peer_node_id++;
    denied[4].data.channel_id--;
    bool good = true;
    for (auto const& pair : denied) {
        good &= wki_ipc_export_task_fds(&task, PEER, pair, map.data(), &count, handoff.data(), &mask) == -EHOSTUNREACH && count == 0 &&
                mask == 0 && file->refcount.load() == 1 && task.fd_table.lookup(FD) == file && handoff[0].file == nullptr;
        irqf = s_ipc_lock.lock_irqsave();
        good &= g_ipc_exports.size() == EXPORTS && g_next_ipc_resource_id == NEXT_ID;
        s_ipc_lock.unlock_irqrestore(irqf);
    }
    WkiIpcSubmissionGuard guard(&task, map.data(), handoff.data(), count, PEER, CHANNELS);
    int const EXPORTED = wki_ipc_export_task_fds(&task, PEER, CHANNELS, map.data(), &count, handoff.data(), &mask);
    if (EXPORTED != 0 || count != 1) {
        guard.cancel();
        task.fd_table.remove(FD);
        ipc_release_file_ref(file);
        return -EIO;
    }
    uint32_t const RESOURCE = map[0].resource_id;
    good &= file->refcount.load() == 3 && wki_ipc_exports_match_submit(map.data(), count, PEER, CHANNELS, CHANNELS.control);
    auto wrong_submit = CHANNELS.control;
    wrong_submit.generation++;
    good &= !wki_ipc_exports_match_submit(map.data(), count, PEER, CHANNELS, wrong_submit);
    wrong_submit = CHANNELS.data;
    good &= !wki_ipc_exports_match_submit(map.data(), count, PEER, CHANNELS, wrong_submit);
    auto drain_closes = [&](uint32_t generation) {
        size_t drained = 0;
        for (;;) {
            PendingProxyPipeClose* pending = nullptr;
            uint64_t const FLAGS = s_ipc_lock.lock_irqsave();
            for (auto it = g_pending_proxy_pipe_closes.begin(); it != g_pending_proxy_pipe_closes.end(); ++it) {
                if ((*it)->home_node == PEER && (*it)->resource_id == RESOURCE) {
                    pending = *it;
                    g_pending_proxy_pipe_closes.erase(it);
                    break;
                }
            }
            s_ipc_lock.unlock_irqrestore(FLAGS);
            if (pending == nullptr) break;
            good &= pending->channel_identity.channel == &storage.data && pending->channel_identity.generation == generation;
            ++drained;
            free_pending_proxy_pipe_close(pending);
        }
        return drained;
    };
    // Retire the export while the submission still owns its independent File
    // handoff pin and channel pair. First cleanup has the original DATA token.
    wki_ipc_cleanup_exported_fds(map.data(), count, PEER, CHANNELS);
    good &= file->refcount.load() == 2 && drain_closes(1) == 1 &&
            !wki_ipc_exports_match_submit(map.data(), count, PEER, CHANNELS, CHANNELS.control);
    PipePumpRetirement retirement{};
    {
        PipePumpArg arg{};
        WkiIpcExport expired{};
        expired.channels = CHANNELS;
        expired.consumer_node = PEER;
        expired.resource_id = RESOURCE;
        irqf = s_ipc_lock.lock_irqsave();
        retirement = collect_pipe_pump_retirement_locked(arg, &expired);
        s_ipc_lock.unlock_irqrestore(irqf);
    }
    good &= ipc_channel_identity_equal(retirement.data_channel, CHANNELS.data);
    good &= wki_channel_close_generation(&storage.control, PEER, WKI_CHAN_RESOURCE, 1);
    good &= wki_channel_close_generation(&storage.data, PEER, WKI_CHAN_IPC_DATA, 1);
    storage.control.active = true;
    storage.control.generation = 2;
    storage.data.active = true;
    storage.data.generation = 2;
    uint16_t const CREDITS = storage.data.tx_credits;
    guard.cancel();                           // export is absent; the stored old identity must not rebind
    finish_pipe_pump_retirement(retirement);  // expired export object is gone
    good &= drain_closes(1) == 0 && file->refcount.load() == 1 && handoff[0].file == nullptr && task.fd_table.lookup(FD) == file &&
            task.pending_wki_submissions == nullptr;
    good &= !send_export_pipe_write_flow(CHANNELS.control, RESOURCE, 4, 0) && storage.control.tx_seq == 0 && storage.data.tx_seq == 0 &&
            storage.data.tx_credits == CREDITS;
    auto fresh = CHANNELS;
    fresh.control.generation = 2;
    fresh.data.generation = 2;
    schedule_pending_pipe_discard(fresh.data, RESOURCE);
    good &= drain_closes(2) == 1;  // current identity can still enter the real queue
    task.fd_table.remove(FD);
    ipc_release_file_ref(file);
    return good ? 0 : -EIO;
}

auto wki_ipc_selftest_control_channel_retirement() -> int {
    if (g_wki.initialized) return -EBUSY;
    constexpr uint16_t HOME = 0x7A81;
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = HOME;
    channel.channel_id = WKI_CHAN_RESOURCE;
    channel.generation = 7;
    ProxyIpcState old{};
    old.active.store(true);
    old.home_node = HOME;
    old.control_channel = {&channel, HOME, WKI_CHAN_RESOURCE, 7};
    // A live token reaches the ordinary uninitialized-transport check. A
    // retired token must be refused earlier without selecting a new channel.
    bool good = wki_ipc_send_control(&old, nullptr, 0) == WKI_ERR_INVALID;
    good &= wki_channel_close_generation(&channel, HOME, WKI_CHAN_RESOURCE, 7);
    good &= wki_ipc_send_control(&old, nullptr, 0) == WKI_ERR_NOT_FOUND;
    channel.active = true;
    channel.generation = 8;
    uint16_t const CREDITS = channel.tx_credits;
    good &= wki_ipc_send_control(&old, nullptr, 0) == WKI_ERR_NOT_FOUND && old.control_channel.generation == 7 && channel.tx_seq == 0 &&
            channel.tx_credits == CREDITS;
    ProxyIpcState fresh{};
    fresh.active.store(true);
    fresh.home_node = HOME;
    fresh.control_channel = {&channel, HOME, WKI_CHAN_RESOURCE, 8};
    good &= wki_ipc_send_control(&fresh, nullptr, 0) == WKI_ERR_INVALID;
    fresh.active.store(false);
    good &= wki_ipc_send_control(&fresh, nullptr, 0) == WKI_ERR_NOT_FOUND;
    return good ? 0 : -EIO;
}

auto wki_ipc_selftest_data_channel_retirement() -> int {
    if (g_wki.initialized) return -EBUSY;
    constexpr uint16_t HOME = 0x7A91;
    constexpr uint32_t RESOURCE = 0x7A910001;
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = HOME;
    channel.channel_id = WKI_CHAN_IPC_DATA;
    channel.generation = 7;
    WkiChannelIdentity identity{&channel, HOME, WKI_CHAN_IPC_DATA, 7};
    std::array<uint8_t, sizeof(DevOpReqPayload) + sizeof(uint32_t)> message{};
    auto* request = reinterpret_cast<DevOpReqPayload*>(message.data());
    request->op_id = OP_PIPE_CLOSE_WRITE;
    request->data_len = sizeof(uint32_t);
    std::memcpy(message.data() + sizeof(DevOpReqPayload), &RESOURCE, sizeof(RESOURCE));
    if (!enqueue_proxy_pipe_close_tx(identity, message.data(), message.size(), RESOURCE, OP_PIPE_CLOSE_WRITE)) return -ENOMEM;
    PendingProxyPipeClose* pending = nullptr;
    uint64_t irqf = s_ipc_lock.lock_irqsave();
    for (auto it = g_pending_proxy_pipe_closes.begin(); it != g_pending_proxy_pipe_closes.end(); ++it) {
        if ((*it)->home_node == HOME && (*it)->resource_id == RESOURCE) {
            pending = *it;
            g_pending_proxy_pipe_closes.erase(it);
            break;
        }
    }
    s_ipc_lock.unlock_irqrestore(irqf);
    if (pending == nullptr) return -EIO;
    bool good = send_pending_proxy_pipe_close(*pending) == WKI_ERR_INVALID;
    // The actual queue allocation is already popped when cleanup scans. It
    // owns copied bytes and the old identity, with no proxy or Task reference.
    wki_ipc_mark_epoch_reset(HOME);
    wki_ipc_cleanup_for_peer(HOME);
    good &= wki_channel_close_generation(&channel, HOME, WKI_CHAN_IPC_DATA, 7);
    channel.active = true;
    channel.generation = 8;
    identity.generation = 8;
    uint16_t const CREDITS = channel.tx_credits;
    good &= pending->channel_identity.generation == 7 && send_pending_proxy_pipe_close(*pending) == WKI_ERR_NOT_FOUND;
    good &= !enqueue_proxy_pipe_close_tx(pending->channel_identity, message.data(), message.size(), RESOURCE, OP_PIPE_CLOSE_WRITE);
    ProxyIpcState proxy{};
    proxy.active.store(true);
    proxy.home_node = HOME;
    proxy.resource_id = RESOURCE;
    proxy.data_channel = pending->channel_identity;
    proxy.message_write_flow_control = true;
    proxy.message_write_credits.store(32);
    proxy.bytes_written.store(19);
    ker::vfs::File file{};
    file.private_data = &proxy;
    file.open_flags = 1;
    std::array<uint8_t, 4> bytes{1, 2, 3, 4};
    // Exercise real blocking-mode pipe write: retirement terminates, refunds
    // its application credit reservation and preserves accepted-byte accounting.
    good &= proxy_pipe_write(&file, bytes.data(), bytes.size(), 0) == -EIO && proxy.message_write_credits.load() == 32 &&
            proxy.bytes_written.load() == 19;
    good &=
        wki_send_accounted(identity, MsgType::DEV_OP_REQ, bytes.data(), bytes.size(), proxy.bytes_written, bytes.size()) == WKI_ERR_INVALID;
    good &= proxy.bytes_written.load() == 19 && channel.tx_seq == 0 && channel.tx_credits == CREDITS;
    free_pending_proxy_pipe_close(pending);
    return good ? 0 : -EIO;
}

auto wki_ipc_selftest_epoch_mark_and_drain() -> int {
    if (g_wki.initialized) return -EBUSY;
    constexpr uint16_t HOME = 0x7A81;
    constexpr uint16_t OTHER = HOME + 1;
    std::array<ker::vfs::File*, 2> files{};
    std::array<WkiIpcExport*, 2> exports{};
    std::array<ProxyIpcState*, 2> proxies{};
    std::array<WkiWaitEntry, 2> waits{};
    bool allocated = true;
    for (size_t i = 0; i < 2; ++i) {
        files[i] = new (std::nothrow) ker::vfs::File{};
        exports[i] = new (std::nothrow) WkiIpcExport{};
        proxies[i] = new (std::nothrow) ProxyIpcState{};
        allocated &= files[i] != nullptr && exports[i] != nullptr && proxies[i] != nullptr;
    }
    if (!allocated) {
        for (size_t i = 0; i < 2; ++i) {
            delete files[i];
            delete exports[i];
            delete proxies[i];
        }
        return -ENOMEM;
    }
    int const CLOSES = g_ipc_file_selftest_closes.load(std::memory_order_acquire);
    for (size_t i = 0; i < 2; ++i) {
        uint16_t const NODE = i == 0 ? HOME : OTHER;
        files[i]->fops = &g_ipc_selftest_file_ops;
        files[i]->refcount.store(1);
        exports[i]->active = true;
        exports[i]->consumer_node = NODE;
        exports[i]->resource_id = 0x7A810001 + i;
        exports[i]->res_type = ResourceType::IPC_SOCKET;
        exports[i]->file = files[i];
        proxies[i]->active.store(true);
        proxies[i]->home_node = NODE;
        proxies[i]->resource_id = 0x7A810010 + i;
        proxies[i]->refcount.store(2);  // registry plus this test's observation pin
        proxies[i]->pending_wait = &waits[i];
        proxies[i]->pending_wait_op = OP_SOCK_SHUTDOWN;
        proxies[i]->pending_wait_cookie = wki_ipc_allocate_wait_cookie_locked(proxies[i], OP_SOCK_SHUTDOWN);
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        g_ipc_exports.push_back(exports[i]);
        g_ipc_proxies.push_back(proxies[i]);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    IpcDevOpWork work{};
    work.hdr.src_node = HOME;
    uint64_t irqf = s_ipc_lock.lock_irqsave();
    work.cleanup_epoch = ipc_peer_cleanup_epoch_locked(HOME);
    bool const CLAIMED = claim_ipc_dev_op_worker_locked(&work);
    s_ipc_lock.unlock_irqrestore(irqf);
    wki_ipc_mark_epoch_reset(HOME);
    bool good = CLAIMED && !exports[0]->active && exports[1]->active && exports[0]->file == files[0] && files[0]->refcount.load() == 1 &&
                !proxies[0]->active.load() && proxies[1]->active.load() && proxies[0]->refcount.load() == 2 &&
                proxies[0]->message_write_error.load() == -EIO && proxies[0]->pending_wait == &waits[0] &&
                waits[0].state.load() == WkiWaitEntry::PENDING && g_ipc_file_selftest_closes.load() == CLOSES;
    irqf = s_ipc_lock.lock_irqsave();
    bool const SECOND_CLAIM = claim_ipc_dev_op_worker_locked(&work);
    good &= ipc_peer_cleanup_active_locked(HOME) && !SECOND_CLAIM && ipc_peer_active_workers_locked(HOME) == 1;
    // Release even an unexpected claim so a failing regression cannot strand
    // the selftest in deferred cleanup's legitimate worker-drain wait.
    if (CLAIMED) release_ipc_dev_op_worker_locked(HOME);
    if (SECOND_CLAIM) release_ipc_dev_op_worker_locked(HOME);
    s_ipc_lock.unlock_irqrestore(irqf);
    wki_ipc_cleanup_for_peer(HOME);
    good &= proxies[0]->refcount.load() == 1 && proxies[0]->pending_wait == nullptr && proxies[0]->pending_wait_status == -EIO &&
            waits[0].state.load() == WkiWaitEntry::DONE && proxies[1]->active.load() && waits[1].state.load() == WkiWaitEntry::PENDING &&
            g_ipc_file_selftest_closes.load() == CLOSES + 1;
    irqf = s_ipc_lock.lock_irqsave();
    good &= !ipc_peer_cleanup_active_locked(HOME) && ipc_peer_active_workers_locked(HOME) == 0;
    s_ipc_lock.unlock_irqrestore(irqf);
    wki_ipc_cleanup_for_peer(OTHER);
    good &=
        proxies[1]->refcount.load() == 1 && waits[1].state.load() == WkiWaitEntry::DONE && g_ipc_file_selftest_closes.load() == CLOSES + 2;
    proxy_release(proxies[0]);
    proxy_release(proxies[1]);
    return good ? 0 : -EIO;
}

auto wki_ipc_selftest_cookie_retirement_preserves_current_waiter() -> int {
    if (g_wki.initialized) return -EBUSY;
    constexpr uint16_t HOME = 0x7A71;
    constexpr uint32_t RESOURCE = 0x7A710001;
    constexpr uint16_t OP = OP_SOCK_SHUTDOWN;
    auto* proxy = new (std::nothrow) ProxyIpcState{};
    if (proxy == nullptr) return -ENOMEM;
    proxy->active.store(true, std::memory_order_release);
    proxy->home_node = HOME;
    proxy->assigned_channel = WKI_CHAN_RESOURCE;
    proxy->resource_id = RESOURCE;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        g_ipc_proxies.push_back(proxy);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    auto register_wait = [&](WkiWaitEntry& wait) {
        uint64_t const IRQF = proxy->lock.lock_irqsave();
        uint16_t const COOKIE = wki_ipc_allocate_wait_cookie_locked(proxy, OP);
        if (COOKIE != 0) {
            proxy->pending_wait = &wait;
            proxy->pending_wait_op = OP;
            proxy->pending_wait_cookie = COOKIE;
            proxy->pending_wait_status = -ETIMEDOUT;
            proxy->pending_wait_resp_len = 0;
        }
        proxy->lock.unlock_irqrestore(IRQF);
        return COOKIE;
    };
    auto reserved = [&](uint16_t cookie) {
        return std::ranges::any_of(proxy->unanswered_controls, [&](auto const& identity) { return identity.cookie == cookie; });
    };
    auto deliver = [&](uint16_t home, uint16_t op, uint16_t cookie) {
        DevOpRespPayload response{.op_id = op, .status = -EPIPE, .data_len = sizeof(RESOURCE), .reserved = cookie};
        std::array<uint8_t, sizeof(response) + sizeof(RESOURCE)> payload{};
        std::memcpy(payload.data(), &response, sizeof(response));
        std::memcpy(payload.data() + sizeof(response), &RESOURCE, sizeof(RESOURCE));
        wki_ipc_handle_dev_op_resp(home, WKI_CHAN_RESOURCE, payload.data(), payload.size());
    };
    WkiWaitEntry old_wait{};
    uint16_t const OLD_COOKIE = register_wait(old_wait);
    wki_ipc_cancel_pending_wait(proxy, &old_wait, -ETIMEDOUT);
    bool good = OLD_COOKIE == 1 && reserved(OLD_COOKIE) && proxy->pending_wait == nullptr;

    // Failed sends can advance through wrap immediately. Their identities are
    // released, while the earlier accepted-but-unanswered cookie is skipped.
    proxy->next_wait_cookie = UINT16_MAX;
    WkiWaitEntry unsent{};
    uint16_t const UNSENT_COOKIE = register_wait(unsent);
    wki_ipc_cancel_pending_wait(proxy, &unsent, WKI_ERR_TX_FAILED, false);
    good &= UNSENT_COOKIE == UINT16_MAX && !reserved(UNSENT_COOKIE) && proxy->next_wait_cookie == 1;
    WkiWaitEntry current{};
    uint16_t const CURRENT_COOKIE = register_wait(current);
    good &= CURRENT_COOKIE == 2 && reserved(OLD_COOKIE) && reserved(CURRENT_COOKIE);
    deliver(HOME + 1, OP, OLD_COOKIE);
    deliver(HOME, OP_SOCK_GETPEERNAME, OLD_COOKIE);
    good &= reserved(OLD_COOKIE);
    deliver(HOME, OP, OLD_COOKIE);
    good &= !reserved(OLD_COOKIE) && reserved(CURRENT_COOKIE) && proxy->pending_wait == &current &&
            current.state.load(std::memory_order_acquire) == WkiWaitEntry::PENDING && proxy->pending_wait_status == -ETIMEDOUT &&
            proxy->pending_wait_resp_len == 0;
    deliver(HOME, OP, CURRENT_COOKIE);
    good &= !reserved(CURRENT_COOKIE) && current.state.load(std::memory_order_acquire) == WkiWaitEntry::DONE;
    good &= wki_ipc_consume_pending_wait_response(proxy, &current, OP, CURRENT_COOKIE, nullptr, 0, nullptr) == -EPIPE;

    // The timer may claim DONE before a real response reaches the proxy lock.
    // Seeing that response frees its identity without overwriting the timeout.
    WkiWaitEntry timed_out{};
    uint16_t const TIMED_COOKIE = register_wait(timed_out);
    wki_wake_op(&timed_out, WKI_ERR_TIMEOUT);
    deliver(HOME, OP, TIMED_COOKIE);
    good &= !reserved(TIMED_COOKIE) && timed_out.result == WKI_ERR_TIMEOUT && proxy->pending_wait == &timed_out &&
            proxy->pending_wait_status == -ETIMEDOUT;
    wki_ipc_cancel_pending_wait(proxy, &timed_out, -ETIMEDOUT);

    std::array<uint16_t, WKI_IPC_MAX_UNANSWERED_CONTROLS> unanswered{};
    for (auto& cookie : unanswered) {
        WkiWaitEntry wait{};
        cookie = register_wait(wait);
        good &= cookie != 0;
        wki_ipc_cancel_pending_wait(proxy, &wait, -ETIMEDOUT);
    }
    uint16_t const NEXT = proxy->next_wait_cookie;
    WkiWaitEntry full{};
    good &= register_wait(full) == 0 && proxy->next_wait_cookie == NEXT && proxy->pending_wait == nullptr && proxy->pending_wait_op == 0;
    // Exact late completion restores capacity; saturation is not permanent.
    deliver(HOME, OP, unanswered[0]);
    WkiWaitEntry recovered{};
    uint16_t const RECOVERED_COOKIE = register_wait(recovered);
    good &= RECOVERED_COOKIE != 0;
    wki_ipc_cancel_pending_wait(proxy, &recovered, WKI_ERR_TX_FAILED, false);
    for (uint16_t cookie : unanswered) deliver(HOME, OP, cookie);
    good &= std::ranges::all_of(proxy->unanswered_controls, [](auto const& identity) { return identity.cookie == 0; }) &&
            proxy->refcount.load(std::memory_order_acquire) == 1;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy_unregister_locked(proxy);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    proxy->pending_wait = nullptr;
    proxy_release(proxy);
    return good ? 0 : -EIO;
}

auto wki_ipc_selftest_healthy_controls_cross_cookie_wrap() -> int {
    ProxyIpcState proxy{};
    constexpr uint16_t OP = OP_PTY_IOCTL;
    // More than two complete cookie spaces of successful request/reply pairs
    // must not impose a lifetime limit on the descriptor.
    for (uint32_t i = 0; i < 2U * UINT16_MAX + 5U; ++i) {
        uint16_t const COOKIE = wki_ipc_allocate_wait_cookie_locked(&proxy, OP);
        if (COOKIE != static_cast<uint16_t>(i % UINT16_MAX + 1U)) return -EIO;
        wki_ipc_retire_wait_cookie_locked(&proxy, OP, COOKIE);
    }
    return 0;
}

auto wki_ipc_selftest_control_response_admission_preserves_waiter() -> int {
    if (g_wki.initialized) return -EBUSY;
    constexpr uint16_t HOME = 0x7A61;
    constexpr uint16_t COOKIE = 0x1234;
    constexpr uint32_t RESOURCE = 0x7A610001;
    constexpr uint32_t VALUE = 0x12345678;
    constexpr size_t REPLY_LEN = sizeof(DevOpRespPayload) + 2 * sizeof(uint32_t);
    bool good = true;
    for (auto handler : {wki_ipc_handle_dev_op_resp, wki_ipc_socket_handle_dev_op_resp}) {
        for (uint16_t op : {OP_SOCK_GETSOCKOPT, OP_PTY_IOCTL, OP_EPOLL_CTL, OP_FUTEX_WAKE}) {
            auto* proxy = new (std::nothrow) ProxyIpcState{};
            if (proxy == nullptr) return -ENOMEM;
            WkiWaitEntry wait{};
            proxy->active.store(true, std::memory_order_release);
            proxy->home_node = HOME;
            proxy->assigned_channel = WKI_CHAN_RESOURCE;
            proxy->resource_id = RESOURCE;
            proxy->pending_wait = &wait;
            proxy->pending_wait_op = op;
            proxy->pending_wait_cookie = COOKIE;
            proxy->pending_wait_status = -ETIMEDOUT;
            std::memset(proxy->pending_wait_resp, 0xA5, sizeof(proxy->pending_wait_resp));
            {
                uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                g_ipc_proxies.push_back(proxy);
                s_ipc_lock.unlock_irqrestore(IRQF);
            }
            DevOpRespPayload response{.op_id = op, .status = 0, .data_len = 2 * sizeof(uint32_t), .reserved = COOKIE};
            // Keep backing storage large even for the rejected declared-length
            // cases, so a regression reports changed state rather than reading
            // outside the selftest allocation.
            std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(uint32_t) + ProxyIpcState::SOCK_CTRL_RESP_MAX> payload{};
            std::memcpy(payload.data() + sizeof(response), &RESOURCE, sizeof(RESOURCE));
            std::memcpy(payload.data() + sizeof(response) + sizeof(RESOURCE), &VALUE, sizeof(VALUE));
            auto deliver = [&](uint16_t channel, uint16_t length) {
                std::memcpy(payload.data(), &response, sizeof(response));
                handler(HOME, channel, payload.data(), length);
            };
            auto unchanged = [&]() {
                return wait.state.load(std::memory_order_acquire) == WkiWaitEntry::PENDING && wait.result == 0 &&
                       proxy->pending_wait == &wait && proxy->pending_wait_op == op && proxy->pending_wait_cookie == COOKIE &&
                       proxy->pending_wait_status == -ETIMEDOUT && proxy->pending_wait_resp_len == 0 &&
                       std::ranges::all_of(proxy->pending_wait_resp, [](uint8_t byte) { return byte == 0xA5; }) &&
                       proxy->refcount.load(std::memory_order_acquire) == 1;
            };
            for (uint16_t channel : {WKI_CHAN_CONTROL, WKI_CHAN_IPC_DATA, WKI_CHAN_DYNAMIC_BASE}) {
                deliver(channel, REPLY_LEN);
                good &= unchanged();
            }
            handler(HOME, WKI_CHAN_RESOURCE, nullptr, REPLY_LEN);
            good &= unchanged();
            for (uint16_t length : {uint16_t{0}, uint16_t{sizeof(response) - 1}, uint16_t{sizeof(response) + 3}, uint16_t{REPLY_LEN - 1}}) {
                deliver(WKI_CHAN_RESOURCE, length);
                good &= unchanged();
            }
            for (uint16_t declared : {uint16_t{0}, uint16_t{3}, uint16_t{9}, uint16_t{UINT16_MAX}}) {
                response.data_len = declared;
                deliver(WKI_CHAN_RESOURCE, REPLY_LEN);
                good &= unchanged();
            }
            response.data_len = 2 * sizeof(uint32_t);
            deliver(WKI_CHAN_RESOURCE, REPLY_LEN);
            good &= wait.state.load(std::memory_order_acquire) == WkiWaitEntry::DONE && wait.result == 0 &&
                    proxy->pending_wait == nullptr && proxy->pending_wait_op == op && proxy->pending_wait_cookie == COOKIE &&
                    proxy->pending_wait_status == 0 && proxy->pending_wait_resp_len == sizeof(VALUE);
            // A duplicate cannot replace the response while the busy slot is
            // retained for the original caller's consume operation.
            response.status = -EIO;
            deliver(WKI_CHAN_RESOURCE, REPLY_LEN);
            uint32_t value = 0;
            uint16_t length = 0;
            good &= wki_ipc_consume_pending_wait_response(proxy, &wait, op, COOKIE, &value, sizeof(value), &length) == 0;
            good &= value == VALUE && length == sizeof(VALUE) && proxy->pending_wait_op == 0 && proxy->pending_wait_cookie == 0 &&
                    proxy->refcount.load(std::memory_order_acquire) == 1;
            {
                uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                proxy_unregister_locked(proxy);
                s_ipc_lock.unlock_irqrestore(IRQF);
            }
            proxy->pending_wait = nullptr;
            proxy_release(proxy);
        }
    }
    return good ? 0 : -EIO;
}

auto wki_ipc_selftest_dev_op_response_uses_home_node_identity() -> int {
    constexpr uint32_t RESOURCE_ID = 0x7E7E;
    constexpr uint16_t HOME_A = 0x0101;
    constexpr uint16_t HOME_B = 0x0202;
    constexpr uint16_t COOKIE = 0x33;
    constexpr int16_t STATUS = -EPIPE;

    auto* proxy_a = new (std::nothrow) ProxyIpcState{};
    auto* proxy_b = new (std::nothrow) ProxyIpcState{};
    if (proxy_a == nullptr || proxy_b == nullptr) {
        delete proxy_a;
        delete proxy_b;
        return -ENOMEM;
    }

    WkiWaitEntry wait_a = {};
    WkiWaitEntry wait_b = {};
    auto setup_proxy = [&](ProxyIpcState* proxy, uint16_t home_node, WkiWaitEntry* wait) {
        proxy->active.store(true, std::memory_order_release);
        proxy->home_node = home_node;
        proxy->resource_id = RESOURCE_ID;
        proxy->refcount.store(1, std::memory_order_release);
        proxy->pending_wait = wait;
        proxy->pending_wait_op = OP_SOCK_SHUTDOWN;
        proxy->pending_wait_cookie = COOKIE;
        proxy->pending_wait_status = -ETIMEDOUT;
        proxy->pending_wait_resp_len = 0;
    };
    setup_proxy(proxy_a, HOME_A, &wait_a);
    setup_proxy(proxy_b, HOME_B, &wait_b);

    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        g_ipc_proxies.push_back(proxy_a);
        g_ipc_proxies.push_back(proxy_b);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    DevOpRespPayload resp = {};
    resp.op_id = OP_SOCK_SHUTDOWN;
    resp.status = STATUS;
    resp.data_len = sizeof(uint32_t);
    resp.reserved = COOKIE;

    std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(uint32_t)> payload = {};
    std::memcpy(payload.data(), &resp, sizeof(resp));
    std::memcpy(payload.data() + sizeof(DevOpRespPayload), &RESOURCE_ID, sizeof(uint32_t));
    wki_ipc_socket_handle_dev_op_resp(HOME_B, WKI_CHAN_RESOURCE, payload.data(), static_cast<uint16_t>(payload.size()));

    uint8_t const STATE_A = wait_a.state.load(std::memory_order_acquire);
    uint8_t const STATE_B = wait_b.state.load(std::memory_order_acquire);
    int result = 0;
    if (STATE_A != static_cast<uint8_t>(WkiWaitEntry::PENDING) || proxy_a->pending_wait != &wait_a ||
        proxy_a->pending_wait_op != OP_SOCK_SHUTDOWN || proxy_a->pending_wait_cookie != COOKIE ||
        proxy_a->pending_wait_status != -ETIMEDOUT) {
        result = -EIO;
    }
    if (STATE_B != static_cast<uint8_t>(WkiWaitEntry::DONE) || wait_b.result != 0 || proxy_b->pending_wait != nullptr ||
        proxy_b->pending_wait_op != OP_SOCK_SHUTDOWN || proxy_b->pending_wait_cookie != COOKIE || proxy_b->pending_wait_status != STATUS) {
        result = -EIO;
    }
    int const CONSUMED_STATUS = wki_ipc_consume_pending_wait_response(proxy_b, &wait_b, OP_SOCK_SHUTDOWN, COOKIE, nullptr, 0, nullptr);
    if (CONSUMED_STATUS != STATUS || proxy_b->pending_wait_op != 0 || proxy_b->pending_wait_cookie != 0) {
        result = -EIO;
    }

    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy_unregister_locked(proxy_a);
        proxy_unregister_locked(proxy_b);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    {
        uint64_t const IRQF = proxy_a->lock.lock_irqsave();
        proxy_a->pending_wait = nullptr;
        proxy_a->pending_wait_op = 0;
        proxy_a->pending_wait_cookie = 0;
        proxy_a->lock.unlock_irqrestore(IRQF);
    }
    proxy_release(proxy_a);
    proxy_release(proxy_b);
    return result;
}
#endif

void wki_ipc_socket_handle_dev_op_resp(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len) {
    DevOpRespPayload resp = {};
    if (!read_ipc_control_response(channel, payload, len, resp)) {
        return;
    }

    uint16_t const DATA_LEN = resp.data_len;
    const uint8_t* resp_data = payload + sizeof(DevOpRespPayload);

    // First 4 bytes of response data = resource_id
    uint32_t resource_id = 0;
    std::memcpy(&resource_id, resp_data, sizeof(uint32_t));
    auto const EXTRA_LEN = static_cast<uint16_t>(DATA_LEN - sizeof(uint32_t));
    const uint8_t* extra_data = resp_data + sizeof(uint32_t);

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    ProxyIpcState* target_proxy = nullptr;
    for (auto* p : g_ipc_proxies) {
        if (p != nullptr && p->active.load(std::memory_order_acquire) && p->home_node == src_node && p->resource_id == resource_id) {
            target_proxy = p;
            target_proxy->refcount.fetch_add(1, std::memory_order_acq_rel);
            break;
        }
    }
    s_ipc_lock.unlock_irqrestore(IRQF);

    if (target_proxy == nullptr) {
        if (resp.op_id == OP_PTY_IOCTL && wait_diag_enabled()) {
            ker::mod::dbg::logger<"wki">::warn("pty_ioctl_resp_no_proxy src=0x%04x res=%u cookie=%u status=%d extra=%u",
                                               static_cast<unsigned>(src_node), resource_id, static_cast<unsigned>(resp.reserved),
                                               static_cast<int>(resp.status), static_cast<unsigned>(EXTRA_LEN));
        }
        return;
    }

    WkiWaitEntry* wait = nullptr;
    bool had_pending_wait = false;
    bool pending_matches = false;
    uint16_t pending_op = 0;
    uint16_t pending_cookie = 0;
    {
        uint64_t const PROXY_IRQF = target_proxy->lock.lock_irqsave();
        // Seeing the exact response ends uncertainty even if the timer won
        // the waiter claim. Unrelated operations cannot release this cookie.
        wki_ipc_retire_wait_cookie_locked(target_proxy, resp.op_id, resp.reserved);
        wait = target_proxy->pending_wait;
        had_pending_wait = wait != nullptr;
        pending_op = target_proxy->pending_wait_op;
        pending_cookie = target_proxy->pending_wait_cookie;
        pending_matches = wait != nullptr && wki_ipc_response_matches_pending(resp.op_id, resp.reserved, *target_proxy);
        if (pending_matches && wki_claim_op(wait)) {
            target_proxy->pending_wait = nullptr;
            // Keep op/cookie populated until the waiting syscall consumes the
            // side-band status/response. They are the busy-slot fence.
            target_proxy->pending_wait_status = static_cast<int>(resp.status);
            if (EXTRA_LEN > 0) {
                uint16_t const COPY_LEN =
                    EXTRA_LEN < ProxyIpcState::SOCK_CTRL_RESP_MAX ? EXTRA_LEN : static_cast<uint16_t>(ProxyIpcState::SOCK_CTRL_RESP_MAX);
                std::memcpy(static_cast<void*>(target_proxy->pending_wait_resp), extra_data, COPY_LEN);
                target_proxy->pending_wait_resp_len = COPY_LEN;
            } else {
                target_proxy->pending_wait_resp_len = 0;
            }
        } else {
            wait = nullptr;  // op_id mismatch — stale response
        }
        target_proxy->lock.unlock_irqrestore(PROXY_IRQF);
    }

    if (resp.op_id == OP_PTY_IOCTL && wait_diag_enabled()) {
        if (wait != nullptr) {
            ker::mod::dbg::logger<"wki">::info("pty_ioctl_resp_match src=0x%04x res=%u cookie=%u status=%d extra=%u",
                                               static_cast<unsigned>(src_node), resource_id, static_cast<unsigned>(resp.reserved),
                                               static_cast<int>(resp.status), static_cast<unsigned>(EXTRA_LEN));
        } else {
            ker::mod::dbg::logger<"wki">::warn(
                "pty_ioctl_resp_stale src=0x%04x res=%u cookie=%u status=%d extra=%u had_wait=%u pending_match=%u pending_op=0x%x "
                "pending_cookie=%u",
                static_cast<unsigned>(src_node), resource_id, static_cast<unsigned>(resp.reserved), static_cast<int>(resp.status),
                static_cast<unsigned>(EXTRA_LEN), had_pending_wait ? 1U : 0U, pending_matches ? 1U : 0U, static_cast<unsigned>(pending_op),
                static_cast<unsigned>(pending_cookie));
        }
    }

    if (wait != nullptr) {
        wki_finish_claimed_op(wait, 0);
    }

    if (target_proxy->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        delete[] target_proxy->ring_buf;
        delete target_proxy;
    }
}

void WkiIpcOutputCompletion::retain_locked(ProxyIpcState* proxy) {
    // Called once per prepared import, at most16, before the task can run.
    if (proxy == nullptr || !proxy->message_write_flow_control) return;
    proxies.at(count++) = proxy;
    ++proxy->completion_refs;
    proxy->refcount.fetch_add(1, std::memory_order_relaxed);
}

auto WkiIpcOutputCompletion::ready() -> bool {
    // Only the completion monitor calls this. All root-task writes have
    // finished, and accepted counters follow channel enqueue order. Freeze
    // once: later background writes must not move the completion goalpost.
    if (!frozen) {
        for (size_t i = 0; i < count; ++i) targets[i] = proxies[i]->bytes_written.load(std::memory_order_acquire);
        frozen = true;
    }
    for (size_t i = 0; i < count; ++i) {
        auto* proxy = proxies[i];
        if (proxy->bytes_delivered.load(std::memory_order_acquire) < targets[i] &&
            proxy->message_write_error.load(std::memory_order_acquire) == 0)
            return false;
    }
    // A closed reader or retired peer is terminal too; it cannot deliver any
    // more bytes. Preserve the process's actual exit status in that case.
    return true;
}

WkiIpcOutputCompletion::~WkiIpcOutputCompletion() {
    for (size_t i = 0; i < count; ++i) {
        auto* proxy = proxies[i];
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        --proxy->completion_refs;
        bool const DROP_REGISTRY =
            proxy->completion_refs == 0 && !proxy->active.load(std::memory_order_acquire) && proxy_unregister_locked(proxy);
        s_ipc_lock.unlock_irqrestore(IRQF);
        if (DROP_REGISTRY) proxy_release(proxy);
        proxy_release(proxy);
    }
}

void wki_ipc_detach_proxy_file(ker::vfs::File* f, ProxyIpcState* proxy) {
    if (proxy == nullptr) {
        if (f != nullptr) {
            f->private_data = nullptr;
        }
        return;
    }

    bool dropped_registry_ref = false;
    PendingPipeDelivery* dropped_pending = nullptr;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy->active.store(false, std::memory_order_release);
        // A completion owner keeps receipt lookup alive, but never retains the
        // File or makes this closed endpoint usable for further operations.
        if (proxy->completion_refs == 0) dropped_registry_ref = proxy_unregister_locked(proxy);
        dropped_pending = take_pending_pipe_delivery_locked(proxy->home_node, proxy->resource_id);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    auto* reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
    wake_blocked_ipc_reader(reader);
    proxy_wake_stream_readers(proxy);
    proxy_wake_message_write_waiters(proxy);
    wki_ipc_proxy_wake_poll_waiters(proxy);
    WkiWaitEntry* pending_wait = nullptr;
    {
        uint64_t const PROXY_IRQF = proxy->lock.lock_irqsave();
        pending_wait = claim_proxy_pending_wait_locked(proxy, -EIO);
        proxy->lock.unlock_irqrestore(PROXY_IRQF);
    }
    if (pending_wait != nullptr) {
        wki_finish_claimed_op(pending_wait, 0);
    }

    if (f != nullptr) {
        f->private_data = nullptr;
    }

    if (dropped_registry_ref) {
        proxy_release(proxy);
    }
    proxy_release(proxy);
    free_pending_pipe_delivery(dropped_pending);
}

// =============================================================================
// Peer cleanup
// =============================================================================

namespace {

struct IpcPeerCleanupBatch {
    std::array<ker::mod::sched::task::Task*, WKI_IPC_MAX_EXPORTS> stopped_pumps = {};
    std::array<ker::vfs::File*, WKI_IPC_MAX_EXPORTS> released_files = {};
    std::array<ProxyIpcState*, WKI_IPC_MAX_PROXIES> detached_proxies = {};
    std::array<PendingPipeDelivery*, WKI_IPC_MAX_EXPORTS> detached_pending = {};
    std::array<PendingProxyPipeClose*, WKI_IPC_PROXY_CLOSE_CLEANUP_BATCH> detached_proxy_closes = {};
    std::array<IpcDevOpWork*, WKI_IPC_DEV_OP_CLEANUP_BATCH> detached_dev_ops = {};
    size_t stopped_pump_count = 0;
    size_t released_file_count = 0;
    size_t detached_proxy_count = 0;
    size_t detached_pending_count = 0;
    size_t detached_proxy_close_count = 0;
    size_t detached_dev_op_count = 0;
    WkiIpcExport* retired_exports = nullptr;
    bool wake_export_pipe_flush = false;
    bool made_progress = false;
};

auto export_needs_peer_cleanup_locked(const WkiIpcExport* exp, uint16_t node_id) -> bool {
    if (exp == nullptr || exp->consumer_node != node_id) {
        return false;
    }
    return exp->active || exp->file != nullptr || exp->pump_task != nullptr || exp->pump_queued ||
           exp->pump_running.load(std::memory_order_acquire);
}

void collect_ipc_peer_cleanup_batch_locked(uint16_t node_id, IpcPeerCleanupBatch& batch) {
    bool batch_full = false;

    for (auto* exp : g_ipc_exports) {
        if (!export_needs_peer_cleanup_locked(exp, node_id)) {
            continue;
        }

        bool const NEEDS_PUMP_SLOT = exp->pump_task != nullptr;
        bool const NEEDS_FILE_SLOT = exp->file != nullptr;
        if ((NEEDS_PUMP_SLOT && batch.stopped_pump_count >= batch.stopped_pumps.size()) ||
            (NEEDS_FILE_SLOT && batch.released_file_count >= batch.released_files.size())) {
            batch_full = true;
            break;
        }

        batch.made_progress = true;
        if (NEEDS_PUMP_SLOT || exp->pump_queued || exp->pump_running.load(std::memory_order_acquire)) {
            auto* pump_task = stop_pipe_pump_locked(exp);
            if (pump_task != nullptr) {
                batch.stopped_pumps.at(batch.stopped_pump_count++) = pump_task;
            }
        }

        if (exp->file != nullptr) {
            batch.released_files.at(batch.released_file_count++) = exp->file;
            exp->file = nullptr;
        }
        exp->active = false;
    }

    if (!batch_full) {
        for (auto it = g_ipc_proxies.begin(); it != g_ipc_proxies.end();) {
            auto* proxy = *it;
            if (proxy == nullptr || proxy->home_node != node_id) {
                ++it;
                continue;
            }
            if (batch.detached_proxy_count >= batch.detached_proxies.size()) {
                batch_full = true;
                break;
            }

            proxy->active.store(false, std::memory_order_release);
            proxy->message_write_error.store(-EIO, std::memory_order_release);
            batch.detached_proxies.at(batch.detached_proxy_count++) = proxy;
            batch.made_progress = true;
            it = g_ipc_proxies.erase(it);
        }
    }

    if (!batch_full) {
        for (auto it = g_pending_pipe_deliveries.begin(); it != g_pending_pipe_deliveries.end();) {
            auto* pending = *it;
            if (pending == nullptr || pending->home_node != node_id) {
                ++it;
                continue;
            }
            if (batch.detached_pending_count >= batch.detached_pending.size()) {
                batch_full = true;
                break;
            }

            batch.detached_pending.at(batch.detached_pending_count++) = pending;
            batch.made_progress = true;
            it = g_pending_pipe_deliveries.erase(it);
        }
    }

    if (!batch_full) {
        for (auto it = g_pending_proxy_pipe_closes.begin(); it != g_pending_proxy_pipe_closes.end();) {
            auto* pending = *it;
            if (pending == nullptr || pending->home_node != node_id) {
                ++it;
                continue;
            }
            if (batch.detached_proxy_close_count >= batch.detached_proxy_closes.size()) {
                break;
            }

            batch.detached_proxy_closes.at(batch.detached_proxy_close_count++) = pending;
            batch.made_progress = true;
            it = g_pending_proxy_pipe_closes.erase(it);
        }
    }

    if (!batch_full) {
        for (auto& queue : g_ipc_dev_op_queues) {
            IpcDevOpWork* previous = nullptr;
            IpcDevOpWork* work = queue.head;
            while (work != nullptr) {
                IpcDevOpWork* const NEXT = work->next;
                if (work->hdr.src_node != node_id) {
                    previous = work;
                    work = NEXT;
                    continue;
                }
                if (batch.detached_dev_op_count >= batch.detached_dev_ops.size()) {
                    batch_full = true;
                    break;
                }

                if (previous != nullptr) {
                    previous->next = NEXT;
                } else {
                    queue.head = NEXT;
                }
                if (queue.tail == work) {
                    queue.tail = previous;
                }
                queue.count--;
                work->next = nullptr;
                batch.detached_dev_ops.at(batch.detached_dev_op_count++) = work;
                batch.made_progress = true;
                work = NEXT;
            }
            if (batch_full) {
                break;
            }
        }
    }

    for (auto* backlog : g_export_pipe_write_backlogs) {
        auto* exp = backlog != nullptr ? backlog->exp : nullptr;
        if (exp != nullptr && exp->consumer_node == node_id) {
            if (backlog != nullptr) {
                backlog->exp = nullptr;
                queue_export_pipe_write_flush_locked(backlog);
            }
            batch.wake_export_pipe_flush = true;
            batch.made_progress = true;
        }
    }

    batch.retired_exports = compact_inactive_exports_locked();
    batch.made_progress = batch.made_progress || batch.retired_exports != nullptr;
}

void drain_ipc_peer_cleanup_batch(IpcPeerCleanupBatch& batch) {
    for (auto* stopped_pump : std::span(batch.stopped_pumps.data(), batch.stopped_pump_count)) {
        wake_pipe_pump(stopped_pump);
    }

    if (batch.wake_export_pipe_flush) {
        wake_all_export_pipe_write_flush_workers();
    }

    for (auto* file : std::span(batch.released_files.data(), batch.released_file_count)) {
        ipc_release_file_ref(file);
    }

    for (auto* proxy : std::span(batch.detached_proxies.data(), batch.detached_proxy_count)) {
        WkiWaitEntry* pending_wait = nullptr;
        {
            uint64_t const PROXY_IRQF = proxy->lock.lock_irqsave();
            pending_wait = claim_proxy_pending_wait_locked(proxy, -EIO);
            proxy->lock.unlock_irqrestore(PROXY_IRQF);
        }
        auto* reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
        wake_blocked_ipc_reader(reader);
        proxy_wake_stream_readers(proxy);
        proxy_wake_message_write_waiters(proxy);
        wki_ipc_proxy_wake_poll_waiters(proxy);
        if (pending_wait != nullptr) {
            wki_finish_claimed_op(pending_wait, 0);
        }
        proxy_release(proxy);
    }

    for (auto* pending : std::span(batch.detached_pending.data(), batch.detached_pending_count)) {
        free_pending_pipe_delivery(pending);
    }

    for (auto* pending : std::span(batch.detached_proxy_closes.data(), batch.detached_proxy_close_count)) {
        free_pending_proxy_pipe_close(pending);
    }
    for (auto* work : std::span(batch.detached_dev_ops.data(), batch.detached_dev_op_count)) {
        free_ipc_dev_op_work(work);
    }
    free_retired_exports(batch.retired_exports);
}

}  // namespace

void wki_ipc_mark_epoch_reset(uint16_t node_id) {
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    begin_ipc_peer_dev_op_cleanup_locked(node_id);
    for (auto* exp : g_ipc_exports) {
        if (exp != nullptr && exp->consumer_node == node_id) exp->active = false;
    }
    for (auto* proxy : g_ipc_proxies) {
        if (proxy != nullptr && proxy->home_node == node_id) {
            proxy->active.store(false, std::memory_order_release);
            proxy->message_write_error.store(-EIO, std::memory_order_release);
        }
    }
    s_ipc_lock.unlock_irqrestore(IRQF);
}

void wki_ipc_cleanup_for_peer(uint16_t node_id) {
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        begin_ipc_peer_dev_op_cleanup_locked(node_id);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    // Closing admission and changing the epoch prevents new claims. Drain the
    // bounded set of already-claimed workers before destroying endpoint state
    // they may still reference or publish into.
    for (;;) {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        bool const DRAINED = ipc_peer_active_workers_locked(node_id) == 0;
        s_ipc_lock.unlock_irqrestore(IRQF);
        if (DRAINED) {
            break;
        }
        ker::mod::sched::kern_yield();
    }

    for (;;) {
        IpcPeerCleanupBatch batch;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        collect_ipc_peer_cleanup_batch_locked(node_id, batch);
        s_ipc_lock.unlock_irqrestore(IRQF);

        bool const STOPPED_PUMP_PENDING = batch.stopped_pump_count != 0;
        drain_ipc_peer_cleanup_batch(batch);
        if (!batch.made_progress) {
            break;
        }

        // A stopped pipe pump retains its export slot until the owning pump
        // worker observes pump_running=false and publishes retirement. Give
        // that already-woken worker a scheduling point before inspecting the
        // same slot again; otherwise this deferred cleanup thread can spin on
        // false progress and starve the worker that makes cleanup converge.
        if (STOPPED_PUMP_PENDING) {
            ker::mod::sched::kern_yield();
        }
    }

    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        end_ipc_peer_dev_op_cleanup_locked(node_id);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
}

// =============================================================================
// Internal RX handlers (for future use with dedicated IPC channels)
// =============================================================================

namespace detail {

void handle_ipc_attach_req(const WkiHeader* /*hdr*/, const uint8_t* /*payload*/, uint16_t /*payload_len*/) {
    // Future: handle explicit IPC resource attach requests
}

void handle_ipc_attach_ack(const WkiHeader* /*hdr*/, const uint8_t* /*payload*/, uint16_t /*payload_len*/) {
    // Future: handle IPC resource attach acknowledgements
}

}  // namespace detail

namespace {

void handle_ipc_dev_op_req_inline(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, IpcDevOpWork** work_owner,
                                  bool* retry_out, WkiChannelIdentity response_channel) {
    if (retry_out != nullptr) {
        *retry_out = false;
    }
    auto retry_owned_work = [&]() {
        if (retry_out != nullptr && work_owner != nullptr && *work_owner != nullptr) {
            *retry_out = true;
        }
    };
    auto send_response = [&](const void* bytes, uint16_t length) {
        if (response_channel.peer_node_id != hdr->src_node || response_channel.channel_id != hdr->channel_id) return WKI_ERR_INVALID;
        return wki_send_on_channel_identity(response_channel, MsgType::DEV_OP_RESP, bytes, length);
    };
    if (payload_len < sizeof(DevOpReqPayload)) {
        return;
    }

    DevOpReqPayload req = {};
    std::memcpy(&req, payload, sizeof(req));
    uint16_t const OP_ID = req.op_id;

    // All IPC ops carry resource_id as first 4 bytes of data
    if (req.data_len < sizeof(uint32_t)) {
        return;
    }
    if (sizeof(DevOpReqPayload) + req.data_len > payload_len) {
        return;
    }
    const auto* data = payload + sizeof(DevOpReqPayload);
    uint32_t resource_id = 0;
    std::memcpy(&resource_id, data, sizeof(uint32_t));
    const auto* op_data = data + sizeof(uint32_t);
    uint16_t op_data_len = req.data_len - sizeof(uint32_t);
    uint16_t request_cookie = 0;
    if (ipc_op_uses_response_cookie(OP_ID)) {
        if (op_data_len < WKI_IPC_OP_COOKIE_BYTES) {
            return;
        }
        std::memcpy(&request_cookie, op_data, WKI_IPC_OP_COOKIE_BYTES);
        op_data += WKI_IPC_OP_COOKIE_BYTES;
        op_data_len = static_cast<uint16_t>(op_data_len - WKI_IPC_OP_COOKIE_BYTES);
    }
    uint16_t const OP_DATA_LEN = op_data_len;
    uint64_t const IPC_CALLSITE = WOS_PERF_CALLSITE();
    auto const IPC_CORRELATION = static_cast<uint32_t>(hdr->seq_num & UINT16_MAX);

    if (OP_ID == OP_PIPE_WRITE_FLOW) {
        if (OP_DATA_LEN < sizeof(uint32_t) + sizeof(int32_t)) {
            return;
        }

        uint32_t credit_bytes = 0;
        int32_t status = 0;
        std::memcpy(&credit_bytes, op_data, sizeof(credit_bytes));
        std::memcpy(&status, op_data + sizeof(credit_bytes), sizeof(status));
        if (status > 0) {
            status = -EIO;
        }

        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        auto* proxy = find_proxy_by_endpoint_locked(hdr->src_node, resource_id, true);
        s_ipc_lock.unlock_irqrestore(IRQF);
        if (proxy == nullptr) {
            return;
        }

        if (status != 0) {
            proxy->message_write_error.store(status, std::memory_order_release);
            proxy->message_write_credits.store(0, std::memory_order_release);
        } else if (proxy->message_write_flow_control) {
            proxy->bytes_delivered.fetch_add(credit_bytes, std::memory_order_release);
            proxy_grant_message_write_credits(proxy, credit_bytes);
        }
        proxy_wake_message_write_waiters(proxy);
        wki_ipc_proxy_wake_poll_waiters(proxy);
        proxy_release(proxy);
        return;
    }

    if (OP_ID == OP_PIPE_DISCARD_PENDING) {
        static_cast<void>(discard_pending_pipe_delivery(hdr->src_node, resource_id));
        return;
    }

    if (OP_ID == OP_PIPE_DATA || OP_ID == OP_PIPE_DATA_FLOW) {
        bool const FLOW_CONTROLLED = OP_ID == OP_PIPE_DATA_FLOW;
        IpcPerfTrace pipe_trace(ker::mod::perf::WkiPerfIpcOp::PIPE_DATA, hdr->src_node, hdr->channel_id, IPC_CALLSITE, OP_DATA_LEN,
                                IPC_CORRELATION);
        if (!FLOW_CONTROLLED && OP_DATA_LEN == 0 && queue_export_pipe_rdma_flush(hdr->src_node, resource_id)) {
            pipe_trace.finish(0);
            return;
        }
        bool const ALREADY_ACCOUNTED = work_owner != nullptr && *work_owner != nullptr && (*work_owner)->payload_accounted;
        if (OP_DATA_LEN != 0 && !ALREADY_ACCOUNTED) {
            g_pipe_payload_bytes.fetch_add(OP_DATA_LEN, std::memory_order_relaxed);
            if (work_owner != nullptr && *work_owner != nullptr) {
                (*work_owner)->payload_accounted = true;
            }
        }

        // Consumer receives pipe data — write into the local proxy ring if
        // present, otherwise route it to the exported home-side pipe.
        ker::vfs::File* export_file = nullptr;
        bool export_pty_immediate = false;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        auto* proxy = FLOW_CONTROLLED ? nullptr : find_proxy_by_endpoint_locked(hdr->src_node, resource_id);
        ProxyIpcState* nonreceiving_proxy = nullptr;
        if (proxy != nullptr && !proxy->can_receive_data) {
            // Node-local resource IDs can collide in opposite directions.
            // A write-only proxy is not a receive target, so keep looking for
            // a same-ID home-side export before treating the DATA as invalid.
            nonreceiving_proxy = proxy;
            proxy = nullptr;
        }
        bool const PENDING_BUFFERED = !FLOW_CONTROLLED && find_pending_pipe_delivery_locked(hdr->src_node, resource_id) != nullptr;
        bool export_exists = false;
        if (proxy == nullptr) {
            auto* exp = find_export_by_resource_id(resource_id);
            if (exp != nullptr && exp->active && exp->file != nullptr && exp->consumer_node == hdr->src_node) {
                export_exists = true;
                bool const HAS_BACKLOG = find_export_pipe_write_backlog_locked(exp) != nullptr;
                if (!FLOW_CONTROLLED && work_owner == nullptr && exp->res_type == ResourceType::IPC_PTY && !HAS_BACKLOG &&
                    exp->file->fops != nullptr && exp->file->fops->vfs_write != nullptr) {
                    export_file = exp->file;
                    export_file->refcount.fetch_add(1, std::memory_order_acq_rel);
                    export_pty_immediate = true;
                }
            }
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
        bool const HAD_NONRECEIVING_PROXY = nonreceiving_proxy != nullptr;
        proxy_release(nonreceiving_proxy);

        if (proxy != nullptr) {
            if (OP_DATA_LEN == 0) {
                proxy_release(proxy);
                return;
            }

            if (PENDING_BUFFERED || proxy->ring_buf == nullptr) {
                if (!queue_pending_pipe_data(hdr->src_node, resource_id, op_data, OP_DATA_LEN, work_owner)) {
                    ker::mod::dbg::log("[WKI] IPC pending pipe DATA queue failed: resource_id=%u len=%u", resource_id, OP_DATA_LEN);
                    retry_owned_work();
                } else {
                    perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::WAKE_READER, hdr->src_node, hdr->channel_id, IPC_CORRELATION, 0,
                                          OP_DATA_LEN, IPC_CALLSITE);
                    wake_proxy_reader(proxy);
                    perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::POLL_WAKE, hdr->src_node, hdr->channel_id, IPC_CORRELATION, 0, 0,
                                          IPC_CALLSITE);
                    wki_ipc_proxy_wake_poll_waiters(proxy);
                }
                proxy_release(proxy);
                pipe_trace.finish(0, OP_DATA_LEN);
                return;
            }

            auto* reader = static_cast<ker::mod::sched::task::Task*>(nullptr);
            bool queue_overflow = false;
            uint64_t const PROXY_IRQF = proxy->lock.lock_irqsave();
            if (!proxy->active.load(std::memory_order_acquire) || proxy->ring_buf == nullptr) {
                proxy->lock.unlock_irqrestore(PROXY_IRQF);
                if (!queue_pending_pipe_data(hdr->src_node, resource_id, op_data, OP_DATA_LEN, work_owner)) {
                    ker::mod::dbg::log("[WKI] IPC pending pipe DATA queue failed: resource_id=%u len=%u", resource_id, OP_DATA_LEN);
                    retry_owned_work();
                } else {
                    perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::WAKE_READER, hdr->src_node, hdr->channel_id, IPC_CORRELATION, 0,
                                          OP_DATA_LEN, IPC_CALLSITE);
                    wake_proxy_reader(proxy);
                    perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::POLL_WAKE, hdr->src_node, hdr->channel_id, IPC_CORRELATION, 0, 0,
                                          IPC_CALLSITE);
                    wki_ipc_proxy_wake_poll_waiters(proxy);
                }
                proxy_release(proxy);
                pipe_trace.finish(0, OP_DATA_LEN);
                return;
            }

            uint32_t const HEAD = proxy->ring_head.load(std::memory_order_relaxed);
            uint32_t const TAIL = proxy->ring_tail.load(std::memory_order_acquire);
            uint32_t const CAP = proxy->ring_capacity;
            uint32_t const FREE_SPACE = CAP - (HEAD - TAIL);

            if (OP_DATA_LEN > FREE_SPACE) {
                queue_overflow = true;
                g_proxy_ring_full_waits.fetch_add(1, std::memory_order_relaxed);
                g_proxy_ring_full_bytes.fetch_add(OP_DATA_LEN, std::memory_order_relaxed);
            } else {
                uint32_t const RING_HEAD = HEAD % CAP;
                uint32_t const FIRST = CAP - RING_HEAD;
                if (FIRST >= OP_DATA_LEN) {
                    std::memcpy(proxy->ring_buf + RING_HEAD, op_data, OP_DATA_LEN);
                } else {
                    std::memcpy(proxy->ring_buf + RING_HEAD, op_data, FIRST);
                    std::memcpy(proxy->ring_buf, op_data + FIRST, OP_DATA_LEN - FIRST);
                }
                proxy->ring_head.store(HEAD + OP_DATA_LEN, std::memory_order_release);

                reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
            }
            proxy->lock.unlock_irqrestore(PROXY_IRQF);

            if (queue_overflow) {
                if (!queue_pending_pipe_data(hdr->src_node, resource_id, op_data, OP_DATA_LEN, work_owner)) {
                    ker::mod::dbg::log("[WKI] IPC pending pipe DATA queue failed: resource_id=%u len=%u", resource_id, OP_DATA_LEN);
                    retry_owned_work();
                } else {
                    perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::WAKE_READER, hdr->src_node, hdr->channel_id, IPC_CORRELATION, 0,
                                          OP_DATA_LEN, IPC_CALLSITE);
                    wake_proxy_reader(proxy);
                    perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::POLL_WAKE, hdr->src_node, hdr->channel_id, IPC_CORRELATION, 0, 0,
                                          IPC_CALLSITE);
                    wki_ipc_proxy_wake_poll_waiters(proxy);
                }
                proxy_release(proxy);
                pipe_trace.finish(0, OP_DATA_LEN);
                return;
            }

            if (reader != nullptr) {
                perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::WAKE_READER, hdr->src_node, hdr->channel_id, IPC_CORRELATION, 0,
                                      OP_DATA_LEN, IPC_CALLSITE);
                wake_blocked_ipc_reader(reader);
            }
            proxy_wake_stream_readers(proxy);
            perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::POLL_WAKE, hdr->src_node, hdr->channel_id, IPC_CORRELATION, 0, 0,
                                  IPC_CALLSITE);
            wki_ipc_proxy_wake_poll_waiters(proxy);
            proxy_release(proxy);
            pipe_trace.finish(0, OP_DATA_LEN);
            return;
        }

        if (!export_exists) {
            if (FLOW_CONTROLLED) {
                auto const CONTROL = capture_ipc_companion_channel(response_channel, WKI_CHAN_IPC_DATA, WKI_CHAN_RESOURCE);
                if (!send_export_pipe_write_flow(CONTROL, resource_id, 0, -EPIPE)) retry_owned_work();
                pipe_trace.finish(-EPIPE);
                return;
            }
            if (HAD_NONRECEIVING_PROXY) {
                // Do not turn impossible/stale DATA for a write-only proxy
                // into unbounded pending allocations in RX context.
                pipe_trace.finish(-EBADF);
                return;
            }
            if (OP_DATA_LEN > 0 && !queue_pending_pipe_data(hdr->src_node, resource_id, op_data, OP_DATA_LEN, work_owner)) {
                ker::mod::dbg::log("[WKI] IPC pending pipe DATA queue failed: resource_id=%u len=%u", resource_id, OP_DATA_LEN);
                retry_owned_work();
            }
            return;
        }
        if (OP_DATA_LEN == 0) {
            pipe_trace.finish(0);
            return;
        }
        if (export_pty_immediate && export_file != nullptr) {
            ssize_t const WRITE_RET = export_pipe_write_nonblocking(export_file, op_data, OP_DATA_LEN);
            ipc_release_file_ref(export_file);

            if (std::cmp_equal(WRITE_RET, OP_DATA_LEN)) {
                uint64_t const NOTE_IRQF = s_ipc_lock.lock_irqsave();
                if (auto* exp = find_export_by_resource_id(resource_id); exp != nullptr && exp->active) {
                    note_export_pipe_data_received_locked(exp, OP_DATA_LEN);
                }
                s_ipc_lock.unlock_irqrestore(NOTE_IRQF);
                pipe_trace.finish(0, OP_DATA_LEN);
                return;
            }

            uint16_t written = 0;
            if (WRITE_RET > 0) {
                written = static_cast<uint16_t>(std::min<ssize_t>(WRITE_RET, OP_DATA_LEN));
                uint64_t const NOTE_IRQF = s_ipc_lock.lock_irqsave();
                if (auto* exp = find_export_by_resource_id(resource_id); exp != nullptr && exp->active) {
                    note_export_pipe_data_received_locked(exp, written);
                }
                s_ipc_lock.unlock_irqrestore(NOTE_IRQF);
            } else if (WRITE_RET != -EAGAIN && WRITE_RET != -EINTR) {
                ker::mod::dbg::log("[WKI] IPC PTY export immediate write failed: resource_id=%u ret=%ld len=%u", resource_id, WRITE_RET,
                                   OP_DATA_LEN);
            }

            if (written >= OP_DATA_LEN) {
                pipe_trace.finish(0, OP_DATA_LEN);
                return;
            }
            if (!queue_export_pipe_write_data(resource_id, op_data + written, static_cast<uint16_t>(OP_DATA_LEN - written), work_owner,
                                              FLOW_CONTROLLED)) {
                ker::mod::dbg::log("[WKI] IPC PTY export backlog queue failed: resource_id=%u written=%u total=%u", resource_id, written,
                                   OP_DATA_LEN);
                retry_owned_work();
            }
            pipe_trace.finish(0, OP_DATA_LEN);
            return;
        }
        if (!queue_export_pipe_write_data(resource_id, op_data, OP_DATA_LEN, work_owner, FLOW_CONTROLLED)) {
            ker::mod::dbg::log("[WKI] IPC export pipe backlog queue failed: resource_id=%u len=%u", resource_id, OP_DATA_LEN);
            retry_owned_work();
        }
        pipe_trace.finish(0, OP_DATA_LEN);
        return;
    }

    if (OP_ID == OP_PIPE_POLL_STATE) {
        // Consumer queries current readiness of the exported pipe/socket.
        // Server checks poll_check on the underlying file and sends back a
        // response with readiness flags.
        ker::vfs::File* f = nullptr;
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            f = ipc_acquire_export_file_locked(resource_id, hdr->src_node);
            s_ipc_lock.unlock_irqrestore(IRQF);
        }

        DevOpRespPayload poll_resp = {};
        poll_resp.op_id = OP_PIPE_POLL_STATE;
        poll_resp.status = 0;
        poll_resp.data_len = sizeof(uint32_t) * 2;  // [resource_id, ready_flags]
        poll_resp.reserved = 0;

        std::array<uint8_t, sizeof(DevOpRespPayload) + (sizeof(uint32_t) * 2)> poll_resp_buf = {};
        std::memcpy(poll_resp_buf.data(), &poll_resp, sizeof(poll_resp));
        std::memcpy(poll_resp_buf.data() + sizeof(DevOpRespPayload), &resource_id, sizeof(uint32_t));

        int ready_flags = 0;
        if (f != nullptr) {
            if (f->fops != nullptr && f->fops->vfs_poll_check != nullptr) {
                ready_flags = f->fops->vfs_poll_check(f, 0x0001 | 0x0004);  // POLLIN | POLLOUT
            }
            ipc_release_file_ref(f);
        } else {
            poll_resp.status = -ENOENT;
        }

        std::memcpy(poll_resp_buf.data() + sizeof(DevOpRespPayload) + sizeof(uint32_t), &ready_flags, sizeof(uint32_t));
        send_response(poll_resp_buf.data(), static_cast<uint16_t>(poll_resp_buf.size()));
        return;
    }

    if (OP_ID == OP_EPOLL_CTL || OP_ID == OP_FUTEX_WAKE) {
        // Neither legacy record names an issued, retained target object:
        // epoll carries ambient FD numbers and futex carries a raw physical
        // address. Peer IPC permission cannot supply that missing authority.
        // Current export planning already refuses epoll and issues no futex
        // grants. Preserve completion identity without touching local state.
        auto const REPLY = unsupported_ipc_control_reply(OP_ID, resource_id, request_cookie);
        send_response(REPLY.data(), static_cast<uint16_t>(REPLY.size()));
        return;
    }

    if (OP_ID == OP_PTY_IOCTL) {
        // Forward ioctl to the home-node PTY file and return result.
        // op_data layout: [cmd:u64][arg_val:u64][arg_in:128B]
        constexpr size_t PTY_ARG_SIZE = 128;
        DevOpRespPayload pty_resp = {};
        pty_resp.op_id = OP_PTY_IOCTL;
        pty_resp.status = -EINVAL;
        pty_resp.data_len = static_cast<uint16_t>(sizeof(uint32_t) + PTY_ARG_SIZE);
        pty_resp.reserved = request_cookie;

        constexpr size_t RESP_BUF_SIZE = sizeof(DevOpRespPayload) + sizeof(uint32_t) + PTY_ARG_SIZE;
        std::array<uint8_t, RESP_BUF_SIZE> pty_resp_buf = {};
        std::memcpy(pty_resp_buf.data() + sizeof(DevOpRespPayload), &resource_id, sizeof(uint32_t));

        uint64_t ioctl_cmd = 0;
        uint64_t ioctl_arg_val = 0;
        constexpr size_t MIN_REQ = sizeof(uint64_t) + sizeof(uint64_t) + PTY_ARG_SIZE;
        if (OP_DATA_LEN >= MIN_REQ) {
            std::memcpy(&ioctl_cmd, op_data, sizeof(uint64_t));
            std::memcpy(&ioctl_arg_val, op_data + sizeof(uint64_t), sizeof(uint64_t));
            const uint8_t* arg_in = op_data + sizeof(uint64_t) + sizeof(uint64_t);
            PtyIoctlMarshal const MARSHAL =
                pty_ioctl_marshalling(static_cast<unsigned long>(ioctl_cmd), static_cast<unsigned long>(ioctl_arg_val));

            if (wait_diag_enabled()) {
                ker::mod::dbg::logger<"wki">::info(
                    "pty_ioctl_handle src=0x%04x res=%u cookie=%u cmd=%s(0x%lx) arg=0x%lx size=%lu in=%u out=%u",
                    static_cast<unsigned>(hdr->src_node), resource_id, static_cast<unsigned>(request_cookie),
                    pty_ioctl_cmd_name(static_cast<unsigned long>(ioctl_cmd)), static_cast<unsigned long>(ioctl_cmd),
                    static_cast<unsigned long>(ioctl_arg_val), static_cast<unsigned long>(MARSHAL.arg_size), MARSHAL.copy_in ? 1U : 0U,
                    MARSHAL.copy_out ? 1U : 0U);
            }

            ker::vfs::File* f = nullptr;
            {
                uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                f = ipc_acquire_export_file_locked(resource_id, hdr->src_node, ResourceType::IPC_PTY);
                s_ipc_lock.unlock_irqrestore(IRQF);
            }

            if (f != nullptr) {
                unsigned long effective_arg = 0;
                alignas(8) std::array<uint8_t, PTY_ARG_SIZE> local_buf = {};
                if (MARSHAL.pointer_arg) {
                    if (MARSHAL.copy_in && MARSHAL.arg_size > 0) {
                        std::memcpy(local_buf.data(), arg_in, MARSHAL.arg_size);
                    }
                    effective_arg = reinterpret_cast<unsigned long>(local_buf.data());
                } else {
                    // Value-type ioctl: pass arg_val directly
                    effective_arg = static_cast<unsigned long>(ioctl_arg_val);
                }

                int const RC = pty_session_mutation(static_cast<unsigned long>(ioctl_cmd))
                                   ? -ENOTSUP
                                   : ker::vfs::devfs::devfs_ioctl(f, static_cast<unsigned long>(ioctl_cmd), effective_arg);
                pty_resp.status = static_cast<int16_t>(RC < 0 ? RC : 0);
                if (wait_diag_enabled()) {
                    ker::mod::dbg::logger<"wki">::info(
                        "pty_ioctl_home_done src=0x%04x res=%u cookie=%u cmd=%s(0x%lx) rc=%d", static_cast<unsigned>(hdr->src_node),
                        resource_id, static_cast<unsigned>(request_cookie), pty_ioctl_cmd_name(static_cast<unsigned long>(ioctl_cmd)),
                        static_cast<unsigned long>(ioctl_cmd), RC);
                }

                // Always copy back local buffer (for output ioctls)
                std::memcpy(pty_resp_buf.data() + sizeof(DevOpRespPayload) + sizeof(uint32_t), local_buf.data(), PTY_ARG_SIZE);
                ipc_release_file_ref(f);
            } else {
                pty_resp.status = -ENOENT;
                if (wait_diag_enabled()) {
                    ker::mod::dbg::logger<"wki">::warn(
                        "pty_ioctl_no_export src=0x%04x res=%u cookie=%u cmd=%s(0x%lx)", static_cast<unsigned>(hdr->src_node), resource_id,
                        static_cast<unsigned>(request_cookie), pty_ioctl_cmd_name(static_cast<unsigned long>(ioctl_cmd)),
                        static_cast<unsigned long>(ioctl_cmd));
                }
            }
        } else if (wait_diag_enabled()) {
            ker::mod::dbg::logger<"wki">::warn("pty_ioctl_bad_request src=0x%04x res=%u cookie=%u data_len=%u",
                                               static_cast<unsigned>(hdr->src_node), resource_id, static_cast<unsigned>(request_cookie),
                                               static_cast<unsigned>(OP_DATA_LEN));
        }

        std::memcpy(pty_resp_buf.data(), &pty_resp, sizeof(pty_resp));
        int const RESP_TX = send_response(pty_resp_buf.data(), static_cast<uint16_t>(pty_resp_buf.size()));
        if (wait_diag_enabled()) {
            ker::mod::dbg::logger<"wki">::info("pty_ioctl_resp_send dst=0x%04x res=%u cookie=%u cmd=%s(0x%lx) status=%d tx=%d",
                                               static_cast<unsigned>(hdr->src_node), resource_id, static_cast<unsigned>(request_cookie),
                                               pty_ioctl_cmd_name(static_cast<unsigned long>(ioctl_cmd)),
                                               static_cast<unsigned long>(ioctl_cmd), static_cast<int>(pty_resp.status), RESP_TX);
        }
        return;
    }

    if (OP_ID == OP_PTY_CLOSE) {
        bool has_expected_bytes = false;
        uint64_t expected_bytes = 0;
        if (OP_DATA_LEN >= sizeof(expected_bytes)) {
            std::memcpy(&expected_bytes, op_data, sizeof(expected_bytes));
            has_expected_bytes = true;
        }

        // PTY payloads use the pipe data path, so close must follow the ordered write backlog or
        // become a pending EOF marker if the matching endpoint has not arrived yet.
        if (!mark_ipc_pty_write_closed_or_pending(hdr->src_node, resource_id, expected_bytes, has_expected_bytes, work_owner)) {
            ker::mod::dbg::logger<"wki">::warn("IPC PTY close queue failed: resource_id=%u", resource_id);
            retry_owned_work();
        }
        return;
    }

    if (OP_ID == OP_PIPE_CLOSE_WRITE || OP_ID == OP_PIPE_CLOSE_WRITE_FLOW) {
        bool const FLOW_CONTROLLED = OP_ID == OP_PIPE_CLOSE_WRITE_FLOW;
        // Server-side: consumer closed write end — close the real pipe's read end
        // Client-side: home node reports write-end closed (EOF)
        bool has_expected_bytes = false;
        uint64_t expected_bytes = 0;
        if (OP_DATA_LEN >= sizeof(expected_bytes)) {
            std::memcpy(&expected_bytes, op_data, sizeof(expected_bytes));
            has_expected_bytes = true;
        }

        uint64_t const IRQF = s_ipc_lock.lock_irqsave();

        // Check consumer-side proxy first
        auto* proxy = find_proxy_by_endpoint_locked(hdr->src_node, resource_id);
        ProxyIpcState* nonreceiving_proxy = nullptr;
        if (proxy != nullptr && !proxy->can_receive_data) {
            nonreceiving_proxy = proxy;
            proxy = nullptr;
        }
        if (proxy != nullptr) {
            s_ipc_lock.unlock_irqrestore(IRQF);

            proxy_mark_pipe_closed(proxy, resource_id);
            proxy_release(proxy);
            return;
        }

        // Check server-side export
        auto* exp = find_export_by_resource_id(resource_id);
        if (exp != nullptr && exp->active && exp->file != nullptr && exp->consumer_node == hdr->src_node) {
            s_ipc_lock.unlock_irqrestore(IRQF);
            proxy_release(nonreceiving_proxy);
            if (!mark_export_pipe_write_closed(resource_id, hdr->src_node, expected_bytes, has_expected_bytes)) {
                ker::mod::dbg::log("[WKI] IPC export pipe close queue failed: resource_id=%u", resource_id);
                retry_owned_work();
            }
            return;
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
        bool const HAD_NONRECEIVING_PROXY = nonreceiving_proxy != nullptr;
        proxy_release(nonreceiving_proxy);

        if (HAD_NONRECEIVING_PROXY) {
            return;
        }

        if (FLOW_CONTROLLED) {
            return;
        }
        if (!mark_pending_pipe_write_closed(hdr->src_node, resource_id, work_owner)) {
            ker::mod::dbg::log("[WKI] IPC pending pipe CLOSE queue failed: resource_id=%u", resource_id);
            retry_owned_work();
        }
        return;
    }

    if (OP_ID == OP_PIPE_CLOSE_READ) {
        // Consumer closed read end — stop the pump, release the export's file
        // ref, and fence any EOF that races with proxy detachment. A running
        // pump emits the discard after its last DATA/CLOSE send; without a
        // pump there can be no later sender, so emit it directly.
        ker::mod::sched::task::Task* pump_task = nullptr;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        auto* exp = find_export_by_resource_id(resource_id);
        if (ipc_export_matches_consumer(exp, resource_id, hdr->src_node)) {
            pump_task = stop_pipe_pump_locked(exp);
            bool const DIRECT_DISCARD = pump_task == nullptr;
            auto const DATA_CHANNEL = exp->channels.data;
            ker::vfs::File* f = exp->file;
            exp->file = nullptr;
            exp->active = false;
            auto* retired_exports = compact_inactive_exports_locked();
            s_ipc_lock.unlock_irqrestore(IRQF);
            wake_pipe_pump(pump_task);
            ipc_release_file_ref(f);
            free_retired_exports(retired_exports);
            if (DIRECT_DISCARD) {
                schedule_pending_pipe_discard(DATA_CHANNEL, resource_id);
            }
            return;
        }
        bool const WRONG_CONSUMER = exp != nullptr;
        s_ipc_lock.unlock_irqrestore(IRQF);
        if (WRONG_CONSUMER) return;
        // An already-retired export can still receive the consumer's ordered
        // close. Re-send the idempotent fence so a late EOF cannot recreate
        // ownerless pre-attachment state.
        schedule_pending_pipe_discard(response_channel, resource_id);
        return;
    }

    if (OP_ID >= OP_SOCK_ACCEPT && OP_ID <= OP_SOCK_SETSOCKOPT) {
        // Build a response with resource_id as first 4 bytes so the consumer
        // can find the right proxy in wki_ipc_socket_handle_dev_op_resp().
        constexpr size_t RESP_HEADER = sizeof(DevOpRespPayload);
        constexpr size_t RID_SIZE = sizeof(uint32_t);

        // Reserve up to 128 bytes for optional response payload (e.g. getpeername addr)
        constexpr size_t EXTRA_MAX = 128;
        std::array<uint8_t, RESP_HEADER + RID_SIZE + EXTRA_MAX> resp_buf = {};
        auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
        resp->op_id = OP_ID;
        resp->status = -ENOSYS;
        resp->data_len = RID_SIZE;  // at minimum include resource_id
        resp->reserved = request_cookie;
        std::memcpy(resp_buf.data() + RESP_HEADER, &resource_id, RID_SIZE);

        if (OP_ID == OP_SOCK_CLOSE) {
            resp->status = mark_export_pipe_write_closed(resource_id, hdr->src_node, 0, false, ResourceType::IPC_SOCKET) ? 0 : -ENOENT;
        } else if (OP_ID == OP_SOCK_SHUTDOWN) {
            // op_data: [how:int32][optional next DATA sequence:uint32]
            if (OP_DATA_LEN >= sizeof(int32_t)) {
                int32_t how = 0;
                std::memcpy(&how, op_data, sizeof(int32_t));
                ker::vfs::File* f = nullptr;
                WkiChannelIdentity data_channel{};
                bool const FENCED = how != 0 && OP_DATA_LEN >= sizeof(int32_t) + sizeof(uint32_t);
                uint32_t boundary = 0;
                uint32_t dispatched = 0;
                bool live = true;
                if (FENCED) {
                    std::memcpy(&boundary, op_data + sizeof(int32_t), sizeof(boundary));
                    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                    auto* exp = find_export_by_resource_id(resource_id);
                    if (ipc_export_matches_consumer(exp, resource_id, hdr->src_node, ResourceType::IPC_SOCKET))
                        data_channel = exp->channels.data;
                    s_ipc_lock.unlock_irqrestore(IRQF);
                    live = wki_ipc_capture_data_sequence(data_channel, false, dispatched);
                }
                bool pending = false;
                {
                    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                    auto* exp = find_export_by_resource_id(resource_id);
                    if (how < 0 || how > 2) {
                        resp->status = -EINVAL;
                    } else if (!ipc_export_matches_consumer(exp, resource_id, hdr->src_node, ResourceType::IPC_SOCKET) ||
                               exp->file == nullptr) {
                        resp->status = -ENOENT;
                    } else if (!live) {
                        resp->status = -EHOSTUNREACH;
                    } else if (FENCED && !ipc_channel_identity_equal(exp->channels.data, data_channel)) {
                        resp->status = -ENOENT;
                    } else {
                        uint64_t const POOL_IRQF = s_ipc_dev_op_pool_lock.lock_irqsave();
                        pending = FENCED && exp->endpoint_delivery_group != 0 &&
                                  (seq_before(dispatched, boundary) ||
                                   endpoint_delivery_prefix_pending_locked(exp->endpoint_delivery_group, data_channel, boundary));
                        s_ipc_dev_op_pool_lock.unlock_irqrestore(POOL_IRQF);
                        if (!pending) f = ipc_acquire_export_file_locked(resource_id, hdr->src_node, ResourceType::IPC_SOCKET);
                    }
                    s_ipc_lock.unlock_irqrestore(IRQF);
                }
                if (pending) {
                    retry_owned_work();
                    return;
                }
                if (f != nullptr) {
                    auto* sock = static_cast<ker::net::Socket*>(f->private_data);
                    if (sock != nullptr && sock->proto_ops != nullptr && sock->proto_ops->shutdown != nullptr) {
                        resp->status = static_cast<int16_t>(sock->proto_ops->shutdown(sock, static_cast<int>(how)));
                    } else {
                        resp->status = -ENOSYS;
                    }
                    ipc_release_file_ref(f);
                }
            } else {
                resp->status = -EINVAL;
            }
        } else if (OP_ID == OP_SOCK_GETPEERNAME) {
            // op_data: [] (no extra args needed)
            ker::vfs::File* f = nullptr;
            {
                uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                f = ipc_acquire_export_file_locked(resource_id, hdr->src_node, ResourceType::IPC_SOCKET);
                s_ipc_lock.unlock_irqrestore(IRQF);
            }
            if (f != nullptr) {
                auto* sock = static_cast<ker::net::Socket*>(f->private_data);
                if (sock != nullptr) {
                    auto const& remote = sock->remote;
                    if (remote.is_ipv4() || remote.is_v4_mapped()) {
                        // The remote-IPC socket wire record remains IPv4-only;
                        // dual-stack mapped endpoints are projected to AF_INET.
                        struct {
                            uint16_t family;
                            uint32_t addr;
                            uint16_t port;
                        } __attribute__((packed)) peer_addr = {};
                        static_assert(sizeof(peer_addr) == 8);
                        peer_addr.family = ker::net::SOCKADDR_V4_FAMILY;
                        peer_addr.addr = remote.ipv4_address().to_host_order();
                        peer_addr.port = remote.port;
                        std::memcpy(resp_buf.data() + RESP_HEADER + RID_SIZE, &peer_addr, sizeof(peer_addr));
                        resp->data_len = static_cast<uint16_t>(RID_SIZE + sizeof(peer_addr));
                        resp->status = 0;
                    } else {
                        resp->status = -EAFNOSUPPORT;
                    }
                } else {
                    resp->status = -ENOTSOCK;
                }
                ipc_release_file_ref(f);
            } else {
                resp->status = -ENOENT;
            }
        } else if (OP_ID == OP_SOCK_GETSOCKOPT) {
            // op_data: [level:int32][optname:int32]
            if (OP_DATA_LEN >= 2 * sizeof(int32_t)) {
                int32_t level = 0;
                int32_t optname = 0;
                std::memcpy(&level, op_data, sizeof(int32_t));
                std::memcpy(&optname, op_data + sizeof(int32_t), sizeof(int32_t));
                ker::vfs::File* f = nullptr;
                {
                    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                    f = ipc_acquire_export_file_locked(resource_id, hdr->src_node, ResourceType::IPC_SOCKET);
                    s_ipc_lock.unlock_irqrestore(IRQF);
                }
                if (f != nullptr) {
                    auto* sock = static_cast<ker::net::Socket*>(f->private_data);
                    if (sock != nullptr && sock->proto_ops != nullptr && sock->proto_ops->getsockopt != nullptr) {
                        std::array<uint8_t, 64> opt_val = {};
                        size_t opt_len = opt_val.size();
                        int const RC =
                            sock->proto_ops->getsockopt(sock, static_cast<int>(level), static_cast<int>(optname), opt_val.data(), &opt_len);
                        resp->status = static_cast<int16_t>(RC);
                        if (RC == 0 && opt_len > 0 && opt_len <= EXTRA_MAX) {
                            std::memcpy(resp_buf.data() + RESP_HEADER + RID_SIZE, opt_val.data(), opt_len);
                            resp->data_len = static_cast<uint16_t>(RID_SIZE + opt_len);
                        }
                    } else {
                        resp->status = -ENOSYS;
                    }
                    ipc_release_file_ref(f);
                } else {
                    resp->status = -ENOENT;
                }
            } else {
                resp->status = -EINVAL;
            }
        } else if (OP_ID == OP_SOCK_SETSOCKOPT) {
            // op_data: [level:int32][optname:int32][optval...]
            if (OP_DATA_LEN >= 2 * sizeof(int32_t)) {
                int32_t level = 0;
                int32_t optname = 0;
                std::memcpy(&level, op_data, sizeof(int32_t));
                std::memcpy(&optname, op_data + sizeof(int32_t), sizeof(int32_t));
                const uint8_t* optval = op_data + (2 * sizeof(int32_t));
                auto const OPTVAL_LEN = static_cast<uint16_t>(OP_DATA_LEN - (2 * sizeof(int32_t)));
                ker::vfs::File* f = nullptr;
                {
                    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                    f = ipc_acquire_export_file_locked(resource_id, hdr->src_node, ResourceType::IPC_SOCKET);
                    s_ipc_lock.unlock_irqrestore(IRQF);
                }
                if (f != nullptr) {
                    auto* sock = static_cast<ker::net::Socket*>(f->private_data);
                    if (sock != nullptr && sock->proto_ops != nullptr && sock->proto_ops->setsockopt != nullptr) {
                        int const RC =
                            sock->proto_ops->setsockopt(sock, static_cast<int>(level), static_cast<int>(optname), optval, OPTVAL_LEN);
                        resp->status = static_cast<int16_t>(RC);
                    } else {
                        resp->status = -ENOSYS;
                    }
                    ipc_release_file_ref(f);
                } else {
                    resp->status = -ENOENT;
                }
            } else {
                resp->status = -EINVAL;
            }
        }

        send_response(resp_buf.data(), static_cast<uint16_t>(RESP_HEADER + resp->data_len));
        return;
    }

    (void)hdr;
}

}  // namespace

namespace detail {

void handle_ipc_dev_op_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannelIdentity response_channel) {
    if (hdr == nullptr || payload == nullptr || payload_len < sizeof(DevOpReqPayload)) {
        return;
    }

    DevOpReqPayload req = {};
    std::memcpy(&req, payload, sizeof(req));
    if (req.data_len < sizeof(uint32_t) || sizeof(DevOpReqPayload) + req.data_len > payload_len) {
        return;
    }

    // Reliable RX reserves every allocation- or I/O-bearing IPC request in
    // fixed storage before ACK publication. Only the bounded legacy epoll
    // refusal is allowed to reach the inline dispatcher.
    if (req.op_id != OP_EPOLL_CTL) {
        return;
    }

    handle_ipc_dev_op_req_inline(hdr, payload, payload_len, nullptr, nullptr, response_channel);
}

void handle_ipc_dev_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    wki_ipc_handle_dev_op_resp(hdr->src_node, hdr->channel_id, payload, payload_len);
}

}  // namespace detail

// =============================================================================
// Public: incoming IPC DEV_OP_REQ dispatch (called from wki.cpp)
// =============================================================================

void wki_ipc_handle_dev_op_req(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len) {
    if (len < sizeof(DevOpReqPayload)) {
        return;
    }
    DevOpReqPayload req = {};
    std::memcpy(&req, payload, sizeof(req));

    // Route IPC ops (0x0700 - 0x07FF)
    if (req.op_id >= 0x0700 && req.op_id <= 0x07FF) {
        WkiHeader hdr = {};
        hdr.src_node = src_node;
        hdr.channel_id = channel;
        detail::handle_ipc_dev_op_req(&hdr, payload, len);
    }
}

}  // namespace ker::net::wki
