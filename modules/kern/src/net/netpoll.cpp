#include "netpoll.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dev/virtio/virtio_net.hpp>
#include <iterator>
#include <net/backlog.hpp>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <net/proto/tcp.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/context_switch.hpp>
#include <platform/sys/spinlock.hpp>
#include <string_view>
#include <util/smallvec.hpp>

namespace ker::net {

using log = ker::mod::dbg::logger<"netpoll">;

extern "C" char __kernel_text_start[];  // NOLINT(readability-identifier-naming)
extern "C" char __kernel_text_end[];    // NOLINT(readability-identifier-naming)

namespace {

constexpr uint32_t NET_LATENCY_DAEMON_SLICE_NS = 2'000'000;
constexpr int NET_LATENCY_DAEMON_NICE = -5;
constexpr int NAPI_WORKER_MAX_FULL_BUDGET_POLLS = 8;
constexpr uint64_t NAPI_WORKER_IDLE_SLEEP_US = 1000;
constexpr uint64_t NAPI_WORKER_CONTENTION_SLEEP_US = 50;
constexpr int NAPI_RESCUE_MAX_PASSES = 4;
constexpr uint64_t NAPI_RESCUE_IDLE_SLEEP_US = 10000;
constexpr uint64_t NAPI_RESCUE_ACTIVE_SLEEP_US = 1000;
constexpr uint64_t BACKLOG_RESCUE_MIN_QUEUE_DEPTH = 64;
constexpr uint64_t NET_WATCHDOG_INTERVAL_US = 5000000;
constexpr size_t NET_WATCHDOG_TCP_LISTENERS = 16;
constexpr size_t NET_WATCHDOG_TCP_CONNS = 32;
constexpr size_t NET_WATCHDOG_RUNQ_TASKS = 16;
constexpr size_t NAPI_INLINE_REGISTRY_CAPACITY = 64;
constexpr size_t NAPI_POLL_SNAPSHOT_CAPACITY = 64;

// Registry of all NAPI structures for worker thread lookup
ker::util::SmallVec<NapiStruct*, NAPI_INLINE_REGISTRY_CAPACITY> g_napi_registry;
ker::mod::sys::Spinlock g_registry_lock;
std::atomic<bool> g_rescue_started{false};
ker::mod::sched::task::Task* g_rescue_task = nullptr;
std::atomic<bool> g_watchdog_started{false};
ker::mod::sched::task::Task* g_watchdog_task = nullptr;

void promote_latency_sensitive_daemon(ker::mod::sched::task::Task* task) {
    if (task == nullptr || task->type != ker::mod::sched::task::TaskType::DAEMON) {
        return;
    }

    task->slice_ns = NET_LATENCY_DAEMON_SLICE_NS;
    ker::mod::sched::set_task_nice(task, NET_LATENCY_DAEMON_NICE);
}

auto poll_fn_is_valid(NapiPollFn poll) -> bool {
    auto const ADDR = reinterpret_cast<uintptr_t>(poll);
    auto const TEXT_START = reinterpret_cast<uintptr_t>(__kernel_text_start);
    auto const TEXT_END = reinterpret_cast<uintptr_t>(__kernel_text_end);
    return ADDR >= TEXT_START && ADDR < TEXT_END;
}

auto cmdline_has_token(const char* cmdline, const char* token) -> bool {
    if (cmdline == nullptr || token == nullptr || token[0] == '\0') {
        return false;
    }

    size_t const TOKEN_LEN = std::strlen(token);
    const char* cur = cmdline;
    while (*cur != '\0') {
        while (*cur == ' ' || *cur == '\t' || *cur == '\n') {
            ++cur;
        }
        if (*cur == '\0') {
            break;
        }
        const char* start = cur;
        while (*cur != '\0' && *cur != ' ' && *cur != '\t' && *cur != '\n') {
            ++cur;
        }
        auto const LEN = static_cast<size_t>(cur - start);
        if (LEN == TOKEN_LEN && std::strncmp(start, token, TOKEN_LEN) == 0) {
            return true;
        }
    }
    return false;
}

auto net_watchdog_enabled_impl() -> bool {
    static std::atomic<int> cached{-1};
    int const VALUE = cached.load(std::memory_order_acquire);
    if (VALUE >= 0) {
        return VALUE != 0;
    }

    bool const ENABLED = cmdline_has_token(ker::init::get_kernel_cmdline(), "net.watchdog");
    cached.store(ENABLED ? 1 : 0, std::memory_order_release);
    return ENABLED;
}

auto napi_rescue_enabled() -> bool {
    static std::atomic<int> cached{-1};
    int const VALUE = cached.load(std::memory_order_acquire);
    if (VALUE >= 0) {
        return VALUE != 0;
    }

    bool const ENABLED = !cmdline_has_token(ker::init::get_kernel_cmdline(), "net.no_napi_rescue");
    cached.store(ENABLED ? 1 : 0, std::memory_order_release);
    return ENABLED;
}

auto napi_state_name(uint8_t state) -> const char* {
    switch (static_cast<NapiState>(state)) {
        case NapiState::IDLE:
            return "IDLE";
        case NapiState::SCHEDULED:
            return "SCHEDULED";
        case NapiState::POLLING:
            return "POLLING";
        case NapiState::DISABLED:
            return "DISABLED";
    }
    return "?";
}

auto tcp_state_name(uint8_t state) -> const char* {
    switch (static_cast<proto::TcpState>(state)) {
        case proto::TcpState::CLOSED:
            return "CLOSED";
        case proto::TcpState::LISTEN:
            return "LISTEN";
        case proto::TcpState::SYN_SENT:
            return "SYN_SENT";
        case proto::TcpState::SYN_RECEIVED:
            return "SYN_RECEIVED";
        case proto::TcpState::ESTABLISHED:
            return "ESTABLISHED";
        case proto::TcpState::FIN_WAIT_1:
            return "FIN_WAIT_1";
        case proto::TcpState::FIN_WAIT_2:
            return "FIN_WAIT_2";
        case proto::TcpState::CLOSE_WAIT:
            return "CLOSE_WAIT";
        case proto::TcpState::CLOSING:
            return "CLOSING";
        case proto::TcpState::LAST_ACK:
            return "LAST_ACK";
        case proto::TcpState::TIME_WAIT:
            return "TIME_WAIT";
    }
    return "?";
}

auto task_type_name(ker::mod::sched::task::TaskType type) -> const char* {
    switch (type) {
        case ker::mod::sched::task::TaskType::DAEMON:
            return "daemon";
        case ker::mod::sched::task::TaskType::PROCESS:
            return "process";
        case ker::mod::sched::task::TaskType::IDLE:
            return "idle";
    }
    return "?";
}

auto task_state_name(ker::mod::sched::task::TaskState state) -> const char* {
    switch (state) {
        case ker::mod::sched::task::TaskState::ACTIVE:
            return "active";
        case ker::mod::sched::task::TaskState::EXITING:
            return "exiting";
        case ker::mod::sched::task::TaskState::DEAD:
            return "dead";
    }
    return "?";
}

auto task_queue_name(enum ker::mod::sched::task::Task::sched_queue queue) -> const char* {
    switch (queue) {
        case ker::mod::sched::task::Task::sched_queue::NONE:
            return "none";
        case ker::mod::sched::task::Task::sched_queue::RUNNABLE:
            return "runnable";
        case ker::mod::sched::task::Task::sched_queue::WAITING:
            return "waiting";
        case ker::mod::sched::task::Task::sched_queue::DEAD_GC:
            return "dead_gc";
    }
    return "?";
}

void log_tcp_owner_scheduler_state(uint64_t cpu_no) {
    auto const CPU_STATE = ker::mod::sched::get_scheduler_cpu_state(cpu_no);
    log::warn(
        "net-watchdog sched_owner_cpu cpu=%llu idle=%u runq=%llu waitq=%llu cur=%llu(%s) type=%u vblk=%u wblk=%u preempt=%u/%u "
        "preempt_owner=0x%llx wait=%s kind=%u callsite=0x%llx saved_rip=0x%llx syscall_timer=%llu/%llu handoff=%llu(%s) "
        "htype=%u hvblk=%u hwblk=%u hpreempt=%u/%u hpreempt_owner=0x%llx hwait=%s hkind=%u hcallsite=0x%llx hsaved_rip=0x%llx "
        "pending=%u timer=%llu/%llu/%llu wake=%llu/%llu local=%llu/%llu last_tick=%llu wait_deadline=%llu",
        static_cast<unsigned long long>(CPU_STATE.cpu_no), CPU_STATE.is_idle ? 1U : 0U,
        static_cast<unsigned long long>(CPU_STATE.runnable_count), static_cast<unsigned long long>(CPU_STATE.wait_queue_count),
        static_cast<unsigned long long>(CPU_STATE.current_pid), CPU_STATE.current_name, static_cast<unsigned>(CPU_STATE.current_type),
        CPU_STATE.current_voluntary_block ? 1U : 0U, CPU_STATE.current_wants_block ? 1U : 0U, CPU_STATE.current_preempt_depth,
        CPU_STATE.current_preempt_pending ? 1U : 0U, static_cast<unsigned long long>(CPU_STATE.current_preempt_owner),
        CPU_STATE.current_wait_channel, static_cast<unsigned>(CPU_STATE.current_wait_kind),
        static_cast<unsigned long long>(CPU_STATE.current_perf_wait_callsite), static_cast<unsigned long long>(CPU_STATE.current_saved_rip),
        static_cast<unsigned long long>(CPU_STATE.scheduler_timer_interrupts),
        static_cast<unsigned long long>(CPU_STATE.scheduler_timer_arms), static_cast<unsigned long long>(CPU_STATE.handoff_pid),
        CPU_STATE.handoff_name, static_cast<unsigned>(CPU_STATE.handoff_type), CPU_STATE.handoff_voluntary_block ? 1U : 0U,
        CPU_STATE.handoff_wants_block ? 1U : 0U, CPU_STATE.handoff_preempt_depth, CPU_STATE.handoff_preempt_pending ? 1U : 0U,
        static_cast<unsigned long long>(CPU_STATE.handoff_preempt_owner), CPU_STATE.handoff_wait_channel,
        static_cast<unsigned>(CPU_STATE.handoff_wait_kind), static_cast<unsigned long long>(CPU_STATE.handoff_perf_wait_callsite),
        static_cast<unsigned long long>(CPU_STATE.handoff_saved_rip), CPU_STATE.resched_timer_pending ? 1U : 0U,
        static_cast<unsigned long long>(CPU_STATE.scheduler_timer_interrupts),
        static_cast<unsigned long long>(CPU_STATE.scheduler_timer_arms), static_cast<unsigned long long>(CPU_STATE.scheduler_timer_disarms),
        static_cast<unsigned long long>(CPU_STATE.wake_ipis_sent), static_cast<unsigned long long>(CPU_STATE.wake_ipis_coalesced),
        static_cast<unsigned long long>(CPU_STATE.local_reschedule_requests),
        static_cast<unsigned long long>(CPU_STATE.local_reschedule_timer_pokes), static_cast<unsigned long long>(CPU_STATE.last_tick_us),
        static_cast<unsigned long long>(CPU_STATE.next_wait_deadline_us));

    std::array<ker::mod::sched::SchedulerRunQueueTaskState, NET_WATCHDOG_RUNQ_TASKS> rows{};
    size_t const COUNT = ker::mod::sched::get_scheduler_runqueue_task_states(cpu_no, rows.data(), rows.size());
    for (size_t i = 0; i < COUNT; ++i) {
        auto const& row = rows.at(i);
        log::warn(
            "net-watchdog runq_task cpu=%llu slot=%zu pid=%llu name=%s type=%s state=%s queue=%s cur=%u handoff=%u heap=%u "
            "heap_slot=%u heap_index=%d owner_cpu=%llu vblk=%u wblk=%u pinned=%u preempt=%u/%u preempt_owner=0x%llx "
            "preempt_start=%llu preempt_max=%llu wait=%s kind=%u callsite=0x%llx saved_rip=0x%llx wake_at=%llu syscall_start=%llu "
            "vruntime=%llu vdeadline=%llu slice=%u/%u yield=%u deferred=%u",
            static_cast<unsigned long long>(cpu_no), i, static_cast<unsigned long long>(row.pid), row.name,
            task_type_name(static_cast<ker::mod::sched::task::TaskType>(row.type)),
            task_state_name(static_cast<ker::mod::sched::task::TaskState>(row.state)),
            task_queue_name(static_cast<enum ker::mod::sched::task::Task::sched_queue>(row.sched_queue)), row.current ? 1U : 0U,
            row.handoff ? 1U : 0U, row.in_heap ? 1U : 0U, row.heap_slot, row.heap_index, static_cast<unsigned long long>(row.owner_cpu),
            row.voluntary_block ? 1U : 0U, row.wants_block ? 1U : 0U, row.cpu_pinned ? 1U : 0U, row.preempt_depth,
            row.preempt_pending ? 1U : 0U, static_cast<unsigned long long>(row.preempt_owner),
            static_cast<unsigned long long>(row.preempt_start_us), static_cast<unsigned long long>(row.preempt_max_us), row.wait_channel,
            static_cast<unsigned>(row.wait_kind), static_cast<unsigned long long>(row.perf_wait_callsite),
            static_cast<unsigned long long>(row.saved_rip), static_cast<unsigned long long>(row.wake_at_us),
            static_cast<unsigned long long>(row.syscall_account_start_us), static_cast<unsigned long long>(row.vruntime),
            static_cast<unsigned long long>(row.vdeadline), row.slice_used_ns, row.slice_ns, row.yield_switch ? 1U : 0U,
            row.deferred_task_switch ? 1U : 0U);
    }
    if (COUNT == rows.size()) {
        log::warn("net-watchdog runq_task_truncated cpu=%llu max=%zu", static_cast<unsigned long long>(cpu_no), rows.size());
    }
}

void log_tcp_owner_task(uint64_t pid, const char* label, uint16_t local_port, uint16_t remote_port) {
    if (pid == 0) {
        return;
    }

    auto* task = ker::mod::sched::find_task_by_pid_safe(pid);
    if (task == nullptr) {
        log::warn("net-watchdog tcp_owner label=%s pid=%llu local_port=%u remote_port=%u task=<missing>", label,
                  static_cast<unsigned long long>(pid), static_cast<unsigned>(local_port), static_cast<unsigned>(remote_port));
        return;
    }

    auto const STATE = task->state.load(std::memory_order_acquire);
    uint64_t const CPU_NO = task->cpu;
    log::warn(
        "net-watchdog tcp_owner label=%s pid=%llu name=%s type=%s state=%s queue=%s cpu=%llu owner_pid=%llu parent=%llu pgid=%llu "
        "vblk=%u wblk=%u pinned=%u preempt=%u/%u preempt_owner=0x%llx preempt_start=%llu preempt_max=%llu wait=%s kind=%u "
        "callsite=0x%llx saved_rip=0x%llx wake_at=%llu queue_id=%u yield=%u deferred=%u syscall_start=%llu slice=%u/%u ref=%u "
        "local_port=%u remote_port=%u",
        label, static_cast<unsigned long long>(pid), task->name != nullptr ? task->name : "?", task_type_name(task->type),
        task_state_name(STATE), task_queue_name(task->sched_queue), static_cast<unsigned long long>(CPU_NO),
        static_cast<unsigned long long>(task->owner_pid), static_cast<unsigned long long>(task->parent_pid),
        static_cast<unsigned long long>(task->pgid), task->is_voluntary_blocked() ? 1U : 0U, task->wants_block ? 1U : 0U,
        task->cpu_pinned ? 1U : 0U, task->preempt_disable_depth, task->preempt_pending ? 1U : 0U,
        static_cast<unsigned long long>(task->preempt_disable_owner), static_cast<unsigned long long>(task->preempt_disable_start_us),
        static_cast<unsigned long long>(task->preempt_disable_max_us), task->wait_channel != nullptr ? task->wait_channel : "-",
        static_cast<unsigned>(task->wait_channel_kind), static_cast<unsigned long long>(task->perf_wait_callsite),
        static_cast<unsigned long long>(task->context.frame.rip), static_cast<unsigned long long>(task->wake_at_us),
        static_cast<unsigned>(task->sched_queue), task->yield_switch ? 1U : 0U, task->deferred_task_switch ? 1U : 0U,
        static_cast<unsigned long long>(task->syscall_account_start_us), task->slice_used_ns, task->slice_ns,
        task->ref_count.load(std::memory_order_acquire), static_cast<unsigned>(local_port), static_cast<unsigned>(remote_port));
    log_tcp_owner_scheduler_state(CPU_NO);
    task->release();
}

void log_net_watchdog_diag() {
    PacketPoolSnapshot pool{};
    bool const POOL_LOCKED = pkt_pool_try_snapshot(pool);
    log::warn(
        "net-watchdog pool lock=%s capacity=%zu baseline=%zu active=%zu free=%zu used=%zu rx_reserve=%zu grow_chunk=%zu "
        "tx_refused=%u grow=%u",
        POOL_LOCKED ? "ok" : "busy", pool.capacity, pool.baseline_capacity, pool.active_capacity, pool.free, pool.used, pool.rx_reserve,
        pool.grow_chunk, pool.tx_refused, pool.expand_in_progress ? 1U : 0U);

    BacklogSnapshot backlog{};
    backlog_get_snapshot(backlog);
    log::warn("net-watchdog backlog ready=%u cpus=%llu queues=%zu queued=%llu", backlog.ready ? 1U : 0U,
              static_cast<unsigned long long>(backlog.num_cpus), backlog.queue_count,
              static_cast<unsigned long long>(backlog.total_queued));
    for (size_t i = 0; i < backlog.queue_count; ++i) {
        auto const& row = backlog.queues.at(i);
        log::warn(
            "net-watchdog backlog_cpu cpu=%llu queued=%llu handler_pid=%llu handler_cpu=%llu active=%u consumer=%u "
            "owner_pid=%llu owner_cpu=%llu owner_site=0x%llx hold_us=%llu stage=%u batch=%llu normal=%llu wki=%llu",
            static_cast<unsigned long long>(row.cpu), static_cast<unsigned long long>(row.queued),
            static_cast<unsigned long long>(row.handler_pid), static_cast<unsigned long long>(row.handler_cpu),
            row.handler_active ? 1U : 0U, row.consumer_active ? 1U : 0U, static_cast<unsigned long long>(row.consumer_owner_pid),
            static_cast<unsigned long long>(row.consumer_owner_cpu), static_cast<unsigned long long>(row.consumer_owner_site),
            static_cast<unsigned long long>(row.consumer_hold_us), static_cast<unsigned>(row.consumer_stage),
            static_cast<unsigned long long>(row.consumer_batch), static_cast<unsigned long long>(row.consumer_normal),
            static_cast<unsigned long long>(row.consumer_wki));
    }

    std::array<NetDeviceSnapshot, MAX_NET_DEVICES> netdev_rows{};
    size_t const NETDEV_COUNT = netdev_snapshot(netdev_rows.data(), netdev_rows.size());
    for (size_t i = 0; i < NETDEV_COUNT; ++i) {
        auto const& row = netdev_rows.at(i);
        log::warn(
            "net-watchdog netdev name=%.*s ifindex=%u state=%u wki_transport=%u rx_forward=%u rx_packets=%llu rx_bytes=%llu "
            "rx_dropped=%llu tx_packets=%llu tx_bytes=%llu tx_dropped=%llu",
            static_cast<int>(NETDEV_NAME_LEN), row.name.data(), row.ifindex, static_cast<unsigned>(row.state), row.wki_transport ? 1U : 0U,
            row.wki_rx_forward ? 1U : 0U, static_cast<unsigned long long>(row.rx_packets), static_cast<unsigned long long>(row.rx_bytes),
            static_cast<unsigned long long>(row.rx_dropped), static_cast<unsigned long long>(row.tx_packets),
            static_cast<unsigned long long>(row.tx_bytes), static_cast<unsigned long long>(row.tx_dropped));
    }
    if (NETDEV_COUNT == netdev_rows.size()) {
        log::warn("net-watchdog netdev_truncated max=%zu", netdev_rows.size());
    }

    std::array<ker::dev::virtio::VirtIONetDiagSnapshot, ker::dev::virtio::VIRTIO_NET_DIAG_MAX_ROWS> virtio_rows{};
    size_t const VIRTIO_COUNT = ker::dev::virtio::virtio_net_diag_snapshot(virtio_rows.data(), virtio_rows.size());
    for (size_t i = 0; i < VIRTIO_COUNT; ++i) {
        auto const& row = virtio_rows.at(i);
        log::warn(
            "net-watchdog virtio name=%.*s ifindex=%u pair=%u active=%u napi=%s work=%u worker_pid=%llu worker_cpu=%llu "
            "polls=%llu completes=%llu rx_free=%u rx_pending=%u rx_avail=%u rx_used=%u rx_last=%u rx_mapped=%u tx_free=%u "
            "tx_pending=%u tx_avail=%u tx_used=%u tx_last=%u tx_mapped=%u",
            static_cast<int>(NETDEV_NAME_LEN), row.name.data(), row.ifindex, static_cast<unsigned>(row.pair), row.active ? 1U : 0U,
            napi_state_name(row.napi_state), row.napi_has_work ? 1U : 0U, static_cast<unsigned long long>(row.napi_worker_pid),
            static_cast<unsigned long long>(row.napi_worker_cpu), static_cast<unsigned long long>(row.napi_polls),
            static_cast<unsigned long long>(row.napi_completes), row.rx.num_free, row.rx.pending, row.rx.avail_idx, row.rx.used_idx,
            row.rx.last_used_idx, row.rx.mapped, row.tx.num_free, row.tx.pending, row.tx.avail_idx, row.tx.used_idx, row.tx.last_used_idx,
            row.tx.mapped);
    }
    if (VIRTIO_COUNT == virtio_rows.size()) {
        log::warn("net-watchdog virtio_truncated max=%zu", virtio_rows.size());
    }

    std::array<proto::TcpListenerSnapshot, NET_WATCHDOG_TCP_LISTENERS> listeners{};
    size_t const LISTENER_COUNT = proto::tcp_listener_snapshot(listeners.data(), listeners.size());
    for (size_t i = 0; i < LISTENER_COUNT; ++i) {
        auto const& row = listeners.at(i);
        log::warn(
            "net-watchdog tcp_listener local=0x%08x:%u state=%s owner_pid=%llu accept_queue=%zu backlog=%d rcvbuf=%zu/%zu "
            "rcv_wnd=%u ref=%u",
            row.local_ip, row.local_port, tcp_state_name(row.state), static_cast<unsigned long long>(row.owner_pid), row.accept_queue,
            row.backlog, row.rcvbuf_used, row.rcvbuf_capacity, row.rcv_wnd, row.refcount);
    }
    if (LISTENER_COUNT == listeners.size()) {
        log::warn("net-watchdog tcp_listener_truncated max=%zu", listeners.size());
    }

    std::array<proto::TcpConnSnapshot, NET_WATCHDOG_TCP_CONNS> conns{};
    size_t const CONN_COUNT = proto::tcp_conn_snapshot(conns.data(), conns.size());
    for (size_t i = 0; i < CONN_COUNT; ++i) {
        auto const& row = conns.at(i);
        log::warn(
            "net-watchdog tcp_conn local=0x%08x:%u remote=0x%08x:%u state=%s owner_pid=%llu rcvbuf=%zu/%zu rcv_nxt=%u "
            "rcv_wnd=%u snd_una=%u snd_nxt=%u snd_wnd=%u ooo=%zu ref=%u sack=%u",
            row.local_ip, row.local_port, row.remote_ip, row.remote_port, tcp_state_name(row.state),
            static_cast<unsigned long long>(row.owner_pid), row.rcvbuf_used, row.rcvbuf_capacity, row.rcv_nxt, row.rcv_wnd, row.snd_una,
            row.snd_nxt, row.snd_wnd, row.ooo_bytes, row.refcount, row.sack_permitted ? 1U : 0U);
        if (row.rcvbuf_used != 0 || row.local_port == 22 || row.remote_port == 22) {
            log_tcp_owner_task(row.owner_pid, "conn", row.local_port, row.remote_port);
        }
    }
    if (CONN_COUNT == conns.size()) {
        log::warn("net-watchdog tcp_conn_truncated max=%zu", conns.size());
    }
}

// Find NapiStruct by matching worker task pointer
auto find_napi_for_current_task() -> NapiStruct* {
    auto* current = ker::mod::sched::get_current_task();
    for (auto* napi : g_napi_registry) {
        if (napi != nullptr && napi->worker == current) {
            return napi;
        }
    }
    return nullptr;
}

// Register a NapiStruct in the global registry
auto register_napi(NapiStruct* napi) -> bool {
    g_registry_lock.lock();
    bool const INSERTED = g_napi_registry.push_back(napi);
    g_registry_lock.unlock();
    if (!INSERTED) {
        log::critical("failed to register NAPI context for %s", napi->dev != nullptr ? napi->dev->name.data() : "?");
        return false;
    }
    ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::NAPI_REG, 0, ker::mod::perf::PERF_FLAG_CT_INSERT,
                                          static_cast<int64_t>(g_napi_registry.size()), 0, 0);
    return true;
}

// Unregister a NapiStruct from the global registry
void unregister_napi(NapiStruct* napi) {
    g_registry_lock.lock();
    for (size_t i = 0; i < g_napi_registry.size(); ++i) {
        if (g_napi_registry.at(i) == napi) {
            g_napi_registry.remove_at(i);
            g_registry_lock.unlock();
            ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::NAPI_REG, 0, ker::mod::perf::PERF_FLAG_CT_REMOVE,
                                                  static_cast<int64_t>(g_napi_registry.size()), 0, 0);
            return;
        }
    }
    g_registry_lock.unlock();
}

void poke_napi_worker_cpu(ker::mod::sched::task::Task* worker) {
    if (worker == nullptr) {
        return;
    }

    uint64_t const WORKER_CPU = worker->cpu;
    if (WORKER_CPU == ker::mod::cpu::current_cpu()) {
        ker::mod::sys::context_switch::request_reschedule();
        return;
    }

    ker::mod::sched::wake_cpu(WORKER_CPU, ker::mod::sched::WakeCpuMode::FORCE);
}

void wake_napi_worker(NapiStruct* napi) {
    if (napi == nullptr || napi->worker == nullptr) {
        return;
    }

    auto* worker = napi->worker;
    ker::mod::sched::kern_wake(worker);
    poke_napi_worker_cpu(worker);
}

auto napi_worker_has_work(NapiStruct* napi) -> bool {
    if (napi == nullptr) {
        return false;
    }

    if (napi->has_work.load(std::memory_order_acquire)) {
        return true;
    }

    if (napi->state.load(std::memory_order_acquire) != NapiState::SCHEDULED) {
        return false;
    }

    napi->has_work.store(true, std::memory_order_release);
    return true;
}

auto napi_rescue_poll_once() -> int {
    int total = 0;
    for (int pass = 0; pass < NAPI_RESCUE_MAX_PASSES; ++pass) {
        int const POLLED = napi_poll_all_pending();
        total += POLLED;
        if (POLLED == 0) {
            break;
        }
        ker::mod::sched::kern_yield();
    }
    if (backlog_rescue_needed(BACKLOG_RESCUE_MIN_QUEUE_DEPTH)) {
        total += backlog_drain_all_pending_inline();
    }
    return total;
}

[[noreturn]] void napi_rescue_loop() {
    for (;;) {
        int const WORK = napi_rescue_poll_once();
        ker::mod::sched::kern_sleep_us(WORK == 0 ? NAPI_RESCUE_IDLE_SLEEP_US : NAPI_RESCUE_ACTIVE_SLEEP_US);
    }
}

[[noreturn]] void net_watchdog_loop() {
    for (;;) {
        ker::mod::sched::kern_sleep_us(NET_WATCHDOG_INTERVAL_US);
        log_net_watchdog_diag();
    }
}

void start_net_watchdog_thread() {
    if (!net_watchdog_enabled_impl()) {
        return;
    }

    bool expected = false;
    if (!g_watchdog_started.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
    }

    g_watchdog_task = ker::mod::sched::task::Task::create_kernel_thread("net_watchdog", net_watchdog_loop);
    if (g_watchdog_task == nullptr) {
        g_watchdog_started.store(false, std::memory_order_release);
        log::warn("failed to create net watchdog thread");
        return;
    }
    promote_latency_sensitive_daemon(g_watchdog_task);
    ker::mod::sched::post_task_balanced(g_watchdog_task);
    log::debug("net watchdog thread started");
}

void start_napi_rescue_thread() {
    if (!napi_rescue_enabled()) {
        return;
    }

    bool expected = false;
    if (!g_rescue_started.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
    }

    g_rescue_task = ker::mod::sched::task::Task::create_kernel_thread("netpoll_rescue", napi_rescue_loop);
    if (g_rescue_task == nullptr) {
        g_rescue_started.store(false, std::memory_order_release);
        log::warn("failed to create NAPI rescue thread");
        return;
    }
    promote_latency_sensitive_daemon(g_rescue_task);
    ker::mod::sched::post_task_balanced(g_rescue_task);
    log::debug("NAPI rescue thread started");
}

// Worker thread main loop
[[noreturn]] void napi_worker_loop(NapiStruct* napi) {
    for (;;) {
        // Disable interrupts before checking work flag.
        asm volatile("cli" ::: "memory");

        if (!napi_worker_has_work(napi)) {
            // No work: block until napi_schedule() explicitly wakes us.
            //
            // The CLI above only protects the work check.  Entering kern_block()
            // with IF clear resumes through the scheduler's sti/hlt/cli path,
            // which is a fragile same-CPL timer-return point for high-rate
            // netpoll workers.  Re-enable first; a racing IRQ wake is preserved
            // by the task's wakeup_pending check inside kern_block().
            asm volatile("sti" ::: "memory");
            ker::mod::sched::kern_sleep_us(NAPI_WORKER_IDLE_SLEEP_US);
            continue;
        }

        // Work available: re-enable interrupts before polling.
        asm volatile("sti" ::: "memory");

        // SCHEDULED -> POLLING. Inline polling can drain the device before the
        // worker observes has_work; if state is already IDLE, clear that stale
        // flag instead of polling an idle queue from the worker.
        NapiState expected = NapiState::SCHEDULED;
        bool entered_polling =
            napi->state.compare_exchange_strong(expected, NapiState::POLLING, std::memory_order_acq_rel, std::memory_order_acquire);
        if (!entered_polling) {
            if (expected == NapiState::DISABLED) {
                // Device down: halt.
                for (;;) {
                    asm volatile("hlt");
                }
            }
            if (expected == NapiState::IDLE) {
                napi->has_work.store(false, std::memory_order_release);
                if (napi->state.load(std::memory_order_acquire) == NapiState::SCHEDULED) {
                    napi->has_work.store(true, std::memory_order_release);
                }
                continue;
            }

            ker::mod::sched::kern_sleep_us(NAPI_WORKER_CONTENTION_SLEEP_US);
            continue;
        }

        // Poll while work remains.
        int full_budget_polls = 0;
        for (;;) {
            if (!poll_fn_is_valid(napi->poll)) {
                log::critical("invalid NAPI poll function for %s: poll=0x%lx napi=%p", napi->dev != nullptr ? napi->dev->name.data() : "?",
                              reinterpret_cast<uintptr_t>(napi->poll), napi);
                napi->state.store(NapiState::DISABLED, std::memory_order_release);
                napi->has_work.store(false, std::memory_order_release);
                for (;;) {
                    asm volatile("hlt");
                }
            }
            int const PROCESSED = napi->poll(napi, napi->weight);
            napi->poll_count++;

            if (PROCESSED < napi->weight) {
                // Driver called napi_complete().
                break;
            }

            if (++full_budget_polls >= NAPI_WORKER_MAX_FULL_BUDGET_POLLS) {
                full_budget_polls = 0;
                ker::mod::sched::kern_yield();
            }
        }

        // Clear work flag.
        napi->has_work.store(false, std::memory_order_release);

        // If IRQ arrived during poll, re-arm work flag.
        if (napi->state.load(std::memory_order_acquire) == NapiState::SCHEDULED) {
            napi->has_work.store(true, std::memory_order_release);
        }
    }
}

// Entry point for worker threads (finds its own NapiStruct)
[[noreturn]] void napi_worker_entry() {
    // Find our NapiStruct by matching worker task pointer
    NapiStruct* my_napi = find_napi_for_current_task();

    if (my_napi == nullptr) {
        log::critical("worker thread could not find its NapiStruct");
        for (;;) {
            asm volatile("hlt");
        }
    }

    log::debug("worker for %s started", my_napi->dev->name.data());

    // Run the worker loop
    napi_worker_loop(my_napi);
}

}  // namespace

void napi_init(NapiStruct* napi, NetDevice* dev, NapiPollFn poll, int weight) {
    napi->dev = dev;
    napi->poll = poll;
    napi->state.store(NapiState::DISABLED, std::memory_order_release);
    napi->weight = (weight > 0) ? weight : NAPI_DEFAULT_WEIGHT;
    napi->worker = nullptr;
    napi->has_work.store(false, std::memory_order_release);
    napi->poll_count = 0;
    napi->complete_count = 0;
}

void napi_enable(NapiStruct* napi, uint64_t cpu_affinity) {
    // Register in global registry first (so worker can find it)
    if (!register_napi(napi)) {
        return;
    }

    // Build thread name: "netpoll_eth0"
    std::array<char, 32> name{};
    const char* dev_name = napi->dev->name.data();
    std::string_view const DEV_NAME = dev_name;
    std::string_view const PREFIX = "netpoll_";
    size_t name_len = PREFIX.size();
    std::ranges::copy(PREFIX, name.begin());
    size_t const COPY_LEN = std::min(DEV_NAME.size(), name.size() - name_len - 1);
    std::ranges::copy(DEV_NAME.substr(0, COPY_LEN), std::next(name.begin(), static_cast<ptrdiff_t>(name_len)));
    name_len += COPY_LEN;
    name.at(name_len) = '\0';

    // Create dedicated worker thread
    napi->worker = ker::mod::sched::task::Task::create_kernel_thread(name.data(), napi_worker_entry);
    if (napi->worker == nullptr) {
        log::warn("failed to create worker thread for %s", dev_name);
        unregister_napi(napi);
        return;
    }

    promote_latency_sensitive_daemon(napi->worker);

    // Schedule the worker thread on the same CPU as its IRQ vector.  Keeping
    // this pinned preserves NAPI/IRQ locality; otherwise event wakes can drift
    // the worker away from the CPU programmed into MSI-X.
    if (cpu_affinity != UINT64_MAX) {
        ker::mod::sched::post_task_pinned_cpu(cpu_affinity, napi->worker);
    } else {
        ker::mod::sched::post_task_balanced(napi->worker);
    }

    // Transition to IDLE state (ready to receive IRQs)
    napi->state.store(NapiState::IDLE, std::memory_order_release);

    start_napi_rescue_thread();
    start_net_watchdog_thread();

    log::debug("enabled for %s (worker PID %lx)", dev_name, napi->worker->pid);
}

auto napi_set_worker_cpu(NapiStruct* napi, uint64_t cpu) -> bool {
    if (napi == nullptr || napi->worker == nullptr) {
        return false;
    }
    auto* worker = napi->worker;
    return ker::mod::sched::pin_task_to_cpu(worker, cpu);
}

void napi_disable(NapiStruct* napi) {
    // Signal worker to stop
    NapiState current = napi->state.load(std::memory_order_acquire);
    while (current != NapiState::DISABLED) {
        if (current == NapiState::POLLING) {
            // Wait for poll to complete
            asm volatile("pause");
            current = napi->state.load(std::memory_order_acquire);
            continue;
        }

        // Try to transition to DISABLED
        if (napi->state.compare_exchange_weak(current, NapiState::DISABLED, std::memory_order_acq_rel, std::memory_order_acquire)) {
            break;
        }
        current = napi->state.load(std::memory_order_acquire);
    }

    // Unregister from global registry
    unregister_napi(napi);

    log::debug("disabled for %s", napi->dev->name.data());
}

auto napi_schedule(NapiStruct* napi) -> bool {
    // Try to transition from IDLE to SCHEDULED (atomic, no lock)
    NapiState expected = NapiState::IDLE;
    if (!napi->state.compare_exchange_strong(expected, NapiState::SCHEDULED, std::memory_order_acq_rel, std::memory_order_acquire)) {
        if (expected == NapiState::SCHEDULED) {
            napi->has_work.store(true, std::memory_order_release);
            if (napi->worker != nullptr) {
                wake_napi_worker(napi);
            }
        }
        return false;
    }

    // Set work flag for interface worker thread
    napi->has_work.store(true, std::memory_order_release);

    // Wake the worker thread.
    if (napi->worker != nullptr) {
        wake_napi_worker(napi);
    }

    return true;
}

void napi_complete(NapiStruct* napi) {
    napi->complete_count++;

    // Transition from POLLING back to IDLE
    NapiState expected = NapiState::POLLING;
    napi->state.compare_exchange_strong(expected, NapiState::IDLE, std::memory_order_acq_rel, std::memory_order_acquire);

    // Driver should re-enable device interrupts after calling this
}

namespace {

auto napi_poll_struct_inline_budget(NapiStruct* napi, int budget) -> int {
    if (napi == nullptr) {
        return 0;
    }

    // Try to enter POLLING from IDLE or SCHEDULED.
    // If the worker is already POLLING, don't interfere.
    NapiState expected = NapiState::IDLE;
    if (!napi->state.compare_exchange_strong(expected, NapiState::POLLING, std::memory_order_acq_rel, std::memory_order_acquire)) {
        expected = NapiState::SCHEDULED;
        if (!napi->state.compare_exchange_strong(expected, NapiState::POLLING, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return 0;  // already polling or disabled
        }
    }

    // Poll once with normal budget.
    // NOTE: The driver's poll function (e.g. virtio_net_poll) calls
    // napi_complete() + re-enables IRQs internally when processed < budget.
    // We must NOT call napi_complete() again in that case, or we corrupt
    // the NAPI state machine - an IRQ arriving between the driver's
    // napi_complete() and ours could schedule the worker (IDLE->SCHEDULED->
    // POLLING), and our stale napi_complete() would yank it back to IDLE,
    // losing packets.
    int const EFFECTIVE_BUDGET = budget > 0 ? budget : napi->weight;
    if (!poll_fn_is_valid(napi->poll)) {
        log::critical("invalid inline NAPI poll function for %s: poll=0x%lx napi=%p", napi->dev != nullptr ? napi->dev->name.data() : "?",
                      reinterpret_cast<uintptr_t>(napi->poll), napi);
        napi->state.store(NapiState::DISABLED, std::memory_order_release);
        napi->has_work.store(false, std::memory_order_release);
        return 0;
    }
    int const PROCESSED = napi->poll(napi, EFFECTIVE_BUDGET);
    napi->poll_count++;

    if (PROCESSED >= EFFECTIVE_BUDGET) {
        // Driver did NOT call napi_complete() (more work pending).
        // Transition back to SCHEDULED so the worker can pick up the
        // remaining work and re-enable IRQs when it's done.
        NapiState exp = NapiState::POLLING;
        napi->state.compare_exchange_strong(exp, NapiState::SCHEDULED, std::memory_order_acq_rel, std::memory_order_acquire);
        napi->has_work.store(true, std::memory_order_release);
        if (napi->worker != nullptr) {
            wake_napi_worker(napi);
        }
    } else {
        // Driver already called napi_complete() and re-enabled IRQs.
        // Clear work flag so worker doesn't spin unnecessarily.
        napi->has_work.store(false, std::memory_order_release);

        // Re-check for race: if an IRQ arrived during our poll, re-arm
        if (napi->state.load(std::memory_order_acquire) == NapiState::SCHEDULED) {
            napi->has_work.store(true, std::memory_order_release);
        }
    }

    return PROCESSED;
}

}  // namespace

auto net_watchdog_enabled() -> bool { return net_watchdog_enabled_impl(); }

auto napi_poll_all_pending() -> int {
    int total = 0;
    // Snapshot count under lock to avoid tearing, then poll without the lock
    // (per-NAPI inline polling is safe to call without the registry lock).
    g_registry_lock.lock();
    size_t count = g_napi_registry.size();
    std::array<NapiStruct*, NAPI_POLL_SNAPSHOT_CAPACITY> snapshot{};
    count = std::min<size_t>(count, snapshot.size());
    for (size_t i = 0; i < count; ++i) {
        snapshot.at(i) = g_napi_registry.at(i);
    }
    g_registry_lock.unlock();

    for (size_t i = 0; i < count; ++i) {
        if (auto* napi = snapshot.at(i); napi != nullptr && napi->dev != nullptr) {
            total += napi_poll_struct_inline_budget(napi, NAPI_DEFAULT_WEIGHT);
        }
    }
    return total;
}

}  // namespace ker::net
