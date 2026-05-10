#include "remote_ipc.hpp"

#include <bits/ssize_t.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <deque>
#include <dev/pty.hpp>
#include <net/socket.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <syscalls_impl/futex/futex.hpp>
#include <utility>
#include <vfs/epoll.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/vfs.hpp>

#include "util/smallvec.hpp"

namespace ker::net::wki {

namespace detail {
static void handle_ipc_dev_op_req_inline(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
}

// =============================================================================
// File-scope globals (anonymous namespace)
// =============================================================================

namespace {

constexpr uint16_t WKI_IPC_FD_ACCESS_MASK = 0x0003;

bool g_ipc_initialized = false;
ker::mod::sys::Spinlock s_ipc_lock;

// Server-side (home node) exports
std::deque<WkiIpcExport*> g_ipc_exports;
uint32_t g_next_ipc_resource_id = 0x7000;

// Client-side (remote node) proxies — tracked for cleanup
std::deque<ProxyIpcState*> g_ipc_proxies;

struct PendingPipeChunk {
    uint8_t* data = nullptr;
    uint16_t len = 0;
    uint16_t offset = 0;
};

struct PendingPipeDelivery {
    uint16_t home_node = WKI_NODE_INVALID;
    uint32_t resource_id = 0;
    std::deque<PendingPipeChunk> chunks;
    uint32_t buffered_bytes = 0;
    bool write_closed = false;
};

std::deque<PendingPipeDelivery*> g_pending_pipe_deliveries;

struct ExportPipeWriteBacklog {
    WkiIpcExport* exp = nullptr;
    std::deque<PendingPipeChunk> chunks;
    uint32_t buffered_bytes = 0;
    bool close_pending = false;
    bool queued = false;
};

std::deque<ExportPipeWriteBacklog*> g_export_pipe_write_backlogs;
std::deque<ExportPipeWriteBacklog*> g_export_pipe_write_flush_queue;
ker::mod::sched::task::Task* g_export_pipe_write_flush_task = nullptr;

struct IpcDevOpWork {
    WkiHeader hdr = {};
    uint8_t* payload = nullptr;
    uint16_t payload_len = 0;
};

std::deque<IpcDevOpWork*> g_ipc_dev_op_queue;
ker::mod::sched::task::Task* g_ipc_dev_op_worker_task = nullptr;
constexpr size_t WKI_IPC_DEV_OP_MAX_PENDING = 256;

constexpr ssize_t WKI_IPC_PIPE_RESTARTSYS = -512;
constexpr uint64_t WKI_IPC_PIPE_WRITE_RETRY_US = 1000;
constexpr uint32_t WKI_IPC_PIPE_EOF_MAX_SEND_ATTEMPTS = 128;

static auto consume_deferred_wait_for_daemon() -> bool;
static auto register_poll_write_waiter(ker::vfs::File* file, bool* ready_now) -> bool;

auto should_proxy_tty_like_file(ker::vfs::File* file) -> bool {
    if (file == nullptr || file->fs_type != ker::vfs::FSType::DEVFS) {
        return false;
    }

    if (ker::dev::pty::pty_is_file(file)) {
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

auto proxy_register_waiter_locked(ker::util::SmallVec<uint64_t, 2>& waiters, uint64_t pid) -> bool {
    for (size_t i = 0; i < waiters.size(); i++) {
        if (waiters[i] == pid) {
            return true;
        }
    }
    return waiters.push_back(pid);
}

void proxy_collect_waiters_locked(ker::util::SmallVec<uint64_t, 2>& waiters, uint64_t* pending, size_t* pending_count) {
    *pending_count = std::min(waiters.size(), size_t{32});
    for (size_t i = 0; i < *pending_count; i++) {
        pending[i] = waiters[i];
    }
    waiters.clear();
}

void proxy_reschedule_waiters(const uint64_t* waiters, size_t waiter_count) {
    for (size_t i = 0; i < waiter_count; i++) {
        auto* waiter = ker::mod::sched::find_task_by_pid_safe(waiters[i]);
        if (waiter == nullptr) {
            continue;
        }

        waiter->deferred_task_switch = false;
        uint64_t target_cpu = waiter->cpu;
        if (waiter->sched_queue == ker::mod::sched::task::Task::sched_queue::WAITING || waiter->voluntary_block) {
            target_cpu = ker::mod::sched::get_least_loaded_cpu();
        }
        ker::mod::sched::reschedule_task_for_cpu(target_cpu, waiter);
        waiter->release();
    }
}

// ---- Proxy fops for pipe (message-based, local ring buffer) ----

void proxy_release(ProxyIpcState* proxy) {
    if (proxy == nullptr) {
        return;
    }
    if (proxy->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        delete[] proxy->ring_buf;

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

auto find_proxy_by_resource_id_locked(uint32_t resource_id) -> ProxyIpcState* {
    for (auto* proxy : g_ipc_proxies) {
        if (proxy != nullptr && proxy->active.load(std::memory_order_acquire) && proxy->resource_id == resource_id) {
            proxy->refcount.fetch_add(1, std::memory_order_acq_rel);
            return proxy;
        }
    }
    return nullptr;
}

auto ipc_acquire_export_file_locked(uint32_t resource_id) -> ker::vfs::File* {
    for (auto* exp : g_ipc_exports) {
        if (exp == nullptr || !exp->active || exp->resource_id != resource_id || exp->file == nullptr) {
            continue;
        }

        exp->file->refcount.fetch_add(1, std::memory_order_acq_rel);
        return exp->file;
    }
    return nullptr;
}

void ipc_release_file_ref(ker::vfs::File* file) {
    if (file == nullptr) {
        return;
    }
    if (file->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        if (file->fops != nullptr && file->fops->vfs_close != nullptr) {
            file->fops->vfs_close(file);
        }
        delete file;
    }
}

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

void free_pending_pipe_delivery(PendingPipeDelivery* pending) {
    if (pending == nullptr) {
        return;
    }
    for (auto& chunk : pending->chunks) {
        delete[] chunk.data;
    }
    delete pending;
}

auto find_export_pipe_write_backlog_locked(WkiIpcExport* exp) -> ExportPipeWriteBacklog* {
    for (auto* backlog : g_export_pipe_write_backlogs) {
        if (backlog != nullptr && backlog->exp == exp) {
            return backlog;
        }
    }
    return nullptr;
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

    for (auto it = g_export_pipe_write_flush_queue.begin(); it != g_export_pipe_write_flush_queue.end();) {
        if (*it == backlog) {
            it = g_export_pipe_write_flush_queue.erase(it);
            continue;
        }
        ++it;
    }

    backlog->queued = false;
}

void free_export_pipe_write_backlog(ExportPipeWriteBacklog* backlog) {
    if (backlog == nullptr) {
        return;
    }

    for (auto& chunk : backlog->chunks) {
        delete[] chunk.data;
    }
    delete backlog;
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
    g_export_pipe_write_backlogs.push_back(backlog);
    return backlog;
}

void queue_export_pipe_write_flush_locked(ExportPipeWriteBacklog* backlog) {
    if (backlog == nullptr || backlog->queued) {
        return;
    }

    backlog->queued = true;
    g_export_pipe_write_flush_queue.push_back(backlog);
}

auto queue_export_pipe_write_data(WkiIpcExport* exp, const uint8_t* data, uint16_t len) -> bool {
    if (exp == nullptr || data == nullptr || len == 0) {
        return true;
    }

    auto* copy = new (std::nothrow) uint8_t[len];
    if (copy == nullptr) {
        return false;
    }
    std::memcpy(copy, data, len);

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    auto* backlog = ensure_export_pipe_write_backlog_locked(exp);
    if (backlog == nullptr) {
        s_ipc_lock.unlock_irqrestore(IRQF);
        delete[] copy;
        return false;
    }

    backlog->chunks.push_back(PendingPipeChunk{.data = copy, .len = len});
    backlog->buffered_bytes += len;
    queue_export_pipe_write_flush_locked(backlog);
    auto* flush_task = g_export_pipe_write_flush_task;
    s_ipc_lock.unlock_irqrestore(IRQF);

    if (flush_task != nullptr) {
        ker::mod::sched::kern_wake(flush_task);
    }
    return true;
}

auto mark_export_pipe_write_closed(WkiIpcExport* exp) -> bool {
    if (exp == nullptr) {
        return false;
    }

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    auto* backlog = ensure_export_pipe_write_backlog_locked(exp);
    if (backlog == nullptr) {
        s_ipc_lock.unlock_irqrestore(IRQF);
        return false;
    }

    backlog->close_pending = true;
    queue_export_pipe_write_flush_locked(backlog);
    auto* flush_task = g_export_pipe_write_flush_task;
    s_ipc_lock.unlock_irqrestore(IRQF);

    if (flush_task != nullptr) {
        ker::mod::sched::kern_wake(flush_task);
    }
    return true;
}

[[noreturn]] void export_pipe_write_flush_thread_fn() {
    for (;;) {
        ExportPipeWriteBacklog* backlog = nullptr;
        uint64_t irqf = s_ipc_lock.lock_irqsave();
        if (!g_export_pipe_write_flush_queue.empty()) {
            backlog = g_export_pipe_write_flush_queue.front();
            g_export_pipe_write_flush_queue.pop_front();
            if (backlog != nullptr) {
                backlog->queued = false;
            }
        }
        s_ipc_lock.unlock_irqrestore(irqf);

        if (backlog == nullptr) {
            ker::mod::sched::kern_block();
            continue;
        }

        WkiIpcExport* exp = nullptr;
        ker::vfs::File* file = nullptr;
        uint32_t resource_id = 0;
        uint16_t remaining = 0;
        uint8_t const* data_ptr = nullptr;
        bool close_pending = false;

        irqf = s_ipc_lock.lock_irqsave();
        exp = backlog->exp;
        if (exp == nullptr || !exp->active || exp->file == nullptr) {
            erase_export_pipe_write_backlog_locked(backlog);
            s_ipc_lock.unlock_irqrestore(irqf);
            free_export_pipe_write_backlog(backlog);
            continue;
        }

        file = exp->file;
        resource_id = exp->resource_id;
        close_pending = backlog->close_pending;

        if (!backlog->chunks.empty()) {
            file->refcount.fetch_add(1, std::memory_order_acq_rel);
            auto& chunk = backlog->chunks.front();
            remaining = static_cast<uint16_t>(chunk.len - chunk.offset);
            data_ptr = chunk.data + chunk.offset;
        } else if (close_pending) {
            exp->file = nullptr;
            exp->active = false;
            erase_export_pipe_write_backlog_locked(backlog);
            s_ipc_lock.unlock_irqrestore(irqf);
            if (file->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                if (file->fops != nullptr && file->fops->vfs_close != nullptr) {
                    file->fops->vfs_close(file);
                }
                delete file;
            }
            free_export_pipe_write_backlog(backlog);
            continue;
        } else {
            erase_export_pipe_write_backlog_locked(backlog);
            s_ipc_lock.unlock_irqrestore(irqf);
            free_export_pipe_write_backlog(backlog);
            continue;
        }
        s_ipc_lock.unlock_irqrestore(irqf);

        ssize_t write_ret = -EIO;
        if (file->fops != nullptr && file->fops->vfs_write != nullptr) {
            write_ret = file->fops->vfs_write(file, data_ptr, remaining, static_cast<size_t>(file->pos));
        }

        irqf = s_ipc_lock.lock_irqsave();
        exp = backlog->exp;
        if (exp == nullptr || !exp->active || exp->file != file) {
            erase_export_pipe_write_backlog_locked(backlog);
            s_ipc_lock.unlock_irqrestore(irqf);
            ipc_release_file_ref(file);
            free_export_pipe_write_backlog(backlog);
            continue;
        }

        if (write_ret > 0) {
            auto& chunk = backlog->chunks.front();
            auto const ADVANCED = static_cast<uint16_t>(std::cmp_less(write_ret, remaining) ? write_ret : remaining);
            chunk.offset = static_cast<uint16_t>(chunk.offset + ADVANCED);
            backlog->buffered_bytes -= ADVANCED;
            if (chunk.offset == chunk.len) {
                delete[] chunk.data;
                backlog->chunks.pop_front();
            }

            if (!backlog->chunks.empty() || backlog->close_pending) {
                queue_export_pipe_write_flush_locked(backlog);
            } else {
                erase_export_pipe_write_backlog_locked(backlog);
            }
            s_ipc_lock.unlock_irqrestore(irqf);
            ipc_release_file_ref(file);
            if (backlog->chunks.empty() && !backlog->close_pending) {
                free_export_pipe_write_backlog(backlog);
            }
            continue;
        }

        if (write_ret == WKI_IPC_PIPE_RESTARTSYS || write_ret == -EAGAIN) {
            queue_export_pipe_write_flush_locked(backlog);
            s_ipc_lock.unlock_irqrestore(irqf);
            bool ready_now = false;
            bool const POLL_REGISTERED = write_ret == -EAGAIN && register_poll_write_waiter(file, &ready_now);
            ipc_release_file_ref(file);
            if (write_ret == WKI_IPC_PIPE_RESTARTSYS || POLL_REGISTERED) {
                bool const WAIT_REGISTERED = write_ret == WKI_IPC_PIPE_RESTARTSYS ? consume_deferred_wait_for_daemon() : POLL_REGISTERED;
                if (WAIT_REGISTERED && !ready_now) {
                    ker::mod::sched::kern_block();
                } else if (!WAIT_REGISTERED) {
                    ker::mod::sched::kern_sleep_us(WKI_IPC_PIPE_WRITE_RETRY_US);
                }
            } else {
                ker::mod::sched::kern_sleep_us(WKI_IPC_PIPE_WRITE_RETRY_US);
            }
            continue;
        }

        ker::mod::dbg::log("[WKI] IPC export pipe write failed: resource_id=%u ret=%ld remaining=%u", resource_id, write_ret, remaining);
        exp->file = nullptr;
        exp->active = false;
        erase_export_pipe_write_backlog_locked(backlog);
        s_ipc_lock.unlock_irqrestore(irqf);
        ipc_release_file_ref(file);
        if (file->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            if (file->fops != nullptr && file->fops->vfs_close != nullptr) {
                file->fops->vfs_close(file);
            }
            delete file;
        }
        free_export_pipe_write_backlog(backlog);
    }
}

auto queue_pending_pipe_data(uint16_t home_node, uint32_t resource_id, const uint8_t* data, uint16_t len) -> bool {
    if (data == nullptr || len == 0) {
        return true;
    }

    auto* copy = new (std::nothrow) uint8_t[len];
    if (copy == nullptr) {
        return false;
    }
    std::memcpy(copy, data, len);

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
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

    pending->chunks.push_back(PendingPipeChunk{.data = copy, .len = len});
    pending->buffered_bytes += len;
    s_ipc_lock.unlock_irqrestore(IRQF);
    return true;
}

auto mark_pending_pipe_closed(uint16_t home_node, uint32_t resource_id) -> bool {
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    auto* pending = find_pending_pipe_delivery_locked(home_node, resource_id);
    if (pending == nullptr) {
        pending = new PendingPipeDelivery();
        if (pending == nullptr) {
            s_ipc_lock.unlock_irqrestore(IRQF);
            return false;
        }
        pending->home_node = home_node;
        pending->resource_id = resource_id;
        g_pending_pipe_deliveries.push_back(pending);
    }
    pending->write_closed = true;
    s_ipc_lock.unlock_irqrestore(IRQF);
    return true;
}

auto drain_pending_pipe_data(uint16_t home_node, uint32_t resource_id, void* buf, size_t count) -> size_t {
    if (buf == nullptr || count == 0) {
        return 0;
    }

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    auto* pending = find_pending_pipe_delivery_locked(home_node, resource_id);
    if (pending == nullptr || pending->chunks.empty()) {
        s_ipc_lock.unlock_irqrestore(IRQF);
        return 0;
    }

    auto& chunk = pending->chunks.front();
    auto const REMAINING = static_cast<uint16_t>(chunk.len - chunk.offset);
    size_t const TO_COPY = count < REMAINING ? count : REMAINING;
    std::memcpy(buf, chunk.data + chunk.offset, TO_COPY);
    chunk.offset = static_cast<uint16_t>(chunk.offset + TO_COPY);
    pending->buffered_bytes -= static_cast<uint32_t>(TO_COPY);

    if (chunk.offset == chunk.len) {
        delete[] chunk.data;
        pending->chunks.pop_front();
    }

    if (pending->chunks.empty()) {
        erase_pending_pipe_delivery_locked(pending);
        s_ipc_lock.unlock_irqrestore(IRQF);
        free_pending_pipe_delivery(pending);
        return TO_COPY;
    }

    s_ipc_lock.unlock_irqrestore(IRQF);
    return TO_COPY;
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

    if (reader != nullptr) {
        reader->deferred_task_switch = false;
        reader->wait_channel = nullptr;
        ker::mod::sched::kern_wake(reader);
    }

    wki_ipc_proxy_wake_poll_waiters(proxy);
}

void wake_proxy_reader(ProxyIpcState* proxy) {
    if (proxy == nullptr) {
        return;
    }

    auto* reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
    if (reader != nullptr) {
        reader->deferred_task_switch = false;
        reader->wait_channel = nullptr;
        ker::mod::sched::kern_wake(reader);
    }
}

auto proxy_pipe_read(ker::vfs::File* f, void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
        return -EBADF;
    }
    if (proxy->ring_buf == nullptr) {
        return -EIO;
    }

    // Spin briefly then block
    for (;;) {
        // Spin briefly then block
        for (int spin = 0; spin < 2000; spin++) {
            uint32_t const HEAD = proxy->ring_head.load(std::memory_order_acquire);
            uint32_t const TAIL = proxy->ring_tail.load(std::memory_order_relaxed);
            uint32_t const AVAIL = HEAD - TAIL;

            if (AVAIL > 0) {
                uint32_t const TO_READ = count < AVAIL ? static_cast<uint32_t>(count) : AVAIL;
                uint32_t const CAP = proxy->ring_capacity;
                uint32_t const RING_TAIL = TAIL % CAP;
                auto* dst = static_cast<uint8_t*>(buf);

                uint32_t const FIRST = CAP - RING_TAIL;
                if (FIRST >= TO_READ) {
                    std::memcpy(dst, proxy->ring_buf + RING_TAIL, TO_READ);
                } else {
                    std::memcpy(dst, proxy->ring_buf + RING_TAIL, FIRST);
                    std::memcpy(dst + FIRST, proxy->ring_buf, TO_READ - FIRST);
                }
                proxy->ring_tail.store(TAIL + TO_READ, std::memory_order_release);
                return static_cast<ssize_t>(TO_READ);
            }

            size_t const QUEUED = drain_pending_pipe_data(proxy->home_node, proxy->resource_id, buf, count);
            if (QUEUED > 0) {
                return static_cast<ssize_t>(QUEUED);
            }

            if (proxy->write_closed.load(std::memory_order_acquire) != 0U) {
                return 0;
            }

            asm volatile("pause" ::: "memory");
        }

        auto* task = ker::mod::sched::get_current_task();
        if (task == nullptr) {
            return -ESRCH;
        }

        // Serialize block registration with incoming DATA/CLOSE and pending
        // overflow queue updates so we don't sleep after a writer already
        // queued bytes for this proxy.
        uint64_t const PENDING_IRQF = s_ipc_lock.lock_irqsave();
        uint64_t const IRQF = proxy->lock.lock_irqsave();
        uint32_t const HEAD = proxy->ring_head.load(std::memory_order_acquire);
        uint32_t const TAIL = proxy->ring_tail.load(std::memory_order_relaxed);
        if (HEAD != TAIL) {
            proxy->lock.unlock_irqrestore(IRQF);
            s_ipc_lock.unlock_irqrestore(PENDING_IRQF);
            continue;
        }
        auto* pending = find_pending_pipe_delivery_locked(proxy->home_node, proxy->resource_id);
        if (pending != nullptr && !pending->chunks.empty()) {
            proxy->lock.unlock_irqrestore(IRQF);
            s_ipc_lock.unlock_irqrestore(PENDING_IRQF);
            continue;
        }
        if (proxy->write_closed.load(std::memory_order_acquire) != 0U) {
            proxy->lock.unlock_irqrestore(IRQF);
            s_ipc_lock.unlock_irqrestore(PENDING_IRQF);
            return 0;
        }

        task->wait_channel = "wki_proxy_pipe";
        task->deferred_task_switch = true;
        proxy->blocked_reader.store(task, std::memory_order_release);
        proxy->lock.unlock_irqrestore(IRQF);
        s_ipc_lock.unlock_irqrestore(PENDING_IRQF);
        return -512;  // ERESTARTSYS
    }
}

auto proxy_pipe_write(ker::vfs::File* f, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr || !proxy->active) {
        return -EBADF;
    }

    // Send data via wire message to home node
    // Message format: [DevOpReqPayload(4B)] [resource_id:u32] [data...]
    constexpr size_t HEADER_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    size_t const MAX_CHUNK = 4096;  // reasonable chunk size per message

    size_t const TO_SEND = count < MAX_CHUNK ? count : MAX_CHUNK;
    size_t const MSG_SIZE = HEADER_SIZE + TO_SEND;
    auto* msg = new (std::nothrow) uint8_t[MSG_SIZE];
    if (msg == nullptr) {
        return -ENOMEM;
    }

    auto* req = reinterpret_cast<DevOpReqPayload*>(msg);
    req->op_id = OP_PIPE_DATA;
    req->data_len = static_cast<uint16_t>(sizeof(uint32_t) + TO_SEND);

    std::memcpy(msg + sizeof(DevOpReqPayload), &proxy->resource_id, sizeof(uint32_t));

    std::memcpy(msg + HEADER_SIZE, buf, TO_SEND);

    int const RET = wki_send(proxy->home_node, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(MSG_SIZE));
    delete[] msg;

    if (RET != WKI_OK) {
        return -EIO;
    }
    return static_cast<ssize_t>(TO_SEND);
}

auto proxy_pipe_close(ker::vfs::File* f) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr) {
        return 0;
    }

    // Send close to home node with resource_id
    uint16_t const OP = (f->fops != nullptr && f->fops->vfs_write == nullptr) ? OP_PIPE_CLOSE_READ : OP_PIPE_CLOSE_WRITE;

    constexpr size_t HEADER_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    uint8_t msg[HEADER_SIZE];
    auto* req = reinterpret_cast<DevOpReqPayload*>(msg);
    req->op_id = OP;
    req->data_len = sizeof(uint32_t);
    std::memcpy(msg + sizeof(DevOpReqPayload), &proxy->resource_id, sizeof(uint32_t));

    if (proxy->home_node != WKI_NODE_INVALID) {
        wki_send(proxy->home_node, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(HEADER_SIZE));
    }

    wki_ipc_detach_proxy_file(f, proxy);
    return 0;
}

auto proxy_pipe_poll_check(ker::vfs::File* f, int events) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr) {
        return 0;
    }

    int ready = 0;
    uint32_t const HEAD = proxy->ring_head.load(std::memory_order_acquire);
    uint32_t const TAIL = proxy->ring_tail.load(std::memory_order_acquire);
    uint32_t const AVAIL = HEAD - TAIL;
    bool const WR_CLOSED = proxy->write_closed.load(std::memory_order_acquire) != 0U;

    if (f->fops != nullptr && f->fops->vfs_write == nullptr) {
        // Read end
        if (((events & 0x0001) != 0) && (AVAIL > 0 || WR_CLOSED)) {
            ready |= 0x0001;  // POLLIN
        }
        if (WR_CLOSED && AVAIL == 0) {
            ready |= 0x0010;  // POLLHUP
        }
    } else {
        // Write end — always writable (sends via wire message)
        if ((events & 0x0004) != 0) {
            ready |= 0x0004;  // POLLOUT
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
};

// ---- Server-side helpers ----

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
        proxy = find_proxy_by_resource_id_locked(resource_id);
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
    uint8_t close_msg[CLOSE_MSG_SIZE] = {};
    auto* req = reinterpret_cast<DevOpReqPayload*>(close_msg);
    req->op_id = OP_PIPE_CLOSE_READ;
    req->data_len = sizeof(uint32_t);
    std::memcpy(close_msg + sizeof(DevOpReqPayload), &resource_id, sizeof(uint32_t));
    if (HOME_NODE != WKI_NODE_INVALID) {
        wki_send(HOME_NODE, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, close_msg, static_cast<uint16_t>(CLOSE_MSG_SIZE));
    }

    wki_ipc_detach_proxy_file(f, proxy);
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

constexpr size_t PTY_IOCTL_ARG_SIZE = 128;  // max bytes we send/receive for ioctl arg

auto proxy_pty_ioctl(ker::vfs::File* f, unsigned long cmd, unsigned long arg) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
        return -EBADF;
    }
    if (proxy->home_node == WKI_NODE_INVALID) {
        return -EHOSTUNREACH;
    }

    // Wire layout (after resource_id): [cmd:u64][arg_val:u64][arg_in:128B]
    constexpr size_t EXTRA = sizeof(uint64_t) + sizeof(uint64_t) + PTY_IOCTL_ARG_SIZE;
    constexpr size_t MSG_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t) + EXTRA;
    uint8_t msg[MSG_SIZE] = {};

    auto* req = reinterpret_cast<DevOpReqPayload*>(msg);
    req->op_id = OP_PTY_IOCTL;
    req->data_len = static_cast<uint16_t>(sizeof(uint32_t) + EXTRA);
    std::memcpy(msg + sizeof(DevOpReqPayload), &proxy->resource_id, sizeof(uint32_t));

    auto cmd64 = static_cast<uint64_t>(cmd);
    auto arg64 = static_cast<uint64_t>(arg);
    size_t cursor = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    std::memcpy(msg + cursor, &cmd64, sizeof(uint64_t));
    cursor += sizeof(uint64_t);
    std::memcpy(msg + cursor, &arg64, sizeof(uint64_t));
    cursor += sizeof(uint64_t);

    // If arg looks like a pointer, copy the pointed-to data into the request
    if (arg >= 4096) {
        std::memcpy(msg + cursor, reinterpret_cast<const void*>(arg), PTY_IOCTL_ARG_SIZE);
    }

    WkiWaitEntry wait = {};
    uint64_t irqf = proxy->lock.lock_irqsave();
    if (proxy->pending_wait != nullptr) {
        proxy->lock.unlock_irqrestore(irqf);
        return -EBUSY;
    }
    proxy->pending_wait = &wait;
    proxy->pending_wait_op = OP_PTY_IOCTL;
    proxy->pending_wait_status = -ETIMEDOUT;
    proxy->pending_wait_resp_len = 0;
    proxy->lock.unlock_irqrestore(irqf);

    int const TX = wki_send(proxy->home_node, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(MSG_SIZE));
    if (TX != WKI_OK) {
        irqf = proxy->lock.lock_irqsave();
        proxy->pending_wait = nullptr;
        proxy->lock.unlock_irqrestore(irqf);
        return -EIO;
    }

    int const WAIT_RC = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    if (WAIT_RC != 0) {
        irqf = proxy->lock.lock_irqsave();
        proxy->pending_wait = nullptr;
        proxy->lock.unlock_irqrestore(irqf);
        return -ETIMEDOUT;
    }

    irqf = proxy->lock.lock_irqsave();
    int const STATUS = proxy->pending_wait_status;
    // Copy response arg_out back to caller's pointer (output ioctls)
    if (arg >= 4096 && proxy->pending_wait_resp_len >= PTY_IOCTL_ARG_SIZE) {
        std::memcpy(reinterpret_cast<void*>(arg), proxy->pending_wait_resp, PTY_IOCTL_ARG_SIZE);
    }
    proxy->pending_wait = nullptr;
    proxy->lock.unlock_irqrestore(irqf);
    return STATUS;
}

auto proxy_pty_isatty(ker::vfs::File* /*f*/) -> bool { return true; }

auto proxy_pty_close(ker::vfs::File* f) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr) {
        return 0;
    }

    // Send OP_PTY_CLOSE to home node (no response expected)
    constexpr size_t MSG_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    uint8_t msg[MSG_SIZE] = {};
    auto* req = reinterpret_cast<DevOpReqPayload*>(msg);
    req->op_id = OP_PTY_CLOSE;
    req->data_len = sizeof(uint32_t);
    std::memcpy(msg + sizeof(DevOpReqPayload), &proxy->resource_id, sizeof(uint32_t));
    if (proxy->home_node != WKI_NODE_INVALID) {
        wki_send(proxy->home_node, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(MSG_SIZE));
    }

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

auto find_proxy_by_resource_id(uint32_t resource_id) -> ProxyIpcState* { return find_proxy_by_resource_id_locked(resource_id); }

auto allocate_ipc_resource_id() -> uint32_t { return g_next_ipc_resource_id++; }

// =============================================================================
// Server-side pipe pump: reads from real pipe, sends data via wire messages
// =============================================================================

struct PipePumpArg {
    std::atomic<WkiIpcExport*> exp{nullptr};
    ker::mod::sched::task::Task* worker = nullptr;
};

constexpr int MAX_PUMPS = 32;
PipePumpArg g_pump_args[MAX_PUMPS];

auto stop_pipe_pump_locked(WkiIpcExport* exp) -> ker::mod::sched::task::Task* {
    if (exp == nullptr) {
        return nullptr;
    }
    exp->pump_running.store(false, std::memory_order_release);
    return exp->pump_task;
}

void wake_pipe_pump(ker::mod::sched::task::Task* task) {
    if (task != nullptr) {
        ker::mod::sched::kern_wake(task);
    }
}

auto consume_deferred_wait_for_daemon() -> bool {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || task->type != ker::mod::sched::task::TaskType::DAEMON) {
        return false;
    }

    // Blocking file ops set deferred_task_switch for the userspace syscall exit
    // path. WKI pump workers park explicitly with kern_block(), so consume the
    // syscall-only marker before entering the scheduler wait path.
    bool const HAD_DEFERRED_WAIT = task->deferred_task_switch;
    task->deferred_task_switch = false;
    task->yield_switch = false;
    return HAD_DEFERRED_WAIT;
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
    task->wait_channel = "wki_pipe_pump_poll";
    if (!file->fops->vfs_poll_register_waiter(file, task->pid)) {
        return false;
    }
    if (ready_now != nullptr && file->fops->vfs_poll_check != nullptr) {
        *ready_now = (file->fops->vfs_poll_check(file, POLL_READ_EVENTS) & POLL_READ_EVENTS) != 0;
    }
    return true;
}

auto register_poll_write_waiter(ker::vfs::File* file, bool* ready_now) -> bool {
    constexpr int POLL_WRITE_EVENTS = 0x0004 | 0x0008;  // POLLOUT | POLLERR
    if (ready_now != nullptr) {
        *ready_now = false;
    }
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || file == nullptr || file->fops == nullptr || file->fops->vfs_poll_register_waiter == nullptr) {
        return false;
    }
    task->wait_channel = "wki_pipe_write_poll";
    if (!file->fops->vfs_poll_register_waiter(file, task->pid)) {
        return false;
    }
    if (ready_now != nullptr && file->fops->vfs_poll_check != nullptr) {
        *ready_now = (file->fops->vfs_poll_check(file, POLL_WRITE_EVENTS) & POLL_WRITE_EVENTS) != 0;
    }
    return true;
}

template <int SLOT>
[[noreturn]] void pipe_pump_thread_fn() {
    auto* arg = &g_pump_args[SLOT];

    for (;;) {
        auto* exp = arg->exp.load(std::memory_order_acquire);
        if (exp == nullptr) {
            ker::mod::sched::kern_block();
            continue;
        }

        ker::vfs::File* file = nullptr;
        uint16_t target = WKI_NODE_INVALID;
        uint32_t resource_id = 0;
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            if (arg->exp.load(std::memory_order_acquire) != exp) {
                s_ipc_lock.unlock_irqrestore(IRQF);
                continue;
            }
            if (!exp->active || exp->file == nullptr) {
                exp->pump_running.store(false, std::memory_order_release);
                exp->pump_task = nullptr;
                arg->exp.store(nullptr, std::memory_order_release);
                s_ipc_lock.unlock_irqrestore(IRQF);
                continue;
            }
            file = exp->file;
            file->refcount.fetch_add(1, std::memory_order_acq_rel);
            target = exp->consumer_node;
            resource_id = exp->resource_id;
            s_ipc_lock.unlock_irqrestore(IRQF);
        }

        constexpr size_t BUF_SIZE = 4096;
        constexpr size_t HEADER_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
        auto* buf = new (std::nothrow) uint8_t[BUF_SIZE];
        auto* msg = new (std::nothrow) uint8_t[HEADER_SIZE + BUF_SIZE];
        if (buf == nullptr || msg == nullptr) {
            delete[] buf;
            delete[] msg;
            ipc_release_file_ref(file);
            exp->pump_running.store(false, std::memory_order_release);
            exp->pump_task = nullptr;
            arg->exp.store(nullptr, std::memory_order_release);
            ker::mod::dbg::log("[WKI] IPC pump: malloc failed for resource_id=%u", resource_id);
            continue;
        }

        ker::mod::dbg::log("[WKI] IPC pipe pump started: resource_id=%u target=0x%04x", resource_id, target);

        while (exp->active && exp->pump_running.load(std::memory_order_acquire)) {
            ssize_t n = 0;
            if (file != nullptr && file->fops != nullptr && file->fops->vfs_read != nullptr) {
                n = file->fops->vfs_read(file, buf, BUF_SIZE, 0);
            } else {
                break;
            }

            if (n > 0) {
                auto* req = reinterpret_cast<DevOpReqPayload*>(msg);
                req->op_id = OP_PIPE_DATA;
                req->data_len = static_cast<uint16_t>(sizeof(uint32_t) + n);
                std::memcpy(msg + sizeof(DevOpReqPayload), &resource_id, sizeof(uint32_t));
                std::memcpy(msg + HEADER_SIZE, buf, static_cast<size_t>(n));

                int ret = WKI_ERR_TX_FAILED;
                uint32_t attempts = 0;
                while (exp->active && exp->pump_running.load(std::memory_order_acquire)) {
                    ret = wki_send(target, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(HEADER_SIZE + n));
                    if (ret == WKI_OK) {
                        break;
                    }
                    ker::mod::sched::kern_sleep_us(ipc_pipe_send_retry_sleep_us(ret, attempts++));
                }
                if (ret != WKI_OK) {
                    ker::mod::dbg::log("[WKI] IPC pipe pump DATA send failed: resource_id=%u ret=%d len=%ld", resource_id, ret, n);
                    break;
                }
            } else if (n == 0) {
                auto* req = reinterpret_cast<DevOpReqPayload*>(msg);
                req->op_id = OP_PIPE_CLOSE_WRITE;
                req->data_len = sizeof(uint32_t);
                std::memcpy(msg + sizeof(DevOpReqPayload), &resource_id, sizeof(uint32_t));
                int ret = WKI_ERR_TX_FAILED;
                uint32_t retries = 0;
                uint32_t attempts = 0;
                while (exp->active && (exp->pump_running.load(std::memory_order_acquire) || attempts == 0) &&
                       attempts < WKI_IPC_PIPE_EOF_MAX_SEND_ATTEMPTS) {
                    attempts++;
                    ret = wki_send(target, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(HEADER_SIZE));
                    if (ret == WKI_OK) {
                        break;
                    }
                    retries++;
                    ker::mod::sched::kern_sleep_us(ipc_pipe_send_retry_sleep_us(ret, attempts));
                }
                if (ret != WKI_OK) {
                    if (attempts != 0 && exp->active) {
                        ker::mod::dbg::log("[WKI] IPC pipe pump EOF send failed: resource_id=%u ret=%d attempts=%u retries=%u", resource_id,
                                           ret, attempts, retries);
                    }
                }
#ifdef WKI_IPC_DEBUG
                else {
                    ker::mod::dbg::log("[WKI] IPC pipe pump EOF: resource_id=%u retries=%u", resource_id, retries);
                }
#endif
                break;
            } else if (n == WKI_IPC_PIPE_RESTARTSYS) {
                if (consume_deferred_wait_for_daemon()) {
                    ker::mod::sched::kern_block();
                } else {
                    ker::mod::sched::kern_sleep_us(1000);
                }
                continue;
            } else if (n == -EAGAIN) {
                bool ready_now = false;
                if (register_poll_read_waiter(file, &ready_now)) {
                    if (!ready_now) {
                        ker::mod::sched::kern_block();
                    }
                } else {
                    ker::mod::sched::kern_sleep_us(1000);
                }
                continue;
            } else {
                ker::mod::dbg::log("[WKI] IPC pipe pump read error: resource_id=%u err=%ld", resource_id, n);
                break;
            }
        }

        delete[] buf;
        delete[] msg;
        ipc_release_file_ref(file);
        exp->pump_running.store(false, std::memory_order_release);
        exp->pump_task = nullptr;
        arg->exp.store(nullptr, std::memory_order_release);
    }
}

using PumpFn = void (*)();
constexpr PumpFn G_PUMP_FNS[MAX_PUMPS] = {
    pipe_pump_thread_fn<0>,  pipe_pump_thread_fn<1>,  pipe_pump_thread_fn<2>,  pipe_pump_thread_fn<3>,  pipe_pump_thread_fn<4>,
    pipe_pump_thread_fn<5>,  pipe_pump_thread_fn<6>,  pipe_pump_thread_fn<7>,  pipe_pump_thread_fn<8>,  pipe_pump_thread_fn<9>,
    pipe_pump_thread_fn<10>, pipe_pump_thread_fn<11>, pipe_pump_thread_fn<12>, pipe_pump_thread_fn<13>, pipe_pump_thread_fn<14>,
    pipe_pump_thread_fn<15>, pipe_pump_thread_fn<16>, pipe_pump_thread_fn<17>, pipe_pump_thread_fn<18>, pipe_pump_thread_fn<19>,
    pipe_pump_thread_fn<20>, pipe_pump_thread_fn<21>, pipe_pump_thread_fn<22>, pipe_pump_thread_fn<23>, pipe_pump_thread_fn<24>,
    pipe_pump_thread_fn<25>, pipe_pump_thread_fn<26>, pipe_pump_thread_fn<27>, pipe_pump_thread_fn<28>, pipe_pump_thread_fn<29>,
    pipe_pump_thread_fn<30>, pipe_pump_thread_fn<31>,
};

void start_pipe_pump(WkiIpcExport* exp) {
    int slot = -1;
    for (int i = 0; i < MAX_PUMPS; i++) {
        WkiIpcExport* expected = nullptr;
        if (g_pump_args[i].exp.compare_exchange_strong(expected, exp, std::memory_order_acq_rel, std::memory_order_acquire)) {
            slot = i;
            break;
        }
    }

    if (slot < 0) {
        ker::mod::dbg::log("[WKI] IPC pump slots exhausted");
        return;
    }

    exp->pump_running.store(true, std::memory_order_release);

    auto* task = g_pump_args[slot].worker;
    if (task == nullptr) {
        task = ker::mod::sched::task::Task::create_kernel_thread("wki_pipe_pump", G_PUMP_FNS[slot]);
        if (task == nullptr) {
            exp->pump_running.store(false, std::memory_order_release);
            exp->pump_task = nullptr;
            g_pump_args[slot].exp.store(nullptr, std::memory_order_release);
            ker::mod::dbg::log("[WKI] IPC pump: failed to create kernel thread");
            return;
        }
        g_pump_args[slot].worker = task;
        ker::mod::sched::post_task_balanced(task);
    } else {
        ker::mod::sched::kern_wake(task);
    }

    exp->pump_task = task;
}

void free_ipc_dev_op_work(IpcDevOpWork* work) {
    if (work == nullptr) {
        return;
    }

    delete[] work->payload;

    delete work;
}

auto ipc_dev_op_expects_response(uint16_t op_id) -> bool {
    return op_id == OP_PIPE_POLL_STATE || op_id == OP_EPOLL_CTL || op_id == OP_FUTEX_WAKE || op_id == OP_PTY_IOCTL ||
           (op_id >= OP_SOCK_ACCEPT && op_id <= OP_SOCK_SETSOCKOPT);
}

void send_ipc_dev_op_error_response(const WkiHeader* hdr, uint16_t op_id, uint32_t resource_id, int16_t status) {
    if (hdr == nullptr || !ipc_dev_op_expects_response(op_id)) {
        return;
    }

    DevOpRespPayload resp = {};
    resp.op_id = op_id;
    resp.status = status;
    resp.data_len = sizeof(uint32_t);

    uint8_t resp_buf[sizeof(DevOpRespPayload) + sizeof(uint32_t)] = {};
    std::memcpy(resp_buf, &resp, sizeof(resp));
    std::memcpy(resp_buf + sizeof(DevOpRespPayload), &resource_id, sizeof(uint32_t));
    wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, resp_buf, static_cast<uint16_t>(sizeof(resp_buf)));
}

auto should_defer_ipc_dev_op(uint16_t op_id, uint32_t resource_id, uint16_t src_node) -> bool {
    if (op_id == OP_PIPE_DATA) {
        bool has_proxy = false;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        auto* proxy = find_proxy_by_resource_id_locked(resource_id);
        if (proxy != nullptr) {
            has_proxy = true;
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
        if (proxy != nullptr) {
            proxy_release(proxy);
        }

        // Proxy ring delivery is bounded and wakes local readers. Export-side
        // writes can enter arbitrary file/socket/PTY ops, so defer them.
        return !has_proxy;
    }

    if (op_id == OP_PIPE_POLL_STATE || op_id == OP_FUTEX_WAKE || op_id == OP_PTY_IOCTL || op_id == OP_PTY_CLOSE ||
        op_id == OP_PIPE_CLOSE_READ || op_id == OP_PIPE_CLOSE_WRITE) {
        (void)src_node;
        return true;
    }

    if (op_id == OP_EPOLL_CTL || (op_id >= OP_SOCK_ACCEPT && op_id <= OP_SOCK_SETSOCKOPT)) {
        (void)src_node;
        // Keep synchronous epoll/socket control ops on the immediate RX path.
        // The deferred IPC worker is a kernel daemon with an empty fd table, so
        // forwarded epoll_ctl() loses the export task's fd namespace there. The
        // socket control tests also rely on prompt request/response ordering for
        // short-lived exported sockets during exec/exit.
        return false;
    }

    return false;
}

auto enqueue_ipc_dev_op_work(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, uint16_t op_id, uint32_t resource_id)
    -> bool {
    auto* work = new (std::nothrow) IpcDevOpWork();
    auto* payload_copy = new (std::nothrow) uint8_t[payload_len];
    if (work == nullptr || payload_copy == nullptr) {
        delete[] payload_copy;

        delete work;

        send_ipc_dev_op_error_response(hdr, op_id, resource_id, -ENOMEM);
        return false;
    }

    work->hdr = *hdr;
    work->payload = payload_copy;
    work->payload_len = payload_len;
    std::memcpy(work->payload, payload, payload_len);

    ker::mod::sched::task::Task* worker = nullptr;
    bool queued = false;
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    if (g_ipc_dev_op_queue.size() < WKI_IPC_DEV_OP_MAX_PENDING) {
        g_ipc_dev_op_queue.push_back(work);
        worker = g_ipc_dev_op_worker_task;
        queued = true;
    }
    s_ipc_lock.unlock_irqrestore(IRQF);

    if (!queued) {
        free_ipc_dev_op_work(work);
        send_ipc_dev_op_error_response(hdr, op_id, resource_id, -EAGAIN);
        return false;
    }

    if (worker != nullptr) {
        ker::mod::sched::kern_wake(worker);
    }
    return true;
}

[[noreturn]] void ipc_dev_op_worker_thread_fn() {
    for (;;) {
        IpcDevOpWork* work = nullptr;
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            if (!g_ipc_dev_op_queue.empty()) {
                work = g_ipc_dev_op_queue.front();
                g_ipc_dev_op_queue.pop_front();
            }
            s_ipc_lock.unlock_irqrestore(IRQF);
        }

        if (work == nullptr) {
            ker::mod::sched::kern_block();
            continue;
        }

        detail::handle_ipc_dev_op_req_inline(&work->hdr, work->payload, work->payload_len);
        free_ipc_dev_op_work(work);
    }
}

}  // namespace

// =============================================================================
// Public API: Initialization
// =============================================================================

void wki_ipc_subsystem_init() {
    if (g_ipc_initialized) {
        return;
    }
    g_ipc_initialized = true;
    g_export_pipe_write_flush_task = ker::mod::sched::task::Task::create_kernel_thread("wki_pipe_write", export_pipe_write_flush_thread_fn);
    if (g_export_pipe_write_flush_task != nullptr) {
        ker::mod::sched::post_task_balanced(g_export_pipe_write_flush_task);
    } else {
        ker::mod::dbg::log("[WKI] IPC export pipe writer thread creation failed");
    }
    g_ipc_dev_op_worker_task = ker::mod::sched::task::Task::create_kernel_thread("wki_ipc_devop", ipc_dev_op_worker_thread_fn);
    if (g_ipc_dev_op_worker_task != nullptr) {
        ker::mod::sched::post_task_balanced(g_ipc_dev_op_worker_task);
    } else {
        ker::mod::dbg::log("[WKI] IPC dev-op worker thread creation failed");
    }
    ker::mod::dbg::log("[WKI] IPC proxy subsystem initialized");
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

    uint64_t pending_waiters[32]{};
    size_t pending_waiter_count = 0;

    uint64_t const IRQF = proxy->lock.lock_irqsave();
    if (!proxy->poll_waiters.empty()) {
        proxy_collect_waiters_locked(proxy->poll_waiters, pending_waiters, &pending_waiter_count);
    }
    proxy->lock.unlock_irqrestore(IRQF);

    proxy_reschedule_waiters(pending_waiters, pending_waiter_count);
}

// =============================================================================
// Public API: Export task fds for remote submission
// =============================================================================

auto wki_ipc_export_task_fds(ker::mod::sched::task::Task* task, uint16_t target_node, WkiIpcFdEntry* map_out, uint16_t* count_out) -> bool {
    if (task == nullptr || map_out == nullptr || count_out == nullptr) {
        return false;
    }

    uint16_t count = 0;
    constexpr uint16_t MAX_IPC_FDS = 16;

    task->fd_table.for_each([&](uint64_t fd_key, void* val) {
        if (val == nullptr || count >= MAX_IPC_FDS) {
            return;
        }
        auto* file = static_cast<ker::vfs::File*>(val);

        ResourceType res_type = ResourceType::CUSTOM;
        if (ker::vfs::vfs_is_pipe_file(file)) {
            res_type = ResourceType::IPC_PIPE;
        } else if (ker::vfs::vfs_is_socket_file(file)) {
            res_type = ResourceType::IPC_SOCKET;
        } else if (ker::vfs::vfs_is_epoll_file(file)) {
            res_type = ResourceType::IPC_EPOLL;
        } else if (should_proxy_tty_like_file(file)) {
            res_type = ResourceType::IPC_PTY;
        } else {
            return;
        }

        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        uint32_t const RESOURCE_ID = allocate_ipc_resource_id();

        auto* exp = new WkiIpcExport();
        exp->active = true;
        exp->resource_id = RESOURCE_ID;
        exp->res_type = res_type;
        exp->file = file;
        file->refcount.fetch_add(1, std::memory_order_relaxed);
        exp->consumer_node = target_node;
        exp->assigned_channel = WKI_CHAN_RESOURCE;

        g_ipc_exports.push_back(exp);
        s_ipc_lock.unlock_irqrestore(IRQF);

        // Start pump thread for pipe read-ends, sockets, and PTY files (data flows home->consumer)
        if (res_type == ResourceType::IPC_PIPE && file->open_flags == 0) {
            start_pipe_pump(exp);
        } else if (res_type == ResourceType::IPC_SOCKET) {
            start_pipe_pump(exp);  // reuse pump: reads from socket file fops then sends OP_PIPE_DATA
        } else if (res_type == ResourceType::IPC_PTY) {
            start_pipe_pump(exp);  // reuse pump: reads from PTY devfs fops then sends OP_PIPE_DATA
        }

        auto& entry = map_out[count];
        entry.local_fd = static_cast<uint16_t>(fd_key);
        entry.res_type = static_cast<uint16_t>(res_type);
        entry.resource_id = RESOURCE_ID;
        entry.home_node = g_wki.my_node_id;
        entry.reserved1 = static_cast<uint16_t>(file->open_flags) & WKI_IPC_FD_ACCESS_MASK;
        entry.rdma_rkey = 0;
        entry.rdma_offset = 0;
        count++;
    });

    *count_out = count;
    return count > 0;
}

// =============================================================================
// Public API: Attach proxy fds on remote task
// =============================================================================

void wki_ipc_attach_task_fds(ker::mod::sched::task::Task* task, const WkiIpcFdEntry* map, uint16_t count) {
    if (task == nullptr || map == nullptr || count == 0) {
        return;
    }

    for (uint16_t i = 0; i < count; i++) {
        const auto& entry = map[i];
        auto* proxy = new ProxyIpcState();
        proxy->active = true;
        proxy->res_type = static_cast<ResourceType>(entry.res_type);
        proxy->home_node = entry.home_node;
        proxy->resource_id = entry.resource_id;
        proxy->assigned_channel = WKI_CHAN_RESOURCE;

        // Allocate local ring buffer for pipe read proxies, sockets, and PTY files (recv data)
        if (proxy->res_type == ResourceType::IPC_PIPE || proxy->res_type == ResourceType::IPC_SOCKET ||
            proxy->res_type == ResourceType::IPC_PTY) {
            constexpr uint32_t PIPE_RING_CAPACITY = 65536;  // 64 KB
            auto* rb = new uint8_t[PIPE_RING_CAPACITY];
            if (rb != nullptr) {
                std::memset(rb, 0, PIPE_RING_CAPACITY);
                proxy->ring_buf = rb;
                proxy->ring_capacity = PIPE_RING_CAPACITY;
            } else {
                ker::mod::dbg::log("[WKI] IPC proxy ring allocation failed: fd=%u res_id=%u cap=%u", entry.local_fd, entry.resource_id,
                                   PIPE_RING_CAPACITY);
            }
            proxy->ring_head.store(0, std::memory_order_relaxed);
            proxy->ring_tail.store(0, std::memory_order_relaxed);
            proxy->write_closed.store(0U, std::memory_order_relaxed);
            proxy->blocked_reader.store(nullptr, std::memory_order_relaxed);
        }

        // Replace the fd's file with proxy file
        auto* existing_file = static_cast<ker::vfs::File*>(task->fd_table.lookup(entry.local_fd));

        auto* proxy_file = new ker::vfs::File();
        proxy_file->fd = static_cast<int>(entry.local_fd);
        proxy_file->private_data = proxy;
        proxy_file->pos = 0;
        proxy_file->is_directory = false;
        proxy_file->fs_type = ker::vfs::FSType::TMPFS;
        proxy_file->refcount = 1;
        proxy_file->fd_flags = 0;
        proxy_file->vfs_path = nullptr;
        proxy_file->dir_fs_count = 0;

        if (proxy->res_type == ResourceType::IPC_PIPE) {
            int const OPEN_FLAGS = static_cast<int>(entry.reserved1 & WKI_IPC_FD_ACCESS_MASK);
            proxy_file->open_flags = OPEN_FLAGS;
            if (OPEN_FLAGS == 0) {
                proxy_file->fops = &g_proxy_pipe_read_fops;
            } else {
                proxy_file->fops = &g_proxy_pipe_write_fops;
            }
        } else if (proxy->res_type == ResourceType::IPC_SOCKET) {
            proxy_file->fops = &g_proxy_socket_fops;
            proxy_file->open_flags = 0;
            proxy_file->fs_type = ker::vfs::FSType::TMPFS;  // not SOCKET to avoid bad cast via fd_to_socket
        } else if (proxy->res_type == ResourceType::IPC_EPOLL) {
            // Allocate a local EpollInstance so kernel epoll_ctl / epoll_pwait work
            // transparently on this fd.  EpollInstance MUST be at offset 0 in
            // ProxyEpollFile so the cast in epoll_ctl is valid.
            auto* epf = new ProxyEpollFile();
            epf->inst.count = 0;
            std::memset(epf->inst.interests.data(), 0, sizeof(epf->inst.interests));
            epf->resource_id = proxy->resource_id;
            epf->home_node = proxy->home_node;
            proxy_file->private_data = epf;  // epoll_ctl casts this to EpollInstance*
            proxy_file->fops = &g_proxy_epoll_fops;
            proxy_file->open_flags = 0;
        } else if (proxy->res_type == ResourceType::IPC_PTY) {
            // PTY proxy: ring-buffer data plane + ioctl forwarding
            // private_data is already set to proxy above
            proxy_file->fops = &g_proxy_pty_fops;
            proxy_file->open_flags = 0;
        } else {
            proxy_file->fops = &g_proxy_pipe_read_fops;
            proxy_file->open_flags = 0;
        }

        if (existing_file != nullptr) {
            task->fd_table.remove(entry.local_fd);
        }
        static_cast<void>(task->fd_table.insert(entry.local_fd, proxy_file));

        bool pending_write_closed = false;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy->refcount.fetch_add(1, std::memory_order_acq_rel);
        g_ipc_proxies.push_back(proxy);
        if (proxy->res_type == ResourceType::IPC_PIPE) {
            auto* pending = find_pending_pipe_delivery_locked(entry.home_node, entry.resource_id);
            pending_write_closed = pending != nullptr && pending->write_closed;
            if (pending != nullptr && pending->write_closed && pending->chunks.empty()) {
                erase_pending_pipe_delivery_locked(pending);
                free_pending_pipe_delivery(pending);
            }
        }
        s_ipc_lock.unlock_irqrestore(IRQF);

        if (pending_write_closed) {
            proxy_mark_pipe_closed(proxy, entry.resource_id);
        }
#ifdef WKI_IPC_DEBUG
        ker::mod::dbg::log("[WKI] IPC proxy attached fd=%u type=%u res_id=%u home=0x%04x", entry.local_fd, entry.res_type,
                           entry.resource_id, entry.home_node);
#endif
    }
}

// =============================================================================
// Doorbell RX handler — called from ISR when IPC doorbell arrives
// =============================================================================

void wki_ipc_doorbell_rx(uint16_t /*src_node*/, uint32_t /*doorbell_value*/) {
    // No longer used — pipe data flows via wire messages, not RDMA doorbells
}

auto wki_ipc_epoll_ctl_forward(ker::vfs::File* epfile, int op, int fd, uint32_t events, uint64_t data) -> int {
    if (epfile == nullptr) {
        return -EINVAL;
    }

    auto* epf = reinterpret_cast<ProxyEpollFile*>(epfile->private_data);
    if (epf == nullptr || epfile->fops != &g_proxy_epoll_fops) {
        return -EOPNOTSUPP;
    }

    ProxyIpcState* proxy = nullptr;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy = find_proxy_by_resource_id_locked(epf->resource_id);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    if (proxy == nullptr) {
        return -EBADF;
    }

    auto release_proxy = [&]() {
        if (proxy != nullptr) {
            proxy_release(proxy);
            proxy = nullptr;
        }
    };

    if (!proxy->active.load(std::memory_order_acquire)) {
        release_proxy();
        return -EBADF;
    }
    if (proxy->home_node == WKI_NODE_INVALID) {
        release_proxy();
        return -EHOSTUNREACH;
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
    constexpr size_t MSG_SIZE = sizeof(DevOpReqPayload) + RID_SIZE + EXTRA_SIZE;
    uint8_t msg[MSG_SIZE] = {};

    auto* req = reinterpret_cast<DevOpReqPayload*>(msg);
    req->op_id = OP_EPOLL_CTL;
    req->data_len = static_cast<uint16_t>(RID_SIZE + EXTRA_SIZE);
    std::memcpy(msg + sizeof(DevOpReqPayload), &proxy->resource_id, RID_SIZE);
    std::memcpy(msg + sizeof(DevOpReqPayload) + RID_SIZE, &ctl, EXTRA_SIZE);

    WkiWaitEntry wait = {};
    uint64_t irqf = proxy->lock.lock_irqsave();
    if (proxy->pending_wait != nullptr) {
        proxy->lock.unlock_irqrestore(irqf);
        release_proxy();
        return -EBUSY;
    }
    proxy->pending_wait = &wait;
    proxy->pending_wait_op = OP_EPOLL_CTL;
    proxy->pending_wait_status = -ETIMEDOUT;
    proxy->pending_wait_resp_len = 0;
    proxy->lock.unlock_irqrestore(irqf);

    int const TX = wki_send(proxy->home_node, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(MSG_SIZE));
    if (TX != WKI_OK) {
        irqf = proxy->lock.lock_irqsave();
        proxy->pending_wait = nullptr;
        proxy->lock.unlock_irqrestore(irqf);
        release_proxy();
        return -EIO;
    }

    int const WAIT_RC = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    if (WAIT_RC != 0) {
        irqf = proxy->lock.lock_irqsave();
        proxy->pending_wait = nullptr;
        proxy->lock.unlock_irqrestore(irqf);
        release_proxy();
        return -ETIMEDOUT;
    }

    irqf = proxy->lock.lock_irqsave();
    int const STATUS = proxy->pending_wait_status;
    proxy->pending_wait = nullptr;
    proxy->lock.unlock_irqrestore(irqf);
    release_proxy();
    return STATUS;
}

// =============================================================================
// DEV_OP response handler for IPC control ops
// =============================================================================

void wki_ipc_handle_dev_op_resp(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len) {
    (void)src_node;
    (void)channel;

    if (len < sizeof(DevOpRespPayload)) {
        return;
    }

    DevOpRespPayload resp = {};
    std::memcpy(&resp, payload, sizeof(resp));
    uint16_t const OP_ID = resp.op_id;

    // Handle pipe close acknowledgement
    if (OP_ID == OP_PIPE_CLOSE_READ || OP_ID == OP_PIPE_CLOSE_WRITE) {
        // Close acknowledged — no further action needed
        return;
    }

    if (OP_ID == OP_PIPE_POLL_STATE) {
        // Home node responded to a poll-state query with readiness flags.
        // Wake any blocked reader so it can re-check availability.
        if (len < static_cast<uint16_t>(sizeof(DevOpRespPayload) + (2 * sizeof(uint32_t)))) {
            return;
        }
        uint32_t poll_resource_id = 0;
        std::memcpy(&poll_resource_id, payload + sizeof(DevOpRespPayload), sizeof(uint32_t));
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        auto* proxy = find_proxy_by_resource_id_locked(poll_resource_id);
        if (proxy != nullptr) {
            proxy->refcount.fetch_add(1, std::memory_order_acq_rel);
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
        if (proxy != nullptr) {
            auto* reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
            if (reader != nullptr) {
                reader->deferred_task_switch = false;
                reader->wait_channel = nullptr;
                ker::mod::sched::kern_wake(reader);
            }
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

void wki_ipc_socket_handle_dev_op_resp(uint16_t /*src_node*/, uint16_t /*channel*/, const uint8_t* payload, uint16_t len) {
    if (len < sizeof(DevOpRespPayload)) {
        return;
    }

    DevOpRespPayload resp = {};
    std::memcpy(&resp, payload, sizeof(resp));

    uint16_t const DATA_LEN = resp.data_len;
    if (len < static_cast<uint16_t>(sizeof(DevOpRespPayload) + DATA_LEN)) {
        return;
    }
    const uint8_t* resp_data = payload + sizeof(DevOpRespPayload);

    // First 4 bytes of response data = resource_id
    if (DATA_LEN < sizeof(uint32_t)) {
        return;
    }
    uint32_t resource_id = 0;
    std::memcpy(&resource_id, resp_data, sizeof(uint32_t));
    auto const EXTRA_LEN = static_cast<uint16_t>(DATA_LEN - sizeof(uint32_t));
    const uint8_t* extra_data = resp_data + sizeof(uint32_t);

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    ProxyIpcState* target_proxy = nullptr;
    for (auto* p : g_ipc_proxies) {
        if (p != nullptr && p->resource_id == resource_id) {
            target_proxy = p;
            target_proxy->refcount.fetch_add(1, std::memory_order_acq_rel);
            break;
        }
    }
    s_ipc_lock.unlock_irqrestore(IRQF);

    if (target_proxy == nullptr) {
        return;
    }

    WkiWaitEntry* wait = nullptr;
    {
        uint64_t const PROXY_IRQF = target_proxy->lock.lock_irqsave();
        wait = target_proxy->pending_wait;
        if (wait != nullptr && target_proxy->pending_wait_op == resp.op_id) {
            target_proxy->pending_wait_status = static_cast<int>(resp.status);
            if (EXTRA_LEN > 0) {
                uint16_t const COPY_LEN =
                    EXTRA_LEN < ProxyIpcState::SOCK_CTRL_RESP_MAX ? EXTRA_LEN : static_cast<uint16_t>(ProxyIpcState::SOCK_CTRL_RESP_MAX);
                std::memcpy(target_proxy->pending_wait_resp, extra_data, COPY_LEN);
                target_proxy->pending_wait_resp_len = COPY_LEN;
            } else {
                target_proxy->pending_wait_resp_len = 0;
            }
        } else {
            wait = nullptr;  // op_id mismatch — stale response
        }
        target_proxy->lock.unlock_irqrestore(PROXY_IRQF);
    }

    if (wait != nullptr) {
        wki_wake_op(wait, 0);
    }

    if (target_proxy->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        delete[] target_proxy->ring_buf;
        delete target_proxy;
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
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy->active.store(false, std::memory_order_release);
        dropped_registry_ref = proxy_unregister_locked(proxy);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    auto* reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
    if (reader != nullptr) {
        reader->deferred_task_switch = false;
        reader->wait_channel = nullptr;
        ker::mod::sched::kern_wake(reader);
    }
    wki_ipc_proxy_wake_poll_waiters(proxy);

    if (f != nullptr) {
        f->private_data = nullptr;
    }

    if (dropped_registry_ref) {
        proxy_release(proxy);
    }
    proxy_release(proxy);
}

// =============================================================================
// Peer cleanup
// =============================================================================

void wki_ipc_cleanup_for_peer(uint16_t node_id) {
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    // NOLINTNEXTLINE(misc-const-correctness)
    ker::mod::sched::task::Task* stopped_pumps[WKI_IPC_MAX_EXPORTS]{};
    size_t stopped_pump_count = 0;

    // Clean up exports for this peer
    for (auto* exp : g_ipc_exports) {
        if (exp->active && exp->consumer_node == node_id) {
            auto* pump_task = stop_pipe_pump_locked(exp);
            if (pump_task != nullptr && stopped_pump_count < WKI_IPC_MAX_EXPORTS) {
                stopped_pumps[stopped_pump_count++] = pump_task;
            }
            if (exp->file != nullptr) {
                // Release the refcount bump taken at export time.
                // If this was the last reference, vfs_close handles teardown.
                if (exp->file->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    if (exp->file->fops != nullptr && exp->file->fops->vfs_close != nullptr) {
                        exp->file->fops->vfs_close(exp->file);
                    }
                    delete exp->file;
                }
                exp->file = nullptr;
            }
            exp->active = false;
        }
    }

    size_t detached_proxy_count = 0;
    // NOLINTBEGIN(misc-const-correctness)
    ProxyIpcState* detached_proxies[WKI_IPC_MAX_PROXIES]{};
    PendingPipeDelivery* detached_pending[WKI_IPC_MAX_EXPORTS]{};
    // NOLINTEND(misc-const-correctness)
    size_t detached_pending_count = 0;
    bool wake_export_pipe_flush = false;
    for (auto it = g_ipc_proxies.begin(); it != g_ipc_proxies.end();) {
        auto* proxy = *it;
        if (proxy != nullptr && proxy->active.load(std::memory_order_acquire) && proxy->home_node == node_id) {
            proxy->active.store(false, std::memory_order_release);
            if (detached_proxy_count < WKI_IPC_MAX_PROXIES) {
                detached_proxies[detached_proxy_count++] = proxy;
            }
            it = g_ipc_proxies.erase(it);
            continue;
        }
        ++it;
    }

    for (auto it = g_pending_pipe_deliveries.begin(); it != g_pending_pipe_deliveries.end();) {
        auto* pending = *it;
        if (pending != nullptr && pending->home_node == node_id) {
            if (detached_pending_count < WKI_IPC_MAX_EXPORTS) {
                detached_pending[detached_pending_count++] = pending;
            }
            it = g_pending_pipe_deliveries.erase(it);
            continue;
        }
        ++it;
    }

    for (auto* backlog : g_export_pipe_write_backlogs) {
        auto* exp = backlog != nullptr ? backlog->exp : nullptr;
        if (exp != nullptr && exp->consumer_node == node_id) {
            if (backlog != nullptr) {
                backlog->exp = nullptr;
                queue_export_pipe_write_flush_locked(backlog);
            }
            wake_export_pipe_flush = true;
        }
    }

    s_ipc_lock.unlock_irqrestore(IRQF);

    for (size_t i = 0; i < stopped_pump_count; i++) {
        wake_pipe_pump(stopped_pumps[i]);
    }

    if (wake_export_pipe_flush && g_export_pipe_write_flush_task != nullptr) {
        ker::mod::sched::kern_wake(g_export_pipe_write_flush_task);
    }

    for (size_t i = 0; i < detached_proxy_count; i++) {
        auto* proxy = detached_proxies[i];
        auto* reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
        if (reader != nullptr) {
            reader->deferred_task_switch = false;
            reader->wait_channel = nullptr;
            ker::mod::sched::kern_wake(reader);
        }
        wki_ipc_proxy_wake_poll_waiters(proxy);
        proxy_release(proxy);
    }

    for (size_t i = 0; i < detached_pending_count; i++) {
        free_pending_pipe_delivery(detached_pending[i]);
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

void handle_ipc_dev_op_req_inline(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
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
    uint16_t const OP_DATA_LEN = req.data_len - sizeof(uint32_t);

    if (OP_ID == OP_PIPE_DATA) {
        // Consumer receives pipe data — write into the local proxy ring if
        // present, otherwise route it to the exported home-side pipe.
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        auto* proxy = find_proxy_by_resource_id(resource_id);
        bool const PENDING_BUFFERED = find_pending_pipe_delivery_locked(hdr->src_node, resource_id) != nullptr;
        ker::vfs::File* export_file = nullptr;
        if (proxy == nullptr) {
            export_file = ipc_acquire_export_file_locked(resource_id);
        }
        s_ipc_lock.unlock_irqrestore(IRQF);

        if (proxy != nullptr) {
            if (OP_DATA_LEN == 0) {
                proxy_release(proxy);
                return;
            }

            if (PENDING_BUFFERED || proxy->ring_buf == nullptr) {
                if (!queue_pending_pipe_data(hdr->src_node, resource_id, op_data, OP_DATA_LEN)) {
                    ker::mod::dbg::log("[WKI] IPC pending pipe DATA queue failed: resource_id=%u len=%u", resource_id, OP_DATA_LEN);
                } else {
                    wake_proxy_reader(proxy);
                    wki_ipc_proxy_wake_poll_waiters(proxy);
                }
                proxy_release(proxy);
                return;
            }

            auto* reader = static_cast<ker::mod::sched::task::Task*>(nullptr);
            bool queue_overflow = false;
            uint64_t const PROXY_IRQF = proxy->lock.lock_irqsave();
            if (!proxy->active.load(std::memory_order_acquire) || proxy->ring_buf == nullptr) {
                proxy->lock.unlock_irqrestore(PROXY_IRQF);
                if (!queue_pending_pipe_data(hdr->src_node, resource_id, op_data, OP_DATA_LEN)) {
                    ker::mod::dbg::log("[WKI] IPC pending pipe DATA queue failed: resource_id=%u len=%u", resource_id, OP_DATA_LEN);
                } else {
                    wake_proxy_reader(proxy);
                    wki_ipc_proxy_wake_poll_waiters(proxy);
                }
                proxy_release(proxy);
                return;
            }

            uint32_t const HEAD = proxy->ring_head.load(std::memory_order_relaxed);
            uint32_t const TAIL = proxy->ring_tail.load(std::memory_order_acquire);
            uint32_t const CAP = proxy->ring_capacity;
            uint32_t const FREE_SPACE = CAP - (HEAD - TAIL);

            if (OP_DATA_LEN > FREE_SPACE) {
                queue_overflow = true;
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
                if (!queue_pending_pipe_data(hdr->src_node, resource_id, op_data, OP_DATA_LEN)) {
                    ker::mod::dbg::log("[WKI] IPC pending pipe DATA queue failed: resource_id=%u len=%u", resource_id, OP_DATA_LEN);
                } else {
                    wake_proxy_reader(proxy);
                }
                proxy_release(proxy);
                return;
            }

            if (reader != nullptr) {
                reader->deferred_task_switch = false;
                reader->wait_channel = nullptr;
                ker::mod::sched::kern_wake(reader);
            }
            wki_ipc_proxy_wake_poll_waiters(proxy);
            proxy_release(proxy);
            return;
        }

        if (export_file == nullptr) {
            if (OP_DATA_LEN > 0 && !queue_pending_pipe_data(hdr->src_node, resource_id, op_data, OP_DATA_LEN)) {
                ker::mod::dbg::log("[WKI] IPC pending pipe DATA queue failed: resource_id=%u len=%u", resource_id, OP_DATA_LEN);
            }
            return;
        }
        if (OP_DATA_LEN == 0) {
            ipc_release_file_ref(export_file);
            return;
        }
        if (export_file->fops != nullptr && export_file->fops->vfs_write != nullptr) {
            ssize_t const WRITE_RET =
                export_file->fops->vfs_write(export_file, op_data, OP_DATA_LEN, static_cast<size_t>(export_file->pos));
            if (std::cmp_not_equal(WRITE_RET, OP_DATA_LEN)) {
                uint16_t written = 0;
                if (WRITE_RET > 0) {
                    written = static_cast<uint16_t>(std::cmp_less(WRITE_RET, OP_DATA_LEN) ? WRITE_RET : OP_DATA_LEN);
                } else if (WRITE_RET != WKI_IPC_PIPE_RESTARTSYS && WRITE_RET != -EAGAIN) {
                    ker::mod::dbg::log("[WKI] IPC export pipe immediate write failed: resource_id=%u ret=%ld len=%u", resource_id,
                                       WRITE_RET, OP_DATA_LEN);
                }

                uint64_t const EXPORT_IRQF = s_ipc_lock.lock_irqsave();
                auto* exp = find_export_by_resource_id(resource_id);
                s_ipc_lock.unlock_irqrestore(EXPORT_IRQF);
                if (exp == nullptr || !queue_export_pipe_write_data(exp, op_data + written, static_cast<uint16_t>(OP_DATA_LEN - written))) {
                    ker::mod::dbg::log("[WKI] IPC export pipe backlog queue failed: resource_id=%u written=%u total=%u", resource_id,
                                       written, OP_DATA_LEN);
                }
            }
        }
        ipc_release_file_ref(export_file);
        return;
    }

    if (OP_ID == OP_PIPE_POLL_STATE) {
        // Consumer queries current readiness of the exported pipe/socket.
        // Server checks poll_check on the underlying file and sends back a
        // response with readiness flags.
        ker::vfs::File* f = nullptr;
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            f = ipc_acquire_export_file_locked(resource_id);
            s_ipc_lock.unlock_irqrestore(IRQF);
        }

        DevOpRespPayload poll_resp = {};
        poll_resp.op_id = OP_PIPE_POLL_STATE;
        poll_resp.status = 0;
        poll_resp.data_len = sizeof(uint32_t) * 2;  // [resource_id, ready_flags]
        poll_resp.reserved = 0;

        uint8_t poll_resp_buf[sizeof(DevOpRespPayload) + (sizeof(uint32_t) * 2)] = {};
        std::memcpy(poll_resp_buf, &poll_resp, sizeof(poll_resp));
        std::memcpy(poll_resp_buf + sizeof(DevOpRespPayload), &resource_id, sizeof(uint32_t));

        int ready_flags = 0;
        if (f != nullptr) {
            if (f->fops != nullptr && f->fops->vfs_poll_check != nullptr) {
                ready_flags = f->fops->vfs_poll_check(f, 0x0001 | 0x0004);  // POLLIN | POLLOUT
            }
            ipc_release_file_ref(f);
        } else {
            poll_resp.status = -ENOENT;
        }

        std::memcpy(poll_resp_buf + sizeof(DevOpRespPayload) + sizeof(uint32_t), &ready_flags, sizeof(uint32_t));
        wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, poll_resp_buf, static_cast<uint16_t>(sizeof(poll_resp_buf)));
        return;
    }

    if (OP_ID == OP_EPOLL_CTL) {
        // Forward epoll_ctl to the home node epoll fd.
        // op_data layout: [op:i32][fd:i32][events:u32][data:u64]
        DevOpRespPayload ctl_resp = {};
        ctl_resp.op_id = OP_EPOLL_CTL;
        ctl_resp.status = -EINVAL;
        ctl_resp.data_len = sizeof(uint32_t);  // [resource_id]

        uint8_t ctl_resp_buf[sizeof(DevOpRespPayload) + sizeof(uint32_t)] = {};
        std::memcpy(ctl_resp_buf, &ctl_resp, sizeof(ctl_resp));
        std::memcpy(ctl_resp_buf + sizeof(DevOpRespPayload), &resource_id, sizeof(uint32_t));

        if (OP_DATA_LEN >= sizeof(int32_t) + sizeof(int32_t) + sizeof(uint32_t) + sizeof(uint64_t)) {
            int32_t ctl_op = 0;
            int32_t ctl_fd = -1;
            uint32_t ctl_events = 0;
            uint64_t ctl_data = 0;
            size_t cursor = 0;
            std::memcpy(&ctl_op, op_data + cursor, sizeof(int32_t));
            cursor += sizeof(int32_t);
            std::memcpy(&ctl_fd, op_data + cursor, sizeof(int32_t));
            cursor += sizeof(int32_t);
            std::memcpy(&ctl_events, op_data + cursor, sizeof(uint32_t));
            cursor += sizeof(uint32_t);
            std::memcpy(&ctl_data, op_data + cursor, sizeof(uint64_t));

            ker::vfs::File* f = nullptr;
            {
                uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                f = ipc_acquire_export_file_locked(resource_id);
                s_ipc_lock.unlock_irqrestore(IRQF);
            }

            if (f != nullptr && ker::vfs::vfs_is_epoll_file(f)) {
                ker::vfs::EpollEvent event = {};
                event.events = ctl_events;
                event.data.u64 = ctl_data;
                ctl_resp.status = static_cast<int16_t>(ker::vfs::epoll_ctl(f->fd, ctl_op, ctl_fd, &event));
                ipc_release_file_ref(f);
            } else if (f != nullptr) {
                ctl_resp.status = -EINVAL;
                ipc_release_file_ref(f);
            } else {
                ctl_resp.status = -ENOENT;
            }
        }

        std::memcpy(ctl_resp_buf, &ctl_resp, sizeof(ctl_resp));
        wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, ctl_resp_buf, static_cast<uint16_t>(sizeof(ctl_resp_buf)));
        return;
    }

    if (OP_ID == OP_FUTEX_WAKE) {
        // Wake waiters for a physical futex address owned by this node.
        // op_data layout: [phys_addr:u64]
        DevOpRespPayload wake_resp = {};
        wake_resp.op_id = OP_FUTEX_WAKE;
        wake_resp.status = -EINVAL;
        wake_resp.data_len = sizeof(uint32_t);  // [resource_id]

        uint8_t wake_resp_buf[sizeof(DevOpRespPayload) + sizeof(uint32_t)] = {};
        std::memcpy(wake_resp_buf, &wake_resp, sizeof(wake_resp));
        std::memcpy(wake_resp_buf + sizeof(DevOpRespPayload), &resource_id, sizeof(uint32_t));

        if (OP_DATA_LEN >= sizeof(uint64_t)) {
            uint64_t phys_addr = 0;
            std::memcpy(&phys_addr, op_data, sizeof(uint64_t));
            wake_resp.status = static_cast<int16_t>(ker::syscall::futex::futex_wake_by_phys(phys_addr));
        }

        std::memcpy(wake_resp_buf, &wake_resp, sizeof(wake_resp));
        wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, wake_resp_buf, static_cast<uint16_t>(sizeof(wake_resp_buf)));
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

        constexpr size_t RESP_BUF_SIZE = sizeof(DevOpRespPayload) + sizeof(uint32_t) + PTY_ARG_SIZE;
        uint8_t pty_resp_buf[RESP_BUF_SIZE] = {};
        std::memcpy(pty_resp_buf + sizeof(DevOpRespPayload), &resource_id, sizeof(uint32_t));

        constexpr size_t MIN_REQ = sizeof(uint64_t) + sizeof(uint64_t) + PTY_ARG_SIZE;
        if (OP_DATA_LEN >= MIN_REQ) {
            uint64_t ioctl_cmd = 0;
            uint64_t ioctl_arg_val = 0;
            std::memcpy(&ioctl_cmd, op_data, sizeof(uint64_t));
            std::memcpy(&ioctl_arg_val, op_data + sizeof(uint64_t), sizeof(uint64_t));
            const uint8_t* arg_in = op_data + sizeof(uint64_t) + sizeof(uint64_t);

            ker::vfs::File* f = nullptr;
            {
                uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                f = ipc_acquire_export_file_locked(resource_id);
                s_ipc_lock.unlock_irqrestore(IRQF);
            }

            if (f != nullptr) {
                unsigned long effective_arg = 0;
                alignas(8) uint8_t local_buf[PTY_ARG_SIZE] = {};
                if (ioctl_arg_val >= 4096) {
                    // Pointer-type ioctl: use local buffer
                    std::memcpy(local_buf, arg_in, PTY_ARG_SIZE);
                    effective_arg = reinterpret_cast<unsigned long>(local_buf);
                } else {
                    // Value-type ioctl: pass arg_val directly
                    effective_arg = static_cast<unsigned long>(ioctl_arg_val);
                }

                int const RC = ker::vfs::devfs::devfs_ioctl(f, static_cast<unsigned long>(ioctl_cmd), effective_arg);
                pty_resp.status = static_cast<int16_t>(RC < 0 ? RC : 0);

                // Always copy back local buffer (for output ioctls)
                std::memcpy(pty_resp_buf + sizeof(DevOpRespPayload) + sizeof(uint32_t), local_buf, PTY_ARG_SIZE);
                ipc_release_file_ref(f);
            } else {
                pty_resp.status = -ENOENT;
            }
        }

        std::memcpy(pty_resp_buf, &pty_resp, sizeof(pty_resp));
        wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, pty_resp_buf, static_cast<uint16_t>(RESP_BUF_SIZE));
        return;
    }

    if (OP_ID == OP_PTY_CLOSE) {
        // Consumer closed its proxy PTY — tear down the export
        ker::mod::sched::task::Task* pump_task = nullptr;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        auto* exp = find_export_by_resource_id(resource_id);
        if (exp != nullptr && exp->active) {
            pump_task = stop_pipe_pump_locked(exp);
            ker::vfs::File* f = exp->file;
            exp->file = nullptr;
            exp->active = false;
            s_ipc_lock.unlock_irqrestore(IRQF);
            wake_pipe_pump(pump_task);
            if (f != nullptr && f->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                if (f->fops != nullptr && f->fops->vfs_close != nullptr) {
                    f->fops->vfs_close(f);
                }
                delete f;
            }
        } else {
            s_ipc_lock.unlock_irqrestore(IRQF);
        }
        return;
    }

    if (OP_ID == OP_PIPE_CLOSE_WRITE) {
        // Server-side: consumer closed write end — close the real pipe's read end
        // Client-side: home node reports write-end closed (EOF)
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();

        // Check consumer-side proxy first
        auto* proxy = find_proxy_by_resource_id(resource_id);
        if (proxy != nullptr) {
            s_ipc_lock.unlock_irqrestore(IRQF);

            proxy_mark_pipe_closed(proxy, resource_id);
            proxy_release(proxy);
            return;
        }

        // Check server-side export
        auto* exp = find_export_by_resource_id(resource_id);
        if (exp != nullptr && exp->active && exp->file != nullptr) {
            s_ipc_lock.unlock_irqrestore(IRQF);
            if (!mark_export_pipe_write_closed(exp)) {
                ker::mod::dbg::log("[WKI] IPC export pipe close queue failed: resource_id=%u", resource_id);
            }
            return;
        }
        s_ipc_lock.unlock_irqrestore(IRQF);

        if (!mark_pending_pipe_closed(hdr->src_node, resource_id)) {
            ker::mod::dbg::log("[WKI] IPC pending EOF queue failed: resource_id=%u", resource_id);
        }
        return;
    }

    if (OP_ID == OP_PIPE_CLOSE_READ) {
        // Consumer closed read end — stop the pump, release the export's file ref.
        ker::mod::sched::task::Task* pump_task = nullptr;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        auto* exp = find_export_by_resource_id(resource_id);
        if (exp != nullptr && exp->active) {
            pump_task = stop_pipe_pump_locked(exp);
            ker::vfs::File* f = exp->file;
            exp->file = nullptr;
            exp->active = false;
            s_ipc_lock.unlock_irqrestore(IRQF);
            wake_pipe_pump(pump_task);
            if (f != nullptr && f->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                if (f->fops != nullptr && f->fops->vfs_close != nullptr) {
                    f->fops->vfs_close(f);
                }
                delete f;
            }
            return;
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
        return;
    }

    if (OP_ID >= OP_SOCK_ACCEPT && OP_ID <= OP_SOCK_SETSOCKOPT) {
        // Build a response with resource_id as first 4 bytes so the consumer
        // can find the right proxy in wki_ipc_socket_handle_dev_op_resp().
        constexpr size_t RESP_HEADER = sizeof(DevOpRespPayload);
        constexpr size_t RID_SIZE = sizeof(uint32_t);

        // Reserve up to 128 bytes for optional response payload (e.g. getpeername addr)
        constexpr size_t EXTRA_MAX = 128;
        uint8_t resp_buf[RESP_HEADER + RID_SIZE + EXTRA_MAX] = {};
        auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf);
        resp->op_id = OP_ID;
        resp->status = -ENOSYS;
        resp->data_len = RID_SIZE;  // at minimum include resource_id
        std::memcpy(resp_buf + RESP_HEADER, &resource_id, RID_SIZE);

        if (OP_ID == OP_SOCK_CLOSE) {
            ker::mod::sched::task::Task* pump_task = nullptr;
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            auto* exp = find_export_by_resource_id(resource_id);
            if (exp != nullptr && exp->active) {
                pump_task = stop_pipe_pump_locked(exp);
                ker::vfs::File* f = exp->file;
                exp->file = nullptr;
                exp->active = false;
                s_ipc_lock.unlock_irqrestore(IRQF);
                wake_pipe_pump(pump_task);

                if (f != nullptr && f->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    if (f->fops != nullptr && f->fops->vfs_close != nullptr) {
                        f->fops->vfs_close(f);
                    }
                    delete f;
                }
                resp->status = 0;
            } else {
                s_ipc_lock.unlock_irqrestore(IRQF);
                resp->status = -ENOENT;
            }
        } else if (OP_ID == OP_SOCK_SHUTDOWN) {
            // op_data: [how:int32]
            if (OP_DATA_LEN >= sizeof(int32_t)) {
                int32_t how = 0;
                std::memcpy(&how, op_data, sizeof(int32_t));
                ker::vfs::File* f = nullptr;
                {
                    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                    f = ipc_acquire_export_file_locked(resource_id);
                    s_ipc_lock.unlock_irqrestore(IRQF);
                }
                if (f != nullptr) {
                    auto* sock = static_cast<ker::net::Socket*>(f->private_data);
                    if (sock != nullptr && sock->proto_ops != nullptr && sock->proto_ops->shutdown != nullptr) {
                        resp->status = static_cast<int16_t>(sock->proto_ops->shutdown(sock, static_cast<int>(how)));
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
        } else if (OP_ID == OP_SOCK_GETPEERNAME) {
            // op_data: [] (no extra args needed)
            ker::vfs::File* f = nullptr;
            {
                uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                f = ipc_acquire_export_file_locked(resource_id);
                s_ipc_lock.unlock_irqrestore(IRQF);
            }
            if (f != nullptr) {
                auto* sock = static_cast<ker::net::Socket*>(f->private_data);
                if (sock != nullptr) {
                    // Serialize peer addr as: [family:u16][addr:u32][port:u16] (AF_INET)
                    struct {
                        uint16_t family;
                        uint32_t addr;
                        uint16_t port;
                    } __attribute__((packed)) peer_addr = {};
                    static_assert(sizeof(peer_addr) == 8);
                    peer_addr.family = static_cast<uint16_t>(sock->domain);
                    peer_addr.addr = sock->remote_v4.addr;
                    peer_addr.port = sock->remote_v4.port;
                    std::memcpy(resp_buf + RESP_HEADER + RID_SIZE, &peer_addr, sizeof(peer_addr));
                    resp->data_len = static_cast<uint16_t>(RID_SIZE + sizeof(peer_addr));
                    resp->status = 0;
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
                    f = ipc_acquire_export_file_locked(resource_id);
                    s_ipc_lock.unlock_irqrestore(IRQF);
                }
                if (f != nullptr) {
                    auto* sock = static_cast<ker::net::Socket*>(f->private_data);
                    if (sock != nullptr && sock->proto_ops != nullptr && sock->proto_ops->getsockopt != nullptr) {
                        uint8_t opt_val[64] = {};
                        size_t opt_len = sizeof(opt_val);
                        int const RC =
                            sock->proto_ops->getsockopt(sock, static_cast<int>(level), static_cast<int>(optname), opt_val, &opt_len);
                        resp->status = static_cast<int16_t>(RC);
                        if (RC == 0 && opt_len > 0 && opt_len <= EXTRA_MAX) {
                            std::memcpy(resp_buf + RESP_HEADER + RID_SIZE, opt_val, opt_len);
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
                    f = ipc_acquire_export_file_locked(resource_id);
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

        wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, resp_buf, static_cast<uint16_t>(RESP_HEADER + resp->data_len));
        return;
    }

    (void)hdr;
}

void handle_ipc_dev_op_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (hdr == nullptr || payload == nullptr || payload_len < sizeof(DevOpReqPayload)) {
        return;
    }

    DevOpReqPayload req = {};
    std::memcpy(&req, payload, sizeof(req));
    if (req.data_len < sizeof(uint32_t) || sizeof(DevOpReqPayload) + req.data_len > payload_len) {
        return;
    }

    uint32_t resource_id = 0;
    std::memcpy(&resource_id, payload + sizeof(DevOpReqPayload), sizeof(uint32_t));

    if (should_defer_ipc_dev_op(req.op_id, resource_id, hdr->src_node)) {
        enqueue_ipc_dev_op_work(hdr, payload, payload_len, req.op_id, resource_id);
        return;
    }

    handle_ipc_dev_op_req_inline(hdr, payload, payload_len);
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
