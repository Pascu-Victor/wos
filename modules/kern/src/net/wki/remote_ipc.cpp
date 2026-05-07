#include "remote_ipc.hpp"

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <deque>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/vfs.hpp>

namespace ker::net::wki {

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

constexpr ssize_t WKI_IPC_PIPE_RESTARTSYS = -512;
constexpr uint64_t WKI_IPC_PIPE_WRITE_RETRY_US = 100;
constexpr uint64_t WKI_IPC_WORKER_IDLE_SLEEP_US = 60'000'000;

// ---- Proxy fops for pipe (message-based, local ring buffer) ----

static void proxy_release(ProxyIpcState* proxy) {
    if (proxy == nullptr) return;
    if (proxy->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        if (proxy->ring_buf != nullptr) {
            delete[] proxy->ring_buf;
        }
        delete proxy;
    }
}

static auto proxy_unregister_locked(ProxyIpcState* proxy) -> bool {
    for (auto it = g_ipc_proxies.begin(); it != g_ipc_proxies.end(); ++it) {
        if (*it == proxy) {
            g_ipc_proxies.erase(it);
            return true;
        }
    }
    return false;
}

static auto find_proxy_by_resource_id_locked(uint32_t resource_id) -> ProxyIpcState* {
    for (auto* proxy : g_ipc_proxies) {
        if (proxy != nullptr && proxy->active.load(std::memory_order_acquire) && proxy->resource_id == resource_id) {
            proxy->refcount.fetch_add(1, std::memory_order_acq_rel);
            return proxy;
        }
    }
    return nullptr;
}

static auto ipc_acquire_export_file_locked(uint32_t resource_id) -> ker::vfs::File* {
    for (auto* exp : g_ipc_exports) {
        if (exp == nullptr || !exp->active || exp->resource_id != resource_id || exp->file == nullptr) {
            continue;
        }

        exp->file->refcount.fetch_add(1, std::memory_order_acq_rel);
        return exp->file;
    }
    return nullptr;
}

static void ipc_release_file_ref(ker::vfs::File* file) {
    if (file == nullptr) return;
    if (file->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        if (file->fops != nullptr && file->fops->vfs_close != nullptr) {
            file->fops->vfs_close(file);
        }
        delete file;
    }
}

static auto find_pending_pipe_delivery_locked(uint16_t home_node, uint32_t resource_id) -> PendingPipeDelivery* {
    for (auto* pending : g_pending_pipe_deliveries) {
        if (pending != nullptr && pending->home_node == home_node && pending->resource_id == resource_id) {
            return pending;
        }
    }
    return nullptr;
}

static void erase_pending_pipe_delivery_locked(PendingPipeDelivery* pending) {
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

static void free_pending_pipe_delivery(PendingPipeDelivery* pending) {
    if (pending == nullptr) {
        return;
    }
    for (auto& chunk : pending->chunks) {
        if (chunk.data != nullptr) {
            delete[] chunk.data;
        }
    }
    delete pending;
}

static auto find_export_pipe_write_backlog_locked(WkiIpcExport* exp) -> ExportPipeWriteBacklog* {
    for (auto* backlog : g_export_pipe_write_backlogs) {
        if (backlog != nullptr && backlog->exp == exp) {
            return backlog;
        }
    }
    return nullptr;
}

static void erase_export_pipe_write_backlog_locked(ExportPipeWriteBacklog* backlog) {
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

static void free_export_pipe_write_backlog(ExportPipeWriteBacklog* backlog) {
    if (backlog == nullptr) {
        return;
    }

    for (auto& chunk : backlog->chunks) {
        if (chunk.data != nullptr) {
            delete[] chunk.data;
        }
    }
    delete backlog;
}

static auto ensure_export_pipe_write_backlog_locked(WkiIpcExport* exp) -> ExportPipeWriteBacklog* {
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

static void queue_export_pipe_write_flush_locked(ExportPipeWriteBacklog* backlog) {
    if (backlog == nullptr || backlog->queued) {
        return;
    }

    backlog->queued = true;
    g_export_pipe_write_flush_queue.push_back(backlog);
}

static auto queue_export_pipe_write_data(WkiIpcExport* exp, const uint8_t* data, uint16_t len) -> bool {
    if (exp == nullptr || data == nullptr || len == 0) {
        return true;
    }

    auto* copy = new (std::nothrow) uint8_t[len];
    if (copy == nullptr) {
        return false;
    }
    std::memcpy(copy, data, len);

    uint64_t irqf = s_ipc_lock.lock_irqsave();
    auto* backlog = ensure_export_pipe_write_backlog_locked(exp);
    if (backlog == nullptr) {
        s_ipc_lock.unlock_irqrestore(irqf);
        delete[] copy;
        return false;
    }

    backlog->chunks.push_back(PendingPipeChunk{.data = copy, .len = len});
    backlog->buffered_bytes += len;
    queue_export_pipe_write_flush_locked(backlog);
    auto* flush_task = g_export_pipe_write_flush_task;
    s_ipc_lock.unlock_irqrestore(irqf);

    if (flush_task != nullptr) {
        ker::mod::sched::kern_wake(flush_task);
    }
    return true;
}

static auto mark_export_pipe_write_closed(WkiIpcExport* exp) -> bool {
    if (exp == nullptr) {
        return false;
    }

    uint64_t irqf = s_ipc_lock.lock_irqsave();
    auto* backlog = ensure_export_pipe_write_backlog_locked(exp);
    if (backlog == nullptr) {
        s_ipc_lock.unlock_irqrestore(irqf);
        return false;
    }

    backlog->close_pending = true;
    queue_export_pipe_write_flush_locked(backlog);
    auto* flush_task = g_export_pipe_write_flush_task;
    s_ipc_lock.unlock_irqrestore(irqf);

    if (flush_task != nullptr) {
        ker::mod::sched::kern_wake(flush_task);
    }
    return true;
}

[[noreturn]] static void export_pipe_write_flush_thread_fn() {
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
            ker::mod::sched::kern_sleep_us(WKI_IPC_WORKER_IDLE_SLEEP_US);
            continue;
        }

        WkiIpcExport* exp = nullptr;
        ker::vfs::File* file = nullptr;
        uint32_t resource_id = 0;
        uint16_t remaining = 0;
        uint8_t* data_ptr = nullptr;
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
            free_export_pipe_write_backlog(backlog);
            continue;
        }

        if (write_ret > 0) {
            auto& chunk = backlog->chunks.front();
            uint16_t advanced = static_cast<uint16_t>(write_ret < remaining ? write_ret : remaining);
            chunk.offset = static_cast<uint16_t>(chunk.offset + advanced);
            backlog->buffered_bytes -= advanced;
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
            if (backlog->chunks.empty() && !backlog->close_pending) {
                free_export_pipe_write_backlog(backlog);
            }
            continue;
        }

        if (write_ret == WKI_IPC_PIPE_RESTARTSYS || write_ret == -EAGAIN) {
            queue_export_pipe_write_flush_locked(backlog);
            s_ipc_lock.unlock_irqrestore(irqf);
            ker::mod::sched::kern_sleep_us(WKI_IPC_PIPE_WRITE_RETRY_US);
            continue;
        }

        ker::mod::dbg::log("[WKI] IPC export pipe write failed: resource_id=%u ret=%ld remaining=%u", resource_id, write_ret, remaining);
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
    }
}

static auto queue_pending_pipe_data(uint16_t home_node, uint32_t resource_id, const uint8_t* data, uint16_t len) -> bool {
    if (data == nullptr || len == 0) {
        return true;
    }

    auto* copy = new (std::nothrow) uint8_t[len];
    if (copy == nullptr) {
        return false;
    }
    std::memcpy(copy, data, len);

    uint64_t irqf = s_ipc_lock.lock_irqsave();
    auto* pending = find_pending_pipe_delivery_locked(home_node, resource_id);
    if (pending == nullptr) {
        pending = new PendingPipeDelivery();
        if (pending == nullptr) {
            s_ipc_lock.unlock_irqrestore(irqf);
            delete[] copy;
            return false;
        }
        pending->home_node = home_node;
        pending->resource_id = resource_id;
        g_pending_pipe_deliveries.push_back(pending);
    }

    pending->chunks.push_back(PendingPipeChunk{.data = copy, .len = len});
    pending->buffered_bytes += len;
    s_ipc_lock.unlock_irqrestore(irqf);
    return true;
}

static auto mark_pending_pipe_closed(uint16_t home_node, uint32_t resource_id) -> bool {
    uint64_t irqf = s_ipc_lock.lock_irqsave();
    auto* pending = find_pending_pipe_delivery_locked(home_node, resource_id);
    if (pending == nullptr) {
        pending = new PendingPipeDelivery();
        if (pending == nullptr) {
            s_ipc_lock.unlock_irqrestore(irqf);
            return false;
        }
        pending->home_node = home_node;
        pending->resource_id = resource_id;
        g_pending_pipe_deliveries.push_back(pending);
    }
    pending->write_closed = true;
    s_ipc_lock.unlock_irqrestore(irqf);
    return true;
}

static auto drain_pending_pipe_data(uint16_t home_node, uint32_t resource_id, void* buf, size_t count) -> size_t {
    if (buf == nullptr || count == 0) {
        return 0;
    }

    uint64_t irqf = s_ipc_lock.lock_irqsave();
    auto* pending = find_pending_pipe_delivery_locked(home_node, resource_id);
    if (pending == nullptr || pending->chunks.empty()) {
        s_ipc_lock.unlock_irqrestore(irqf);
        return 0;
    }

    auto& chunk = pending->chunks.front();
    uint16_t remaining = static_cast<uint16_t>(chunk.len - chunk.offset);
    size_t to_copy = count < remaining ? count : remaining;
    std::memcpy(buf, chunk.data + chunk.offset, to_copy);
    chunk.offset = static_cast<uint16_t>(chunk.offset + to_copy);
    pending->buffered_bytes -= static_cast<uint32_t>(to_copy);

    if (chunk.offset == chunk.len) {
        delete[] chunk.data;
        pending->chunks.pop_front();
    }

    if (pending->chunks.empty()) {
        erase_pending_pipe_delivery_locked(pending);
        s_ipc_lock.unlock_irqrestore(irqf);
        free_pending_pipe_delivery(pending);
        return to_copy;
    }

    s_ipc_lock.unlock_irqrestore(irqf);
    return to_copy;
}

static void proxy_mark_pipe_closed(ProxyIpcState* proxy, uint32_t resource_id) {
    if (proxy == nullptr) {
        return;
    }

    (void)resource_id;

    auto* reader = static_cast<ker::mod::sched::task::Task*>(nullptr);
    uint64_t proxy_irqf = proxy->lock.lock_irqsave();
    proxy->write_closed.store(true, std::memory_order_release);
    reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
    proxy->lock.unlock_irqrestore(proxy_irqf);

#ifdef WKI_IPC_DEBUG
    ker::mod::dbg::log("[WKI] IPC proxy EOF received: resource_id=%u reader_pid=0x%lx", resource_id, reader != nullptr ? reader->pid : 0UL);
#endif

    if (reader != nullptr) {
        reader->deferredTaskSwitch = false;
        reader->wait_channel = nullptr;
        ker::mod::sched::kern_wake(reader);
    }
}

static void wake_proxy_reader(ProxyIpcState* proxy) {
    if (proxy == nullptr) {
        return;
    }

    auto* reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
    if (reader != nullptr) {
        reader->deferredTaskSwitch = false;
        reader->wait_channel = nullptr;
        ker::mod::sched::kern_wake(reader);
    }
}

auto proxy_pipe_read(ker::vfs::File* f, void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) return -EBADF;
    if (proxy->ring_buf == nullptr) return -EIO;

    // Spin briefly then block
    for (;;) {
        // Spin briefly then block
        for (int spin = 0; spin < 2000; spin++) {
            uint32_t head = proxy->ring_head.load(std::memory_order_acquire);
            uint32_t tail = proxy->ring_tail.load(std::memory_order_relaxed);
            uint32_t avail = head - tail;

            if (avail > 0) {
                uint32_t to_read = count < avail ? static_cast<uint32_t>(count) : avail;
                uint32_t cap = proxy->ring_capacity;
                uint32_t ring_tail = tail % cap;
                auto* dst = static_cast<uint8_t*>(buf);

                uint32_t first = cap - ring_tail;
                if (first >= to_read) {
                    std::memcpy(dst, proxy->ring_buf + ring_tail, to_read);
                } else {
                    std::memcpy(dst, proxy->ring_buf + ring_tail, first);
                    std::memcpy(dst + first, proxy->ring_buf, to_read - first);
                }
                proxy->ring_tail.store(tail + to_read, std::memory_order_release);
                return static_cast<ssize_t>(to_read);
            }

            size_t queued = drain_pending_pipe_data(proxy->home_node, proxy->resource_id, buf, count);
            if (queued > 0) {
                return static_cast<ssize_t>(queued);
            }

            if (proxy->write_closed.load(std::memory_order_acquire)) {
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
        uint64_t pending_irqf = s_ipc_lock.lock_irqsave();
        uint64_t irqf = proxy->lock.lock_irqsave();
        uint32_t head = proxy->ring_head.load(std::memory_order_acquire);
        uint32_t tail = proxy->ring_tail.load(std::memory_order_relaxed);
        if (head != tail) {
            proxy->lock.unlock_irqrestore(irqf);
            s_ipc_lock.unlock_irqrestore(pending_irqf);
            continue;
        }
        auto* pending = find_pending_pipe_delivery_locked(proxy->home_node, proxy->resource_id);
        if (pending != nullptr && !pending->chunks.empty()) {
            proxy->lock.unlock_irqrestore(irqf);
            s_ipc_lock.unlock_irqrestore(pending_irqf);
            continue;
        }
        if (proxy->write_closed.load(std::memory_order_acquire)) {
            proxy->lock.unlock_irqrestore(irqf);
            s_ipc_lock.unlock_irqrestore(pending_irqf);
            return 0;
        }

        task->wait_channel = "wki_proxy_pipe";
        task->deferredTaskSwitch = true;
        proxy->blocked_reader.store(task, std::memory_order_release);
        proxy->lock.unlock_irqrestore(irqf);
        s_ipc_lock.unlock_irqrestore(pending_irqf);
        return -512;  // ERESTARTSYS
    }
}

auto proxy_pipe_write(ker::vfs::File* f, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr || !proxy->active) return -EBADF;

    // Send data via wire message to home node
    // Message format: [DevOpReqPayload(4B)] [resource_id:u32] [data...]
    constexpr size_t HEADER_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    size_t max_chunk = 4096;  // reasonable chunk size per message

    size_t to_send = count < max_chunk ? count : max_chunk;
    size_t msg_size = HEADER_SIZE + to_send;
    auto* msg = new (std::nothrow) uint8_t[msg_size];
    if (msg == nullptr) return -ENOMEM;

    auto* req = reinterpret_cast<DevOpReqPayload*>(msg);
    req->op_id = OP_PIPE_DATA;
    req->data_len = static_cast<uint16_t>(sizeof(uint32_t) + to_send);

    auto* res_id_ptr = reinterpret_cast<uint32_t*>(msg + sizeof(DevOpReqPayload));
    *res_id_ptr = proxy->resource_id;

    std::memcpy(msg + HEADER_SIZE, buf, to_send);

    int ret = wki_send(proxy->home_node, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(msg_size));
    delete[] msg;

    if (ret != WKI_OK) return -EIO;
    return static_cast<ssize_t>(to_send);
}

auto proxy_pipe_close(ker::vfs::File* f) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr) return 0;

    // Send close to home node with resource_id
    uint16_t op = (f->fops != nullptr && f->fops->vfs_write == nullptr) ? OP_PIPE_CLOSE_READ : OP_PIPE_CLOSE_WRITE;

    constexpr size_t HEADER_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    uint8_t msg[HEADER_SIZE];
    auto* req = reinterpret_cast<DevOpReqPayload*>(msg);
    req->op_id = op;
    req->data_len = sizeof(uint32_t);
    auto* res_id_ptr = reinterpret_cast<uint32_t*>(msg + sizeof(DevOpReqPayload));
    *res_id_ptr = proxy->resource_id;

    if (proxy->home_node != WKI_NODE_INVALID) {
        wki_send(proxy->home_node, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(HEADER_SIZE));
    }

    bool dropped_registry_ref = false;
    {
        uint64_t irqf = s_ipc_lock.lock_irqsave();
        proxy->active.store(false, std::memory_order_release);
        dropped_registry_ref = proxy_unregister_locked(proxy);
        s_ipc_lock.unlock_irqrestore(irqf);
    }

    // Wake any task blocked in proxy_pipe_read so it doesn't sleep forever.
    auto* reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
    if (reader != nullptr) {
        reader->deferredTaskSwitch = false;
        reader->wait_channel = nullptr;
        ker::mod::sched::kern_wake(reader);
    }

    f->private_data = nullptr;
    if (dropped_registry_ref) {
        proxy_release(proxy);
    }
    proxy_release(proxy);
    return 0;
}

auto proxy_pipe_poll_check(ker::vfs::File* f, int events) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr) return 0;

    int ready = 0;
    uint32_t head = proxy->ring_head.load(std::memory_order_acquire);
    uint32_t tail = proxy->ring_tail.load(std::memory_order_acquire);
    uint32_t avail = head - tail;
    bool wr_closed = proxy->write_closed.load(std::memory_order_acquire);

    if (f->fops != nullptr && f->fops->vfs_write == nullptr) {
        // Read end
        if ((events & 0x0001) && (avail > 0 || wr_closed)) ready |= 0x0001;  // POLLIN
        if (wr_closed && avail == 0) ready |= 0x0010;                        // POLLHUP
    } else {
        // Write end — always writable (sends via wire message)
        if (events & 0x0004) ready |= 0x0004;  // POLLOUT
    }
    return ready;
}

auto proxy_pipe_poll_register_waiter(ker::vfs::File* /*f*/, uint64_t /*pid*/) -> bool {
    // For now, rely on the doorbell wake mechanism
    return true;
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

auto find_export_by_resource_id(uint32_t resource_id) -> WkiIpcExport* {
    for (auto* exp : g_ipc_exports) {
        if (exp->active && exp->resource_id == resource_id) return exp;
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

template <int SLOT>
[[noreturn]] static void pipe_pump_thread_fn() {
    auto* arg = &g_pump_args[SLOT];
    constexpr uint64_t IDLE_SLEEP_US = 60'000'000;

    for (;;) {
        auto* exp = arg->exp.load(std::memory_order_acquire);
        if (exp == nullptr) {
            ker::mod::sched::kern_sleep_us(IDLE_SLEEP_US);
            continue;
        }

        auto* file = exp->file;
        uint16_t target = exp->consumer_node;
        uint32_t resource_id = exp->resource_id;

        constexpr size_t BUF_SIZE = 4096;
        constexpr size_t HEADER_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
        auto* buf = new (std::nothrow) uint8_t[BUF_SIZE];
        auto* msg = new (std::nothrow) uint8_t[HEADER_SIZE + BUF_SIZE];
        if (buf == nullptr || msg == nullptr) {
            if (buf) delete[] buf;
            if (msg) delete[] msg;
            exp->pump_running.store(false, std::memory_order_release);
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
                auto* res_id_ptr = reinterpret_cast<uint32_t*>(msg + sizeof(DevOpReqPayload));
                *res_id_ptr = resource_id;
                std::memcpy(msg + HEADER_SIZE, buf, static_cast<size_t>(n));

                int ret = WKI_ERR_TX_FAILED;
                while (exp->active && exp->pump_running.load(std::memory_order_acquire)) {
                    ret = wki_send(target, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(HEADER_SIZE + n));
                    if (ret == WKI_OK) {
                        break;
                    }
                    ker::mod::sched::kern_sleep_us(ret == WKI_ERR_NO_CREDITS ? 200 : 1000);
                }
                if (ret != WKI_OK) {
                    ker::mod::dbg::log("[WKI] IPC pipe pump DATA send failed: resource_id=%u ret=%d len=%ld", resource_id, ret, n);
                    break;
                }
            } else if (n == 0) {
                auto* req = reinterpret_cast<DevOpReqPayload*>(msg);
                req->op_id = OP_PIPE_CLOSE_WRITE;
                req->data_len = sizeof(uint32_t);
                auto* res_id_ptr = reinterpret_cast<uint32_t*>(msg + sizeof(DevOpReqPayload));
                *res_id_ptr = resource_id;
                int ret = WKI_ERR_TX_FAILED;
                uint32_t retries = 0;
                while (exp->active && exp->pump_running.load(std::memory_order_acquire)) {
                    ret = wki_send(target, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(HEADER_SIZE));
                    if (ret == WKI_OK) {
                        break;
                    }
                    retries++;
                    ker::mod::sched::kern_sleep_us(ret == WKI_ERR_NO_CREDITS ? 200 : 1000);
                }
                if (ret != WKI_OK) {
                    ker::mod::dbg::log("[WKI] IPC pipe pump EOF send failed: resource_id=%u ret=%d retries=%u", resource_id, ret, retries);
                }
#ifdef WKI_IPC_DEBUG
                else {
                    ker::mod::dbg::log("[WKI] IPC pipe pump EOF: resource_id=%u retries=%u", resource_id, retries);
                }
#endif
                break;
            } else if (n == -512) {
                ker::mod::sched::kern_sleep_us(100);
                continue;
            } else {
                ker::mod::dbg::log("[WKI] IPC pipe pump read error: resource_id=%u err=%ld", resource_id, n);
                break;
            }
        }

        delete[] buf;
        delete[] msg;
        exp->pump_running.store(false, std::memory_order_release);
        arg->exp.store(nullptr, std::memory_order_release);
    }
}

using PumpFn = void (*)();
static constexpr PumpFn g_pump_fns[MAX_PUMPS] = {
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
        task = ker::mod::sched::task::Task::createKernelThread("wki_pipe_pump", g_pump_fns[slot]);
        if (task == nullptr) {
            exp->pump_running.store(false, std::memory_order_release);
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

}  // namespace

// =============================================================================
// Public API: Initialization
// =============================================================================

void wki_ipc_subsystem_init() {
    if (g_ipc_initialized) return;
    g_ipc_initialized = true;
    g_export_pipe_write_flush_task = ker::mod::sched::task::Task::createKernelThread("wki_pipe_write", export_pipe_write_flush_thread_fn);
    if (g_export_pipe_write_flush_task != nullptr) {
        ker::mod::sched::post_task_balanced(g_export_pipe_write_flush_task);
    } else {
        ker::mod::dbg::log("[WKI] IPC export pipe writer thread creation failed");
    }
    ker::mod::dbg::log("[WKI] IPC proxy subsystem initialized");
}

// =============================================================================
// Public API: Export task fds for remote submission
// =============================================================================

auto wki_ipc_export_task_fds(ker::mod::sched::task::Task* task, uint16_t target_node, WkiIpcFdEntry* map_out, uint16_t* count_out) -> bool {
    if (task == nullptr || map_out == nullptr || count_out == nullptr) return false;

    uint16_t count = 0;
    constexpr uint16_t MAX_IPC_FDS = 16;

    task->fd_table.for_each([&](uint64_t fd_key, void* val) {
        if (val == nullptr || count >= MAX_IPC_FDS) return;
        auto* file = static_cast<ker::vfs::File*>(val);

        ResourceType res_type = ResourceType::CUSTOM;
        if (ker::vfs::vfs_is_pipe_file(file)) {
            res_type = ResourceType::IPC_PIPE;
        } else if (ker::vfs::vfs_is_socket_file(file)) {
            res_type = ResourceType::IPC_SOCKET;
        } else if (ker::vfs::vfs_is_epoll_file(file)) {
            return;  // Epoll instances stay pinned
        } else {
            return;
        }

        uint64_t irqf = s_ipc_lock.lock_irqsave();
        uint32_t resource_id = allocate_ipc_resource_id();

        auto* exp = new WkiIpcExport();
        exp->active = true;
        exp->resource_id = resource_id;
        exp->res_type = res_type;
        exp->file = file;
        file->refcount.fetch_add(1, std::memory_order_relaxed);
        exp->consumer_node = target_node;
        exp->assigned_channel = WKI_CHAN_RESOURCE;

        g_ipc_exports.push_back(exp);
        s_ipc_lock.unlock_irqrestore(irqf);

        // Start pump thread for pipe read-ends (data flows home->consumer)
        if (res_type == ResourceType::IPC_PIPE && file->open_flags == 0) {
            start_pipe_pump(exp);
        }

        auto& entry = map_out[count];
        entry.local_fd = static_cast<uint16_t>(fd_key);
        entry.res_type = static_cast<uint16_t>(res_type);
        entry.resource_id = resource_id;
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
    if (task == nullptr || map == nullptr || count == 0) return;

    for (uint16_t i = 0; i < count; i++) {
        const auto& entry = map[i];
        auto* proxy = new ProxyIpcState();
        proxy->active = true;
        proxy->res_type = static_cast<ResourceType>(entry.res_type);
        proxy->home_node = entry.home_node;
        proxy->resource_id = entry.resource_id;
        proxy->assigned_channel = WKI_CHAN_RESOURCE;

        // Allocate local ring buffer for pipe read proxies
        if (proxy->res_type == ResourceType::IPC_PIPE) {
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
            proxy->write_closed.store(false, std::memory_order_relaxed);
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
            int open_flags = static_cast<int>(entry.reserved1 & WKI_IPC_FD_ACCESS_MASK);
            proxy_file->open_flags = open_flags;
            if (open_flags == 0) {
                proxy_file->fops = &g_proxy_pipe_read_fops;
            } else {
                proxy_file->fops = &g_proxy_pipe_write_fops;
            }
        } else if (proxy->res_type == ResourceType::IPC_SOCKET) {
            proxy_file->fops = &g_proxy_pipe_read_fops;
            proxy_file->open_flags = 0;
            proxy_file->fs_type = ker::vfs::FSType::SOCKET;
        } else {
            proxy_file->fops = &g_proxy_pipe_read_fops;
            proxy_file->open_flags = 0;
        }

        if (existing_file != nullptr) {
            task->fd_table.remove(entry.local_fd);
        }
        static_cast<void>(task->fd_table.insert(entry.local_fd, proxy_file));

        bool pending_write_closed = false;
        uint64_t irqf = s_ipc_lock.lock_irqsave();
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
        s_ipc_lock.unlock_irqrestore(irqf);

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

// =============================================================================
// DEV_OP response handler for IPC control ops
// =============================================================================

void wki_ipc_handle_dev_op_resp(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len) {
    (void)src_node;
    (void)channel;

    if (len < sizeof(DevOpRespPayload)) return;

    auto* resp = reinterpret_cast<const DevOpRespPayload*>(payload);
    uint16_t op_id = resp->op_id;

    // Handle pipe close acknowledgement
    if (op_id == OP_PIPE_CLOSE_READ || op_id == OP_PIPE_CLOSE_WRITE) {
        // Close acknowledged — no further action needed
        return;
    }
}

// =============================================================================
// Peer cleanup
// =============================================================================

void wki_ipc_cleanup_for_peer(uint16_t node_id) {
    uint64_t irqf = s_ipc_lock.lock_irqsave();

    // Clean up exports for this peer
    for (auto* exp : g_ipc_exports) {
        if (exp->active && exp->consumer_node == node_id) {
            exp->pump_running.store(false, std::memory_order_release);
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

    ProxyIpcState* detached_proxies[WKI_IPC_MAX_PROXIES]{};
    size_t detached_proxy_count = 0;
    PendingPipeDelivery* detached_pending[WKI_IPC_MAX_EXPORTS]{};
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

    s_ipc_lock.unlock_irqrestore(irqf);

    if (wake_export_pipe_flush && g_export_pipe_write_flush_task != nullptr) {
        ker::mod::sched::kern_wake(g_export_pipe_write_flush_task);
    }

    for (size_t i = 0; i < detached_proxy_count; i++) {
        auto* proxy = detached_proxies[i];
        auto* reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
        if (reader != nullptr) {
            reader->deferredTaskSwitch = false;
            reader->wait_channel = nullptr;
            ker::mod::sched::kern_wake(reader);
        }
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

void handle_ipc_dev_op_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevOpReqPayload)) return;

    auto* req = reinterpret_cast<const DevOpReqPayload*>(payload);
    uint16_t op_id = req->op_id;

    // All IPC ops carry resource_id as first 4 bytes of data
    if (req->data_len < sizeof(uint32_t)) return;
    const auto* data = payload + sizeof(DevOpReqPayload);
    uint32_t resource_id = *reinterpret_cast<const uint32_t*>(data);
    const auto* op_data = data + sizeof(uint32_t);
    uint16_t op_data_len = req->data_len - sizeof(uint32_t);

    if (op_id == OP_PIPE_DATA) {
        // Consumer receives pipe data — write into the local proxy ring if
        // present, otherwise route it to the exported home-side pipe.
        uint64_t irqf = s_ipc_lock.lock_irqsave();
        auto* proxy = find_proxy_by_resource_id(resource_id);
        bool pending_buffered = find_pending_pipe_delivery_locked(hdr->src_node, resource_id) != nullptr;
        ker::vfs::File* export_file = nullptr;
        if (proxy == nullptr) {
            export_file = ipc_acquire_export_file_locked(resource_id);
        }
        s_ipc_lock.unlock_irqrestore(irqf);

        if (proxy != nullptr) {
            if (op_data_len == 0) {
                proxy_release(proxy);
                return;
            }

            if (pending_buffered || proxy->ring_buf == nullptr) {
                if (!queue_pending_pipe_data(hdr->src_node, resource_id, op_data, op_data_len)) {
                    ker::mod::dbg::log("[WKI] IPC pending pipe DATA queue failed: resource_id=%u len=%u", resource_id, op_data_len);
                } else {
                    wake_proxy_reader(proxy);
                }
                proxy_release(proxy);
                return;
            }

            auto* reader = static_cast<ker::mod::sched::task::Task*>(nullptr);
            bool queue_overflow = false;
            uint64_t proxy_irqf = proxy->lock.lock_irqsave();
            if (!proxy->active.load(std::memory_order_acquire) || proxy->ring_buf == nullptr) {
                proxy->lock.unlock_irqrestore(proxy_irqf);
                if (!queue_pending_pipe_data(hdr->src_node, resource_id, op_data, op_data_len)) {
                    ker::mod::dbg::log("[WKI] IPC pending pipe DATA queue failed: resource_id=%u len=%u", resource_id, op_data_len);
                } else {
                    wake_proxy_reader(proxy);
                }
                proxy_release(proxy);
                return;
            }

            uint32_t head = proxy->ring_head.load(std::memory_order_relaxed);
            uint32_t tail = proxy->ring_tail.load(std::memory_order_acquire);
            uint32_t cap = proxy->ring_capacity;
            uint32_t free_space = cap - (head - tail);

            if (op_data_len > free_space) {
                queue_overflow = true;
            } else {
                uint32_t ring_head = head % cap;
                uint32_t first = cap - ring_head;
                if (first >= op_data_len) {
                    std::memcpy(proxy->ring_buf + ring_head, op_data, op_data_len);
                } else {
                    std::memcpy(proxy->ring_buf + ring_head, op_data, first);
                    std::memcpy(proxy->ring_buf, op_data + first, op_data_len - first);
                }
                proxy->ring_head.store(head + op_data_len, std::memory_order_release);

                reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
            }
            proxy->lock.unlock_irqrestore(proxy_irqf);

            if (queue_overflow) {
                if (!queue_pending_pipe_data(hdr->src_node, resource_id, op_data, op_data_len)) {
                    ker::mod::dbg::log("[WKI] IPC pending pipe DATA queue failed: resource_id=%u len=%u", resource_id, op_data_len);
                } else {
                    wake_proxy_reader(proxy);
                }
                proxy_release(proxy);
                return;
            }

            if (reader != nullptr) {
                reader->deferredTaskSwitch = false;
                reader->wait_channel = nullptr;
                ker::mod::sched::kern_wake(reader);
            }
            proxy_release(proxy);
            return;
        }

        if (export_file == nullptr) {
            if (op_data_len > 0 && !queue_pending_pipe_data(hdr->src_node, resource_id, op_data, op_data_len)) {
                ker::mod::dbg::log("[WKI] IPC pending pipe DATA queue failed: resource_id=%u len=%u", resource_id, op_data_len);
            }
            return;
        }
        if (op_data_len == 0) {
            ipc_release_file_ref(export_file);
            return;
        }
        if (export_file->fops != nullptr && export_file->fops->vfs_write != nullptr) {
            ssize_t write_ret = export_file->fops->vfs_write(export_file, op_data, op_data_len, static_cast<size_t>(export_file->pos));
            if (write_ret != static_cast<ssize_t>(op_data_len)) {
                uint16_t written = 0;
                if (write_ret > 0) {
                    written = static_cast<uint16_t>(write_ret < op_data_len ? write_ret : op_data_len);
                } else if (write_ret != WKI_IPC_PIPE_RESTARTSYS && write_ret != -EAGAIN) {
                    ker::mod::dbg::log("[WKI] IPC export pipe immediate write failed: resource_id=%u ret=%ld len=%u", resource_id,
                                       write_ret, op_data_len);
                }

                uint64_t export_irqf = s_ipc_lock.lock_irqsave();
                auto* exp = find_export_by_resource_id(resource_id);
                s_ipc_lock.unlock_irqrestore(export_irqf);
                if (exp == nullptr || !queue_export_pipe_write_data(exp, op_data + written, static_cast<uint16_t>(op_data_len - written))) {
                    ker::mod::dbg::log("[WKI] IPC export pipe backlog queue failed: resource_id=%u written=%u total=%u", resource_id,
                                       written, op_data_len);
                }
            }
        }
        ipc_release_file_ref(export_file);
        return;
    }

    if (op_id == OP_PIPE_CLOSE_WRITE) {
        // Server-side: consumer closed write end — close the real pipe's read end
        // Client-side: home node reports write-end closed (EOF)
        uint64_t irqf = s_ipc_lock.lock_irqsave();

        // Check consumer-side proxy first
        auto* proxy = find_proxy_by_resource_id(resource_id);
        if (proxy != nullptr) {
            s_ipc_lock.unlock_irqrestore(irqf);

            proxy_mark_pipe_closed(proxy, resource_id);
            proxy_release(proxy);
            return;
        }

        // Check server-side export
        auto* exp = find_export_by_resource_id(resource_id);
        if (exp != nullptr && exp->active && exp->file != nullptr) {
            s_ipc_lock.unlock_irqrestore(irqf);
            if (!mark_export_pipe_write_closed(exp)) {
                ker::mod::dbg::log("[WKI] IPC export pipe close queue failed: resource_id=%u", resource_id);
            }
            return;
        }
        s_ipc_lock.unlock_irqrestore(irqf);

        if (!mark_pending_pipe_closed(hdr->src_node, resource_id)) {
            ker::mod::dbg::log("[WKI] IPC pending EOF queue failed: resource_id=%u", resource_id);
        }
        return;
    }

    if (op_id == OP_PIPE_CLOSE_READ) {
        // Consumer closed read end — stop the pump, release the export's file ref.
        uint64_t irqf = s_ipc_lock.lock_irqsave();
        auto* exp = find_export_by_resource_id(resource_id);
        if (exp != nullptr && exp->active) {
            exp->pump_running.store(false, std::memory_order_release);
            ker::vfs::File* f = exp->file;
            exp->file = nullptr;
            exp->active = false;
            s_ipc_lock.unlock_irqrestore(irqf);
            if (f != nullptr && f->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                if (f->fops != nullptr && f->fops->vfs_close != nullptr) {
                    f->fops->vfs_close(f);
                }
                delete f;
            }
            return;
        }
        s_ipc_lock.unlock_irqrestore(irqf);
        return;
    }

    (void)hdr;
}

void handle_ipc_dev_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    wki_ipc_handle_dev_op_resp(hdr->src_node, hdr->channel_id, payload, payload_len);
}

}  // namespace detail

// =============================================================================
// Public: incoming IPC DEV_OP_REQ dispatch (called from wki.cpp)
// =============================================================================

void wki_ipc_handle_dev_op_req(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len) {
    if (len < sizeof(DevOpReqPayload)) return;
    auto* req = reinterpret_cast<const DevOpReqPayload*>(payload);

    // Route IPC ops (0x0700 - 0x07FF)
    if (req->op_id >= 0x0700 && req->op_id <= 0x07FF) {
        WkiHeader hdr = {};
        hdr.src_node = src_node;
        hdr.channel_id = channel;
        detail::handle_ipc_dev_op_req(&hdr, payload, len);
    }
}

}  // namespace ker::net::wki
