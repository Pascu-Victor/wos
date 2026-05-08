#include "remote_vfs.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <memory>
#include <net/wki/dev_proxy.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <vfs/mount.hpp>
#include <vfs/vfs.hpp>

#include "platform/sys/spinlock.hpp"
#include "vfs/file.hpp"
#include "vfs/file_operations.hpp"
#include "vfs/stat.hpp"

namespace ker::net::wki {

// -------------------------------------------------------------------------------
// Storage
// -------------------------------------------------------------------------------

namespace {

auto transport_supports_vfs_write_push_rdma(const WkiTransport* transport) -> bool {
    if (transport == nullptr || transport->name == nullptr) {
        return false;
    }

    // ivshmem: rdma_write is a local memcpy — payload is visible before the control
    // response is even enqueued.
    //
    // RoCE: rdma_write and wki_send both use the same netdev TX queue, so RDMA_WRITE
    // frames are enqueued before the WKI control message. Write-push is therefore
    // safe for RoCE.
    return std::strcmp(transport->name, "wki-ivshmem") == 0 || std::strcmp(transport->name, "wki-roce") == 0;
}

auto transport_supports_vfs_read_push_rdma(const WkiTransport* transport) -> bool {
    if (transport == nullptr || transport->name == nullptr) {
        return false;
    }

    // In read-push mode the server has already written into our local RDMA
    // region, and the consumer-side "rdma_read" below is just a local sync/copy.
    // That is true for ivshmem, but not for RoCE: a RoCE rdma_read would issue a
    // network read against neighbor 0/local rkey, fail, and leave stale zeros in
    // the bounce buffer. RoCE reads must use pull-mode staging or message I/O.
    return std::strcmp(transport->name, "wki-ivshmem") == 0;
}

// Returns true for transports where the safe VFS read model is client-pull:
// server stages data locally, client issues rdma_read (synchronous, spin-waits
// for DOORBELL completion) to fetch it after receiving the control response.
auto transport_supports_rdma_read_pull(const WkiTransport* transport) -> bool {
    if (transport == nullptr || transport->name == nullptr) {
        return false;
    }
    return std::strcmp(transport->name, "wki-roce") == 0;
}

auto perf_current_pid() -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr ? task->pid : 0;
}

auto perf_current_cpu() -> uint32_t {
    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr ? static_cast<uint32_t>(task->cpu) : 0U;
}

auto take_preserved_export_id(std::deque<VfsExport>* stale_exports, const char* name) -> uint32_t {
    if (stale_exports == nullptr || name == nullptr) {
        return 0;
    }

    for (auto it = stale_exports->begin(); it != stale_exports->end(); ++it) {
        if (it->active && std::strncmp(static_cast<const char*>(it->name), name, VFS_EXPORT_NAME_LEN) == 0) {
            uint32_t resource_id = it->resource_id;
            stale_exports->erase(it);
            return resource_id;
        }
    }

    return 0;
}

auto max_preserved_export_id(const std::deque<VfsExport>& stale_exports) -> uint32_t {
    uint32_t max_resource_id = 0;
    for (const auto& exp : stale_exports) {
        if (exp.active && exp.resource_id > max_resource_id) {
            max_resource_id = exp.resource_id;
        }
    }
    return max_resource_id;
}

auto perf_vfs_op(uint16_t op_id) -> uint8_t {
    switch (op_id) {
        case OP_VFS_OPEN:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::OPEN);
        case OP_VFS_STAT:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::STAT);
        case OP_VFS_CLOSE:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::CLOSE);
        case OP_VFS_MKDIR:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::MKDIR);
        case OP_VFS_WRITE:
        case OP_VFS_WRITE_RDMA:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::WRITE);
        case OP_VFS_READ_RDMA:
        case OP_VFS_READ_BULK:
        case OP_VFS_READ:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::READ);
        case OP_VFS_READDIR_BATCH:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::READDIR);
        case OP_VFS_SEEK_END:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::SEEK);
        case OP_VFS_TRUNCATE:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::TRUNCATE);
        case OP_VFS_UNLINK:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::UNLINK);
        case OP_VFS_RMDIR:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::RMDIR);
        case OP_VFS_RENAME:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::RENAME);
        case OP_VFS_READLINK:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::READLINK);
        default:
            return static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::RETRY);
    }
}

auto perf_vfs_bytes(uint16_t op_id, uint16_t req_len, uint16_t resp_len) -> uint64_t {
    switch (op_id) {
        case OP_VFS_READ:
        case OP_VFS_READ_RDMA:
        case OP_VFS_READ_BULK:
        case OP_VFS_READDIR_BATCH:
        case OP_VFS_READLINK:
        case OP_VFS_STAT:
            return resp_len;
        case OP_VFS_WRITE:
        case OP_VFS_WRITE_RDMA:
            return req_len;
        default:
            return std::max<uint16_t>(req_len, resp_len);
    }
}

void perf_record_vfs_point(uint8_t op, uint16_t peer, uint16_t channel, int32_t status, uint32_t aux, uint64_t callsite) {
    if (!ker::mod::perf::is_wki_recording_enabled()) {
        return;
    }

    uint32_t correlation = ker::mod::perf::next_wki_trace_correlation();
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS, op,
                                     ker::mod::perf::WkiPerfPhase::POINT, peer, channel, correlation, status, aux, callsite);
    uint32_t latency_us = (op == static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::ATTACH_WAIT) ||
                           op == static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::PROXY_WAIT))
                              ? aux
                              : 0U;
    bool has_latency = op == static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::ATTACH_WAIT) ||
                       op == static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::PROXY_WAIT);
    uint32_t retries = (op == static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::RETRY)) ? 1U : 0U;
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, op, peer, channel, status, latency_us, has_latency,
                                       retries, 0);
}

// -----------------------------------------------------------------------------
// Server side
// -----------------------------------------------------------------------------

std::deque<VfsExport> g_vfs_exports;       // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::deque<RemoteVfsFd> g_remote_fds;      // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
int32_t g_next_remote_fd = 1;              // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_next_vfs_resource_id = 0x1000;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto find_remote_fd(uint16_t consumer_node, uint16_t channel_id, int32_t fd_id) -> RemoteVfsFd* {
    for (auto& rfd : g_remote_fds) {
        if (rfd.active && rfd.consumer_node == consumer_node && rfd.channel_id == channel_id && rfd.fd_id == fd_id) {
            return &rfd;
        }
    }
    return nullptr;
}

auto alloc_remote_fd(uint16_t consumer_node, uint16_t channel_id, ker::vfs::File* file) -> int32_t {
    int32_t fd_id = g_next_remote_fd++;

    RemoteVfsFd rfd;
    rfd.active = true;
    rfd.consumer_node = consumer_node;
    rfd.channel_id = channel_id;
    rfd.fd_id = fd_id;
    rfd.file = file;
    rfd.last_activity_us = wki_now_us();

    g_remote_fds.push_back(rfd);
    return fd_id;
}

// D10: Update last_activity_us on a remote FD
void touch_remote_fd(RemoteVfsFd* rfd) {
    if (rfd != nullptr) {
        rfd->last_activity_us = wki_now_us();
    }
}

// Build full absolute path from export_path + relative_path
void build_full_path(char* out, size_t out_size, const char* export_path, const char* relative_path, uint16_t rel_len) {
    size_t export_len = strlen(export_path);

    // Copy export path
    size_t pos = 0;
    if (export_len > 0 && export_len < out_size - 1) {
        memcpy(out, export_path, export_len);
        pos = export_len;
    }

    // Add separator if needed
    if (pos > 0 && out[pos - 1] != '/' && rel_len > 0) {
        if (pos < out_size - 1) {
            out[pos++] = '/';
        }
    }

    // Copy relative path
    size_t copy_len = rel_len;
    if (pos + copy_len >= out_size) {
        copy_len = out_size - pos - 1;
    }
    if (copy_len > 0) {
        memcpy(out + pos, relative_path, copy_len);
        pos += copy_len;
    }

    out[pos] = '\0';
}

void build_export_name(char* out, size_t out_size, const char* export_path) {
    if (out == nullptr || out_size == 0 || export_path == nullptr) {
        return;
    }

    const char* visible_path = export_path;
    if (ker::mod::sched::has_run_queues()) {
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr) {
            size_t root_len = std::strlen(task->root);
            if (root_len > 1 && std::strncmp(export_path, task->root, root_len) == 0 &&
                (export_path[root_len] == '\0' || export_path[root_len] == '/')) {
                visible_path = export_path + root_len;
                if (*visible_path == '\0') {
                    visible_path = "/";
                }
            }
        }
    }

    std::snprintf(out, out_size, "%s", visible_path);
}

// Check if the resolved server-side path would cross into a REMOTE (WKI proxy) mount.
// This prevents recursive proxying: e.g. a client reads /wki/nodeA/wki/nodeB/... which
// would cause the server to proxy through its own WKI mounts, creating a chain that
// times out or loops.
bool path_crosses_remote_mount(const char* path) {
    if (path == nullptr) {
        return false;
    }

    char resolved_path[512] = {};
    const char* mount_path = path;
    if (ker::vfs::resolve_mount_path(path, resolved_path, sizeof(resolved_path)) == 0) {
        mount_path = resolved_path;
    }

    auto* mp = ker::vfs::find_mount_point(mount_path);
    return mp != nullptr && mp->fs_type == ker::vfs::FSType::REMOTE;
}

bool path_crosses_remote_mount_direct(const char* path) {
    if (path == nullptr) {
        return false;
    }

    auto* mp = ker::vfs::find_mount_point(path);
    return mp != nullptr && mp->fs_type == ker::vfs::FSType::REMOTE;
}

bool path_crosses_recursive_self_alias(const char* path) {
    if (path == nullptr) {
        return false;
    }

    constexpr char host_prefix[] = "/wki/host";
    constexpr size_t host_prefix_len = sizeof(host_prefix) - 1;
    if (std::strncmp(path, host_prefix, host_prefix_len) == 0 && (path[host_prefix_len] == '\0' || path[host_prefix_len] == '/')) {
        return true;
    }

    char self_prefix[WKI_HOSTNAME_MAX + 6] = {};
    std::snprintf(self_prefix, sizeof(self_prefix), "/wki/%s", g_wki.local_hostname);
    size_t self_prefix_len = std::strlen(self_prefix);
    return std::strncmp(path, self_prefix, self_prefix_len) == 0 && (path[self_prefix_len] == '\0' || path[self_prefix_len] == '/');
}

bool path_crosses_recursive_wki_boundary(const char* path) {
    return path_crosses_remote_mount(path) || path_crosses_recursive_self_alias(path);
}

bool path_crosses_recursive_wki_boundary_direct(const char* backing_path, const char* visible_path) {
    return path_crosses_remote_mount_direct(backing_path) || path_crosses_recursive_self_alias(visible_path);
}

bool should_hide_recursive_wki_entry(const RemoteFileContext* ctx, const ker::vfs::DirEntry& entry) {
    if (ctx == nullptr || !ctx->hide_recursive_wki_entries) {
        return false;
    }

    if (std::strcmp(entry.d_name.data(), "host") == 0) {
        return true;
    }

    return ctx->recursive_wki_hostname[0] != '\0' && std::strcmp(entry.d_name.data(), ctx->recursive_wki_hostname) == 0;
}

// -----------------------------------------------------------------------------
// Consumer side
// -----------------------------------------------------------------------------

// D7: Directory listing cache - avoids repeated round-trips for readdir()
struct DirCacheEntry {
    ProxyVfsState* proxy = nullptr;
    int32_t remote_fd = -1;
    uint64_t cache_time_us = 0;
    bool complete = false;  // true if we fetched all entries (server returned error/empty)
    std::deque<ker::vfs::DirEntry> entries;

    static constexpr uint64_t STALE_US = 5000000;  // 5 seconds
};

bool try_get_visible_cached_dirent(const DirCacheEntry* cache, const RemoteFileContext* ctx, size_t visible_index,
                                   ker::vfs::DirEntry* out) {
    if (cache == nullptr || out == nullptr) {
        return false;
    }

    size_t current_visible = 0;
    for (const auto& candidate : cache->entries) {
        if (should_hide_recursive_wki_entry(ctx, candidate)) {
            continue;
        }
        if (current_visible == visible_index) {
            *out = candidate;
            return true;
        }
        current_visible++;
    }

    return false;
}

std::deque<DirCacheEntry> g_dir_cache;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto find_dir_cache(ProxyVfsState* proxy, int32_t remote_fd) -> DirCacheEntry* {
    for (auto& dc : g_dir_cache) {
        if (dc.proxy == proxy && dc.remote_fd == remote_fd) {
            return &dc;
        }
    }
    return nullptr;
}

void invalidate_dir_cache(ProxyVfsState* proxy, int32_t remote_fd) {
    std::erase_if(g_dir_cache, [&](const DirCacheEntry& dc) { return dc.proxy == proxy && dc.remote_fd == remote_fd; });
}

std::deque<std::unique_ptr<ProxyVfsState>> g_vfs_proxies;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_remote_vfs_initialized = false;                     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

ker::mod::sys::Spinlock s_vfs_lock;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

struct VfsProxyResponseLookup {
    ProxyVfsState* matched_state = nullptr;
    uint16_t stale_expected_op = 0;
    uint16_t stale_expected_seq = 0;
    size_t active_candidates = 0;
    size_t pending_candidates = 0;
    bool saw_pending_candidate = false;
};

auto find_vfs_proxy_for_response_locked(uint16_t owner_node, uint16_t channel_id, uint16_t resp_op, uint16_t resp_seq)
    -> VfsProxyResponseLookup {
    VfsProxyResponseLookup lookup = {};

    for (auto& p : g_vfs_proxies) {
        if (!p->active || p->owner_node != owner_node || p->assigned_channel != channel_id) {
            continue;
        }

        lookup.active_candidates++;
        if (!p->op_pending.load(std::memory_order_acquire)) {
            continue;
        }

        lookup.pending_candidates++;

        p->lock.lock();
        if (!p->op_pending.load(std::memory_order_acquire)) {
            p->lock.unlock();
            continue;
        }

        bool op_match = resp_op == p->op_expected_id;
        bool seq_match = resp_seq == 0 || resp_seq == p->op_expected_seq;
        if (op_match && seq_match) {
            lookup.matched_state = p.get();
            return lookup;
        }

        if (!lookup.saw_pending_candidate) {
            lookup.stale_expected_op = p->op_expected_id;
            lookup.stale_expected_seq = p->op_expected_seq;
            lookup.saw_pending_candidate = true;
        }

        p->lock.unlock();
    }

    return lookup;
}

auto find_vfs_proxy_by_attach(uint16_t owner_node, uint32_t resource_id) -> ProxyVfsState* {
    for (auto& p : g_vfs_proxies) {
        if (p->attach_pending && p->owner_node == owner_node && p->resource_id == resource_id) {
            return p.get();
        }
    }
    return nullptr;
}

auto find_vfs_proxy_by_mount(const char* mount_path) -> ProxyVfsState* {
    for (auto& p : g_vfs_proxies) {
        if (p->active && strncmp(static_cast<const char*>(p->local_mount_path), mount_path, VFS_EXPORT_PATH_LEN) == 0) {
            return p.get();
        }
    }
    return nullptr;
}

auto peek_channel_tx_seq16(uint16_t owner_node, uint16_t channel_id, uint16_t* seq_out) -> bool {
    if (seq_out == nullptr) {
        return false;
    }

    WkiChannel* ch = wki_channel_get(owner_node, channel_id);
    if (ch == nullptr) {
        return false;
    }

    ch->lock.lock();
    *seq_out = static_cast<uint16_t>(ch->tx_seq & UINT16_MAX);
    ch->lock.unlock();
    return true;
}

constexpr uint64_t VFS_READLINK_CACHE_SUCCESS_RETENTION_US = 100000;
constexpr uint64_t VFS_READLINK_CACHE_NEGATIVE_RETENTION_US = 60000000;

auto readlink_cache_retention_us(int status) -> uint64_t {
    return (status == 0) ? VFS_READLINK_CACHE_SUCCESS_RETENTION_US : VFS_READLINK_CACHE_NEGATIVE_RETENTION_US;
}

void reset_readlink_cache_entry(ProxyVfsState::ReadlinkCacheEntry& entry) {
    entry.valid = false;
    entry.status = 0;
    entry.target_len = 0;
    entry.cached_at_us = 0;
    entry.path[0] = '\0';
    entry.target[0] = '\0';
}

void invalidate_readlink_cache_locked(ProxyVfsState* state) {
    if (state == nullptr) {
        return;
    }

    for (auto& entry : state->readlink_cache) {
        reset_readlink_cache_entry(entry);
    }
}

void invalidate_readlink_cache(ProxyVfsState* state) {
    if (state == nullptr) {
        return;
    }

    state->lock.lock();
    invalidate_readlink_cache_locked(state);
    state->lock.unlock();
}

auto readlink_status_is_cacheable(int status) -> bool {
    return status == 0 || status == -EINVAL || status == -ENOENT || status == -ENOTDIR;
}

auto try_readlink_cache_lookup(ProxyVfsState* state, const char* fs_relative_path, char* buf, size_t bufsize, ssize_t* result_out) -> bool {
    if (state == nullptr || fs_relative_path == nullptr || result_out == nullptr) {
        return false;
    }

    if (std::strlen(fs_relative_path) >= VFS_READLINK_CACHE_TEXT_MAX) {
        return false;
    }

    bool found = false;
    uint64_t now = wki_now_us();
    state->lock.lock();
    for (auto& entry : state->readlink_cache) {
        if (!entry.valid) {
            continue;
        }

        if (now - entry.cached_at_us > readlink_cache_retention_us(entry.status)) {
            reset_readlink_cache_entry(entry);
            continue;
        }

        if (std::strncmp(entry.path.data(), fs_relative_path, VFS_READLINK_CACHE_TEXT_MAX) != 0) {
            continue;
        }

        if (entry.status != 0) {
            *result_out = entry.status;
            found = true;
            break;
        }

        size_t to_copy = std::min(bufsize, static_cast<size_t>(entry.target_len));
        memcpy(buf, entry.target.data(), to_copy);
        *result_out = static_cast<ssize_t>(to_copy);
        found = true;
        break;
    }
    state->lock.unlock();

    return found;
}

void cache_readlink_result(ProxyVfsState* state, const char* fs_relative_path, int status, const char* target, uint16_t target_len) {
    if (state == nullptr || fs_relative_path == nullptr || !readlink_status_is_cacheable(status)) {
        return;
    }

    size_t path_len = std::strlen(fs_relative_path);
    if (path_len >= VFS_READLINK_CACHE_TEXT_MAX) {
        return;
    }

    if (status == 0 && (target == nullptr || target_len == 0 || target_len >= VFS_READLINK_CACHE_TEXT_MAX)) {
        return;
    }

    uint64_t now = wki_now_us();
    state->lock.lock();

    ProxyVfsState::ReadlinkCacheEntry* selected = nullptr;
    ProxyVfsState::ReadlinkCacheEntry* oldest = nullptr;
    for (auto& entry : state->readlink_cache) {
        if (entry.valid && now - entry.cached_at_us > readlink_cache_retention_us(entry.status)) {
            reset_readlink_cache_entry(entry);
        }

        if (!entry.valid) {
            if (selected == nullptr) {
                selected = &entry;
            }
            continue;
        }

        if (std::strncmp(entry.path.data(), fs_relative_path, VFS_READLINK_CACHE_TEXT_MAX) == 0) {
            selected = &entry;
            break;
        }

        if (oldest == nullptr || entry.cached_at_us < oldest->cached_at_us) {
            oldest = &entry;
        }
    }

    if (selected == nullptr) {
        selected = oldest;
    }
    if (selected != nullptr) {
        reset_readlink_cache_entry(*selected);
        selected->valid = true;
        selected->status = static_cast<int16_t>(status);
        selected->cached_at_us = now;
        memcpy(selected->path.data(), fs_relative_path, path_len + 1);
        if (status == 0) {
            selected->target_len = target_len;
            memcpy(selected->target.data(), target, target_len);
        }
    }

    state->lock.unlock();
}

// Helper: send DEV_OP_REQ and spin-wait for response
auto vfs_proxy_send_and_wait(ProxyVfsState* state, uint16_t op_id, const uint8_t* req_data, uint16_t req_data_len, void* resp_buf,
                             uint16_t resp_buf_max, uint16_t* resp_len_out = nullptr, uint64_t wait_timeout_us = WKI_OP_TIMEOUT_US) -> int {
    uint32_t correlation = ker::mod::perf::next_wki_trace_correlation();
    uint64_t callsite = WOS_PERF_CALLSITE();
    uint64_t proxy_wait_start = wki_now_us();

    if (resp_len_out != nullptr) {
        *resp_len_out = 0;
    }

    auto req_total = static_cast<uint16_t>(sizeof(DevOpReqPayload) + req_data_len);
    auto* req_buf = new (std::nothrow) uint8_t[req_total];
    if (req_buf == nullptr) {
        return -1;
    }

    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf);
    req->op_id = op_id;
    req->data_len = req_data_len;

    if (req_data_len > 0 && req_data != nullptr) {
        memcpy(req_buf + sizeof(DevOpReqPayload), req_data, req_data_len);
    }

    // Serialize: spin until we can claim the proxy for this operation.
    // Another thread may be using the same proxy (e.g., concurrent stat+open).
    while (true) {
        state->lock.lock();
        if (!state->op_pending.load(std::memory_order_acquire)) {
            break;
        }
        state->lock.unlock();
        wki_spin_yield();
        if (!state->op_pending.load(std::memory_order_acquire)) {
            continue;
        }
        ker::mod::sched::kern_yield();
    }

    uint32_t proxy_wait_us = static_cast<uint32_t>(wki_now_us() - proxy_wait_start);
    if (proxy_wait_us > 0) {
        perf_record_vfs_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::PROXY_WAIT), state->owner_node, state->assigned_channel, 0,
                              proxy_wait_us, callsite);
    }

    uint16_t expected_seq = 0;
    if (!peek_channel_tx_seq16(state->owner_node, state->assigned_channel, &expected_seq)) {
        state->lock.unlock();
        delete[] req_buf;
        return -1;
    }

    // Lock held, op_pending still false - set up wait entry and response fields
    WkiWaitEntry wait = {};
    state->op_wait_entry = &wait;
    state->op_expected_id = op_id;
    state->op_expected_seq = expected_seq;
    state->op_status = 0;
    state->op_resp_buf = resp_buf;
    state->op_resp_max = resp_buf_max;
    state->op_resp_len = 0;
    // Publish the request identity before making the slot visible to the RX path.
    state->op_pending.store(true, std::memory_order_release);
    state->lock.unlock();

    uint64_t started_us = wki_now_us();
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id),
                                     ker::mod::perf::WkiPerfPhase::BEGIN, state->owner_node, state->assigned_channel, correlation, 0,
                                     req_data_len, callsite);

    int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, req_buf, req_total);
    delete[] req_buf;

    if (send_ret != WKI_OK) {
        state->lock.lock();
        state->op_wait_entry = nullptr;
        ker::mod::dbg::log("[WKI] VFS op SEND_FAIL_CLEANUP: node=0x%04x ch=%u op=%u seq=%u rc=%d", state->owner_node,
                           state->assigned_channel, state->op_expected_id, state->op_expected_seq, send_ret);
        state->op_expected_id = 0;
        state->op_expected_seq = 0;
        state->op_resp_buf = nullptr;
        state->op_resp_max = 0;
        state->op_resp_len = 0;
        state->op_pending.store(false, std::memory_order_relaxed);
        state->lock.unlock();
        uint32_t elapsed_us = static_cast<uint32_t>(wki_now_us() - started_us);
        ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                         perf_vfs_op(op_id), ker::mod::perf::WkiPerfPhase::END, state->owner_node, state->assigned_channel,
                                         correlation, send_ret, elapsed_us, callsite);
        ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id), state->owner_node,
                                           state->assigned_channel, send_ret, elapsed_us, true, 0, req_data_len);
        ker::mod::dbg::log("[WKI] vfs_proxy_send_and_wait send failed: node=0x%04x ch=%u op=%u rc=%d", state->owner_node,
                           state->assigned_channel, op_id, send_ret);
        return send_ret;
    }

    int wait_rc = wki_wait_for_op(&wait, wait_timeout_us);
    if (wait_rc != 0) {
        state->lock.lock();
        state->op_wait_entry = nullptr;
        // Responses carry a per-request sequence cookie. Once a wait times out,
        // we can release the channel immediately and let late replies be dropped
        // either because no op is pending or because the next request has a
        // different sequence.
        ker::mod::dbg::log("[WKI] VFS op TIMEOUT_CLEANUP: node=0x%04x ch=%u op=%u seq=%u rc=%d", state->owner_node, state->assigned_channel,
                           state->op_expected_id, state->op_expected_seq, wait_rc);
        state->op_expected_id = 0;
        state->op_expected_seq = 0;
        state->op_resp_buf = nullptr;
        state->op_resp_max = 0;
        state->op_resp_len = 0;
        if (state->op_pending.load(std::memory_order_acquire)) {
            state->op_pending.store(false, std::memory_order_release);
        }
        state->lock.unlock();
        uint32_t elapsed_us = static_cast<uint32_t>(wki_now_us() - started_us);
        ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                         perf_vfs_op(op_id), ker::mod::perf::WkiPerfPhase::END, state->owner_node, state->assigned_channel,
                                         correlation, wait_rc, elapsed_us, callsite);
        ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id), state->owner_node,
                                           state->assigned_channel, wait_rc, elapsed_us, true, 0, req_data_len);
        if (wait_rc == WKI_ERR_TIMEOUT) {
            ker::mod::dbg::log("[WKI] vfs_proxy_send_and_wait timeout: node=0x%04x ch=%u op=%u wait_us=%llu", state->owner_node,
                               state->assigned_channel, op_id, static_cast<unsigned long long>(wait_timeout_us));
        } else {
            ker::mod::dbg::log("[WKI] vfs_proxy_send_and_wait aborted: node=0x%04x ch=%u op=%u rc=%d", state->owner_node,
                               state->assigned_channel, op_id, wait_rc);
        }
        return wait_rc;
    }

    state->lock.lock();
    state->op_wait_entry = nullptr;
    int status = static_cast<int>(state->op_status);
    uint16_t resp_len = state->op_resp_len;
    state->op_expected_id = 0;
    state->op_expected_seq = 0;
    state->op_resp_buf = nullptr;
    state->op_resp_max = 0;
    // Release the proxy slot only after we have consumed the result - this prevents
    // a new vfs_proxy_send_and_wait from overwriting op_expected_id/seq between
    // handle_vfs_op_resp waking us and this teardown running (the race that causes
    // pending_candidates=1 with expected_op=0/expected_seq=0 in stale-response logs).
    state->op_pending.store(false, std::memory_order_release);
    state->lock.unlock();

    uint32_t elapsed_us = static_cast<uint32_t>(wki_now_us() - started_us);
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id),
                                     ker::mod::perf::WkiPerfPhase::END, state->owner_node, state->assigned_channel, correlation, status,
                                     elapsed_us, callsite);
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(op_id), state->owner_node,
                                       state->assigned_channel, status, elapsed_us, true, 0, perf_vfs_bytes(op_id, req_data_len, resp_len));

    if (resp_len_out != nullptr) {
        *resp_len_out = resp_len;
    }

    return status;
}

auto vfs_proxy_write_rdma_and_wait(ProxyVfsState* state, int32_t remote_fd, int64_t offset, const uint8_t* src, uint32_t chunk,
                                   uint32_t* written_out) -> int {
    if (state == nullptr || src == nullptr || written_out == nullptr || state->rdma_transport == nullptr ||
        state->rdma_bounce_buf == nullptr || state->rdma_server_write_rkey == 0) {
        return -1;
    }

    *written_out = 0;

    uint32_t correlation = ker::mod::perf::next_wki_trace_correlation();
    uint64_t callsite = WOS_PERF_CALLSITE();
    uint64_t proxy_wait_start = wki_now_us();

    while (true) {
        state->lock.lock();
        if (!state->op_pending.load(std::memory_order_acquire)) {
            break;
        }
        state->lock.unlock();
        wki_spin_yield();
        if (!state->op_pending.load(std::memory_order_acquire)) {
            continue;
        }
        ker::mod::sched::kern_yield();
    }

    uint32_t proxy_wait_us = static_cast<uint32_t>(wki_now_us() - proxy_wait_start);
    if (proxy_wait_us > 0) {
        perf_record_vfs_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::PROXY_WAIT), state->owner_node, state->assigned_channel, 0,
                              proxy_wait_us, callsite);
    }

    uint16_t expected_seq = 0;
    if (!peek_channel_tx_seq16(state->owner_node, state->assigned_channel, &expected_seq)) {
        state->lock.unlock();
        return -1;
    }

    WkiWaitEntry wait = {};
    uint32_t resp_written = 0;
    state->op_wait_entry = &wait;
    state->op_expected_id = OP_VFS_WRITE_RDMA;
    state->op_expected_seq = expected_seq;
    state->op_status = 0;
    state->op_resp_buf = &resp_written;
    state->op_resp_max = sizeof(resp_written);
    state->op_resp_len = 0;
    state->op_pending.store(true, std::memory_order_release);
    state->lock.unlock();

    uint64_t started_us = wki_now_us();
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                     perf_vfs_op(OP_VFS_WRITE_RDMA), ker::mod::perf::WkiPerfPhase::BEGIN, state->owner_node,
                                     state->assigned_channel, correlation, 0, chunk, callsite);

    memcpy(state->rdma_bounce_buf, src, chunk);
    int rdma_ret = state->rdma_transport->rdma_write(state->rdma_transport, state->owner_node, state->rdma_server_write_rkey, 0,
                                                     state->rdma_bounce_buf, chunk);
    if (rdma_ret != 0) {
        state->lock.lock();
        state->op_wait_entry = nullptr;
        state->op_expected_id = 0;
        state->op_expected_seq = 0;
        state->op_resp_buf = nullptr;
        state->op_resp_max = 0;
        state->op_resp_len = 0;
        state->op_pending.store(false, std::memory_order_relaxed);
        state->lock.unlock();

        uint32_t elapsed_us = static_cast<uint32_t>(wki_now_us() - started_us);
        ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                         perf_vfs_op(OP_VFS_WRITE_RDMA), ker::mod::perf::WkiPerfPhase::END, state->owner_node,
                                         state->assigned_channel, correlation, rdma_ret, elapsed_us, callsite);
        ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(OP_VFS_WRITE_RDMA), state->owner_node,
                                           state->assigned_channel, rdma_ret, elapsed_us, true, 0, chunk);
        return rdma_ret;
    }

    std::array<uint8_t, 16> ctrl{};
    memcpy(ctrl.data(), &remote_fd, sizeof(int32_t));
    memcpy(ctrl.data() + 4, &offset, sizeof(int64_t));
    memcpy(ctrl.data() + 12, &chunk, sizeof(uint32_t));

    auto req_total = static_cast<uint16_t>(sizeof(DevOpReqPayload) + ctrl.size());
    auto* req_buf = new (std::nothrow) uint8_t[req_total];
    if (req_buf == nullptr) {
        state->lock.lock();
        state->op_wait_entry = nullptr;
        state->op_expected_id = 0;
        state->op_expected_seq = 0;
        state->op_resp_buf = nullptr;
        state->op_resp_max = 0;
        state->op_resp_len = 0;
        state->op_pending.store(false, std::memory_order_relaxed);
        state->lock.unlock();
        return -1;
    }

    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf);
    req->op_id = OP_VFS_WRITE_RDMA;
    req->data_len = static_cast<uint16_t>(ctrl.size());
    memcpy(req_buf + sizeof(DevOpReqPayload), ctrl.data(), ctrl.size());

    int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, req_buf, req_total);
    delete[] req_buf;
    if (send_ret != WKI_OK) {
        state->lock.lock();
        state->op_wait_entry = nullptr;
        state->op_expected_id = 0;
        state->op_expected_seq = 0;
        state->op_resp_buf = nullptr;
        state->op_resp_max = 0;
        state->op_resp_len = 0;
        state->op_pending.store(false, std::memory_order_relaxed);
        state->lock.unlock();

        uint32_t elapsed_us = static_cast<uint32_t>(wki_now_us() - started_us);
        ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                         perf_vfs_op(OP_VFS_WRITE_RDMA), ker::mod::perf::WkiPerfPhase::END, state->owner_node,
                                         state->assigned_channel, correlation, send_ret, elapsed_us, callsite);
        ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(OP_VFS_WRITE_RDMA), state->owner_node,
                                           state->assigned_channel, send_ret, elapsed_us, true, 0, chunk);
        return send_ret;
    }

    int wait_rc = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    if (wait_rc != 0) {
        state->lock.lock();
        state->op_wait_entry = nullptr;
        state->op_expected_id = 0;
        state->op_expected_seq = 0;
        state->op_resp_buf = nullptr;
        state->op_resp_max = 0;
        state->op_resp_len = 0;
        if (state->op_pending.load(std::memory_order_acquire)) {
            state->op_pending.store(false, std::memory_order_release);
        }
        state->lock.unlock();

        uint32_t elapsed_us = static_cast<uint32_t>(wki_now_us() - started_us);
        ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                         perf_vfs_op(OP_VFS_WRITE_RDMA), ker::mod::perf::WkiPerfPhase::END, state->owner_node,
                                         state->assigned_channel, correlation, wait_rc, elapsed_us, callsite);
        ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(OP_VFS_WRITE_RDMA), state->owner_node,
                                           state->assigned_channel, wait_rc, elapsed_us, true, 0, chunk);
        return wait_rc;
    }

    state->lock.lock();
    state->op_wait_entry = nullptr;
    int status = static_cast<int>(state->op_status);
    uint16_t resp_len = state->op_resp_len;
    state->op_expected_id = 0;
    state->op_expected_seq = 0;
    state->op_resp_buf = nullptr;
    state->op_resp_max = 0;
    state->op_pending.store(false, std::memory_order_release);
    state->lock.unlock();

    uint32_t elapsed_us = static_cast<uint32_t>(wki_now_us() - started_us);
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_VFS,
                                     perf_vfs_op(OP_VFS_WRITE_RDMA), ker::mod::perf::WkiPerfPhase::END, state->owner_node,
                                     state->assigned_channel, correlation, status, elapsed_us, callsite);
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_VFS, perf_vfs_op(OP_VFS_WRITE_RDMA), state->owner_node,
                                       state->assigned_channel, status, elapsed_us, true, 0,
                                       perf_vfs_bytes(OP_VFS_WRITE_RDMA, static_cast<uint16_t>(chunk), resp_len));

    *written_out = resp_written;
    return status;
}

constexpr uint32_t VFS_READ_RETRIES = 2;

// Remote readlink is used during path resolution and its result is often a
// cached negative lookup (-EINVAL / -ENOENT). Under heavy CPU load on the
// serving node, a 50 ms timeout is too aggressive: the late READLINK response
// can still arrive on the same sequenced VFS channel and block a subsequent
// WRITE_RDMA response behind the older sequence number. Use a more patient
// schedule so we do not create overlapping in-flight VFS ops on one channel
// during normal benchmark load.
constexpr std::array<uint64_t, 3> VFS_READLINK_TIMEOUTS_US = {1000000, 5000000, WKI_OP_TIMEOUT_US};

auto vfs_read_retry_timeout_us(uint16_t op_id, uint32_t attempt) -> uint64_t {
    if (op_id != OP_VFS_READLINK) {
        return WKI_OP_TIMEOUT_US;
    }

    size_t index = attempt;
    if (index >= VFS_READLINK_TIMEOUTS_US.size()) {
        index = VFS_READLINK_TIMEOUTS_US.size() - 1;
    }
    return VFS_READLINK_TIMEOUTS_US[index];
}

auto vfs_read_status_is_retryable(int status) -> bool {
    return status == WKI_ERR_TIMEOUT || status == WKI_ERR_NO_CREDITS || status == WKI_ERR_TX_FAILED;
}

auto vfs_proxy_read_with_retry(ProxyVfsState* state, uint16_t op_id, const uint8_t* req_data, uint16_t req_data_len, void* resp_buf,
                               uint16_t resp_buf_max, uint16_t* resp_len_out = nullptr, const char* debug_path = nullptr) -> int {
    for (uint32_t attempt = 0;; ++attempt) {
        int status = vfs_proxy_send_and_wait(state, op_id, req_data, req_data_len, resp_buf, resp_buf_max, resp_len_out,
                                             vfs_read_retry_timeout_us(op_id, attempt));
        if (!vfs_read_status_is_retryable(status) || attempt >= VFS_READ_RETRIES) {
            return status;
        }

        perf_record_vfs_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::RETRY), state->owner_node, state->assigned_channel, status,
                              attempt + 1, WOS_PERF_CALLSITE());

        if (op_id == OP_VFS_READLINK && debug_path != nullptr) {
            ker::mod::dbg::log("[WKI] retrying VFS readlink after transient failure: node=0x%04x ch=%u path='%s' rc=%d attempt=%u",
                               state->owner_node, state->assigned_channel, debug_path, status, attempt + 1);
        } else {
            ker::mod::dbg::log("[WKI] retrying VFS read after transient failure: node=0x%04x ch=%u op=%u rc=%d attempt=%u",
                               state->owner_node, state->assigned_channel, op_id, status, attempt + 1);
        }
    }
}

// -----------------------------------------------------------------------------
// D6: Write-behind flush helper
// -----------------------------------------------------------------------------

auto flush_write_behind(RemoteFileContext* ctx) -> int {
    if (ctx->write_buf == nullptr || ctx->write_buf->pending_len == 0) {
        return 0;
    }

    auto* wb = ctx->write_buf;
    auto* src = wb->data.data();
    auto remaining = static_cast<uint32_t>(wb->pending_len);
    auto cur_offset = wb->pending_offset;
    auto keep_pending_tail = [&](const uint8_t* tail_src, uint32_t tail_len, int64_t tail_offset) {
        if (tail_len == 0) {
            wb->pending_offset = -1;
            wb->pending_len = 0;
            return;
        }

        if (tail_src != wb->data.data()) {
            memmove(wb->data.data(), tail_src, tail_len);
        }
        wb->pending_offset = tail_offset;
        wb->pending_len = static_cast<uint16_t>(tail_len);
    };

    // RDMA path: push pending data to server's pre-registered receive buffer,
    // then send a tiny control message. Avoids embedding data in WKI packets.
    if (transport_supports_vfs_write_push_rdma(ctx->proxy->rdma_transport) && ctx->proxy->rdma_capable &&
        ctx->proxy->rdma_transport != nullptr && ctx->proxy->rdma_server_write_rkey != 0 && ctx->proxy->rdma_bounce_buf != nullptr) {
        while (remaining > 0) {
            uint32_t chunk = std::min(remaining, VFS_RDMA_BOUNCE_SIZE);

            uint32_t written = 0;
            int status = vfs_proxy_write_rdma_and_wait(ctx->proxy, ctx->remote_fd, cur_offset, src, chunk, &written);
            if (status != 0) {
                keep_pending_tail(src, remaining, cur_offset);
                return status;
            }

            if (written == 0) {
                keep_pending_tail(src, remaining, cur_offset);
                return -EIO;
            }
            src += written;
            cur_offset += static_cast<int64_t>(written);
            remaining -= written;
        }
    } else {
        // Message path (fallback): embed data in the WKI packet, capped to ethernet MTU
        constexpr uint32_t WRITE_HDR_OVERHEAD = sizeof(DevOpReqPayload) + 12;
        auto max_data = static_cast<uint32_t>(WKI_ETH_MAX_PAYLOAD - WRITE_HDR_OVERHEAD);

        while (remaining > 0) {
            uint32_t chunk = (remaining > max_data) ? max_data : remaining;

            auto req_data_len = static_cast<uint16_t>(12 + chunk);
            auto* req_data = new (std::nothrow) uint8_t[req_data_len];
            if (req_data == nullptr) {
                break;
            }

            int32_t remote_fd = ctx->remote_fd;
            memcpy(req_data, &remote_fd, sizeof(int32_t));
            memcpy(req_data + 4, &cur_offset, sizeof(int64_t));
            memcpy(req_data + 12, src, chunk);

            uint32_t written = 0;
            int status = vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_WRITE, req_data, req_data_len, &written, sizeof(uint32_t));
            delete[] req_data;
            if (status != 0) {
                keep_pending_tail(src, remaining, cur_offset);
                return status;
            }
            if (written == 0) {
                keep_pending_tail(src, remaining, cur_offset);
                return -EIO;
            }

            src += written;
            cur_offset += static_cast<int64_t>(written);
            remaining -= written;
        }
    }

    wb->pending_offset = -1;
    wb->pending_len = 0;
    return 0;
}

// -----------------------------------------------------------------------------
// Consumer-side FileOperations
// -----------------------------------------------------------------------------

auto remote_vfs_close(ker::vfs::File* f) -> int {
    if (f == nullptr || f->private_data == nullptr) {
        return -1;
    }
    auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
    if (ctx->proxy == nullptr || !ctx->proxy->active) {
        delete ctx->read_cache;
        delete ctx->write_buf;
        delete ctx;
        f->private_data = nullptr;
        return -1;
    }

    // D6: Flush pending writes before closing
    int flush_status = flush_write_behind(ctx);

    // D7: Invalidate directory cache for this file
    s_vfs_lock.lock();
    invalidate_dir_cache(ctx->proxy, ctx->remote_fd);
    s_vfs_lock.unlock();

    // Send OP_VFS_CLOSE: {remote_fd:i32} = 4 bytes
    int32_t remote_fd = ctx->remote_fd;
    vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_CLOSE, reinterpret_cast<const uint8_t*>(&remote_fd), sizeof(int32_t), nullptr, 0);

    // D6: Free caches
    delete ctx->read_cache;
    delete ctx->write_buf;

    delete ctx;
    f->private_data = nullptr;
    return flush_status;
}

auto remote_vfs_read(ker::vfs::File* f, void* buf, size_t count, size_t offset) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr || buf == nullptr) {
        return -1;
    }
    auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
    if (ctx->proxy == nullptr || !ctx->proxy->active) {
        return -1;
    }

    // D6: Flush any pending writes to ensure read-after-write consistency
    if (ctx->write_buf != nullptr && ctx->write_buf->pending_len > 0) {
        int flush_status = flush_write_behind(ctx);
        if (flush_status != 0) {
            return flush_status;
        }
    }

    auto* dest = static_cast<uint8_t*>(buf);
    auto remaining = static_cast<uint32_t>(count);
    auto cur_offset = static_cast<int64_t>(offset);
    ssize_t total_read = 0;

    // -- Bulk RDMA path ----------------------------------------------------
    // Two modes:
    //  (a) Direct bulk - request > 64 KB: issue OP_VFS_READ_BULK for each chunk.
    //  (b) Prefetch   - request ≤ 64 KB (typical cp/cat): on the first small read
    //      (or cache miss), pre-fetch up to 2 MB from the server into the bulk
    //      buffer and serve subsequent sequential reads from it.  This turns
    //      hundreds of 4 KB RPCs into one bulk RDMA write.
    //
    // The bulk buffer is per-mount; `bulk_owner_fd` tracks which open file last
    // filled it.  When ownership changes the old prefetch is implicitly stale.
    // Bulk RDMA path: pull mode (RoCE) checked first, then push mode (ivshmem).
    // Pull: server stages data in its bulk staging buf; client rdma_reads to fetch.
    // Push: server rdma_writes directly into client's bulk buf (consumer_rkey).
    const bool BULK_PULL_CAPABLE = transport_supports_rdma_read_pull(ctx->proxy->rdma_transport) && ctx->proxy->bulk_rdma_capable &&
                                   ctx->proxy->rdma_transport != nullptr && ctx->proxy->rdma_server_bulk_staging_rkey != 0 &&
                                   ctx->proxy->rdma_bulk_buf != nullptr;
    const bool BULK_PUSH_CAPABLE = !BULK_PULL_CAPABLE && transport_supports_vfs_read_push_rdma(ctx->proxy->rdma_transport) &&
                                   ctx->proxy->bulk_rdma_capable && ctx->proxy->rdma_transport != nullptr &&
                                   ctx->proxy->rdma_bulk_rkey != 0 && ctx->proxy->rdma_bulk_buf != nullptr;
    if (BULK_PULL_CAPABLE || BULK_PUSH_CAPABLE) {
#ifdef WKI_DEBUG
        if (cur_offset == 0) {
            ker::mod::dbg::log("[WKI] remote_vfs_read: BULK RDMA %s ch=%u rd=%d count=%u", bulk_pull_capable ? "pull" : "push",
                               ctx->proxy->assigned_channel, ctx->remote_fd, remaining);
        }
#endif
        // -- (b) Prefetch path: try to satisfy from the existing bulk cache --
        if (remaining <= VFS_RDMA_BOUNCE_SIZE && ctx->proxy->bulk_owner_fd == ctx->remote_fd && ctx->bulk_cached_len > 0 &&
            cur_offset >= ctx->bulk_cached_offset && cur_offset < ctx->bulk_cached_offset + static_cast<int64_t>(ctx->bulk_cached_len)) {
            auto off_in_buf = static_cast<uint32_t>(cur_offset - ctx->bulk_cached_offset);
            auto available = ctx->bulk_cached_len - off_in_buf;
            auto to_copy = std::min(remaining, available);

            memcpy(dest, ctx->proxy->rdma_bulk_buf + off_in_buf, to_copy);
            dest += to_copy;
            cur_offset += static_cast<int64_t>(to_copy);
            remaining -= to_copy;
            total_read += static_cast<ssize_t>(to_copy);

            if (remaining == 0) {
                return total_read;
            }
            // Partial hit - fall through to refill below
        }

        // -- Bulk fetch: either (a) direct or (b) prefetch refill ----------
        while (remaining > 0) {
            // For small reads, prefetch the full bulk buffer size; for large
            // reads, transfer exactly what is needed.
            uint32_t fetch_size =
                (remaining <= VFS_RDMA_BOUNCE_SIZE) ? ctx->proxy->rdma_bulk_size : std::min(remaining, ctx->proxy->rdma_bulk_size);

            // Pull mode sends rkey=0 (server ignores it; data goes to staging buf).
            // Push mode sends the client's bulk buf rkey.
            uint32_t req_rkey = BULK_PULL_CAPABLE ? 0 : ctx->proxy->rdma_bulk_rkey;
            std::array<uint8_t, 20> req{};
            memcpy(req.data(), &ctx->remote_fd, sizeof(int32_t));
            memcpy(req.data() + 4, &fetch_size, sizeof(uint32_t));
            memcpy(req.data() + 8, &cur_offset, sizeof(int64_t));
            memcpy(req.data() + 16, &req_rkey, sizeof(uint32_t));

            uint32_t bytes_read = 0;
            int status = vfs_proxy_read_with_retry(ctx->proxy, OP_VFS_READ_BULK, req.data(), 20, &bytes_read, sizeof(uint32_t));
            if (status != 0 || bytes_read == 0) {
                // Invalidate cache on error
                ctx->bulk_cached_len = 0;
                ctx->proxy->bulk_owner_fd = -1;
                break;
            }

            if (BULK_PULL_CAPABLE) {
                // Pull mode: server staged data; fetch it via rdma_read from staging buf.
                int rdma_ret = ctx->proxy->rdma_transport->rdma_read(ctx->proxy->rdma_transport, ctx->proxy->owner_node,
                                                                     ctx->proxy->rdma_server_bulk_staging_rkey, 0,
                                                                     ctx->proxy->rdma_bulk_buf, bytes_read);
                if (rdma_ret != 0) {
                    ctx->bulk_cached_len = 0;
                    ctx->proxy->bulk_owner_fd = -1;
                    break;
                }
            } else {
                // Push mode: server wrote data into shared memory at rdma_bulk_rkey.
                int rdma_ret = ctx->proxy->rdma_transport->rdma_read(ctx->proxy->rdma_transport, 0, ctx->proxy->rdma_bulk_rkey, 0,
                                                                     ctx->proxy->rdma_bulk_buf, bytes_read);
                if (rdma_ret != 0) {
                    ctx->bulk_cached_len = 0;
                    ctx->proxy->bulk_owner_fd = -1;
                    break;
                }
            }

            // Update prefetch cache metadata
            ctx->proxy->bulk_owner_fd = ctx->remote_fd;
            ctx->bulk_cached_offset = cur_offset;
            ctx->bulk_cached_len = bytes_read;

            auto to_copy = std::min(bytes_read, remaining);
            memcpy(dest, ctx->proxy->rdma_bulk_buf, to_copy);

            dest += to_copy;
            cur_offset += static_cast<int64_t>(to_copy);
            remaining -= to_copy;
            total_read += static_cast<ssize_t>(to_copy);

            if (bytes_read < fetch_size) {
                break;  // Short read: EOF
            }

            // For prefetch path, one fetch is enough - subsequent small reads
            // will hit the cache at the top of this function.
            if (remaining <= VFS_RDMA_BOUNCE_SIZE) {
                break;
            }
        }
        return (total_read > 0 || remaining == static_cast<uint32_t>(count)) ? total_read : -1;
    }

    // RDMA path: 64 KB per round-trip vs ~1400 B per message.
    // Pull mode (RoCE): server stages data; client rdma_reads from server staging buf.
    // Push mode (ivshmem): server rdma_writes to client's bounce buf; client reads locally.
    const bool rdma_pull_capable = transport_supports_rdma_read_pull(ctx->proxy->rdma_transport) && ctx->proxy->rdma_capable &&
                                   ctx->proxy->rdma_transport != nullptr && ctx->proxy->rdma_server_read_staging_rkey != 0 &&
                                   ctx->proxy->rdma_bounce_buf != nullptr;
    const bool rdma_push_capable = !rdma_pull_capable && transport_supports_vfs_read_push_rdma(ctx->proxy->rdma_transport) &&
                                   ctx->proxy->rdma_capable && ctx->proxy->rdma_transport != nullptr && ctx->proxy->rdma_read_rkey != 0 &&
                                   ctx->proxy->rdma_bounce_buf != nullptr;
    if (rdma_pull_capable || rdma_push_capable) {
        while (remaining > 0) {
            uint32_t chunk = std::min(remaining, VFS_RDMA_BOUNCE_SIZE);

            // Pull mode sends rkey=0 (server stages locally); push mode sends client bounce buf rkey.
            uint32_t req_rkey = rdma_pull_capable ? 0 : ctx->proxy->rdma_read_rkey;
            // Request: {fd:i32, len:u32, off:i64, consumer_rkey:u32} = 20 bytes
            std::array<uint8_t, 20> req{};
            memcpy(req.data(), &ctx->remote_fd, sizeof(int32_t));
            memcpy(req.data() + 4, &chunk, sizeof(uint32_t));
            memcpy(req.data() + 8, &cur_offset, sizeof(int64_t));
            memcpy(req.data() + 16, &req_rkey, sizeof(uint32_t));

            // Response: {bytes_read:u32} = 4 bytes
            uint32_t bytes_read = 0;
            int status = vfs_proxy_read_with_retry(ctx->proxy, OP_VFS_READ_RDMA, req.data(), 20, &bytes_read, sizeof(uint32_t));
            if (status != 0 || bytes_read == 0) {
                break;
            }

            auto to_copy = std::min(static_cast<uint32_t>(bytes_read), remaining);
            if (rdma_pull_capable) {
                // Pull mode: server staged data; fetch via rdma_read from server's staging buf.
                int rdma_ret = ctx->proxy->rdma_transport->rdma_read(ctx->proxy->rdma_transport, ctx->proxy->owner_node,
                                                                     ctx->proxy->rdma_server_read_staging_rkey, 0,
                                                                     ctx->proxy->rdma_bounce_buf, to_copy);
                if (rdma_ret != 0) {
                    break;
                }
            } else {
                // Push mode: server wrote into shared memory at rdma_read_rkey; read locally.
                int rdma_ret = ctx->proxy->rdma_transport->rdma_read(ctx->proxy->rdma_transport, 0, ctx->proxy->rdma_read_rkey, 0,
                                                                     ctx->proxy->rdma_bounce_buf, to_copy);
                if (rdma_ret != 0) {
                    break;
                }
            }
            memcpy(dest, ctx->proxy->rdma_bounce_buf, to_copy);

            dest += to_copy;
            cur_offset += static_cast<int64_t>(to_copy);
            remaining -= to_copy;
            total_read += static_cast<ssize_t>(to_copy);

            if (bytes_read < chunk) {
                break;  // Short read: EOF
            }
        }
        return (total_read > 0 || remaining == static_cast<uint32_t>(count)) ? total_read : -1;
    }

    // Message path (fallback): data embedded in DEV_OP_RESP, capped to ethernet MTU.
    // Max data per response
    auto max_resp_data = static_cast<uint32_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload));

    // D6: Check read-ahead cache first
    if (ctx->read_cache != nullptr && ctx->read_cache->cached_len > 0) {
        auto* rc = ctx->read_cache;
        int64_t cache_end = rc->cached_offset + rc->cached_len;

        if (cur_offset >= rc->cached_offset && cur_offset < cache_end) {
            // Cache hit (full or partial)
            auto cache_off = static_cast<uint16_t>(cur_offset - rc->cached_offset);
            auto available = static_cast<uint16_t>(rc->cached_len - cache_off);
            auto to_copy = static_cast<uint16_t>(std::min(static_cast<uint32_t>(available), remaining));

            memcpy(dest, &rc->data[cache_off], to_copy);
            dest += to_copy;
            cur_offset += to_copy;
            remaining -= to_copy;
            total_read += to_copy;

            if (remaining == 0) {
                return total_read;
            }
        }
    }

    while (remaining > 0) {
        auto fetch_size = static_cast<uint32_t>(VFS_CACHE_SIZE);
        fetch_size = std::min(fetch_size, max_resp_data);

        if (ctx->read_cache == nullptr) {
            ctx->read_cache = new ReadAheadCache();  // NOLINT(cppcoreguidelines-owning-memory)
        }
        uint8_t* fetch_dest = ctx->read_cache->data.data();
        bool using_cache = true;

        uint32_t chunk = fetch_size;

        // Build request: {remote_fd:i32, len:u32, offset:i64} = 16 bytes
        std::array<uint8_t, 16> req_data{};
        memcpy(req_data.data(), &ctx->remote_fd, sizeof(int32_t));
        memcpy(req_data.data() + 4, &chunk, sizeof(uint32_t));
        memcpy(req_data.data() + 8, &cur_offset, sizeof(int64_t));

        int status = vfs_proxy_read_with_retry(ctx->proxy, OP_VFS_READ, req_data.data(), 16, fetch_dest, static_cast<uint16_t>(chunk));
        if (status != 0) {
            return (total_read > 0) ? total_read : -1;
        }

        uint16_t bytes_read = ctx->proxy->op_resp_len;
        if (bytes_read == 0) {
            break;  // EOF
        }
#ifdef WKI_DEBUG
        // Diagnostic: log first chunk per read call
        if (total_read == 0 && bytes_read > 3) {
            ker::mod::dbg::log("[WKI] msg_read: ch=%u rd=%d off=%ld bytes=%u data=[%02x %02x %02x %02x]", ctx->proxy->assigned_channel,
                               ctx->remote_fd, static_cast<long>(cur_offset), bytes_read, fetch_dest[0], fetch_dest[1], fetch_dest[2],
                               fetch_dest[3]);
        }
#endif
        if (using_cache) {
            // Fill read-ahead cache and copy requested portion to caller
            ctx->read_cache->cached_offset = cur_offset;
            ctx->read_cache->cached_len = bytes_read;

            auto to_copy = static_cast<uint16_t>(std::min(static_cast<uint32_t>(bytes_read), remaining));
            memcpy(dest, ctx->read_cache->data.data(), to_copy);

            dest += to_copy;
            cur_offset += to_copy;
            remaining -= to_copy;
            total_read += to_copy;
        } else {
            dest += bytes_read;
            cur_offset += bytes_read;
            remaining -= bytes_read;
            total_read += bytes_read;
        }

        if (bytes_read < chunk) {
            break;  // Short read (EOF or partial)
        }
    }

    return total_read;
}

auto remote_vfs_write(ker::vfs::File* f, const void* buf, size_t count, size_t offset) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr || buf == nullptr) {
        return -1;
    }
    auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
    if (ctx->proxy == nullptr || !ctx->proxy->active) {
        return -1;
    }

    // D6: Invalidate read-ahead cache on write (stale data)
    if (ctx->read_cache != nullptr) {
        ctx->read_cache->cached_len = 0;
        ctx->read_cache->cached_offset = -1;
    }

    // Invalidate bulk prefetch cache on write
    ctx->bulk_cached_len = 0;
    ctx->bulk_cached_offset = -1;

    const auto* src = static_cast<const uint8_t*>(buf);
    auto remaining = static_cast<uint32_t>(count);
    auto cur_offset = static_cast<int64_t>(offset);
    ssize_t total_written = 0;

    // Max data per request: payload - DevOpReqPayload(4) - {remote_fd(4)+offset(8)} = data
    constexpr uint32_t WRITE_HDR_OVERHEAD = sizeof(DevOpReqPayload) + 12;
    auto max_data = static_cast<uint32_t>(WKI_ETH_MAX_PAYLOAD - WRITE_HDR_OVERHEAD);

    // D6: Try to buffer sequential writes
    while (remaining > 0) {
        // Lazily allocate write-behind buffer
        if (ctx->write_buf == nullptr) {
            ctx->write_buf = new WriteBehindBuffer();  // NOLINT(cppcoreguidelines-owning-memory)
        }

        auto* wb = ctx->write_buf;

        // Check if this write is sequential and fits in the buffer
        bool is_sequential = (wb->pending_len == 0) || (wb->pending_offset + wb->pending_len == cur_offset);
        auto space = static_cast<uint16_t>(VFS_CACHE_SIZE - wb->pending_len);

        if (is_sequential && space > 0) {
            // Buffer this write
            auto to_buffer = static_cast<uint16_t>(std::min(static_cast<uint32_t>(space), remaining));
            memcpy(&wb->data[wb->pending_len], src, to_buffer);
            if (wb->pending_len == 0) {
                wb->pending_offset = cur_offset;
            }
            wb->pending_len += to_buffer;

            src += to_buffer;
            cur_offset += to_buffer;
            remaining -= to_buffer;
            total_written += to_buffer;

            // If buffer is full, flush it
            if (wb->pending_len >= VFS_CACHE_SIZE) {
                int flush_status = flush_write_behind(ctx);
                if (flush_status != 0) {
                    return (total_written > 0) ? total_written : flush_status;
                }
            }
            continue;
        }

        // Non-sequential or buffer full: flush existing buffer first
        int flush_status = flush_write_behind(ctx);
        if (flush_status != 0) {
            return (total_written > 0) ? total_written : flush_status;
        }

        // If the data exceeds the buffer size, send directly (bypass buffering)
        if (remaining >= VFS_CACHE_SIZE) {
            uint32_t chunk = (remaining > max_data) ? max_data : remaining;

            auto req_data_len = static_cast<uint16_t>(12 + chunk);
            auto* req_data = new (std::nothrow) uint8_t[req_data_len];
            if (req_data == nullptr) {
                return (total_written > 0) ? total_written : -1;
            }

            int32_t remote_fd = ctx->remote_fd;
            memcpy(req_data, &remote_fd, sizeof(int32_t));
            memcpy(req_data + 4, &cur_offset, sizeof(int64_t));
            memcpy(req_data + 12, src, chunk);

            uint32_t written = 0;
            int status = vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_WRITE, req_data, req_data_len, &written, sizeof(uint32_t));
            delete[] req_data;

            if (status != 0) {
                return (total_written > 0) ? total_written : -1;
            }

            src += written;
            cur_offset += static_cast<int64_t>(written);
            remaining -= written;
            total_written += static_cast<ssize_t>(written);

            if (written < chunk) {
                break;  // Short write
            }
        }
        // Loop back - remaining data will be buffered on next iteration
    }

    return total_written;
}

auto remote_vfs_lseek(ker::vfs::File* f, off_t offset, int whence) -> off_t {
    if (f == nullptr) {
        return -1;
    }

    switch (whence) {
        case 0:  // SEEK_SET
            f->pos = offset;
            break;
        case 1:  // SEEK_CUR
            f->pos += offset;
            break;
        case 2: {  // SEEK_END - V2: server roundtrip to get file size
            if (f->private_data == nullptr) {
                ker::mod::dbg::log("remote_vfs_lseek: SEEK_END private_data is null");
                return -1;
            }
            auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
            if (ctx->proxy == nullptr || !ctx->proxy->active) {
                ker::mod::dbg::log("remote_vfs_lseek: SEEK_END proxy null/inactive (proxy=%p active=%d)", ctx->proxy,
                                   (ctx->proxy != nullptr) ? (int)ctx->proxy->active : -1);
                return -1;
            }

            // Flush pending writes before seek
            int flush_status = flush_write_behind(ctx);
            if (flush_status != 0) {
                return flush_status;
            }

            // Request: {remote_fd:i32, offset:i64} = 12 bytes
            std::array<uint8_t, 12> req{};
            memcpy(req.data(), &ctx->remote_fd, sizeof(int32_t));
            auto off64 = static_cast<int64_t>(offset);
            memcpy(req.data() + 4, &off64, sizeof(int64_t));
#ifdef WKI_DEBUG
            ker::mod::dbg::log("remote_vfs_lseek: SEEK_END remote_fd=%d node=0x%04x ch=%u", ctx->remote_fd, ctx->proxy->owner_node,
                               ctx->proxy->assigned_channel);
#endif
            // Response: {pos:i64} = 8 bytes
            int64_t new_pos = -1;
            int ret = vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_SEEK_END, req.data(), 12, &new_pos, sizeof(new_pos));
#ifdef WKI_DEBUG
            ker::mod::dbg::log("remote_vfs_lseek: SEEK_END ret=%d new_pos=%ld", ret, (long)new_pos);
#endif
            if (ret < 0) {
                return -1;
            }

            f->pos = new_pos;
            break;
        }
        default:
            return -1;
    }

    return f->pos;
}

auto remote_vfs_isatty(ker::vfs::File* /*f*/) -> bool { return false; }

auto remote_vfs_readdir(ker::vfs::File* f, ker::vfs::DirEntry* entry, size_t index) -> int {
    if (f == nullptr || f->private_data == nullptr || entry == nullptr) {
        return -1;
    }
    auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
    if (ctx->proxy == nullptr || !ctx->proxy->active) {
        return -1;
    }

    // D7: Check directory cache first (under lock)
    s_vfs_lock.lock();
    DirCacheEntry* cache = find_dir_cache(ctx->proxy, ctx->remote_fd);

    // Invalidate stale cache
    if (cache != nullptr && (wki_now_us() - cache->cache_time_us) > DirCacheEntry::STALE_US) {
        invalidate_dir_cache(ctx->proxy, ctx->remote_fd);
        cache = nullptr;
    }

    // Cache hit: entry already fetched
    if (cache != nullptr && try_get_visible_cached_dirent(cache, ctx, index, entry)) {
        s_vfs_lock.unlock();
        return 0;
    }

    // Cache hit but we already know directory is exhausted
    if (cache != nullptr && cache->complete) {
        s_vfs_lock.unlock();
        return -1;
    }

    // Create cache on first access
    if (cache == nullptr) {
        g_dir_cache.push_back({});
        cache = &g_dir_cache.back();
        cache->proxy = ctx->proxy;
        cache->remote_fd = ctx->remote_fd;
        cache->cache_time_us = wki_now_us();
        cache->complete = false;
    }
    s_vfs_lock.unlock();

    // Fetch entries from server in batches.
    // One OP_VFS_READDIR_BATCH request returns up to MAX_BATCH_ENTRIES entries,
    // filling the full jumbo frame - typically the entire directory in one round-trip.
    constexpr uint32_t MAX_BATCH_ENTRIES =
        static_cast<uint32_t>((WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload) - sizeof(uint32_t)) / sizeof(ker::vfs::DirEntry));
    constexpr size_t BATCH_RESP_SIZE = sizeof(uint32_t) + MAX_BATCH_ENTRIES * sizeof(ker::vfs::DirEntry);

    auto* batch_buf = new (std::nothrow) uint8_t[BATCH_RESP_SIZE];
    if (batch_buf == nullptr) {
        return -1;
    }

    while (true) {
        s_vfs_lock.lock();
        cache = find_dir_cache(ctx->proxy, ctx->remote_fd);
        if (cache == nullptr) {
            s_vfs_lock.unlock();
            delete[] batch_buf;
            return -1;
        }
        if (try_get_visible_cached_dirent(cache, ctx, index, entry)) {
            s_vfs_lock.unlock();
            delete[] batch_buf;
            return 0;
        }
        if (cache->complete) {
            s_vfs_lock.unlock();
            delete[] batch_buf;
            return -1;
        }
        auto fetch_idx = static_cast<uint32_t>(cache->entries.size());
        s_vfs_lock.unlock();

        // Request: {fd:i32, start_idx:u32, max_count:u32} = 12 bytes
        std::array<uint8_t, 12> req_data{};
        memcpy(req_data.data(), &ctx->remote_fd, sizeof(int32_t));
        memcpy(req_data.data() + 4, &fetch_idx, sizeof(uint32_t));
        memcpy(req_data.data() + 8, &MAX_BATCH_ENTRIES, sizeof(uint32_t));

        memset(batch_buf, 0, BATCH_RESP_SIZE);
        int status = vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_READDIR_BATCH, req_data.data(), 12, batch_buf,
                                             static_cast<uint16_t>(BATCH_RESP_SIZE));
        if (status != 0 || ctx->proxy->op_resp_len < sizeof(uint32_t)) {
            s_vfs_lock.lock();
            cache = find_dir_cache(ctx->proxy, ctx->remote_fd);
            if (cache != nullptr) {
                cache->complete = true;
            }
            s_vfs_lock.unlock();
            delete[] batch_buf;
            return -1;
        }

        uint32_t count = 0;
        memcpy(&count, batch_buf, sizeof(uint32_t));

        s_vfs_lock.lock();
        cache = find_dir_cache(ctx->proxy, ctx->remote_fd);
        if (cache != nullptr) {
            for (uint32_t i = 0; i < count && i < MAX_BATCH_ENTRIES; i++) {
                ker::vfs::DirEntry fetched = {};
                memcpy(&fetched, batch_buf + sizeof(uint32_t) + (i * sizeof(ker::vfs::DirEntry)), sizeof(ker::vfs::DirEntry));
                cache->entries.push_back(fetched);
            }
            if (count < MAX_BATCH_ENTRIES) {
                cache->complete = true;  // Server returned fewer than asked -> end of directory
            }
        }
        s_vfs_lock.unlock();
    }
}

// D8: readlink via remote VFS proxy
auto remote_vfs_readlink(ker::vfs::File* f, char* buf, size_t bufsize) -> ssize_t {  // NOLINT(readability-non-const-parameter)
    if (f == nullptr || f->private_data == nullptr || buf == nullptr || bufsize == 0) {
        return -1;
    }
    auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
    if (ctx->proxy == nullptr || !ctx->proxy->active) {
        return -1;
    }

    // We don't have a path available from the File. readlink on an already-opened
    // remote file is not meaningful - readlink operates on paths, not FDs.
    // Return -1 as unsupported on open files.
    (void)bufsize;
    return -1;
}

// V2: Remote truncate
auto remote_vfs_truncate(ker::vfs::File* f, off_t length) -> int {
    if (f == nullptr || f->private_data == nullptr) {
        return -1;
    }
    auto* ctx = static_cast<RemoteFileContext*>(f->private_data);
    if (ctx->proxy == nullptr || !ctx->proxy->active) {
        return -1;
    }

    // Flush pending writes before truncate
    int flush_status = flush_write_behind(ctx);
    if (flush_status != 0) {
        return flush_status;
    }

    // Invalidate read-ahead cache (file size changes)
    if (ctx->read_cache != nullptr) {
        ctx->read_cache->cached_offset = -1;
        ctx->read_cache->cached_len = 0;
    }

    // Request: {remote_fd:i32, length:i64} = 12 bytes
    std::array<uint8_t, 12> req{};
    memcpy(req.data(), &ctx->remote_fd, sizeof(int32_t));
    int64_t len64 = static_cast<int64_t>(length);
    memcpy(req.data() + 4, &len64, sizeof(int64_t));

    int ret = vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_TRUNCATE, req.data(), 12, nullptr, 0);
    return ret;
}

// Static FileOperations for remote VFS files
ker::vfs::FileOperations g_remote_vfs_fops = {
    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
    .vfs_open = nullptr,
    .vfs_close = remote_vfs_close,
    .vfs_read = remote_vfs_read,
    .vfs_write = remote_vfs_write,
    .vfs_lseek = remote_vfs_lseek,
    .vfs_isatty = remote_vfs_isatty,
    .vfs_readdir = remote_vfs_readdir,
    .vfs_readlink = remote_vfs_readlink,
    .vfs_truncate = remote_vfs_truncate,
    .vfs_poll_check = nullptr,
    .vfs_poll_register_waiter = nullptr,
};

}  // namespace

// -------------------------------------------------------------------------------
// Init
// -------------------------------------------------------------------------------

void wki_remote_vfs_init() {
    if (g_remote_vfs_initialized) {
        return;
    }
    g_remote_vfs_initialized = true;
    ker::mod::dbg::log("[WKI] Remote VFS subsystem initialized");
}

// -------------------------------------------------------------------------------
// Server Side - VFS Export Management
// -------------------------------------------------------------------------------

auto wki_remote_vfs_export_add_internal(const char* export_path, const char* name, uint32_t preferred_resource_id) -> uint32_t {
    if (export_path == nullptr || name == nullptr) {
        return 0;
    }

    s_vfs_lock.lock();

    // Check if this path is already exported (prevent duplicates)
    for (const auto& existing : g_vfs_exports) {
        if (existing.active && strcmp(existing.export_path, export_path) == 0) {
            // Already exported - return existing resource_id
            uint32_t existing_id = existing.resource_id;
            s_vfs_lock.unlock();
            return existing_id;
        }
    }

    VfsExport exp;
    exp.active = true;
    if (preferred_resource_id != 0) {
        exp.resource_id = preferred_resource_id;
        if (g_next_vfs_resource_id <= preferred_resource_id) {
            g_next_vfs_resource_id = preferred_resource_id + 1;
        }
    } else {
        exp.resource_id = g_next_vfs_resource_id++;
    }

    size_t path_len = strlen(export_path);
    if (path_len >= VFS_EXPORT_PATH_LEN) {
        path_len = VFS_EXPORT_PATH_LEN - 1;
    }
    memcpy(static_cast<void*>(exp.export_path), export_path, path_len);
    exp.export_path[path_len] = '\0';

    size_t name_len = strlen(name);
    if (name_len >= VFS_EXPORT_NAME_LEN) {
        name_len = VFS_EXPORT_NAME_LEN - 1;
    }
    memcpy(static_cast<void*>(exp.name), name, name_len);
    exp.name[name_len] = '\0';

    g_vfs_exports.push_back(exp);
    uint32_t result_id = exp.resource_id;
    s_vfs_lock.unlock();

    ker::mod::dbg::log("[WKI] VFS export added: %s -> %s (resource_id=%u)", static_cast<const char*>(exp.name),
                       static_cast<const char*>(exp.export_path), result_id);
    return result_id;
}

auto wki_remote_vfs_export_add(const char* export_path, const char* name) -> uint32_t {
    return wki_remote_vfs_export_add_internal(export_path, name, 0);
}

auto wki_remote_vfs_find_export(uint32_t resource_id) -> VfsExport* {
    s_vfs_lock.lock();
    for (auto& exp : g_vfs_exports) {
        if (exp.active && exp.resource_id == resource_id) {
            s_vfs_lock.unlock();
            return &exp;
        }
    }
    s_vfs_lock.unlock();
    return nullptr;
}

namespace {

void advertise_exports_to_peer(uint16_t peer_node) {
    s_vfs_lock.lock();
    size_t export_count = g_vfs_exports.size();
    s_vfs_lock.unlock();

    for (size_t idx = 0; idx < export_count; idx++) {
        s_vfs_lock.lock();
        if (idx >= g_vfs_exports.size()) {
            s_vfs_lock.unlock();
            break;
        }
        auto& exp = g_vfs_exports[idx];
        if (!exp.active) {
            s_vfs_lock.unlock();
            continue;
        }

        // Build ResourceAdvertPayload + name under lock
        uint8_t name_len = 0;
        while (name_len < 63 && exp.name[name_len] != '\0') {
            name_len++;
        }

        auto total_len = static_cast<uint16_t>(sizeof(ResourceAdvertPayload) + name_len);
        std::array<uint8_t, sizeof(ResourceAdvertPayload) + 64> buf{};

        auto* adv = reinterpret_cast<ResourceAdvertPayload*>(buf.data());
        adv->node_id = g_wki.my_node_id;
        adv->resource_type = static_cast<uint16_t>(ResourceType::VFS);
        adv->resource_id = exp.resource_id;
        adv->flags = 0;
        adv->name_len = name_len;
        memcpy(buf.data() + sizeof(ResourceAdvertPayload), static_cast<const void*>(exp.name), name_len);
        s_vfs_lock.unlock();

        wki_send(peer_node, WKI_CHAN_CONTROL, MsgType::RESOURCE_ADVERT, buf.data(), total_len);
    }
}

}  // namespace

void wki_remote_vfs_advertise_exports_to_peer(uint16_t peer_node) {
    if (!g_remote_vfs_initialized || peer_node == WKI_NODE_INVALID) {
        return;
    }

    WkiPeer* peer = wki_peer_find(peer_node);
    if (peer == nullptr || peer->state != PeerState::CONNECTED) {
        return;
    }

    advertise_exports_to_peer(peer_node);
}

void wki_remote_vfs_advertise_exports() {
    if (!g_remote_vfs_initialized) {
        return;
    }

    for (size_t p = 0; p < WKI_MAX_PEERS; p++) {
        WkiPeer* peer = &g_wki.peers[p];
        if (peer->node_id == WKI_NODE_INVALID || peer->state != PeerState::CONNECTED) {
            continue;
        }
        advertise_exports_to_peer(peer->node_id);
    }
}

// -------------------------------------------------------------------------------
// Server Side - VFS Operation Handlers
// -------------------------------------------------------------------------------

namespace detail {

void handle_vfs_op(const WkiHeader* hdr, uint16_t channel_id, const char* export_path, const char* export_name, uint16_t op_id,
                   const uint8_t* data, uint16_t data_len) {
    const uint16_t req_cookie = static_cast<uint16_t>(hdr->seq_num & UINT16_MAX);
    auto send_simple_resp = [&](DevOpRespPayload& resp) {
        resp.reserved = req_cookie;
        wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
    };
    auto send_buffered_resp = [&](void* resp_buf, uint16_t resp_len) {
        if (resp_buf != nullptr && resp_len >= sizeof(DevOpRespPayload)) {
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf);
            resp->reserved = req_cookie;
        }
        wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf, resp_len);
    };

    switch (op_id) {
        case OP_VFS_OPEN: {
            // Request: {flags:u32, mode:u32, path_len:u16, path[path_len]}
            if (data_len < 10) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_OPEN;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = req_cookie;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            uint32_t flags = 0;
            uint32_t mode = 0;
            uint16_t path_len = 0;
            memcpy(&flags, data, sizeof(uint32_t));
            memcpy(&mode, data + 4, sizeof(uint32_t));
            memcpy(&path_len, data + 8, sizeof(uint16_t));

            if (data_len < 10 + path_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_OPEN;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = req_cookie;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            // Build full path: export_path + "/" + relative_path
            std::array<char, 512> full_path{};
            std::array<char, 512> full_visible_path{};
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 10), path_len);
            build_full_path(full_visible_path.data(), full_visible_path.size(), export_name, reinterpret_cast<const char*>(data + 10),
                            path_len);

            // Block recursive proxying: don't let a remote client traverse into our WKI mounts
            if (path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_OPEN;
                resp.status = -EPERM;
                resp.data_len = 0;
                resp.reserved = req_cookie;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            // Open using the export backing path, independent of whichever task
            // context happened to receive the packet.
            ker::vfs::File* file = ker::vfs::vfs_open_file_resolved(full_path.data(), static_cast<int>(flags), static_cast<int>(mode));
            if (file == nullptr) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_OPEN;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = req_cookie;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            // Allocate a remote FD
            s_vfs_lock.lock();
            int32_t fd_id = alloc_remote_fd(hdr->src_node, channel_id, file);
            s_vfs_lock.unlock();

            // Response: {remote_fd:i32, is_dir:u8} = 5 bytes
            std::array<uint8_t, sizeof(DevOpRespPayload) + 5> resp_buf{};  // NOLINT(modernize-avoid-c-arrays)
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = OP_VFS_OPEN;
            resp->status = 0;
            resp->data_len = 5;
            resp->reserved = req_cookie;
            memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &fd_id, sizeof(int32_t));
            uint8_t is_dir = file->is_directory ? 1 : 0;
            memcpy(resp_buf.data() + sizeof(DevOpRespPayload) + 4, &is_dir, 1);

            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf.data(), static_cast<uint16_t>(sizeof(DevOpRespPayload) + 5));

#ifdef DEBUG_WKI_VFS
            // Diagnostic: immediately read first 4 bytes at open time to verify file data
            if (file->fops != nullptr && file->fops->vfs_read != nullptr && !file->is_directory) {
                uint8_t probe[4] = {0xEE, 0xEE, 0xEE, 0xEE};
                ssize_t probe_n = file->fops->vfs_read(file, probe, 4, 0);
                ker::mod::dbg::log(
                    "[WKI-SRV] VFS_OPEN: node=0x%04x path='%s' fd=%d fs_type=%d"
                    " probe=[%02x %02x %02x %02x] probe_bytes=%ld",
                    hdr->src_node, full_path.data(), fd_id, static_cast<int>(file->fs_type), probe[0], probe[1], probe[2], probe[3],
                    static_cast<long>(probe_n));
            }
#endif
            break;
        }

        case OP_VFS_READ: {
            // Request: {remote_fd:i32, len:u32, offset:i64} = 16 bytes
            if (data_len < 16) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = req_cookie;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            int32_t fd_id = 0;
            uint32_t len = 0;
            int64_t offset = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&len, data + 4, sizeof(uint32_t));
            memcpy(&offset, data + 8, sizeof(int64_t));

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_read == nullptr) {
                s_vfs_lock.unlock();
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = req_cookie;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            // Clamp to max response payload
            auto max_resp_data = static_cast<uint16_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload));
            len = std::min<uint32_t>(len, max_resp_data);

            auto resp_total = static_cast<uint16_t>(sizeof(DevOpRespPayload) + len);
            auto* resp_buf = new (std::nothrow) uint8_t[resp_total];
            if (resp_buf == nullptr) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = req_cookie;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf);
            uint8_t* read_buf = resp_buf + sizeof(DevOpRespPayload);

            // Pre-fill buffer with a sentinel pattern to detect whether vfs_read actually writes
            std::memset(read_buf, 0xAA, len);

            ssize_t bytes_read = local_file->fops->vfs_read(local_file, read_buf, len, static_cast<size_t>(offset));

#ifdef DEBUG_WKI_VFS
            // Diagnostic: log every read at offset 0 with full details
            if (offset == 0 && bytes_read > 3) {
                bool all_zero = (read_buf[0] == 0x00 && read_buf[1] == 0x00 && read_buf[2] == 0x00 && read_buf[3] == 0x00);
                ker::mod::dbg::log("[WKI-SRV] VFS_READ fd=%d bytes=%ld off=%ld fs_type=%d hdr=[%02x %02x %02x %02x]", fd_id,
                                   static_cast<long>(bytes_read), static_cast<long>(offset), static_cast<int>(local_file->fs_type),
                                   read_buf[0], read_buf[1], read_buf[2], read_buf[3]);

                // If allzero: re-read from the SAME file handle to check consistency
                if (all_zero) {
                    uint8_t vbuf[16] = {};
                    std::memset(vbuf, 0xCC, 16);
                    ssize_t vr = local_file->fops->vfs_read(local_file, vbuf, 16, 0);
                    ker::mod::dbg::log("[WKI-SRV] VERIFY re-read same fd: bytes=%ld data=[%02x %02x %02x %02x]", static_cast<long>(vr),
                                       vbuf[0], vbuf[1], vbuf[2], vbuf[3]);

                    // Also try opening and reading /etc/hostname to verify XFS works at all
                    auto* test_file = ker::vfs::vfs_open_file("/etc/hostname", 0, 0);
                    if (test_file != nullptr && test_file->fops != nullptr && test_file->fops->vfs_read != nullptr) {
                        uint8_t hbuf[16] = {};
                        ssize_t hr = test_file->fops->vfs_read(test_file, hbuf, 16, 0);
                        ker::mod::dbg::log("[WKI-SRV] VERIFY /etc/hostname: bytes=%ld fs_type=%d data=[%02x %02x %02x %02x]",
                                           static_cast<long>(hr), static_cast<int>(test_file->fs_type), hbuf[0], hbuf[1], hbuf[2], hbuf[3]);
                        if (test_file->fops->vfs_close != nullptr) {
                            test_file->fops->vfs_close(test_file);
                        }
                    }
                }
            }
#endif

            resp->op_id = OP_VFS_READ;
            if (bytes_read >= 0) {
                resp->status = 0;
                resp->data_len = static_cast<uint16_t>(bytes_read);
            } else {
                resp->status = static_cast<int16_t>(bytes_read);
                resp->data_len = 0;
            }
            resp->reserved = req_cookie;

            uint16_t send_len = (bytes_read > 0) ? static_cast<uint16_t>(sizeof(DevOpRespPayload) + bytes_read)
                                                 : static_cast<uint16_t>(sizeof(DevOpRespPayload));
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf, send_len);
            delete[] resp_buf;
            break;
        }

        case OP_VFS_WRITE: {
            // Request: {remote_fd:i32, offset:i64, data[N]} = 12 + N bytes
            if (data_len < 12) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_WRITE;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int32_t fd_id = 0;
            int64_t offset = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&offset, data + 4, sizeof(int64_t));

            const uint8_t* write_data = data + 12;
            auto write_len = static_cast<uint16_t>(data_len - 12);

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_write == nullptr) {
                s_vfs_lock.unlock();
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_WRITE;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            ssize_t bytes_written = local_file->fops->vfs_write(local_file, write_data, write_len, static_cast<size_t>(offset));

            // Response: {written:u32} = 4 bytes
            std::array<uint8_t, sizeof(DevOpRespPayload) + 4> resp_buf{};  // NOLINT(modernize-avoid-c-arrays)
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = OP_VFS_WRITE;
            resp->status = (bytes_written >= 0) ? static_cast<int16_t>(0)
                                                : static_cast<int16_t>(std::max(bytes_written, static_cast<ssize_t>(SHRT_MIN)));
            resp->data_len = 4;
            auto written = static_cast<uint32_t>((bytes_written >= 0) ? bytes_written : 0);
            memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &written, sizeof(uint32_t));

            send_buffered_resp(resp_buf.data(), static_cast<uint16_t>(sizeof(DevOpRespPayload) + 4));
            break;
        }

        case OP_VFS_CLOSE: {
            // Request: {remote_fd:i32} = 4 bytes
            if (data_len < 4) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_CLOSE;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int32_t fd_id = 0;
            memcpy(&fd_id, data, sizeof(int32_t));

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            touch_remote_fd(rfd);
            ker::vfs::File* close_file = nullptr;
            int16_t status = -1;
            if (rfd != nullptr && rfd->file != nullptr) {
                close_file = rfd->file;
                rfd->file = nullptr;
                rfd->active = false;
                status = 0;
            }
            std::erase_if(g_remote_fds, [](const RemoteVfsFd& r) { return !r.active; });
            s_vfs_lock.unlock();

            if (close_file != nullptr) {
                if (close_file->fops != nullptr && close_file->fops->vfs_close != nullptr) {
                    close_file->fops->vfs_close(close_file);
                }
                delete close_file;
            }

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_CLOSE;
            resp.status = status;
            resp.data_len = 0;
            send_simple_resp(resp);
            break;
        }

        case OP_VFS_READDIR: {
            // Request: {remote_fd:i32, index:u32} = 8 bytes
            if (data_len < 8) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int32_t fd_id = 0;
            uint32_t index = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&index, data + 4, sizeof(uint32_t));

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_readdir == nullptr) {
                s_vfs_lock.unlock();
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            ker::vfs::DirEntry entry = {};
            int ret = local_file->fops->vfs_readdir(local_file, &entry, index);

            if (ret != 0) {
                // End of directory or error
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR;
                resp.status = static_cast<int16_t>(ret);
                resp.data_len = 0;
                send_simple_resp(resp);
            } else {
                // Send entry
                auto resp_total = static_cast<uint16_t>(sizeof(DevOpRespPayload) + sizeof(ker::vfs::DirEntry));
                std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(ker::vfs::DirEntry)> resp_buf{};
                auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
                resp->op_id = OP_VFS_READDIR;
                resp->status = 0;
                resp->data_len = sizeof(ker::vfs::DirEntry);
                memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &entry, sizeof(ker::vfs::DirEntry));
                send_buffered_resp(resp_buf.data(), resp_total);
            }
            break;
        }

        case OP_VFS_READDIR_BATCH: {
            // Request: {fd:i32, start_idx:u32, max_count:u32} = 12 bytes
            // Response: {count:u32} + count×DirEntry - packs as many entries as
            // fit in one jumbo frame, typically the whole directory in one shot.
            if (data_len < 12) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR_BATCH;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int32_t fd_id = 0;
            uint32_t start_idx = 0;
            uint32_t max_count = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&start_idx, data + 4, sizeof(uint32_t));
            memcpy(&max_count, data + 8, sizeof(uint32_t));

            // Clamp to what fits in one frame
            constexpr uint32_t HARD_MAX =
                static_cast<uint32_t>((WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload) - sizeof(uint32_t)) / sizeof(ker::vfs::DirEntry));
            if (max_count == 0 || max_count > HARD_MAX) max_count = HARD_MAX;

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_readdir == nullptr) {
                s_vfs_lock.unlock();
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR_BATCH;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            // Allocate response: DevOpRespPayload + {count:u32} + max_count×DirEntry
            auto data_size = static_cast<uint32_t>(sizeof(uint32_t) + max_count * sizeof(ker::vfs::DirEntry));
            auto resp_total = static_cast<uint32_t>(sizeof(DevOpRespPayload) + data_size);
            auto* resp_buf = new (std::nothrow) uint8_t[resp_total];
            if (resp_buf == nullptr) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR_BATCH;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }
            memset(resp_buf, 0, resp_total);

            uint8_t* entries_base = resp_buf + sizeof(DevOpRespPayload) + sizeof(uint32_t);
            uint32_t count = 0;
            for (uint32_t i = 0; i < max_count; i++) {
                ker::vfs::DirEntry entry = {};
                int ret = local_file->fops->vfs_readdir(local_file, &entry, start_idx + i);
                if (ret != 0) break;
                memcpy(entries_base + i * sizeof(ker::vfs::DirEntry), &entry, sizeof(ker::vfs::DirEntry));
                count++;
            }

            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf);
            resp->op_id = OP_VFS_READDIR_BATCH;
            resp->status = 0;
            resp->data_len = static_cast<uint16_t>(sizeof(uint32_t) + count * sizeof(ker::vfs::DirEntry));
            memcpy(resp_buf + sizeof(DevOpRespPayload), &count, sizeof(uint32_t));

            auto send_len = static_cast<uint16_t>(sizeof(DevOpRespPayload) + sizeof(uint32_t) + count * sizeof(ker::vfs::DirEntry));
            send_buffered_resp(resp_buf, send_len);
            delete[] resp_buf;
            break;
        }

        case OP_VFS_STAT: {
            // Request: {path_len:u16, path[path_len]}
            if (data_len < 2) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_STAT;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            uint16_t path_len = 0;
            memcpy(&path_len, data, sizeof(uint16_t));

            if (data_len < 2 + path_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_STAT;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            std::array<char, 512> full_path{};
            std::array<char, 512> full_visible_path{};
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 2), path_len);
            build_full_path(full_visible_path.data(), full_visible_path.size(), export_name, reinterpret_cast<const char*>(data + 2),
                            path_len);

            if (path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_STAT;
                resp.status = -EPERM;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            ker::vfs::stat statbuf = {};
            int ret = ker::vfs::vfs_stat_resolved(full_path.data(), &statbuf);

            if (ret != 0) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_STAT;
                resp.status = static_cast<int16_t>(ret);
                resp.data_len = 0;
                send_simple_resp(resp);
            } else {
                auto resp_total = static_cast<uint16_t>(sizeof(DevOpRespPayload) + sizeof(ker::vfs::stat));
                auto* resp_buf = new (std::nothrow) uint8_t[resp_total];
                if (resp_buf == nullptr) {
                    DevOpRespPayload resp = {};
                    resp.op_id = OP_VFS_STAT;
                    resp.status = -1;
                    resp.data_len = 0;
                    send_simple_resp(resp);
                    break;
                }

                auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf);
                resp->op_id = OP_VFS_STAT;
                resp->status = 0;
                resp->data_len = sizeof(ker::vfs::stat);
                memcpy(resp_buf + sizeof(DevOpRespPayload), &statbuf, sizeof(ker::vfs::stat));
                send_buffered_resp(resp_buf, resp_total);
                delete[] resp_buf;
            }
            break;
        }

        case OP_VFS_MKDIR: {
            // Request: {mode:u32, path_len:u16, path[path_len]}
            if (data_len < 6) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_MKDIR;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            uint32_t mode = 0;
            uint16_t path_len = 0;
            memcpy(&mode, data, sizeof(uint32_t));
            memcpy(&path_len, data + 4, sizeof(uint16_t));

            if (data_len < 6 + path_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_MKDIR;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            std::array<char, 512> full_path{};
            std::array<char, 512> full_visible_path{};
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 6), path_len);
            build_full_path(full_visible_path.data(), full_visible_path.size(), export_name, reinterpret_cast<const char*>(data + 6),
                            path_len);

            if (path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_MKDIR;
                resp.status = -EPERM;
                resp.data_len = 0;
                send_simple_resp(resp);
                break;
            }

            int ret = ker::vfs::vfs_mkdir(full_visible_path.data(), static_cast<int>(mode));

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_MKDIR;
            resp.status = static_cast<int16_t>(ret);
            resp.data_len = 0;
            send_simple_resp(resp);
            break;
        }

        case OP_VFS_READLINK: {
            // D8: Request: {path_len:u16, path[path_len]}
            if (data_len < 2) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READLINK;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = req_cookie;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            uint16_t path_len = 0;
            memcpy(&path_len, data, sizeof(uint16_t));

            if (data_len < 2 + path_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READLINK;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = req_cookie;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            std::array<char, 512> full_path{};
            std::array<char, 512> full_visible_path{};
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 2), path_len);
            build_full_path(full_visible_path.data(), full_visible_path.size(), export_name, reinterpret_cast<const char*>(data + 2),
                            path_len);

            if (path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READLINK;
                resp.status = -EPERM;
                resp.data_len = 0;
                resp.reserved = req_cookie;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            // Read the symlink target
            std::array<char, 512> target_buf{};
            ssize_t target_len = ker::vfs::vfs_readlink_resolved(full_path.data(), target_buf.data(), target_buf.size() - 1);

            if (target_len < 0) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READLINK;
                resp.status = static_cast<int16_t>(target_len);
                resp.data_len = 0;
                resp.reserved = req_cookie;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            } else {
                // Response: {target_len:u16, target[]}
                auto resp_data_len = static_cast<uint16_t>(2 + target_len);
                auto resp_total = static_cast<uint16_t>(sizeof(DevOpRespPayload) + resp_data_len);
                auto* resp_buf = new (std::nothrow) uint8_t[resp_total];
                if (resp_buf == nullptr) {
                    DevOpRespPayload resp = {};
                    resp.op_id = OP_VFS_READLINK;
                    resp.status = -1;
                    resp.data_len = 0;
                    resp.reserved = req_cookie;
                    wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                    break;
                }

                auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf);
                resp->op_id = OP_VFS_READLINK;
                resp->status = 0;
                resp->data_len = resp_data_len;
                resp->reserved = req_cookie;

                auto tlen = static_cast<uint16_t>(target_len);
                memcpy(resp_buf + sizeof(DevOpRespPayload), &tlen, sizeof(uint16_t));
                memcpy(resp_buf + sizeof(DevOpRespPayload) + 2, target_buf.data(), target_len);

                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf, resp_total);
                delete[] resp_buf;
            }
            break;
        }

        case OP_VFS_SYMLINK: {
            // D8: Request: {target_len:u16, target[], link_len:u16, link[]}
            if (data_len < 4) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_SYMLINK;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            uint16_t target_len = 0;
            memcpy(&target_len, data, sizeof(uint16_t));

            if (data_len < 4 + target_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_SYMLINK;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            uint16_t link_len = 0;
            memcpy(&link_len, data + 2 + target_len, sizeof(uint16_t));

            if (data_len < 4 + target_len + link_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_SYMLINK;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            // Null-terminate target
            std::array<char, 512> target_str{};
            size_t copy_tlen = std::min<size_t>(target_len, target_str.size() - 1);
            memcpy(target_str.data(), data + 2, copy_tlen);

            // Build full link path
            std::array<char, 512> full_link{};
            build_full_path(full_link.data(), full_link.size(), export_path, reinterpret_cast<const char*>(data + 4 + target_len),
                            link_len);

            if (path_crosses_recursive_wki_boundary(full_link.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_SYMLINK;
                resp.status = -EPERM;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            int ret = ker::vfs::vfs_symlink(target_str.data(), full_link.data());

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_SYMLINK;
            resp.status = static_cast<int16_t>(ret);
            resp.data_len = 0;
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }

            // =====================================================================
            // V2: Missing VFS operations
            // =====================================================================

        case OP_VFS_UNLINK: {
            // Request: {path_len:u16, path[path_len]}
            if (data_len < 2) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_UNLINK;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            uint16_t path_len = 0;
            memcpy(&path_len, data, sizeof(uint16_t));
            if (data_len < 2 + path_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_UNLINK;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            std::array<char, 512> full_path{};
            std::array<char, 512> full_visible_path{};
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 2), path_len);
            build_full_path(full_visible_path.data(), full_visible_path.size(), export_name, reinterpret_cast<const char*>(data + 2),
                            path_len);

            if (path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_UNLINK;
                resp.status = -EPERM;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            int ret = ker::vfs::vfs_unlink(full_visible_path.data());

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_UNLINK;
            resp.status = static_cast<int16_t>(ret);
            resp.data_len = 0;
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }

        case OP_VFS_RMDIR: {
            // Request: {path_len:u16, path[path_len]}
            if (data_len < 2) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_RMDIR;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            uint16_t path_len = 0;
            memcpy(&path_len, data, sizeof(uint16_t));
            if (data_len < 2 + path_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_RMDIR;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            std::array<char, 512> full_path{};
            std::array<char, 512> full_visible_path{};
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 2), path_len);
            build_full_path(full_visible_path.data(), full_visible_path.size(), export_name, reinterpret_cast<const char*>(data + 2),
                            path_len);

            if (path_crosses_recursive_wki_boundary_direct(full_path.data(), full_visible_path.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_RMDIR;
                resp.status = -EPERM;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            int ret = ker::vfs::vfs_rmdir(full_visible_path.data());

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_RMDIR;
            resp.status = static_cast<int16_t>(ret);
            resp.data_len = 0;
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }

        case OP_VFS_RENAME: {
            // Request: {old_path_len:u16, old_path[], new_path_len:u16, new_path[]}
            if (data_len < 4) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_RENAME;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            uint16_t old_len = 0;
            memcpy(&old_len, data, sizeof(uint16_t));
            if (data_len < 4 + old_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_RENAME;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            uint16_t new_len = 0;
            memcpy(&new_len, data + 2 + old_len, sizeof(uint16_t));
            if (data_len < 4 + old_len + new_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_RENAME;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            std::array<char, 512> old_full{};
            build_full_path(old_full.data(), old_full.size(), export_path, reinterpret_cast<const char*>(data + 2), old_len);

            std::array<char, 512> new_full{};
            build_full_path(new_full.data(), new_full.size(), export_path, reinterpret_cast<const char*>(data + 4 + old_len), new_len);

            if (path_crosses_recursive_wki_boundary(old_full.data()) || path_crosses_recursive_wki_boundary(new_full.data())) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_RENAME;
                resp.status = -EPERM;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            int ret = ker::vfs::vfs_rename(old_full.data(), new_full.data());

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_RENAME;
            resp.status = static_cast<int16_t>(ret);
            resp.data_len = 0;
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }

        case OP_VFS_FSYNC: {
            // Request: {remote_fd:i32}
            if (data_len < 4) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_FSYNC;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            int32_t fd_id = 0;
            memcpy(&fd_id, data, sizeof(int32_t));

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            int ret = -1;
            if (rfd != nullptr && rfd->file != nullptr) {
                touch_remote_fd(rfd);
                // Flush write-through: fsync the underlying file if possible
                // Most in-kernel filesystems (tmpfs, fat32) write immediately,
                // so this is effectively a no-op but ensures correctness.
                ret = 0;
            }
            s_vfs_lock.unlock();

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_FSYNC;
            resp.status = static_cast<int16_t>(ret);
            resp.data_len = 0;
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }

        case OP_VFS_TRUNCATE: {
            // Request: {remote_fd:i32, length:i64} = 12 bytes
            if (data_len < 12) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_TRUNCATE;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            int32_t fd_id = 0;
            int64_t length = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&length, data + 4, sizeof(int64_t));

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            ker::vfs::File* local_file = nullptr;
            int ret = -1;
            if (rfd != nullptr && rfd->file != nullptr && rfd->file->fops != nullptr && rfd->file->fops->vfs_truncate != nullptr) {
                touch_remote_fd(rfd);
                local_file = rfd->file;
            }
            s_vfs_lock.unlock();

            if (local_file != nullptr) {
                ret = local_file->fops->vfs_truncate(local_file, static_cast<off_t>(length));
            }

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_TRUNCATE;
            resp.status = static_cast<int16_t>(ret);
            resp.data_len = 0;
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }

        case OP_VFS_SEEK_END: {
            // Request: {remote_fd:i32, offset:i64} = 12 bytes
            // Response: {status:i32, pos:i64} = 12 bytes
            if (data_len < 12) {
                ker::mod::dbg::log("[WKI-SRV] SEEK_END: data_len too short (%u < 12)", data_len);
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_SEEK_END;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = req_cookie;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            int32_t fd_id = 0;
            int64_t offset = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&offset, data + 4, sizeof(int64_t));

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            int64_t new_pos = -1;
            int ret = -1;
            ker::vfs::File* local_file = nullptr;

            if (rfd == nullptr) {
                ker::mod::dbg::log("[WKI-SRV] SEEK_END: find_remote_fd FAILED node=0x%04x ch=%u fd=%d", hdr->src_node, channel_id, fd_id);
            } else if (rfd->file == nullptr) {
                ker::mod::dbg::log("[WKI-SRV] SEEK_END: rfd->file is NULL fd=%d", fd_id);
            } else if (rfd->file->fops == nullptr) {
                ker::mod::dbg::log("[WKI-SRV] SEEK_END: fops is NULL fd=%d", fd_id);
            } else if (rfd->file->fops->vfs_lseek == nullptr) {
                ker::mod::dbg::log("[WKI-SRV] SEEK_END: fops->vfs_lseek is NULL fd=%d fs_type=%d", fd_id, (int)rfd->file->fs_type);
            } else {
                touch_remote_fd(rfd);
                local_file = rfd->file;
            }
            s_vfs_lock.unlock();

            if (local_file != nullptr) {
                off_t pos = local_file->fops->vfs_lseek(local_file, static_cast<off_t>(offset), 2 /* SEEK_END */);
#ifdef DEBUG_WKI_VFS
                ker::mod::dbg::log("[WKI-SRV] SEEK_END: fd=%d underlying_lseek returned %ld", fd_id, (long)pos);
#endif
                if (pos >= 0) {
                    new_pos = pos;
                    ret = 0;
                }
            }

            // Response: {pos:i64} = 8 bytes
            std::array<uint8_t, sizeof(DevOpRespPayload) + 8> resp_buf{};
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = OP_VFS_SEEK_END;
            resp->status = static_cast<int16_t>(ret);
            resp->data_len = 8;
            resp->reserved = req_cookie;
            memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &new_pos, sizeof(int64_t));
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf.data(), static_cast<uint16_t>(sizeof(DevOpRespPayload) + 8));
            break;
        }

        case OP_VFS_READ_RDMA: {
            // Request: {fd:i32, len:u32, off:i64, consumer_rkey:u32} = 20 bytes
            // Two modes depending on transport:
            //   Push (ivshmem): rdma_write data to consumer's bounce buf (consumer_rkey), send tiny resp.
            //   Pull (RoCE):    stage data in server's read staging buf; consumer rdma_reads after resp.
            auto send_rdma_read_err = [&]() {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ_RDMA;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = req_cookie;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            };

            if (data_len < 20) {
                send_rdma_read_err();
                break;
            }

            int32_t fd_id = 0;
            uint32_t len = 0;
            int64_t offset = 0;
            uint32_t consumer_rkey = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&len, data + 4, sizeof(uint32_t));
            memcpy(&offset, data + 8, sizeof(int64_t));
            memcpy(&consumer_rkey, data + 16, sizeof(uint32_t));

            len = std::min<uint32_t>(len, VFS_RDMA_BOUNCE_SIZE);

            // Determine mode: pull if server has a read staging buf for this binding.
            uint8_t* read_staging = wki_dev_server_get_vfs_read_staging_buf(hdr->src_node, channel_id);
            WkiPeer* rdma_peer = nullptr;
            if (read_staging == nullptr) {
                // Push mode: need rdma_write capability.
                rdma_peer = wki_peer_find(hdr->src_node);
                if (rdma_peer == nullptr || rdma_peer->rdma_transport == nullptr || rdma_peer->rdma_transport->rdma_write == nullptr) {
                    send_rdma_read_err();
                    break;
                }
            }

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_read == nullptr) {
                s_vfs_lock.unlock();
                send_rdma_read_err();
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            // Allocate a temporary buffer, read into it, then either push or stage.
            auto* read_buf = new (std::nothrow) uint8_t[len];
            if (read_buf == nullptr) {
                send_rdma_read_err();
                break;
            }

            ssize_t bytes_read = local_file->fops->vfs_read(local_file, read_buf, len, static_cast<size_t>(offset));
            if (bytes_read > 0) {
                if (read_staging != nullptr) {
                    // Pull mode: copy into server staging buf; client will rdma_read from it.
                    memcpy(read_staging, read_buf, static_cast<uint32_t>(bytes_read));
                } else {
                    // Push mode: rdma_write data directly into consumer's bounce buf.
                    rdma_peer->rdma_transport->rdma_write(rdma_peer->rdma_transport, hdr->src_node, consumer_rkey, 0, read_buf,
                                                          static_cast<uint32_t>(bytes_read));
                }
            }
            delete[] read_buf;

            // Response: {bytes_read:u32} = 4 bytes (data is now in consumer bounce buf)
            uint32_t br = (bytes_read > 0) ? static_cast<uint32_t>(bytes_read) : 0;
            std::array<uint8_t, sizeof(DevOpRespPayload) + 4> resp_buf{};
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = OP_VFS_READ_RDMA;
            resp->status = (bytes_read >= 0) ? 0 : static_cast<int16_t>(bytes_read);
            resp->data_len = 4;
            resp->reserved = req_cookie;
            memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &br, sizeof(uint32_t));
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf.data(), static_cast<uint16_t>(resp_buf.size()));
            break;
        }

        case OP_VFS_READ_BULK: {
            // Bulk RDMA read - identical request layout to OP_VFS_READ_RDMA but len
            // is not capped to 64 KB.
            // Two modes:
            //   Push (ivshmem): rdma_write up to 2 MB into consumer's bulk buf (consumer_rkey).
            //   Pull (RoCE):    stage data in server's bulk staging buf; client rdma_reads after resp.
            auto send_bulk_read_err = [&]() {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ_BULK;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = req_cookie;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            };

            if (data_len < 20) {
                send_bulk_read_err();
                break;
            }

            int32_t fd_id = 0;
            uint32_t len = 0;
            int64_t offset = 0;
            uint32_t consumer_rkey = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&len, data + 4, sizeof(uint32_t));
            memcpy(&offset, data + 8, sizeof(int64_t));
            memcpy(&consumer_rkey, data + 16, sizeof(uint32_t));

            // Cap to the consumer's registered bulk size (VFS_RDMA_BULK_SIZE)
            len = std::min<uint32_t>(len, VFS_RDMA_BULK_SIZE);

            // Determine mode: pull if server has a bulk staging buf for this binding.
            uint8_t* bulk_staging = wki_dev_server_get_vfs_bulk_staging_buf(hdr->src_node, channel_id);
            WkiPeer* bulk_peer = nullptr;
            if (bulk_staging == nullptr) {
                bulk_peer = wki_peer_find(hdr->src_node);
                if (bulk_peer == nullptr || bulk_peer->rdma_transport == nullptr || bulk_peer->rdma_transport->rdma_write == nullptr) {
                    send_bulk_read_err();
                    break;
                }
            }

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_read == nullptr) {
                s_vfs_lock.unlock();
                send_bulk_read_err();
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            auto* read_buf = new (std::nothrow) uint8_t[len];
            if (read_buf == nullptr) {
                send_bulk_read_err();
                break;
            }

            ssize_t bytes_read = local_file->fops->vfs_read(local_file, read_buf, len, static_cast<size_t>(offset));
            if (bytes_read > 0) {
                if (bulk_staging != nullptr) {
                    // Pull mode: copy into server staging buf; client will rdma_read from it.
                    memcpy(bulk_staging, read_buf, static_cast<uint32_t>(bytes_read));
                } else {
                    // Push mode: rdma_write directly into consumer's bulk buf.
                    bulk_peer->rdma_transport->rdma_write(bulk_peer->rdma_transport, hdr->src_node, consumer_rkey, 0, read_buf,
                                                          static_cast<uint32_t>(bytes_read));
                }
            }
            delete[] read_buf;

            uint32_t br = (bytes_read > 0) ? static_cast<uint32_t>(bytes_read) : 0;
            std::array<uint8_t, sizeof(DevOpRespPayload) + 4> bulk_resp_buf{};
            auto* bulk_resp = reinterpret_cast<DevOpRespPayload*>(bulk_resp_buf.data());
            bulk_resp->op_id = OP_VFS_READ_BULK;
            bulk_resp->status = (bytes_read >= 0) ? 0 : static_cast<int16_t>(bytes_read);
            bulk_resp->data_len = 4;
            bulk_resp->reserved = req_cookie;
            memcpy(bulk_resp_buf.data() + sizeof(DevOpRespPayload), &br, sizeof(uint32_t));
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, bulk_resp_buf.data(), static_cast<uint16_t>(bulk_resp_buf.size()));
            break;
        }

        case OP_VFS_WRITE_RDMA: {
            // Request: {fd:i32, off:i64, len:u32} = 16 bytes
            // Consumer already rdma_wrote the data into our pre-registered write buffer
            // before sending this control message.
            auto send_rdma_write_err = [&]() {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_WRITE_RDMA;
                resp.status = -1;
                resp.data_len = 0;
                send_simple_resp(resp);
            };

            if (data_len < 16) {
                send_rdma_write_err();
                break;
            }

            int32_t fd_id = 0;
            int64_t offset = 0;
            uint32_t len = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&offset, data + 4, sizeof(int64_t));
            memcpy(&len, data + 12, sizeof(uint32_t));

            len = std::min<uint32_t>(len, VFS_RDMA_BOUNCE_SIZE);

            uint8_t* write_src = wki_dev_server_get_vfs_write_buf(hdr->src_node, channel_id);
            if (write_src == nullptr) {
                send_rdma_write_err();
                break;
            }

            s_vfs_lock.lock();
            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_write == nullptr) {
                s_vfs_lock.unlock();
                send_rdma_write_err();
                break;
            }
            touch_remote_fd(rfd);
            ker::vfs::File* local_file = rfd->file;
            s_vfs_lock.unlock();

            ssize_t bytes_written = local_file->fops->vfs_write(local_file, write_src, len, static_cast<size_t>(offset));

            // Response: {bytes_written:u32} = 4 bytes
            uint32_t bw = (bytes_written > 0) ? static_cast<uint32_t>(bytes_written) : 0;
            std::array<uint8_t, sizeof(DevOpRespPayload) + 4> resp_buf{};
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = OP_VFS_WRITE_RDMA;
            resp->status = (bytes_written >= 0) ? 0 : static_cast<int16_t>(bytes_written);
            resp->data_len = 4;
            memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &bw, sizeof(uint32_t));
            send_buffered_resp(resp_buf.data(), static_cast<uint16_t>(resp_buf.size()));
            break;
        }

        default: {
            DevOpRespPayload resp = {};
            resp.op_id = op_id;
            resp.status = -1;
            resp.data_len = 0;
            send_simple_resp(resp);
            break;
        }
    }
}

}  // namespace detail

// -------------------------------------------------------------------------------
// Consumer Side - Mount / Open / Response Handlers
// -------------------------------------------------------------------------------

auto wki_remote_vfs_mount(uint16_t owner_node, uint32_t resource_id, const char* local_mount_path) -> int {
    if (local_mount_path == nullptr) {
        return -1;
    }

    // Allocate proxy state
    s_vfs_lock.lock();
    g_vfs_proxies.push_back(std::make_unique<ProxyVfsState>());
    auto* state = g_vfs_proxies.back().get();

    state->owner_node = owner_node;
    state->resource_id = resource_id;

    size_t path_len = strlen(local_mount_path);
    if (path_len >= VFS_EXPORT_PATH_LEN) {
        path_len = VFS_EXPORT_PATH_LEN - 1;
    }
    memcpy(static_cast<void*>(state->local_mount_path), local_mount_path, path_len);
    state->local_mount_path[path_len] = '\0';

    WkiWaitEntry wait = {};
    state->attach_wait_entry = &wait;
    state->attach_pending.store(true, std::memory_order_release);
    state->attach_status = 0;
    state->attach_channel = 0;
    s_vfs_lock.unlock();

    // Send DEV_ATTACH_REQ
    DevAttachReqPayload attach_req = {};
    attach_req.target_node = owner_node;
    attach_req.resource_type = static_cast<uint16_t>(ResourceType::VFS);
    attach_req.resource_id = resource_id;
    attach_req.attach_mode = static_cast<uint8_t>(AttachMode::PROXY);

    WkiChannel* reserved_channel = nullptr;
    if (wki_requester_controls_dynamic_channel(g_wki.my_node_id, owner_node)) {
        reserved_channel = wki_channel_alloc(owner_node, PriorityClass::THROUGHPUT);
        if (reserved_channel == nullptr) {
            state->attach_wait_entry = nullptr;
            state->attach_pending.store(false, std::memory_order_relaxed);
            s_vfs_lock.lock();
            g_vfs_proxies.pop_back();
            s_vfs_lock.unlock();
            return -1;
        }
        attach_req.requested_channel = reserved_channel->channel_id;
    } else {
        attach_req.requested_channel = 0;
    }

    int send_ret = wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ, &attach_req, sizeof(attach_req));
    if (send_ret != WKI_OK) {
        state->attach_wait_entry = nullptr;
        state->attach_pending.store(false, std::memory_order_relaxed);
        if (reserved_channel != nullptr) {
            wki_channel_close(reserved_channel);
        }
        s_vfs_lock.lock();
        g_vfs_proxies.pop_back();
        s_vfs_lock.unlock();
        return -1;
    }

    uint64_t attach_started_us = wki_now_us();
    int wait_rc = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    state->attach_wait_entry = nullptr;
    perf_record_vfs_point(static_cast<uint8_t>(ker::mod::perf::WkiPerfVfsOp::ATTACH_WAIT), owner_node, state->attach_channel, wait_rc,
                          static_cast<uint32_t>(wki_now_us() - attach_started_us), WOS_PERF_CALLSITE());
    if (wait_rc != 0) {
        state->attach_pending.store(false, std::memory_order_relaxed);
        if (reserved_channel != nullptr) {
            wki_channel_close(reserved_channel);
        }
        s_vfs_lock.lock();
        g_vfs_proxies.pop_back();
        s_vfs_lock.unlock();
        if (wait_rc == WKI_ERR_TIMEOUT) {
            ker::mod::dbg::log("[WKI] Remote VFS attach timeout: node=0x%04x res_id=%u", owner_node, resource_id);
        } else {
            ker::mod::dbg::log("[WKI] Remote VFS attach aborted: node=0x%04x res_id=%u rc=%d", owner_node, resource_id, wait_rc);
        }
        return -1;
    }

    if (state->attach_status != static_cast<uint8_t>(DevAttachStatus::OK)) {
        ker::mod::dbg::log("[WKI] Remote VFS attach rejected: status=%u", state->attach_status);
        if (reserved_channel != nullptr) {
            wki_channel_close(reserved_channel);
        }
        s_vfs_lock.lock();
        g_vfs_proxies.pop_back();
        s_vfs_lock.unlock();
        return -1;
    }

    if (reserved_channel != nullptr) {
        if (state->attach_channel != reserved_channel->channel_id) {
            ker::mod::dbg::log("[WKI] Remote VFS attach channel mismatch: node=0x%04x res_id=%u requested=%u assigned=%u", owner_node,
                               resource_id, reserved_channel->channel_id, state->attach_channel);
            DevDetachPayload det = {};
            det.target_node = owner_node;
            det.resource_type = static_cast<uint16_t>(ResourceType::VFS);
            det.resource_id = resource_id;
            wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));
            wki_channel_close(reserved_channel);
            s_vfs_lock.lock();
            g_vfs_proxies.pop_back();
            s_vfs_lock.unlock();
            return -1;
        }
    } else {
        reserved_channel = wki_channel_reserve(owner_node, state->attach_channel, PriorityClass::THROUGHPUT);
        if (reserved_channel == nullptr) {
            ker::mod::dbg::log("[WKI] Remote VFS attach local reserve failed: node=0x%04x res_id=%u ch=%u", owner_node, resource_id,
                               state->attach_channel);
            DevDetachPayload det = {};
            det.target_node = owner_node;
            det.resource_type = static_cast<uint16_t>(ResourceType::VFS);
            det.resource_id = resource_id;
            wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));
            s_vfs_lock.lock();
            g_vfs_proxies.pop_back();
            s_vfs_lock.unlock();
            return -1;
        }
    }

    s_vfs_lock.lock();
    state->assigned_channel = reserved_channel->channel_id;
    state->max_op_size = state->attach_max_op_size;
    state->active = true;
    s_vfs_lock.unlock();

    // RDMA setup: if the server advertised a write-recv rkey, register a local
    // bounce buffer so the server can rdma_write file data directly into it.
    if (state->rdma_server_write_rkey != 0) {
        WkiPeer* peer = wki_peer_find(owner_node);
        if (peer != nullptr && peer->rdma_transport != nullptr && peer->rdma_transport->rdma_register_region != nullptr) {
            auto* bbuf = new (std::nothrow) uint8_t[VFS_RDMA_BOUNCE_SIZE];
            if (bbuf != nullptr) {
                uint32_t rkey = 0;
                int reg_ret = peer->rdma_transport->rdma_register_region(peer->rdma_transport, reinterpret_cast<uint64_t>(bbuf),
                                                                         VFS_RDMA_BOUNCE_SIZE, &rkey);
                if (reg_ret == 0 && rkey != 0) {
                    state->rdma_bounce_buf = bbuf;
                    state->rdma_read_rkey = rkey;
                    state->rdma_transport = peer->rdma_transport;
                    state->rdma_capable = true;
                    ker::mod::dbg::log(
                        "[WKI] VFS RDMA enabled: node=0x%04x read_rkey=%u write_rkey=%u read_staging_rkey=%u bulk_staging_rkey=%u",
                        owner_node, rkey, state->rdma_server_write_rkey, state->rdma_server_read_staging_rkey,
                        state->rdma_server_bulk_staging_rkey);
                } else {
                    delete[] bbuf;
                    state->rdma_server_write_rkey = 0;
                    ker::mod::dbg::log("[WKI] VFS RDMA region reg failed: node=0x%04x", owner_node);
                }
            } else {
                state->rdma_server_write_rkey = 0;
            }
        } else {
            state->rdma_server_write_rkey = 0;
        }
    }

    // Bulk RDMA setup: allocate and register a larger buffer (2 MB) for bulk reads.
    // This is separate from the 64 KB bounce buffer so that small reads and writes
    // still use the compact buffer while large sequential reads benefit from fewer
    // round-trips.
    if (state->rdma_capable && state->rdma_transport != nullptr && state->rdma_transport->rdma_register_region != nullptr) {
        auto* bulk_buf = new (std::nothrow) uint8_t[VFS_RDMA_BULK_SIZE];
        if (bulk_buf != nullptr) {
            uint32_t bulk_rkey = 0;
            int reg_ret = state->rdma_transport->rdma_register_region(state->rdma_transport, reinterpret_cast<uint64_t>(bulk_buf),
                                                                      VFS_RDMA_BULK_SIZE, &bulk_rkey);
            if (reg_ret == 0 && bulk_rkey != 0) {
                state->rdma_bulk_buf = bulk_buf;
                state->rdma_bulk_rkey = bulk_rkey;
                state->rdma_bulk_size = VFS_RDMA_BULK_SIZE;
                state->bulk_rdma_capable = true;
                ker::mod::dbg::log("[WKI] VFS bulk RDMA enabled: node=0x%04x bulk_rkey=%u size=%u", owner_node, bulk_rkey,
                                   VFS_RDMA_BULK_SIZE);
            } else {
                delete[] bulk_buf;
                ker::mod::dbg::log("[WKI] VFS bulk RDMA region reg failed: node=0x%04x", owner_node);
            }
        }
    }

    // Create the mount point with "remote" fstype
    int mount_ret = ker::vfs::mount_filesystem(local_mount_path, "remote", nullptr);
    if (mount_ret != 0) {
        ker::mod::dbg::log("[WKI] Remote VFS mount failed at %s", local_mount_path);
        s_vfs_lock.lock();
        state->active = false;
        g_vfs_proxies.pop_back();
        s_vfs_lock.unlock();
        return -1;
    }

    // Configure the exact REMOTE mount we just created.  During pivot_root(),
    // mount_filesystem() and this lookup can observe different task roots, so
    // try both the current-root-resolved path and the raw mount path.  Never use
    // longest-prefix lookup here: if the exact mount is temporarily invisible,
    // that can return the containing XFS root and corrupt its private_data.
    char resolved_mount[512] = {};
    bool configured = false;
    if (ker::vfs::resolve_mount_path(local_mount_path, resolved_mount, sizeof(resolved_mount)) == 0) {
        configured = ker::vfs::configure_mount_point_exact(resolved_mount, ker::vfs::FSType::REMOTE, state, &g_remote_vfs_fops);
    }
    if (!configured) {
        configured = ker::vfs::configure_mount_point_exact(local_mount_path, ker::vfs::FSType::REMOTE, state, &g_remote_vfs_fops);
    }
    if (!configured) {
        ker::mod::dbg::log("[WKI] Remote VFS mount configuration failed at %s", local_mount_path);
        ker::vfs::unmount_filesystem(local_mount_path);
        s_vfs_lock.lock();
        state->active = false;
        g_vfs_proxies.pop_back();
        s_vfs_lock.unlock();
        return -1;
    }

    ker::mod::dbg::log("[WKI] Remote VFS mounted: %s -> node=0x%04x res_id=%u ch=%u", local_mount_path, owner_node, resource_id,
                       state->assigned_channel);
    return 0;
}

void wki_remote_vfs_unmount(const char* local_mount_path) {
    if (local_mount_path == nullptr) {
        return;
    }

    s_vfs_lock.lock();
    auto* state = find_vfs_proxy_by_mount(local_mount_path);
    if (state == nullptr) {
        s_vfs_lock.unlock();
        return;
    }

    // Capture what we need before unlock
    uint16_t owner_node = state->owner_node;
    uint16_t assigned_channel = state->assigned_channel;
    uint32_t resource_id = state->resource_id;
    state->active = false;

    ker::vfs::vfs_stream_cache_invalidate_remote_scope(state);
    s_vfs_lock.unlock();

    // Send DEV_DETACH
    DevDetachPayload det = {};
    det.target_node = owner_node;
    det.resource_type = static_cast<uint16_t>(ResourceType::VFS);
    det.resource_id = resource_id;
    wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));

    // Close the dynamic channel
    WkiChannel* ch = wki_channel_get(owner_node, assigned_channel);
    if (ch != nullptr) {
        wki_channel_close(ch);
    }

    // Unmount
    ker::vfs::unmount_filesystem(local_mount_path);
}

auto wki_remote_vfs_find_mount_for_resource(uint16_t owner_node, uint32_t resource_id, char* out, size_t out_size) -> bool {
    if (out == nullptr || out_size == 0) {
        return false;
    }

    out[0] = '\0';

    s_vfs_lock.lock();
    for (auto& proxy : g_vfs_proxies) {
        if (!proxy->active || proxy->owner_node != owner_node || proxy->resource_id != resource_id) {
            continue;
        }

        size_t mount_len = std::strlen(proxy->local_mount_path);
        if (mount_len + 1 > out_size) {
            break;
        }

        std::memcpy(out, proxy->local_mount_path, mount_len + 1);
        s_vfs_lock.unlock();
        return true;
    }
    s_vfs_lock.unlock();

    return false;
}

auto wki_remote_vfs_open_path(const char* fs_relative_path, int flags, int mode, void* mount_private_data) -> ker::vfs::File* {
    auto* state = static_cast<ProxyVfsState*>(mount_private_data);
    if (state == nullptr || !state->active) {
        return nullptr;
    }

    // Build request: {flags:u32, mode:u32, path_len:u16, path[N]}
    auto path_len = static_cast<uint16_t>(strlen(fs_relative_path));
    auto req_data_len = static_cast<uint16_t>(10 + path_len);
    auto* req_data = new (std::nothrow) uint8_t[req_data_len + 1];
    if (req_data == nullptr) {
        return nullptr;
    }

    auto u_flags = static_cast<uint32_t>(flags);
    auto u_mode = static_cast<uint32_t>(mode);
    memcpy(req_data, &u_flags, sizeof(uint32_t));
    memcpy(req_data + 4, &u_mode, sizeof(uint32_t));
    memcpy(req_data + 8, &path_len, sizeof(uint16_t));
    if (path_len > 0) {
        strcpy(reinterpret_cast<char*>(req_data + 10), fs_relative_path);
    }

    // Response: {remote_fd:i32, is_dir:u8} = 5 bytes
    struct {
        int32_t fd;
        uint8_t is_dir;
    } __attribute__((packed)) open_resp = {};
    int status = vfs_proxy_send_and_wait(state, OP_VFS_OPEN, req_data, req_data_len, &open_resp, sizeof(open_resp));
    delete[] req_data;

    if (status != 0 || open_resp.fd < 0) {
        return nullptr;
    }

    // Allocate File + RemoteFileContext
    auto* file = new (std::nothrow) ker::vfs::File{};
    if (file == nullptr) {
        return nullptr;
    }

    auto* ctx = new (std::nothrow) RemoteFileContext{};
    if (ctx == nullptr) {
        delete file;
        return nullptr;
    }

    ctx->proxy = state;
    ctx->remote_fd = open_resp.fd;

    const char* path = fs_relative_path;
    while (path != nullptr && *path == '/') {
        path++;
    }
    if (path != nullptr && std::strcmp(path, "wki") == 0) {
        ctx->hide_recursive_wki_entries = true;
        const char* owner_hostname = wki_peer_get_hostname(state->owner_node);
        if (owner_hostname != nullptr) {
            std::strncpy(ctx->recursive_wki_hostname, owner_hostname, sizeof(ctx->recursive_wki_hostname) - 1);
            ctx->recursive_wki_hostname[sizeof(ctx->recursive_wki_hostname) - 1] = '\0';
        }
    }

    file->fd = -1;  // Will be assigned by vfs_alloc_fd
    file->private_data = ctx;
    file->fops = &g_remote_vfs_fops;
    file->pos = 0;
    file->is_directory = (open_resp.is_dir != 0);
    file->fs_type = ker::vfs::FSType::REMOTE;
    file->refcount = 1;
    file->open_flags = flags;
    file->fd_flags = 0;
    file->vfs_path = nullptr;                      // Set by vfs_open after return
    file->dir_fs_count = static_cast<size_t>(-1);  // Unknown FS entry count

    return file;
}

auto wki_remote_vfs_stat(void* mount_private_data, const char* fs_relative_path, ker::vfs::stat* statbuf) -> int {
    auto* state = static_cast<ProxyVfsState*>(mount_private_data);
    if (state == nullptr || !state->active || statbuf == nullptr) {
        return -1;
    }

    // Build request: {path_len:u16, path[N]}
    auto path_len = static_cast<uint16_t>(strlen(fs_relative_path));
    auto req_data_len = static_cast<uint16_t>(2 + path_len);
    std::array<uint8_t, 514> req_stack{};  // NOLINT(modernize-avoid-c-arrays)
    uint8_t* req_data = req_stack.data();

    memcpy(req_data, &path_len, sizeof(uint16_t));
    if (path_len > 0) {
        strcpy(reinterpret_cast<char*>(req_data + 2), fs_relative_path);
    }

    // Use a kernel-stack buffer for the proxy response.  The caller's statbuf
    // may be a user-space pointer, but handle_vfs_op_resp can run in netpoll
    // context where user pages are not mapped.
    ker::vfs::stat kernel_buf{};
    int status = vfs_proxy_send_and_wait(state, OP_VFS_STAT, req_data, req_data_len, &kernel_buf, sizeof(ker::vfs::stat));
    if (status == 0) {
        memcpy(statbuf, &kernel_buf, sizeof(ker::vfs::stat));
    }
    return status;
}

auto wki_remote_vfs_mkdir(void* mount_private_data, const char* fs_relative_path, int mode) -> int {
    auto* state = static_cast<ProxyVfsState*>(mount_private_data);
    if (state == nullptr || !state->active) {
        return -1;
    }

    // Build request: {mode:u32, path_len:u16, path[N]}
    auto path_len = static_cast<uint16_t>(strlen(fs_relative_path));
    auto req_data_len = static_cast<uint16_t>(6 + path_len);
    std::array<uint8_t, 518> req_stack{};  // NOLINT(modernize-avoid-c-arrays)
    uint8_t* req_data = req_stack.data();

    auto u_mode = static_cast<uint32_t>(mode);
    memcpy(req_data, &u_mode, sizeof(uint32_t));
    memcpy(req_data + 4, &path_len, sizeof(uint16_t));
    if (path_len > 0) {
        strcpy(reinterpret_cast<char*>(req_data + 6), fs_relative_path);
    }

    int status = vfs_proxy_send_and_wait(state, OP_VFS_MKDIR, req_data, req_data_len, nullptr, 0);
    if (status == 0) {
        invalidate_readlink_cache(state);
    }
    return status;
}

// Consumer side: unlink a file on the remote server
auto wki_remote_vfs_unlink(void* mount_private_data, const char* fs_relative_path) -> int {
    auto* state = static_cast<ProxyVfsState*>(mount_private_data);
    if (state == nullptr || !state->active) return -1;

    auto path_len = static_cast<uint16_t>(strlen(fs_relative_path));
    auto req_data_len = static_cast<uint16_t>(2 + path_len);
    std::array<uint8_t, 514> req_stack{};
    uint8_t* req_data = req_stack.data();

    memcpy(req_data, &path_len, sizeof(uint16_t));
    if (path_len > 0) {
        strcpy(reinterpret_cast<char*>(req_data + 2), fs_relative_path);
    }

    int status = vfs_proxy_send_and_wait(state, OP_VFS_UNLINK, req_data, req_data_len, nullptr, 0);
    if (status == 0) {
        invalidate_readlink_cache(state);
    }
    return status;
}

// Consumer side: remove a directory on the remote server
auto wki_remote_vfs_rmdir(void* mount_private_data, const char* fs_relative_path) -> int {
    auto* state = static_cast<ProxyVfsState*>(mount_private_data);
    if (state == nullptr || !state->active) return -1;

    auto path_len = static_cast<uint16_t>(strlen(fs_relative_path));
    auto req_data_len = static_cast<uint16_t>(2 + path_len);
    std::array<uint8_t, 514> req_stack{};
    uint8_t* req_data = req_stack.data();

    memcpy(req_data, &path_len, sizeof(uint16_t));
    if (path_len > 0) {
        strcpy(reinterpret_cast<char*>(req_data + 2), fs_relative_path);
    }

    int status = vfs_proxy_send_and_wait(state, OP_VFS_RMDIR, req_data, req_data_len, nullptr, 0);
    if (status == 0) {
        invalidate_readlink_cache(state);
    }
    return status;
}

// Consumer side: rename a file/directory on the remote server
auto wki_remote_vfs_rename(void* mount_private_data, const char* old_fs_path, const char* new_fs_path) -> int {
    auto* state = static_cast<ProxyVfsState*>(mount_private_data);
    if (state == nullptr || !state->active) {
        return -1;
    }

    auto old_len = static_cast<uint16_t>(strlen(old_fs_path));
    auto new_len = static_cast<uint16_t>(strlen(new_fs_path));
    auto req_data_len = static_cast<uint16_t>(4 + old_len + new_len);
    std::array<uint8_t, 1028> req_stack{};
    uint8_t* req_data = req_stack.data();

    memcpy(req_data, &old_len, sizeof(uint16_t));
    if (old_len > 0) {
        strcpy(reinterpret_cast<char*>(req_data + 2), old_fs_path);
    }
    memcpy(req_data + 2 + old_len, &new_len, sizeof(uint16_t));
    if (new_len > 0) {
        strcpy(reinterpret_cast<char*>(req_data + 4 + old_len), new_fs_path);
    }

    int status = vfs_proxy_send_and_wait(state, OP_VFS_RENAME, req_data, req_data_len, nullptr, 0);
    if (status == 0) {
        invalidate_readlink_cache(state);
    }
    return status;
}

// Consumer side: readlink on a remote path (for vfs_readlink / resolve_symlinks)
auto wki_remote_vfs_readlink_path(void* mount_private_data, const char* fs_relative_path, char* buf, size_t bufsize) -> ssize_t {
    auto* state = static_cast<ProxyVfsState*>(mount_private_data);
    if (state == nullptr || !state->active || fs_relative_path == nullptr || buf == nullptr || bufsize == 0) {
        return -1;
    }

    // The root of a mounted remote export is the mount point itself, not a
    // symlink inside the export.
    if (fs_relative_path[0] == '\0') {
        return -EINVAL;
    }

    if (state->readlink_unsupported.load(std::memory_order_acquire)) {
        return -ENOSYS;
    }

    ssize_t cached_result = 0;
    if (try_readlink_cache_lookup(state, fs_relative_path, buf, bufsize, &cached_result)) {
        return cached_result;
    }

    // Build request: {path_len:u16, path[N]}
    auto path_len = static_cast<uint16_t>(strlen(fs_relative_path));
    auto req_data_len = static_cast<uint16_t>(2 + path_len);
    std::array<uint8_t, 514> req_stack{};
    uint8_t* req_data = req_stack.data();

    memcpy(req_data, &path_len, sizeof(uint16_t));
    if (path_len > 0) {
        strcpy(reinterpret_cast<char*>(req_data + 2), fs_relative_path);
    }

    // Response: {target_len:u16, target[]}
    std::array<uint8_t, 514> resp_buf{};
    uint16_t resp_len = 0;
    int status = vfs_proxy_read_with_retry(state, OP_VFS_READLINK, req_data, req_data_len, resp_buf.data(),
                                           static_cast<uint16_t>(resp_buf.size()), &resp_len, fs_relative_path);
    if (status != 0) {
        if (status == -ENOSYS) {
            state->readlink_unsupported.store(true, std::memory_order_release);
            return status;
        }

        if (status == -EINVAL || status == -ENOENT) {
            cache_readlink_result(state, fs_relative_path, status, nullptr, 0);
            return status;
        }

        ker::mod::dbg::log("[WKI] remote_vfs_readlink_path failed: node=0x%04x ch=%u path='%s' status=%d", state->owner_node,
                           state->assigned_channel, fs_relative_path, status);
        return status;
    }

    if (resp_len < 2) {
        return -1;
    }

    uint16_t target_len = 0;
    memcpy(&target_len, resp_buf.data(), sizeof(uint16_t));
    if (target_len == 0 || target_len + 2 > resp_len) {
        return -1;
    }

    auto to_copy = static_cast<size_t>(target_len);
    if (to_copy > bufsize) {
        to_copy = bufsize;
    }
    memcpy(buf, resp_buf.data() + 2, to_copy);
    cache_readlink_result(state, fs_relative_path, 0, reinterpret_cast<const char*>(resp_buf.data() + 2), target_len);
    return static_cast<ssize_t>(to_copy);
}

auto wki_remote_vfs_get_fops() -> ker::vfs::FileOperations* { return &g_remote_vfs_fops; }

// -------------------------------------------------------------------------------
// Consumer Side - RX Handlers
// -------------------------------------------------------------------------------

namespace detail {

void handle_vfs_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevAttachAckPayload)) {
        return;
    }

    const auto* ack = reinterpret_cast<const DevAttachAckPayload*>(payload);

    s_vfs_lock.lock();
    ProxyVfsState* state = find_vfs_proxy_by_attach(hdr->src_node, ack->resource_id);
    if (state == nullptr) {
        s_vfs_lock.unlock();
        return;
    }

    state->attach_status = ack->status;
    state->attach_channel = ack->assigned_channel;
    state->attach_max_op_size = ack->max_op_size;

    // Store server write-recv rkey if VFS RDMA is available
    if ((ack->rdma_flags & DEV_ATTACH_RDMA_VFS) != 0 && ack->blk_zone_id != 0) {
        state->rdma_server_write_rkey = ack->blk_zone_id;
    }
    // Store server-side read/bulk staging rkeys for RoCE pull mode
    if ((ack->rdma_flags & DEV_ATTACH_RDMA_VFS_READ) != 0 && ack->rdma_read_staging_rkey != 0) {
        state->rdma_server_read_staging_rkey = ack->rdma_read_staging_rkey;
    }
    if ((ack->rdma_flags & DEV_ATTACH_RDMA_BULK_PULL) != 0 && ack->rdma_bulk_staging_rkey != 0) {
        state->rdma_server_bulk_staging_rkey = ack->rdma_bulk_staging_rkey;
    }

    state->attach_pending.store(false, std::memory_order_release);
    if (state->attach_wait_entry != nullptr) {
        wki_wake_op(state->attach_wait_entry, 0);
    }
    s_vfs_lock.unlock();
}

void handle_vfs_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevOpRespPayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const DevOpRespPayload*>(payload);
    const uint8_t* resp_data = payload + sizeof(DevOpRespPayload);
    uint16_t resp_data_len = resp->data_len;

    if (sizeof(DevOpRespPayload) + resp_data_len > payload_len) {
        return;
    }

    // Match the response against the exact pending proxy on this channel.
    // Channel IDs can be reused, and under failure/recovery we can transiently
    // have more than one active proxy referencing the same channel.
    s_vfs_lock.lock();
    VfsProxyResponseLookup lookup = find_vfs_proxy_for_response_locked(hdr->src_node, hdr->channel_id, resp->op_id, resp->reserved);
    ProxyVfsState* state = lookup.matched_state;
    if (state == nullptr) {
        if (lookup.saw_pending_candidate) {
            ker::mod::dbg::log(
                "[WKI] Ignoring stale VFS response: node=0x%04x ch=%u resp_op=%u expected_op=%u resp_seq=%u expected_seq=%u "
                "active_candidates=%llu pending_candidates=%llu",
                hdr->src_node, hdr->channel_id, resp->op_id, lookup.stale_expected_op, resp->reserved, lookup.stale_expected_seq,
                static_cast<unsigned long long>(lookup.active_candidates), static_cast<unsigned long long>(lookup.pending_candidates));
        }
        s_vfs_lock.unlock();
        return;
    }

    state->op_status = resp->status;

    if (resp_data_len > 0 && state->op_resp_buf != nullptr) {
        uint16_t copy_len = (resp_data_len > state->op_resp_max) ? state->op_resp_max : resp_data_len;
        memcpy(state->op_resp_buf, resp_data, copy_len);
        state->op_resp_len = copy_len;
#ifdef WKI_DEBUG
        // Diagnostic: check if we received zeros from server
        if (resp->op_id == OP_VFS_READ && copy_len >= 4) {
            auto* d = static_cast<const uint8_t*>(state->op_resp_buf);
            auto* s = resp_data;
            ker::mod::dbg::log(
                "[WKI-CLI] op_resp: ch=%u op=%u len=%u"
                " src=[%02x %02x %02x %02x] dst=[%02x %02x %02x %02x]",
                hdr->channel_id, resp->op_id, copy_len, s[0], s[1], s[2], s[3], d[0], d[1], d[2], d[3]);
        }
#endif
    } else {
        state->op_resp_len = 0;
    }

    WkiWaitEntry* wait_entry = state->op_wait_entry;
    // Do NOT clear op_pending here. The waiter's success teardown in
    // vfs_proxy_send_and_wait will clear it while holding state->lock after
    // consuming the result. Clearing it here (outside state->lock) opens a
    // window where a new request can steal the slot and overwrite
    // op_expected_id/seq before the original waiter's teardown runs.
    state->lock.unlock();

    if (wait_entry != nullptr) {
        wki_wake_op(wait_entry, 0);
    }
    s_vfs_lock.unlock();
}

}  // namespace detail

// -------------------------------------------------------------------------------
// D10: Stale Remote FD Garbage Collection
// -------------------------------------------------------------------------------

constexpr uint64_t STALE_FD_TIMEOUT_US = 30000000;  // 30 seconds

void wki_remote_vfs_gc_stale_fds() {
    uint64_t now = wki_now_us();
    bool any_removed = false;

    for (auto& rfd : g_remote_fds) {
        if (!rfd.active) {
            continue;
        }

        // Only GC if the FD has been idle for a long time
        if (now - rfd.last_activity_us < STALE_FD_TIMEOUT_US) {
            continue;
        }

        // Only GC if the consumer peer is NOT connected (crashed/fenced without closing)
        WkiPeer* peer = wki_peer_find(rfd.consumer_node);
        if (peer != nullptr && peer->state == PeerState::CONNECTED) {
            continue;
        }

        // Stale: close the file and mark inactive
        if (rfd.file != nullptr) {
            if (rfd.file->fops != nullptr && rfd.file->fops->vfs_close != nullptr) {
                rfd.file->fops->vfs_close(rfd.file);
            }
            delete rfd.file;
            rfd.file = nullptr;
        }
        rfd.active = false;
        any_removed = true;
        ker::mod::dbg::log("[WKI] GC stale remote FD %d (consumer 0x%04x)", rfd.fd_id, rfd.consumer_node);
    }

    if (any_removed) {
        std::erase_if(g_remote_fds, [](const RemoteVfsFd& rfd) { return !rfd.active; });
    }
}

// -------------------------------------------------------------------------------
// D9: Auto-Discover Exportable Mount Points
// -------------------------------------------------------------------------------

void wki_remote_vfs_auto_discover_internal(std::deque<VfsExport>* stale_exports) {
    size_t mount_count = ker::vfs::get_mount_count();
    for (size_t i = 0; i < mount_count; i++) {
        auto* mp = ker::vfs::get_mount_at(i);
        if (mp == nullptr || mp->path == nullptr) {
            continue;
        }

        // Skip REMOTE mounts (don't re-export remote filesystems)
        // Skip DEVFS mounts (not meaningful to export)
        // Skip PROCFS mounts (remote /proc shows server processes, not client's)
        if (mp->fs_type == ker::vfs::FSType::REMOTE || mp->fs_type == ker::vfs::FSType::DEVFS || mp->fs_type == ker::vfs::FSType::PROCFS) {
            continue;
        }

        char export_name[VFS_EXPORT_NAME_LEN] = {};
        build_export_name(export_name, sizeof(export_name), mp->path);

        // Check if this visible export name is already exported.
        bool already_exported = false;
        for (const auto& exp : g_vfs_exports) {
            if (exp.active && strncmp(static_cast<const char*>(exp.name), export_name, VFS_EXPORT_NAME_LEN) == 0) {
                already_exported = true;
                break;
            }
        }
        if (already_exported) {
            continue;
        }

        // Export and advertise the visible path in the caller's current root
        // namespace. After pivot_root("/rootfs", ...), this yields "/",
        // "/boot", and "/oldroot", which resolve correctly for all tasks whose
        // root has been updated to "/rootfs".
        uint32_t preserved_resource_id = take_preserved_export_id(stale_exports, export_name);
        uint32_t resource_id = wki_remote_vfs_export_add_internal(mp->path, export_name, preserved_resource_id);
        if (resource_id != 0) {
            wki_dev_server_refresh_vfs_binding(resource_id, mp->path, export_name);
        }
    }
}

void wki_remote_vfs_auto_discover() {
    if (!g_remote_vfs_initialized) {
        return;
    }

    wki_remote_vfs_auto_discover_internal(nullptr);

    wki_remote_vfs_advertise_exports();
}

void wki_remote_vfs_refresh_exports() {
    if (!g_remote_vfs_initialized) {
        return;
    }

    wki_remote_vfs_auto_discover_internal(nullptr);
}

void wki_remote_vfs_rebuild_exports() {
    if (!g_remote_vfs_initialized) {
        return;
    }

    std::deque<VfsExport> stale_exports;
    s_vfs_lock.lock();
    for (const auto& exp : g_vfs_exports) {
        if (exp.active) {
            stale_exports.push_back(exp);
        }
    }
    g_vfs_exports.clear();
    g_next_vfs_resource_id = 0x1000;
    uint32_t max_stale_resource_id = max_preserved_export_id(stale_exports);
    if (g_next_vfs_resource_id <= max_stale_resource_id) {
        g_next_vfs_resource_id = max_stale_resource_id + 1;
    }
    s_vfs_lock.unlock();

    wki_remote_vfs_auto_discover_internal(&stale_exports);

    for (const auto& exp : stale_exports) {
        ResourceAdvertPayload withdraw = {};
        withdraw.node_id = g_wki.my_node_id;
        withdraw.resource_type = static_cast<uint16_t>(ResourceType::VFS);
        withdraw.resource_id = exp.resource_id;
        withdraw.flags = 0;
        withdraw.name_len = 0;

        for (size_t p = 0; p < WKI_MAX_PEERS; p++) {
            WkiPeer* peer = &g_wki.peers[p];
            if (peer->node_id == WKI_NODE_INVALID || peer->state != PeerState::CONNECTED) {
                continue;
            }
            wki_send(peer->node_id, WKI_CHAN_CONTROL, MsgType::RESOURCE_WITHDRAW, &withdraw, sizeof(withdraw));
        }
    }

    wki_remote_vfs_advertise_exports();
}

// -------------------------------------------------------------------------------
// Fencing Cleanup
// -------------------------------------------------------------------------------

void wki_remote_vfs_cleanup_for_peer(uint16_t node_id) {
    // Server side: close all remote FDs for this consumer
    for (auto& rfd : g_remote_fds) {
        if (!rfd.active || rfd.consumer_node != node_id) {
            continue;
        }
        if (rfd.file != nullptr) {
            if (rfd.file->fops != nullptr && rfd.file->fops->vfs_close != nullptr) {
                rfd.file->fops->vfs_close(rfd.file);
            }
            delete rfd.file;
            rfd.file = nullptr;
        }
        rfd.active = false;
    }
    std::erase_if(g_remote_fds, [](const RemoteVfsFd& rfd) { return !rfd.active; });

    // Consumer side: fail pending ops, unmount, and deactivate proxies
    for (auto& p : g_vfs_proxies) {
        if (!p->active || p->owner_node != node_id) {
            continue;
        }

        ker::vfs::vfs_stream_cache_invalidate_remote_scope(p.get());
        invalidate_readlink_cache(p.get());

        if (p->op_pending.load(std::memory_order_acquire)) {
            ker::mod::dbg::log("[WKI] VFS op FENCE_CLEANUP: node=0x%04x ch=%u op=%u seq=%u mount=%s", p->owner_node, p->assigned_channel,
                               p->op_expected_id, p->op_expected_seq, static_cast<const char*>(p->local_mount_path));
            p->op_status = -1;
            p->op_expected_id = 0;
            p->op_expected_seq = 0;
            p->op_pending.store(false, std::memory_order_release);
            if (p->op_wait_entry != nullptr) {
                wki_wake_op(p->op_wait_entry, -1);
            }
        }
        if (p->attach_pending.load(std::memory_order_acquire)) {
            p->attach_pending.store(false, std::memory_order_release);
            if (p->attach_wait_entry != nullptr) {
                wki_wake_op(p->attach_wait_entry, -1);
            }
        }

        WkiChannel* ch = wki_channel_get(p->owner_node, p->assigned_channel);
        if (ch != nullptr) {
            wki_channel_close(ch);
        }

        ker::mod::dbg::log("[WKI] Remote VFS proxy fenced: %s node=0x%04x", static_cast<const char*>(p->local_mount_path), node_id);
        p->active = false;
    }
}

}  // namespace ker::net::wki
