#include "remote_vfs.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <deque>
#include <memory>
#include <net/wki/dev_proxy.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <vfs/mount.hpp>
#include <vfs/vfs.hpp>

namespace ker::net::wki {

// ═══════════════════════════════════════════════════════════════════════════════
// Storage
// ═══════════════════════════════════════════════════════════════════════════════

namespace {

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

// -----------------------------------------------------------------------------
// Consumer side
// -----------------------------------------------------------------------------

// D7: Directory listing cache — avoids repeated round-trips for readdir()
struct DirCacheEntry {
    ProxyVfsState* proxy = nullptr;
    int32_t remote_fd = -1;
    uint64_t cache_time_us = 0;
    bool complete = false;  // true if we fetched all entries (server returned error/empty)
    std::deque<ker::vfs::DirEntry> entries;

    static constexpr uint64_t STALE_US = 5000000;  // 5 seconds
};
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

// unique_ptr indirection: ProxyVfsState contains Spinlock which has deleted move-assignment
std::deque<std::unique_ptr<ProxyVfsState>> g_vfs_proxies;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_remote_vfs_initialized = false;                     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto find_vfs_proxy_by_channel(uint16_t owner_node, uint16_t channel_id) -> ProxyVfsState* {
    for (auto& p : g_vfs_proxies) {
        if (p->active && p->owner_node == owner_node && p->assigned_channel == channel_id) {
            return p.get();
        }
    }
    return nullptr;
}

auto find_vfs_proxy_by_attach(uint16_t owner_node) -> ProxyVfsState* {
    for (auto& p : g_vfs_proxies) {
        if (p->attach_pending && p->owner_node == owner_node) {
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

// Helper: send DEV_OP_REQ and spin-wait for response
auto vfs_proxy_send_and_wait(ProxyVfsState* state, uint16_t op_id, const uint8_t* req_data, uint16_t req_data_len, void* resp_buf,
                             uint16_t resp_buf_max) -> int {
    auto req_total = static_cast<uint16_t>(sizeof(DevOpReqPayload) + req_data_len);
    auto* req_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(req_total));
    if (req_buf == nullptr) {
        return -1;
    }

    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf);
    req->op_id = op_id;
    req->data_len = req_data_len;

    if (req_data_len > 0 && req_data != nullptr) {
        memcpy(req_buf + sizeof(DevOpReqPayload), req_data, req_data_len);
    }

    state->lock.lock();
    state->op_pending = true;
    state->op_status = 0;
    state->op_resp_buf = resp_buf;
    state->op_resp_max = resp_buf_max;
    state->op_resp_len = 0;
    state->lock.unlock();

    int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, req_buf, req_total);
    ker::mod::mm::dyn::kmalloc::free(req_buf);

    if (send_ret != WKI_OK) {
        state->op_pending = false;
        return -1;
    }

    // Spin-wait for response with memory fence
    uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
    while (state->op_pending) {
        asm volatile("mfence" ::: "memory");
        if (!state->op_pending) {
            break;
        }
        if (wki_now_us() >= deadline) {
            state->op_pending = false;
            return -1;
        }
        wki_spin_yield();
    }

    return static_cast<int>(state->op_status);
}

// -----------------------------------------------------------------------------
// D6: Write-behind flush helper
// -----------------------------------------------------------------------------

void flush_write_behind(RemoteFileContext* ctx) {
    if (ctx->write_buf == nullptr || ctx->write_buf->pending_len == 0) {
        return;
    }

    auto* wb = ctx->write_buf;
    auto* src = wb->data.data();
    auto remaining = static_cast<uint32_t>(wb->pending_len);
    auto cur_offset = wb->pending_offset;

    constexpr uint32_t WRITE_HDR_OVERHEAD = sizeof(DevOpReqPayload) + 12;
    auto max_data = static_cast<uint32_t>(WKI_ETH_MAX_PAYLOAD - WRITE_HDR_OVERHEAD);

    while (remaining > 0) {
        uint32_t chunk = (remaining > max_data) ? max_data : remaining;

        auto req_data_len = static_cast<uint16_t>(12 + chunk);
        auto* req_data = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(req_data_len));
        if (req_data == nullptr) {
            break;
        }

        int32_t remote_fd = ctx->remote_fd;
        memcpy(req_data, &remote_fd, sizeof(int32_t));
        memcpy(req_data + 4, &cur_offset, sizeof(int64_t));
        memcpy(req_data + 12, src, chunk);

        uint32_t written = 0;
        vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_WRITE, req_data, req_data_len, &written, sizeof(uint32_t));
        ker::mod::mm::dyn::kmalloc::free(req_data);

        src += chunk;
        cur_offset += static_cast<int64_t>(chunk);
        remaining -= chunk;
    }

    wb->pending_offset = -1;
    wb->pending_len = 0;
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
        ker::mod::mm::dyn::kmalloc::free(ctx);
        f->private_data = nullptr;
        return -1;
    }

    // D6: Flush pending writes before closing
    flush_write_behind(ctx);

    // D7: Invalidate directory cache for this file
    invalidate_dir_cache(ctx->proxy, ctx->remote_fd);

    // Send OP_VFS_CLOSE: {remote_fd:i32} = 4 bytes
    int32_t remote_fd = ctx->remote_fd;
    vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_CLOSE, reinterpret_cast<const uint8_t*>(&remote_fd), sizeof(int32_t), nullptr, 0);

    // D6: Free caches
    delete ctx->read_cache;
    delete ctx->write_buf;

    ker::mod::mm::dyn::kmalloc::free(ctx);
    f->private_data = nullptr;
    return 0;
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
        flush_write_behind(ctx);
    }

    auto* dest = static_cast<uint8_t*>(buf);
    auto remaining = static_cast<uint32_t>(count);
    auto cur_offset = static_cast<int64_t>(offset);
    ssize_t total_read = 0;

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
        // D6: Request max(remaining, VFS_CACHE_SIZE) to pre-fill read-ahead cache
        uint32_t fetch_size = std::max(remaining, static_cast<uint32_t>(VFS_CACHE_SIZE));
        fetch_size = std::min(fetch_size, max_resp_data);

        // Lazily allocate read-ahead cache
        if (ctx->read_cache == nullptr && fetch_size > remaining) {
            ctx->read_cache = new ReadAheadCache();  // NOLINT(cppcoreguidelines-owning-memory)
        }

        // Use cache buffer for fetching if we're reading ahead
        uint8_t* fetch_dest = dest;
        bool using_cache = false;
        if (ctx->read_cache != nullptr && fetch_size > remaining) {
            fetch_dest = ctx->read_cache->data.data();
            using_cache = true;
        }

        uint32_t chunk = fetch_size;

        // Build request: {remote_fd:i32, len:u32, offset:i64} = 16 bytes
        std::array<uint8_t, 16> req_data{};
        memcpy(req_data.data(), &ctx->remote_fd, sizeof(int32_t));
        memcpy(req_data.data() + 4, &chunk, sizeof(uint32_t));
        memcpy(req_data.data() + 8, &cur_offset, sizeof(int64_t));

        int status = vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_READ, req_data.data(), 16, fetch_dest, static_cast<uint16_t>(chunk));
        if (status != 0) {
            return (total_read > 0) ? total_read : -1;
        }

        uint16_t bytes_read = ctx->proxy->op_resp_len;
        if (bytes_read == 0) {
            break;  // EOF
        }

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
                flush_write_behind(ctx);
            }
            continue;
        }

        // Non-sequential or buffer full: flush existing buffer first
        flush_write_behind(ctx);

        // If the data exceeds the buffer size, send directly (bypass buffering)
        if (remaining >= VFS_CACHE_SIZE) {
            uint32_t chunk = (remaining > max_data) ? max_data : remaining;

            auto req_data_len = static_cast<uint16_t>(12 + chunk);
            auto* req_data = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(req_data_len));
            if (req_data == nullptr) {
                return (total_written > 0) ? total_written : -1;
            }

            int32_t remote_fd = ctx->remote_fd;
            memcpy(req_data, &remote_fd, sizeof(int32_t));
            memcpy(req_data + 4, &cur_offset, sizeof(int64_t));
            memcpy(req_data + 12, src, chunk);

            uint32_t written = 0;
            int status = vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_WRITE, req_data, req_data_len, &written, sizeof(uint32_t));
            ker::mod::mm::dyn::kmalloc::free(req_data);

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
        // Loop back — remaining data will be buffered on next iteration
    }

    return total_written;
}

auto remote_vfs_lseek(ker::vfs::File* f, off_t offset, int whence) -> off_t {
    // Local-only: adjust f->pos without network roundtrip
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
        case 2:  // SEEK_END — not supported without server roundtrip
            return -1;
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

    // D7: Check directory cache first
    DirCacheEntry* cache = find_dir_cache(ctx->proxy, ctx->remote_fd);

    // Invalidate stale cache
    if (cache != nullptr && (wki_now_us() - cache->cache_time_us) > DirCacheEntry::STALE_US) {
        invalidate_dir_cache(ctx->proxy, ctx->remote_fd);
        cache = nullptr;
    }

    // Cache hit: entry already fetched
    if (cache != nullptr && index < cache->entries.size()) {
        *entry = cache->entries[index];
        return 0;
    }

    // Cache hit but we already know directory is exhausted
    if (cache != nullptr && cache->complete && index >= cache->entries.size()) {
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

    // Fetch entries from server until we have the requested index (or hit end)
    while (cache->entries.size() <= index) {
        auto fetch_idx = static_cast<uint32_t>(cache->entries.size());

        std::array<uint8_t, 8> req_data{};
        memcpy(req_data.data(), &ctx->remote_fd, sizeof(int32_t));
        memcpy(req_data.data() + 4, &fetch_idx, sizeof(uint32_t));

        ker::vfs::DirEntry fetched = {};
        int status = vfs_proxy_send_and_wait(ctx->proxy, OP_VFS_READDIR, req_data.data(), 8, &fetched, sizeof(ker::vfs::DirEntry));
        if (status != 0 || ctx->proxy->op_resp_len == 0) {
            cache->complete = true;
            return -1;
        }

        cache->entries.push_back(fetched);
    }

    *entry = cache->entries[index];
    return 0;
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
    // remote file is not meaningful — readlink operates on paths, not FDs.
    // Return -1 as unsupported on open files.
    (void)bufsize;
    return -1;
}

// Static FileOperations for remote VFS files
ker::vfs::FileOperations g_remote_vfs_fops = {
    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
    .vfs_open = nullptr,           .vfs_close = remote_vfs_close,   .vfs_read = remote_vfs_read,       .vfs_write = remote_vfs_write,
    .vfs_lseek = remote_vfs_lseek, .vfs_isatty = remote_vfs_isatty, .vfs_readdir = remote_vfs_readdir, .vfs_readlink = remote_vfs_readlink,
};

}  // namespace

// ═══════════════════════════════════════════════════════════════════════════════
// Init
// ═══════════════════════════════════════════════════════════════════════════════

void wki_remote_vfs_init() {
    if (g_remote_vfs_initialized) {
        return;
    }
    g_remote_vfs_initialized = true;
    ker::mod::dbg::log("[WKI] Remote VFS subsystem initialized");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Server Side — VFS Export Management
// ═══════════════════════════════════════════════════════════════════════════════

auto wki_remote_vfs_export_add(const char* export_path, const char* name) -> uint32_t {
    if (export_path == nullptr || name == nullptr) {
        return 0;
    }

    // Check if this path is already exported (prevent duplicates)
    for (const auto& existing : g_vfs_exports) {
        if (existing.active && strcmp(existing.export_path, export_path) == 0) {
            // Already exported - return existing resource_id
            return existing.resource_id;
        }
    }

    VfsExport exp;
    exp.active = true;
    exp.resource_id = g_next_vfs_resource_id++;

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

    ker::mod::dbg::log("[WKI] VFS export added: %s -> %s (resource_id=%u)", static_cast<const char*>(exp.name),
                       static_cast<const char*>(exp.export_path), exp.resource_id);
    return exp.resource_id;
}

auto wki_remote_vfs_find_export(uint32_t resource_id) -> VfsExport* {
    for (auto& exp : g_vfs_exports) {
        if (exp.active && exp.resource_id == resource_id) {
            return &exp;
        }
    }
    return nullptr;
}

void wki_remote_vfs_advertise_exports() {
    if (!g_remote_vfs_initialized) {
        return;
    }

    for (auto& exp : g_vfs_exports) {
        if (!exp.active) {
            continue;
        }

        // Build ResourceAdvertPayload + name
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

        for (size_t p = 0; p < WKI_MAX_PEERS; p++) {
            WkiPeer* peer = &g_wki.peers[p];
            if (peer->node_id == WKI_NODE_INVALID || peer->state != PeerState::CONNECTED) {
                continue;
            }
            wki_send(peer->node_id, WKI_CHAN_CONTROL, MsgType::RESOURCE_ADVERT, buf.data(), total_len);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Server Side — VFS Operation Handlers
// ═══════════════════════════════════════════════════════════════════════════════

namespace detail {

void handle_vfs_op(const WkiHeader* hdr, uint16_t channel_id, const char* export_path, uint16_t op_id, const uint8_t* data,
                   uint16_t data_len) {
    switch (op_id) {
        case OP_VFS_OPEN: {
            // Request: {flags:u32, mode:u32, path_len:u16, path[path_len]}
            if (data_len < 10) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_OPEN;
                resp.status = -1;
                resp.data_len = 0;
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
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            // Build full path: export_path + "/" + relative_path
            std::array<char, 512> full_path{};  // NOLINT(modernize-avoid-c-arrays)
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 10), path_len);

            // Open the file using vfs_open_file (no task/FD context required)
            ker::vfs::File* file = ker::vfs::vfs_open_file(full_path.data(), static_cast<int>(flags), static_cast<int>(mode));
            if (file == nullptr) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_OPEN;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            // Allocate a remote FD
            int32_t fd_id = alloc_remote_fd(hdr->src_node, channel_id, file);

            // Response: {remote_fd:i32} = 4 bytes
            std::array<uint8_t, sizeof(DevOpRespPayload) + 4> resp_buf{};  // NOLINT(modernize-avoid-c-arrays)
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = OP_VFS_OPEN;
            resp->status = 0;
            resp->data_len = 4;
            memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &fd_id, sizeof(int32_t));

            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf.data(), static_cast<uint16_t>(sizeof(DevOpRespPayload) + 4));

            ker::mod::dbg::log("[WKI] VFS open: node=0x%04x path=%s fd=%d", hdr->src_node, full_path, fd_id);
            break;
        }

        case OP_VFS_READ: {
            // Request: {remote_fd:i32, len:u32, offset:i64} = 16 bytes
            if (data_len < 16) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            int32_t fd_id = 0;
            uint32_t len = 0;
            int64_t offset = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&len, data + 4, sizeof(uint32_t));
            memcpy(&offset, data + 8, sizeof(int64_t));

            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_read == nullptr) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }
            touch_remote_fd(rfd);

            // Clamp to max response payload
            auto max_resp_data = static_cast<uint16_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload));
            len = std::min<uint32_t>(len, max_resp_data);

            auto resp_total = static_cast<uint16_t>(sizeof(DevOpRespPayload) + len);
            auto* resp_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(resp_total));
            if (resp_buf == nullptr) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READ;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf);
            uint8_t* read_buf = resp_buf + sizeof(DevOpRespPayload);

            ssize_t bytes_read = rfd->file->fops->vfs_read(rfd->file, read_buf, len, static_cast<size_t>(offset));

            resp->op_id = OP_VFS_READ;
            if (bytes_read >= 0) {
                resp->status = 0;
                resp->data_len = static_cast<uint16_t>(bytes_read);
            } else {
                resp->status = static_cast<int16_t>(bytes_read);
                resp->data_len = 0;
            }
            resp->reserved = 0;

            uint16_t send_len = (bytes_read > 0) ? static_cast<uint16_t>(sizeof(DevOpRespPayload) + bytes_read)
                                                 : static_cast<uint16_t>(sizeof(DevOpRespPayload));
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf, send_len);
            ker::mod::mm::dyn::kmalloc::free(resp_buf);
            break;
        }

        case OP_VFS_WRITE: {
            // Request: {remote_fd:i32, offset:i64, data[N]} = 12 + N bytes
            if (data_len < 12) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_WRITE;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            int32_t fd_id = 0;
            int64_t offset = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&offset, data + 4, sizeof(int64_t));

            const uint8_t* write_data = data + 12;
            auto write_len = static_cast<uint16_t>(data_len - 12);

            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_write == nullptr) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_WRITE;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }
            touch_remote_fd(rfd);

            ssize_t bytes_written = rfd->file->fops->vfs_write(rfd->file, write_data, write_len, static_cast<size_t>(offset));

            // Response: {written:u32} = 4 bytes
            std::array<uint8_t, sizeof(DevOpRespPayload) + 4> resp_buf{};  // NOLINT(modernize-avoid-c-arrays)
            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = OP_VFS_WRITE;
            resp->status = (bytes_written >= 0) ? static_cast<int16_t>(0)
                                                : static_cast<int16_t>(std::max(bytes_written, static_cast<ssize_t>(SHRT_MIN)));
            resp->data_len = 4;
            auto written = static_cast<uint32_t>((bytes_written >= 0) ? bytes_written : 0);
            memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &written, sizeof(uint32_t));

            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf.data(), static_cast<uint16_t>(sizeof(DevOpRespPayload) + 4));
            break;
        }

        case OP_VFS_CLOSE: {
            // Request: {remote_fd:i32} = 4 bytes
            if (data_len < 4) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_CLOSE;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            int32_t fd_id = 0;
            memcpy(&fd_id, data, sizeof(int32_t));

            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            touch_remote_fd(rfd);
            int16_t status = -1;
            if (rfd != nullptr && rfd->file != nullptr) {
                if (rfd->file->fops != nullptr && rfd->file->fops->vfs_close != nullptr) {
                    rfd->file->fops->vfs_close(rfd->file);
                }
                ker::mod::mm::dyn::kmalloc::free(rfd->file);
                rfd->file = nullptr;
                rfd->active = false;
                status = 0;
            }

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_CLOSE;
            resp.status = status;
            resp.data_len = 0;
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));

            // Clean up inactive remote FDs
            std::erase_if(g_remote_fds, [](const RemoteVfsFd& rfd) { return !rfd.active; });
            break;
        }

        case OP_VFS_READDIR: {
            // Request: {remote_fd:i32, index:u32} = 8 bytes
            if (data_len < 8) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            int32_t fd_id = 0;
            uint32_t index = 0;
            memcpy(&fd_id, data, sizeof(int32_t));
            memcpy(&index, data + 4, sizeof(uint32_t));

            RemoteVfsFd* rfd = find_remote_fd(hdr->src_node, channel_id, fd_id);
            if (rfd == nullptr || rfd->file == nullptr || rfd->file->fops == nullptr || rfd->file->fops->vfs_readdir == nullptr) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }
            touch_remote_fd(rfd);

            ker::vfs::DirEntry entry = {};
            int ret = rfd->file->fops->vfs_readdir(rfd->file, &entry, index);

            if (ret != 0) {
                // End of directory or error
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READDIR;
                resp.status = static_cast<int16_t>(ret);
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            } else {
                // Send entry
                auto resp_total = static_cast<uint16_t>(sizeof(DevOpRespPayload) + sizeof(ker::vfs::DirEntry));
                std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(ker::vfs::DirEntry)> resp_buf{};
                auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
                resp->op_id = OP_VFS_READDIR;
                resp->status = 0;
                resp->data_len = sizeof(ker::vfs::DirEntry);
                memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &entry, sizeof(ker::vfs::DirEntry));
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf.data(), resp_total);
            }
            break;
        }

        case OP_VFS_STAT: {
            // Request: {path_len:u16, path[path_len]}
            if (data_len < 2) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_STAT;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            uint16_t path_len = 0;
            memcpy(&path_len, data, sizeof(uint16_t));

            if (data_len < 2 + path_len) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_STAT;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            std::array<char, 512> full_path{};
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 2), path_len);

            ker::vfs::stat statbuf = {};
            int ret = ker::vfs::vfs_stat(full_path.data(), &statbuf);

            if (ret != 0) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_STAT;
                resp.status = static_cast<int16_t>(ret);
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            } else {
                auto resp_total = static_cast<uint16_t>(sizeof(DevOpRespPayload) + sizeof(ker::vfs::stat));
                auto* resp_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(resp_total));
                if (resp_buf == nullptr) {
                    DevOpRespPayload resp = {};
                    resp.op_id = OP_VFS_STAT;
                    resp.status = -1;
                    resp.data_len = 0;
                    wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                    break;
                }

                auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf);
                resp->op_id = OP_VFS_STAT;
                resp->status = 0;
                resp->data_len = sizeof(ker::vfs::stat);
                resp->reserved = 0;
                memcpy(resp_buf + sizeof(DevOpRespPayload), &statbuf, sizeof(ker::vfs::stat));
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf, resp_total);
                ker::mod::mm::dyn::kmalloc::free(resp_buf);
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
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
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
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            std::array<char, 512> full_path{};
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 6), path_len);

            int ret = ker::vfs::vfs_mkdir(full_path.data(), static_cast<int>(mode));

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_MKDIR;
            resp.status = static_cast<int16_t>(ret);
            resp.data_len = 0;
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }

        case OP_VFS_READLINK: {
            // D8: Request: {path_len:u16, path[path_len]}
            if (data_len < 2) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READLINK;
                resp.status = -1;
                resp.data_len = 0;
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
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            std::array<char, 512> full_path{};
            build_full_path(full_path.data(), full_path.size(), export_path, reinterpret_cast<const char*>(data + 2), path_len);

            // Read the symlink target
            std::array<char, 512> target_buf{};
            ssize_t target_len = ker::vfs::vfs_readlink(full_path.data(), target_buf.data(), target_buf.size() - 1);

            if (target_len < 0) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_VFS_READLINK;
                resp.status = static_cast<int16_t>(target_len);
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            } else {
                // Response: {target_len:u16, target[]}
                auto resp_data_len = static_cast<uint16_t>(2 + target_len);
                auto resp_total = static_cast<uint16_t>(sizeof(DevOpRespPayload) + resp_data_len);
                auto* resp_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(resp_total));
                if (resp_buf == nullptr) {
                    DevOpRespPayload resp = {};
                    resp.op_id = OP_VFS_READLINK;
                    resp.status = -1;
                    resp.data_len = 0;
                    wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                    break;
                }

                auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf);
                resp->op_id = OP_VFS_READLINK;
                resp->status = 0;
                resp->data_len = resp_data_len;
                resp->reserved = 0;

                auto tlen = static_cast<uint16_t>(target_len);
                memcpy(resp_buf + sizeof(DevOpRespPayload), &tlen, sizeof(uint16_t));
                memcpy(resp_buf + sizeof(DevOpRespPayload) + 2, target_buf.data(), target_len);

                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf, resp_total);
                ker::mod::mm::dyn::kmalloc::free(resp_buf);
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

            int ret = ker::vfs::vfs_symlink(target_str.data(), full_link.data());

            DevOpRespPayload resp = {};
            resp.op_id = OP_VFS_SYMLINK;
            resp.status = static_cast<int16_t>(ret);
            resp.data_len = 0;
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }

        default: {
            DevOpRespPayload resp = {};
            resp.op_id = op_id;
            resp.status = -1;
            resp.data_len = 0;
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }
    }
}

}  // namespace detail

// ═══════════════════════════════════════════════════════════════════════════════
// Consumer Side — Mount / Open / Response Handlers
// ═══════════════════════════════════════════════════════════════════════════════

auto wki_remote_vfs_mount(uint16_t owner_node, uint32_t resource_id, const char* local_mount_path) -> int {
    if (local_mount_path == nullptr) {
        return -1;
    }

    // Allocate proxy state
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

    // Send DEV_ATTACH_REQ
    DevAttachReqPayload attach_req = {};
    attach_req.target_node = owner_node;
    attach_req.resource_type = static_cast<uint16_t>(ResourceType::VFS);
    attach_req.resource_id = resource_id;
    attach_req.attach_mode = static_cast<uint8_t>(AttachMode::PROXY);
    attach_req.requested_channel = 0;

    state->attach_pending = true;
    state->attach_status = 0;
    state->attach_channel = 0;

    int send_ret = wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ, &attach_req, sizeof(attach_req));
    if (send_ret != WKI_OK) {
        g_vfs_proxies.pop_back();
        return -1;
    }

    // Spin-wait for attach ACK with memory fence
    uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
    while (state->attach_pending) {
        asm volatile("mfence" ::: "memory");
        if (!state->attach_pending) {
            break;
        }
        if (wki_now_us() >= deadline) {
            state->attach_pending = false;
            g_vfs_proxies.pop_back();
            ker::mod::dbg::log("[WKI] Remote VFS attach timeout: node=0x%04x res_id=%u", owner_node, resource_id);
            return -1;
        }
        wki_spin_yield();
    }

    if (state->attach_status != static_cast<uint8_t>(DevAttachStatus::OK)) {
        ker::mod::dbg::log("[WKI] Remote VFS attach rejected: status=%u", state->attach_status);
        g_vfs_proxies.pop_back();
        return -1;
    }

    state->assigned_channel = state->attach_channel;
    state->max_op_size = state->attach_max_op_size;
    state->active = true;

    // Create the mount point with "remote" fstype
    int mount_ret = ker::vfs::mount_filesystem(local_mount_path, "remote", nullptr);
    if (mount_ret != 0) {
        ker::mod::dbg::log("[WKI] Remote VFS mount failed at %s", local_mount_path);
        state->active = false;
        g_vfs_proxies.pop_back();
        return -1;
    }

    // Find the mount point and set private_data + fops
    ker::vfs::MountPoint* mount = ker::vfs::find_mount_point(local_mount_path);
    if (mount != nullptr) {
        mount->private_data = state;
        mount->fops = &g_remote_vfs_fops;
    }

    ker::mod::dbg::log("[WKI] Remote VFS mounted: %s -> node=0x%04x res_id=%u ch=%u", local_mount_path, owner_node, resource_id,
                       state->assigned_channel);
    return 0;
}

void wki_remote_vfs_unmount(const char* local_mount_path) {
    if (local_mount_path == nullptr) {
        return;
    }

    auto* state = find_vfs_proxy_by_mount(local_mount_path);
    if (state == nullptr) {
        return;
    }

    // Send DEV_DETACH
    DevDetachPayload det = {};
    det.target_node = state->owner_node;
    det.resource_type = static_cast<uint16_t>(ResourceType::VFS);
    det.resource_id = state->resource_id;
    wki_send(state->owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));

    // Close the dynamic channel
    WkiChannel* ch = wki_channel_get(state->owner_node, state->assigned_channel);
    if (ch != nullptr) {
        wki_channel_close(ch);
    }

    // Unmount
    ker::vfs::unmount_filesystem(local_mount_path);

    state->active = false;

    // Remove inactive
    for (auto it = g_vfs_proxies.begin(); it != g_vfs_proxies.end();) {
        if (!(*it)->active) {
            it = g_vfs_proxies.erase(it);
        } else {
            ++it;
        }
    }
}

auto wki_remote_vfs_open_path(const char* fs_relative_path, int flags, int mode, void* mount_private_data) -> ker::vfs::File* {
    auto* state = static_cast<ProxyVfsState*>(mount_private_data);
    if (state == nullptr || !state->active) {
        return nullptr;
    }

    // Build request: {flags:u32, mode:u32, path_len:u16, path[N]}
    auto path_len = static_cast<uint16_t>(strlen(fs_relative_path));
    auto req_data_len = static_cast<uint16_t>(10 + path_len);
    auto* req_data = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(req_data_len + 1));
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

    // Response: {remote_fd:i32} = 4 bytes
    int32_t remote_fd = -1;
    int status = vfs_proxy_send_and_wait(state, OP_VFS_OPEN, req_data, req_data_len, &remote_fd, sizeof(int32_t));
    ker::mod::mm::dyn::kmalloc::free(req_data);

    if (status != 0 || remote_fd < 0) {
        return nullptr;
    }

    // Allocate File + RemoteFileContext
    auto* file = static_cast<ker::vfs::File*>(ker::mod::mm::dyn::kmalloc::malloc(sizeof(ker::vfs::File)));
    if (file == nullptr) {
        return nullptr;
    }

    auto* ctx = static_cast<RemoteFileContext*>(ker::mod::mm::dyn::kmalloc::malloc(sizeof(RemoteFileContext)));
    if (ctx == nullptr) {
        ker::mod::mm::dyn::kmalloc::free(file);
        return nullptr;
    }

    ctx->proxy = state;
    ctx->remote_fd = remote_fd;

    file->fd = -1;  // Will be assigned by vfs_alloc_fd
    file->private_data = ctx;
    file->fops = &g_remote_vfs_fops;
    file->pos = 0;
    file->is_directory = false;
    file->fs_type = ker::vfs::FSType::REMOTE;
    file->refcount = 1;

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

    int status = vfs_proxy_send_and_wait(state, OP_VFS_STAT, req_data, req_data_len, statbuf, sizeof(ker::vfs::stat));
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
    return status;
}

auto wki_remote_vfs_get_fops() -> ker::vfs::FileOperations* { return &g_remote_vfs_fops; }

// ═══════════════════════════════════════════════════════════════════════════════
// Consumer Side — RX Handlers
// ═══════════════════════════════════════════════════════════════════════════════

namespace detail {

void handle_vfs_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevAttachAckPayload)) {
        return;
    }

    const auto* ack = reinterpret_cast<const DevAttachAckPayload*>(payload);

    ProxyVfsState* state = find_vfs_proxy_by_attach(hdr->src_node);
    if (state == nullptr) {
        return;
    }

    state->attach_status = ack->status;
    state->attach_channel = ack->assigned_channel;
    state->attach_max_op_size = ack->max_op_size;

    asm volatile("" ::: "memory");
    state->attach_pending = false;
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

    // Find VFS proxy by (src_node, channel_id)
    ProxyVfsState* state = find_vfs_proxy_by_channel(hdr->src_node, hdr->channel_id);
    if (state == nullptr || !state->op_pending) {
        return;
    }

    state->lock.lock();
    state->op_status = resp->status;

    if (resp_data_len > 0 && state->op_resp_buf != nullptr) {
        uint16_t copy_len = (resp_data_len > state->op_resp_max) ? state->op_resp_max : resp_data_len;
        memcpy(state->op_resp_buf, resp_data, copy_len);
        state->op_resp_len = copy_len;
    } else {
        state->op_resp_len = 0;
    }

    state->lock.unlock();

    asm volatile("" ::: "memory");
    state->op_pending = false;
}

}  // namespace detail

// ═══════════════════════════════════════════════════════════════════════════════
// D10: Stale Remote FD Garbage Collection
// ═══════════════════════════════════════════════════════════════════════════════

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
            ker::mod::mm::dyn::kmalloc::free(rfd.file);
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

// ═══════════════════════════════════════════════════════════════════════════════
// D9: Auto-Discover Exportable Mount Points
// ═══════════════════════════════════════════════════════════════════════════════

void wki_remote_vfs_auto_discover() {
    if (!g_remote_vfs_initialized) {
        return;
    }

    size_t mount_count = ker::vfs::get_mount_count();
    for (size_t i = 0; i < mount_count; i++) {
        auto* mp = ker::vfs::get_mount_at(i);
        if (mp == nullptr || mp->path == nullptr) {
            continue;
        }

        // Skip REMOTE mounts (don't re-export remote filesystems)
        // Skip DEVFS mounts (not meaningful to export)
        if (mp->fs_type == ker::vfs::FSType::REMOTE || mp->fs_type == ker::vfs::FSType::DEVFS) {
            continue;
        }

        // Check if this path is already exported
        bool already_exported = false;
        for (const auto& exp : g_vfs_exports) {
            if (exp.active && strncmp(static_cast<const char*>(exp.export_path), mp->path, VFS_EXPORT_PATH_LEN) == 0) {
                already_exported = true;
                break;
            }
        }
        if (already_exported) {
            continue;
        }

        wki_remote_vfs_export_add(mp->path, mp->path);
    }

    wki_remote_vfs_advertise_exports();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fencing Cleanup
// ═══════════════════════════════════════════════════════════════════════════════

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
            ker::mod::mm::dyn::kmalloc::free(rfd.file);
            rfd.file = nullptr;
        }
        rfd.active = false;
    }
    std::erase_if(g_remote_fds, [](const RemoteVfsFd& rfd) { return !rfd.active; });

    // Consumer side: fail pending ops and deactivate proxies
    for (auto& p : g_vfs_proxies) {
        if (!p->active || p->owner_node != node_id) {
            continue;
        }

        if (p->op_pending) {
            p->op_status = -1;
            p->op_pending = false;
        }

        WkiChannel* ch = wki_channel_get(p->owner_node, p->assigned_channel);
        if (ch != nullptr) {
            wki_channel_close(ch);
        }

        ker::mod::dbg::log("[WKI] Remote VFS proxy fenced: %s node=0x%04x", static_cast<const char*>(p->local_mount_path), node_id);
        p->active = false;
    }

    for (auto it = g_vfs_proxies.begin(); it != g_vfs_proxies.end();) {
        if (!(*it)->active) {
            it = g_vfs_proxies.erase(it);
        } else {
            ++it;
        }
    }
}

}  // namespace ker::net::wki
